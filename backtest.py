import pyupbit
import pandas as pd
from datetime import datetime, timedelta, timezone
from ta.trend import ADXIndicator
from ta.momentum import StochRSIIndicator

# ===================== 설정 =====================
TICKER        = "KRW-MIRA"
INTERVAL      = "minute5"
BACKTEST_DAYS = 7

INITIAL_CASH  = 100_000
INVEST_RATIO  = 1.00
FEE_RATE      = 0.0005
SLIPPAGE      = 0.0005

TENKAN_PERIOD = 9
KIJUN_PERIOD  = 26
ADX_PERIOD    = 14
STOCH_LEN     = 14
STOCH_K       = 14
STOCH_D       = 5
SMA_PERIOD    = 48

# 분할 청산 트리거(보유수익률 %, 진입가 대비)와 비중
PARTIAL_TRIGGERS = [0.5, 1.0, 1.5, 2.0]     # %
PARTIAL_FRACTION = 0.25                # 각 트리거마다 잔여의 25%

STOP_LOSS_PCT    = -1.0                # %
EXIT_ON_TENKAN_SLOPE = True            # 텐칸 기울기 하락 시 전량 매도

# ===================== 데이터 로딩 =====================
def fetch_minutes(ticker, interval="minute1", days=3, per_call=200):
    end = datetime.now(timezone.utc)
    frames, remain = [], days * 24 * 60
    to_cursor = end
    for _ in range((remain // per_call) + 5):
        df = pyupbit.get_ohlcv(ticker, interval=interval, count=per_call, to=to_cursor)
        if df is None or df.empty:
            break
        frames.append(df)
        oldest = df.index.min()
        to_cursor = oldest - timedelta(seconds=1)
        remain -= len(df)
        if remain <= 0: break
    if not frames: return pd.DataFrame()
    return pd.concat(frames).sort_index().drop_duplicates()

# ===================== 인디케이터 =====================
def add_indicators(df):
    high, low, close = df['high'], df['low'], df['close']
    # Ichimoku: Tenkan/Kijun (basic)
    df['tenkan'] = (high.rolling(TENKAN_PERIOD).max() + low.rolling(TENKAN_PERIOD).min())/2
    df['kijun']  = (high.rolling(KIJUN_PERIOD).max() + low.rolling(KIJUN_PERIOD).min())/2
    df['tenkan_prev'] = df['tenkan'].shift(1)

    # DMI
    adx = ADXIndicator(high=high, low=low, close=close, window=ADX_PERIOD)
    df['di_pos'] = adx.adx_pos()
    df['di_neg'] = adx.adx_neg()

    # Stoch RSI
    stoch = StochRSIIndicator(close=close, window=STOCH_LEN, smooth1=STOCH_K, smooth2=STOCH_D)
    df['stoch_k'] = stoch.stochrsi_k()
    df['stoch_d'] = stoch.stochrsi_d()

    # SMA
    df['sma48'] = close.rolling(SMA_PERIOD).mean()

    # Buy signal
    golden = (df['tenkan'] > df['kijun']) & (df['tenkan_prev'] <= df['kijun'].shift(1))
    dmi_ok = df['di_pos'] > df['di_neg']
    stoch_ok = df['stoch_k'] > df['stoch_d']
    price_above = df['close'] > df['sma48']
    df['buy_sig'] = golden & dmi_ok & stoch_ok & price_above
    return df

# ===================== 백테스트 =====================
def backtest(df):
    cash   = INITIAL_CASH
    units  = 0.0
    entry  = None
    in_pos = False

    # 각 포지션별 트리거 소모 여부
    hit = None

    records = []
    cols_needed = ['tenkan','tenkan_prev','kijun','di_pos','di_neg',
                   'stoch_k','stoch_d','sma48','buy_sig','high','low','close']
    for ts, row in df.iterrows():
        if any(pd.isna(row[c]) for c in cols_needed):
            continue

        price = float(row['close'])
        high  = float(row['high'])
        low   = float(row['low'])

        # ===== 매수 =====
        if (not in_pos) and bool(row['buy_sig']):
            buy = price * (1 + SLIPPAGE)
            budget = cash * INVEST_RATIO
            if budget <= 0: 
                continue
            qty = budget / buy
            cost = buy * qty
            fee  = cost * FEE_RATE
            out  = cost + fee
            if out > cash:
                qty = cash / (buy * (1 + FEE_RATE))
                if qty <= 0: 
                    continue
                cost = buy * qty
                fee  = cost * FEE_RATE
                out  = cost + fee
            cash  -= out
            units  = qty
            entry  = buy
            in_pos = True
            # 트리거 초기화(새 포지션마다 리셋)
            hit = {thr: False for thr in PARTIAL_TRIGGERS}
            records.append({"time": ts, "side": "BUY", "price": round(buy,2),
                            "qty": qty, "cash_after": round(cash,2),
                            "note": "조건매수(골크 & DI+>DI- & K>D & Close>SMA48)"})
            continue

        # ===== 보유 =====
        if in_pos:
            # 0) 손절: 저가가 손절가 이하이면 전량 청산
            stop_loss_price = entry * (1 + STOP_LOSS_PCT/100.0)
            if low <= stop_loss_price and units > 0:
                sell = stop_loss_price * (1 - SLIPPAGE)
                proceed = sell * units
                fee = proceed * FEE_RATE
                cash += (proceed - fee)
                pnl = (sell - entry) / entry * 100.0
                records.append({"time": ts, "side": "SELL", "price": round(sell,2),
                                "qty": units, "cash_after": round(cash,2),
                                "pnl_%": round(pnl,4), "note": "손절 -1.0%"})
                units = 0.0; entry = None; in_pos = False
                continue

            # 1) 분할 청산: 한 캔들에 여러 트리거 동시에 체결 가능
            #   트리거는 '진입가 대비 수익률' 기준 → 목표가 = entry*(1+thr%)
            for thr in sorted(PARTIAL_TRIGGERS):
                if units <= 0: break
                if hit[thr]:    # 이미 체결한 트리거는 건너뜀
                    continue
                target = entry * (1 + thr/100.0)
                if high >= target:
                    sell_qty = units * PARTIAL_FRACTION
                    sell = target  # 보수적: 목표가 체결 가정 (원하면 -SLIPPAGE 적용)
                    proceed = sell * sell_qty
                    fee = proceed * FEE_RATE
                    cash += (proceed - fee)
                    pnl = (sell - entry) / entry * 100.0
                    units -= sell_qty
                    hit[thr] = True
                    records.append({"time": ts, "side": "SELL", "price": round(sell,2),
                                    "qty": sell_qty, "cash_after": round(cash,2),
                                    "pnl_%": round(pnl,4),
                                    "note": f"부분청산 +{thr:.2f}%"})

            # 2) 텐칸 기울기 하락 시 잔량 전량 매도(종가 기준)
            if EXIT_ON_TENKAN_SLOPE and units > 0:
                if row['tenkan'] < row['tenkan_prev']:
                    sell = price * (1 - SLIPPAGE)
                    proceed = sell * units
                    fee = proceed * FEE_RATE
                    cash += (proceed - fee)
                    pnl = (sell - entry) / entry * 100.0
                    records.append({"time": ts, "side": "SELL", "price": round(sell,2),
                                    "qty": units, "cash_after": round(cash,2),
                                    "pnl_%": round(pnl,4), "note": "텐칸 기울기 하락"})
                    units = 0.0; entry = None; in_pos = False

    # 미청산 포지션 강제청산
    if in_pos and len(df) > 0:
        last = float(df.iloc[-1]['close']) * (1 - SLIPPAGE)
        proceed = last * units
        fee = proceed * FEE_RATE
        cash += (proceed - fee)
        pnl = (last - entry) / entry * 100.0
        records.append({"time": df.index[-1], "side": "SELL", "price": round(last,2),
                        "qty": units, "cash_after": round(cash,2),
                        "pnl_%": round(pnl,4), "note": "종가 강제청산"})
        units = 0.0; entry = None; in_pos = False

    log = pd.DataFrame(records)
    sells = log[log['side'] == 'SELL'].copy()
    total_trades = int(len(sells))
    wins = int((sells['pnl_%'] > 0).sum()) if total_trades else 0
    win_rate = (wins / total_trades * 100.0) if total_trades else 0.0
    final_value = cash
    total_return_pct = (final_value - INITIAL_CASH) / INITIAL_CASH * 100.0

    summary = {
        "초기자본(원)": INITIAL_CASH,
        "최종자산(원)": round(final_value, 2),
        "총청산건수": total_trades,
        "승수": wins,
        "승률(%)": round(win_rate, 2),
        "총수익률(%)": round(total_return_pct, 2)
    }
    return log, summary

# ===================== 실행 =====================
def main():
    df = fetch_minutes(TICKER, INTERVAL, BACKTEST_DAYS)
    if df.empty:
        print("데이터 로딩 실패."); return
    df = add_indicators(df)
    log, summary = backtest(df)

    pd.set_option('display.width', None)
    pd.set_option('display.max_rows', 200)
    print("\n=== 거래 로그(최근 50건) ===")
    print(log.tail(50))
    print("\n=== 백테스트 요약 ===")
    for k, v in summary.items():
        print(f"{k}: {v}")

if __name__ == "__main__":
    main()
