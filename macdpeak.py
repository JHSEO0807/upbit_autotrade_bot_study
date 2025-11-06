import math
import numpy as np
import pandas as pd
import pyupbit
from ta.trend import MACD
from datetime import datetime

# ===================== 사용자 설정 =====================
TICKER               = "KRW-SOL"        # 종목
INTERVAL             = "minute5"        # 분봉
COUNT                = 2000             # 캔들 수(최대 2000 권장)
INITIAL_CASH         = 1_000_000        # 초기자본(원)

# 수수료/슬리피지 (왕복 합계 대략 0.1% 가정: 체결당 0.05% + 슬리피지 0.05%)
FEE_RATE             = 0.0005           # 매수/매도 각각 수수료
SLIPPAGE             = 0.0005           # 매수/매도 각각 슬리피지

# MACD 파라미터
MACD_FAST            = 12
MACD_SLOW            = 26
MACD_SIGNAL          = 9

# 피크(고점) 판정/매도 트리거
MIN_PEAK_HIST        = 0.0              # 피크로 인정할 히스토그램 최소값(노이즈 제거)
PEAK_DROP_PCT        = 0.35             # 피크 대비 히스토그램 하락 비율(예: 0.35 = 35%)
CONSEC_DOWN_BARS     = 2                # 히스토그램 연속 하락 봉 수

# 진입 조건(예시): MACD 골든크로스 + 히스토그램 최근 n→1봉 상승
REQUIRE_HIST_RISING  = True
RISING_LOOKBACK      = 2
# =======================================================

def load_ohlcv(ticker, interval, count):
    df = pyupbit.get_ohlcv(ticker=ticker, interval=interval, count=count)
    if df is None or df.empty:
        raise RuntimeError("OHLCV 데이터를 가져오지 못했습니다.")
    return df

def add_macd(df):
    macd = MACD(close=df['close'], window_slow=MACD_SLOW, window_fast=MACD_FAST, window_sign=MACD_SIGNAL)
    df['macd']   = macd.macd()
    df['signal'] = macd.macd_signal()
    df['hist']   = macd.macd_diff()
    return df

def find_hist_peaks(df, min_peak_hist=0.0):
    """로컬 최대값: hist[t-1] < hist[t] > hist[t+1] AND hist[t] >= min_peak_hist"""
    hist = df['hist'].values
    peaks = np.zeros(len(df), dtype=bool)
    for i in range(1, len(df)-1):
        if hist[i-1] < hist[i] > hist[i+1] and hist[i] >= min_peak_hist:
            peaks[i] = True
    df['hist_peak'] = peaks
    return df

def is_macd_golden_cross(df, i):
    if i == 0: 
        return False
    prev = df.iloc[i-1]
    curr = df.iloc[i]
    return (prev['macd'] <= prev['signal']) and (curr['macd'] > curr['signal'])

def is_macd_dead_cross(df, i):
    if i == 0:
        return False
    prev = df.iloc[i-1]
    curr = df.iloc[i]
    return (prev['macd'] >= prev['signal']) and (curr['macd'] < curr['signal'])

def hist_recent_rising(df, i, lookback=2):
    if i - lookback < 0:
        return False
    seg = df['hist'].iloc[i-lookback:i+1].values
    return all(seg[j] < seg[j+1] for j in range(len(seg)-1))

def apply_slippage(price, side):
    if side == "buy":
        return price * (1 + SLIPPAGE)
    else:
        return price * (1 - SLIPPAGE)

def backtest(df):
    cash = INITIAL_CASH
    qty = 0.0
    in_pos = False
    entry_idx = None
    entry_price = None

    # 피크 추적(보유 중에만 의미)
    best_hist = -np.inf
    best_hist_time = None
    consec_down = 0

    trades = []

    for i in range(len(df)):
        row = df.iloc[i]
        price = row['close']
        hist  = row['hist']

        # 진입
        if not in_pos:
            cross_ok = is_macd_golden_cross(df, i)
            rise_ok  = (not REQUIRE_HIST_RISING) or hist_recent_rising(df, i, RISING_LOOKBACK)
            if cross_ok and rise_ok:
                adj_price = apply_slippage(price, "buy")
                if adj_price > 0 and cash > 0:
                    qty = (cash * (1 - FEE_RATE)) / adj_price
                    cash = 0.0
                    in_pos = True
                    entry_idx = i
                    entry_price = adj_price

                    # 피크 초기화
                    best_hist = hist
                    best_hist_time = df.index[i]  # 진입 시점의 hist를 초기 피크로
                    consec_down = 0
                    print(f"[진입] {df.index[i]} | 가격={adj_price:.6f} | hist={hist:.6f}")
        else:
            # 보유 중 피크 갱신
            if hist > best_hist:
                best_hist = hist
                best_hist_time = df.index[i]
                consec_down = 0
            else:
                consec_down = consec_down + 1 if hist < df['hist'].iloc[i-1] else 0

            # 매도 트리거
            drop_ok = False
            if best_hist > 0:
                drop_ok = (best_hist - hist) / best_hist >= PEAK_DROP_PCT

            down_ok = consec_down >= CONSEC_DOWN_BARS
            dc_ok = is_macd_dead_cross(df, i)

            if drop_ok or down_ok or dc_ok:
                adj_price = apply_slippage(price, "sell")
                proceeds = qty * adj_price * (1 - FEE_RATE)
                trade_ret = (adj_price - entry_price) / entry_price

                # 콘솔 출력: 고점 시간대 & 매도 시간대
                print(f"[피크] {best_hist_time} | hist_peak={best_hist:.6f}")
                print(f"[청산] {df.index[i]} | 이유={'PEAK_DROP' if drop_ok else ('CONSEC_DOWN' if down_ok else 'DEAD_CROSS')} | 가격={adj_price:.6f} | 수익률={trade_ret*100:.3f}%")
                print("-"*80)

                cash = proceeds
                trades.append({
                    "entry_time": df.index[entry_idx],
                    "peak_time": best_hist_time,
                    "exit_time": df.index[i],
                    "entry_price": entry_price,
                    "exit_price": adj_price,
                    "return_pct": trade_ret * 100.0,
                    "peak_hist": best_hist,
                    "hist_at_exit": hist,
                    "exit_reason": "PEAK_DROP" if drop_ok else ("CONSEC_DOWN" if down_ok else "DEAD_CROSS")
                })

                # 상태 초기화
                qty = 0.0
                in_pos = False
                entry_idx = None
                entry_price = None
                best_hist = -np.inf
                best_hist_time = None
                consec_down = 0

    # 마지막 봉 강제 청산(선택)
    if in_pos:
        last = df.iloc[-1]
        adj_price = apply_slippage(last['close'], "sell")
        proceeds = qty * adj_price * (1 - FEE_RATE)
        trade_ret = (adj_price - entry_price) / entry_price
        print(f"[피크] {best_hist_time} | hist_peak={best_hist:.6f}")
        print(f"[청산] {df.index[-1]} | 이유=FORCE_EXIT_LAST_BAR | 가격={adj_price:.6f} | 수익률={trade_ret*100:.3f}%")
        print("-"*80)

        cash = proceeds
        trades.append({
            "entry_time": df.index[entry_idx],
            "peak_time": best_hist_time,
            "exit_time": df.index[-1],
            "entry_price": entry_price,
            "exit_price": adj_price,
            "return_pct": trade_ret * 100.0,
            "peak_hist": best_hist,
            "hist_at_exit": last['hist'],
            "exit_reason": "FORCE_EXIT_LAST_BAR"
        })

    # 성과 요약(복리)
    ending_equity = cash
    for t in trades:
        # 이미 현금 기준으로 갱신했지만, 요약은 복리 시뮬레이션으로 표준화
        pass
    if len(trades) > 0:
        equity = INITIAL_CASH
        for t in trades:
            equity *= (1 + t["return_pct"]/100.0)
        ending_equity = equity

    total_return_pct = (ending_equity / INITIAL_CASH - 1) * 100.0

    # 콘솔 요약
    print("\n=== 설정 요약 ===")
    print(f"TICKER={TICKER}, INTERVAL={INTERVAL}, COUNT={COUNT}")
    print(f"MACD=({MACD_FAST},{MACD_SLOW},{MACD_SIGNAL}), MIN_PEAK_HIST={MIN_PEAK_HIST}")
    print(f"PEAK_DROP_PCT={PEAK_DROP_PCT}, CONSEC_DOWN_BARS={CONSEC_DOWN_BARS}")
    print(f"수수료={FEE_RATE*100:.3f}%/체결, 슬리피지={SLIPPAGE*100:.3f}%/체결")

    print("\n=== 결과 요약 ===")
    print(f"거래횟수: {len(trades)}")
    if len(trades) > 0:
        win = sum(1 for t in trades if t["return_pct"] > 0)
        winrate = win / len(trades) * 100.0
        avg_ret = sum(t["return_pct"] for t in trades) / len(trades)
        print(f"승률: {win}/{len(trades)} = {winrate:.2f}%")
        print(f"평균 거래수익률: {avg_ret:.3f}%")
        print(f"총 수익률(복리): {total_return_pct:.2f}%  | 최종자산: {math.floor(ending_equity):,}원")
    else:
        print("거래가 발생하지 않았습니다. 파라미터를 조정해보세요.")

def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 데이터 수집 중... {TICKER} {INTERVAL} {COUNT}개")
    df = load_ohlcv(TICKER, INTERVAL, COUNT)
    df = add_macd(df)
    df = find_hist_peaks(df, MIN_PEAK_HIST)
    backtest(df)

if __name__ == "__main__":
    main()
