#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pyupbit
import pandas as pd
import numpy as np
import time
import logging
from datetime import datetime

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('upbit_autotrade.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Upbit API keys (recommended to use environment variables or separate config file)
ACCESS_KEY = "your_access_key"
API_SECRET = "your_secret_key"

# Global settings
DRY_RUN = True  # Set to True for paper trading (simulation), False for real trading
INITIAL_BALANCE = 1000000  # Initial virtual balance for dry run (1 million KRW)
EXCLUDED_COINS = ['KRW-XRP', 'KRW-BTC', 'KRW-ETH', 'KRW-USDT']  # Coins to exclude
MIN_VOLUME = 20000000000  # Minimum trading volume: 20 billion KRW
TOP_GAINERS_COUNT = 20  # Top N gainers
MONITOR_INTERVAL = 300  # Monitoring interval in seconds (5 minutes)
INVESTMENT_PER_COIN = 0.95  # Investment ratio per coin (95% of balance)


class UpbitAutoTrader:
    def __init__(self, access_key, secret_key, dry_run=True):
        """Initialize Upbit auto-trading bot"""
        self.dry_run = dry_run
        self.upbit = pyupbit.Upbit(access_key, secret_key) if not dry_run else None
        self.target_coins = []  # Current target coins list

        # Virtual portfolio for dry run mode
        if self.dry_run:
            self.virtual_krw_balance = INITIAL_BALANCE
            self.virtual_portfolio = {}  # {ticker: amount}
            self.trade_history = []  # Track all trades
            logger.info(f"*** DRY RUN MODE ENABLED - NO REAL TRADES WILL BE EXECUTED ***")
            logger.info(f"Initial virtual balance: {self.virtual_krw_balance:,.0f} KRW")

    def get_top_gainers(self):
        """Get top gainers by daily change rate with volume filter"""
        try:
            # Get all KRW market tickers
            tickers = pyupbit.get_tickers(fiat="KRW")

            # Filter out excluded coins
            tickers = [t for t in tickers if t not in EXCLUDED_COINS]

            market_data = []

            for ticker in tickers:
                try:
                    # Get current price
                    current = pyupbit.get_current_price(ticker)
                    if current is None:
                        continue

                    # Get daily OHLCV data (last 2 days)
                    df = pyupbit.get_ohlcv(ticker, interval="day", count=2)
                    if df is None or len(df) < 2:
                        continue

                    # Previous day close
                    prev_close = df.iloc[-2]['close']
                    # Current price
                    current_price = current

                    # Calculate change rate
                    change_rate = ((current_price - prev_close) / prev_close) * 100

                    # Get trading volume in KRW
                    volume_krw = df.iloc[-1]['value']  # Today's trading volume

                    # Filter by minimum volume (20 billion KRW)
                    if volume_krw >= MIN_VOLUME:
                        market_data.append({
                            'ticker': ticker,
                            'change_rate': change_rate,
                            'volume_krw': volume_krw,
                            'current_price': current_price
                        })

                    time.sleep(0.1)  # Avoid API rate limit

                except Exception as e:
                    logger.warning(f"Failed to get data for {ticker}: {e}")
                    continue

            # Sort by change rate and select top N
            market_data.sort(key=lambda x: x['change_rate'], reverse=True)
            top_coins = market_data[:TOP_GAINERS_COUNT]

            logger.info(f"Top {TOP_GAINERS_COUNT} gainers with volume >= {MIN_VOLUME/100000000:.0f} billion KRW:")
            for coin in top_coins:
                logger.info(f"  {coin['ticker']}: {coin['change_rate']:.2f}%, "
                          f"Volume: {coin['volume_krw']/100000000:.0f} billion KRW")

            return [coin['ticker'] for coin in top_coins]

        except Exception as e:
            logger.error(f"Error in get_top_gainers: {e}")
            return []

    def calculate_sma(self, df, period):
        """Calculate Simple Moving Average"""
        return df['close'].rolling(window=period).mean()

    def calculate_adx(self, df, period=14):
        """Calculate ADX, DI+, DI-"""
        high = df['high']
        low = df['low']
        close = df['close']

        # Calculate +DM, -DM
        high_diff = high.diff()
        low_diff = -low.diff()

        plus_dm = high_diff.copy()
        minus_dm = low_diff.copy()

        plus_dm[((high_diff < low_diff) | (high_diff < 0))] = 0
        minus_dm[((low_diff < high_diff) | (low_diff < 0))] = 0

        # Calculate TR (True Range)
        tr1 = high - low
        tr2 = abs(high - close.shift())
        tr3 = abs(low - close.shift())
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        # Calculate ATR
        atr = tr.rolling(window=period).mean()

        # Calculate +DI, -DI
        plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)

        # Calculate DX
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di)

        # Calculate ADX
        adx = dx.rolling(window=period).mean()

        return adx, plus_di, minus_di

    def check_buy_condition(self, ticker):
        """
        Check buy conditions:
        - SMA5 > SMA10 > SMA20
        - SMA40 > SMA40[1]
        - DI+ > ADX > DI-
        """
        try:
            # Get sufficient data for ADX calculation
            df = pyupbit.get_ohlcv(ticker, interval="minute5", count=200)

            if df is None or len(df) < 50:
                return False

            # Calculate SMAs
            df['sma5'] = self.calculate_sma(df, 5)
            df['sma10'] = self.calculate_sma(df, 10)
            df['sma20'] = self.calculate_sma(df, 20)
            df['sma40'] = self.calculate_sma(df, 40)

            # Calculate ADX, DI+, DI-
            df['adx'], df['plus_di'], df['minus_di'] = self.calculate_adx(df)

            # Get latest data
            latest = df.iloc[-1]
            prev_sma40 = df.iloc[-2]['sma40']

            # Check conditions
            condition1 = latest['sma5'] > latest['sma10'] > latest['sma20']
            condition2 = latest['sma40'] > prev_sma40
            condition3 = latest['plus_di'] > latest['adx'] > latest['minus_di']

            if pd.isna(condition1) or pd.isna(condition2) or pd.isna(condition3):
                return False

            result = condition1 and condition2 and condition3

            if result:
                logger.info(f"Buy signal for {ticker}: SMA5={latest['sma5']:.2f}, "
                          f"SMA10={latest['sma10']:.2f}, SMA20={latest['sma20']:.2f}, "
                          f"SMA40={latest['sma40']:.2f}, SMA40[1]={prev_sma40:.2f}, "
                          f"DI+={latest['plus_di']:.2f}, ADX={latest['adx']:.2f}, "
                          f"DI-={latest['minus_di']:.2f}")

            return result

        except Exception as e:
            logger.error(f"Error checking buy condition for {ticker}: {e}")
            return False

    def check_sell_condition(self, ticker):
        """
        Check sell conditions:
        - ADX < ADX[1] and ADX[1] < ADX[2] and ADX[2] < ADX[3]
        (ADX declining for 3 consecutive candles)
        """
        try:
            df = pyupbit.get_ohlcv(ticker, interval="minute5", count=200)

            if df is None or len(df) < 50:
                return False

            # Calculate ADX
            df['adx'], _, _ = self.calculate_adx(df)

            # Get last 4 ADX values
            adx_0 = df.iloc[-1]['adx']  # Current
            adx_1 = df.iloc[-2]['adx']  # 1 candle ago
            adx_2 = df.iloc[-3]['adx']  # 2 candles ago
            adx_3 = df.iloc[-4]['adx']  # 3 candles ago

            if pd.isna(adx_0) or pd.isna(adx_1) or pd.isna(adx_2) or pd.isna(adx_3):
                return False

            result = (adx_0 < adx_1) and (adx_1 < adx_2) and (adx_2 < adx_3)

            if result:
                logger.info(f"Sell signal for {ticker}: ADX={adx_0:.2f}, "
                          f"ADX[1]={adx_1:.2f}, ADX[2]={adx_2:.2f}, ADX[3]={adx_3:.2f}")

            return result

        except Exception as e:
            logger.error(f"Error checking sell condition for {ticker}: {e}")
            return False

    def get_balance(self, ticker=None):
        """Get balance (virtual or real depending on mode)"""
        try:
            if self.dry_run:
                # Virtual balance for dry run
                if ticker is None:
                    return self.virtual_krw_balance
                else:
                    return self.virtual_portfolio.get(ticker, 0)
            else:
                # Real balance
                if ticker is None:
                    return self.upbit.get_balance("KRW")
                else:
                    coin = ticker.split('-')[1]
                    return self.upbit.get_balance(coin)
        except Exception as e:
            logger.error(f"Error getting balance for {ticker}: {e}")
            return 0

    def buy_coin(self, ticker):
        """Buy coin (virtual or real depending on mode)"""
        try:
            krw_balance = self.get_balance()

            if krw_balance < 5000:  # Minimum order amount
                logger.warning(f"Insufficient KRW balance: {krw_balance}")
                return False

            # Check current holdings
            holdings = [t for t in self.target_coins if self.get_balance(t) > 0]

            # Calculate buy amount for diversification
            buy_amount = krw_balance * INVESTMENT_PER_COIN

            if buy_amount < 5000:
                logger.warning(f"Buy amount too small: {buy_amount}")
                return False

            # Get current price
            current_price = pyupbit.get_current_price(ticker)
            if current_price is None:
                logger.warning(f"Cannot get price for {ticker}")
                return False

            if self.dry_run:
                # Virtual buy for dry run
                coin_amount = buy_amount / current_price
                self.virtual_krw_balance -= buy_amount
                self.virtual_portfolio[ticker] = self.virtual_portfolio.get(ticker, 0) + coin_amount

                # Record trade
                trade = {
                    'time': datetime.now(),
                    'type': 'BUY',
                    'ticker': ticker,
                    'amount': coin_amount,
                    'price': current_price,
                    'value': buy_amount
                }
                self.trade_history.append(trade)

                logger.info(f"[DRY RUN] BUY SUCCESS: {ticker}, "
                          f"Amount: {coin_amount:.4f} coins, "
                          f"Price: {current_price:,.0f} KRW, "
                          f"Total: {buy_amount:.0f} KRW")
                return True
            else:
                # Real buy
                result = self.upbit.buy_market_order(ticker, buy_amount)

                if result:
                    logger.info(f"BUY SUCCESS: {ticker}, Amount: {buy_amount:.0f} KRW")
                    return True
                else:
                    logger.warning(f"BUY FAILED: {ticker}")
                    return False

        except Exception as e:
            logger.error(f"Error buying {ticker}: {e}")
            return False

    def sell_coin(self, ticker):
        """Sell coin (virtual or real depending on mode)"""
        try:
            balance = self.get_balance(ticker)

            if balance <= 0:
                return False

            current_price = pyupbit.get_current_price(ticker)
            if current_price is None:
                return False

            sell_value = balance * current_price

            if self.dry_run:
                # Virtual sell for dry run
                self.virtual_krw_balance += sell_value

                # Record trade before removing from portfolio
                trade = {
                    'time': datetime.now(),
                    'type': 'SELL',
                    'ticker': ticker,
                    'amount': balance,
                    'price': current_price,
                    'value': sell_value
                }
                self.trade_history.append(trade)

                # Remove from portfolio
                if ticker in self.virtual_portfolio:
                    del self.virtual_portfolio[ticker]

                logger.info(f"[DRY RUN] SELL SUCCESS: {ticker}, "
                          f"Amount: {balance:.4f} coins, "
                          f"Price: {current_price:,.0f} KRW, "
                          f"Total: {sell_value:.0f} KRW")
                return True
            else:
                # Real sell
                result = self.upbit.sell_market_order(ticker, balance)

                if result:
                    logger.info(f"SELL SUCCESS: {ticker}, Amount: {balance}, "
                              f"Value: {sell_value:.0f} KRW")
                    return True
                else:
                    logger.warning(f"SELL FAILED: {ticker}")
                    return False

        except Exception as e:
            logger.error(f"Error selling {ticker}: {e}")
            return False

    def update_target_coins(self):
        """Update target coins list"""
        new_targets = self.get_top_gainers()

        # Sell coins that are no longer in target list
        for ticker in self.target_coins:
            if ticker not in new_targets:
                balance = self.get_balance(ticker)
                if balance > 0:
                    logger.info(f"{ticker} removed from target list. Selling...")
                    self.sell_coin(ticker)

        self.target_coins = new_targets
        logger.info(f"Updated target coins: {len(self.target_coins)} coins")

    def monitor_and_trade(self):
        """Monitor and execute trades"""
        logger.info("=== Monitoring and Trading ===")

        # Check holdings and sell/buy conditions
        for ticker in self.target_coins:
            balance = self.get_balance(ticker)

            if balance > 0:
                # Holding coin - check sell condition
                if self.check_sell_condition(ticker):
                    logger.info(f"Sell condition met for {ticker}")
                    self.sell_coin(ticker)
            else:
                # Not holding - check buy condition
                if self.check_buy_condition(ticker):
                    logger.info(f"Buy condition met for {ticker}")
                    self.buy_coin(ticker)

        # Print current portfolio
        self.print_portfolio()

    def print_portfolio(self):
        """Print current portfolio"""
        logger.info("=== Current Portfolio ===")
        krw_balance = self.get_balance()
        logger.info(f"KRW Balance: {krw_balance:,.0f} KRW")

        total_value = krw_balance

        for ticker in self.target_coins:
            balance = self.get_balance(ticker)
            if balance > 0:
                current_price = pyupbit.get_current_price(ticker)
                if current_price:
                    value = balance * current_price
                    total_value += value
                    logger.info(f"  {ticker}: {balance:.4f} coins, "
                              f"Value: {value:,.0f} KRW")

        logger.info(f"Total Portfolio Value: {total_value:,.0f} KRW")

        # Show P&L for dry run mode
        if self.dry_run:
            pnl = total_value - INITIAL_BALANCE
            pnl_percent = (pnl / INITIAL_BALANCE) * 100
            logger.info(f"P&L: {pnl:+,.0f} KRW ({pnl_percent:+.2f}%)")
            logger.info(f"Total Trades: {len(self.trade_history)}")

        logger.info("=" * 50)

    def print_trade_summary(self):
        """Print trade history summary (for dry run mode)"""
        if not self.dry_run or len(self.trade_history) == 0:
            return

        logger.info("\n" + "=" * 60)
        logger.info("=== TRADING SUMMARY ===")
        logger.info("=" * 60)

        buy_count = sum(1 for t in self.trade_history if t['type'] == 'BUY')
        sell_count = sum(1 for t in self.trade_history if t['type'] == 'SELL')

        logger.info(f"Total Trades: {len(self.trade_history)} (Buy: {buy_count}, Sell: {sell_count})")
        logger.info(f"\nRecent Trades:")

        # Show last 10 trades
        for trade in self.trade_history[-10:]:
            logger.info(f"  {trade['time'].strftime('%Y-%m-%d %H:%M:%S')} - "
                      f"{trade['type']:4s} {trade['ticker']:12s} "
                      f"{trade['amount']:.4f} @ {trade['price']:,.0f} = "
                      f"{trade['value']:,.0f} KRW")

        # Calculate final P&L
        krw_balance = self.get_balance()
        total_value = krw_balance

        for ticker in self.virtual_portfolio:
            balance = self.virtual_portfolio[ticker]
            if balance > 0:
                current_price = pyupbit.get_current_price(ticker)
                if current_price:
                    total_value += balance * current_price

        final_pnl = total_value - INITIAL_BALANCE
        final_pnl_percent = (final_pnl / INITIAL_BALANCE) * 100

        logger.info(f"\n=== FINAL RESULTS ===")
        logger.info(f"Initial Balance: {INITIAL_BALANCE:,.0f} KRW")
        logger.info(f"Final Value: {total_value:,.0f} KRW")
        logger.info(f"Total P&L: {final_pnl:+,.0f} KRW ({final_pnl_percent:+.2f}%)")
        logger.info("=" * 60)

    def run(self):
        """Main execution loop"""
        logger.info("=== Upbit Auto Trading Bot Started ===")
        logger.info(f"Mode: {'DRY RUN (Paper Trading)' if self.dry_run else 'LIVE TRADING'}")
        logger.info(f"Monitor Interval: {MONITOR_INTERVAL} seconds")
        logger.info(f"Min Volume: {MIN_VOLUME/100000000:.0f} billion KRW")
        logger.info(f"Top Gainers: {TOP_GAINERS_COUNT}")

        iteration = 0

        while True:
            try:
                iteration += 1
                logger.info(f"\n{'='*60}")
                logger.info(f"Iteration #{iteration} - {datetime.now()}")
                logger.info(f"{'='*60}")

                # Update target coins list
                self.update_target_coins()

                # Monitor and trade
                self.monitor_and_trade()

                # Wait
                logger.info(f"Waiting {MONITOR_INTERVAL} seconds...\n")
                time.sleep(MONITOR_INTERVAL)

            except KeyboardInterrupt:
                logger.info("\nBot stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                logger.info(f"Waiting {MONITOR_INTERVAL} seconds before retry...")
                time.sleep(MONITOR_INTERVAL)

        # Print final summary when bot stops
        self.print_trade_summary()


def main():
    """Main function"""
    # Check API keys (not required for dry run mode)
    if not DRY_RUN:
        if ACCESS_KEY == "your_access_key" or API_SECRET == "your_secret_key":
            logger.error("Please set your Upbit API keys in the script!")
            logger.error("You can get API keys from: https://upbit.com/mypage/open_api_management")
            return
    else:
        logger.info("Running in DRY RUN mode - API keys not required")

    # Run auto-trading bot
    trader = UpbitAutoTrader(ACCESS_KEY, API_SECRET, dry_run=DRY_RUN)
    trader.run()


if __name__ == "__main__":
    main()
