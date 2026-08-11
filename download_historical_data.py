# This is short position trading bot using Kite Connect API. It monitors the stock price in real-time, evaluates trading signals based on EMA and price action, and manages trades by setting entry, stop-loss, and target levels. It also logs trade details to a CSV file for record-keeping.
# version 1.0 
# fixed bug of trade_target value calculation.
# changed the candle index from -2 to -1 for signal generation and stop loss reset.
# this is beta version with live trading activated. Please use it with caution and at your own risk. The author is not responsible for any financial loss incurred while using this bot.
# version 1.1
# added 3 consecutive candle condition for break even update for live trading script
# version 1.2   date: 24-July-2026
# made TIMEFRAME fully configurable — fixed hardcoded "minute" calls in reset_short_stop()
# and evaluate_short_signal(), derived CONFIRM_TIMEOUT and main-loop wake-up from the
# actual candle duration instead of assuming 60 seconds.
# version 1.3   date: 31-July-2026
# replaced the fixed "3 consecutive candles below entry" breakeven rule with a
# Breakeven Track Candle (BTC) model: reset_short_stop() -> update_breakeven_track_candle()
# tracks/confirms the BTC once per closed candle (main loop); once confirmed, monitor_trade()
# watches live LTP and moves stop to breakeven the instant LTP breaks the confirmed BTC's low.

import pandas as pd
import threading
import time as tme
import csv
import os
import re
from datetime import datetime, timedelta, time
from kiteconnect import KiteTicker,KiteConnect
import sys



#========================================
#Logger
FILE = "HistoricalData_2min_today.csv"


def load_creds():
	creds = {}

	with open("cred.inf", "r") as f:
		for line in f:
			line = line.strip()

			if "=" in line:
				k, v = line.split("=", 1)
				creds[k.strip()] = v.strip()

	return creds
#============================================
#==============================================
# Data
def get_kite():
	creds = load_creds()

	kite = KiteConnect(api_key=creds["API_KEY"])
	kite.timeout = 20
	token_path = os.path.join(os.path.dirname(__file__), "access_token.txt")

	with open(token_path, "r") as f:
		access_token = f.read().strip()

	kite.set_access_token(access_token)

	return kite


def get_token(kite):
	instruments = kite.instruments("NSE")

	for ins in instruments:
		if ins["tradingsymbol"] == SYM:
			return ins["instrument_token"]

	raise Exception("Instrument not found")



to_date = datetime.now()
#to_date = datetime(2026, 6, 7, 15, 0)  # For testing purposes
from_date = to_date - timedelta(days=1)
#from_date = datetime(2026, 6, 6, 9, 15)  # For testing purposes



SYM = "HINDUNILVR"
EXCHANGE = "NSE"
TIMEFRAME = "2minute" #for 1 minute it is "minute", for 5 minute it is "5minute", 15minute, 30minute, 60minute, etc.


kite = get_kite()
token = get_token(kite)

api_key = kite.api_key
access_token = kite.access_token


data = kite.historical_data(
			token,
			from_date,
			to_date,
			TIMEFRAME
		)

df = pd.DataFrame(data)

print(df.head())
df.to_csv(FILE, index=False)
