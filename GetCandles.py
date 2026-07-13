# This is long position trading bot using Kite Connect API. It monitors the stock price in real-time, evaluates trading signals based on EMA and price action, and manages trades by setting entry, stop-loss, and target levels. It also logs trade details to a CSV file for record-keeping.
# Version 1.0
# changed the candle index from -2 to -1 for signal generation and stop loss reset.

import pandas as pd
import time as tme
import csv
import os
from datetime import datetime, timedelta, time
from kiteconnect import KiteTicker,KiteConnect



#========================================
#Logger
FILE = "historical_Data.csv"


def init_log():

	if os.path.exists(FILE):
		return

	with open(FILE, "w", newline="") as f:

		writer = csv.writer(f)

		writer.writerow([
			"Date",
			"Open",
			"High",
			"Low",
			"Close",
			"Volume",
		])



def log_trade(row):

	with open(FILE, "a", newline="") as f:

		writer = csv.writer(f)

		writer.writerow(row)

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


def get_candles(kite, token):
	to_date = datetime.now()
	#to_date = datetime(2026, 6, 5, 15, 0)  # For testing purposes
	from_date = to_date - timedelta(days=30)
	#from_date = datetime(2026, 6, 5, 9, 15)  # For testing purposes
	for attempt in range(3):
		try:
			data = kite.historical_data(
				token,
				from_date,
				to_date,
				TIMEFRAME
			)
			
			return pd.DataFrame(data)

			
				
		except Exception as e:
			print(f"Historical data attempt {attempt+1} failed: {e}")

			if attempt < 2:
				tme.sleep(2)
			else:
				raise
#======================================
#====================================

init_log()
# ======================
# CONFIG
# ======================

EMA_PERIOD = 5
MAX_QTY = 100
MAX_TRADES = 4
MAX_LOSS = -400
SYM = "HINDUNILVR"
EXCHANGE = "NSE"
TIMEFRAME = "minute" #for 1 minute it is "minute", for 5 minute it is "5minute".

MAX_LOSS_PER_TRADE = abs(MAX_LOSS) / MAX_TRADES

# ======================
# GLOBALS
# ======================

kite = get_kite()
token = get_token(kite)



api_key = kite.api_key
access_token = kite.access_token

df = get_candles(kite, token)

df = df.drop_duplicates(subset=["date"], keep="last")
df = df.sort_values("date")

df.to_csv(FILE, index=False)

