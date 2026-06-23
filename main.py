import pandas as pd
import threading
import time as tme
import csv
import os
from datetime import datetime, timedelta, time
from kiteconnect import KiteTicker,KiteConnect



#========================================
#Logger
FILE = "trades.csv"


def init_log():

	if os.path.exists(FILE):
		return

	with open(FILE, "w", newline="") as f:

		writer = csv.writer(f)

		writer.writerow([
			"time",
			"side",
			"entry",
			"stop",
			"target",
			"qty",
			"exit",
			"pnl"
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
	from_date = to_date - timedelta(days=1)
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
				time.sleep(2)
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

position = None

entry = None
stop = None
target = None
qty = None

live_pnl = 0

ltp = None
signal_time = None

state_lock = threading.Lock()

# ======================
# WEBSOCKET
# ======================

api_key = kite.api_key
access_token = kite.access_token

kws = KiteTicker(api_key, access_token)


def on_ticks(ws, ticks):
	global ltp

	if ticks:
		ltp = ticks[0]["last_price"]


def on_connect(ws, response):
	print("WebSocket Connected")

	ws.subscribe([token])

	ws.set_mode(
		ws.MODE_LTP,
		[token] 
	)


kws.on_ticks = on_ticks
kws.on_connect = on_connect


def start_ws():
	kws.connect(threaded=True)


# ======================
# TRADE MANAGEMENT
# ======================

def monitor_trade():
	global position
	global live_pnl
	global ltp
	global signal_time


	while True:
		if position == "BUY" and signal_time is not None:
			elapsed = (datetime.now() - signal_time).total_seconds()
			if elapsed > 50:  # 1 minute
				with state_lock:
					print(datetime.now(), "BUY TIMED OUT — price never broke above entry", round(entry, 2))
					position = None
					signal_time = None
					tme.sleep(0.1)
					continue

		if position not in ("BUY", "BUY_CONFIRMED"):
			tme.sleep(0.1)
			continue

		if ltp is None:
			tme.sleep(0.1)
			continue
		#print(ltp, stop)


		with state_lock:

			if position == "BUY" and ltp > entry:

				position = "BUY_CONFIRMED"

				print(
					"\n===================="
				)

				print(
					datetime.now(),
					"BUY CONFIRMED"
				)

				print(
					"Entry:",
					round(entry, 2)
				)

				print(
					"Stop:",
					round(stop, 2)
				)

				print(
					"Target:",
					round(target, 2)
				)

				print(
					"Qty:",
					qty
				)

				print(
					"====================\n"
				)

				log_trade([
					datetime.now(),
					"BUY CONFIRMED",
					"Entry:",
					round(entry, 2),
					"Stop",
					round(stop, 2),
					"Target:",
					round(target, 2),
						])


			if ltp <= stop and position == "BUY_CONFIRMED":
				#print(ltp, stop)


				exit_price = ltp

				pnl = (exit_price - entry) * qty

				live_pnl += pnl

				print("Exit:",
				round(exit_price, 2))

				print(
					datetime.now(),
					"STOP LOSS HIT",
					"Exit:",
					round(exit_price, 2),
					"Stop:",
					round(stop, 2),
					"PnL:",
					round(pnl, 2),
					"Total:",
					round(live_pnl, 2)
					)
				
				log_trade([
					datetime.now(),
					"STOP LOSS HIT",
					"Exit:",
					exit_price,
					"PnL:",
					round(pnl, 2),
					"Total:",
					round(live_pnl, 2)

						])
	
				position = None


			elif ltp >= target and position == "BUY_CONFIRMED":

				exit_price = ltp

				pnl = (exit_price - entry) * qty

				live_pnl += pnl

				print("Exit:",
					round(exit_price, 2))
				

				print(
					datetime.now(),
					"TARGET HIT",
					"Exit:",
					exit_price,
					"PnL:",
					round(pnl, 2),
					"Total:",
					round(live_pnl, 2)
				)
				
				
				log_trade([
					datetime.now(),
					"TARGET HIT",
					"Exit:",
					exit_price,
					"PnL:",
					round(pnl, 2),
					"Total:",
					round(live_pnl, 2)
					])
				

				position = None

			tme.sleep(0.1)


#=======================
#RESET STOP LOSS
#=======================
def reset_stop():
	global stop

	try:

		to_date = datetime.now()
		from_date = to_date - timedelta(days=1)

		data = kite.historical_data(
			token,
			from_date,
			to_date,
			"minute"
		)

		if not data:
			return

		df = pd.DataFrame(data)

		signal_low_stop = df["low"].iloc[-1]

		with state_lock:

			if (
				position == "BUY_CONFIRMED"
				and signal_low_stop > entry
				and stop < entry
			):

				stop = entry

				print(
					datetime.now(),
					"STOP MOVED TO BREAKEVEN",
					round(stop, 2)
				)

	except Exception as e:

		print(
			datetime.now(),
			"RESET STOP ERROR:",
			e
		)

# ======================
# SIGNAL GENERATION
# ======================

def evaluate_signal():
	global position
	global entry
	global stop
	global target
	global qty
	global signal_time
	

	to_date = datetime.now()

	from_date = to_date - timedelta(days=1)

	data = kite.historical_data(

		token,
		from_date,
		to_date,
		"minute"
	)

	if not data:
		return

	df = pd.DataFrame(data)

	if len(df) < EMA_PERIOD + 2:
		return

	df["EMA"] = (
		df["close"]
		.ewm(
			span=EMA_PERIOD,
			adjust=False
		)
		.mean()
	)

	signal_high = df["high"].iloc[-1]
	signal_low = df["low"].iloc[-1]
	signal_ema = df["EMA"].iloc[-1]


	'''print(
		datetime.now(),
		"signal_high:",
		signal_high,
		"signal_ema:",
		round(signal_ema, 2),
	)'''

	if position is not None:
		return
	

	if signal_high < signal_ema and position == None:

		entry_price = signal_high
		stop_price = signal_low

		risk = entry_price - stop_price

		if risk <= 0:
			return

		elif risk >0 and risk <=0.5:
			stop_price = stop_price-(2-risk)
			risk = entry_price - stop_price


		trade_qty = int(MAX_LOSS_PER_TRADE / risk)


		if trade_qty <= 0:
			return

		trade_target = entry_price + (risk * 5)

		with state_lock:

			entry = entry_price
			stop = stop_price
			target = trade_target
			qty = trade_qty

			signal_time = datetime.now()
			
			position = "BUY"
			print('BUY SIGNAL','entry: ',entry,'stop: ',stop,'target:',target,'qty:',qty)

			

		
			


# ======================
# STARTUP
# ======================
try:
	print(kite.profile())
	print("REST API OK")
	
except Exception as e:
	print("REST API FAILED:", e)


start_ws()


timeout = 30
start = tme.time()

while ltp is None:

	if tme.time() - start > timeout:
		print("No ticks received.")
		break

	print("Waiting for first tick...")
	tme.sleep(1)

threading.Thread(
	target=monitor_trade,
	daemon=True
).start()


# ======================
# MAIN LOOP
# ======================

while True:

	current_time = datetime.now().time()

	if current_time < time(9, 30):
		tme.sleep(30)
		continue

	if current_time > time(15, 15):

		print(
			datetime.now(),
			"Market Closed"
		)
		kws.close()
		
		break

	if live_pnl <= MAX_LOSS:

		print(
			datetime.now(),
			"Max Loss Reached"
		)

		break

	if position == "BUY_CONFIRMED":
		reset_stop()

	if position is None:
		evaluate_signal()
		print('Signal search started at: ',current_time)

	current_second = datetime.now().second

	sleep_time = 60 - current_second


	tme.sleep(sleep_time) 