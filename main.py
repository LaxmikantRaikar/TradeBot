import pandas as pd
import threading
import time as tme
import csv
import os
from datetime import datetime, timedelta, time
from kiteconnect import KiteTicker,KiteConnect
import sys



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

position = None

entry = None
stop = None
target = None
qty = None
signal_high = None

live_pnl = 0

ltp = None
signal_time = None
loss_trades = 0
breakeven_reached = False

state_lock = threading.Lock()

# Protects latest market price
price_lock = threading.Lock()

candles = []              # Closed candles
current_candle = None     # Candle currently forming
last_minute = None
candle_lock = threading.Lock()

# ======================
# WEBSOCKET
# ======================

api_key = kite.api_key
access_token = kite.access_token

kws = KiteTicker(api_key, access_token)


def on_ticks(ws, ticks):

	global ltp
	global current_candle
	global last_minute
	global candles

	if not ticks:
		return

	tick = ticks[0]

	# Latest traded price
	with price_lock:
		ltp = tick["last_price"]

	# Exchange timestamp
	ts = tick.get("exchange_timestamp")

	if ts is None:
		return

	minute = ts.replace(second=0, microsecond=0)

	with candle_lock:

		# -----------------------------------
		# First tick after program starts
		# -----------------------------------
		if current_candle is None:

			current_candle = {
				"date": minute,
				"open": ltp,
				"high": ltp,
				"low": ltp,
				"close": ltp
			}

			last_minute = minute
			return

		# -----------------------------------
		# Same candle
		# -----------------------------------
		if minute == last_minute:

			current_candle["high"] = max(
				current_candle["high"],
				ltp
			)

			current_candle["low"] = min(
				current_candle["low"],
				ltp
			)

			current_candle["close"] = ltp

			return

		# -----------------------------------
		# New minute started
		# Previous candle is complete
		# -----------------------------------

		completed_candle = current_candle
		candles.append(completed_candle)

		# Keep last 500 candles
		if len(candles) > 500:
			candles.pop(0)

		print(
			datetime.now(),
			"CANDLE CLOSED:",
			completed_candle["date"],
			"O:", completed_candle["open"],
			"H:", completed_candle["high"],
			"L:", completed_candle["low"],
			"C:", completed_candle["close"]
		)

		# Create new candle
		current_candle = {
			"date": minute,
			"open": ltp,
			"high": ltp,
			"low": ltp,
			"close": ltp
		}

		last_minute = minute

	# Evaluate signal AFTER releasing the lock
	if position is None:
		evaluate_signal()


def on_connect(ws, response):
	print("WebSocket Connected")

	ws.subscribe([token])

	ws.set_mode(
		ws.MODE_FULL,
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
	global signal_time
	global loss_trades
	global breakeven_reached

	while True:
		
		with price_lock:
			current_ltp = ltp

		# Wait for first tick
		if current_ltp is None:
			tme.sleep(0.05)
			continue

		# -----------------------------
		# Copy shared variables safely
		# -----------------------------
		with state_lock:

			local_position = position
			local_entry = entry
			local_stop = stop
			local_target = target
			local_qty = qty
			local_signal_time = signal_time
			local_signal_high = signal_high
			local_breakeven = breakeven_reached
			

		# -----------------------------
		# No trade running
		# -----------------------------
		if local_position not in ("BUY", "BUY_CONFIRMED"):
			tme.sleep(0.05)
			continue

		# -----------------------------
		# BUY timeout
		# -----------------------------
		if local_position == "BUY":

			elapsed = (
				datetime.now() - local_signal_time
			).total_seconds()

			if elapsed > 60:

				with state_lock:

					if position == "BUY":

						print(
							datetime.now(),
							"BUY TIMED OUT"
						)

						position = None
						signal_time = None

				continue

			# Breakout confirmation
			if current_ltp > local_signal_high:

				with state_lock:

					if position == "BUY":

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
							round(entry,2)
						)

						print(
							"Stop:",
							round(stop,2)
						)

						print(
							"Target:",
							round(target,2)
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
							round(entry,2),
							round(stop,2),
							round(target,2),
							qty
						])

				continue

		# -----------------------------
		# Stop Loss
		# -----------------------------
		if local_position == "BUY_CONFIRMED":

			if current_ltp <= local_stop:

				pnl = (current_ltp - local_entry) * local_qty

				with state_lock:

					live_pnl += pnl

					if not local_breakeven:
						loss_trades += 1

					position = None
					signal_time = None
					breakeven_reached = False

				print(
					datetime.now(),
					"STOP LOSS HIT",
					"Exit:", round(current_ltp,2),
					"PnL:", round(pnl,2),
					"Total:", round(live_pnl,2)
				)

				log_trade([
					datetime.now(),
					"STOP LOSS",
					round(current_ltp,2),
					round(pnl,2),
					round(live_pnl,2)
				])

				continue

			# -----------------------------
			# Target
			# -----------------------------
			if current_ltp >= local_target:

				pnl = (current_ltp - local_entry) * local_qty

				with state_lock:

					live_pnl += pnl

					position = None
					signal_time = None
					breakeven_reached = False

				print(
					datetime.now(),
					"TARGET HIT",
					"Exit:", round(current_ltp,2),
					"PnL:", round(pnl,2),
					"Total:", round(live_pnl,2)
				)

				log_trade([
					datetime.now(),
					"TARGET HIT",
					round(current_ltp,2),
					round(pnl,2),
					round(live_pnl,2)
				])

		tme.sleep(0.05)


#=======================
# RESET STOP LOSS
#=======================

def reset_stop():

	global stop
	global loss_trades
	global breakeven_reached

	try:

		with candle_lock:

			if len(candles) < 2:
				return

			df = pd.DataFrame(candles.copy())

		# Last completed candle
		signal_low_stop = df.iloc[-1]["low"]

		with state_lock:

			local_entry = entry
			local_stop = stop
			local_position = position

			if (
				local_position == "BUY_CONFIRMED"
				and signal_low_stop > local_entry
				and local_stop < local_entry
				):
				stop = local_entry
				breakeven_reached = True

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
	global signal_high

	with candle_lock:

		# Copy candles to avoid modification while calculating EMA
		df = pd.DataFrame(candles.copy())

	if len(df) < EMA_PERIOD + 2:
		return

	# Calculate EMA
	df["EMA"] = df["close"].ewm(
		span=EMA_PERIOD,
		adjust=False
	).mean()

	# Last candle is already CLOSED because on_ticks()
	# adds only completed candles.
	candle = df.iloc[-1]

	with state_lock:
		if position is not None:
			return

	# BUY condition
	if candle["high"] < candle["EMA"]:

		with state_lock:

			signal_high = candle["high"]

			entry = signal_high

			stop = candle["low"]

			risk = entry - stop

			if risk <= 0:
				return

			# Minimum stop distance
			if risk <= 0.5:

				stop = stop - (2 - risk)

				risk = entry - stop

				if risk <= 0:
					return

			qty_ = int(MAX_LOSS_PER_TRADE / risk)

			#qty_ = min(qty_, MAX_QTY)

			if qty_ <= 0:
				return

			target = entry + (risk * 5)

			qty = qty_

			signal_time = datetime.now()

			position = "BUY"

		print(
			"\n========================"
		)

		print(
			datetime.now(),
			"BUY SIGNAL"
		)

		print(
			"Time   :", candle["date"]
		)

		print(
			"Entry  :", round(entry,2)
		)

		print(
			"Stop   :", round(stop,2)
		)

		print(
			"Target :", round(target,2)
		)

		print(
			"Qty    :", qty
		)

		print(
			"EMA    :", round(candle["EMA"],2)
		)

		print(
			"========================\n"
		)
			

		
			


# ======================
# STARTUP
# ======================
try:
	print(kite.profile())
	print(datetime.now()," REST API OK")
	
except Exception as e:
	print(datetime.now()," REST API FAILED:", e)
	sys.exit(1)

# ======================
# Load historical candles once
# ======================

df = get_candles(kite, token)

with candle_lock:
	candles = df.to_dict("records")

print(f"Loaded {len(candles)} historical candles")


start_ws()


timeout = 30
start = tme.time()

while True:

	with price_lock:
		if ltp is not None:
			break

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

# ======================
# MAIN LOOP
# ======================

while True:

	current_time = datetime.now().time()

	# Before market opens
	if current_time < time(9, 30):
		tme.sleep(10)
		continue

	# Market closed
	if current_time >= time(15, 15):

		print(
			datetime.now(),
			"Market Closed"
		)

		kws.close()
		break

	# Daily loss limit
	if live_pnl <= MAX_LOSS:

		print(
			datetime.now(),
			"Max Loss Reached"
		)

		kws.close()
		break

	# Maximum losing trades
	if loss_trades >= MAX_TRADES:

		print(
			datetime.now(),
			"Max Loss Trades Reached"
		)

		kws.close()
		break

	# Manage existing trade
	if position == "BUY_CONFIRMED":
		reset_stop()

	# Small sleep to reduce CPU usage
	tme.sleep(0.5) 