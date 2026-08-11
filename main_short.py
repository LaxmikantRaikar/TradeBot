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
# version 1.4   date: 10-Aug-2026
# FIX: main-loop candle-boundary scheduling was anchored to midnight, but Kite's
# intraday candles are anchored to market open (09:15:00). For any TIMEFRAME that
# doesn't evenly divide the 09:15 offset (e.g. 2minute, 5minute, 15minute...), this
# caused the loop to wake up and fetch historical_data() up to CANDLE_SECONDS-1
# early — i.e. while the most recent real candle was still forming, not closed.
# Fixed by anchoring the wake-up schedule to MARKET_OPEN instead of midnight,
# plus a small CLOSE_PUBLISH_BUFFER to avoid racing Kite's candle-publish latency
# right at the boundary.
# version 1.5   date: 10-Aug-2026
# FIX (root cause of "high == low" / partial-candle reads seen in live logs):
# kite.historical_data(from_date, to_date=now(), interval) returns the CURRENTLY
# FORMING candle as its last row whenever `now()` falls inside that candle's
# still-open period — it does not wait for the candle to seal, no matter how
# long CLOSE_PUBLISH_BUFFER is. df.iloc[-1] was therefore frequently a live,
# partially-built candle (often open==high==low==close, since only 1-2 ticks
# had arrived), not the last CLOSED candle. This polluted both the EMA
# calculation and the signal/BTC candle in evaluate_short_signal() and
# update_breakeven_track_candle(). Fixed by explicitly filtering historical_data
# to only candles whose full period has elapsed (candle_start + CANDLE_SECONDS
# <= now) via get_closed_candles(), and using that filtered frame everywhere
# instead of the raw response.

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
FILE = "trades_short.csv"


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
MAX_LOSS = -40
SYM = "HINDUNILVR"
EXCHANGE = "NSE"
TIMEFRAME = "2minute" #for 1 minute it is "minute", for 5 minute it is "5minute", 15minute, 30minute, 60minute, etc.

MAX_LOSS_PER_TRADE = abs(MAX_LOSS) / MAX_TRADES

# NSE cash market opens at 09:15:00. All of Kite's intraday candle boundaries
# for any timeframe are anchored to this time, not to midnight.
MARKET_OPEN = time(9, 15, 0)

# Small safety buffer (seconds) added AFTER a candle boundary before we fetch
# historical_data(), to avoid racing Kite's candle-publish latency and reading
# a still-forming candle as if it were closed.
CLOSE_PUBLISH_BUFFER = 3


def get_candle_seconds(timeframe):
	"""
	Converts a Kite Connect interval string into its duration in seconds.
	Supports: "minute", "3minute", "5minute", "10minute", "15minute",
	"30minute", "60minute". Raises if given an unsupported/daily interval,
	since this bot's timing logic assumes an intraday candle.
	"""

	if timeframe == "minute":
		return 60

	match = re.fullmatch(r"(\d+)minute", timeframe)

	if match:
		return int(match.group(1)) * 60

	raise ValueError(
		f"Unsupported TIMEFRAME for this bot: '{timeframe}'. "
		f"Use one of: minute, 3minute, 5minute, 10minute, 15minute, 30minute, 60minute."
	)


CANDLE_SECONDS = get_candle_seconds(TIMEFRAME)

# Buffer (in seconds) kept before the candle closes, to decide whether a SHORT
# signal has been confirmed in time. This stays a fixed buffer regardless of
# TIMEFRAME (previously hardcoded as 60 - 58 = 2 seconds for 1-minute candles).
BUFFER_SECONDS = 2

CONFIRM_TIMEOUT = CANDLE_SECONDS - BUFFER_SECONDS


def seconds_until_next_candle_close(now=None):
	"""
	Returns how long to sleep (in seconds) until CLOSE_PUBLISH_BUFFER seconds
	after the next real candle boundary, where boundaries are anchored to
	MARKET_OPEN (09:15:00) rather than midnight.

	Anchoring to midnight is wrong whenever CANDLE_SECONDS doesn't evenly
	divide the 09:15 offset-from-midnight (33300s) — which is every
	TIMEFRAME except plain "minute". For those cases the midnight-anchored
	schedule wakes the loop up early, while the most recent candle from
	Kite is still forming, so evaluate_short_signal() / 
	update_breakeven_track_candle() end up reading a partial candle as
	df.iloc[-1].
	"""

	if now is None:
		now = datetime.now()

	market_open_dt = now.replace(
		hour=MARKET_OPEN.hour,
		minute=MARKET_OPEN.minute,
		second=MARKET_OPEN.second,
		microsecond=0
	)

	seconds_since_open = (now - market_open_dt).total_seconds()

	if seconds_since_open < 0:
		seconds_since_open = 0

	remainder = seconds_since_open % CANDLE_SECONDS

	if remainder == 0:
		sleep_time = CLOSE_PUBLISH_BUFFER
	else:
		sleep_time = (CANDLE_SECONDS - remainder) + CLOSE_PUBLISH_BUFFER

	return sleep_time


def get_closed_candles(df):
	"""
	Kite's historical_data() includes the currently-forming candle as the
	last row whenever `to_date` (now) falls inside that candle's still-open
	period. This filters df down to only candles whose full period has
	actually elapsed, so callers never accidentally read a live/partial
	candle (which typically shows open==high==low==close, or a much
	narrower range than the real closed candle will end up with).

	Returns a DataFrame (possibly empty) containing only closed candles,
	in the same order as the input.
	"""

	if df.empty:
		return df

	candle_span = timedelta(seconds=CANDLE_SECONDS)

	tz = df["date"].dt.tz

	now_ts = pd.Timestamp.now(tz=tz) if tz is not None else pd.Timestamp.now()

	return df[df["date"] + candle_span <= now_ts]


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
loss_trades = 0
breakeven_reached = False

# Breakeven Track Candle (BTC) state — tracks the candle used to decide
# when the stop should be moved to breakeven for the current SHORT trade.
alert_candle_high = None   # high of the alert/signal candle (set at entry)
btc_high = None
btc_low = None
btc_confirmed = False
last_btc_candle_time = None

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
	global loss_trades
	global breakeven_reached
	global stop
	global btc_high
	global btc_low
	global btc_confirmed
	global last_btc_candle_time


	while True:

		if position == "SHORT" and signal_time is not None:
			elapsed = (datetime.now() - signal_time).total_seconds()
			if elapsed > CONFIRM_TIMEOUT:  # scales with TIMEFRAME, minus BUFFER_SECONDS
				with state_lock:
					#print(datetime.now(), "SHORT TIMED OUT — price never broke below entry", round(entry, 2))
					position = None
					signal_time = None
					btc_high = None
					btc_low = None
					btc_confirmed = False
					last_btc_candle_time = None
					tme.sleep(0.1)
					continue

		if position not in ("SHORT", "SHORT_CONFIRMED"):
			tme.sleep(0.1)
			continue

		if ltp is None:
			tme.sleep(0.1)
			continue
		#print(ltp, stop)


		with state_lock:

			# --- BTC-based breakeven trigger (tick-driven) ---
			# Once a Breakeven Track Candle has been confirmed (condition 2),
			# we stop tracking candles and instead watch live LTP: the moment
			# LTP breaks below the confirmed BTC's low, move stop to breakeven.
			if (
				position == "SHORT_CONFIRMED"
				and btc_confirmed
				and not breakeven_reached
				and btc_low is not None
				and ltp < btc_low
				and stop > entry
			):
				stop = entry
				breakeven_reached = True

				print(
					datetime.now(),
					"STOP MOVED TO BREAKEVEN (BTC low broken)",
					round(stop, 2)
				)

			if position == "SHORT" and ltp < entry:

				position = "SHORT_CONFIRMED"
				
				print(
					"\n====================\n",
					datetime.now(),
					"SHORT CONFIRMED",
					"Entry:",
					round(ltp, 2),
					"Stop:",
					round(stop, 2),
					"Target:",
					round(target, 2),
					"Qty:",
					round(qty, 2)
					)
				
	
				log_trade([
					datetime.now(),
					"SHORT CONFIRMED",
					"Entry:",
					round(entry, 2),
					"Stop",
					round(stop, 2),
					"Target:",
					round(target, 2),
						])
				
				'''order_id = kite.place_order( 
						variety=kite.VARIETY_REGULAR,
						exchange=kite.EXCHANGE_NSE,
						tradingsymbol=SYM,
						transaction_type=kite.TRANSACTION_TYPE_SELL,
						quantity=qty,
						product=kite.PRODUCT_MIS,
						order_type=kite.ORDER_TYPE_MARKET,
						market_protection= -1
						)'''


			if ltp >= stop and position == "SHORT_CONFIRMED":
				#print(ltp, stop)


				exit_price = ltp

				pnl = (entry - exit_price) * qty

				live_pnl += pnl

				'''print("Exit:",
				round(exit_price, 2))'''

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
					round(live_pnl, 2),
					"\n====================\n",
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
				
				'''order_id = kite.place_order( 
						variety=kite.VARIETY_REGULAR,
						exchange=kite.EXCHANGE_NSE,
						tradingsymbol=SYM,
						transaction_type=kite.TRANSACTION_TYPE_BUY,
						quantity=qty,
						product=kite.PRODUCT_MIS,
						order_type=kite.ORDER_TYPE_MARKET,
						market_protection= -1
						)'''
				
				if not breakeven_reached:
					loss_trades += 1
	
				position = None
				signal_time = None
				breakeven_reached = False
				btc_high = None
				btc_low = None
				btc_confirmed = False
				last_btc_candle_time = None

			tme.sleep(0.1)


#=======================
# BREAKEVEN TRACK CANDLE (BTC)
#=======================
# Rules:
# - After entry, the first candle whose high is below the alert candle's
#   high becomes the Breakeven Track Candle (BTC).
# - Every candle after that (NC = Next Candle) is compared to the current BTC:
#     1. NC.low < BTC.low and NC.high < BTC.high  -> NC becomes the new BTC
#     2. NC.low > BTC.low and NC.high > BTC.high  -> BTC is CONFIRMED, stop searching
#     3. NC.low > BTC.low and NC.high < BTC.high  -> BTC unchanged (inside bar)
#     4. NC.low < BTC.low and NC.high > BTC.high  -> NC becomes the new BTC (outside bar)
# - Once confirmed, this function stops updating BTC. The actual breakeven
#   move happens tick-by-tick in monitor_trade() when LTP breaks BTC.low.
def update_breakeven_track_candle():
	global btc_high
	global btc_low
	global btc_confirmed
	global last_btc_candle_time

	try:

		to_date = datetime.now()
		from_date = to_date - timedelta(days=1)

		data = kite.historical_data(
			token,
			from_date,
			to_date,
			TIMEFRAME
		)

		if not data:
			return

		df = pd.DataFrame(data)

		df = get_closed_candles(df)

		if df.empty:
			return   # most recent candle hasn't fully closed yet — nothing new to process

		nc = df.iloc[-1]          # most recent CLOSED candle = Next Candle
		nc_time = nc["date"]
		nc_high = nc["high"]
		nc_low = nc["low"]

		with state_lock:

			if position != "SHORT_CONFIRMED":
				return

			if btc_confirmed:
				return  # already confirmed, search has stopped for this trade

			if last_btc_candle_time == nc_time:
				return  # this candle already processed

			last_btc_candle_time = nc_time

			# No BTC yet: first candle after alert with high < alert high becomes BTC
			if btc_high is None:

				if alert_candle_high is not None and nc_high < alert_candle_high:
					btc_high = nc_high
					btc_low = nc_low

					'''print(
						datetime.now(),
						"BTC SET",
						round(btc_high, 2),
						round(btc_low, 2)
					)'''

				return

			# Compare NC against the current BTC
			if nc_low < btc_low and nc_high < btc_high:
				# condition 1: new BTC
				btc_high = nc_high
				btc_low = nc_low

			elif nc_low > btc_low and nc_high > btc_high:
				# condition 2: BTC confirmed, stop searching
				btc_confirmed = True

				print(
					datetime.now(),
					"BTC CONFIRMED",
					"BTC_high ",
					round(btc_high, 2),
					"BTC_low ",round(btc_low, 2)
				)

			elif nc_low > btc_low and nc_high < btc_high:
				# condition 3: inside bar, BTC unchanged
				pass

			elif nc_low < btc_low and nc_high > btc_high:
				# condition 4: outside bar, new BTC
				btc_high = nc_high
				btc_low = nc_low

	except Exception as e:

		print(
			datetime.now(),
			"BTC TRACK ERROR:",
			e
		)

# ======================
# SIGNAL GENERATION
# ======================

def evaluate_short_signal():
	global position
	global entry
	global stop
	global target
	global qty
	global signal_time
	global alert_candle_high
	global btc_high
	global btc_low
	global btc_confirmed
	global last_btc_candle_time
	

	to_date = datetime.now()

	from_date = to_date - timedelta(days=1)

	data = kite.historical_data(

		token,
		from_date,
		to_date,
		TIMEFRAME
	)

	if not data:
		return

	df = pd.DataFrame(data)

	df = get_closed_candles(df)   # drop the still-forming candle before any calc

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


	print(
		datetime.now(),
		"signal_low:",
		signal_low,
		"signal_high:",
		signal_high,
		"signal_ema:",
		round(signal_ema, 2),
	)

	if position is not None:
		return
	

	if signal_low > signal_ema and position == None:

		entry_price = signal_low  #2128.6
		stop_price = signal_high  #2129

		risk = stop_price - entry_price  

		#risk = 2129-2128.6 = 0.4

		if risk <= 0:
			return

		elif risk >0 and risk < 1:
			stop_price = stop_price+(1-risk)  # 2129+(1-0.4) = 2129.6
			risk = stop_price - entry_price   #risk = 2129.6-2128.6 = 1

		trade_qty = int(MAX_LOSS_PER_TRADE / risk)


		if trade_qty <= 0:
			return

		trade_target = entry_price - (risk * 5)

		with state_lock:

			entry = entry_price
			stop = stop_price
			target = trade_target
			qty = trade_qty

			signal_time = datetime.now()

			# alert candle = the signal candle just used to generate this trade
			alert_candle_high = signal_high
			btc_high = None
			btc_low = None
			btc_confirmed = False
			last_btc_candle_time = None

			position = "SHORT"
			print('SHORT SIGNAL','entry: ',entry,'stop: ',stop,'target:',target,'qty:',qty)


# ======================
# STARTUP
# ======================
try:
	#print(kite.profile())
	print(datetime.now()," REST API OK")
	
except Exception as e:
	print(datetime.now()," REST API FAILED:", e)
	sys.exit(1)


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

	'''if live_pnl <= MAX_LOSS:

		print(
			datetime.now(),
			"Max Loss Reached"
		)
		break'''


	if loss_trades >= MAX_TRADES:
		print(datetime.now(),"Max Loss Trades Reached")
		break


	if position == "SHORT_CONFIRMED":
		update_breakeven_track_candle()

	if position is None:
		evaluate_short_signal()
		'''print('Signal search started at: ',current_time)'''

	# Sleep until CLOSE_PUBLISH_BUFFER seconds after the next real candle
	# boundary, anchored to MARKET_OPEN (09:15:00) — not midnight — so this
	# lines up with Kite's actual candle close times for any TIMEFRAME.
	sleep_time = seconds_until_next_candle_close()

	tme.sleep(sleep_time)