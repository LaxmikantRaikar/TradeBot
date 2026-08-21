# This is long position trading bot using Kite Connect API. It monitors the stock price in real-time, evaluates trading signals based on EMA and price action, and manages trades by setting entry, stop-loss, and target levels. It also logs trade details to a CSV file for record-keeping.
# Version 1.0
# changed the candle index from -2 to -1 for signal generation and stop loss reset.
# this is beta version with live trading activated. Please use it with caution and at your own risk. The author is not responsible for any financial loss incurred while using this bot.
# version 1.1
# version 1.2   date: 21-Aug-2026
# Ported the BTC (Breakeven Track Candle) + TTC (Trailing Track Candle / trailing
# target) model from the SHORT-side bot (main_short.py), mirrored for LONG:
#  - TIMEFRAME is now fully driven through CANDLE_SECONDS / CONFIRM_TIMEOUT
#    (previously several historical_data() calls hardcoded "minute").
#  - get_closed_candles() filters out the still-forming candle so signal/BTC/TTC
#    logic never accidentally reads a live, partial candle.
#  - seconds_until_next_candle_close() anchors the main-loop wake-up schedule to
#    MARKET_OPEN (09:15:00) instead of a fixed 60s, so it works for any TIMEFRAME.
#  - BREAKEVEN: update_breakeven_track_candle() tracks a BTC using higher-high/
#    higher-low continuation candles; once a pullback candle confirms the BTC
#    (lower high + lower low), monitor_trade() watches live LTP and moves stop
#    to breakeven the instant LTP breaks back ABOVE the confirmed BTC's high.
#  - TRAILING TARGET: after breakeven, update_trailing_track_candle() repeats the
#    same two-stage process on 5-minute candles (TRAIL_TIMEFRAME) to track a TTC;
#    once a TTC pattern-confirms and LTP then breaks ABOVE the TTC's high,
#    trailing_target is tightened UP to the TTC's low. The trade exits the
#    instant LTP falls back to trailing_target, ratcheting tighter (higher) as
#    new TTCs confirm.

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

			return get_closed_candles(pd.DataFrame(data))
				
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
TIMEFRAME = "minute" #for 1 minute it is "minute", for 5 minute it is "5minute", 15minute, 30minute, 60minute, etc.

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

# Timeframe used ONLY for the post-breakeven trailing target logic. Per spec
# this is always 5-minute candles, regardless of what TIMEFRAME the entry/BTC
# logic above is using.
TRAIL_TIMEFRAME = "5minute"
TRAIL_CANDLE_SECONDS = get_candle_seconds(TRAIL_TIMEFRAME)

# Buffer (in seconds) kept before the candle closes, to decide whether a BUY
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
	Kite is still forming, so evaluate_signal() /
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


def get_closed_candles(df, candle_seconds=None):
	"""
	Kite's historical_data() includes the currently-forming candle as the
	last row whenever `to_date` (now) falls inside that candle's still-open
	period. This filters df down to only candles whose full period has
	actually elapsed, so callers never accidentally read a live/partial
	candle (which typically shows open==high==low==close, or a much
	narrower range than the real closed candle will end up with).

	candle_seconds defaults to the main strategy CANDLE_SECONDS, but callers
	working with a different candle duration (e.g. the 5-minute trailing
	target candles) can pass their own value.

	Returns a DataFrame (possibly empty) containing only closed candles,
	in the same order as the input.
	"""

	if df.empty:
		return df

	if candle_seconds is None:
		candle_seconds = CANDLE_SECONDS

	candle_span = timedelta(seconds=candle_seconds)

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
# when the stop should be moved to breakeven for the current BUY trade.
# Mirrored from the SHORT bot: for LONG we track higher-high/higher-low
# continuation candles and confirm on a pullback (lower high + lower low).
alert_candle_low = None   # low of the alert/signal candle (set at entry)
btc_high = None
btc_low = None
btc_confirmed = False
last_btc_candle_time = None

# Trailing Target state — once the stop has been moved to breakeven, we
# start tracking a Trailing Track Candle (TTC) on TRAIL_TIMEFRAME (5-minute)
# candles, using the SAME confirmation logic as the BTC above. Each time a
# TTC is confirmed AND LTP then breaks back above that TTC's high,
# trailing_target is tightened UP to that TTC's low. The trade then exits
# the instant LTP falls back to trailing_target, instead of waiting for the
# original breakeven stop.
trailing_active = False        # True once breakeven has been reached for this trade
breakeven_time = None          # timestamp breakeven was achieved; only candles closing after this are eligible
ttc_high = None
ttc_low = None
ttc_confirmed = False
last_ttc_candle_time = None
trailing_target = None         # tightened exit level once a TTC is confirmed; None until then

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
	global trailing_active
	global breakeven_time
	global ttc_high
	global ttc_low
	global ttc_confirmed
	global last_ttc_candle_time
	global trailing_target


	while True:

		if position == "BUY" and signal_time is not None:
			elapsed = (datetime.now() - signal_time).total_seconds()
			if elapsed > CONFIRM_TIMEOUT:  # scales with TIMEFRAME, minus BUFFER_SECONDS
				with state_lock:
					#print(datetime.now(), "BUY TIMED OUT — price never broke above entry", round(entry, 2))
					position = None
					signal_time = None
					btc_high = None
					btc_low = None
					btc_confirmed = False
					last_btc_candle_time = None
					trailing_active = False
					breakeven_time = None
					ttc_high = None
					ttc_low = None
					ttc_confirmed = False
					last_ttc_candle_time = None
					trailing_target = None
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

			# --- BTC-based breakeven trigger (tick-driven) ---
			# Once a Breakeven Track Candle has been confirmed (condition 2),
			# we stop tracking candles and instead watch live LTP: the moment
			# LTP breaks back ABOVE the confirmed BTC's high, move stop to
			# breakeven.
			if (
				position == "BUY_CONFIRMED"
				and btc_confirmed
				and not breakeven_reached
				and btc_high is not None
				and ltp > btc_high
				and stop < entry
			):
				stop = entry
				breakeven_reached = True

				print(
					datetime.now(),
					"STOP MOVED TO BREAKEVEN (BTC high broken)",
					round(stop, 2)
				)

				# Start trailing target tracking: from here on we look for a
				# Trailing Track Candle (TTC) on 5-minute candles, using the
				# same confirmation logic as the BTC above. Only candles that
				# CLOSE after this moment are eligible to seed/confirm a TTC.
				trailing_active = True
				breakeven_time = datetime.now()
				ttc_high = None
				ttc_low = None
				ttc_confirmed = False
				last_ttc_candle_time = None
				trailing_target = None

			# --- TTC-based trailing target trigger (tick-driven) ---
			# Same two-stage pattern as the BTC/breakeven trigger above: once a
			# TTC has been pattern-confirmed (candle-only, in
			# update_trailing_track_candle()), we watch live LTP and only ACT
			# the moment LTP breaks above that confirmed TTC's high. At that
			# point trailing_target is tightened UP to the TTC's low, and the
			# search resets so a further, tighter TTC can be found next.
			if (
				position == "BUY_CONFIRMED"
				and trailing_active
				and ttc_confirmed
				and ttc_high is not None
				and ltp > ttc_high
			):
				candidate_target = ttc_low

				if trailing_target is None or candidate_target > trailing_target:
					trailing_target = candidate_target

					print(
						datetime.now(),
						"TRAILING TARGET SET (TTC high broken)",
						round(trailing_target, 2)
					)

				# resume the candle search for the NEXT (tighter) TTC
				ttc_high = None
				ttc_low = None
				ttc_confirmed = False

			if position == "BUY" and ltp > entry:

				position = "BUY_CONFIRMED"
				
				print(
					"\n====================\n",
					datetime.now(),
					"BUY CONFIRMED",
					"Entry:",
					round(entry, 2),
					"Stop:",
					round(stop, 2),
					"Target:",
					round(target, 2),
					"Qty:",
					round(qty, 2)
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


			# Effective exit level: once a Trailing Track Candle (TTC) has been
			# confirmed and LTP-triggered (post-breakeven), trailing_target is
			# tighter than the breakeven stop and takes priority. Until then,
			# fall back to stop.
			effective_exit_level = trailing_target if trailing_target is not None else stop

			if ltp <= effective_exit_level and position == "BUY_CONFIRMED":
				#print(ltp, stop)


				exit_price = ltp

				pnl = (exit_price - entry) * qty

				live_pnl += pnl

				exit_reason = "TRAILING TARGET HIT" if trailing_target is not None else "STOP LOSS HIT"

				'''print("Exit:",
				round(exit_price, 2))'''

				print(
					datetime.now(),
					exit_reason,
					"Exit:",
					round(exit_price, 2),
					"Exit Level:",
					round(effective_exit_level, 2),
					"PnL:",
					round(pnl, 2),
					"Total:",
					round(live_pnl, 2),
					"\n====================\n",
					)
				
				log_trade([
					datetime.now(),
					exit_reason,
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
						transaction_type=kite.TRANSACTION_TYPE_SELL,
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
				trailing_active = False
				breakeven_time = None
				ttc_high = None
				ttc_low = None
				ttc_confirmed = False
				last_ttc_candle_time = None
				trailing_target = None

			tme.sleep(0.1)


#=======================
# BREAKEVEN TRACK CANDLE (BTC)
#=======================
# Rules (mirrored from the SHORT bot for a LONG trade):
# - After entry, the first candle whose low is above the alert candle's
#   low becomes the Breakeven Track Candle (BTC).
# - Every candle after that (NC = Next Candle) is compared to the current BTC:
#     1. NC.high > BTC.high and NC.low > BTC.low  -> NC becomes the new BTC
#     2. NC.high < BTC.high and NC.low < BTC.low  -> BTC is CONFIRMED, stop searching
#     3. NC.high < BTC.high and NC.low > BTC.low  -> BTC unchanged (inside bar)
#     4. NC.high > BTC.high and NC.low < BTC.low  -> NC becomes the new BTC (outside bar)
# - Once confirmed, this function stops updating BTC. The actual breakeven
#   move happens tick-by-tick in monitor_trade() when LTP breaks BTC.high.
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

			if position != "BUY_CONFIRMED":
				return

			if btc_confirmed:
				return  # already confirmed, search has stopped for this trade

			if last_btc_candle_time == nc_time:
				return  # this candle already processed

			last_btc_candle_time = nc_time

			# No BTC yet: first candle after alert with low > alert low becomes BTC
			if btc_high is None:

				if alert_candle_low is not None and nc_low > alert_candle_low:
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
			if nc_high > btc_high and nc_low > btc_low:
				# condition 1: new BTC
				btc_high = nc_high
				btc_low = nc_low

			elif nc_high < btc_high and nc_low < btc_low:
				# condition 2: BTC confirmed, stop searching
				btc_confirmed = True

				print(
					datetime.now(),
					"BTC CONFIRMED",
					"BTC_high ",
					round(btc_high, 2),
					"BTC_low ",round(btc_low, 2)
				)

			elif nc_high < btc_high and nc_low > btc_low:
				# condition 3: inside bar, BTC unchanged
				pass

			elif nc_high > btc_high and nc_low < btc_low:
				# condition 4: outside bar, new BTC
				btc_high = nc_high
				btc_low = nc_low

	except Exception as e:

		print(
			datetime.now(),
			"BTC TRACK ERROR:",
			e
		)

#=======================
# TRAILING TARGET (Trailing Track Candle - TTC)
#=======================
# Runs only after the stop has been moved to breakeven (trailing_active).
# Uses TRAIL_TIMEFRAME (5-minute) candles and the SAME confirmation logic
# as the BTC above, but only considers candles that CLOSE after breakeven
# was reached ("rounded time" = the first fully-closed 5-min candle once
# we start looking, naturally aligned since we only accept closed candles).
#
# - The first eligible closed 5-min candle after breakeven seeds the TTC.
# - Every next candle (NC) is compared to the current TTC:
#     1. NC.high > TTC.high and NC.low > TTC.low  -> NC becomes the new TTC
#     2. NC.high < TTC.high and NC.low < TTC.low  -> TTC pattern CONFIRMED:
#          ttc_high/ttc_low freeze and candle search pauses. The actual
#          trailing_target is only set tick-by-tick in monitor_trade() once
#          LTP breaks back ABOVE this TTC's high (mirrors the BTC's
#          two-stage confirm -> LTP-break design).
#     3. NC.high < TTC.high and NC.low > TTC.low  -> TTC unchanged (inside bar)
#     4. NC.high > TTC.high and NC.low < TTC.low  -> NC becomes the new TTC (outside bar)
#
# The trade exits (in monitor_trade()) the instant LTP falls back down to
# trailing_target, which supersedes the plain breakeven stop once set, and
# only ever moves UP (tighter), never down.
def update_trailing_track_candle():
	global ttc_high
	global ttc_low
	global ttc_confirmed
	global last_ttc_candle_time
	global trailing_target

	try:

		to_date = datetime.now()
		from_date = to_date - timedelta(days=1)

		data = kite.historical_data(
			token,
			from_date,
			to_date,
			TRAIL_TIMEFRAME
		)

		if not data:
			return

		df = pd.DataFrame(data)

		df = get_closed_candles(df, TRAIL_CANDLE_SECONDS)

		if df.empty:
			return   # most recent 5-min candle hasn't fully closed yet

		with state_lock:

			if position != "BUY_CONFIRMED" or not trailing_active:
				return

			if breakeven_time is None:
				return

			if ttc_confirmed:
				return  # waiting for LTP to break TTC high (handled in monitor_trade); pause candle search

			# Only candles that closed AFTER breakeven was reached are eligible.
			tz = df["date"].dt.tz
			bt_ref = pd.Timestamp(breakeven_time)

			if tz is not None and bt_ref.tzinfo is None:
				bt_ref = bt_ref.tz_localize(tz)

			eligible = df[df["date"] > bt_ref]

			if eligible.empty:
				return

			nc = eligible.iloc[-1]          # most recent CLOSED, eligible 5-min candle
			nc_time = nc["date"]
			nc_high = nc["high"]
			nc_low = nc["low"]

			print(
				datetime.now(),nc_time,"TTC CANDLE",
				"HIGH: ",round(nc_high, 2),"LOW: ",round(nc_low, 2))

			if last_ttc_candle_time == nc_time:
				return  # this candle already processed

			last_ttc_candle_time = nc_time

			# No TTC yet: first eligible candle after breakeven seeds the TTC
			if ttc_high is None:
				ttc_high = nc_high
				ttc_low = nc_low
				ttc_confirmed = False
				return

			# Compare NC against the current TTC
			if nc_high > ttc_high and nc_low > ttc_low:
				# condition 1: new TTC
				ttc_high = nc_high
				ttc_low = nc_low
				print("TTC_HIGH :",round(ttc_high, 2),"TTC_LOW :",round(ttc_low, 2))

			elif nc_high < ttc_high and nc_low < ttc_low:
				# condition 2: TTC pattern confirmed. Mirrors the BTC behaviour --
				# ttc_high/ttc_low stay FROZEN here and candle search pauses
				# (see the ttc_confirmed guard above). The actual trailing_target
				# is only set tick-by-tick in monitor_trade() once LTP breaks
				# above this TTC's high.
				ttc_confirmed = True

				print(
					datetime.now(),
					"TTC CONFIRMED",
					"TTC_high ",
					round(ttc_high, 2),
					"TTC_low ", round(ttc_low, 2)
				)

			elif nc_high < ttc_high and nc_low > ttc_low:
				# condition 3: inside bar, TTC unchanged
				print(datetime.now(),"# condition 3: inside bar, TTC unchanged")
				pass

			elif nc_high > ttc_high and nc_low < ttc_low:
				# condition 4: outside bar, new TTC
				ttc_high = nc_high
				ttc_low = nc_low
				print(datetime.now(),"# condition 4: outside bar, new TTC","TTC_HIGH: ",round(ttc_high, 2),"TTC_LOW: ",round(ttc_low, 2))

	except Exception as e:

		print(
			datetime.now(),
			"TTC TRACK ERROR:",
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
	global alert_candle_low
	global btc_high
	global btc_low
	global btc_confirmed
	global last_btc_candle_time
	global trailing_active
	global breakeven_time
	global ttc_high
	global ttc_low
	global ttc_confirmed
	global last_ttc_candle_time
	global trailing_target
	

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

		elif risk >0 and risk < 1:
			stop_price = stop_price-(1-risk)
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

			# alert candle = the signal candle just used to generate this trade
			alert_candle_low = signal_low
			btc_high = None
			btc_low = None
			btc_confirmed = False
			last_btc_candle_time = None
			trailing_active = False
			breakeven_time = None
			ttc_high = None
			ttc_low = None
			ttc_confirmed = False
			last_ttc_candle_time = None
			trailing_target = None

			position = "BUY"
			'''print('BUY SIGNAL','entry: ',entry,'stop: ',stop,'target:',target,'qty:',qty)'''

			

		
			


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

	if live_pnl <= MAX_LOSS:
		print(
			datetime.now(),
			"Max Loss Reached"
		)
		break


	if loss_trades >= MAX_TRADES:
		print(datetime.now(),"Max Loss Trades Reached")
		break


	if position == "BUY_CONFIRMED":
		update_breakeven_track_candle()

		if trailing_active:
			update_trailing_track_candle()

	if position is None:
		evaluate_signal()
		'''print('Signal search started at: ',current_time)'''

	# Sleep until CLOSE_PUBLISH_BUFFER seconds after the next real candle
	# boundary, anchored to MARKET_OPEN (09:15:00) — not midnight — so this
	# lines up with Kite's actual candle close times for any TIMEFRAME.
	sleep_time = seconds_until_next_candle_close()

	tme.sleep(sleep_time)