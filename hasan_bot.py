#!/usr/bin/env python3
# ============================================
# hassan_final_scalp_bot_v11_webserver.py
# V11 – Ultra Institutional Scalper
# + Web Server for UptimeRobot (Render 24/7)
# ============================================

import asyncio, json, time, ssl, logging, os, random, traceback, socket
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from collections import deque
import aiohttp
from aiohttp import web
from websockets.legacy.client import connect
import numpy as np
from logging.handlers import RotatingFileHandler

# ---------- LOGGING ----------
class ColoredFormatter(logging.Formatter):
    COLORS = {
        'DEBUG': '\033[96m', 'INFO': '\033[92m', 'WARNING': '\033[93m',
        'ERROR': '\033[91m', 'CRITICAL': '\033[97;41m', 'RESET': '\033[0m'
    }
    def format(self, record):
        t = datetime.now().strftime('%H:%M:%S')
        c = self.COLORS.get(record.levelname, '')
        return f"{c}[{t}] {record.levelname}: {record.getMessage()}{self.COLORS['RESET']}"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('HassanBotV11')
logger.handlers.clear()
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColoredFormatter())
logger.addHandler(console_handler)
file_handler = RotatingFileHandler('hassan_bot_v11.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
logger.addHandler(file_handler)

# ============================================
# CONFIGURATION
# ============================================
CONFIG = {
    # === API & TELEGRAM ===
    "BINANCE_PUBLIC_API": "https://api.binance.com",
    "BINANCE_WS_PUBLIC": "wss://stream.binance.com:9443/ws",
    "TELEGRAM_BOT_TOKEN": "8294766234:AAFA7tsO7IPCLligqOiY_XNo9-rPy-m1chs",
    "SIGNAL_CHANNEL": "-1003881031372",

    # === SYMBOLS ===
    "SYMBOLS": ["ETHUSDT", "SOLUSDT"],
    "BASE_SYMBOL": "BTCUSDT",

    # === TIMEFRAMES ===
    "TIMEFRAMES": ["1m", "3m", "5m", "15m", "1h"],
    "CANDLE_LIMITS": {"1m": 150, "3m": 100, "5m": 100, "15m": 80, "1h": 60},

    # === ENGINES PARAMS ===
    "WHALE_MIN_VOLUME_USD": 15000, "WHALE_WINDOW_SECONDS": 120, "WHALE_MIN_TRADES": 2,
    "ORDERBOOK_IMBALANCE_THRESHOLD": 0.60, "ORDERBOOK_DEPTH_LIMIT": 20,
    "VOLUME_SPIKE_MULTIPLIER": 1.8,
    "MOMENTUM_LOOKBACK_CANDLES": 3, "MOMENTUM_ATR_MULTIPLIER": 1.5, "MOMENTUM_VOLUME_SPIKE_MULTIPLIER": 1.3,
    "STRUCTURE_LOOKBACK_CANDLES": 50, "STRUCTURE_SWING_STRENGTH": 3,
    "CORRELATION_WINDOW": 20, "MAX_CORRELATION_DIVERGENCE": 0.25,
    "MIN_DELTA_RATIO": 0.25,
    "RSI_OVERBOUGHT": 65, "RSI_OVERSOLD": 35,

    # === TREND (EMA 9/21) ===
    "TREND_TIMEFRAME": "1h", "TREND_EMA_FAST": 9, "TREND_EMA_SLOW": 21,
    "TREND_MIN_CANDLES": 30, "TREND_MIN_DIFF_PCT": 0.15,

    # === ENTRY / EXIT ===
    "MIN_CONFIDENCE": 70, "MIN_CONFIDENCE_CONSECUTIVE": 78,
    "MAX_SPREAD_PCT": 0.05, "MIN_LIQUIDITY_USD": 2000000,
    "MIN_ATR_PCT": 0.20,
    "ATR_MULTIPLIER_SL": 1.2,
    "ATR_MULTIPLIER_TP1": 1.3, "ATR_MULTIPLIER_TP2": 2.0, "ATR_MULTIPLIER_TP3": 3.5,
    "RISK_PER_TRADE": 0.004, "BASE_CAPITAL": 1000,
    "TP1_SIZE": 0.4, "TP2_SIZE": 0.35, "TP3_SIZE": 0.25,
    "MAX_SCALP_MINUTES": 10, "MAX_OPEN_TRADES": 2, "MAX_OPEN_PER_SYMBOL": 1, "COOLDOWN_SECONDS": 120,
    "EXTENDED_DURATION": 15, "MIN_PROGRESS_PCT": 0.25,

    # === SAFEGUARDS ===
    "DAILY_LOSS_CIRCUIT_BREAKER": 2.5, "VOLATILITY_CIRCUIT_BREAKER": 2.0, "ATR_SPIKE_FILTER": 3.0,
    "TRAILING_ATR_MULTIPLIER": 0.4, "ACTIVE_HOURS_START": 7, "ACTIVE_HOURS_END": 22,
    "CONSECUTIVE_LOSS_LIMIT": 3,

    # === SHARP MOVE ===
    "SHARP_MOVE_PCT_THRESHOLD": 0.5, "SHARP_MOVE_VOL_SPIKE": 2.0, "SHARP_MOVE_COOLDOWN": 60,

    # === NEWS BLACKOUT ===
    "NEWS_BLACKOUT_MINUTES": 30,
    "ECONOMIC_CALENDAR": [
        # FOMC, CPI, NFP, GDP, PPI, Retail Sales
        ("2026-06-17", "18:00", "FOMC", 120),
        ("2026-07-29", "18:00", "FOMC", 120),
        ("2026-09-16", "18:00", "FOMC", 120),
        ("2026-11-04", "19:00", "FOMC", 120),
        ("2026-12-09", "19:00", "FOMC", 120),
        ("2026-06-10", "12:30", "CPI", 90),
        ("2026-07-15", "12:30", "CPI", 90),
        ("2026-08-12", "12:30", "CPI", 90),
        ("2026-09-16", "12:30", "CPI", 90),
        ("2026-10-14", "12:30", "CPI", 90),
        ("2026-11-18", "13:30", "CPI", 90),
        ("2026-12-16", "13:30", "CPI", 90),
        ("2026-06-05", "12:30", "NFP", 90),
        ("2026-07-02", "12:30", "NFP", 90),
        ("2026-08-07", "12:30", "NFP", 90),
        ("2026-09-04", "12:30", "NFP", 90),
        ("2026-10-02", "12:30", "NFP", 90),
        ("2026-11-06", "13:30", "NFP", 90),
        ("2026-12-04", "13:30", "NFP", 90),
        ("2026-07-30", "12:30", "GDP", 90),
        ("2026-10-29", "12:30", "GDP", 90),
        ("2026-06-16", "12:30", "Retail Sales", 60),
    ],

    # === WEB SERVER ===
    "WEB_SERVER_PORT": 10000,
}

# ============================================
# DATA MODELS
# ============================================
@dataclass
class Candle:
    timestamp: datetime; open: float; high: float; low: float; close: float
    volume: float; symbol: str; timeframe: str; trades: int = 0

    def __post_init__(self):
        if self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)
        if self.high < self.low:
            self.high, self.low = self.low, self.high
        self.open = max(self.low, min(self.open, self.high))
        self.close = max(self.low, min(self.close, self.high))
        self.volume = max(0, self.volume)

    @property
    def body_size(self): return abs(self.close - self.open)
    @property
    def total_range(self): return self.high - self.low
    @property
    def upper_shadow(self): return self.high - max(self.open, self.close)
    @property
    def lower_shadow(self): return min(self.open, self.close) - self.low
    @property
    def is_bullish(self): return self.close > self.open

@dataclass
class ScalpSignal:
    id: str; symbol: str; direction: str; entry_price: float; stop_loss: float
    take_profit: List[float]; confidence: float; star_rating: str
    momentum_strength: str; whale_status: str; market_structure: str
    risk_reward: float; reason_summary: str; timestamp: datetime
    risk_percent: float; position_size: float = 0.0
    signal_type: str = "STANDARD"

@dataclass
class ActiveTrade:
    signal_id: str; symbol: str; direction: str; entry_price: float; stop_loss: float
    tp1: float; tp2: float; tp3: float; position_size: float; entry_time: datetime
    remaining_size: float; tp1_hit: bool = False; tp2_hit: bool = False
    status: str = "OPEN"; exit_price: float = 0.0; pnl_pct: float = 0.0

# ============================================
# MATH UTILS
# ============================================
class MathUtils:
    @staticmethod
    def mean(v): return sum(v)/len(v) if v else 0.0
    @staticmethod
    def std(v):
        if len(v)<2: return 0.0
        m=MathUtils.mean(v)
        return (sum((x-m)**2 for x in v)/(len(v)-1))**0.5
    @staticmethod
    def ema(v, p):
        if len(v)<p: return []
        k=2/(p+1); e=[v[0]]
        for i in range(1,len(v)): e.append(v[i]*k+e[-1]*(1-k))
        return e
    @staticmethod
    def rsi(prices, period=14):
        if len(prices)<period+1: return 50.0
        deltas=[prices[i]-prices[i-1] for i in range(1,len(prices))]
        gains=[d if d>0 else 0 for d in deltas]
        losses=[-d if d<0 else 0 for d in deltas]
        ag=MathUtils.mean(gains[:period]); al=MathUtils.mean(losses[:period])
        return 100-(100/(1+ag/al)) if al else 100.0
    @staticmethod
    def atr(highs, lows, closes, period=14):
        if len(highs)<period: return 0.0
        tr=[max(highs[i]-lows[i], abs(highs[i]-closes[i-1]), abs(lows[i]-closes[i-1])) for i in range(1,len(highs))]
        return MathUtils.mean(tr[-period:])
    @staticmethod
    def correlation(x, y):
        if len(x)!=len(y) or len(x)<2: return 0.0
        mx=MathUtils.mean(x); my=MathUtils.mean(y)
        num=sum((x[i]-mx)*(y[i]-my) for i in range(len(x)))
        dx=(sum((xi-mx)**2 for xi in x))**0.5
        dy=(sum((yi-my)**2 for yi in y))**0.5
        return num/(dx*dy) if dx*dy else 0.0

# ============================================
# RATE LIMITER / MARKET DATA
# ============================================
class RateLimiter:
    def __init__(self):
        self.sem=asyncio.Semaphore(3)
        self.lock=asyncio.Lock()
        self.last=0.0
    async def acquire(self):
        await self.sem.acquire()
        async with self.lock:
            now=time.time()
            if self.last and now-self.last<0.3:
                await asyncio.sleep(0.3-(now-self.last))
            self.last=time.time()
    def release(self): self.sem.release()

def rate_limited(func):
    async def wrapper(self, *a, **kw):
        await self.rate_limiter.acquire()
        try: return await func(self, *a, **kw)
        finally: self.rate_limiter.release()
    return wrapper

class MarketDataHub:
    def __init__(self):
        self.base=CONFIG["BINANCE_PUBLIC_API"]; self.ws=CONFIG["BINANCE_WS_PUBLIC"]
        self.sess=None
        self.ssl=ssl.create_default_context()
        self.ssl.check_hostname=False
        self.ssl.verify_mode=ssl.CERT_NONE
        self.rate_limiter=RateLimiter()
        self.candles={s:{tf:deque(maxlen=CONFIG["CANDLE_LIMITS"][tf]) for tf in CONFIG["TIMEFRAMES"]} for s in CONFIG["SYMBOLS"]}
        self.orderbooks={s:{"bids":[],"asks":[]} for s in CONFIG["SYMBOLS"]}
        self.trades={s:deque(maxlen=1000) for s in CONFIG["SYMBOLS"]}
        self.req=0; self.err=0

    async def ensure_session(self):
        try:
            if not self.sess or self.sess.closed:
                connector = aiohttp.TCPConnector(ssl=False, force_close=True, family=socket.AF_INET)
                timeout = aiohttp.ClientTimeout(total=30, sock_read=15)
                self.sess = aiohttp.ClientSession(timeout=timeout, connector=connector)
        except Exception as e:
            logger.error(f"Session creation failed: {e}")
            self.sess = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30, sock_read=15))

    @rate_limited
    async def fetch(self, endpoint, params=None):
        await self.ensure_session()
        url=f"{self.base}{endpoint}"
        for attempt in range(5):
            try:
                self.req+=1
                async with self.sess.get(url, params=params, ssl=self.ssl) as r:
                    if r.status==200: return await r.json()
                    elif r.status==429: await asyncio.sleep(int(r.headers.get('Retry-After',3)))
                    elif r.status>=500: await asyncio.sleep(2*(attempt+1))
                    else: break
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                self.err+=1
                logger.warning(f"Fetch attempt {attempt+1} failed for {endpoint}: {e}")
                await self.ensure_session()
                await asyncio.sleep(min(2**attempt, 30))
        return None

    async def price(self, sym):
        d=await self.fetch("/api/v3/ticker/price",{"symbol":sym})
        return float(d["price"]) if d and "price" in d else None

    async def spread(self, sym):
        d=await self.fetch("/api/v3/ticker/bookTicker",{"symbol":sym})
        if d:
            b=float(d["bidPrice"]); a=float(d["askPrice"]); sp=(a-b)/b*100 if b else 0
            return {"bid":b,"ask":a,"spread_pct":sp}
        return {"bid":0,"ask":0,"spread_pct":100}

    async def orderbook(self, sym):
        d=await self.fetch("/api/v3/depth",{"symbol":sym,"limit":50})
        if d:
            self.orderbooks[sym]={
                "bids":[[float(p),float(q)] for p,q in d.get("bids",[])],
                "asks":[[float(p),float(q)] for p,q in d.get("asks",[])]
            }

    async def agg_trades(self, sym):
        d=await self.fetch("/api/v3/aggTrades",{"symbol":sym,"limit":100})
        if d:
            for t in reversed(d):
                self.trades[sym].append({
                    "price":float(t["p"]),"quantity":float(t["q"]),
                    "time":datetime.fromtimestamp(t["T"]/1000,tz=timezone.utc),
                    "is_buyer_maker":t["m"]
                })

    async def volume_24h(self, sym):
        d=await self.fetch("/api/v3/ticker/24hr",{"symbol":sym})
        if d and "quoteVolume" in d:
            return float(d["quoteVolume"])
        return 0

    async def klines(self, sym, interval, limit=100):
        d=await self.fetch("/api/v3/klines",{"symbol":sym,"interval":interval,"limit":limit})
        res=[]
        if d:
            for k in d:
                try:
                    res.append(Candle(
                        timestamp=datetime.fromtimestamp(k[0]/1000,tz=timezone.utc),
                        open=float(k[1]),high=float(k[2]),low=float(k[3]),
                        close=float(k[4]),volume=float(k[5]),symbol=sym,
                        timeframe=interval,trades=k[8]
                    ))
                except: pass
        return res

    async def ws_klines(self, sym, interval):
        stream=f"{sym.lower()}@kline_{interval}"
        url=f"{self.ws}/{stream}"
        delay=1
        while True:
            try:
                async with connect(url, ssl=self.ssl, ping_interval=15, ping_timeout=8) as ws:
                    logger.info(f"✅ WS {sym} {interval}")
                    delay=1
                    while True:
                        try:
                            msg=await asyncio.wait_for(ws.recv(), timeout=20)
                            data=json.loads(msg)
                            if 'k' not in data: continue
                            k=data['k']
                            if not k.get('x'): continue
                            c=Candle(
                                timestamp=datetime.fromtimestamp(k['t']/1000,tz=timezone.utc),
                                open=float(k['o']),high=float(k['h']),low=float(k['l']),
                                close=float(k['c']),volume=float(k['v']),symbol=sym,
                                timeframe=interval,trades=k.get('n',0)
                            )
                            self.candles[sym][interval].append(c)
                            if interval=="1m": yield c
                        except asyncio.TimeoutError: continue
                        except Exception: break
            except Exception as e:
                logger.error(f"WS {sym} {interval}: {e}")
                await asyncio.sleep(delay)
                delay=min(delay*2,30)

# ============================================
# 6 ENGINES
# ============================================
class OrderbookImbalanceEngine:
    def __init__(self, hub): self.hub=hub
    def analyze(self, sym):
        ob=self.hub.orderbooks[sym]
        bids=ob.get("bids",[]); asks=ob.get("asks",[])
        if not bids or not asks: return {"direction":"NEUTRAL","score":0}
        d=20; bv=sum(q for _,q in bids[:d]); av=sum(q for _,q in asks[:d])
        if bv+av==0: return {"direction":"NEUTRAL","score":0}
        ratio=bv/(bv+av)
        if ratio>0.60: return {"direction":"LONG","score":min(100,(ratio-0.5)*400)}
        if ratio<0.40: return {"direction":"SHORT","score":min(100,(0.5-ratio)*400)}
        return {"direction":"NEUTRAL","score":0}

class WhaleDetectorEngine:
    def __init__(self, hub): self.hub=hub
    def analyze(self, sym):
        trades=list(self.hub.trades[sym])
        if not trades: return {"direction":"NEUTRAL","score":0,"details":"ساکت"}
        cutoff=datetime.now(timezone.utc)-timedelta(seconds=120)
        large=[t for t in trades if t["time"]>=cutoff and t["price"]*t["quantity"]>=15000]
        if len(large)<2: return {"direction":"NEUTRAL","score":0,"details":"ساکت"}
        total=sum(t["price"]*t["quantity"] for t in large)
        buy=sum(t["price"]*t["quantity"] for t in large if not t["is_buyer_maker"])
        if buy>total*0.6: return {"direction":"LONG","score":min(100,len(large)*20),"details":"خرید سنگین 🐋"}
        if buy<total*0.4: return {"direction":"SHORT","score":min(100,len(large)*20),"details":"فروش سنگین 🐋"}
        return {"direction":"NEUTRAL","score":min(50,len(large)*10),"details":"فعال متعادل"}

class MomentumBreakoutEngine:
    @staticmethod
    def analyze(candles):
        n=3
        if len(candles)<n+10: return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        recent=candles[-n:]
        start=recent[0].open; end=recent[-1].close
        change=end-start
        atr=MathUtils.atr([c.high for c in candles],[c.low for c in candles],[c.close for c in candles])
        if atr==0: return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        ratio=abs(change)/(atr*1.5)
        if ratio<1: return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        direction="LONG" if change>0 else "SHORT"
        avg_vol_recent=MathUtils.mean([c.volume for c in recent])
        avg_vol_hist=MathUtils.mean([c.volume for c in candles[-(n+20):-n]]) or 1
        if avg_vol_recent<avg_vol_hist*1.3:
            return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        if direction=="LONG" and sum(1 for c in recent if c.is_bullish)<2:
            return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        if direction=="SHORT" and sum(1 for c in recent if not c.is_bullish)<2:
            return {"direction":"NEUTRAL","score":0,"strength":"بی‌نوسان"}
        score=min(100,ratio*35)
        if ratio<1.5: strength="ضعیف"
        elif ratio<2.5: strength="متوسط"
        elif ratio<4: strength="قوی"
        else: strength="انفجاری 🚀"
        return {"direction":direction,"score":score,"strength":strength}

class MarketStructureEngine:
    @staticmethod
    def analyze(candles):
        if len(candles)<50: return {"position":"نامشخص","bias":"NEUTRAL","score":0}
        highs=[c.high for c in candles]; lows=[c.low for c in candles]
        swings=[]
        for i in range(5, len(highs)-5):
            if highs[i]==max(highs[i-5:i+6]): swings.append(("HIGH", highs[i]))
            if lows[i]==min(lows[i-5:i+6]): swings.append(("LOW", lows[i]))
        if len(swings)<2: return {"position":"نامشخص","bias":"NEUTRAL","score":0}
        current=candles[-1].close
        last_high=max(s[1] for s in swings if s[0]=="HIGH") if any(s[0]=="HIGH" for s in swings) else current
        last_low=min(s[1] for s in swings if s[0]=="LOW") if any(s[0]=="LOW" for s in swings) else current
        if current<last_low*1.02 and current>last_low*0.98:
            return {"position":"کف ساختاری 🟢","bias":"LONG","score":80}
        if current>last_high*0.98 and current<last_high*1.02:
            return {"position":"سقف ساختاری 🔴","bias":"SHORT","score":80}
        if current>last_high*1.02:
            return {"position":"شکست صعودی 🚀","bias":"LONG","score":90}
        if current<last_low*0.98:
            return {"position":"شکست نزولی 💥","bias":"SHORT","score":90}
        return {"position":"میانه روند ↔️","bias":"NEUTRAL","score":30}

class OrderflowDeltaEngine:
    def __init__(self, hub): self.hub=hub
    def analyze(self, sym, direction):
        trades=list(self.hub.trades[sym])
        if not trades: return {"direction":"NEUTRAL","score":0}
        cutoff=datetime.now(timezone.utc)-timedelta(seconds=120)
        recent=[t for t in trades if t["time"]>=cutoff]
        if not recent: return {"direction":"NEUTRAL","score":0}
        buy=sum(t["price"]*t["quantity"] for t in recent if not t["is_buyer_maker"])
        sell=sum(t["price"]*t["quantity"] for t in recent if t["is_buyer_maker"])
        total=buy+sell
        if total==0: return {"direction":"NEUTRAL","score":0}
        delta=(buy-sell)/total
        if direction=="LONG" and delta>0.25: return {"direction":"LONG","score":min(100,delta*120)}
        if direction=="SHORT" and delta<-0.25: return {"direction":"SHORT","score":min(100,-delta*120)}
        return {"direction":"NEUTRAL","score":0}

class RsiEngine:
    @staticmethod
    def analyze(candles):
        if len(candles)<15: return {"direction":"NEUTRAL","score":0,"value":50}
        rsi=MathUtils.rsi([c.close for c in candles])
        direction="NEUTRAL"; score=0
        if rsi<CONFIG["RSI_OVERSOLD"]: direction="LONG"; score=(CONFIG["RSI_OVERSOLD"]-rsi)*3
        elif rsi>CONFIG["RSI_OVERBOUGHT"]: direction="SHORT"; score=(rsi-CONFIG["RSI_OVERBOUGHT"])*3
        return {"direction":direction,"score":min(score,100),"value":rsi}

class CorrelationFilter:
    def __init__(self, hub): self.hub=hub
    def check(self, sym, signal_direction):
        if sym==CONFIG["BASE_SYMBOL"]: return True
        btc_candles=list(self.hub.candles.get(CONFIG["BASE_SYMBOL"],{}).get("5m",[]))
        sym_candles=list(self.hub.candles[sym]["5m"])
        if len(btc_candles)<20 or len(sym_candles)<20: return True
        btc_returns=[(btc_candles[i].close-btc_candles[i-1].close)/btc_candles[i-1].close for i in range(-20,0)]
        sym_returns=[(sym_candles[i].close-sym_candles[i-1].close)/sym_candles[i-1].close for i in range(-20,0)]
        corr=MathUtils.correlation(btc_returns, sym_returns)
        btc_direction="LONG" if btc_returns[-1]>0 else "SHORT"
        if corr>0.5 and signal_direction!=btc_direction:
            return False
        return True

class CircuitBreaker:
    def __init__(self):
        self.daily_pnl=0.0
        self.last_reset=datetime.now(timezone.utc).date()
        self.is_triggered=False
    def reset_if_new_day(self):
        today=datetime.now(timezone.utc).date()
        if today!=self.last_reset:
            self.daily_pnl=0.0; self.last_reset=today; self.is_triggered=False
    def check(self, candles_5m):
        self.reset_if_new_day()
        if self.daily_pnl<=-2.5: self.is_triggered=True; return True
        if len(candles_5m)>=6:
            change=abs(candles_5m[-1].close-candles_5m[-6].close)/candles_5m[-6].close*100
            if change>=2.0: self.is_triggered=True; return True
        return False
    def add_pnl(self, pnl): self.daily_pnl+=pnl

class TrendFilterEngine:
    def __init__(self, hub): self.hub=hub
    def analyze(self, sym):
        tf=CONFIG["TREND_TIMEFRAME"]
        candles=list(self.hub.candles[sym].get(tf,[]))
        if len(candles)<CONFIG["TREND_MIN_CANDLES"]:
            return {"trend":"NEUTRAL","allowed_long":True,"allowed_short":True,"reason":"کندل ناکافی"}
        closes=[c.close for c in candles]
        ema9=MathUtils.ema(closes,9); ema21=MathUtils.ema(closes,21)
        if not ema9 or not ema21:
            return {"trend":"NEUTRAL","allowed_long":True,"allowed_short":True,"reason":"EMA ناکافی"}
        fast=ema9[-1]; slow=ema21[-1]
        diff_pct=(fast-slow)/slow*100
        if fast>slow and diff_pct>CONFIG["TREND_MIN_DIFF_PCT"]:
            return {"trend":"BULLISH","allowed_long":True,"allowed_short":False,"reason":f"صعودی ({diff_pct:.2f}%)"}
        elif fast<slow and diff_pct<-CONFIG["TREND_MIN_DIFF_PCT"]:
            return {"trend":"BEARISH","allowed_long":False,"allowed_short":True,"reason":f"نزولی ({diff_pct:.2f}%)"}
        else:
            return {"trend":"NEUTRAL","allowed_long":True,"allowed_short":True,"reason":"خنثی"}

class SharpMoveDetector:
    def __init__(self, hub): self.hub=hub; self.last_alert=datetime.min.replace(tzinfo=timezone.utc)
    def detect(self, sym):
        now=datetime.now(timezone.utc)
        if (now-self.last_alert).total_seconds()<CONFIG["SHARP_MOVE_COOLDOWN"]: return None
        c1m=list(self.hub.candles[sym]["1m"])
        if len(c1m)<10: return None
        last=c1m[-1]; prev=c1m[-2]
        move_pct=abs(last.close-prev.close)/prev.close*100
        if move_pct<CONFIG["SHARP_MOVE_PCT_THRESHOLD"]: return None
        avg_vol=MathUtils.mean([c.volume for c in c1m[-10:-1]]) if len(c1m)>=10 else last.volume
        if last.volume<avg_vol*CONFIG["SHARP_MOVE_VOL_SPIKE"]: return None
        self.last_alert=now
        return {"detected":True,"direction":"LONG" if last.close>prev.close else "SHORT",
                "price":last.close,"move_pct":round(move_pct,2)}

class NewsBlackoutEngine:
    def __init__(self):
        self.calendar=CONFIG["ECONOMIC_CALENDAR"]
        self.blackout=CONFIG["NEWS_BLACKOUT_MINUTES"]
    def is_blackout(self):
        now=datetime.now(timezone.utc)
        for d,t,n,i in self.calendar:
            if d==now.strftime("%Y-%m-%d"):
                try:
                    h,m=map(int,t.split(":"))
                    ev=now.replace(hour=h,minute=m,second=0,microsecond=0)
                    diff=(now-ev).total_seconds()/60
                    if -self.blackout<=diff<=i: return True, n
                except: continue
        return False, ""

# ============================================
# V10 SIGNAL GENERATOR
# ============================================
class UltimateSignalGenerator:
    def __init__(self, hub, breaker):
        self.hub=hub; self.breaker=breaker
        self.ob=OrderbookImbalanceEngine(hub); self.wh=WhaleDetectorEngine(hub)
        self.mom=MomentumBreakoutEngine(); self.ms=MarketStructureEngine()
        self.delta=OrderflowDeltaEngine(hub); self.rsi_eng=RsiEngine()
        self.corr=CorrelationFilter(hub); self.trend=TrendFilterEngine(hub)
        self.sharp=SharpMoveDetector(hub); self.news=NewsBlackoutEngine()
        self.last_sig={}; self.daily_trades=0; self.consecutive_losses=0

    def _star_rating(self, c):
        if c>=85: return "⭐⭐⭐⭐⭐"
        if c>=78: return "⭐⭐⭐⭐"
        if c>=70: return "⭐⭐⭐"
        return "⭐⭐"

    async def generate(self, sym):
        now=datetime.now(timezone.utc)
        if not (7<=now.hour<22): return None

        blackout, name = self.news.is_blackout()
        if blackout: logger.info(f"⛔ {sym} News: {name}"); return None

        c5m=list(self.hub.candles[sym]["5m"])
        if self.breaker.check(c5m): return None
        if sym in self.last_sig and (now-self.last_sig[sym]).total_seconds()<120: return None
        if self.daily_trades>=25: return None

        trend_result=self.trend.analyze(sym)
        logger.info(f"📈 {sym} Trend: {trend_result['trend']}")

        vol_24h=await self.hub.volume_24h(sym)
        if vol_24h<CONFIG["MIN_LIQUIDITY_USD"]:
            logger.info(f"⏭️ {sym} Low vol 24h (${vol_24h:,.0f})"); return None

        sp=await self.hub.spread(sym)
        if sp["spread_pct"]>0.05: return None
        await self.hub.orderbook(sym); await self.hub.agg_trades(sym)

        c1m=list(self.hub.candles[sym]["1m"])
        if len(c1m)<20 or len(c5m)<10: return None

        sharp=self.sharp.detect(sym)
        if sharp:
            d=sharp["direction"]; entry=sharp["price"]
            atr=MathUtils.atr([c.high for c in c5m],[c.low for c in c5m],[c.close for c in c5m])
            if atr==0: atr=entry*0.002
            sl=entry-atr*1.0 if d=="LONG" else entry+atr*1.0
            tp1=entry+atr*1.5 if d=="LONG" else entry-atr*1.5
            tp2=entry+atr*2.5 if d=="LONG" else entry-atr*2.5
            tp3=entry+atr*4.0 if d=="LONG" else entry-atr*4.0
            rr=abs(tp3-entry)/abs(entry-sl) if entry!=sl else 2.0
            ps=(CONFIG["BASE_CAPITAL"]*CONFIG["RISK_PER_TRADE"])/abs(entry-sl) if entry!=sl else 0
            sig=ScalpSignal(
                id=f"{sym}_{now.strftime('%H%M%S')}_{random.randint(100,999)}",
                symbol=sym,direction=d,entry_price=entry,stop_loss=sl,
                take_profit=[tp1,tp2,tp3],confidence=80,
                star_rating="⭐⭐⭐⭐",momentum_strength="انفجاری 🚀",
                whale_status="نامشخص",market_structure="حرکت شارپ",
                risk_reward=round(rr,2),reason_summary=f"Sharp Move {sharp['move_pct']}%",
                timestamp=now,risk_percent=CONFIG["RISK_PER_TRADE"],position_size=ps,
                signal_type="SHARP"
            )
            self.last_sig[sym]=now; self.daily_trades+=1
            logger.info(f"⚡ SHARP {sym} {d}")
            return sig

        ob_r=self.ob.analyze(sym); wh_r=self.wh.analyze(sym)
        mom_r=self.mom.analyze(c5m); ms_r=self.ms.analyze(c5m)
        rsi_r=self.rsi_eng.analyze(c5m)

        pre=[("ob",ob_r),("whale",wh_r),("momentum",mom_r),("structure",ms_r),("rsi",rsi_r)]
        lv=sum(1 for _,r in pre if r.get("direction")=="LONG")
        sv=sum(1 for _,r in pre if r.get("direction")=="SHORT")

        if lv == sv:
            logger.info(f"⏭️ {sym} Tie vote (L:{lv} S:{sv}) or no consensus")
            return None
        init = "LONG" if lv > sv else "SHORT"

        delta_r=self.delta.analyze(sym, init)
        engines=pre+[("delta",delta_r)]

        total_score=sum(r.get("score",0) for _,r in engines if r.get("direction")==init)
        confidence=min(95, total_score/6*0.85)

        if init=="LONG" and not trend_result["allowed_long"]:
            logger.info(f"🚫 {sym} LONG blocked by trend"); return None
        if init=="SHORT" and not trend_result["allowed_short"]:
            logger.info(f"🚫 {sym} SHORT blocked by trend"); return None

        if trend_result["trend"]=="NEUTRAL" and mom_r.get("strength") not in ["قوی","انفجاری 🚀"]:
            logger.info(f"⏭️ {sym} Neutral trend + weak momentum"); return None

        if init=="LONG" and rsi_r["value"]>CONFIG["RSI_OVERBOUGHT"]: confidence*=0.8
        if init=="SHORT" and rsi_r["value"]<CONFIG["RSI_OVERSOLD"]: confidence*=0.8

        if not self.corr.check(sym, init): confidence*=0.7

        if ms_r.get("bias")=="LONG" and init=="SHORT": confidence*=0.6
        elif ms_r.get("bias")=="SHORT" and init=="LONG": confidence*=0.6

        min_conf=CONFIG["MIN_CONFIDENCE_CONSECUTIVE"] if self.consecutive_losses>=CONFIG["CONSECUTIVE_LOSS_LIMIT"] else CONFIG["MIN_CONFIDENCE"]
        if confidence<min_conf: logger.info(f"⏭️ {sym} Conf {confidence:.0f}% < {min_conf}%"); return None

        price=await self.hub.price(sym)
        if not price: return None
        atr=MathUtils.atr([c.high for c in c5m],[c.low for c in c5m],[c.close for c in c5m])
        if atr==0: atr=price*0.002

        atr_pct=(atr/price)*100
        if atr_pct<CONFIG["MIN_ATR_PCT"]: logger.info(f"⏭️ {sym} ATR low ({atr_pct:.3f}%)"); return None

        if init=="LONG":
            sl=price-atr*CONFIG["ATR_MULTIPLIER_SL"]
            tp1=price+atr*1.3; tp2=price+atr*2.0; tp3=price+atr*3.5
        else:
            sl=price+atr*CONFIG["ATR_MULTIPLIER_SL"]
            tp1=price-atr*1.3; tp2=price-atr*2.0; tp3=price-atr*3.5

        rr=abs(tp3-price)/abs(price-sl) if price!=sl else 0
        ps=(CONFIG["BASE_CAPITAL"]*CONFIG["RISK_PER_TRADE"])/abs(price-sl) if price!=sl else 0

        sig=ScalpSignal(
            id=f"{sym}_{now.strftime('%H%M%S')}_{random.randint(100,999)}",
            symbol=sym,direction=init,entry_price=price,stop_loss=sl,
            take_profit=[tp1,tp2,tp3],confidence=confidence,
            star_rating=self._star_rating(confidence),
            momentum_strength=mom_r.get("strength","نامشخص"),
            whale_status=wh_r.get("details","نامشخص"),
            market_structure=ms_r.get("position","نامشخص"),
            risk_reward=round(rr,2),
            reason_summary=f"روند:{trend_result['trend']} | {mom_r.get('strength','')} | RSI:{rsi_r['value']:.0f}",
            timestamp=now,risk_percent=CONFIG["RISK_PER_TRADE"],position_size=ps,
            signal_type="STANDARD"
        )
        self.last_sig[sym]=now; self.daily_trades+=1
        logger.info(f"🎯 SIGNAL {sym} {init} Conf:{confidence:.0f}% {sig.star_rating}")
        return sig

    def report_result(self, pnl):
        if pnl<=0: self.consecutive_losses+=1
        else: self.consecutive_losses=0

# ============================================
# SMART TRADE MANAGER
# ============================================
class TradeManager:
    def __init__(self, hub, breaker):
        self.hub=hub; self.breaker=breaker
        self.active={}; self.closed=[]; self.sym_count={}
        self.max_dur=CONFIG["MAX_SCALP_MINUTES"]; self.ext_dur=CONFIG["EXTENDED_DURATION"]
        self.min_prog=CONFIG["MIN_PROGRESS_PCT"]

    def open(self, sig):
        if len(self.active)>=CONFIG["MAX_OPEN_TRADES"] or self.sym_count.get(sig.symbol,0)>=CONFIG["MAX_OPEN_PER_SYMBOL"]:
            return None
        t=ActiveTrade(
            signal_id=sig.id,symbol=sig.symbol,direction=sig.direction,
            entry_price=sig.entry_price,stop_loss=sig.stop_loss,
            tp1=sig.take_profit[0],tp2=sig.take_profit[1],tp3=sig.take_profit[2],
            position_size=sig.position_size,entry_time=sig.timestamp,
            remaining_size=sig.position_size
        )
        self.active[sig.id]=t; self.sym_count[sig.symbol]=self.sym_count.get(sig.symbol,0)+1
        return t

    async def monitor(self, cb=None):
        while True:
            await asyncio.sleep(5)
            closed=[]
            for tid,t in list(self.active.items()):
                try:
                    p=await self.hub.price(t.symbol)
                    if not p: continue
                    dur=(datetime.now(timezone.utc)-t.entry_time).total_seconds()/60
                    if t.direction=="LONG":
                        prog=(p-t.entry_price)/(t.tp1-t.entry_price) if t.tp1!=t.entry_price else 0
                        fav=p>t.entry_price
                    else:
                        prog=(t.entry_price-p)/(t.entry_price-t.tp1) if t.entry_price!=t.tp1 else 0
                        fav=p<t.entry_price
                    near=abs(p-t.entry_price)/t.entry_price<0.001
                    sc=False; reason=""
                    if (t.direction=="LONG" and p<=t.stop_loss) or (t.direction=="SHORT" and p>=t.stop_loss):
                        sc=True; reason="STOP_LOSS"
                    elif dur>=self.max_dur:
                        if prog>=self.min_prog and fav:
                            if dur<self.ext_dur: sc=False
                            else: sc=True; reason="TIME_EXIT_EXTENDED"
                        elif near:
                            if dur<self.ext_dur: sc=False
                            else: sc=True; reason="TIME_EXIT"
                        else: sc=True; reason="TIME_EXIT"
                    if sc and reason:
                        t.status=reason; t.exit_price=p
                        t.pnl_pct=(p-t.entry_price)/t.entry_price*100 if t.direction=="LONG" else (t.entry_price-p)/t.entry_price*100
                        closed.append(tid); continue
                    if t.tp2_hit:
                        c5=list(self.hub.candles[t.symbol]["5m"])
                        if len(c5)>=14:
                            atr=MathUtils.atr([c.high for c in c5],[c.low for c in c5],[c.close for c in c5])
                            if t.direction=="LONG":
                                ns=p-atr*CONFIG["TRAILING_ATR_MULTIPLIER"]
                                if ns>t.stop_loss: t.stop_loss=ns
                            else:
                                ns=p+atr*CONFIG["TRAILING_ATR_MULTIPLIER"]
                                if ns<t.stop_loss: t.stop_loss=ns
                    if not t.tp1_hit:
                        if (t.direction=="LONG" and p>=t.tp1) or (t.direction=="SHORT" and p<=t.tp1):
                            t.tp1_hit=True; t.remaining_size*=(1-CONFIG["TP1_SIZE"])
                            if cb: asyncio.create_task(cb(t,"TP1"))
                    elif not t.tp2_hit:
                        if (t.direction=="LONG" and p>=t.tp2) or (t.direction=="SHORT" and p<=t.tp2):
                            t.tp2_hit=True; t.remaining_size*=(1-CONFIG["TP2_SIZE"])
                            if cb: asyncio.create_task(cb(t,"TP2"))
                    elif t.tp2_hit:
                        if (t.direction=="LONG" and p>=t.tp3) or (t.direction=="SHORT" and p<=t.tp3):
                            t.status="TP3_FULL"; t.exit_price=p
                            t.pnl_pct=(p-t.entry_price)/t.entry_price*100 if t.direction=="LONG" else (t.entry_price-p)/t.entry_price*100
                            closed.append(tid)
                except Exception as e: logger.error(f"Monitor {tid}: {e}")
            for tid in closed:
                t=self.active.pop(tid); self.closed.append(t)
                self.sym_count[t.symbol]=max(0,self.sym_count.get(t.symbol,1)-1)
                self.breaker.add_pnl(t.pnl_pct)
                if cb: asyncio.create_task(cb(t,t.status))

# ============================================
# TELEGRAM BOT
# ============================================
try:
    from telegram import Bot, ReplyKeyboardMarkup
    from telegram.ext import Application, CommandHandler, MessageHandler, filters
    from telegram.constants import ParseMode
    TELE=True
except:
    TELE=False

if TELE:
    class TelegramBot:
        def __init__(self, token, channel, hub, tm, gen, breaker):
            self.bot=Bot(token=token); self.channel=channel; self.hub=hub
            self.tm=tm; self.gen=gen; self.breaker=breaker
            self.app=Application.builder().token(token).build()
            self.app.add_handler(CommandHandler("start", self._start))
            self.app.add_handler(CommandHandler("scalp", self._scalp))
            self.app.add_handler(CommandHandler("health", self._health))
            self.app.add_handler(CommandHandler("trades", self._trades))
            self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._btn))

        async def startup(self):
            try: await self.bot.send_message(self.channel,"🚀 *ربات V11 فعال شد*\n✅ 24/7 روی Render\n✅ بدون نیاز به پروکسی",parse_mode=ParseMode.MARKDOWN)
            except: pass

        async def _start(self, u, c):
            kb=[["⚡ تحلیل اسکالپ","📊 سلامت"],["📋 معاملات","📈 گزارش"]]
            await u.message.reply_text("🤖 *V11*",reply_markup=ReplyKeyboardMarkup(kb,resize_keyboard=True),parse_mode=ParseMode.MARKDOWN)

        async def _btn(self, u, c):
            t=u.message.text
            if "اسکالپ" in t: await self._scalp(u,c)
            elif "سلامت" in t: await self._health(u,c)
            elif "معاملات" in t: await self._trades(u,c)
            elif "گزارش" in t: await self._daily(u,c)

        async def _scalp(self, u, c):
            try:
                sym="SOLUSDT"; p=await self.hub.price(sym)
                if not p: await u.message.reply_text("⚠️ قیمت نیست"); return
                c5=list(self.hub.candles[sym]["5m"])
                if len(c5)<20: await u.message.reply_text("⏳ کندل"); return
                rsi=MathUtils.rsi([x.close for x in c5])
                atr=MathUtils.atr([x.high for x in c5],[x.low for x in c5],[x.close for x in c5])
                mom=MomentumBreakoutEngine.analyze(c5)
                ms=MarketStructureEngine.analyze(c5)
                trend=TrendFilterEngine(self.hub).analyze(sym)
                await u.message.reply_text(f"📊 {sym}\n💰 {p:.4f}\n📈 RSI:{rsi:.1f} ATR:{atr/p*100:.2f}%\n📈 روند:{trend['trend']}\n🚀 {mom.get('strength')}\n🏗 {ms.get('position')}",parse_mode=ParseMode.MARKDOWN)
            except: await u.message.reply_text("❌ خطا")

        async def _health(self, u, c):
            cb="⛔" if self.breaker.is_triggered else "✅"
            d=[t for t in self.tm.closed if t.entry_time.date()==datetime.now(timezone.utc).date()]
            pnl=sum(t.pnl_pct for t in d); wr=len([t for t in d if t.pnl_pct>0])/len(d)*100 if d else 0
            await u.message.reply_text(f"❤️ مدارشکن:{cb}\n📊 API:{self.hub.req}\n⚠️ خطا:{self.hub.err}\n📈 باز:{len(self.tm.active)}\n📋 امروز:{len(d)}\n✅ WR:{wr:.0f}%\n💰 PnL:{pnl:.2f}%",parse_mode=ParseMode.MARKDOWN)

        async def _trades(self, u, c):
            if not self.tm.active: await u.message.reply_text("هیچ"); return
            lines=[f"{t.symbol} {t.direction}" for t in self.tm.active.values()]
            await u.message.reply_text("\n".join(lines))

        async def _daily(self, u, c):
            d=[t for t in self.tm.closed if t.entry_time.date()==datetime.now(timezone.utc).date()]
            if not d: await u.message.reply_text("امروز خبری نیست"); return
            w=[t for t in d if t.pnl_pct>0]; l=[t for t in d if t.pnl_pct<=0]
            await u.message.reply_text(f"📆 امروز\n📊 {len(d)}\n✅ {len(w)}\n❌ {len(l)}\n🎯 WR:{len(w)/len(d)*100:.0f}%\n💰 {sum(t.pnl_pct for t in d):.2f}%",parse_mode=ParseMode.MARKDOWN)

        async def send_signal(self, sig):
            em="🟢" if sig.direction=="LONG" else "🔴"
            msg=(f"{em} *{sig.symbol} [{sig.signal_type}]*\n{'─'*30}\n⭐ {sig.star_rating} ({sig.confidence:.0f}%)\n"
                 f"🎯 {sig.entry_price:.4f}\n🛑 SL {sig.stop_loss:.4f}\n✅ TP1 {sig.take_profit[0]:.4f}\n"
                 f"✅ TP2 {sig.take_profit[1]:.4f}\n✅ TP3 {sig.take_profit[2]:.4f}\n"
                 f"🚀 {sig.momentum_strength} | 🐋 {sig.whale_status}\n🏗 {sig.market_structure}\n📝 {sig.reason_summary}")
            try: await self.bot.send_message(self.channel,text=msg,parse_mode=ParseMode.MARKDOWN)
            except: pass

        async def trade_update(self, trade, event):
            em={"TP1":"✅","TP2":"✅✅","TP3_FULL":"🏁","STOP_LOSS":"🛑","TIME_EXIT":"⏰","TIME_EXIT_EXTENDED":"⏰🔄"}.get(event,"📊")
            try: await self.bot.send_message(self.channel,text=f"{em} {event} {trade.symbol} | PnL {trade.pnl_pct:+.2f}%")
            except: pass

        async def start_polling(self): await self.app.initialize(); await self.app.start(); await self.app.updater.start_polling()
else:
    class TelegramBot:
        def __init__(self,*a,**kw): pass
        async def startup(self): pass
        async def send_signal(self,*a): pass
        async def trade_update(self,*a): pass
        async def start_polling(self): pass

# ============================================
# HEALTH CHECK WEB SERVER (for UptimeRobot)
# ============================================
async def health_check(request):
    return web.Response(text="OK")

async def run_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', CONFIG["WEB_SERVER_PORT"])
    await site.start()
    logger.info(f"✅ Health check server started on port {CONFIG['WEB_SERVER_PORT']}")

# ============================================
# MAIN BOT
# ============================================
class HassanBot:
    def __init__(self):
        self.hub=MarketDataHub()
        self.breaker=CircuitBreaker()
        self.gen=UltimateSignalGenerator(self.hub, self.breaker)
        self.tm=TradeManager(self.hub, self.breaker)
        self.tele=TelegramBot(CONFIG["TELEGRAM_BOT_TOKEN"],CONFIG["SIGNAL_CHANNEL"],self.hub,self.tm,self.gen,self.breaker)

    async def init_data(self):
        logger.info("📊 Loading data...")
        for sym in CONFIG["SYMBOLS"]:
            for tf in CONFIG["TIMEFRAMES"]:
                try:
                    c=await self.hub.klines(sym,tf,CONFIG["CANDLE_LIMITS"][tf])
                    if c: self.hub.candles[sym][tf].extend(c)
                except: pass
        logger.info("✅ Ready")

    async def run_engine(self):
        locks={s:asyncio.Lock() for s in CONFIG["SYMBOLS"]}
        async def analyze(sym):
            async with locks[sym]:
                try:
                    if self.breaker.is_triggered: return
                    sig=await self.gen.generate(sym)
                    if sig:
                        t=self.tm.open(sig)
                        if t: await self.tele.send_signal(sig)
                except: pass
        async def ws(sym,tf):
            async for _ in self.hub.ws_klines(sym,tf):
                if tf=="1m" and not locks[sym].locked(): asyncio.create_task(analyze(sym))
        async def rest():
            while True:
                for sym in CONFIG["SYMBOLS"]:
                    try: await asyncio.gather(self.hub.orderbook(sym),self.hub.agg_trades(sym),return_exceptions=True)
                    except: pass
                await asyncio.sleep(1)
        async def periodic():
            while True:
                for sym in CONFIG["SYMBOLS"]:
                    if not locks[sym].locked(): asyncio.create_task(analyze(sym))
                await asyncio.sleep(10)
        async def reset():
            while True: self.breaker.reset_if_new_day(); await asyncio.sleep(60)
        await asyncio.gather(*[asyncio.create_task(ws(s,tf)) for s in CONFIG["SYMBOLS"] for tf in CONFIG["TIMEFRAMES"]],
                           asyncio.create_task(rest()),asyncio.create_task(periodic()),asyncio.create_task(reset()),
                           asyncio.create_task(self.tm.monitor(self.tele.trade_update)))

    async def start(self):
        logger.info("🥇 V11 Starting – Render 24/7")
        # Start health check web server
        asyncio.create_task(run_web_server())
        await self.init_data()
        await self.tele.start_polling()
        await self.tele.startup()
        await self.run_engine()

async def main():
    await HassanBot().start()

if __name__=="__main__":
    asyncio.run(main())
