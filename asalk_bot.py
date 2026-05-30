#!/usr/bin/env python3
# ============================================
# ultimate_bybit_bot_OKX_RENDER.py
# ALL original engines + OKX API + REST-only + Web server
# ============================================

import asyncio, json, time, ssl, logging, os, sys, random, socket
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Deque, Any, Tuple
from dataclasses import dataclass, field
from collections import deque, Counter
import aiohttp
from aiohttp import web
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
logger = logging.getLogger('BybitBot')
logger.handlers.clear()
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColoredFormatter())
logger.addHandler(console_handler)
file_handler = RotatingFileHandler('bybit_bot.log', maxBytes=5*1024*1024, backupCount=3, encoding='utf-8')
file_handler.setFormatter(logging.Formatter('[%(asctime)s] %(levelname)s: %(message)s'))
logger.addHandler(file_handler)

# ---------- CONFIG ----------
CONFIG = {
    "TELEGRAM_BOT_TOKEN": "8591890952:AAFXFwpVpFSj0W_7ucxqPQl26LCxCNnjFpk",
    "SIGNAL_CHANNEL": "-1003741859681",
    "OKX_API_URL": "https://www.okx.com",
    "SYMBOLS": ["SOL-USDT-SWAP"],
    "TIMEFRAMES": {"scalp": ["1m", "5m", "15m"], "midterm": ["1H"]},
    "CANDLE_LIMITS": {"1m": 200, "5m": 200, "15m": 150, "1H": 200},
    "WHALE_MIN_VOLUME_USD": 15000,
    "WHALE_MIN_TRADES": 3,
    "WHALE_WINDOW_MINUTES": 10,
    "ORDERBOOK_IMBALANCE_THRESHOLD": 0.6,
    "ORDERBOOK_DEPTH_LIMIT": 20,
    "PATTERN_STRENGTH_THRESHOLD": 60,
    "ENGULFING_BODY_RATIO_MIN": 0.8,
    "DOJI_BODY_RATIO_MAX": 0.15,
    "HAMMER_SHADOW_RATIO_MIN": 2.0,
    "STRUCTURE_SWING_PERIOD": 5,
    "BOS_CANDLE_BODY_RATIO": 0.6,
    "LIQUIDITY_SWEEP_VOLUME_SPIKE": 1.2,
    "LIQUIDITY_SWEEP_WICK_RATIO": 0.7,
    "ATR_TREND_THRESHOLD": 0.8,
    "VOLATILITY_RANGING_MAX": 1.5,
    "MIN_CONFIDENCE": 45,
    "SCORE_WEIGHTS": {"trend": 22, "volume": 12, "whale": 18, "pattern": 14,
                      "liquidity": 10, "structure": 12, "regime": 8, "orderbook": 4,
                      "temperature": 5, "delta": 10},
    "RISK_PER_TRADE": 0.01,
    "DYNAMIC_RISK_MIN": 0.005,
    "DYNAMIC_RISK_MAX": 0.02,
    "BASE_CAPITAL": 1000,
    "MAX_DAILY_SIGNALS": 15,
    "MIN_RR_RATIO": 1.3,
    "MAX_RISK_RATIO": 0.015,
    "MIN_LIQUIDITY": 750000,
    "MAX_SPREAD_PCT": 0.25,
    "MAX_OPEN_TRADES": 2,
    "DAILY_LOSS_LIMIT": 1.5,
    "MAX_DRAWDOWN_PCT": 3.0,
    "MAX_CONSECUTIVE_LOSSES": 5,
    "DEFENSIVE_MODE_RISK": 0.005,
    "TP1_PERCENT": 0.4, "TP2_PERCENT": 0.3, "TP3_PERCENT": 0.3,
    "TRADE_LIFECYCLE_CHECK_INTERVAL": 1,
    "MAX_RETRIES": 3, "RETRY_DELAY": 2.5,
    "CONNECTION_TIMEOUT": 30, "REQUEST_TIMEOUT": 15,
    "MAX_CONCURRENT_REQUESTS": 2, "REQUEST_DELAY": 0.3, "API_RATE_LIMIT": 800,
    "ANALYSIS_INTERVAL": 60,
    "SIGNAL_COOLDOWN": 300,
    "SYMBOL_COOLDOWN": 45,
    "HEALTH_CHECK_INTERVAL": 30,
    "SHARP_MOVE_PCT_THRESHOLD": 0.5,
    "REST_UPDATE_INTERVAL": 10,
    "WEB_SERVER_PORT": 10000,
}

# ---------- DATA MODELS ----------
@dataclass
class Candle:
    timestamp: datetime; open: float; high: float; low: float; close: float
    volume: float; symbol: str; timeframe: str
    trades: int = 0; is_closed: bool = True

    def __post_init__(self):
        if self.timestamp.tzinfo is None:
            self.timestamp = self.timestamp.replace(tzinfo=timezone.utc)
        if self.high < self.low:
            self.high, self.low = self.low, self.high
        self.open = max(self.low, min(self.open, self.high))
        self.close = max(self.low, min(self.close, self.high))
        self.volume = max(0, self.volume)

    @property
    def body_size(self) -> float: return abs(self.close - self.open)
    @property
    def total_range(self) -> float: return self.high - self.low
    @property
    def body_ratio(self) -> float:
        rng = self.total_range
        return self.body_size / rng if rng != 0 else 0.0
    @property
    def is_bullish(self) -> bool: return self.close > self.open
    @property
    def is_bearish(self) -> bool: return self.close < self.open
    @property
    def upper_shadow(self) -> float: return self.high - max(self.open, self.close)
    @property
    def lower_shadow(self) -> float: return min(self.open, self.close) - self.low


@dataclass
class Signal:
    id: str; symbol: str; direction: str; entry_price: float; stop_loss: float
    take_profit: List[float]; confidence: float; timeframe: str; reason: str
    whale_volume: float; timestamp: datetime
    risk_per_trade: float = CONFIG["RISK_PER_TRADE"]; position_size: float = 0.0
    trade_type: str = "MID_TERM"; leverage: int = 10
    regime: str = ""; score_breakdown: Dict[str, float] = field(default_factory=dict)


@dataclass
class Trade:
    id: str; symbol: str; direction: str; entry_price: float; stop_loss: float
    tp1: float; tp2: float; tp3: float
    position_size: float = 0.0; risk_per_trade: float = CONFIG["RISK_PER_TRADE"]
    confidence: float = 0.0
    open_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: str = "OPEN"; tp1_hit: bool = False; tp2_hit: bool = False; tp3_hit: bool = False
    exit_price: float = 0.0; pnl_pct: float = 0.0
    trade_type: str = "MID_TERM"; leverage: int = 10
    trailing_stop: float = 0.0
    peak_price: float = 0.0


# ---------- MATH UTILS ----------
class MathUtils:
    @staticmethod
    def mean(v): return sum(v) / len(v) if v else 0.0
    @staticmethod
    def std(v):
        if len(v) < 2: return 0.0
        m = MathUtils.mean(v)
        return (sum((x - m) ** 2 for x in v) / (len(v) - 1)) ** 0.5
    @staticmethod
    def ema(v, p):
        if len(v) < p: return []
        k = 2 / (p + 1)
        e = [v[0]]
        for i in range(1, len(v)):
            e.append(v[i] * k + e[-1] * (1 - k))
        return e
    @staticmethod
    def rsi(prices, period=14):
        if len(prices) < period + 1: return 50.0
        deltas = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        gains = [d if d > 0 else 0 for d in deltas]
        losses = [-d if d < 0 else 0 for d in deltas]
        ag = MathUtils.mean(gains[:period])
        al = MathUtils.mean(losses[:period])
        if al == 0: return 100.0
        return 100 - (100 / (1 + ag / al))
    @staticmethod
    def atr(highs, lows, closes, period=14):
        if len(highs) < period: return 0.0
        tr = []
        for i in range(1, len(highs)):
            tr.append(max(highs[i] - lows[i],
                          abs(highs[i] - closes[i - 1]),
                          abs(lows[i] - closes[i - 1])))
        return MathUtils.mean(tr[-period:])
    @staticmethod
    def correlation(x, y):
        if len(x) != len(y) or len(x) < 2: return 0.0
        mx = MathUtils.mean(x); my = MathUtils.mean(y)
        num = sum((x[i] - mx) * (y[i] - my) for i in range(len(x)))
        den_x = sum((xi - mx) ** 2 for xi in x)
        den_y = sum((yi - my) ** 2 for yi in y)
        if den_x == 0 or den_y == 0: return 0.0
        return num / ((den_x * den_y) ** 0.5)


# ---------- RATE LIMITER ----------
class RateLimiter:
    def __init__(self):
        self.sem = asyncio.Semaphore(CONFIG["MAX_CONCURRENT_REQUESTS"])
        self.lock = asyncio.Lock()
        self.last = 0.0
        self.cnt = 0

    async def acquire(self):
        await self.sem.acquire()
        async with self.lock:
            now = time.time()
            if self.last and now - self.last < CONFIG["REQUEST_DELAY"]:
                await asyncio.sleep(CONFIG["REQUEST_DELAY"] - (now - self.last))
            self.last = time.time()
            self.cnt += 1

    def release(self):
        self.sem.release()


def rate_limited(func):
    async def wrapper(self, *args, **kwargs):
        await self.rate_limiter.acquire()
        try:
            return await func(self, *args, **kwargs)
        finally:
            self.rate_limiter.release()
    return wrapper


# ---------- MARKET DATA HUB (OKX REST only) ----------
class MarketDataHub:
    def __init__(self):
        self.base = CONFIG["OKX_API_URL"]
        self.sess = None
        self.ssl = ssl.create_default_context()
        self.rate_limiter = RateLimiter()
        self.symbols = CONFIG["SYMBOLS"]
        self.candle_buffers: Dict[str, Dict[str, Deque[Candle]]] = {
            sym: {tf: deque(maxlen=CONFIG["CANDLE_LIMITS"][tf]) for tf in CONFIG["CANDLE_LIMITS"]}
            for sym in self.symbols
        }
        self.orderbook_snapshots: Dict[str, Dict] = {sym: {"bids": [], "asks": []} for sym in self.symbols}
        self.trade_buffers: Dict[str, Deque[Dict]] = {sym: deque(maxlen=1500) for sym in self.symbols}
        self.price_history: Dict[str, Deque[float]] = {sym: deque(maxlen=100) for sym in self.symbols}
        self.request_count = 0
        self.error_count = 0
        self.open_interest: Dict[str, float] = {}
        self.funding_rate: Dict[str, float] = {}

    async def get_session(self):
        if self.sess is None or self.sess.closed:
            timeout = aiohttp.ClientTimeout(total=CONFIG["CONNECTION_TIMEOUT"], connect=10,
                                            sock_read=CONFIG["REQUEST_TIMEOUT"])
            connector = aiohttp.TCPConnector(family=socket.AF_INET, ttl_dns_cache=300, force_close=True)
            self.sess = aiohttp.ClientSession(timeout=timeout, connector=connector)
        return self.sess

    @rate_limited
    async def fetch(self, endpoint, params=None):
        url = f"{self.base}{endpoint}"
        session = await self.get_session()
        for attempt in range(CONFIG["MAX_RETRIES"]):
            try:
                self.request_count += 1
                async with session.get(url, params=params, ssl=self.ssl) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 429:
                        await asyncio.sleep(int(resp.headers.get('Retry-After', 5)))
                    elif resp.status >= 500:
                        await asyncio.sleep(CONFIG["RETRY_DELAY"] * (attempt + 1))
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logger.warning(f"Request error {endpoint}: {e}")
                self.error_count += 1
                if attempt < CONFIG["MAX_RETRIES"] - 1:
                    await asyncio.sleep(CONFIG["RETRY_DELAY"] * (attempt + 1))
        return None

    async def update_klines(self, symbol, interval):
        # OKX uses bar format: 1m, 5m, 15m, 1H
        bar = interval.replace("h", "H")
        data = await self.fetch(f"/api/v5/market/candles?instId={symbol}&bar={bar}&limit={CONFIG['CANDLE_LIMITS'][interval]}")
        if data and data.get("code") == "0":
            candles = []
            for k in reversed(data["data"]):
                try:
                    candles.append(Candle(
                        timestamp=datetime.fromtimestamp(int(k[0]) / 1000, tz=timezone.utc),
                        open=float(k[1]), high=float(k[2]), low=float(k[3]), close=float(k[4]),
                        volume=float(k[5]), symbol=symbol, timeframe=interval
                    ))
                except: pass
            if candles:
                self.candle_buffers[symbol][interval] = deque(candles, maxlen=CONFIG["CANDLE_LIMITS"][interval])
            if interval == "1H" and candles:
                self.price_history[symbol].append(candles[-1].close)

    async def update_all_klines(self, symbol):
        tasks = [self.update_klines(symbol, tf) for tf in CONFIG["CANDLE_LIMITS"]]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def get_ticker_24hr(self, symbol):
        data = await self.fetch(f"/api/v5/market/ticker?instId={symbol}")
        if data and data.get("code") == "0" and data.get("data"):
            ticker = data["data"][0]
            return {
                "volume": float(ticker.get("vol24h", 0)),
                "lastPrice": float(ticker.get("last", 0)),
                "high": float(ticker.get("high24h", 0)),
                "low": float(ticker.get("low24h", 0)),
                "quoteVolume": float(ticker.get("volCcy24h", 0)),
                "priceChangePercent": 0
            }
        return {}

    async def get_order_book(self, symbol):
        data = await self.fetch(f"/api/v5/market/books?instId={symbol}&sz=50")
        if data and data.get("code") == "0" and data.get("data"):
            book = data["data"][0]
            bids = [[float(p), float(q)] for p, q, *_ in book.get("bids", [])]
            asks = [[float(p), float(q)] for p, q, *_ in book.get("asks", [])]
            self.orderbook_snapshots[symbol] = {"bids": bids, "asks": asks}

    async def get_agg_trades(self, symbol):
        data = await self.fetch(f"/api/v5/market/trades?instId={symbol}&limit=100")
        if data and data.get("code") == "0":
            for t in reversed(data["data"]):
                side = t.get("side", "")
                self.trade_buffers[symbol].append({
                    "price": float(t["px"]), "qty": float(t["sz"]),
                    "time": datetime.fromtimestamp(int(t["ts"]) / 1000, tz=timezone.utc),
                    "is_buyer_maker": side == "buy"
                })

    async def get_open_interest(self, symbol):
        # OKX swap OI
        data = await self.fetch(f"/api/v5/public/open-interest?instId={symbol}")
        if data and data.get("code") == "0" and data.get("data"):
            self.open_interest[symbol] = float(data["data"][0].get("oi", 0))
            return float(data["data"][0].get("oi", 0))
        return None

    async def get_funding_rate(self, symbol):
        data = await self.fetch(f"/api/v5/public/funding-rate?instId={symbol}")
        if data and data.get("code") == "0" and data.get("data"):
            self.funding_rate[symbol] = float(data["data"][0].get("fundingRate", 0))
            return float(data["data"][0].get("fundingRate", 0))
        return None

    async def get_current_price(self, symbol):
        data = await self.fetch(f"/api/v5/market/ticker?instId={symbol}")
        if data and data.get("code") == "0" and data.get("data"):
            return float(data["data"][0]["last"])
        return None

    async def get_best_bid_ask(self, symbol):
        data = await self.fetch(f"/api/v5/market/ticker?instId={symbol}")
        if data and data.get("code") == "0" and data.get("data"):
            ticker = data["data"][0]
            bid = float(ticker.get("bidPx", 0))
            ask = float(ticker.get("askPx", 0))
            spread = (ask - bid) / bid * 100 if bid > 0 else 0.0
            return {"bid": bid, "ask": ask, "spread": spread}
        return {"bid": 0, "ask": 0, "spread": 0}


# ---------- ENGINES (all original) ----------
class WhaleIntelligenceEngine:
    def __init__(self, dm: MarketDataHub): self.dm = dm; self.m = MathUtils()
    async def detect(self, symbol) -> Dict[str, Any]:
        trades = list(self.dm.trade_buffers.get(symbol, []))
        if not trades: return {"detected": False, "score": 0, "direction": None}
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=CONFIG["WHALE_WINDOW_MINUTES"])
        large = [t for t in trades if t["time"] >= cutoff and t["price"] * t["qty"] >= CONFIG["WHALE_MIN_VOLUME_USD"]]
        present = len(large) >= CONFIG["WHALE_MIN_TRADES"]
        score = 0; direction = None
        if present:
            total = sum(t["price"] * t["qty"] for t in large)
            buy_vol = sum(t["price"] * t["qty"] for t in large if not t["is_buyer_maker"])
            sell_vol = total - buy_vol
            direction = "BUY" if buy_vol > sell_vol * 1.5 else "SELL" if sell_vol > buy_vol * 1.5 else None
            score = min(len(large) * 20, 100)
        return {"detected": present, "score": score, "direction": direction}

class OrderBookEngine:
    def __init__(self, dm: MarketDataHub): self.dm = dm
    def get_imbalance(self, symbol):
        ob = self.dm.orderbook_snapshots.get(symbol, {"bids": [], "asks": []})
        bids = ob.get("bids", []); asks = ob.get("asks", [])
        bid_liq = sum(q for _, q in bids[:CONFIG["ORDERBOOK_DEPTH_LIMIT"]])
        ask_liq = sum(q for _, q in asks[:CONFIG["ORDERBOOK_DEPTH_LIMIT"]])
        total = bid_liq + ask_liq
        if total == 0: return {"direction": "NEUTRAL", "score": 0}
        ratio = bid_liq / total
        direction = "BUY" if ratio > 0.5 else "SELL"
        score = abs(ratio - 0.5) * 200
        return {"direction": direction, "score": min(score, 100)}

class MarketStructureEngine:
    def analyze(self, candles: List[Candle]) -> Dict[str, Any]:
        if len(candles) < CONFIG["STRUCTURE_SWING_PERIOD"] * 2 + 1:
            return {"trend": "NEUTRAL", "bos": None}
        highs = [c.high for c in candles]; lows = [c.low for c in candles]
        swings = []; period = CONFIG["STRUCTURE_SWING_PERIOD"]
        for i in range(period, len(highs) - period):
            if highs[i] == max(highs[i - period:i + period + 1]): swings.append({"type": "HIGH", "price": highs[i]})
            if lows[i] == min(lows[i - period:i + period + 1]): swings.append({"type": "LOW", "price": lows[i]})
        if len(swings) < 2: return {"trend": "NEUTRAL", "bos": None}
        hh = sum(1 for i in range(1, len(swings)) if swings[i]["type"] == "HIGH" and swings[i]["price"] > swings[i - 1]["price"])
        lh = sum(1 for i in range(1, len(swings)) if swings[i]["type"] == "LOW" and swings[i]["price"] > swings[i - 1]["price"])
        trend = "BULLISH" if hh > 0 and lh > 0 else "BEARISH" if hh == 0 and lh == 0 else "NEUTRAL"
        bos = None
        if trend == "BULLISH":
            ph = [s["price"] for s in swings if s["type"] == "HIGH"]
            if ph and candles[-1].close > max(ph[:-1]) and candles[-1].body_ratio > CONFIG["BOS_CANDLE_BODY_RATIO"]:
                bos = {"direction": "BULLISH"}
        elif trend == "BEARISH":
            pl = [s["price"] for s in swings if s["type"] == "LOW"]
            if pl and candles[-1].close < min(pl[:-1]) and candles[-1].body_ratio > CONFIG["BOS_CANDLE_BODY_RATIO"]:
                bos = {"direction": "BEARISH"}
        return {"trend": trend, "bos": bos}

class CandlePatternDetector:
    def __init__(self): self.m = MathUtils()
    def detect(self, candles: List[Candle]) -> Dict[str, Any]:
        if len(candles) < 2: return {}
        c1, c2 = candles[-2], candles[-1]; patterns = {}
        if c2.body_size > c1.body_size:
            if c1.is_bearish and c2.is_bullish and c2.close > c1.open and c2.open < c1.close:
                patterns["engulfing"] = {"direction": "BUY", "score": 70}
            elif c1.is_bullish and c2.is_bearish and c2.close < c1.open and c2.open > c1.close:
                patterns["engulfing"] = {"direction": "SELL", "score": 70}
        if c2.lower_shadow > c2.body_size * CONFIG["HAMMER_SHADOW_RATIO_MIN"] and c2.upper_shadow < c2.body_size * 0.5:
            patterns["hammer"] = {"direction": "BUY", "score": 70}
        if c2.upper_shadow > c2.body_size * CONFIG["HAMMER_SHADOW_RATIO_MIN"] and c2.lower_shadow < c2.body_size * 0.5:
            patterns["hammer"] = {"direction": "SELL", "score": 70}
        if c2.body_ratio < CONFIG["DOJI_BODY_RATIO_MAX"]: patterns["doji"] = {"direction": "NEUTRAL", "score": 40}
        return patterns

class LiquidityEngine:
    def __init__(self, dm: MarketDataHub): self.dm = dm; self.m = MathUtils()
    async def analyze(self, symbol) -> Dict[str, Any]:
        candles = list(self.dm.candle_buffers.get(symbol, {}).get("5m", []))
        if len(candles) < 10: return {"sweep": False}
        recent_high = max(c.high for c in candles[:-2]); recent_low = min(c.low for c in candles[:-2])
        last = candles[-1]
        prev_volumes = [c.volume for c in candles[-20:-1]]
        vol_spike = False
        if prev_volumes:
            avg_vol = self.m.mean(prev_volumes)
            vol_spike = last.volume > avg_vol * CONFIG["LIQUIDITY_SWEEP_VOLUME_SPIKE"]
        total_range = last.total_range
        if total_range == 0: return {"sweep": False}
        if last.high > recent_high and last.close < recent_high and last.upper_shadow / total_range > CONFIG["LIQUIDITY_SWEEP_WICK_RATIO"] and vol_spike:
            return {"sweep": True, "direction": "BEARISH"}
        if last.low < recent_low and last.close > recent_low and last.lower_shadow / total_range > CONFIG["LIQUIDITY_SWEEP_WICK_RATIO"] and vol_spike:
            return {"sweep": True, "direction": "BULLISH"}
        return {"sweep": False}

class MarketRegimeDetector:
    def detect(self, candles: List[Candle]) -> str:
        if len(candles) < 20: return "RANGING"
        closes = [c.close for c in candles]
        atr = MathUtils.atr([c.high for c in candles], [c.low for c in candles], closes)
        ema20 = MathUtils.ema(closes, 20); ema50 = MathUtils.ema(closes, 50)
        if not ema20 or not ema50: return "RANGING"
        if atr / closes[-1] * 100 > CONFIG["ATR_TREND_THRESHOLD"] and abs(ema20[-1] - ema50[-1]) / ema50[-1] > 0.01:
            return "TRENDING"
        return "RANGING"

class MultiTimeframeAnalyzer:
    def __init__(self, dm: MarketDataHub): self.dm = dm; self.m = MathUtils()
    async def analyze(self, symbol: str) -> Dict[str, Any]:
        c1h = list(self.dm.candle_buffers.get(symbol, {}).get("1H", []))
        c15m = list(self.dm.candle_buffers.get(symbol, {}).get("15m", []))
        if len(c1h) < 20 or len(c15m) < 20: return {}
        def trend_of(c):
            cl = [x.close for x in c]
            e20 = self.m.ema(cl, 20); e50 = self.m.ema(cl, 50)
            if not e20 or not e50: return "BEARISH"
            return "BULLISH" if e20[-1] > e50[-1] else "BEARISH"
        t1h = trend_of(c1h); t15m = trend_of(c15m)
        strength = self._strength(c1h)
        return {"trend_1h": t1h, "aligned": t1h == t15m, "strength": strength}

    def _strength(self, candles):
        if len(candles) < 20: return 0.0
        closes = [c.close for c in candles]; rsi = MathUtils.rsi(closes)
        atr = MathUtils.atr([c.high for c in candles], [c.low for c in candles], closes)
        ema20 = MathUtils.ema(closes, 20)
        if not ema20 or len(ema20) < 5: return 0.0
        slope = (ema20[-1] - ema20[-5]) / ema20[-5] * 100 if ema20[-5] != 0 else 0.0
        score = 0
        if rsi: score += max(0, 30 - abs(rsi - 50))
        score += min(abs(slope) * 5, 30)
        if atr and closes: score += min(atr / closes[-1] * 100 * 5, 40)
        return min(score, 100)


class MarketTemperatureEngine:
    def __init__(self, dm: MarketDataHub): self.dm = dm; self.m = MathUtils()
    async def get_temperature(self, symbol: str, timeframe="5m") -> float:
        candles = list(self.dm.candle_buffers[symbol].get(timeframe, []))
        if len(candles) < 5: return 30.0
        closes = [c.close for c in candles]; highs = [c.high for c in candles]; lows = [c.low for c in candles]
        price = closes[-1]
        volumes = [c.volume for c in candles[-5:]]
        avg_vol = self.m.mean(volumes) if volumes else 1
        vol_ratio = candles[-1].volume / avg_vol if avg_vol > 0 else 1.0
        vol_score = min(100, max(0, (vol_ratio - 0.5) / 2.0 * 100))
        atr = MathUtils.atr(highs, lows, closes)
        atr_pct = (atr / price) * 100 if price > 0 else 0
        vola_score = min(100, max(0, (atr_pct - 0.05) / 0.45 * 100))
        temperature = 0.6 * vol_score + 0.4 * vola_score
        return round(temperature, 1)


class CumulativeVolumeDelta:
    def __init__(self, dm: MarketDataHub):
        self.dm = dm
        self.cvd_history: Dict[str, Deque[Tuple[datetime, float]]] = {}

    def update(self, symbol: str):
        trades = list(self.dm.trade_buffers.get(symbol, []))
        if not trades: return
        now = datetime.now(timezone.utc)
        recent = [t for t in trades if t["time"] >= now - timedelta(minutes=5)]
        if not recent: return
        delta = sum((t["qty"] if not t["is_buyer_maker"] else -t["qty"]) for t in recent)
        if symbol not in self.cvd_history:
            self.cvd_history[symbol] = deque(maxlen=100)
        if self.cvd_history[symbol]:
            delta += self.cvd_history[symbol][-1][1]
        self.cvd_history[symbol].append((now, delta))

    def get_divergence(self, symbol: str) -> Optional[str]:
        candles_5m = list(self.dm.candle_buffers[symbol].get("5m", []))
        if len(candles_5m) < 10 or symbol not in self.cvd_history or len(self.cvd_history[symbol]) < 3:
            return None
        recent_price = [c.close for c in candles_5m[-3:]]
        recent_cvd = [v for _, v in list(self.cvd_history[symbol])[-3:]]
        if recent_price[-1] > recent_price[0] and recent_cvd[-1] < recent_cvd[0]:
            return "BEARISH"
        if recent_price[-1] < recent_price[0] and recent_cvd[-1] > recent_cvd[0]:
            return "BULLISH"
        return None

class VolumeProfileEngine:
    def __init__(self, dm: MarketDataHub): self.dm = dm
    def get_poc(self, symbol: str, lookback_bars=50) -> Optional[float]:
        candles = list(self.dm.candle_buffers[symbol].get("5m", []))
        if len(candles) < lookback_bars: return None
        profile = Counter()
        for c in candles[-lookback_bars:]:
            bucket = round(c.close, 2)
            profile[bucket] += c.volume
        if not profile: return None
        return profile.most_common(1)[0][0]

class SharpMoveDetector:
    def __init__(self, dm: MarketDataHub):
        self.dm = dm; self.m = MathUtils()
        self.last_alert = datetime.min.replace(tzinfo=timezone.utc)
        self.alert_cooldown = 60

    def detect(self, symbol: str) -> Optional[Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        if (now - self.last_alert).total_seconds() < self.alert_cooldown: return None
        candles_1m = list(self.dm.candle_buffers[symbol].get("1m", []))
        if len(candles_1m) >= 2:
            last_1m = candles_1m[-1]; prev_1m = candles_1m[-2]
            move_pct = abs(last_1m.close - prev_1m.close) / prev_1m.close * 100
            if move_pct >= CONFIG["SHARP_MOVE_PCT_THRESHOLD"]:
                direction = "UP" if last_1m.close > prev_1m.close else "DOWN"
                avg_vol = self.m.mean([c.volume for c in candles_1m[-10:-1]]) if len(candles_1m) >= 10 else last_1m.volume
                vol_spike = last_1m.volume > avg_vol * 2.0
                self.last_alert = now
                return {"detected": True, "direction": direction, "price": last_1m.close,
                        "move_pct": round(move_pct, 2), "volume_spike": vol_spike, "timestamp": now}
        return None

class OpenInterestEngine:
    def __init__(self, dm: MarketDataHub):
        self.dm = dm
        self.oi_history: Dict[str, Deque[Tuple[datetime, float]]] = {}

    async def update(self, symbol: str):
        oi = await self.dm.get_open_interest(symbol)
        fr = await self.dm.get_funding_rate(symbol)
        if oi:
            if symbol not in self.oi_history:
                self.oi_history[symbol] = deque(maxlen=50)
            self.oi_history[symbol].append((datetime.now(timezone.utc), oi))

    def get_health(self, symbol: str) -> Dict[str, Any]:
        if symbol not in self.oi_history or len(self.oi_history[symbol]) < 5:
            return {"oi_trend": "NEUTRAL", "funding": 0}
        recent_oi = [v for _, v in list(self.oi_history[symbol])[-5:]]
        oi_trend = "UP" if recent_oi[-1] > recent_oi[0] else "DOWN"
        fr = self.dm.funding_rate.get(symbol, 0)
        return {"oi_trend": oi_trend, "funding": fr}


class OrderflowDeltaEngine:
    def __init__(self, dm: MarketDataHub): self.dm = dm
    def get_delta(self, symbol: str, minutes=3) -> float:
        trades = list(self.dm.trade_buffers.get(symbol, []))
        if not trades: return 0.0
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        recent = [t for t in trades if t["time"] >= cutoff]
        if not recent: return 0.0
        buy_vol = sum(t["qty"] for t in recent if not t["is_buyer_maker"])
        sell_vol = sum(t["qty"] for t in recent if t["is_buyer_maker"])
        return buy_vol - sell_vol

    def get_delta_score(self, symbol: str, direction: str) -> float:
        delta = self.get_delta(symbol)
        if delta > 0 and direction == "BUY": return 10.0
        elif delta < 0 and direction == "SELL": return 10.0
        elif delta > 0 and direction == "SELL": return -5.0
        elif delta < 0 and direction == "BUY": return -5.0
        return 0.0


class MarketContextEngine:
    def __init__(self, dm: MarketDataHub):
        self.dm = dm; self.recent_trades: List[Dict] = []
        self.last_fng_update = datetime.min.replace(tzinfo=timezone.utc)
        self.cached_fng = 50
        self.economic_events = [
            ("2026-06-17", "18:00", "FOMC", 120), ("2026-07-29", "18:00", "FOMC", 120),
            ("2026-09-16", "18:00", "FOMC", 120), ("2026-06-10", "12:30", "CPI", 90),
            ("2026-07-15", "12:30", "CPI", 90), ("2026-06-05", "12:30", "NFP", 90),
        ]

    async def get_fear_greed_index(self) -> int:
        now = datetime.now(timezone.utc)
        if (now - self.last_fng_update).total_seconds() < 3600: return self.cached_fng
        try:
            async with aiohttp.ClientSession() as sess:
                async with sess.get("https://api.alternative.me/fng/", timeout=10) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self.cached_fng = int(data["data"][0]["value"])
                        self.last_fng_update = now
        except: pass
        return self.cached_fng

    def get_session(self) -> str:
        now = datetime.now(timezone.utc); hour = now.hour
        if 8 <= hour < 17: return "LONDON"
        elif 13 <= hour < 22: return "NEW_YORK"
        else: return "ASIA"

    def get_session_score(self) -> float:
        session = self.get_session(); hour = datetime.now(timezone.utc).hour
        if 13 <= hour < 17: return 1.0
        if session == "LONDON": return 0.9
        elif session == "NEW_YORK": return 0.85
        return 0.6

    def get_news_impact(self) -> Tuple[bool, int]:
        now = datetime.now(timezone.utc); current_date = now.strftime("%Y-%m-%d")
        for event_date, event_time, event_name, impact_minutes in self.economic_events:
            if event_date == current_date:
                hour, minute = map(int, event_time.split(":"))
                event_dt = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
                diff = (now - event_dt).total_seconds() / 60
                if -30 <= diff <= impact_minutes: return True, int(diff)
        return False, 0

    def record_trade(self, pnl: float):
        self.recent_trades.append({"pnl": pnl, "time": datetime.now(timezone.utc)})
        if len(self.recent_trades) > 50: self.recent_trades = self.recent_trades[-50:]

    def get_self_performance_score(self) -> float:
        today = datetime.now(timezone.utc).date()
        today_trades = [t for t in self.recent_trades if t["time"].date() == today]
        if len(today_trades) < 5: return 0.7
        wins = sum(1 for t in today_trades if t["pnl"] > 0)
        win_rate = wins / len(today_trades)
        if win_rate >= 0.7: return 1.0
        elif win_rate >= 0.5: return 0.8
        return 0.5

    async def get_context_score(self) -> float:
        is_news, minutes_to_news = self.get_news_impact()
        if is_news:
            if minutes_to_news < 0: return 0.0
            return 30.0
        fng = await self.get_fear_greed_index()
        session_score = self.get_session_score()
        perf_score = self.get_self_performance_score()
        if fng > 80: fng_score = 0.4
        elif fng < 20: fng_score = 0.5
        elif 40 <= fng <= 60: fng_score = 0.9
        else: fng_score = 0.7
        context = (fng_score * 0.3 + session_score * 0.4 + perf_score * 0.3) * 100
        return round(context, 1)


class KellyPositionSizer:
    def __init__(self, win_rate=0.55, avg_win=2.0, avg_loss=-1.0):
        self.win_rate = win_rate; self.avg_win = avg_win; self.avg_loss = abs(avg_loss)
    def update(self, win_rate, avg_win, avg_loss):
        self.win_rate = win_rate; self.avg_win = avg_win; self.avg_loss = abs(avg_loss)
    def get_kelly_fraction(self) -> float:
        if self.avg_loss == 0: return 0.01
        b = self.avg_win / self.avg_loss; p = self.win_rate; q = 1 - p
        kelly = (p * b - q) / b if b != 0 else 0
        return min(max(0, kelly * 0.5), 0.02)


# ---------- SIGNAL GENERATOR ----------
class SignalGenerator:
    def __init__(self, dm: MarketDataHub):
        self.dm = dm; self.m = MathUtils()
        self.whale = WhaleIntelligenceEngine(dm)
        self.ob = OrderBookEngine(dm)
        self.struct = MarketStructureEngine()
        self.pattern = CandlePatternDetector()
        self.liq = LiquidityEngine(dm)
        self.regime = MarketRegimeDetector()
        self.mtf = MultiTimeframeAnalyzer(dm)
        self.market_temp = MarketTemperatureEngine(dm)
        self.kelly = KellyPositionSizer(win_rate=0.55, avg_win=2.0, avg_loss=-1.0)
        self.cvd = CumulativeVolumeDelta(dm)
        self.volume_profile = VolumeProfileEngine(dm)
        self.sharp_move = SharpMoveDetector(dm)
        self.oi_engine = OpenInterestEngine(dm)
        self.delta_engine = OrderflowDeltaEngine(dm)
        self.context_engine = MarketContextEngine(dm)
        self.last_signal = datetime.min.replace(tzinfo=timezone.utc)
        self.consecutive_losses = 0

    async def generate(self, symbol: str) -> Optional[Signal]:
        logger.info(f"ANALYSIS START {symbol}")
        now = datetime.now(timezone.utc)
        self.cvd.update(symbol)
        await self.oi_engine.update(symbol)

        if (now - self.last_signal).total_seconds() < CONFIG["SIGNAL_COOLDOWN"]: return None
        if self.consecutive_losses > 0:
            if (now - self.last_signal).total_seconds() < CONFIG["SIGNAL_COOLDOWN"] * (1 + self.consecutive_losses * 0.5):
                return None

        context_score = await self.context_engine.get_context_score()
        if context_score < 30: return None

        ticker = await self.dm.get_ticker_24hr(symbol)
        if ticker:
            vol = float(ticker.get("volume", 0)); price = float(ticker.get("lastPrice", 0))
            if vol * price < CONFIG["MIN_LIQUIDITY"]: return None
        bba = await self.dm.get_best_bid_ask(symbol)
        if bba["spread"] > CONFIG["MAX_SPREAD_PCT"]: return None

        c1m = list(self.dm.candle_buffers[symbol].get("1m", []))
        c5m = list(self.dm.candle_buffers[symbol].get("5m", []))
        if len(c1m) < 20 or len(c5m) < 20: return None

        sharp = self.sharp_move.detect(symbol)
        if sharp and sharp["detected"]:
            direction = "BUY" if sharp["direction"] == "UP" else "SELL"
            entry = sharp["price"]
            atr_5m = self.m.atr([c.high for c in c5m], [c.low for c in c5m], [c.close for c in c5m])
            if atr_5m == 0: atr_5m = entry * 0.003
            sl = entry - max(atr_5m * 1.2, entry * 0.002) if direction == "BUY" else entry + max(atr_5m * 1.2, entry * 0.002)
            tp1 = entry + atr_5m * 1.5 if direction == "BUY" else entry - atr_5m * 1.5
            tp2 = entry + atr_5m * 2.5 if direction == "BUY" else entry - atr_5m * 2.5
            tp3 = entry + atr_5m * 4.0 if direction == "BUY" else entry - atr_5m * 4.0
            pos_size = CONFIG["DYNAMIC_RISK_MIN"] * (context_score / 100)
            sig = Signal(id=f"SOL_{now.strftime('%H%M%S')}_{random.randint(100, 999)}", symbol=symbol,
                         direction=direction, entry_price=entry, stop_loss=sl, take_profit=[tp1, tp2, tp3],
                         confidence=70, timeframe="1m", reason=f"SharpMove:{direction}",
                         whale_volume=0, timestamp=now, risk_per_trade=pos_size, trade_type="SCALP", regime="SHARP")
            self.last_signal = now
            return sig

        mtf = await self.mtf.analyze(symbol)
        aligned = mtf.get("aligned", False)
        regime = self.regime.detect(c1m)

        temp = await self.market_temp.get_temperature(symbol, "5m")
        if temp < 30: return None

        rsi_5m = MathUtils.rsi([c.close for c in c5m])
        reversal_signal = False; reversal_direction = None
        if rsi_5m < 35 and mtf["trend_1h"] == "BULLISH": reversal_signal = True; reversal_direction = "BUY"
        elif rsi_5m > 65 and mtf["trend_1h"] == "BEARISH": reversal_signal = True; reversal_direction = "SELL"

        if reversal_signal:
            direction = reversal_direction; entry = c5m[-1].close
            atr_5m = self.m.atr([c.high for c in c5m], [c.low for c in c5m], [c.close for c in c5m])
            if atr_5m == 0: atr_5m = entry * 0.003
            sl = entry - max(atr_5m * 1.5, entry * 0.0025) if direction == "BUY" else entry + max(atr_5m * 1.5, entry * 0.0025)
            tp1 = entry + (entry - sl) * 1.5 if direction == "BUY" else entry - (sl - entry) * 1.5
            tp2 = entry + (entry - sl) * 2.5 if direction == "BUY" else entry - (sl - entry) * 2.5
            tp3 = entry + (entry - sl) * 3.5 if direction == "BUY" else entry - (sl - entry) * 3.5
            pos_size = CONFIG["DYNAMIC_RISK_MIN"] * (context_score / 100)
            sig = Signal(id=f"SOL_{now.strftime('%H%M%S')}_{random.randint(100, 999)}", symbol=symbol,
                         direction=direction, entry_price=entry, stop_loss=sl, take_profit=[tp1, tp2, tp3],
                         confidence=50, timeframe="5m", reason=f"Reversal:{direction} RSI:{rsi_5m:.1f}",
                         whale_volume=0, timestamp=now, risk_per_trade=pos_size, trade_type="SCALP", regime=regime)
            self.last_signal = now
            return sig

        breakout_signal = False; direction_break = None
        c5m_highs = [c.high for c in c5m[-20:]]; c5m_lows = [c.low for c in c5m[-20:]]
        prev_vols_break = [c.volume for c in c5m[-21:-1]]
        avg_vol_break = self.m.mean(prev_vols_break) if prev_vols_break else 0
        if avg_vol_break > 0:
            vol_ratio_break = c5m[-1].volume / avg_vol_break
            if vol_ratio_break > 2.0:
                if c5m[-1].close > max(c5m_highs[:-1]): breakout_signal = True; direction_break = "BUY"
                elif c5m[-1].close < min(c5m_lows[:-1]): breakout_signal = True; direction_break = "SELL"

        if regime == "RANGING" and not breakout_signal:
            prev_vols = [c.volume for c in c5m[-20:]]; vol_spike = False
            if prev_vols:
                avg_vol = self.m.mean(prev_vols); vol_spike = c5m[-1].volume > avg_vol * 1.8
            ob_imb = self.ob.get_imbalance(symbol)
            if not vol_spike or ob_imb["score"] < 50: return None

        direction = direction_break if breakout_signal else ("BUY" if mtf["trend_1h"] == "BULLISH" else "SELL")

        whale = await self.whale.detect(symbol)
        ob_imb = self.ob.get_imbalance(symbol)
        struct_5m = self.struct.analyze(c5m)
        patterns = self.pattern.detect(c5m)
        liquidity = await self.liq.analyze(symbol)

        prev_vols = [c.volume for c in c5m[-20:]]; volume_spike = False
        if prev_vols:
            avg_vol = self.m.mean(prev_vols); volume_spike = c5m[-1].volume > avg_vol * 1.5

        w = CONFIG["SCORE_WEIGHTS"]; scores = {}
        scores["trend"] = w["trend"] * (mtf.get("strength", 50) / 100)
        scores["volume"] = w["volume"] if volume_spike else 0
        whale_weight = w["whale"] * (0.5 if regime == "RANGING" else 1.0)
        scores["whale"] = min(whale["score"] / 100 * whale_weight, whale_weight) if whale["detected"] and whale.get("direction") == direction else 0
        pat_score = 0
        for p in patterns.values():
            if p.get("direction") == direction: pat_score += p["score"] / 100 * (w["pattern"] / 2)
        scores["pattern"] = min(pat_score, w["pattern"])
        scores["liquidity"] = w["liquidity"] if liquidity["sweep"] and liquidity.get("direction") == direction else 0
        scores["structure"] = w["structure"] / 2 if struct_5m["trend"] == direction else 0
        if struct_5m.get("bos") and struct_5m["bos"]["direction"] == direction: scores["structure"] += w["structure"] / 2
        scores["regime"] = w["regime"] if regime == "TRENDING" else w["regime"] * 0.3
        scores["orderbook"] = min(ob_imb["score"] / 100 * w["orderbook"], w["orderbook"]) if ob_imb["direction"] == direction else 0
        scores["temperature"] = (temp / 100) * w["temperature"]

        delta_bonus = self.delta_engine.get_delta_score(symbol, direction)
        if delta_bonus > 0: scores["delta"] = delta_bonus
        elif delta_bonus < 0: scores["delta"] = delta_bonus

        cvd_div = self.cvd.get_divergence(symbol)
        if cvd_div:
            if cvd_div == "BULLISH" and direction == "BUY": scores["volume"] += 5
            elif cvd_div == "BEARISH" and direction == "SELL": scores["volume"] += 5

        poc = self.volume_profile.get_poc(symbol)
        if poc:
            dist_pct = abs(price - poc) / price * 100
            if dist_pct < 0.3: logger.info(f"Price near POC {poc:.2f}")

        oi_health = self.oi_engine.get_health(symbol)
        if oi_health["oi_trend"] == "UP" and direction == "BUY": scores["trend"] += 3
        elif oi_health["oi_trend"] == "DOWN" and direction == "SELL": scores["trend"] += 3

        confidence = sum(scores.values())
        if not breakout_signal and not aligned: confidence *= 0.8
        if breakout_signal and confidence < 45: confidence = 45
        confidence *= (context_score / 100)

        if confidence < CONFIG["MIN_CONFIDENCE"]: return None

        atr_5m = self.m.atr([c.high for c in c5m], [c.low for c in c5m], [c.close for c in c5m])
        if atr_5m == 0: atr_5m = c5m[-1].close * 0.003

        entry = c5m[-1].close; min_sl_dist = entry * 0.003
        if direction == "BUY":
            sl = entry - max(atr_5m * 1.8, min_sl_dist)
            tp1 = entry + atr_5m * 1.5; tp2 = entry + atr_5m * 2.5; tp3 = entry + atr_5m * 4.0
        else:
            sl = entry + max(atr_5m * 1.8, min_sl_dist)
            tp1 = entry - atr_5m * 1.5; tp2 = entry - atr_5m * 2.5; tp3 = entry - atr_5m * 4.0

        kelly_frac = self.kelly.get_kelly_fraction()
        dynamic_risk = max(CONFIG["DYNAMIC_RISK_MIN"], min(CONFIG["DYNAMIC_RISK_MAX"], kelly_frac))
        dynamic_risk *= (context_score / 100)

        sig = Signal(id=f"SOL_{now.strftime('%H%M%S')}_{random.randint(100, 999)}", symbol=symbol,
                     direction=direction, entry_price=entry, stop_loss=sl, take_profit=[tp1, tp2, tp3],
                     confidence=confidence, timeframe="5m",
                     reason=f"Trend:{direction} Regime:{regime} Temp:{temp:.0f}",
                     whale_volume=whale.get("score", 0), timestamp=now, risk_per_trade=dynamic_risk,
                     trade_type="MID_TERM", regime=regime, score_breakdown=scores)
        self.last_signal = now
        return sig

    def report_result(self, pnl: float):
        if pnl <= 0: self.consecutive_losses += 1
        else: self.consecutive_losses = 0
        self.kelly.win_rate = self.kelly.win_rate * 0.9 + (0.1 if pnl > 0 else 0)
        self.kelly.avg_win = self.kelly.avg_win * 0.9 + (pnl * 0.1 if pnl > 0 else 0)
        self.kelly.avg_loss = self.kelly.avg_loss * 0.9 + (abs(pnl) * 0.1 if pnl <= 0 else 0)
        self.context_engine.record_trade(pnl)


# ---------- MARKET MENTOR ENGINE ----------
class MarketMentorEngine:
    def __init__(self, dm: MarketDataHub): self.dm = dm; self.m = MathUtils()

    async def full_analysis(self, symbol: str) -> str:
        price = await self.dm.get_current_price(symbol)
        if not price: return "❌ قیمت در دسترس نیست."
        candles_1h = list(self.dm.candle_buffers[symbol].get("1H", []))
        candles_5m = list(self.dm.candle_buffers[symbol].get("5m", []))
        if len(candles_5m) < 20 or len(candles_1h) < 20: return "⏳ کندل ناکافی."
        closes_1h = [c.close for c in candles_1h]
        ema20 = self.m.ema(closes_1h, 20); ema50 = self.m.ema(closes_1h, 50)
        if not ema20 or not ema50: return "📊 EMA ناکافی."
        trend = "صعودی 📈" if ema20[-1] > ema50[-1] else "نزولی 📉" if ema20[-1] < ema50[-1] else "خنثی 🔄"
        atr = MathUtils.atr([c.high for c in candles_5m], [c.low for c in candles_5m], [c.close for c in candles_5m])
        if atr == 0: atr = price * 0.003
        vol_desc = "بالا ⚡" if atr > 0.008 * price else "پایین 🌊" if atr < 0.003 * price else "معمولی"
        rsi = MathUtils.rsi([c.close for c in candles_5m])
        rsi_desc = "اشباع خرید" if rsi > 65 else "اشباع فروش" if rsi < 35 else "خنثی"
        decision = "ورود نکن"
        if trend.startswith("صعودی") and rsi < 65: decision = "لانگ 🟢"
        elif trend.startswith("نزولی") and rsi > 35: decision = "شورت 🔴"
        return (f"📊 تحلیل SOL\n💰 {price:.4f}\n• روند ۱h: {trend}\n• نوسان: {vol_desc}\n"
                f"• RSI (5m): {rsi:.1f} – {rsi_desc}\n💡 {decision}")

    async def scalp_analysis(self, symbol: str) -> str:
        price = await self.dm.get_current_price(symbol)
        if not price: return "❌ قیمت در دسترس نیست."
        candles_5m = list(self.dm.candle_buffers[symbol].get("5m", []))
        if len(candles_5m) < 20: return "⏳ کندل کم."
        closes = [c.close for c in candles_5m]; highs = [c.high for c in candles_5m]; lows = [c.low for c in candles_5m]
        atr = MathUtils.atr(highs, lows, closes)
        if atr == 0: atr = price * 0.003
        rsi = MathUtils.rsi(closes)
        direction = None
        if 60 < rsi < 70 and atr/price > 0.002: direction = "BUY"
        elif 30 < rsi < 40 and atr/price > 0.002: direction = "SELL"
        if not direction: return f"📊 اسکالپ SOL\n💰 {price:.4f}\n📈 RSI:{rsi:.1f}\n📊 ATR:{atr:.4f}\n💡 شرایط ضعیف"
        if direction == "BUY":
            sl = price - atr * 1.5; tp1 = price + atr * 1.5; tp2 = price + atr * 2.5; tp3 = price + atr * 3.5
        else:
            sl = price + atr * 1.5; tp1 = price - atr * 1.5; tp2 = price - atr * 2.5; tp3 = price - atr * 3.5
        return (f"⚡ سیگنال اسکالپ SOL {'🟢' if direction=='BUY' else '🔴'}\n"
                f"🎯 {price:.4f} | SL:{sl:.4f}\n✅ TP1:{tp1:.4f} TP2:{tp2:.4f} TP3:{tp3:.4f}\n📊 RSI:{rsi:.1f}")

    async def swing_analysis(self, symbol: str) -> str:
        price = await self.dm.get_current_price(symbol)
        if not price: return "❌ قیمت در دسترس نیست."
        candles_1h = list(self.dm.candle_buffers[symbol].get("1H", []))
        if len(candles_1h) < 20: return "⏳ کندل کم."
        closes = [c.close for c in candles_1h]; highs = [c.high for c in candles_1h]; lows = [c.low for c in candles_1h]
        ema20 = self.m.ema(closes, 20); ema50 = self.m.ema(closes, 50)
        if not ema20 or not ema50: return "📊 EMA ناکافی"
        trend = "BULLISH" if ema20[-1] > ema50[-1] else "BEARISH"
        atr = MathUtils.atr(highs, lows, closes)
        if trend == "BULLISH":
            sl = min(lows[-10:]) if len(lows) >= 10 else price * 0.97; risk = price - sl
            tp1 = price + risk * 1.5; tp2 = price + risk * 2.5; tp3 = price + risk * 3.5
        else:
            sl = max(highs[-10:]) if len(highs) >= 10 else price * 1.03; risk = sl - price
            tp1 = price - risk * 1.5; tp2 = price - risk * 2.5; tp3 = price - risk * 3.5
        return (f"📈 میان‌مدت SOL {'🟢' if trend=='BULLISH' else '🔴'}\n"
                f"🎯 {price:.4f} | SL:{sl:.4f}\n✅ TP1:{tp1:.4f} TP2:{tp2:.4f} TP3:{tp3:.4f}\n📊 روند: {trend}")

    async def key_levels(self, symbol: str) -> str:
        price = await self.dm.get_current_price(symbol)
        if not price: return "❌ قیمت در دسترس نیست."
        candles_1h = list(self.dm.candle_buffers[symbol].get("1H", []))
        if len(candles_1h) < 20: return "⏳ کندل کم."
        highs = [c.high for c in candles_1h]; lows = [c.low for c in candles_1h]
        resistances = []; supports = []
        for i in range(5, len(highs) - 5):
            if highs[i] == max(highs[i - 5:i + 6]): resistances.append(highs[i])
            if lows[i] == min(lows[i - 5:i + 6]): supports.append(lows[i])
        res_count = Counter(resistances); sup_count = Counter(supports)
        sorted_res = sorted(res_count.items(), key=lambda x: (-x[1], -x[0]))[:5]
        sorted_sup = sorted(sup_count.items(), key=lambda x: (-x[1], x[0]))[:5]
        lines = [f"📊 سطوح کلیدی SOL (قیمت: {price:.4f})"]
        if sorted_res: lines.append(f"🔴 مقاومت‌ها: {' | '.join(f'{p:.4f}(x{c})' for p, c in sorted_res)}")
        if sorted_sup: lines.append(f"🟢 حمایت‌ها: {' | '.join(f'{p:.4f}(x{c})' for p, c in sorted_sup)}")
        return "\n".join(lines)

    async def daily_summary(self, closed_trades: List[Trade]) -> str:
        today = datetime.now(timezone.utc).date()
        today_trades = [t for t in closed_trades if t.open_time.date() == today]
        if not today_trades: return "📆 امروز معامله‌ای نبود."
        wins = [t for t in today_trades if t.pnl_pct > 0]; losses = [t for t in today_trades if t.pnl_pct <= 0]
        return (f"📆 امروز SOL\n• معاملات: {len(today_trades)}\n• برد: {len(wins)} | باخت: {len(losses)}\n"
                f"• نرخ برد: {len(wins)/len(today_trades)*100:.0f}%\n• سود/ضرر: {sum(t.pnl_pct for t in today_trades):.2f}%")


# ---------- RISK & TRADE MANAGEMENT ----------
class PortfolioRiskManager:
    def __init__(self): self.open = 0; self.lock = asyncio.Lock()
    async def can_open(self): return self.open < CONFIG["MAX_OPEN_TRADES"]
    async def inc(self): self.open += 1
    async def dec(self):
        if self.open > 0: self.open -= 1

class TradeManager:
    def __init__(self, dm, rep, risk_manager=None, signal_gen=None):
        self.dm = dm; self.rep = rep; self.risk_manager = risk_manager
        self.signal_gen = signal_gen
        self.active: Dict[str, Trade] = {}
        self.on_close_callback = None

    def add(self, sig: Signal):
        t = Trade(sig.id, sig.symbol, sig.direction, sig.entry_price, sig.stop_loss,
                  sig.take_profit[0], sig.take_profit[1], sig.take_profit[2],
                  position_size=sig.position_size, risk_per_trade=sig.risk_per_trade,
                  confidence=sig.confidence, leverage=sig.leverage)
        t.trailing_stop = sig.stop_loss; t.peak_price = sig.entry_price
        self.active[sig.id] = t

    async def run(self):
        while True:
            await asyncio.sleep(CONFIG["TRADE_LIFECYCLE_CHECK_INTERVAL"])
            for symbol in CONFIG["SYMBOLS"]:
                p = await self.dm.get_current_price(symbol)
                if not p: continue
                c5m = list(self.dm.candle_buffers[symbol].get("5m", []))
                atr = MathUtils.atr([c.high for c in c5m], [c.low for c in c5m], [c.close for c in c5m]) if len(c5m) >= 14 else p * 0.003
                closed = []
                for tid, t in list(self.active.items()):
                    if t.symbol != symbol: continue
                    if t.direction == "BUY":
                        if p <= t.stop_loss:
                            t.status = "STOPPED"; t.exit_price = p
                            t.pnl_pct = (p - t.entry_price) / t.entry_price * 100
                            await self.rep.update(t, "STOPPED"); closed.append(tid)
                            continue
                        if p >= t.tp1:
                            new_stop = max(t.trailing_stop, p - atr * 1.2); t.trailing_stop = new_stop
                            if p <= t.trailing_stop:
                                t.status = "CLOSED"; t.exit_price = p
                                t.pnl_pct = (p - t.entry_price) / t.entry_price * 100
                                await self.rep.update(t, "CLOSED"); closed.append(tid)
                                continue
                        if not t.tp1_hit and p >= t.tp1:
                            t.tp1_hit = True; t.position_size *= (1 - CONFIG["TP1_PERCENT"])
                            await self.rep.update(t, "TP1")
                        elif t.tp1_hit and not t.tp2_hit and p >= t.tp2:
                            t.tp2_hit = True; t.position_size *= (1 - CONFIG["TP2_PERCENT"])
                            await self.rep.update(t, "TP2")
                        elif t.tp1_hit and t.tp2_hit and p >= t.tp3:
                            t.status = "CLOSED"; t.exit_price = p
                            t.pnl_pct = (p - t.entry_price) / t.entry_price * 100
                            await self.rep.update(t, "CLOSED"); closed.append(tid)
                    else:
                        if p >= t.stop_loss:
                            t.status = "STOPPED"; t.exit_price = p
                            t.pnl_pct = (t.entry_price - p) / t.entry_price * 100
                            await self.rep.update(t, "STOPPED"); closed.append(tid)
                            continue
                        if p <= t.tp1:
                            new_stop = min(t.trailing_stop, p + atr * 1.2); t.trailing_stop = new_stop
                            if p >= t.trailing_stop:
                                t.status = "CLOSED"; t.exit_price = p
                                t.pnl_pct = (t.entry_price - p) / t.entry_price * 100
                                await self.rep.update(t, "CLOSED"); closed.append(tid)
                                continue
                        if not t.tp1_hit and p <= t.tp1:
                            t.tp1_hit = True; t.position_size *= (1 - CONFIG["TP1_PERCENT"])
                            await self.rep.update(t, "TP1")
                        elif t.tp1_hit and not t.tp2_hit and p <= t.tp2:
                            t.tp2_hit = True; t.position_size *= (1 - CONFIG["TP2_PERCENT"])
                            await self.rep.update(t, "TP2")
                        elif t.tp1_hit and t.tp2_hit and p <= t.tp3:
                            t.status = "CLOSED"; t.exit_price = p
                            t.pnl_pct = (t.entry_price - p) / t.entry_price * 100
                            await self.rep.update(t, "CLOSED"); closed.append(tid)
                for tid in closed:
                    t_closed = self.active.pop(tid, None)
                    if t_closed:
                        if self.signal_gen: self.signal_gen.report_result(t_closed.pnl_pct)
                        if self.on_close_callback: asyncio.create_task(self.on_close_callback(t_closed))
                    if self.risk_manager: await self.risk_manager.dec()


# ---------- TELEGRAM ----------
try:
    from telegram import Bot, ReplyKeyboardMarkup
    from telegram.constants import ParseMode
    from telegram.ext import Application, CommandHandler, MessageHandler, filters
    TELE = True
except ImportError:
    TELE = False

if TELE:
    class TelegramBot:
        def __init__(self, bot_token, channel_id, mentor=None, trade_manager=None):
            self.bot = Bot(token=bot_token); self.channel = channel_id
            self.mentor = mentor; self.trade_manager = trade_manager
            self.closed_trades = []
            self.app = Application.builder().token(bot_token).build()
            self.app.add_handler(CommandHandler("start", self._start_command))
            self.app.add_handler(CommandHandler("scalp", self._scalp_command))
            self.app.add_handler(CommandHandler("swing", self._swing_command))
            self.app.add_handler(CommandHandler("health", self._health_command))
            self.app.add_handler(CommandHandler("trades", self._trades_command))
            self.app.add_handler(CommandHandler("levels", self._key_levels_command))
            self.app.add_handler(CommandHandler("summary", self._daily_summary_command))
            self.app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, self._handle_buttons))

        async def startup(self): await self._send("🚀 *ربات SOL روی OKX فعال شد*")
        async def signal(self, sig: Signal):
            em = "📈" if sig.direction == "BUY" else "📉"; d = "خرید" if sig.direction == "BUY" else "فروش"
            msg = (f"{em} *سیگنال SOL - {d}*\n"
                   f"🎯 ورود: {sig.entry_price:.4f} | SL: {sig.stop_loss:.4f}\n"
                   f"✅ TP1: {sig.take_profit[0]:.4f} | TP2: {sig.take_profit[1]:.4f} | TP3: {sig.take_profit[2]:.4f}\n"
                   f"📊 اطمینان: {sig.confidence:.0f}%\n📝 {sig.reason}")
            await self._send(msg)
        async def update(self, t: Trade, e: str):
            fa = {"TP1":"✅ TP1","TP2":"✅ TP2","CLOSED":"🏁 بسته شد","STOPPED":"🛑 SL"}
            await self._send(f"{fa.get(e,e)} SOL | ورود: {t.entry_price:.4f} | PnL: {t.pnl_pct:.2f}%")
        async def _send(self, txt):
            try: await self.bot.send_message(chat_id=self.channel, text=txt, parse_mode=ParseMode.MARKDOWN)
            except: pass

        async def _start_command(self, update, context):
            keyboard = [
                ["⚡ تحلیل اسکالپ", "📊 سلامت ربات"],
                ["📈 میان‌مدت", "📋 معاملات باز"],
                ["🏔 سطوح کلیدی", "📆 خلاصه امروز"]
            ]
            reply_markup = ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            await update.message.reply_text("🤖 *ربات SOL روی OKX*", reply_markup=reply_markup, parse_mode=ParseMode.MARKDOWN)

        async def _handle_buttons(self, update, context):
            text = update.message.text
            if "اسکالپ" in text: await self._scalp_command(update, context)
            elif "سلامت" in text: await self._health_command(update, context)
            elif "میان‌مدت" in text: await self._swing_command(update, context)
            elif "معاملات" in text: await self._trades_command(update, context)
            elif "سطوح" in text: await self._key_levels_command(update, context)
            elif "خلاصه" in text: await self._daily_summary_command(update, context)

        async def _scalp_command(self, update, context):
            if not self.mentor: return
            res = await self.mentor.scalp_analysis("SOL-USDT-SWAP")
            await update.message.reply_text(res, parse_mode=ParseMode.MARKDOWN)

        async def _swing_command(self, update, context):
            if not self.mentor: return
            res = await self.mentor.swing_analysis("SOL-USDT-SWAP")
            await update.message.reply_text(res, parse_mode=ParseMode.MARKDOWN)

        async def _health_command(self, update, context):
            dm = self.mentor.dm if self.mentor else None
            if not dm: return
            msg = (f"📊 *وضعیت ربات*\n• درخواست‌های API: {dm.request_count}\n"
                   f"• خطاها: {dm.error_count}\n• کندل‌های 5m: {len(dm.candle_buffers['SOL-USDT-SWAP'].get('5m',[]))}")
            await update.message.reply_text(msg, parse_mode=ParseMode.MARKDOWN)

        async def _trades_command(self, update, context):
            if not self.trade_manager or not self.trade_manager.active:
                await update.message.reply_text("هیچ معامله‌ی بازی وجود ندارد."); return
            dm = self.mentor.dm if self.mentor else None
            price = await dm.get_current_price("SOL-USDT-SWAP") if dm else None
            lines = []
            for t in self.trade_manager.active.values():
                pnl = (price - t.entry_price) / t.entry_price * 100 if price else 0.0
                if t.direction == "SELL": pnl = -pnl
                lines.append(f"• {t.direction} @ {t.entry_price:.4f} | PnL:{pnl:.2f}%")
            await update.message.reply_text("📋 *معاملات باز:*\n" + "\n".join(lines), parse_mode=ParseMode.MARKDOWN)

        async def _key_levels_command(self, update, context):
            if not self.mentor: return
            res = await self.mentor.key_levels("SOL-USDT-SWAP")
            await update.message.reply_text(res, parse_mode=ParseMode.MARKDOWN)

        async def _daily_summary_command(self, update, context):
            closed = self.closed_trades
            if not self.mentor: return
            res = await self.mentor.daily_summary(closed)
            await update.message.reply_text(res, parse_mode=ParseMode.MARKDOWN)

        async def start_polling(self):
            await self.app.initialize(); await self.app.start(); await self.app.updater.start_polling()
else:
    class TelegramBot:
        def __init__(self, *args, **kwargs): pass
        async def startup(self): pass
        async def signal(self, *a): pass
        async def update(self, *a): pass
        async def start_polling(self): pass


# ---------- HEALTH CHECK WEB SERVER ----------
async def health_check(request):
    return web.Response(text="OK")

async def run_web_server():
    app = web.Application()
    app.router.add_get('/', health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', CONFIG["WEB_SERVER_PORT"])
    await site.start()
    logger.info(f"✅ Health check on port {CONFIG['WEB_SERVER_PORT']}")


# ---------- MAIN BOT ----------
class UltimateBybitBot:
    def __init__(self):
        self.dm = MarketDataHub()
        self.gen = SignalGenerator(self.dm)
        self.risk = PortfolioRiskManager()
        self.mentor = MarketMentorEngine(self.dm)
        self.closed_trades: List[Trade] = []
        self.tm = TradeManager(self.dm, None, self.risk, signal_gen=self.gen)
        self.rep = TelegramBot(CONFIG["TELEGRAM_BOT_TOKEN"], CONFIG["SIGNAL_CHANNEL"],
                               mentor=self.mentor, trade_manager=self.tm)
        self.tm.rep = self.rep
        async def record_closed(trade: Trade): self.closed_trades.append(trade)
        self.tm.on_close_callback = record_closed
        self.rep.closed_trades = self.closed_trades
        self.is_running = False

    async def rest_update_loop(self):
        while self.is_running:
            for symbol in CONFIG["SYMBOLS"]:
                try:
                    await self.dm.update_all_klines(symbol)
                    await self.dm.get_order_book(symbol)
                    await self.dm.get_agg_trades(symbol)
                except Exception as e:
                    logger.error(f"REST update error {symbol}: {e}")
            await asyncio.sleep(CONFIG["REST_UPDATE_INTERVAL"])

    async def analysis_loop(self):
        while self.is_running:
            for symbol in CONFIG["SYMBOLS"]:
                try:
                    if not await self.risk.can_open(): continue
                    sig = await self.gen.generate(symbol)
                    if sig:
                        await self.rep.signal(sig)
                        self.tm.add(sig)
                        await self.risk.inc()
                except Exception as e:
                    logger.error(f"Analysis error {symbol}: {e}")
            await asyncio.sleep(CONFIG["ANALYSIS_INTERVAL"])

    async def start(self):
        for symbol in CONFIG["SYMBOLS"]:
            await self.dm.update_all_klines(symbol)
        self.is_running = True

        asyncio.create_task(run_web_server())

        await self.rep.start_polling()
        await self.rep.startup()

        await asyncio.gather(
            self.rest_update_loop(),
            self.analysis_loop(),
            self.tm.run()
        )


async def main():
    bot = UltimateBybitBot()
    await bot.start()

if __name__ == "__main__":
    asyncio.run(main())
