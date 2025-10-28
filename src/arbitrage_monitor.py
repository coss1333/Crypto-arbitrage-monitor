#!/usr/bin/env python3
import asyncio
import aiohttp
import os
import math
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import yaml
from dotenv import load_dotenv

# ----------------------- Config models -----------------------

@dataclass
class Settings:
    symbols: List[str]
    quote: str
    min_edge_pct: float
    poll_interval_sec: int
    exchanges_enabled: Dict[str, bool]
    fees_taker_bps: Dict[str, float]
    withdrawal_estimate_usdt: Dict[str, float]
    telegram_dry_run: bool

    @staticmethod
    def load(path: str) -> "Settings":
        with open(path, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        exchanges_enabled = cfg.get("exchanges", {})
        fees = cfg.get("fees_taker_bps", {})
        withdrawal = cfg.get("withdrawal_estimate_usdt", {})
        telegram = cfg.get("telegram", {})
        return Settings(
            symbols=cfg.get("symbols", ["BTC", "ETH"]),
            quote=cfg.get("quote", "USDT"),
            min_edge_pct=float(cfg.get("min_edge_pct", 0.2)),
            poll_interval_sec=int(cfg.get("poll_interval_sec", 5)),
            exchanges_enabled=exchanges_enabled,
            fees_taker_bps=fees,
            withdrawal_estimate_usdt=withdrawal,
            telegram_dry_run=bool(telegram.get("dry_run", False)),
        )

# ----------------------- Exchange clients -----------------------

class ExchangeBase:
    name: str

    def __init__(self, session: aiohttp.ClientSession, quote: str):
        self.session = session
        self.quote = quote

    def symbol_pair(self, base: str) -> Optional[str]:
        """Return API symbol format for this exchange or None if unsupported."""
        raise NotImplementedError

    async def fetch_book(self, base: str) -> Optional[Tuple[float, float]]:
        """Return (best_bid, best_ask) or None if unavailable."""
        raise NotImplementedError

# Binance
class Binance(ExchangeBase):
    name = "binance"
    def symbol_pair(self, base: str) -> Optional[str]:
        return f"{base}{self.quote}"
    async def fetch_book(self, base: str) -> Optional[Tuple[float, float]]:
        sym = self.symbol_pair(base)
        url = f"https://api.binance.com/api/v3/ticker/bookTicker?symbol={sym}"
        try:
            async with self.session.get(url, timeout=8) as r:
                if r.status != 200: return None
                data = await r.json()
                return float(data["bidPrice"]), float(data["askPrice"])
        except Exception:
            return None

# Bybit (spot)
class Bybit(ExchangeBase):
    name = "bybit"
    def symbol_pair(self, base: str) -> Optional[str]:
        return f"{base}{self.quote}"
    async def fetch_book(self, base: str) -> Optional[Tuple[float, float]]:
        sym = self.symbol_pair(base)
        url = f"https://api.bybit.com/v5/market/tickers?category=spot&symbol={sym}"
        try:
            async with self.session.get(url, timeout=8) as r:
                if r.status != 200: return None
                j = await r.json()
                arr = j.get("result", {}).get("list", [])
                if not arr: return None
                d = arr[0]
                return float(d["bid1Price"]), float(d["ask1Price"])
        except Exception:
            return None

# KuCoin
class KuCoin(ExchangeBase):
    name = "kucoin"
    def symbol_pair(self, base: str) -> Optional[str]:
        return f"{base}-{self.quote}"
    async def fetch_book(self, base: str) -> Optional[Tuple[float, float]]:
        sym = self.symbol_pair(base)
        url = f"https://api.kucoin.com/api/v1/market/orderbook/level1?symbol={sym}"
        try:
            async with self.session.get(url, timeout=8) as r:
                if r.status != 200: return None
                j = await r.json()
                d = j.get("data", {})
                if not d: return None
                return float(d["bestBid"]), float(d["bestAsk"])
        except Exception:
            return None

# Kraken (XBT вместо BTC; USDT поддерживается)
class Kraken(ExchangeBase):
    name = "kraken"
    def symbol_pair(self, base: str) -> Optional[str]:
        base_map = {"BTC":"XBT"}
        b = base_map.get(base, base)
        return f"{b}{self.quote}"
    async def fetch_book(self, base: str) -> Optional[Tuple[float, float]]:
        pair = self.symbol_pair(base)
        url = f"https://api.kraken.com/0/public/Ticker?pair={pair}"
        try:
            async with self.session.get(url, timeout=10) as r:
                if r.status != 200: return None
                j = await r.json()
                res = j.get("result", {})
                if not res: return None
                d = next(iter(res.values()))
                bid = float(d["b"][0]); ask = float(d["a"][0])
                return bid, ask
        except Exception:
            return None

# Bitstamp
class Bitstamp(ExchangeBase):
    name = "bitstamp"
    def symbol_pair(self, base: str) -> Optional[str]:
        return f"{base}{self.quote}".lower()
    async def fetch_book(self, base: str) -> Optional[Tuple[float, float]]:
        sym = self.symbol_pair(base)
        url = f"https://www.bitstamp.net/api/v2/ticker/{sym}"
        try:
            async with self.session.get(url, timeout=10) as r:
                if r.status != 200: return None
                d = await r.json()
                return float(d["bid"]), float(d["ask"])
        except Exception:
            return None

# Coinbase (может не иметь USDT для всех монет; оставлено опционально)
class Coinbase(ExchangeBase):
    name = "coinbase"
    def symbol_pair(self, base: str) -> Optional[str]:
        return f"{base}-{self.quote}"
    async def fetch_book(self, base: str) -> Optional[Tuple[float, float]]:
        sym = self.symbol_pair(base)
        url = f"https://api.exchange.coinbase.com/products/{sym}/ticker"
        try:
            async with self.session.get(url, timeout=8) as r:
                if r.status != 200: return None
                d = await r.json()
                bid = float(d["bid"]); ask = float(d["ask"])
                return bid, ask
        except Exception:
            return None

# ----------------------- Telegram -----------------------

class Telegram:
    def __init__(self, token: str, chat_id: str, dry_run: bool = False):
        self.token = token
        self.chat_id = chat_id
        self.dry_run = dry_run

    async def send(self, session: aiohttp.ClientSession, text: str) -> None:
        if self.dry_run:
            print("[TELEGRAM DRY RUN]\\n" + text)
            return
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
        try:
            async with session.post(url, json=payload, timeout=8) as r:
                if r.status != 200:
                    print(f"[telegram] HTTP {r.status}")
        except Exception as e:
            print(f"[telegram] error: {e}")

# ----------------------- Arbitrage logic -----------------------

def apply_fee(price: float, bps: float, side: str) -> float:
    """Return price adjusted by taker fee. side='buy' -> price*(1+fee), side='sell' -> price*(1-fee)"""
    fee = bps / 10000.0
    if side == "buy":
        return price * (1.0 + fee)
    else:
        return price * (1.0 - fee)

@dataclass
class Quote:
    bid: float
    ask: float

async def fetch_all_books(session: aiohttp.ClientSession, exchanges: List[ExchangeBase], base: str) -> Dict[str, Quote]:
    out: Dict[str, Quote] = {}
    tasks = [ex.fetch_book(base) for ex in exchanges]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for ex, res in zip(exchanges, results):
        if isinstance(res, Exception) or res is None:
            continue
        bid, ask = res
        if bid > 0 and ask > 0:
            out[ex.name] = Quote(bid=bid, ask=ask)
    return out

def best_opportunity(base: str, books: Dict[str, Quote], fees_bps: Dict[str, float], withdrawal_estimate: float) -> Optional[Dict]:
    if not books or len(books) < 2:
        return None
    best = None
    # try all exchange pairs
    for buy_ex, buy_q in books.items():
        for sell_ex, sell_q in books.items():
            if sell_ex == buy_ex: 
                continue
            buy_fee = fees_bps.get(buy_ex, 0.0)
            sell_fee = fees_bps.get(sell_ex, 0.0)
            adj_buy = apply_fee(buy_q.ask, buy_fee, "buy")
            adj_sell = apply_fee(sell_q.bid, sell_fee, "sell")
            gross = adj_sell - adj_buy - withdrawal_estimate
            edge_pct = 100.0 * (gross / adj_buy)
            if best is None or edge_pct > best["edge_pct"]:
                best = {
                    "base": base,
                    "buy_ex": buy_ex, "buy_price": buy_q.ask, "buy_fee_bps": buy_fee, "buy_price_adj": adj_buy,
                    "sell_ex": sell_ex, "sell_price": sell_q.bid, "sell_fee_bps": sell_fee, "sell_price_adj": adj_sell,
                    "withdrawal": withdrawal_estimate,
                    "edge_pct": edge_pct,
                }
    return best

def format_alert(op: Dict) -> str:
    return (
        f"🚨 <b>ARBITRAGE</b> {op['base']}/USDT\n"
        f"Buy @ <b>{op['buy_ex']}</b>: ask={op['buy_price']:.6f} (fee {op['buy_fee_bps']:.2f} bps ⇒ {op['buy_price_adj']:.6f})\n"
        f"Sell @ <b>{op['sell_ex']}</b>: bid={op['sell_price']:.6f} (fee {op['sell_fee_bps']:.2f} bps ⇒ {op['sell_price_adj']:.6f})\n"
        f"Withdrawal est.: {op['withdrawal']} USDT\n"
        f"➡️ <b>Edge ≈ {op['edge_pct']:.3f}%</b>"
    )

# ----------------------- Main -----------------------

async def main():
    load_dotenv()
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    chat_id = os.getenv("TELEGRAM_CHAT_ID", "").strip()

    settings = Settings.load(os.path.join(os.path.dirname(__file__), "..", "config.yaml"))
    if not settings.telegram_dry_run and (not token or not chat_id):
        print("ERROR: Нет TELEGRAM_BOT_TOKEN или TELEGRAM_CHAT_ID в .env. Включите telegram.dry_run=true в config.yaml для теста без отправки.")
        return

    conn = aiohttp.TCPConnector(limit=64, ttl_dns_cache=300)
    async with aiohttp.ClientSession(connector=conn) as session:
        tg = Telegram(token, chat_id, dry_run=settings.telegram_dry_run)

        # build enabled exchanges
        ex_list: List[ExchangeBase] = []
        if settings.exchanges_enabled.get("binance", False): ex_list.append(Binance(session, settings.quote))
        if settings.exchanges_enabled.get("bybit", False): ex_list.append(Bybit(session, settings.quote))
        if settings.exchanges_enabled.get("kucoin", False): ex_list.append(KuCoin(session, settings.quote))
        if settings.exchanges_enabled.get("kraken", False): ex_list.append(Kraken(session, settings.quote))
        if settings.exchanges_enabled.get("bitstamp", False): ex_list.append(Bitstamp(session, settings.quote))
        if settings.exchanges_enabled.get("coinbase", False): ex_list.append(Coinbase(session, settings.quote))

        print(f"Monitoring {settings.symbols} quoted in {settings.quote} across {[ex.name for ex in ex_list]}")
        last_alert: Dict[str, float] = {}  # base -> last edge pct sent

        while True:
            t0 = time.time()
            for base in settings.symbols:
                books = await fetch_all_books(session, ex_list, base)
                if len(books) < 2:
                    print(f"[{base}] мало данных: {books.keys()}")
                    continue
                op = best_opportunity(
                    base=base,
                    books=books,
                    fees_bps=settings.fees_taker_bps,
                    withdrawal_estimate=settings.withdrawal_estimate_usdt.get(base, 0.0),
                )
                if not op:
                    continue
                # alert if edge exceeds threshold and improved since last alert by 0.05%
                if op["edge_pct"] >= settings.min_edge_pct:
                    prev = last_alert.get(base, -1e9)
                    if op["edge_pct"] - prev >= 0.05:
                        last_alert[base] = op["edge_pct"]
                        msg = format_alert(op)
                        print(msg)
                        await tg.send(session, msg)
            # sleep remainder of poll interval
            dt = time.time() - t0
            await asyncio.sleep(max(0, settings.poll_interval_sec - dt))

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
