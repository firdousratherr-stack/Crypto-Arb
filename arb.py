import asyncio
import os
import sqlite3
import time
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from datetime import datetime, timezone, timedelta
import ccxt.async_support as ccxt_async
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler
)

# ==========================================
# 1. CONFIGURATION
# ==========================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8848406877:AAHuBsI_IXmFTvVg8EKu-r7XZm9Gy9uYTfA")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "X")

SCAN_INTERVAL_SECONDS = 30
DEFAULT_TRADE_SIZE_USD = 100.0
DEFAULT_MIN_PROFIT_USER = 5.0
DEFAULT_MIN_SPREAD_PCT = 0.5
DEFAULT_MAX_SPREAD_PCT = 50.0
DEFAULT_MAX_RESULTS = 15
SCAN_CONCURRENCY = 12

MIN_24H_VOLUME_USD = 40000
GENERIC_WITHDRAW_FEE_COIN_UNITS = 1.0
STRICT_CONTRACT_MATCH = False
CURRENCY_REFRESH_SECONDS = 1200

IST = timezone(timedelta(hours=5, minutes=30))

EXCHANGE_CONFIG = {
    'gate': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.0001, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.5, 'DOGE': 5.0}},
    'lbank': {'fee': 0.001, 'withdraw_fees': {'BTC': 0.0002, 'ETH': 0.003, 'SOL': 0.015, 'XRP': 1.0, 'DOGE': 10.0}},
    'bitrue': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.00015, 'ETH': 0.0025, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 6.0}},
    'xt': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.0002, 'ETH': 0.003, 'SOL': 0.02, 'XRP': 0.8, 'DOGE': 8.0}},
    'ascendex': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.005, 'SOL': 0.01, 'XRP': 1.0, 'DOGE': 10.0}},
    'poloniex': {'fee': 0.00145, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.005, 'SOL': 0.01, 'XRP': 1.0, 'DOGE': 10.0}},
    'bingx': {'fee': 0.001, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.005, 'SOL': 0.01, 'XRP': 1.0, 'DOGE': 10.0}},
    'digifinex': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.005, 'SOL': 0.01, 'XRP': 1.0, 'DOGE': 10.0}},
    'binance': {'fee': 0.001, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 4.0}},
    'bybit': {'fee': 0.001, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 5.0}},
    'okx': {'fee': 0.001, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 5.0}},
    'kucoin': {'fee': 0.001, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 5.0}},
    'mexc': {'fee': 0.001, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 5.0}},
    'bitget': {'fee': 0.001, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 5.0}},
    'htx': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 5.0}},
    'kraken': {'fee': 0.0026, 'withdraw_fees': {'BTC': 0.0005, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 5.0}},
    'bitfinex': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.0004, 'ETH': 0.0013, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 5.0}}
}

UNIVERSAL_SYMBOLS = []
SYMBOL_EXCHANGE_MAP = {}
CURRENCY_STATUS = {}
CONTRACT_ADDRESSES = {}

# ==========================================
# 2. DATABASE
# ==========================================
def init_db():
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            is_premium INTEGER DEFAULT 0,
            is_banned INTEGER DEFAULT 0,
            registered_at TEXT,
            trade_size_usd REAL NOT NULL DEFAULT 100.0,
            min_net_profit_usd REAL NOT NULL DEFAULT 5.0,
            min_spread_pct REAL NOT NULL DEFAULT 0.5,
            max_spread_pct REAL NOT NULL DEFAULT 50.0,
            max_results INTEGER NOT NULL DEFAULT 15,
            paused INTEGER NOT NULL DEFAULT 0,
            loose_mode INTEGER NOT NULL DEFAULT 0,
            paper_balance REAL NOT NULL DEFAULT 0.0,
            last_action TEXT,
            last_active TEXT
        )
    """)

    c.execute("PRAGMA table_info(users)")
    existing_cols = {row[1] for row in c.fetchall()}
    required_cols = {
        'trade_size_usd': 'REAL NOT NULL DEFAULT 100.0',
        'min_net_profit_usd': 'REAL NOT NULL DEFAULT 5.0',
        'min_spread_pct': 'REAL NOT NULL DEFAULT 0.5',
        'max_spread_pct': 'REAL NOT NULL DEFAULT 50.0',
        'max_results': 'INTEGER NOT NULL DEFAULT 15',
        'paused': 'INTEGER NOT NULL DEFAULT 0',
        'loose_mode': 'INTEGER NOT NULL DEFAULT 0',
        'paper_balance': 'REAL NOT NULL DEFAULT 0.0',
        'is_banned': 'INTEGER DEFAULT 0',
        'last_action': 'TEXT',
        'last_active': 'TEXT'
    }
    for col, col_def in required_cols.items():
        if col not in existing_cols:
            c.execute(f"ALTER TABLE users ADD COLUMN {col} {col_def}")

    c.execute("""
        CREATE TABLE IF NOT EXISTS access_keys (
            key_code TEXT PRIMARY KEY,
            is_used INTEGER DEFAULT 0,
            used_by INTEGER
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS watchlist (
            user_id INTEGER NOT NULL,
            symbol TEXT NOT NULL,
            created_at TEXT DEFAULT (datetime('now')),
            PRIMARY KEY (user_id, symbol)
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS user_actions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT,
            created_at TEXT
        )
    """)

    c.execute("INSERT OR IGNORE INTO access_keys (key_code) VALUES ('VIP-ALPHA-2026'), ('VIP-BETA-777'), ('VIP-PRO-999')")
    conn.commit()
    conn.close()

def now_ist():
    return datetime.now(IST).strftime("%d %b %Y, %I:%M:%S %p")

def log_action(user_id: int, action: str):
    now = now_ist()
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("INSERT INTO user_actions (user_id, action, created_at) VALUES (?, ?, ?)", (user_id, action, now))
    c.execute("UPDATE users SET last_action = ?, last_active = ? WHERE user_id = ?", (action, now, user_id))
    c.execute("""
        DELETE FROM user_actions 
        WHERE user_id = ? AND id NOT IN (
            SELECT id FROM user_actions WHERE user_id = ? ORDER BY id DESC LIMIT 40
        )
    """, (user_id, user_id))
    conn.commit()
    conn.close()

def get_user_actions(user_id: int, limit: int = 15):
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("SELECT action, created_at FROM user_actions WHERE user_id = ? ORDER BY id DESC LIMIT ?", (user_id, limit))
    rows = c.fetchall()
    conn.close()
    return rows

def get_user_settings(user_id: int):
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("""
        SELECT trade_size_usd, min_net_profit_usd, min_spread_pct, max_spread_pct, 
               max_results, paused, is_banned, loose_mode, paper_balance 
        FROM users WHERE user_id = ?
    """, (user_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    trade_size, min_profit, min_spread, max_spread, max_results, paused, is_banned, loose_mode, paper_balance = row
    c.execute("SELECT symbol FROM watchlist WHERE user_id = ?", (user_id,))
    watchlist = {s.upper() for (s,) in c.fetchall()}
    conn.close()
    return {
        'trade_size_usd': trade_size,
        'min_net_profit_usd': min_profit,
        'min_spread_pct': min_spread,
        'max_spread_pct': max_spread,
        'max_results': max_results,
        'paused': bool(paused),
        'is_banned': bool(is_banned),
        'loose_mode': bool(loose_mode),
        'paper_balance': paper_balance,
        'watchlist': watchlist
    }

def update_user_setting(user_id: int, field: str, amount):
    conn = sqlite3.connect("arbitrage_users.db")
    conn.execute(f"UPDATE users SET {field} = ? WHERE user_id = ?", (amount, user_id)).connection.commit()
    conn.close()

def toggle_loose_mode_db(user_id: int) -> bool:
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("SELECT loose_mode FROM users WHERE user_id = ?", (user_id,))
    current = c.fetchone()[0]
    new_val = 0 if current else 1
    c.execute("UPDATE users SET loose_mode = ? WHERE user_id = ?", (new_val, user_id))
    conn.commit()
    conn.close()
    return bool(new_val)

def add_paper_profit(user_id: int, amount: float):
    conn = sqlite3.connect("arbitrage_users.db")
    conn.execute("UPDATE users SET paper_balance = paper_balance + ? WHERE user_id = ?", (amount, user_id)).connection.commit()
    conn.close()

def get_paper_leaderboard():
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("SELECT username, paper_balance FROM users WHERE paper_balance > 0 ORDER BY paper_balance DESC LIMIT 10")
    rows = c.fetchall()
    conn.close()
    return rows

def is_user_premium(user_id: int) -> bool:
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("SELECT is_premium, is_banned FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    conn.close()
    return bool(row and row[0] == 1 and row[1] == 0)

def activate_user_key(user_id: int, username: str, key_code: str) -> bool:
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("SELECT is_used FROM access_keys WHERE key_code = ?", (key_code,))
    row = c.fetchone()
    if not row or row[0] != 0:
        conn.close()
        return False
    c.execute("UPDATE access_keys SET is_used = 1, used_by = ? WHERE key_code = ?", (user_id, key_code))
    c.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
    if c.fetchone():
        c.execute("UPDATE users SET username=?, is_premium=1, is_banned=0, registered_at=? WHERE user_id=?", (username, now_ist(), user_id))
    else:
        c.execute("INSERT INTO users (user_id, username, is_premium, registered_at) VALUES (?,?,1,?)", (user_id, username, now_ist()))
    conn.commit()
    conn.close()
    return True

def get_all_premium_users():
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE is_premium=1 AND is_banned=0")
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def get_all_users_detailed():
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("SELECT user_id, username, is_premium, is_banned, registered_at, trade_size_usd, min_net_profit_usd, paused, last_action, last_active FROM users ORDER BY last_active DESC NULLS LAST")
    rows = c.fetchall()
    conn.close()
    return rows

def get_user_full_info(user_id: int):
    conn = sqlite3.connect("arbitrage_users.db")
    c = conn.cursor()
    c.execute("SELECT user_id, username, is_premium, is_banned, registered_at, trade_size_usd, min_net_profit_usd, min_spread_pct, max_spread_pct, max_results, paused, last_action, last_active, loose_mode, paper_balance FROM users WHERE user_id = ?", (user_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
    c.execute("SELECT symbol FROM watchlist WHERE user_id = ?", (user_id,))
    watchlist = [r[0] for r in c.fetchall()]
    conn.close()
    return {
        "user_id": row[0], "username": row[1], "is_premium": bool(row[2]), "is_banned": bool(row[3]), "registered_at": row[4],
        "trade_size_usd": row[5], "min_net_profit_usd": row[6], "min_spread_pct": row[7], "max_spread_pct": row[8],
        "max_results": row[9], "paused": bool(row[10]), "last_action": row[11], "last_active": row[12], 
        "loose_mode": bool(row[13]), "paper_balance": row[14], "watchlist": watchlist
    }

# ==========================================
# 3. MARKET ENGINE
# ==========================================
ccxt_instances = {
    'gate': ccxt_async.gate({'enableRateLimit': True, 'timeout': 25000, 'options': {'defaultType': 'spot'}}),
    'lbank': ccxt_async.lbank({'enableRateLimit': True, 'timeout': 25000}),
    'bitrue': ccxt_async.bitrue({'enableRateLimit': True, 'timeout': 25000}),
    'xt': ccxt_async.xt({'enableRateLimit': True, 'timeout': 25000}),
    'ascendex': ccxt_async.ascendex({'enableRateLimit': True, 'timeout': 25000}),
    'poloniex': ccxt_async.poloniex({'enableRateLimit': True, 'timeout': 25000}),
    'bingx': ccxt_async.bingx({'enableRateLimit': True, 'timeout': 25000}),
    'digifinex': ccxt_async.digifinex({'enableRateLimit': True, 'timeout': 25000}),
    'binance': ccxt_async.binance({'enableRateLimit': True, 'timeout': 25000}),
    'bybit': ccxt_async.bybit({'enableRateLimit': True, 'timeout': 25000}),
    'okx': ccxt_async.okx({'enableRateLimit': True, 'timeout': 25000}),
    'kucoin': ccxt_async.kucoin({'enableRateLimit': True, 'timeout': 25000}),
    'mexc': ccxt_async.mexc({'enableRateLimit': True, 'timeout': 25000}),
    'bitget': ccxt_async.bitget({'enableRateLimit': True, 'timeout': 25000}),
    'htx': ccxt_async.htx({'enableRateLimit': True, 'timeout': 25000}),
    'kraken': ccxt_async.kraken({'enableRateLimit': True, 'timeout': 25000}),
    'bitfinex': ccxt_async.bitfinex({'enableRateLimit': True, 'timeout': 25000}),
}

async def load_universal_symbols():
    global UNIVERSAL_SYMBOLS, SYMBOL_EXCHANGE_MAP, CURRENCY_STATUS, CONTRACT_ADDRESSES
    print("Loading markets across all exchanges...")
    exchange_markets = {}
    
    for name, obj in ccxt_instances.items():
        try:
            markets = await obj.load_markets()
            spot_symbols = {s for s, m in markets.items() if m.get('spot') and m.get('quote') == 'USDT' and m.get('active') is not False}
            exchange_markets[name] = spot_symbols
            
            try:
                currencies = await obj.fetch_currencies()
                status = {}
                contracts = {}
                if isinstance(currencies, dict):
                    for code, cur in currencies.items():
                        if not isinstance(cur, dict):
                            continue
                        code_up = code.upper()
                        deposit = cur.get('deposit', cur.get('active'))
                        withdraw = cur.get('withdraw', cur.get('active'))
                        info = cur.get('info') or {}
                        if isinstance(info, dict):
                            for k in ['depositEnable', 'deposit_enable', 'deposit']:
                                if k in info: deposit = str(info[k]).lower() in ('1', 'true', 'yes', 'enabled')
                            for k in ['withdrawEnable', 'withdraw_enable', 'withdraw']:
                                if k in info: withdraw = str(info[k]).lower() in ('1', 'true', 'yes', 'enabled')
                        status[code_up] = {'deposit': deposit, 'withdraw': withdraw}

                        contract = None
                        networks = cur.get('networks') or {}
                        if isinstance(networks, dict):
                            for net in networks.values():
                                if isinstance(net, dict):
                                    addr = net.get('contractAddress') or net.get('address')
                                    if addr:
                                        contract = str(addr).lower()
                                        break
                        if contract:
                            contracts[code_up] = contract
                CURRENCY_STATUS[name] = status
                CONTRACT_ADDRESSES[name] = contracts
            except Exception:
                CURRENCY_STATUS[name] = {}
                CONTRACT_ADDRESSES[name] = {}
        except Exception:
            exchange_markets[name] = set()

    symbol_to_exchanges = {}
    for name, syms in exchange_markets.items():
        for s in syms:
            symbol_to_exchanges.setdefault(s, set()).add(name)
    SYMBOL_EXCHANGE_MAP = {s: exs for s, exs in symbol_to_exchanges.items() if len(exs) >= 2}
    UNIVERSAL_SYMBOLS = sorted(SYMBOL_EXCHANGE_MAP.keys())
    print(f"Universal pairs loaded: {len(UNIVERSAL_SYMBOLS)}")

async def fetch_ccxt_ticker(exchange_name, exchange_obj, symbol):
    try:
        ticker = await exchange_obj.fetch_ticker(symbol)
        if ticker and ticker.get('last'):
            volume = float(ticker.get('quoteVolume') or (float(ticker.get('baseVolume', 0)) * float(ticker['last'])))
            return exchange_name, float(ticker['last']), volume
    except Exception:
        pass
    return exchange_name, None, 0.0

async def scan_symbol_prices(symbol: str):
    known = SYMBOL_EXCHANGE_MAP.get(symbol, set(ccxt_instances.keys()))
    tasks = [fetch_ccxt_ticker(n, o, symbol) for n, o in ccxt_instances.items() if n in known]
    results = await asyncio.gather(*tasks) if tasks else []
    prices, volumes = {}, []
    for name, price, vol in results:
        if price is not None:
            prices[name] = price
            if vol > 0: volumes.append(vol)
    if volumes and max(volumes) < MIN_24H_VOLUME_USD: return {}
    return prices

def can_transfer(coin: str, buy_ex: str, sell_ex: str) -> bool:
    coin = coin.upper()
    buy_s = CURRENCY_STATUS.get(buy_ex, {}).get(coin)
    sell_s = CURRENCY_STATUS.get(sell_ex, {}).get(coin)
    if not buy_s or not sell_s: return False
    return buy_s.get('withdraw') is True and sell_s.get('deposit') is True

def contracts_match(coin: str, buy_ex: str, sell_ex: str) -> bool:
    coin = coin.upper()
    a1 = CONTRACT_ADDRESSES.get(buy_ex, {}).get(coin)
    a2 = CONTRACT_ADDRESSES.get(sell_ex, {}).get(coin)
    if not a1 or not a2: return not STRICT_CONTRACT_MATCH
    return a1 == a2

def calculate_net_arbitrage(symbol: str, prices: dict, trade_size_usd: float, loose_mode: bool = False):
    if len(prices) < 2: return None
    base = symbol.split('/')[0]
    buy_ex = min(prices, key=prices.get)
    sell_ex = max(prices, key=prices.get)
    buy_price = prices[buy_ex]
    sell_price = prices[sell_ex]

    if not loose_mode:
        if not can_transfer(base, buy_ex, sell_ex): return None
        if not contracts_match(base, buy_ex, sell_ex): return None

    coin_amount = trade_size_usd / buy_price
    gross = coin_amount * sell_price - trade_size_usd
    buy_fee = trade_size_usd * EXCHANGE_CONFIG.get(buy_ex, {}).get('fee', 0.002)
    sell_fee = (coin_amount * sell_price) * EXCHANGE_CONFIG.get(sell_ex, {}).get('fee', 0.002)
    wd_fee = EXCHANGE_CONFIG.get(buy_ex, {}).get('withdraw_fees', {}).get(base, GENERIC_WITHDRAW_FEE_COIN_UNITS) * sell_price
    net = gross - buy_fee - sell_fee - wd_fee
    spread = (net / trade_size_usd) * 100 if trade_size_usd else 0

    return {
        'symbol': symbol, 'buy_ex': buy_ex.upper(), 'buy_price': buy_price,
        'sell_ex': sell_ex.upper(), 'sell_price': sell_price, 'coin_amount': coin_amount, 
        'gross_profit': gross, 'buy_fee': buy_fee, 'sell_fee': sell_fee, 'withdraw_fee': wd_fee,
        'net_profit': net, 'net_spread_pct': spread, 'trade_size': trade_size_usd, 'loose_mode': loose_mode
    }

def format_detailed_alert(arb: dict) -> str:
    loose_warn = "\n⚠️ **LOOSE MODE ACTIVE: Verify deposits & contracts manually!**\n" if arb.get('loose_mode') else ""
    return f"""🚨 **HIGH-MARGIN ARBITRAGE**{loose_warn}

**Pair:** `{arb['symbol']}`
━━━━━━━━━━━━━━━━━━━━
🟢 **BUY**
   Exchange : `{arb['buy_ex']}`
   Price    : `${arb['buy_price']:.6f}`

🔴 **SELL**
   Exchange : `{arb['sell_ex']}`
   Price    : `${arb['sell_price']:.6f}`
━━━━━━━━━━━━━━━━━━━━
📊 **Profit Breakdown**
• Gross Profit   : `${arb['gross_profit']:.2f}`
• Trading Fees   : `- ${arb['buy_fee'] + arb['sell_fee']:.2f}`
• Withdrawal Fee : `- ${arb['withdraw_fee']:.2f}`
• **Net Profit** : `${arb['net_profit']:.2f}`
• **Net Spread** : `{arb['net_spread_pct']:.2f}%`
━━━━━━━━━━━━━━━━━━━━
📋 **Extra Details**
• Trade Size     : `${arb['trade_size']:.2f}`
• Coin Amount    : `{arb['coin_amount']:.6f}`"""

async def get_orderbook_text(symbol: str) -> str:
    known = list(SYMBOL_EXCHANGE_MAP.get(symbol, ccxt_instances.keys()))[:3]
    lines = [f"📖 **Order Book: {symbol}**\n"]
    for name in known:
        try:
            ob = await ccxt_instances[name].fetch_order_book(symbol, limit=6)
            lines.append(f"**{name.upper()}**\nBuy (Bids):")
            for p, v in ob.get('bids', [])[:5]: lines.append(f"`{p:.5f}` × {v:.4f}")
            lines.append("Sell (Asks):")
            for p, v in ob.get('asks', [])[:5]: lines.append(f"`{p:.5f}` × {v:.4f}")
            lines.append("")
        except Exception:
            continue
    if len(lines) <= 1: return f"Could not fetch order book for `{symbol}`"
    return "\n".join(lines)

async def scan_all_symbols(min_profit=None, min_spread=None, max_spread=None, symbols=None, trade_size=100.0, loose_mode=False):
    target = symbols if symbols is not None else UNIVERSAL_SYMBOLS
    semaphore = asyncio.Semaphore(SCAN_CONCURRENCY)
    results = []

    async def _scan(sym):
        async with semaphore:
            prices = await scan_symbol_prices(sym)
            arb = calculate_net_arbitrage(sym, prices, trade_size, loose_mode)
            if arb:
                if min_profit is not None and arb['net_profit'] < min_profit: return
                if min_spread is not None and arb['net_spread_pct'] < min_spread: return
                if max_spread is not None and arb['net_spread_pct'] > max_spread: return
                results.append(arb)

    await asyncio.gather(*[_scan(s) for s in target])
    results.sort(key=lambda x: x['net_profit'], reverse=True)
    return results

async def fetch_all_prices(symbols=None):
    target = symbols if symbols is not None else UNIVERSAL_SYMBOLS
    semaphore = asyncio.Semaphore(SCAN_CONCURRENCY)
    prices_map = {}
    async def _f(sym):
        async with semaphore:
            p = await scan_symbol_prices(sym)
            if p: prices_map[sym] = p
    await asyncio.gather(*[_f(s) for s in target])
    return prices_map

# ==========================================
# 4. HANDLERS (Users)
# ==========================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/start")

    if is_user_premium(uid):
        kb = [[InlineKeyboardButton("⚡ Run Scan", callback_data="run_manual_scan"), InlineKeyboardButton("⚙️ Filters", callback_data="show_filters")]]
        
        exchange_names = " • ".join([k.capitalize() for k in ccxt_instances.keys()])
        
        text = f"""👑 **Arbitrage Terminal Active**

Tracked pairs: `{len(UNIVERSAL_SYMBOLS)}`
Exchanges ({len(ccxt_instances)}):
{exchange_names}

📌 **Quick Commands:**
`/scan` - Find opportunities
`/filters` - View settings
`/loosemode` - Toggle contract verification
`/portfolio` - Paper trading stats
`/help` - Full command list"""

        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")
    else:
        await update.message.reply_text("🔒 **Access Required**\n\nUse:\n`/register YOUR_KEY`", parse_mode="Markdown")

async def register_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/register")
    if not context.args: return await update.message.reply_text("⚠️ Usage:\n`/register YOUR_KEY`", parse_mode="Markdown")
    if activate_user_key(uid, update.effective_user.username or "Anon", context.args[0]):
        await update.message.reply_text("🎉 **Registration Successful!** Type `/start` to begin.", parse_mode="Markdown")
    else: await update.message.reply_text("❌ Invalid or already used key.")

async def scan_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message or update.callback_query.message
    if update.callback_query: await update.callback_query.answer()
    
    uid = update.effective_user.id
    log_action(uid, "/scan")
    if not is_user_premium(uid): return await message.reply_text("🔒 Premium required.")

    settings = get_user_settings(uid)
    if not settings: return await message.reply_text("Settings not found.")

    status = await message.reply_text(f"🔍 Scanning markets across {len(ccxt_instances)} exchanges...")
    results = await scan_all_symbols(
        min_profit=settings['min_net_profit_usd'], min_spread=settings['min_spread_pct'],
        max_spread=settings['max_spread_pct'], symbols=list(settings['watchlist']) if settings['watchlist'] else None,
        trade_size=settings['trade_size_usd'], loose_mode=settings['loose_mode']
    )

    if not results: return await status.edit_text("No opportunities found. Relax filters or try `/loosemode`.")

    top = results[:settings['max_results']]
    await status.edit_text(f"📊 Found **{len(results)}** opportunities (showing {len(top)})")

    for i, arb in enumerate(top, 1):
        text = f"**#{i}**\n" + format_detailed_alert(arb)
        kb = [[InlineKeyboardButton("📖 View Order Book", callback_data=f"ob:{arb['symbol']}")],
              [InlineKeyboardButton("🎮 Paper Trade This!", callback_data=f"pt:{arb['net_profit']:.2f}")]]
        try:
            await message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")
            await asyncio.sleep(0.35)
        except Exception:
            pass

async def loosemode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/loosemode")
    if not is_user_premium(uid): return await update.message.reply_text("🔒 Premium required.")
    
    if toggle_loose_mode_db(uid):
        await update.message.reply_text("🔓 **Loose Mode ENABLED**\nBot will skip network & contract checks. You must verify manually before trading!")
    else:
        await update.message.reply_text("🔒 **Loose Mode DISABLED**\nBot is now verifying deposits, withdrawals, and contract matches.")

async def portfolio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/portfolio")
    if not is_user_premium(uid): return await update.message.reply_text("🔒 Premium required.")
    
    settings = get_user_settings(uid)
    await update.message.reply_text(f"🎮 **Your Paper Trading Portfolio**\n\nTotal Virtual Profit: **${settings['paper_balance']:.2f}**\n\nUse `/leaderboard` to see top traders!", parse_mode="Markdown")

async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_user_premium(update.effective_user.id): return await update.message.reply_text("🔒 Premium required.")
    leaders = get_paper_leaderboard()
    if not leaders: return await update.message.reply_text("No paper trades recorded yet!")
    lines = ["🏆 **Paper Trading Leaderboard**\n"]
    for i, (name, bal) in enumerate(leaders, 1): lines.append(f"{i}. @{name or 'Anon'} - **${bal:.2f}**")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def orderbook_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_user_premium(update.effective_user.id): return await update.message.reply_text("🔒 Premium required.")
    if not context.args: return await update.message.reply_text("⚠️ Usage:\n`/ob BTC/USDT`", parse_mode="Markdown")
    msg = await update.message.reply_text(f"📖 Fetching order book for `{context.args[0].upper()}`...")
    await msg.edit_text(await get_orderbook_text(context.args[0].upper()), parse_mode="Markdown")

async def filters_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/filters")
    if not is_user_premium(uid): return await update.message.reply_text("🔒 Premium required.")
    s = get_user_settings(uid)
    text = f"""⚙️ **Your Settings**

• Trade Size   : `${s['trade_size_usd']:.0f}`
• Min Profit   : `${s['min_net_profit_usd']:.1f}`
• Min Spread   : `{s['min_spread_pct']:.1f}%`
• Max Spread   : `{s['max_spread_pct']:.1f}%`
• Loose Mode   : `{'ON 🔓' if s['loose_mode'] else 'OFF 🔒'}`
• Alerts Status: `{'Paused' if s['paused'] else 'Active'}`
• Watchlist    : `{len(s['watchlist'])} coins`
• Paper Balance: `${s['paper_balance']:.2f}`

📋 **Commands:** `/settradesize 100`, `/setminprofit 5`, `/setminspread 0.8`, `/loosemode`, `/pause`, `/watchlist`"""
    await update.message.reply_text(text, parse_mode="Markdown")

async def set_value(update, context, field, type_func, name):
    uid = update.effective_user.id
    if not context.args: return await update.message.reply_text(f"⚠️ Example: `/{update.message.text.split()[0][1:]} 5`")
    update_user_setting(uid, field, type_func(context.args[0]))
    await update.message.reply_text(f"✅ {name} set to **{context.args[0]}**")

async def setminprofit_command(update: Update, context: ContextTypes.DEFAULT_TYPE): await set_value(update, context, 'min_net_profit_usd', float, "Min net profit")
async def setminspread_command(update: Update, context: ContextTypes.DEFAULT_TYPE): await set_value(update, context, 'min_spread_pct', float, "Min spread")
async def setmaxspread_command(update: Update, context: ContextTypes.DEFAULT_TYPE): await set_value(update, context, 'max_spread_pct', float, "Max spread")
async def setmaxresults_command(update: Update, context: ContextTypes.DEFAULT_TYPE): await set_value(update, context, 'max_results', int, "Max results")
async def settradesize_command(update: Update, context: ContextTypes.DEFAULT_TYPE): await set_value(update, context, 'trade_size_usd', float, "Trade size")

async def watch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_user_premium(uid): return await update.message.reply_text("🔒 Premium required.")
    if not context.args: return await update.message.reply_text("⚠️ Usage:\n`/watch BTC/USDT`", parse_mode="Markdown")
    symbol = context.args[0].upper()
    conn = sqlite3.connect("arbitrage_users.db")
    try:
        conn.execute("INSERT INTO watchlist (user_id, symbol) VALUES (?, ?)", (uid, symbol))
        conn.commit()
        await update.message.reply_text(f"✅ Added `{symbol}` to your watchlist.", parse_mode="Markdown")
    except sqlite3.IntegrityError:
        await update.message.reply_text(f"⚠️ `{symbol}` is already in your watchlist.", parse_mode="Markdown")
    finally:
        conn.close()

async def unwatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_user_premium(uid): return await update.message.reply_text("🔒 Premium required.")
    if not context.args: return await update.message.reply_text("⚠️ Usage:\n`/unwatch BTC/USDT`", parse_mode="Markdown")
    symbol = context.args[0].upper()
    conn = sqlite3.connect("arbitrage_users.db")
    conn.execute("DELETE FROM watchlist WHERE user_id = ? AND symbol = ?", (uid, symbol))
    if conn.total_changes > 0:
        await update.message.reply_text(f"🗑️ Removed `{symbol}` from your watchlist.", parse_mode="Markdown")
    else:
        await update.message.reply_text(f"⚠️ `{symbol}` was not in your watchlist.", parse_mode="Markdown")
    conn.commit()
    conn.close()

async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_user_setting(update.effective_user.id, 'paused', 1)
    await update.message.reply_text("⏸ Background alerts **paused**.")

async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    update_user_setting(update.effective_user.id, 'paused', 0)
    await update.message.reply_text("▶️ Background alerts **resumed**.")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """🤖 **Full Command List**

🔍 **Core Commands**
`/scan` - Find arbitrage opportunities
`/loosemode` - Toggle strict contract verification
`/ob BTC/USDT` - View order book for a coin

🎮 **Paper Trading**
`/portfolio` - View paper trading stats
`/leaderboard` - View top paper traders

⚙️ **Filters & Settings**
`/filters` - View your active settings
`/settradesize 100` - Set trade size in USD
`/setminprofit 5` - Set min profit in USD
`/setminspread 0.8` - Set min spread percentage
`/setmaxspread 50` - Set max spread percentage
`/setmaxresults 15` - Set max results to show

🔔 **Watchlist & Alerts**
`/watch BTC/USDT` - Add coin to watchlist
`/unwatch BTC/USDT` - Remove coin from watchlist
`/pause` - Pause background alerts
`/resume` - Resume background alerts"""
    await update.message.reply_text(text, parse_mode="Markdown")


# ==========================================
# 5. HANDLERS (Admin - Complete Set)
# ==========================================
async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ Unauthorized")
    users = get_all_users_detailed()
    lines = [f"**Users: {len(users)}**\n"]
    for u in users[:30]:
        status = "🚫" if u[3] else ("✅" if u[2] else "❌")
        lines.append(f"`{u[0]}` @{u[1] or '—'} {status} | {u[9] or 'never'}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def userinfo_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ `/userinfo secret user_id`")
    try: target = int(context.args[1])
    except ValueError: return await update.message.reply_text("Invalid ID")
    info = get_user_full_info(target)
    if not info: return await update.message.reply_text("User not found")
    actions = "\n".join([f"`{a[1]}` → {a[0]}" for a in get_user_actions(target, 12)]) or "None"
    text = f"""👤 `{info['user_id']}` @{info['username'] or '—'}
Status: {'🚫 Banned' if info['is_banned'] else ('Premium' if info['is_premium'] else 'Normal')}
Loose Mode: {'ON' if info['loose_mode'] else 'OFF'} | Paper Balance: ${info['paper_balance']:.2f}
Last active: `{info['last_active']}`

📜 Last actions:
{actions}"""
    await update.message.reply_text(text, parse_mode="Markdown")

async def ban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ `/ban secret user_id`")
    conn = sqlite3.connect("arbitrage_users.db")
    conn.execute("UPDATE users SET is_banned = 1, is_premium = 0 WHERE user_id = ?", (int(context.args[1]),)).connection.commit()
    conn.close()
    await update.message.reply_text(f"🚫 User `{context.args[1]}` banned.")

async def unban_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ `/unban secret user_id`")
    conn = sqlite3.connect("arbitrage_users.db")
    conn.execute("UPDATE users SET is_banned = 0 WHERE user_id = ?", (int(context.args[1]),)).connection.commit()
    conn.close()
    await update.message.reply_text(f"✅ User `{context.args[1]}` unbanned.")

async def revoke_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ `/revoke secret user_id`")
    uid = int(context.args[1])
    conn = sqlite3.connect("arbitrage_users.db")
    conn.execute("UPDATE users SET is_premium = 0, is_banned = 0 WHERE user_id = ?", (uid,))
    conn.execute("UPDATE access_keys SET is_used = 0, used_by = NULL WHERE used_by = ?", (uid,)).connection.commit()
    conn.close()
    await update.message.reply_text(f"🔒 Access of `{uid}` revoked.")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ Unauthorized")
    users = get_all_users_detailed()
    prem = sum(1 for u in users if u[2] and not u[3])
    banned = sum(1 for u in users if u[3])
    await update.message.reply_text(f"📊 Total: {len(users)} | Premium: {prem} | Banned: {banned} | Pairs: {len(UNIVERSAL_SYMBOLS)}")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ Unauthorized")
    msg = " ".join(context.args[1:])
    ok = fail = 0
    for uid in get_all_premium_users():
        try:
            await context.bot.send_message(uid, f"📢 **Admin Message**\n\n{msg}", parse_mode="Markdown")
            ok += 1
            await asyncio.sleep(0.04)
        except Exception:
            fail += 1
    await update.message.reply_text(f"✅ Sent: {ok} | Failed: {fail}")

async def sendto_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 3 or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ `/sendto secret user_id message`")
    try:
        await context.bot.send_message(int(context.args[1]), f"🔒 **Admin Message**\n\n{' '.join(context.args[2:])}", parse_mode="Markdown")
        await update.message.reply_text("✅ Sent")
    except Exception as e: await update.message.reply_text(f"❌ {e}")

async def generate_key_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ Unauthorized")
    key = context.args[1]
    conn = sqlite3.connect("arbitrage_users.db")
    conn.execute("INSERT OR IGNORE INTO access_keys (key_code) VALUES (?)", (key,)).connection.commit()
    conn.close()
    await update.message.reply_text(f"✅ New key created:\n`{key}`", parse_mode="Markdown")

async def givepremium_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ Usage: `/givepremium secret user_id`")
    try:
        target_uid = int(context.args[1])
        conn = sqlite3.connect("arbitrage_users.db")
        conn.execute("INSERT OR IGNORE INTO users (user_id, is_premium, registered_at) VALUES (?, 1, ?)", (target_uid, now_ist()))
        conn.execute("UPDATE users SET is_premium = 1, is_banned = 0 WHERE user_id = ?", (target_uid,)).connection.commit()
        conn.close()
        await update.message.reply_text(f"✅ Successfully granted premium access to user `{target_uid}`.", parse_mode="Markdown")
        await context.bot.send_message(target_uid, "🎉 **An Admin has granted you Premium Access!**\nType /start to begin.", parse_mode="Markdown")
    except Exception as e: await update.message.reply_text(f"❌ Error: {e}")

async def deluser_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if len(context.args) < 2 or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ Usage: `/deluser secret user_id`")
    try:
        target_uid = int(context.args[1])
        conn = sqlite3.connect("arbitrage_users.db")
        conn.execute("DELETE FROM users WHERE user_id = ?", (target_uid,))
        conn.execute("DELETE FROM watchlist WHERE user_id = ?", (target_uid,))
        conn.execute("DELETE FROM user_actions WHERE user_id = ?", (target_uid,)).connection.commit()
        conn.close()
        await update.message.reply_text(f"🗑️ User `{target_uid}` completely wiped.", parse_mode="Markdown")
    except Exception as e: await update.message.reply_text(f"❌ Error: {e}")

async def backup_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ Usage: `/backup secret`")
    try:
        await update.message.reply_document(document=open("arbitrage_users.db", "rb"), filename=f"arbitrage_backup_{int(time.time())}.db")
    except Exception as e: await update.message.reply_text(f"❌ Failed: {e}")


# ==========================================
# 6. CLOUD PORT FIX / HEALTH SERVER (THREADED)
# ==========================================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(b"Bot is healthy!")
        
    def log_message(self, format, *args):
        pass # Suppress logging to keep console clean

def run_health_server():
    port = int(os.environ.get("PORT", 8080))
    server = HTTPServer(('0.0.0.0', port), HealthCheckHandler)
    print(f"✅ Cloud Health-Check Server listening on port {port}")
    server.serve_forever()


# ==========================================
# 7. ROUTER & DAEMON
# ==========================================
async def button_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    uid = update.effective_user.id
    data = query.data

    if data == "run_manual_scan":
        await query.answer()
        await scan_command(update, context)
    elif data == "show_filters":
        await query.answer()
        class FakeUpdate:
            def __init__(self, original):
                self.message = original.callback_query.message
                self.effective_user = original.effective_user
        await filters_command(FakeUpdate(update), context)
    elif data.startswith("ob:"):
        await query.answer()
        symbol = data[3:]
        await query.message.reply_text(f"📖 Loading `{symbol}`...")
        await query.message.reply_text(await get_orderbook_text(symbol), parse_mode="Markdown")
    elif data.startswith("pt:"):
        prof = float(data.split(":")[1])
        add_paper_profit(uid, prof)
        
        kb = query.message.reply_markup.inline_keyboard
        new_kb = []
        for row in kb:
            new_row = []
            for btn in row:
                if btn.callback_data == data:
                    new_row.append(InlineKeyboardButton(f"✅ Executed (+${prof:.2f})", callback_data="ignore"))
                else:
                    new_row.append(btn)
            new_kb.append(new_row)
            
        await query.message.edit_reply_markup(InlineKeyboardMarkup(new_kb))
        await query.answer(f"✅ Successfully paper traded! Earned ${prof:.2f}", show_alert=True)
    elif data == "ignore":
        await query.answer("You already traded this opportunity!", show_alert=False)

async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    print(f"Error caught in telegram dispatcher: {context.error}")

async def background_arbitrage_daemon(app):
    await asyncio.sleep(4)
    await load_universal_symbols()
    last_refresh = time.time()

    while True:
        try:
            if time.time() - last_refresh > CURRENCY_REFRESH_SECONDS:
                await load_universal_symbols()
                last_refresh = time.time()

            users = get_all_premium_users()
            if users and UNIVERSAL_SYMBOLS:
                prices_map = await fetch_all_prices()
                for uid in users:
                    settings = get_user_settings(uid)
                    if not settings or settings['paused'] or settings['is_banned']: continue
                    alerts = []
                    for sym, prices in prices_map.items():
                        if settings['watchlist'] and sym not in settings['watchlist']: continue
                        arb = calculate_net_arbitrage(sym, prices, settings['trade_size_usd'], settings['loose_mode'])
                        if (arb and arb['net_profit'] >= settings['min_net_profit_usd'] and
                            arb['net_spread_pct'] >= settings['min_spread_pct'] and arb['net_spread_pct'] <= settings['max_spread_pct']):
                            alerts.append(arb)
                            
                    alerts.sort(key=lambda x: x['net_profit'], reverse=True)
                    for arb in alerts[:5]:
                        kb = [[InlineKeyboardButton("📖 View Order Book", callback_data=f"ob:{arb['symbol']}")],
                              [InlineKeyboardButton("🎮 Paper Trade This!", callback_data=f"pt:{arb['net_profit']:.2f}")]]
                        try:
                            await app.bot.send_message(uid, format_detailed_alert(arb), reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")
                            await asyncio.sleep(1.2)
                        except Exception:
                            pass
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
        except Exception as e:
            print(f"Daemon error: {e}")
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def post_init(app):
    commands = [
        BotCommand("start", "Start the bot"), BotCommand("scan", "Find arbitrage opportunities"),
        BotCommand("loosemode", "Toggle strict contract checks"), BotCommand("portfolio", "View paper trading stats"),
        BotCommand("leaderboard", "View top traders"), BotCommand("ob", "View order book of a coin"),
        BotCommand("filters", "View & change your settings"), BotCommand("pause", "Pause alerts"),
        BotCommand("resume", "Resume alerts"), BotCommand("help", "Show all commands"),
    ]
    await app.bot.set_my_commands(commands)
    print("✅ Command menu registered")
    asyncio.create_task(background_arbitrage_daemon(app))

async def post_shutdown(app):
    for o in ccxt_instances.values():
        try: await o.close()
        except Exception: pass

def main():
    init_db()
    
    # Start the robust threaded web server for Render health checks
    threading.Thread(target=run_health_server, daemon=True).start()
    
    app = (ApplicationBuilder().token(BOT_TOKEN).connect_timeout(30).read_timeout(30).write_timeout(30).post_init(post_init).post_shutdown(post_shutdown).build())

    handlers = [
        ("start", start_command), ("register", register_command), ("help", help_command),
        ("scan", scan_command), ("ob", orderbook_command), ("loosemode", loosemode_command), 
        ("portfolio", portfolio_command), ("leaderboard", leaderboard_command), ("filters", filters_command), 
        ("setminprofit", setminprofit_command), ("setminspread", setminspread_command),
        ("setmaxspread", setmaxspread_command), ("setmaxresults", setmaxresults_command),
        ("settradesize", settradesize_command), ("pause", pause_command), ("resume", resume_command),
        ("watch", watch_command), ("unwatch", unwatch_command),
        
        ("users", users_command), ("userinfo", userinfo_command), ("ban", ban_command),
        ("unban", unban_command), ("revoke", revoke_command), ("stats", stats_command),
        ("broadcast", broadcast_command), ("sendto", sendto_command), ("generatekey", generate_key_command),
        ("givepremium", givepremium_command), ("deluser", deluser_command), ("backup", backup_command)
    ]
    for cmd, func in handlers: app.add_handler(CommandHandler(cmd, func))

    app.add_handler(CallbackQueryHandler(button_router))
    app.add_error_handler(error_handler)

    print("Bot started (All 17 exchanges, Loose Mode, Paper Trading & Admin controls active)...")
    app.run_polling()

if __name__ == "__main__":
    main()
