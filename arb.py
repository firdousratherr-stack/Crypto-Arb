import asyncio
import os
import sqlite3
import time
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
BOT_TOKEN = "8848406877:AAHuBsI_IXmFTvVg8EKu-r7XZm9Gy9uYTfA"          # ← Replace with your real token
ADMIN_SECRET = "X"

SCAN_INTERVAL_SECONDS = 30
DEFAULT_TRADE_SIZE_USD = 100.0
DEFAULT_MIN_PROFIT_USER = 5.0
DEFAULT_MIN_SPREAD_PCT = 0.5
DEFAULT_MAX_SPREAD_PCT = 50.0
DEFAULT_MAX_RESULTS = 15
SCAN_CONCURRENCY = 8

MIN_24H_VOLUME_USD = 40000
GENERIC_WITHDRAW_FEE_COIN_UNITS = 1.0
STRICT_CONTRACT_MATCH = False
CURRENCY_REFRESH_SECONDS = 1200

IST = timezone(timedelta(hours=5, minutes=30))

EXCHANGE_CONFIG = {
    'gate': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.0001, 'ETH': 0.002, 'SOL': 0.01, 'XRP': 0.5, 'DOGE': 5.0}},
    'lbank': {'fee': 0.001, 'withdraw_fees': {'BTC': 0.0002, 'ETH': 0.003, 'SOL': 0.015, 'XRP': 1.0, 'DOGE': 10.0}},
    'bitrue': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.00015, 'ETH': 0.0025, 'SOL': 0.01, 'XRP': 0.2, 'DOGE': 6.0}},
    'xt': {'fee': 0.002, 'withdraw_fees': {'BTC': 0.0002, 'ETH': 0.003, 'SOL': 0.02, 'XRP': 0.8, 'DOGE': 8.0}}
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
}

async def load_universal_symbols():
    global UNIVERSAL_SYMBOLS, SYMBOL_EXCHANGE_MAP, CURRENCY_STATUS, CONTRACT_ADDRESSES
    print("Loading markets...")
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
                for code, cur in currencies.items():
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
            except Exception as e:
                CURRENCY_STATUS[name] = {}
                CONTRACT_ADDRESSES[name] = {}
        except Exception as e:
            exchange_markets[name] = set()

    symbol_to_exchanges = {}
    for name, syms in exchange_markets.items():
        for s in syms:
            symbol_to_exchanges.setdefault(s, set()).add(name)
    SYMBOL_EXCHANGE_MAP = {s: exs for s, exs in symbol_to_exchanges.items() if len(exs) >= 2}
    UNIVERSAL_SYMBOLS = sorted(SYMBOL_EXCHANGE_MAP.keys())

async def fetch_ccxt_ticker(exchange_name, exchange_obj, symbol):
    try:
        ticker = await exchange_obj.fetch_ticker(symbol)
        if ticker and ticker.get('last'):
            volume = float(ticker.get('quoteVolume') or (float(ticker.get('baseVolume', 0)) * float(ticker['last'])))
            return exchange_name, float(ticker['last']), volume
    except: pass
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

    # LOOSE MODE: If True, completely ignores the withdrawal/deposit & contract match checks.
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
    return (
        f"🚨 **HIGH-MARGIN ARBITRAGE**{loose_warn}\n\n"
        f"**Pair:** `{arb['symbol']}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"🟢 **BUY**\n   Exchange : `{arb['buy_ex']}`\n   Price    : `${arb['buy_price']:.6f}`\n\n"
        f"🔴 **SELL**\n   Exchange : `{arb['sell_ex']}`\n   Price    : `${arb['sell_price']:.6f}`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 **Profit Breakdown**\n"
        f"• Gross Profit   : `${arb['gross_profit']:.2f}`\n"
        f"• Trading Fees   : `- ${arb['buy_fee'] + arb['sell_fee']:.2f}`\n"
        f"• Withdrawal Fee : `- ${arb['withdraw_fee']:.2f}`\n"
        f"• **Net Profit** : `${arb['net_profit']:.2f}`\n"
        f"• **Net Spread** : `{arb['net_spread_pct']:.2f}%`\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📋 **Extra Details**\n"
        f"• Trade Size     : `${arb['trade_size']:.2f}`\n"
        f"• Coin Amount    : `{arb['coin_amount']:.6f}`\n"
    )

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
        except: continue
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
        text = (
            f"👑 **Arbitrage Terminal Active**\n\nTracked pairs: `{len(UNIVERSAL_SYMBOLS)}`\n"
            f"Exchanges: Gate • LBank • Bitrue • XT\n\n📌 **Quick Commands:**\n"
            f"`/scan` 
