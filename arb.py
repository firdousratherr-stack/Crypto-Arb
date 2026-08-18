import asyncio
import os
import sqlite3
import time
import logging
from datetime import datetime, timezone, timedelta
import ccxt.async_support as ccxt_async
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    ContextTypes,
    CallbackQueryHandler
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==========================================
# 1. CONFIGURATION
# ==========================================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8848406877:AAHuBsI_IXmFTvVg8EKu-r7XZm9Gy9uYTfA")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "X")
ADMIN_USER_ID = 1140410671  # Hardcoded Premium Admin ID

SCAN_INTERVAL_SECONDS = 25
DEFAULT_TRADE_SIZE_USD = 100.0
MIN_24H_VOLUME_USD = 30000
GENERIC_WITHDRAW_FEE_COIN_UNITS = 1.0
CURRENCY_REFRESH_SECONDS = 3600

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

# In-Memory Cache Structures
UNIVERSAL_SYMBOLS = []
SYMBOL_EXCHANGE_MAP = {}
GLOBAL_PRICE_CACHE = {}
LAST_PRICE_UPDATE = 0
CACHE_LOCK = asyncio.Lock()
ccxt_instances = {}

# ==========================================
# 2. DATABASE & STATE MANAGEMENT
# ==========================================
def get_db():
    conn = sqlite3.connect("arbitrage_users.db", timeout=15.0)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = get_db()
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
    try:
        now = now_ist()
        conn = get_db()
        c = conn.cursor()
        c.execute("INSERT INTO user_actions (user_id, action, created_at) VALUES (?, ?, ?)", (user_id, action, now))
        c.execute("UPDATE users SET last_action = ?, last_active = ? WHERE user_id = ?", (action, now, user_id))
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_user_settings(user_id: int):
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("""
            SELECT trade_size_usd, min_net_profit_usd, min_spread_pct, max_spread_pct, 
                   max_results, paused, is_banned, loose_mode, paper_balance 
            FROM users WHERE user_id = ?
        """, (user_id,))
        row = c.fetchone()
        
        if not row and user_id == ADMIN_USER_ID:
            c.execute("INSERT OR IGNORE INTO users (user_id, username, is_premium, registered_at) VALUES (?, ?, 1, ?)", (user_id, "Admin", now_ist()))
            conn.commit()
            trade_size, min_profit, min_spread, max_spread, max_results, paused, is_banned, loose_mode, paper_balance = (100.0, 5.0, 0.5, 50.0, 15, 0, 0, 0, 0.0)
        elif not row:
            conn.close()
            return None
        else:
            trade_size, min_profit, min_spread, max_spread, max_results, paused, is_banned, loose_mode, paper_balance = row

        c.execute("SELECT symbol FROM watchlist WHERE user_id = ?", (user_id,))
        watchlist = {s.upper() for (s,) in c.fetchall()}
        conn.close()
        return {
            'trade_size_usd': trade_size, 'min_net_profit_usd': min_profit, 'min_spread_pct': min_spread,
            'max_spread_pct': max_spread, 'max_results': max_results, 'paused': bool(paused),
            'is_banned': bool(is_banned), 'loose_mode': bool(loose_mode), 'paper_balance': paper_balance, 'watchlist': watchlist
        }
    except Exception:
        return None

def update_user_setting(user_id: int, field: str, amount):
    try:
        conn = get_db()
        conn.execute(f"UPDATE users SET {field} = ? WHERE user_id = ?", (amount, user_id))
        conn.commit()
        conn.close()
    except Exception:
        pass

def toggle_loose_mode_db(user_id: int) -> bool:
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT loose_mode FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        if not row:
            conn.close()
            return False
        new_val = 0 if row[0] else 1
        c.execute("UPDATE users SET loose_mode = ? WHERE user_id = ?", (new_val, user_id))
        conn.commit()
        conn.close()
        return bool(new_val)
    except Exception:
        return False

def add_paper_profit(user_id: int, amount: float):
    try:
        conn = get_db()
        conn.execute("UPDATE users SET paper_balance = paper_balance + ? WHERE user_id = ?", (amount, user_id))
        conn.commit()
        conn.close()
    except Exception:
        pass

def get_paper_leaderboard():
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT username, paper_balance FROM users WHERE paper_balance > 0 ORDER BY paper_balance DESC LIMIT 10")
        rows = c.fetchall()
        conn.close()
        return rows
    except Exception:
        return []

def is_user_premium(user_id: int) -> bool:
    if user_id == ADMIN_USER_ID:
        return True
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT is_premium, is_banned FROM users WHERE user_id = ?", (user_id,))
        row = c.fetchone()
        conn.close()
        return bool(row and row[0] == 1 and row[1] == 0)
    except Exception:
        return False

def activate_user_key(user_id: int, username: str, key_code: str) -> bool:
    try:
        conn = get_db()
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
    except Exception:
        return False

def get_all_premium_users():
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_id FROM users WHERE is_premium=1 AND is_banned=0")
        rows = c.fetchall()
        conn.close()
        user_ids = [r[0] for r in rows]
        if ADMIN_USER_ID not in user_ids:
            user_ids.append(ADMIN_USER_ID)
        return user_ids
    except Exception:
        return [ADMIN_USER_ID]

def get_all_users_detailed():
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute("SELECT user_id, username, is_premium, is_banned, registered_at, trade_size_usd, min_net_profit_usd, paused, last_action, last_active FROM users ORDER BY last_active DESC NULLS LAST")
        rows = c.fetchall()
        conn.close()
        return rows
    except Exception:
        return []

# ==========================================
# 3. MARKET ENGINE (CACHE-DRIVEN)
# ==========================================
_exchanges_to_init = {
    'gate': {'enableRateLimit': True, 'timeout': 8000, 'options': {'defaultType': 'spot'}},
    'lbank': {'enableRateLimit': True, 'timeout': 8000},
    'bitrue': {'enableRateLimit': True, 'timeout': 8000},
    'xt': {'enableRateLimit': True, 'timeout': 8000},
    'ascendex': {'enableRateLimit': True, 'timeout': 8000},
    'poloniex': {'enableRateLimit': True, 'timeout': 8000},
    'bingx': {'enableRateLimit': True, 'timeout': 8000},
    'digifinex': {'enableRateLimit': True, 'timeout': 8000},
    'binance': {'enableRateLimit': True, 'timeout': 8000},
    'bybit': {'enableRateLimit': True, 'timeout': 8000},
    'okx': {'enableRateLimit': True, 'timeout': 8000},
    'kucoin': {'enableRateLimit': True, 'timeout': 8000},
    'mexc': {'enableRateLimit': True, 'timeout': 8000},
    'bitget': {'enableRateLimit': True, 'timeout': 8000},
    'htx': {'enableRateLimit': True, 'timeout': 8000},
    'kraken': {'enableRateLimit': True, 'timeout': 8000},
    'bitfinex': {'enableRateLimit': True, 'timeout': 8000},
}

async def _load_single_exchange(name, obj):
    spot_symbols = set()
    try:
        markets = await asyncio.wait_for(obj.load_markets(), timeout=12.0)
        for s, m in markets.items():
            if isinstance(m, dict) and m.get('spot') and m.get('quote') == 'USDT' and m.get('active') is not False:
                spot_symbols.add(s)
    except Exception:
        pass
    return name, spot_symbols

async def load_universal_symbols():
    global UNIVERSAL_SYMBOLS, SYMBOL_EXCHANGE_MAP
    try:
        logger.info("Updating market pairings across exchanges...")
        tasks = [_load_single_exchange(name, obj) for name, obj in ccxt_instances.items()]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        exchange_markets = {}
        for res in results:
            if isinstance(res, tuple):
                name, spot_symbols = res
                exchange_markets[name] = spot_symbols

        symbol_to_exchanges = {}
        for name, syms in exchange_markets.items():
            for s in syms:
                symbol_to_exchanges.setdefault(s, set()).add(name)

        SYMBOL_EXCHANGE_MAP = {s: exs for s, exs in symbol_to_exchanges.items() if len(exs) >= 2}
        UNIVERSAL_SYMBOLS = sorted(SYMBOL_EXCHANGE_MAP.keys())
        logger.info(f"Loaded {len(UNIVERSAL_SYMBOLS)} universal pairs across active exchanges.")
    except Exception as e:
        logger.error(f"Error in load_universal_symbols: {e}")

async def refresh_global_prices():
    global GLOBAL_PRICE_CACHE, LAST_PRICE_UPDATE
    async def _fetch_single(name, obj):
        try:
            tkrs = await asyncio.wait_for(obj.fetch_tickers(), timeout=7.0)
            return name, tkrs
        except Exception:
            return name, {}

    tasks = [_fetch_single(name, obj) for name, obj in ccxt_instances.items()]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    prices_map = {}
    for res in results:
        if not isinstance(res, tuple): continue
        name, tkrs = res
        if not tkrs: continue
        for sym, t in tkrs.items():
            if sym not in UNIVERSAL_SYMBOLS: continue
            last = t.get('last')
            if last is None or float(last) <= 0: continue
            vol = float(t.get('quoteVolume') or (float(t.get('baseVolume', 0)) * float(last)))
            if vol < MIN_24H_VOLUME_USD: continue
            if sym not in prices_map:
                prices_map[sym] = {}
            prices_map[sym][name] = float(last)

    filtered = {sym: p for sym, p in prices_map.items() if len(p) >= 2}
    async with CACHE_LOCK:
        GLOBAL_PRICE_CACHE = filtered
        LAST_PRICE_UPDATE = time.time()

def calculate_net_arbitrage(symbol: str, prices: dict, trade_size_usd: float, loose_mode: bool = False):
    if len(prices) < 2: return None
    base = symbol.split('/')[0]
    buy_ex = min(prices, key=prices.get)
    sell_ex = max(prices, key=prices.get)
    buy_price = prices[buy_ex]
    sell_price = prices[sell_ex]

    if buy_price <= 0: return None
    coin_amount = trade_size_usd / buy_price
    gross = (coin_amount * sell_price) - trade_size_usd
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
    loose_warn = "\n⚠️ **LOOSE MODE ACTIVE: Verify manually!**\n" if arb.get('loose_mode') else ""
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
    known = list(SYMBOL_EXCHANGE_MAP.get(symbol, ccxt_instances.keys()))[:2]
    lines = [f"📖 **Order Book: {symbol}**\n"]
    for name in known:
        try:
            ob = await asyncio.wait_for(ccxt_instances[name].fetch_order_book(symbol, limit=5), timeout=3.0)
            lines.append(f"**{name.upper()}**\nBuy (Bids):")
            for p, v in ob.get('bids', [])[:4]: lines.append(f"`{p:.5f}` × {v:.4f}")
            lines.append("Sell (Asks):")
            for p, v in ob.get('asks', [])[:4]: lines.append(f"`{p:.5f}` × {v:.4f}")
            lines.append("")
        except Exception:
            continue
    if len(lines) <= 1: return f"Could not fetch order book for `{symbol}` right now."
    return "\n".join(lines)

def get_cached_arbitrage(min_profit=None, min_spread=None, max_spread=None, symbols=None, trade_size=100.0, loose_mode=False):
    results = []
    target_symbols = set(symbols) if symbols else None

    for sym, prices in GLOBAL_PRICE_CACHE.items():
        if target_symbols and sym not in target_symbols: continue
        arb = calculate_net_arbitrage(sym, prices, trade_size, loose_mode)
        if arb:
            if min_profit is not None and arb['net_profit'] < min_profit: continue
            if min_spread is not None and arb['net_spread_pct'] < min_spread: continue
            if max_spread is not None and arb['net_spread_pct'] > max_spread: continue
            results.append(arb)

    results.sort(key=lambda x: x['net_profit'], reverse=True)
    return results

# ==========================================
# 4. COMMAND HANDLERS
# ==========================================
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/start")

    if is_user_premium(uid):
        kb = [[InlineKeyboardButton("⚡ Instant Scan", callback_data="run_manual_scan"), InlineKeyboardButton("⚙️ Filters", callback_data="show_filters")]]
        exchange_names = " • ".join([k.capitalize() for k in ccxt_instances.keys()])
        text = f"""👑 **Arbitrage Terminal Active**

Tracked pairs: `{len(UNIVERSAL_SYMBOLS)}`
Exchanges ({len(ccxt_instances)}):
{exchange_names}

📌 **Quick Commands:**
`/scan` - Instant market opportunities
`/filters` - View your settings & alerts status
`/loosemode` - Toggle contract verification
`/portfolio` - Paper trading stats
`/pause` / `/resume` - Manage background alerts
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

    results = get_cached_arbitrage(
        min_profit=settings['min_net_profit_usd'], min_spread=settings['min_spread_pct'],
        max_spread=settings['max_spread_pct'], symbols=list(settings['watchlist']) if settings['watchlist'] else None,
        trade_size=settings['trade_size_usd'], loose_mode=settings['loose_mode']
    )

    if not results:
        age = int(time.time() - LAST_PRICE_UPDATE) if LAST_PRICE_UPDATE else 0
        return await message.reply_text(f"🔍 No arbitrage opportunities meet your filter criteria (Market cache updated {age}s ago).")

    top = results[:settings['max_results']]
    await message.reply_text(f"📊 Found **{len(results)}** opportunities (Showing top {len(top)}):", parse_mode="Markdown")

    for i, arb in enumerate(top, 1):
        text = f"**#{i}**\n" + format_detailed_alert(arb)
        kb = [[InlineKeyboardButton("📖 View Order Book", callback_data=f"ob:{arb['symbol']}")],
              [InlineKeyboardButton("🎮 Paper Trade This!", callback_data=f"pt:{arb['net_profit']:.2f}")]]
        try:
            await message.reply_text(text, reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")
            await asyncio.sleep(0.08)
        except Exception:
            pass

async def loosemode_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/loosemode")
    if not is_user_premium(uid): return
    if toggle_loose_mode_db(uid): await update.message.reply_text("🔓 **Loose Mode ENABLED**")
    else: await update.message.reply_text("🔒 **Loose Mode DISABLED**")

async def portfolio_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/portfolio")
    if not is_user_premium(uid): return
    settings = get_user_settings(uid)
    await update.message.reply_text(f"🎮 **Your Paper Trading Portfolio**\n\nTotal Virtual Profit: **${settings['paper_balance']:.2f}**", parse_mode="Markdown")

async def leaderboard_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_user_premium(update.effective_user.id): return
    leaders = get_paper_leaderboard()
    if not leaders: return await update.message.reply_text("No paper trades recorded yet!")
    lines = ["🏆 **Paper Trading Leaderboard**\n"]
    for i, (name, bal) in enumerate(leaders, 1): lines.append(f"{i}. @{name or 'Anon'} - **${bal:.2f}**")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def orderbook_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_user_premium(update.effective_user.id): return
    if not context.args: return await update.message.reply_text("⚠️ Usage:\n`/ob BTC/USDT`")
    msg = await update.message.reply_text("📖 Fetching real-time order book...")
    await msg.edit_text(await get_orderbook_text(context.args[0].upper()), parse_mode="Markdown")

async def filters_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/filters")
    if not is_user_premium(uid): return
    s = get_user_settings(uid)
    text = f"""⚙️ **Your Settings**
• Alerts Status: `{'PAUSED ⏸' if s['paused'] else 'ACTIVE ▶️'}`
• Trade Size   : `${s['trade_size_usd']:.0f}`
• Min Profit   : `${s['min_net_profit_usd']:.1f}`
• Min Spread   : `{s['min_spread_pct']:.1f}%`
• Max Spread   : `{s['max_spread_pct']:.1f}%`
• Loose Mode   : `{'ON 🔓' if s['loose_mode'] else 'OFF 🔒'}`
• Paper Balance: `${s['paper_balance']:.2f}`"""
    await update.message.reply_text(text, parse_mode="Markdown")

async def set_value(update, context, field, type_func, name):
    uid = update.effective_user.id
    if not context.args: return await update.message.reply_text("⚠️ Value required.")
    try:
        val = type_func(context.args[0])
        update_user_setting(uid, field, val)
        await update.message.reply_text(f"✅ {name} set to **{val}**")
    except ValueError:
        await update.message.reply_text("⚠️ Invalid number.")

async def setminprofit_command(update, context): await set_value(update, context, 'min_net_profit_usd', float, "Min net profit")
async def setminspread_command(update, context): await set_value(update, context, 'min_spread_pct', float, "Min spread")
async def setmaxspread_command(update, context): await set_value(update, context, 'max_spread_pct', float, "Max spread")
async def setmaxresults_command(update, context): await set_value(update, context, 'max_results', int, "Max results")
async def settradesize_command(update, context): await set_value(update, context, 'trade_size_usd', float, "Trade size")

async def watch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_user_premium(uid) or not context.args: return
    sym = context.args[0].upper()
    conn = get_db()
    try:
        conn.execute("INSERT INTO watchlist (user_id, symbol) VALUES (?, ?)", (uid, sym))
        conn.commit()
        await update.message.reply_text(f"✅ Added `{sym}` to watchlist.")
    except Exception: pass
    finally: conn.close()

async def unwatch_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if not is_user_premium(uid) or not context.args: return
    sym = context.args[0].upper()
    conn = get_db()
    conn.execute("DELETE FROM watchlist WHERE user_id = ? AND symbol = ?", (uid, sym))
    conn.commit()
    conn.close()
    await update.message.reply_text(f"🗑️ Removed `{sym}` from watchlist.")

async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/pause")
    if not is_user_premium(uid):
        return await update.message.reply_text("🔒 Premium required.")
    update_user_setting(uid, 'paused', 1)
    await update.message.reply_text("⏸ **Background alerts paused.**\n(You can still use `/scan` manually at any time.)", parse_mode="Markdown")

async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    log_action(uid, "/resume")
    if not is_user_premium(uid):
        return await update.message.reply_text("🔒 Premium required.")
    update_user_setting(uid, 'paused', 0)
    await update.message.reply_text("▶️ **Background alerts resumed.**", parse_mode="Markdown")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = """🤖 **Full Command List**
🔍 **Core:** `/scan`, `/loosemode`, `/ob BTC/USDT`
🎮 **Paper Trading:** `/portfolio`, `/leaderboard`
⚙️ **Filters:** `/filters`, `/settradesize`, `/setminprofit`, `/setminspread`, `/setmaxspread`, `/setmaxresults`
🔔 **Alert Controls:** `/watch`, `/unwatch`, `/pause`, `/resume`"""
    await update.message.reply_text(text, parse_mode="Markdown")

async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0] != ADMIN_SECRET: return await update.message.reply_text("⛔ Unauthorized")
    users = get_all_users_detailed()
    lines = [f"**Users: {len(users)}**\n"]
    for u in users[:30]:
        status = "🚫" if u[3] else ("✅" if u[2] else "❌")
        lines.append(f"`{u[0]}` @{u[1] or '—'} {status} | {u[9] or 'never'}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args or context.args[0] != ADMIN_SECRET: return
    msg = " ".join(context.args[1:])
    ok = fail = 0
    for uid in get_all_premium_users():
        try:
            await context.bot.send_message(uid, f"📢 **Admin Message**\n\n{msg}", parse_mode="Markdown")
            ok += 1
            await asyncio.sleep(0.04)
        except Exception: fail += 1
    await update.message.reply_text(f"✅ Sent: {ok} | Failed: {fail}")

# ==========================================
# 5. BUTTON ROUTER & BACKGROUND DAEMON
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
        sym = data[3:]
        await query.message.reply_text(await get_orderbook_text(sym), parse_mode="Markdown")
    elif data.startswith("pt:"):
        prof = float(data.split(":")[1])
        add_paper_profit(uid, prof)
        await query.answer(f"✅ Paper traded! Earned ${prof:.2f}", show_alert=True)

async def background_daemon(app):
    await asyncio.sleep(1)
    await load_universal_symbols()
    last_refresh = time.time()

    while True:
        try:
            if time.time() - last_refresh > CURRENCY_REFRESH_SECONDS:
                await load_universal_symbols()
                last_refresh = time.time()

            await refresh_global_prices()
            users = get_all_premium_users()

            if users and GLOBAL_PRICE_CACHE:
                for uid in users:
                    settings = get_user_settings(uid)
                    if not settings or settings['paused'] or settings['is_banned']: continue
                    
                    alerts = get_cached_arbitrage(
                        min_profit=settings['min_net_profit_usd'],
                        min_spread=settings['min_spread_pct'],
                        max_spread=settings['max_spread_pct'],
                        symbols=list(settings['watchlist']) if settings['watchlist'] else None,
                        trade_size=settings['trade_size_usd'],
                        loose_mode=settings['loose_mode']
                    )

                    for arb in alerts[:3]:
                        kb = [[InlineKeyboardButton("📖 View Order Book", callback_data=f"ob:{arb['symbol']}")],
                              [InlineKeyboardButton("🎮 Paper Trade This!", callback_data=f"pt:{arb['net_profit']:.2f}")]]
                        try:
                            await app.bot.send_message(uid, format_detailed_alert(arb), reply_markup=InlineKeyboardMarkup(kb), parse_mode="Markdown")
                            await asyncio.sleep(0.5)
                        except Exception:
                            pass
        except Exception as e:
            logger.error(f"Daemon non-fatal error: {e}")

        await asyncio.sleep(SCAN_INTERVAL_SECONDS)

async def global_error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Telegram Exception caught: {context.error}")

async def post_init(app):
    global ccxt_instances
    for ex_id, config in _exchanges_to_init.items():
        if hasattr(ccxt_async, ex_id):
            ccxt_instances[ex_id] = getattr(ccxt_async, ex_id)(config)
            
    logger.info("CCXT Exchange instances initialized successfully.")
    asyncio.create_task(background_daemon(app))

async def post_shutdown(app):
    for o in ccxt_instances.values():
        try: 
            await o.close()
        except Exception: 
            pass

def main():
    init_db()
    
    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .connect_timeout(20)
        .read_timeout(20)
        .write_timeout(20)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    handlers = [
        ("start", start_command), ("register", register_command), ("help", help_command),
        ("scan", scan_command), ("ob", orderbook_command), ("loosemode", loosemode_command), 
        ("portfolio", portfolio_command), ("leaderboard", leaderboard_command), ("filters", filters_command), 
        ("setminprofit", setminprofit_command), ("setminspread", setminspread_command),
        ("setmaxspread", setmaxspread_command), ("setmaxresults", setmaxresults_command),
        ("settradesize", settradesize_command), ("pause", pause_command), ("resume", resume_command),
        ("watch", watch_command), ("unwatch", unwatch_command),
        ("users", users_command), ("broadcast", broadcast_command)
    ]
    for cmd, func in handlers:
        app.add_handler(CommandHandler(cmd, func))

    app.add_handler(CallbackQueryHandler(button_router))
    app.add_error_handler(global_error_handler)
    
    logger.info("Bot started with drop_pending_updates & cache engine enabled...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
