import asyncio
import html
import json
import random
import sqlite3
import string
import time
from datetime import datetime
from typing import Any, Dict, Optional

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    PreCheckoutQuery,
    LabeledPrice,
    SuccessfulPayment,
)

# ========== КОНФИГУРАЦИЯ ==========
BOT_TOKEN = "8200340859:AAFziC0Vk2KH71AwnCPvQBkyCfBl50eVMrs"
ADMIN_IDS = [8478884644]
SUPPORT_USERNAME = "@CashOverseer"
SUPPORT_USERNAME2 = "@anti_account"
CHANNEL_LINK = "https://t.me/+CJ7BZGR0FAY4YTky"
MANUAL_DEPOSIT_CHAT = "https://t.me/+5Xm6srsM9GI2NDky"

CURRENCY_NAME = "WILLD GRAMM"
START_BALANCE = 100.0
MIN_BET = 1.0
MAX_BET = 10000.0
DAILY_BONUS = 250.0
CHECK_FEE_PERCENT = 0.06

STARS_RATE = 2200
MIN_STARS = 1
MAX_STARS = 1500
WITHDRAW_FEE = 0.05

BONUS_COOLDOWN_SECONDS = 24 * 60 * 60

BANK_TERMS = {7: 0.03, 14: 0.07, 30: 0.18}

RED_NUMBERS = {1, 3, 5, 7, 9, 12, 14, 16, 18, 19, 21, 23, 25, 27, 30, 32, 34, 36}

TOWER_MULTIPLIERS = [1.20, 1.48, 1.86, 2.35, 2.95, 3.75, 4.85, 6.15]
GOLD_MULTIPLIERS = [1.15, 1.35, 1.62, 2.0, 2.55, 3.25, 4.2]
DIAMOND_MULTIPLIERS = [1.12, 1.28, 1.48, 1.72, 2.02, 2.4, 2.92, 3.6]
LLAMA_MULTIPLIERS = [1.25, 1.56, 1.95, 2.44, 3.05, 3.81, 4.77, 5.96, 7.45, 9.31]

USER_STATUSES = {
    0: "👤 Игрок",
    1: "🛡️ Помощник",
    2: "👑 Создатель",
    3: "⭐ VIP",
    4: "🎲 Хайроллер",
}

ROULETTE_STICKER_PACK = "IrisAdvanceRoulette"

# ========== БД ==========
DB_PATH = "data.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, username TEXT, coins REAL DEFAULT 100.0, stars INTEGER DEFAULT 0, total_games INTEGER DEFAULT 0, total_wins INTEGER DEFAULT 0, status INTEGER DEFAULT 0, checks TEXT DEFAULT '[]', withdraws TEXT DEFAULT '[]', deposit_stars INTEGER DEFAULT 0)")
    cursor.execute("CREATE TABLE IF NOT EXISTS bets (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, bet_amount REAL, choice TEXT, outcome TEXT, win INTEGER, payout REAL, ts INTEGER)")
    cursor.execute("CREATE TABLE IF NOT EXISTS checks (code TEXT PRIMARY KEY, creator_id TEXT, per_user REAL, remaining INTEGER, claimed TEXT, created_at INTEGER)")
    cursor.execute("CREATE TABLE IF NOT EXISTS promos (name TEXT PRIMARY KEY, reward REAL, claimed TEXT, remaining_activations INTEGER, created_by TEXT)")
    cursor.execute("CREATE TABLE IF NOT EXISTS bank_deposits (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, principal REAL, rate REAL, term_days INTEGER, opened_at INTEGER, status TEXT, closed_at INTEGER)")
    cursor.execute("CREATE TABLE IF NOT EXISTS json_data (key TEXT PRIMARY KEY, value TEXT)")
    for col in ['username', 'stars', 'total_games', 'total_wins', 'withdraws', 'deposit_stars']:
        try:
            cursor.execute(f"ALTER TABLE users ADD COLUMN {col}")
        except: pass
    try:
        cursor.execute("ALTER TABLE bank_deposits ADD COLUMN closed_at INTEGER")
    except: pass
    conn.commit()
    conn.close()
    print("✅ База данных готова")

init_db()

# ========== СОСТОЯНИЯ ==========
class CheckCreateStates(StatesGroup):
    waiting_amount = State()
    waiting_count = State()

class CheckClaimStates(StatesGroup):
    waiting_code = State()

class PromoStates(StatesGroup):
    waiting_code = State()

class NewPromoStates(StatesGroup):
    waiting_code = State()
    waiting_reward = State()
    waiting_activations = State()

class BankStates(StatesGroup):
    waiting_amount = State()

class RouletteStates(StatesGroup):
    waiting_amount = State()
    waiting_choice = State()

class CrashStates(StatesGroup):
    waiting_amount = State()
    waiting_target = State()

class CubeStates(StatesGroup):
    waiting_amount = State()
    waiting_guess = State()

class DiceStates(StatesGroup):
    waiting_amount = State()
    waiting_guess = State()

class FootballStates(StatesGroup):
    waiting_amount = State()

class BasketStates(StatesGroup):
    waiting_amount = State()

class TowerStates(StatesGroup):
    waiting_amount = State()

class GoldStates(StatesGroup):
    waiting_amount = State()

class DiamondStates(StatesGroup):
    waiting_amount = State()

class MinesStates(StatesGroup):
    waiting_amount = State()
    waiting_mines = State()

class OchkoStates(StatesGroup):
    waiting_amount = State()
    waiting_confirm = State()

class LlamaStates(StatesGroup):
    waiting_amount = State()

class DartsStates(StatesGroup):
    waiting_amount = State()
    waiting_choice = State()

class WithdrawStates(StatesGroup):
    waiting_amount = State()

class SetStatusStates(StatesGroup):
    waiting_user = State()
    waiting_status = State()

class AdminGiveStates(StatesGroup):
    waiting_data = State()

# ========== АКТИВНЫЕ ИГРЫ ==========
TOWER_GAMES = {}
GOLD_GAMES = {}
DIAMOND_GAMES = {}
MINES_GAMES = {}
OCHKO_GAMES = {}
LLAMA_GAMES = {}

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def now_ts():
    return int(time.time())

def fmt_money(value):
    value = round(float(value), 2)
    if value >= 1000000:
        return f"{value/1000000:.1f}M {CURRENCY_NAME}"
    elif value >= 1000:
        return f"{value/1000:.1f}K {CURRENCY_NAME}"
    return f"{value:.2f} {CURRENCY_NAME}"

def fmt_dt(ts):
    return datetime.fromtimestamp(ts).strftime("%d.%m.%Y %H:%M")

def fmt_left(seconds):
    seconds = max(0, int(seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}ч {m}м"
    if m > 0:
        return f"{m}м {s}с"
    return f"{s}с"

def parse_amount(text):
    raw = str(text or "").strip().lower().replace(" ", "").replace(",", ".")
    if raw in ["все", "всё", "all"]:
        return -1
    multiplier = 1.0
    if raw.endswith(("к", "k")):
        raw = raw[:-1]
        multiplier = 1000.0
    elif raw.endswith(("м", "m")):
        raw = raw[:-1]
        multiplier = 1000000.0
    value = float(raw) * multiplier
    if value <= 0:
        raise ValueError
    if value > MAX_BET:
        raise ValueError(f"Максимальная ставка: {fmt_money(MAX_BET)}")
    return round(value, 2)

def normalize_text(text):
    s = str(text or "").lower().strip()
    for symbol in ["💰", "👤", "🎁", "🎮", "🧾", "🏦", "🎟", "❓", "✨", "•", "|", "⭐"]:
        s = s.replace(symbol, " ")
    return " ".join(s.split())

def escape_html(text):
    return html.escape(str(text or ""), quote=False)

def mention_user(user_id, name=None):
    label = escape_html(name or f"Игрок {user_id}")
    return f'<a href="tg://user?id={int(user_id)}">{label}</a>'

def is_admin_user(user_id):
    return int(user_id) in ADMIN_IDS

def get_user_status_text(status):
    return USER_STATUSES.get(status, USER_STATUSES[0])

def ensure_user_in_conn(conn, user_id, username=None):
    conn.execute("INSERT OR IGNORE INTO users (id, username, coins, stars, total_games, total_wins, status, checks, withdraws, deposit_stars) VALUES (?, ?, ?, 0, 0, 0, 0, '[]', '[]', 0)", (str(user_id), username or "", START_BALANCE))
    if username:
        conn.execute("UPDATE users SET username = ? WHERE id = ?", (username, str(user_id)))

def ensure_user(user_id, username=None):
    conn = get_db()
    try:
        ensure_user_in_conn(conn, user_id, username)
        conn.commit()
    finally:
        conn.close()

def get_user(user_id):
    conn = get_db()
    try:
        ensure_user_in_conn(conn, user_id)
        row = conn.execute("SELECT * FROM users WHERE id = ?", (str(user_id),)).fetchone()
        conn.commit()
        return row
    finally:
        conn.close()

def set_json_value(key, value):
    conn = get_db()
    try:
        conn.execute("INSERT INTO json_data (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value", (key, json.dumps(value, ensure_ascii=False)))
        conn.commit()
    finally:
        conn.close()

def get_json_value(key, default=None):
    conn = get_db()
    try:
        row = conn.execute("SELECT value FROM json_data WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        try:
            return json.loads(row["value"])
        except:
            return default
    finally:
        conn.close()

def add_coins(user_id, amount):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        conn.execute("UPDATE users SET coins = coins + ? WHERE id = ?", (round(amount, 2), str(user_id)))
        row = conn.execute("SELECT coins FROM users WHERE id = ?", (str(user_id),)).fetchone()
        conn.commit()
        return float(row["coins"] or 0)
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def add_stars(user_id, stars):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        conn.execute("UPDATE users SET stars = stars + ?, deposit_stars = deposit_stars + ? WHERE id = ?", (stars, stars, str(user_id)))
        conn.execute("UPDATE users SET coins = coins + ? WHERE id = ?", (stars * STARS_RATE, str(user_id)))
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def set_user_status(user_id, status):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        conn.execute("UPDATE users SET status = ? WHERE id = ?", (status, str(user_id)))
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def reserve_bet(user_id, bet):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        row = conn.execute("SELECT coins FROM users WHERE id = ?", (str(user_id),)).fetchone()
        coins = float(row["coins"] or 0)
        if bet == -1:
            bet = coins
        if coins < bet:
            conn.rollback()
            return False, coins
        new_balance = round(coins - bet, 2)
        conn.execute("UPDATE users SET coins = ? WHERE id = ?", (new_balance, str(user_id)))
        conn.commit()
        return True, new_balance
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def finalize_reserved_bet(user_id, bet, payout, choice, outcome):
    payout = round(max(0.0, payout), 2)
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        if payout > 0:
            conn.execute("UPDATE users SET coins = coins + ? WHERE id = ?", (payout, str(user_id)))
        conn.execute("INSERT INTO bets (user_id, bet_amount, choice, outcome, win, payout, ts) VALUES (?, ?, ?, ?, ?, ?, ?)", (str(user_id), round(bet, 2), choice, outcome, 1 if payout > 0 else 0, payout, now_ts()))
        if payout > 0:
            conn.execute("UPDATE users SET total_wins = total_wins + 1 WHERE id = ?", (str(user_id),))
        conn.execute("UPDATE users SET total_games = total_games + 1 WHERE id = ?", (str(user_id),))
        row = conn.execute("SELECT coins FROM users WHERE id = ?", (str(user_id),)).fetchone()
        conn.commit()
        return float(row["coins"] or 0)
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def settle_instant_bet(user_id, bet, payout, choice, outcome):
    payout = round(max(0.0, payout), 2)
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        row = conn.execute("SELECT coins FROM users WHERE id = ?", (str(user_id),)).fetchone()
        coins = float(row["coins"] or 0)
        if bet == -1:
            bet = coins
        if coins < bet:
            conn.rollback()
            return False, coins
        new_balance = round(coins - bet + payout, 2)
        conn.execute("UPDATE users SET coins = ? WHERE id = ?", (new_balance, str(user_id)))
        conn.execute("INSERT INTO bets (user_id, bet_amount, choice, outcome, win, payout, ts) VALUES (?, ?, ?, ?, ?, ?, ?)", (str(user_id), round(bet, 2), choice, outcome, 1 if payout > 0 else 0, payout, now_ts()))
        if payout > 0:
            conn.execute("UPDATE users SET total_wins = total_wins + 1 WHERE id = ?", (str(user_id),))
        conn.execute("UPDATE users SET total_games = total_games + 1 WHERE id = ?", (str(user_id),))
        conn.commit()
        return True, new_balance
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def get_profile_stats(user_id):
    conn = get_db()
    try:
        ensure_user_in_conn(conn, user_id)
        user = conn.execute("SELECT * FROM users WHERE id = ?", (str(user_id),)).fetchone()
        dep = conn.execute("SELECT COUNT(*) AS active_count, COALESCE(SUM(principal), 0) AS active_sum FROM bank_deposits WHERE user_id = ? AND status = 'active'", (str(user_id),)).fetchone()
        conn.commit()
        return {
            "coins": float(user["coins"] or 0),
            "stars": int(user["stars"] or 0),
            "status": int(user["status"] or 0),
            "total_games": int(user["total_games"] or 0),
            "total_wins": int(user["total_wins"] or 0),
            "active_deposits": int(dep["active_count"] or 0),
            "active_deposit_sum": float(dep["active_sum"] or 0),
            "deposit_stars": int(user["deposit_stars"] or 0),
            "username": user["username"] or "",
        }
    finally:
        conn.close()

def get_top_balances(limit=10):
    conn = get_db()
    try:
        rows = conn.execute("SELECT id, username, coins FROM users ORDER BY coins DESC LIMIT ?", (limit,)).fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()

def get_top_deposits(limit=10):
    conn = get_db()
    try:
        rows = conn.execute("SELECT u.id, u.username, COALESCE(SUM(bd.principal), 0) as total_deposit FROM users u LEFT JOIN bank_deposits bd ON u.id = bd.user_id AND bd.status = 'active' GROUP BY u.id ORDER BY total_deposit DESC LIMIT ?", (limit,)).fetchall()
        conn.commit()
        return rows
    finally:
        conn.close()

# ========== ЧЕКИ ==========
def generate_check_code(conn):
    while True:
        code = "".join(random.choices(string.ascii_uppercase + string.digits, k=8))
        row = conn.execute("SELECT 1 FROM checks WHERE code = ?", (code,)).fetchone()
        if not row:
            return code

def create_check_atomic(user_id, per_user, count):
    total = round(per_user * count, 2)
    fee = round(total * CHECK_FEE_PERCENT, 2)
    total_with_fee = round(total + fee, 2)
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        row = conn.execute("SELECT coins FROM users WHERE id = ?", (str(user_id),)).fetchone()
        coins = float(row["coins"] or 0)
        if coins < total_with_fee:
            conn.rollback()
            return False, f"Недостаточно средств. Нужно {fmt_money(total_with_fee)} (включая комиссию 6%)"
        code = generate_check_code(conn)
        conn.execute("UPDATE users SET coins = coins - ? WHERE id = ?", (total_with_fee, str(user_id)))
        conn.execute("INSERT INTO checks (code, creator_id, per_user, remaining, claimed, created_at) VALUES (?, ?, ?, ?, ?, ?)", (code, str(user_id), round(per_user, 2), int(count), "[]", now_ts()))
        conn.commit()
        return True, code
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def claim_check_atomic(user_id, code):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        row = conn.execute("SELECT * FROM checks WHERE code = ?", (code.upper(),)).fetchone()
        if not row:
            conn.rollback()
            return False, "Чек не найден.", 0.0
        if int(row["remaining"] or 0) <= 0:
            conn.rollback()
            return False, "Этот чек уже закончился.", 0.0
        claimed_raw = row["claimed"] or "[]"
        try:
            claimed = json.loads(claimed_raw)
        except:
            claimed = []
        if str(user_id) in {str(x) for x in claimed}:
            conn.rollback()
            return False, "Ты уже активировал этот чек.", 0.0
        claimed.append(str(user_id))
        reward = round(float(row["per_user"] or 0), 2)
        conn.execute("UPDATE users SET coins = coins + ? WHERE id = ?", (reward, str(user_id)))
        conn.execute("UPDATE checks SET remaining = remaining - 1, claimed = ? WHERE code = ?", (json.dumps(claimed), code.upper()))
        conn.commit()
        return True, "Чек успешно активирован.", reward
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def list_my_checks(user_id):
    conn = get_db()
    try:
        rows = conn.execute("SELECT code, per_user, remaining, created_at FROM checks WHERE creator_id = ? ORDER BY created_at DESC LIMIT 10", (str(user_id),)).fetchall()
        return list(rows)
    finally:
        conn.close()

# ========== ПРОМОКОДЫ ==========
def normalize_promo_code(text):
    code = str(text or "").strip().upper()
    allowed = set(string.ascii_uppercase + string.digits + "_-")
    if not (3 <= len(code) <= 24):
        raise ValueError
    if any(ch not in allowed for ch in code):
        raise ValueError
    return code

def redeem_promo_atomic(user_id, code):
    promo_name = code.upper().strip()
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        row = conn.execute("SELECT * FROM promos WHERE name = ?", (promo_name,)).fetchone()
        if not row:
            conn.rollback()
            return False, "Промокод не найден.", 0.0
        remaining = int(row["remaining_activations"] or 0)
        if remaining <= 0:
            conn.rollback()
            return False, "Промокод уже закончился.", 0.0
        try:
            claimed = json.loads(row["claimed"] or "[]")
        except:
            claimed = []
        if str(user_id) in {str(x) for x in claimed}:
            conn.rollback()
            return False, "Ты уже активировал этот промокод.", 0.0
        reward = round(float(row["reward"] or 0), 2)
        claimed.append(str(user_id))
        conn.execute("UPDATE users SET coins = coins + ? WHERE id = ?", (reward, str(user_id)))
        conn.execute("UPDATE promos SET claimed = ?, remaining_activations = remaining_activations - 1 WHERE name = ?", (json.dumps(claimed), promo_name))
        conn.commit()
        return True, "Промокод активирован.", reward
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def create_promo(code, reward, activations, admin_id):
    conn = get_db()
    try:
        conn.execute("INSERT INTO promos (name, reward, claimed, remaining_activations, created_by) VALUES (?, ?, '[]', ?, ?) ON CONFLICT(name) DO UPDATE SET reward = excluded.reward, remaining_activations = excluded.remaining_activations, claimed = '[]', created_by = excluded.created_by", (code.upper().strip(), round(reward, 2), int(activations), str(admin_id)))
        conn.commit()
    finally:
        conn.close()

# ========== БАНК ==========
def add_deposit(user_id, amount, term_days):
    rate = BANK_TERMS.get(term_days)
    if rate is None:
        return False, "Неверный срок депозита."
    ok, _ = reserve_bet(user_id, amount)
    if not ok:
        return False, "Недостаточно средств для открытия депозита."
    conn = get_db()
    try:
        conn.execute("INSERT INTO bank_deposits (user_id, principal, rate, term_days, opened_at, status, closed_at) VALUES (?, ?, ?, ?, ?, 'active', NULL)", (str(user_id), round(amount, 2), float(rate), int(term_days), now_ts()))
        conn.commit()
        return True, "Депозит открыт."
    finally:
        conn.close()

def list_user_deposits(user_id, active_only=False):
    conn = get_db()
    try:
        if active_only:
            rows = conn.execute("SELECT * FROM bank_deposits WHERE user_id = ? AND status = 'active' ORDER BY id DESC", (str(user_id),)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM bank_deposits WHERE user_id = ? ORDER BY id DESC LIMIT 15", (str(user_id),)).fetchall()
        return list(rows)
    finally:
        conn.close()

def withdraw_matured_deposits(user_id):
    now = now_ts()
    conn = get_db()
    total_payout = 0.0
    closed_count = 0
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        rows = conn.execute("SELECT * FROM bank_deposits WHERE user_id = ? AND status = 'active'", (str(user_id),)).fetchall()
        for row in rows:
            unlock_ts = int(row["opened_at"] or 0) + int(row["term_days"] or 0) * 86400
            if now < unlock_ts:
                continue
            principal = float(row["principal"] or 0)
            rate = float(row["rate"] or 0)
            payout = round(principal * (1.0 + rate), 2)
            total_payout += payout
            closed_count += 1
            conn.execute("UPDATE bank_deposits SET status = 'closed', closed_at = ? WHERE id = ?", (now, int(row["id"])))
        if total_payout > 0:
            conn.execute("UPDATE users SET coins = coins + ? WHERE id = ?", (round(total_payout, 2), str(user_id)))
        conn.commit()
        return closed_count, round(total_payout, 2)
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

def get_bank_summary(user_id):
    conn = get_db()
    try:
        ensure_user_in_conn(conn, user_id)
        user = conn.execute("SELECT coins FROM users WHERE id = ?", (str(user_id),)).fetchone()
        deps = conn.execute("SELECT COUNT(*) AS count_active, COALESCE(SUM(principal), 0) AS active_sum FROM bank_deposits WHERE user_id = ? AND status = 'active'", (str(user_id),)).fetchone()
        conn.commit()
        return {
            "coins": float(user["coins"] or 0),
            "count_active": int(deps["count_active"] or 0),
            "active_sum": float(deps["active_sum"] or 0),
        }
    finally:
        conn.close()

# ========== ВЫВОД СРЕДСТВ ==========
def request_withdraw(user_id, amount):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        ensure_user_in_conn(conn, user_id)
        row = conn.execute("SELECT coins FROM users WHERE id = ?", (str(user_id),)).fetchone()
        coins = float(row["coins"] or 0)
        if amount == -1:
            amount = coins
        if coins < amount:
            conn.rollback()
            return False, "Недостаточно средств."
        if amount < 100:
            conn.rollback()
            return False, "Минимальная сумма вывода: 100 WILLD GRAMM"
        fee = round(amount * WITHDRAW_FEE, 2)
        amount_net = round(amount - fee, 2)
        conn.execute("UPDATE users SET coins = coins - ? WHERE id = ?", (amount, str(user_id)))
        withdraws_raw = row["withdraws"] or "[]"
        try:
            withdraws = json.loads(withdraws_raw)
        except:
            withdraws = []
        withdraws.append({"amount": amount, "amount_net": amount_net, "fee": fee, "ts": now_ts(), "status": "pending"})
        conn.execute("UPDATE users SET withdraws = ? WHERE id = ?", (json.dumps(withdraws), str(user_id)))
        conn.commit()
        return True, f"✅ Заявка на вывод {fmt_money(amount_net)} (комиссия {fmt_money(fee)}) отправлена администратору."
    except:
        conn.rollback()
        raise
    finally:
        conn.close()

# ========== ИГРОВЫЕ ФУНКЦИИ ==========
def get_roulette_color(number):
    if number == 0:
        return "green"
    if number in RED_NUMBERS:
        return "red"
    return "black"

async def play_roulette_with_sticker(message, bet, choice):
    number = random.randint(0, 36)
    sticker_name = f"{ROULETTE_STICKER_PACK}/{number}"
    await message.answer_sticker(sticker_name)
    await asyncio.sleep(1.5)
    color = get_roulette_color(number)
    parity = "zero"
    if number != 0:
        parity = "even" if number % 2 == 0 else "odd"
    win = False
    multiplier = 0.0
    if choice == "red" and color == "red":
        win, multiplier = True, 2.0
    elif choice == "black" and color == "black":
        win, multiplier = True, 2.0
    elif choice == "even" and parity == "even":
        win, multiplier = True, 2.0
    elif choice == "odd" and parity == "odd":
        win, multiplier = True, 2.0
    elif choice == "zero" and number == 0:
        win, multiplier = True, 36.0
    payout = round(bet * multiplier, 2) if win else 0.0
    color_emoji = {"red": "🔴", "black": "⚫", "green": "🟢"}[color]
    color_text = {"red": "красное", "black": "чёрное", "green": "зеро"}[color]
    result_text = f"{color_emoji} Выпало **{number}** ({color_text})"
    return win, payout, result_text, number

async def crash_roll_with_animation(message, target):
    msg = await message.answer("🚀 Ракета взлетает...")
    await asyncio.sleep(1)
    await msg.edit_text("🚀🚀 Ракета набирает высоту...")
    await asyncio.sleep(1)
    await msg.edit_text("🚀🚀🚀 Ракета в космосе!")
    await asyncio.sleep(0.8)
    u = random.random()
    if u < 0.06:
        crash_multiplier = 1.00
    elif u < 0.55:
        crash_multiplier = round(random.uniform(1.01, 1.80), 2)
    elif u < 0.80:
        crash_multiplier = round(random.uniform(1.81, 2.80), 2)
    elif u < 0.93:
        crash_multiplier = round(random.uniform(2.81, 4.50), 2)
    elif u < 0.985:
        crash_multiplier = round(random.uniform(4.51, 9.50), 2)
    else:
        crash_multiplier = round(random.uniform(9.51, 10.0), 2)
    await msg.edit_text(f"💥 **КРАШ!** Множитель упал на **x{crash_multiplier}**!")
    await asyncio.sleep(0.5)
    win = target <= crash_multiplier
    return win, crash_multiplier

def football_value_text(value):
    return "⚽ ГОЛ!" if value >= 3 else "❌ Мимо"

def basketball_value_text(value):
    return "🏀 ПОПАДАНИЕ!" if value in {4, 5} else "❌ Промах"

def mines_multiplier(opened_count, mines_count):
    if opened_count <= 0:
        return 1.0
    safe_cells = 9 - mines_count
    base = 9.0 / max(1.0, safe_cells)
    return round((base ** opened_count) * 0.95, 2)

def make_deck():
    ranks = ["2", "3", "4", "5", "6", "7", "8", "9", "10", "J", "Q", "K", "A"]
    suits = ["♠", "♥", "♦", "♣"]
    deck = [(rank, suit) for rank in ranks for suit in suits]
    random.shuffle(deck)
    return deck

def card_points(rank):
    if rank in {"J", "Q", "K"}:
        return 10
    if rank == "A":
        return 11
    return int(rank)

def hand_value(cards):
    total = sum(card_points(rank) for rank, _ in cards)
    aces = sum(1 for rank, _ in cards if rank == "A")
    while total > 21 and aces > 0:
        total -= 10
        aces -= 1
    return total

def format_hand(cards):
    return " ".join(f"{r}{s}" for r, s in cards)

def render_ochko_table(game, reveal_dealer):
    player_cards = game["player"]
    dealer_cards = game["dealer"]
    player_value = hand_value(player_cards)
    if reveal_dealer:
        dealer_line = f"{format_hand(dealer_cards)} ({hand_value(dealer_cards)})"
    else:
        first = f"{dealer_cards[0][0]}{dealer_cards[0][1]}"
        dealer_line = f"{first} ??"
    return f"🎴 **Очко**\nСтавка: **{fmt_money(game['bet'])}**\n\nДилер: {dealer_line}\nТы: {format_hand(player_cards)} ({player_value})"

def clear_active_sessions(user_id):
    TOWER_GAMES.pop(user_id, None)
    GOLD_GAMES.pop(user_id, None)
    DIAMOND_GAMES.pop(user_id, None)
    MINES_GAMES.pop(user_id, None)
    OCHKO_GAMES.pop(user_id, None)
    LLAMA_GAMES.pop(user_id, None)

def tower_text(game):
    level = int(game["level"])
    bet = float(game["bet"])
    current_mult = TOWER_MULTIPLIERS[level - 1] if level > 0 else 0
    current_win = bet * current_mult if level > 0 else 0
    next_mult = TOWER_MULTIPLIERS[level] if level < len(TOWER_MULTIPLIERS) else TOWER_MULTIPLIERS[-1]
    return f"🗼 **Башня**\nСтавка: **{fmt_money(bet)}**\nЭтаж: **{level}**\nМножитель: **x{current_mult:.2f}**\nВыигрыш: **{fmt_money(current_win)}**\nСледующий: **x{next_mult:.2f}**\n\nВыбери секцию 1-3:"

def gold_text(game):
    step = int(game["step"])
    bet = float(game["bet"])
    cur_mult = GOLD_MULTIPLIERS[step - 1] if step > 0 else 0
    cur_win = bet * cur_mult if step > 0 else 0
    next_mult = GOLD_MULTIPLIERS[step] if step < len(GOLD_MULTIPLIERS) else GOLD_MULTIPLIERS[-1]
    return f"🥇 **Золото**\nСтавка: **{fmt_money(bet)}**\nРаунд: **{step}**\nМножитель: **x{cur_mult:.2f}**\nВыигрыш: **{fmt_money(cur_win)}**\nСледующий: **x{next_mult:.2f}**\n\nВыбери плитку 1-4 (одна с ловушкой):"

def diamond_text(game):
    step = int(game["step"])
    bet = float(game["bet"])
    cur_mult = DIAMOND_MULTIPLIERS[step - 1] if step > 0 else 0
    cur_win = bet * cur_mult if step > 0 else 0
    next_mult = DIAMOND_MULTIPLIERS[step] if step < len(DIAMOND_MULTIPLIERS) else DIAMOND_MULTIPLIERS[-1]
    return f"💎 **Алмазы**\nСтавка: **{fmt_money(bet)}**\nШаг: **{step}**\nМножитель: **x{cur_mult:.2f}**\nВыигрыш: **{fmt_money(cur_win)}**\nСледующий: **x{next_mult:.2f}**\n\nВыбери кристалл 1-5 (один бракованный):"

def mines_text(game):
    bet = float(game["bet"])
    opened_count = len(game["opened"])
    mines_count = int(game["mines_count"])
    mult = mines_multiplier(opened_count, mines_count)
    potential = round(bet * mult, 2)
    return f"💣 **Мины**\nСтавка: **{fmt_money(bet)}**\nМин: **{mines_count}**\nОткрыто: **{opened_count}**\nМножитель: **x{mult:.2f}**\nПотенциал: **{fmt_money(potential)}**\n\nОткрывай клетки 1-9:"

def llama_text(game):
    level = int(game["level"])
    bet = float(game["bet"])
    current_mult = LLAMA_MULTIPLIERS[level - 1] if level > 0 else 0
    current_win = bet * current_mult if level > 0 else 0
    next_mult = LLAMA_MULTIPLIERS[level] if level < len(LLAMA_MULTIPLIERS) else LLAMA_MULTIPLIERS[-1]
    return f"🦙 **Ламы**\nСтавка: **{fmt_money(bet)}**\nУровень: **{level}**\nМножитель: **x{current_mult:.2f}**\nВыигрыш: **{fmt_money(current_win)}**\nСледующий: **x{next_mult:.2f}**\n\nВыбери ламу 1-4 (одна злая):"

# ========== КЛАВИАТУРЫ ==========
def main_menu_kb():
    buttons = []
    buttons.append([InlineKeyboardButton(text="💰 Профиль", callback_data="menu:profile")])
    buttons.append([InlineKeyboardButton(text="🎮 Игры", callback_data="menu:games")])
    buttons.append([InlineKeyboardButton(text="🏦 Банк", callback_data="menu:bank")])
    buttons.append([InlineKeyboardButton(text="⭐ Пополнить", callback_data="menu:deposit"), InlineKeyboardButton(text="💸 Вывести", callback_data="menu:withdraw")])
    buttons.append([InlineKeyboardButton(text="🎁 Бонус", callback_data="menu:bonus"), InlineKeyboardButton(text="🏆 Топ", callback_data="menu:top")])
    buttons.append([InlineKeyboardButton(text="🧾 Чеки", callback_data="menu:checks"), InlineKeyboardButton(text="🎟 Промо", callback_data="menu:promo")])
    buttons.append([InlineKeyboardButton(text="❓ Помощь", callback_data="menu:help"), InlineKeyboardButton(text="🆘 Поддержка", callback_data="menu:support")])
    if is_admin_user(8478884644):
        buttons.append([InlineKeyboardButton(text="🛠️ Админ", callback_data="admin:menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Выдать монеты", callback_data="admin:give")],
        [InlineKeyboardButton(text="👑 Выдать статус", callback_data="admin:setstatus")],
        [InlineKeyboardButton(text="🎟 Создать промо", callback_data="admin:createpromo")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="📋 Заявки на вывод", callback_data="admin:withdraws")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])

def games_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗼 Башня", callback_data="game:tower"), InlineKeyboardButton(text="🥇 Золото", callback_data="game:gold")],
        [InlineKeyboardButton(text="💎 Алмазы", callback_data="game:diamonds"), InlineKeyboardButton(text="💣 Мины", callback_data="game:mines")],
        [InlineKeyboardButton(text="🎴 Очко", callback_data="game:ochko"), InlineKeyboardButton(text="🦙 Ламы", callback_data="game:llama")],
        [InlineKeyboardButton(text="🎡 Рулетка", callback_data="game:roulette"), InlineKeyboardButton(text="📈 Краш", callback_data="game:crash")],
        [InlineKeyboardButton(text="🎲 Кубик", callback_data="game:cube"), InlineKeyboardButton(text="🎯 Кости", callback_data="game:dice")],
        [InlineKeyboardButton(text="⚽ Футбол", callback_data="game:football"), InlineKeyboardButton(text="🏀 Баскет", callback_data="game:basket")],
        [InlineKeyboardButton(text="🎯 Дартс", callback_data="game:darts")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])

def profile_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Пополнить", callback_data="menu:deposit"), InlineKeyboardButton(text="💸 Вывести", callback_data="menu:withdraw")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])

def bank_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Открыть депозит", callback_data="bank:open")],
        [InlineKeyboardButton(text="📜 Мои депозиты", callback_data="bank:list")],
        [InlineKeyboardButton(text="💰 Снять зрелые", callback_data="bank:withdraw")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])

def bank_terms_kb():
    rows = []
    for days, rate in BANK_TERMS.items():
        rows.append([InlineKeyboardButton(text=f"{days} дн. (+{int(rate * 100)}%)", callback_data=f"bank:term:{days}")])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="bank:term:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def checks_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать чек", callback_data="checks:create")],
        [InlineKeyboardButton(text="💸 Активировать чек", callback_data="checks:claim")],
        [InlineKeyboardButton(text="📄 Мои чеки", callback_data="checks:my")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])

def donate_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ 1 Star (2200)", callback_data="donate:1")],
        [InlineKeyboardButton(text="⭐ 5 Stars (11000)", callback_data="donate:5")],
        [InlineKeyboardButton(text="⭐ 10 Stars (22000)", callback_data="donate:10")],
        [InlineKeyboardButton(text="⭐ 25 Stars (55000)", callback_data="donate:25")],
        [InlineKeyboardButton(text="⭐ 50 Stars (110000)", callback_data="donate:50")],
        [InlineKeyboardButton(text="⭐ 100 Stars (220000)", callback_data="donate:100")],
        [InlineKeyboardButton(text="⭐ 500 Stars (1.1M)", callback_data="donate:500")],
        [InlineKeyboardButton(text="⭐ 1500 Stars (3.3M)", callback_data="donate:1500")],
        [InlineKeyboardButton(text="🔄 Ручное пополнение", callback_data="deposit:manual")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:deposit")],
    ])

def help_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📜 Правила", callback_data="help:rules")],
        [InlineKeyboardButton(text="🎮 Как играть", callback_data="help:howto")],
        [InlineKeyboardButton(text="📢 Наш канал", url=CHANNEL_LINK)],
        [InlineKeyboardButton(text="🆘 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])

def tower_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1", callback_data="tower:pick:1"), InlineKeyboardButton(text="2", callback_data="tower:pick:2"), InlineKeyboardButton(text="3", callback_data="tower:pick:3")],
        [InlineKeyboardButton(text="💰 Забрать", callback_data="tower:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="tower:cancel")],
    ])

def gold_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🧱 1", callback_data="gold:pick:1"), InlineKeyboardButton(text="🧱 2", callback_data="gold:pick:2"), InlineKeyboardButton(text="🧱 3", callback_data="gold:pick:3"), InlineKeyboardButton(text="🧱 4", callback_data="gold:pick:4")],
        [InlineKeyboardButton(text="💰 Забрать", callback_data="gold:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="gold:cancel")],
    ])

def diamond_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔹 1", callback_data="diamond:pick:1"), InlineKeyboardButton(text="🔹 2", callback_data="diamond:pick:2"), InlineKeyboardButton(text="🔹 3", callback_data="diamond:pick:3"), InlineKeyboardButton(text="🔹 4", callback_data="diamond:pick:4"), InlineKeyboardButton(text="🔹 5", callback_data="diamond:pick:5")],
        [InlineKeyboardButton(text="💰 Забрать", callback_data="diamond:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="diamond:cancel")],
    ])

def mines_kb(game, reveal_all=False):
    opened = set(game["opened"])
    mines = set(game["mines"])
    rows = []
    for start in (1, 4, 7):
        row = []
        for idx in range(start, start + 3):
            if idx in opened:
                text = "✅"
                callback = "mines:noop"
            elif reveal_all and idx in mines:
                text = "💣"
                callback = "mines:noop"
            else:
                text = str(idx)
                callback = f"mines:cell:{idx}"
            row.append(InlineKeyboardButton(text=text, callback_data=callback))
        rows.append(row)
    rows.append([InlineKeyboardButton(text="💰 Забрать", callback_data="mines:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="mines:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def ochko_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Взять", callback_data="ochko:hit"), InlineKeyboardButton(text="✋ Стоп", callback_data="ochko:stand")],
    ])

def ochko_confirm_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Начать", callback_data="ochko:start"), InlineKeyboardButton(text="❌ Отмена", callback_data="ochko:cancel")],
    ])

def llama_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="1", callback_data="llama:pick:1"), InlineKeyboardButton(text="2", callback_data="llama:pick:2"), InlineKeyboardButton(text="3", callback_data="llama:pick:3"), InlineKeyboardButton(text="4", callback_data="llama:pick:4")],
        [InlineKeyboardButton(text="💰 Забрать", callback_data="llama:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="llama:cancel")],
    ])

def roulette_choice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔴 Красное (x2)", callback_data="roulette:choice:red"), InlineKeyboardButton(text="⚫ Черное (x2)", callback_data="roulette:choice:black")],
        [InlineKeyboardButton(text="2️⃣ Чет (x2)", callback_data="roulette:choice:even"), InlineKeyboardButton(text="1️⃣ Нечет (x2)", callback_data="roulette:choice:odd")],
        [InlineKeyboardButton(text="0️⃣ Зеро (x36)", callback_data="roulette:choice:zero")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="roulette:cancel")],
    ])

def darts_choice_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Красное (x2)", callback_data="darts:choice:red"), InlineKeyboardButton(text="🎯 Черное (x2)", callback_data="darts:choice:black")],
        [InlineKeyboardButton(text="🎯 Чет (x2)", callback_data="darts:choice:even"), InlineKeyboardButton(text="🎯 Нечет (x2)", callback_data="darts:choice:odd")],
        [InlineKeyboardButton(text="🎯 Зеро (x36)", callback_data="darts:choice:zero")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="darts:cancel")],
    ])

def withdraw_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Вывести все", callback_data="withdraw:all")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:profile")],
    ])

def deposit_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Через Stars", callback_data="menu:donate")],
        [InlineKeyboardButton(text="💳 Ручное пополнение", callback_data="deposit:manual")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:profile")],
    ])

# ========== БОТ ==========
dp = Dispatcher(storage=MemoryStorage())
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

# ========== ОСНОВНЫЕ КОМАНДЫ ==========
@dp.message(CommandStart())
async def start_command(message: Message, state: FSMContext):
    user = message.from_user
    ensure_user(user.id, user.username or "")
    await state.clear()
    clear_active_sessions(user.id)
    stats = get_profile_stats(user.id)
    await message.answer(
        f"🦙 **Добро пожаловать в WILLD GRAMM!**\n\n"
        f"🎮 **Игровой бот с выводом и донатом**\n\n"
        f"💰 Твой баланс: **{fmt_money(stats['coins'])}**\n"
        f"👤 Твой статус: {get_user_status_text(stats['status'])}\n\n"
        f"📖 **Как играть:**\n"
        f"• Введи команду с игрой и ставкой\n"
        f"• Пример: `башня 500 2`\n"
        f"• Вместо суммы можно написать `все`\n\n"
        f"🎮 **Доступные игры:**\n"
        f"`башня`, `золото`, `алмазы`, `мины`, `рул`, `краш`,\n"
        f"`кубик`, `кости`, `очко`, `футбол`, `баскет`, `ламы`, `дартс`\n\n"
        f"⬇️ Используй кнопки ниже для навигации:",
        reply_markup=main_menu_kb()
    )

@dp.message(Command("admin"))
async def admin_command(message: Message):
    if not is_admin_user(message.from_user.id):
        await message.answer("⛔ У тебя нет доступа к админ-панели.")
        return
    await message.answer("🛠️ **Админ-панель**\n\nВыбери действие:", reply_markup=admin_kb())

@dp.message(Command("menu"))
async def menu_command(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("📱 **Главное меню**", reply_markup=main_menu_kb())

@dp.message(lambda m: normalize_text(m.text) in {"отмена", "/cancel", "cancel"})
async def cancel_any(message: Message, state: FSMContext):
    await state.clear()
    clear_active_sessions(message.from_user.id)
    await message.answer("🛑 Действие отменено.", reply_markup=main_menu_kb())

@dp.message(StateFilter(None), lambda m: normalize_text(m.text) in {"б", "баланс", "/balance", "balance", "профиль", "profile"})
async def profile_command(message: Message):
    stats = get_profile_stats(message.from_user.id)
    await message.answer(
        f"👤 **Твой профиль**\n\n"
        f"🆔 ID: `{message.from_user.id}`\n"
        f"👤 Статус: {get_user_status_text(stats['status'])}\n\n"
        f"💰 Баланс: **{fmt_money(stats['coins'])}**\n"
        f"⭐ Stars: **{stats['stars']}**\n\n"
        f"🎮 Сыграно игр: **{stats['total_games']}**\n"
        f"🏆 Побед: **{stats['total_wins']}**\n"
        f"📊 WR: **{(stats['total_wins']/max(1,stats['total_games'])*100):.1f}%**\n\n"
        f"🏦 Активных депозитов: **{stats['active_deposits']}**\n"
        f"💵 Сумма в депозитах: **{fmt_money(stats['active_deposit_sum'])}**",
        reply_markup=profile_kb()
    )

@dp.message(StateFilter(None), lambda m: normalize_text(m.text) in {"бонус", "/bonus", "bonus"})
async def bonus_command(message: Message):
    user_id = message.from_user.id
    key = f"bonus_ts:{user_id}"
    last = int(get_json_value(key, 0) or 0)
    now = now_ts()
    if now - last < BONUS_COOLDOWN_SECONDS:
        left = BONUS_COOLDOWN_SECONDS - (now - last)
        await message.answer(f"🎁 Ты уже получил бонус сегодня!\nОсталось: **{fmt_left(left)}**")
        return
    ok, balance = settle_instant_bet(user_id, 0.0, DAILY_BONUS, "bonus", "daily_bonus")
    if not ok:
        await message.answer("❌ Ошибка при выдаче бонуса")
        return
    set_json_value(key, now)
    await message.answer(f"🎁 **Ежедневный бонус!**\n+{fmt_money(DAILY_BONUS)}\nНовый баланс: **{fmt_money(balance)}**")

@dp.message(StateFilter(None), lambda m: normalize_text(m.text) in {"топ", "/top", "top"})
async def top_command(message: Message):
    balance_rows = get_top_balances(10)
    deposit_rows = get_top_deposits(10)
    text = "🏆 **ТОП ИГРОКОВ WILLD GRAMM**\n\n"
    text += "💰 **По балансу:**\n"
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for idx, row in enumerate(balance_rows, 1):
        icon = medals.get(idx, f"{idx}.")
        name = row["username"] or f"User_{row['id'][:6]}"
        text += f"{icon} {escape_html(name)} — {fmt_money(row['coins'])}\n"
    text += "\n🏦 **По депозитам:**\n"
    for idx, row in enumerate(deposit_rows, 1):
        icon = medals.get(idx, f"{idx}.")
        name = row["username"] or f"User_{row['id'][:6]}"
        text += f"{icon} {escape_html(name)} — {fmt_money(row['total_deposit'])}\n"
    await message.answer(text)

@dp.message(StateFilter(None), lambda m: normalize_text(m.text) in {"помощь", "/help", "help"})
async def help_command(message: Message):
    await message.answer(
        f"❓ **Помощь WILLD GRAMM**\n\n"
        f"**📜 Правила:**\n"
        f"• Ставки: от {MIN_BET} до {MAX_BET}\n"
        f"• Вывод средств: комиссия 5%\n"
        f"• Создание чека: комиссия 6%\n\n"
        f"**🎮 Как играть:**\n"
        f"• Введи название игры и ставку\n"
        f"• Пример: `башня 500 2`\n"
        f"• Вместо суммы можно написать `все`\n\n"
        f"**💰 Пополнение:**\n"
        f"• Через Stars: 1 Star = {STARS_RATE} {CURRENCY_NAME}\n"
        f"• Ручное: по ссылке в меню пополнения\n\n"
        f"**📢 Наш канал:** {CHANNEL_LINK}\n"
        f"**🆘 Поддержка:** {SUPPORT_USERNAME} / {SUPPORT_USERNAME2}",
        reply_markup=help_kb()
    )

# ========== КНОПКИ МЕНЮ ==========
@dp.callback_query(F.data == "menu:main")
async def menu_main_cb(query: CallbackQuery, state: FSMContext):
    await state.clear()
    await query.message.edit_text("📱 **Главное меню**", reply_markup=main_menu_kb())
    await query.answer()

@dp.callback_query(F.data == "menu:profile")
async def menu_profile_cb(query: CallbackQuery):
    stats = get_profile_stats(query.from_user.id)
    await query.message.edit_text(
        f"👤 **Твой профиль**\n\n"
        f"🆔 ID: `{query.from_user.id}`\n"
        f"👤 Статус: {get_user_status_text(stats['status'])}\n\n"
        f"💰 Баланс: **{fmt_money(stats['coins'])}**\n"
        f"⭐ Stars: **{stats['stars']}**\n\n"
        f"🎮 Сыграно игр: **{stats['total_games']}**\n"
        f"🏆 Побед: **{stats['total_wins']}**\n"
        f"🏦 Депозитов: **{stats['active_deposits']}**",
        reply_markup=profile_kb()
    )
    await query.answer()

@dp.callback_query(F.data == "menu:games")
async def menu_games_cb(query: CallbackQuery):
    await query.message.edit_text(
        "🎮 **Выбери игру**\n\n"
        "Ты также можешь использовать команды:\n"
        "`башня 1000 2`\n`золото 1000`\n`алмазы 1000`\n`мины 1000 3`\n"
        "`рул 1000 чет`\n`краш 1000 2.5`\n`кубик 1000 5`\n`кости 1000 м`\n"
        "`очко 1000`\n`футбол 1000 гол`\n`баскет 1000`\n`ламы 1000`\n`дартс 1000 красное`",
        reply_markup=games_kb()
    )
    await query.answer()

@dp.callback_query(F.data == "menu:bank")
async def menu_bank_cb(query: CallbackQuery):
    summary = get_bank_summary(query.from_user.id)
    await query.message.edit_text(
        f"🏦 **Банк**\n\n"
        f"💰 Баланс: **{fmt_money(summary['coins'])}**\n"
        f"📊 Активных депозитов: **{summary['count_active']}**\n"
        f"💵 Сумма в депозитах: **{fmt_money(summary['active_sum'])}**\n\n"
        f"**Доходность:**\n"
        f"• 7 дней: +3%\n"
        f"• 14 дней: +7%\n"
        f"• 30 дней: +18%",
        reply_markup=bank_kb()
    )
    await query.answer()

@dp.callback_query(F.data == "menu:deposit")
async def menu_deposit_cb(query: CallbackQuery):
    await query.message.edit_text(
        f"⭐ **Пополнение баланса**\n\n"
        f"**Способ 1: Telegram Stars**\n"
        f"1 Star = {STARS_RATE} {CURRENCY_NAME}\n"
        f"Мин: {MIN_STARS} Stars, Макс: {MAX_STARS} Stars\n\n"
        f"**Способ 2: Ручное пополнение**\n"
        f"Перейдите в чат: {MANUAL_DEPOSIT_CHAT}\n"
        f"Следуйте инструкции в закреплённом сообщении",
        reply_markup=deposit_kb()
    )
    await query.answer()

@dp.callback_query(F.data == "menu:donate")
async def menu_donate_cb(query: CallbackQuery):
    await query.message.edit_text(
        f"⭐ **Пополнение через Stars**\n\n"
        f"Курс: 1 Star = {STARS_RATE} {CURRENCY_NAME}\n"
        f"Мин: {MIN_STARS} Stars\n"
        f"Макс: {MAX_STARS} Stars\n\n"
        f"Выбери сумму:",
        reply_markup=donate_kb()
    )
    await query.answer()

@dp.callback_query(F.data == "deposit:manual")
async def deposit_manual_cb(query: CallbackQuery):
    await query.message.answer(
        f"💳 **Ручное пополнение**\n\n"
        f"1. Перейдите в чат: {MANUAL_DEPOSIT_CHAT}\n"
        f"2. Прочитайте закреплённое сообщение\n"
        f"3. Следуйте инструкции\n\n"
        f"После перевода средства поступят в течение пары минут.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📱 Перейти в чат", url=MANUAL_DEPOSIT_CHAT)],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:deposit")]
        ])
    )
    await query.answer()

@dp.callback_query(F.data == "menu:withdraw")
async def menu_withdraw_cb(query: CallbackQuery, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_amount)
    await query.message.edit_text(
        "💸 **Вывод средств**\n\n"
        "Комиссия: 5%\n"
        "Минимальная сумма: 100 WILLD GRAMM\n\n"
        "Введи сумму для вывода (или напиши `все`):",
        reply_markup=withdraw_kb()
    )
    await query.answer()

@dp.callback_query(F.data == "withdraw:all")
async def withdraw_all_cb(query: CallbackQuery, state: FSMContext):
    await state.update_data(amount=-1)
    await process_withdraw(query.message, state, query.from_user.id)
    await query.answer()

@dp.message(WithdrawStates.waiting_amount)
async def withdraw_amount(message: Message, state: FSMContext):
    try:
        amount = parse_amount(message.text)
    except:
        await message.answer("❌ Введи корректную сумму.")
        return
    await state.update_data(amount=amount)
    await process_withdraw(message, state, message.from_user.id)

async def process_withdraw(message: Message, state: FSMContext, user_id: int):
    data = await state.get_data()
    amount = float(data.get("amount", 0))
    await state.clear()
    ok, msg = request_withdraw(user_id, amount)
    if ok:
        await message.answer(f"✅ {msg}", reply_markup=main_menu_kb())
    else:
        await message.answer(f"❌ {msg}", reply_markup=profile_kb())

@dp.callback_query(F.data == "menu:bonus")
async def menu_bonus_cb(query: CallbackQuery):
    user_id = query.from_user.id
    key = f"bonus_ts:{user_id}"
    last = int(get_json_value(key, 0) or 0)
    now = now_ts()
    if now - last < BONUS_COOLDOWN_SECONDS:
        left = BONUS_COOLDOWN_SECONDS - (now - last)
        await query.message.answer(f"🎁 Ты уже получил бонус сегодня!\nОсталось: **{fmt_left(left)}**")
    else:
        ok, balance = settle_instant_bet(user_id, 0.0, DAILY_BONUS, "bonus", "daily_bonus")
        if ok:
            set_json_value(key, now)
            await query.message.answer(f"🎁 **Ежедневный бонус!**\n+{fmt_money(DAILY_BONUS)}\nНовый баланс: **{fmt_money(balance)}**")
        else:
            await query.message.answer("❌ Ошибка при выдаче бонуса")
    await query.answer()

@dp.callback_query(F.data == "menu:top")
async def menu_top_cb(query: CallbackQuery):
    balance_rows = get_top_balances(10)
    deposit_rows = get_top_deposits(10)
    text = "🏆 **ТОП ИГРОКОВ WILLD GRAMM**\n\n"
    text += "💰 **По балансу:**\n"
    medals = {1: "🥇", 2: "🥈", 3: "🥉"}
    for idx, row in enumerate(balance_rows, 1):
        icon = medals.get(idx, f"{idx}.")
        name = row["username"] or f"User_{row['id'][:6]}"
        text += f"{icon} {escape_html(name)} — {fmt_money(row['coins'])}\n"
    text += "\n🏦 **По депозитам:**\n"
    for idx, row in enumerate(deposit_rows, 1):
        icon = medals.get(idx, f"{idx}.")
        name = row["username"] or f"User_{row['id'][:6]}"
        text += f"{icon} {escape_html(name)} — {fmt_money(row['total_deposit'])}\n"
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")]
    ]))
    await query.answer()

@dp.callback_query(F.data == "menu:checks")
async def menu_checks_cb(query: CallbackQuery):
    await query.message.edit_text(
        "🧾 **Чеки**\n\n"
        "Создание чека: комиссия 6%\n"
        "Активация чека: бесплатно\n\n"
        "Выбери действие:",
        reply_markup=checks_kb()
    )
    await query.answer()

@dp.callback_query(F.data == "menu:promo")
async def menu_promo_cb(query: CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_code)
    await query.message.edit_text(
        "🎟 **Активация промокода**\n\n"
        "Введи код промокода:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")]
        ])
    )
    await query.answer()

@dp.message(PromoStates.waiting_code)
async def promo_code_input(message: Message, state: FSMContext):
    code = message.text.strip()
    ok, msg, reward = redeem_promo_atomic(message.from_user.id, code)
    await state.clear()
    if not ok:
        await message.answer(f"❌ {msg}")
        return
    await message.answer(f"✅ {msg}\n+{fmt_money(reward)}", reply_markup=main_menu_kb())

@dp.callback_query(F.data == "menu:help")
async def menu_help_cb(query: CallbackQuery):
    await query.message.edit_text(
        f"❓ **Помощь WILLD GRAMM**\n\n"
        f"**Правила:**\n"
        f"• Ставки: от {MIN_BET} до {MAX_BET}\n"
        f"• Вывод: комиссия 5%\n"
        f"• Чек: комиссия 6%\n\n"
        f"**Как играть:**\n"
        f"• Введи название игры и ставку\n"
        f"• Пример: `башня 500 2`\n"
        f"• Вместо суммы можно написать `все`\n\n"
        f"**Канал:** {CHANNEL_LINK}\n"
        f"**Поддержка:** {SUPPORT_USERNAME}",
        reply_markup=help_kb()
    )
    await query.answer()

@dp.callback_query(F.data == "menu:support")
async def menu_support_cb(query: CallbackQuery):
    await query.message.answer(
        f"🆘 **Поддержка WILLD GRAMM**\n\n"
        f"По всем вопросам:\n"
        f"{SUPPORT_USERNAME}\n"
        f"{SUPPORT_USERNAME2}\n\n"
        f"📢 Наш канал: {CHANNEL_LINK}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="💬 Написать поддержке", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")]
        ])
    )
    await query.answer()

# ========== АДМИН-МЕНЮ ==========
@dp.callback_query(F.data == "admin:menu")
async def admin_menu_cb(query: CallbackQuery):
    if not is_admin_user(query.from_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return
    await query.message.edit_text("🛠️ **Админ-панель**\n\nВыбери действие:", reply_markup=admin_kb())
    await query.answer()

@dp.callback_query(F.data == "admin:give")
async def admin_give_cb(query: CallbackQuery, state: FSMContext):
    if not is_admin_user(query.from_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return
    await state.set_state(AdminGiveStates.waiting_data)
    await query.message.edit_text(
        "💰 **Выдать монеты**\n\n"
        "Введи ID пользователя и сумму через пробел:\n"
        "Пример: `8478884644 1000`",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]
        ])
    )
    await query.answer()

@dp.message(AdminGiveStates.waiting_data)
async def admin_give_process(message: Message, state: FSMContext):
    if not is_admin_user(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        await state.clear()
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.answer("❌ Неверный формат. Пример: `8478884644 1000`")
        return
    try:
        user_id = int(parts[0])
        amount = float(parts[1])
    except:
        await message.answer("❌ Неверный формат. Пример: `8478884644 1000`")
        return
    if amount <= 0:
        await message.answer("❌ Сумма должна быть положительной")
        return
    new_balance = add_coins(user_id, amount)
    await state.clear()
    await message.answer(f"✅ Выдано {fmt_money(amount)} пользователю `{user_id}`\nНовый баланс: {fmt_money(new_balance)}", reply_markup=admin_kb())

@dp.callback_query(F.data == "admin:setstatus")
async def admin_setstatus_cb(query: CallbackQuery, state: FSMContext):
    if not is_admin_user(query.from_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return
    await state.set_state(SetStatusStates.waiting_user)
    await query.message.edit_text(
        "👑 **Выдать статус**\n\n"
        "Введи ID пользователя или @username:\n"
        "Пример: `8478884644` или `@username`",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]
        ])
    )
    await query.answer()

@dp.message(SetStatusStates.waiting_user)
async def admin_status_user(message: Message, state: FSMContext):
    if not is_admin_user(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        await state.clear()
        return
    text = message.text.strip()
    user_id = None
    if text.startswith("@"):
        conn = get_db()
        row = conn.execute("SELECT id FROM users WHERE username = ?", (text[1:],)).fetchone()
        conn.close()
        if row:
            user_id = int(row["id"])
    else:
        try:
            user_id = int(text)
        except:
            pass
    if not user_id:
        await message.answer("❌ Пользователь не найден")
        return
    await state.update_data(target_id=user_id)
    await state.set_state(SetStatusStates.waiting_status)
    await message.answer("Выбери статус:", reply_markup=admin_status_kb())

@dp.callback_query(SetStatusStates.waiting_status, F.data.startswith("admin:status:"))
async def admin_status_choose(query: CallbackQuery, state: FSMContext):
    if not is_admin_user(query.from_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return
    status = int(query.data.split(":")[-1])
    data = await state.get_data()
    user_id = data.get("target_id")
    if not user_id:
        await query.answer("❌ Ошибка, попробуй снова", show_alert=True)
        await state.clear()
        return
    set_user_status(user_id, status)
    await state.clear()
    await query.message.edit_text(f"✅ Статус пользователя `{user_id}` изменён на {get_user_status_text(status)}", reply_markup=admin_kb())
    await query.answer()

@dp.callback_query(F.data == "admin:createpromo")
async def admin_createpromo_cb(query: CallbackQuery, state: FSMContext):
    if not is_admin_user(query.from_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return
    await state.set_state(NewPromoStates.waiting_code)
    await query.message.edit_text(
        "🎟 **Создание промокода**\n\n"
        "Введи код промокода (3-24 символа, A-Z, 0-9, _-):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]
        ])
    )
    await query.answer()

@dp.message(NewPromoStates.waiting_code)
async def admin_promo_code(message: Message, state: FSMContext):
    if not is_admin_user(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        await state.clear()
        return
    try:
        code = normalize_promo_code(message.text)
    except:
        await message.answer("❌ Некорректный код. Используй A-Z, 0-9, _- (3-24 символа)")
        return
    await state.update_data(code=code)
    await state.set_state(NewPromoStates.waiting_reward)
    await message.answer("💰 Введи награду (количество монет):")

@dp.message(NewPromoStates.waiting_reward)
async def admin_promo_reward(message: Message, state: FSMContext):
    if not is_admin_user(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        await state.clear()
        return
    try:
        reward = parse_amount(message.text)
        if reward == -1:
            raise ValueError
    except:
        await message.answer("❌ Введи корректную сумму награды")
        return
    await state.update_data(reward=reward)
    await state.set_state(NewPromoStates.waiting_activations)
    await message.answer("🔢 Введи количество активаций (целое число):")

@dp.message(NewPromoStates.waiting_activations)
async def admin_promo_activations(message: Message, state: FSMContext):
    if not is_admin_user(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        await state.clear()
        return
    try:
        activations = int(message.text)
    except:
        await message.answer("❌ Введи целое число")
        return
    if activations <= 0:
        await message.answer("❌ Количество активаций должно быть больше 0")
        return
    data = await state.get_data()
    code = data.get("code")
    reward = data.get("reward")
    create_promo(code, reward, activations, message.from_user.id)
    await state.clear()
    await message.answer(f"✅ **Промокод создан!**\n\nКод: `{code}`\nНаграда: {fmt_money(reward)}\nАктиваций: {activations}", reply_markup=admin_kb())

@dp.callback_query(F.data == "admin:stats")
async def admin_stats_cb(query: CallbackQuery):
    if not is_admin_user(query.from_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return
    conn = get_db()
    try:
        total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        total_coins = conn.execute("SELECT SUM(coins) FROM users").fetchone()[0] or 0
        total_bets = conn.execute("SELECT COUNT(*) FROM bets").fetchone()[0]
        total_promo = conn.execute("SELECT COUNT(*) FROM promos").fetchone()[0]
        total_checks = conn.execute("SELECT COUNT(*) FROM checks").fetchone()[0]
        total_deposits = conn.execute("SELECT COUNT(*) FROM bank_deposits WHERE status='active'").fetchone()[0]
    finally:
        conn.close()
    await query.message.edit_text(
        f"📊 **Статистика бота WILLD GRAMM**\n\n"
        f"👥 Пользователей: **{total_users}**\n"
        f"💰 Всего монет: **{fmt_money(total_coins)}**\n"
        f"🎮 Всего ставок: **{total_bets}**\n"
        f"🎟 Промокодов: **{total_promo}**\n"
        f"🧾 Чеков: **{total_checks}**\n"
        f"🏦 Активных депозитов: **{total_deposits}**",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]
        ])
    )
    await query.answer()

@dp.callback_query(F.data == "admin:withdraws")
async def admin_withdraws_cb(query: CallbackQuery):
    if not is_admin_user(query.from_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return
    conn = get_db()
    try:
        rows = conn.execute("SELECT id, username, withdraws FROM users").fetchall()
        pending = []
        for row in rows:
            try:
                withdraws = json.loads(row["withdraws"] or "[]")
                for w in withdraws:
                    if w.get("status") == "pending":
                        pending.append((row["id"], row["username"], w))
            except:
                pass
    finally:
        conn.close()
    if not pending:
        await query.message.edit_text(
            "📋 **Заявки на вывод**\n\nНет активных заявок.",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]
            ])
        )
    else:
        text = "📋 **Заявки на вывод**\n\n"
        for user_id, username, w in pending[:10]:
            text += f"👤 {mention_user(int(user_id), username)}\n"
            text += f"💰 Сумма: {fmt_money(w['amount'])}\n"
            text += f"💸 К выдаче: {fmt_money(w['amount_net'])}\n"
            text += f"📅 {fmt_dt(w['ts'])}\n"
            text += f"➡️ /approve_withdraw {user_id}\n\n"
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]
        ]))
    await query.answer()

@dp.message(Command("approve_withdraw"))
async def approve_withdraw(message: Message):
    if not is_admin_user(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        return
    parts = message.text.strip().split()
    if len(parts) != 2:
        await message.answer("❌ Формат: /approve_withdraw USER_ID")
        return
    user_id = parts[1]
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT withdraws FROM users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            await message.answer("❌ Пользователь не найден")
            return
        withdraws = json.loads(row["withdraws"] or "[]")
        updated = False
        for w in withdraws:
            if w.get("status") == "pending":
                w["status"] = "approved"
                updated = True
                break
        if updated:
            conn.execute("UPDATE users SET withdraws = ? WHERE id = ?", (json.dumps(withdraws), user_id))
            conn.commit()
            await message.answer(f"✅ Заявка на вывод для пользователя {mention_user(int(user_id))} одобрена")
        else:
            await message.answer("❌ Нет активных заявок на вывод")
    except Exception as e:
        conn.rollback()
        await message.answer(f"❌ Ошибка: {e}")
    finally:
        conn.close()

# ========== ДОНАТ ==========
@dp.callback_query(F.data.startswith("donate:"))
async def donate_callback(query: CallbackQuery):
    stars = int(query.data.split(":")[1])
    if stars < MIN_STARS or stars > MAX_STARS:
        await query.answer(f"❌ Минимум {MIN_STARS} Stars, максимум {MAX_STARS}", show_alert=True)
        return
    prices = [LabeledPrice(label=f"{stars} Telegram Stars", amount=stars)]
    await query.bot.send_invoice(
        chat_id=query.from_user.id,
        title="Пополнение WILLD GRAMM",
        description=f"Получи {fmt_money(stars * STARS_RATE)} за {stars} Stars",
        payload=f"donate_{stars}_{query.from_user.id}",
        provider_token="",
        currency="XTR",
        prices=prices,
        start_parameter="donate",
    )
    await query.answer()

@dp.pre_checkout_query()
async def pre_checkout_handler(pre_checkout: PreCheckoutQuery):
    await pre_checkout.answer(ok=True)

@dp.message(F.successful_payment)
async def successful_payment_handler(message: Message):
    payment = message.successful_payment
    payload = payment.invoice_payload
    try:
        stars = int(payload.split("_")[1])
        user_id = int(payload.split("_")[2])
        if user_id != message.from_user.id:
            return
        add_stars(user_id, stars)
        await message.answer(
            f"✅ **Пополнение успешно!**\n\n"
            f"⭐ {stars} Stars → {fmt_money(stars * STARS_RATE)}\n\n"
            f"Баланс обновлён. Приятной игры в WILLD GRAMM! 🎮",
            reply_markup=main_menu_kb()
        )
    except:
        await message.answer("❌ Ошибка при обработке платежа. Обратитесь к администратору.")

# ========== ИГРЫ (КОМАНДЫ) ==========
@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("башня"))
async def tower_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 2:
        await message.answer("❌ Формат: `башня ставка [мины 1-3]`\nПример: `башня 1000 2`")
        return
    try:
        bet = parse_amount(parts[1])
    except:
        await message.answer("❌ Неверная ставка.")
        return
    mines = 1
    if len(parts) >= 3:
        try:
            mines = int(parts[2])
            if mines < 1 or mines > 3:
                mines = 1
        except:
            pass
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    ok, _ = reserve_bet(message.from_user.id, bet)
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    TOWER_GAMES[message.from_user.id] = {"bet": bet, "level": 0, "mines": mines}
    await message.answer(tower_text(TOWER_GAMES[message.from_user.id]), reply_markup=tower_kb())

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("золото"))
async def gold_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) != 2:
        await message.answer("❌ Формат: `золото ставка`\nПример: `золото 1000`")
        return
    try:
        bet = parse_amount(parts[1])
    except:
        await message.answer("❌ Неверная ставка.")
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    ok, _ = reserve_bet(message.from_user.id, bet)
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    GOLD_GAMES[message.from_user.id] = {"bet": bet, "step": 0}
    await message.answer(gold_text(GOLD_GAMES[message.from_user.id]), reply_markup=gold_kb())

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("алмазы"))
async def diamond_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 2:
        await message.answer("❌ Формат: `алмазы ставка`\nПример: `алмазы 1000`")
        return
    try:
        bet = parse_amount(parts[1])
    except:
        await message.answer("❌ Неверная ставка.")
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    ok, _ = reserve_bet(message.from_user.id, bet)
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    DIAMOND_GAMES[message.from_user.id] = {"bet": bet, "step": 0}
    await message.answer(diamond_text(DIAMOND_GAMES[message.from_user.id]), reply_markup=diamond_kb())

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("мины"))
async def mines_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 2:
        await message.answer("❌ Формат: `мины ставка [мины 1-5]`\nПример: `мины 1000 3`")
        return
    try:
        bet = parse_amount(parts[1])
    except:
        await message.answer("❌ Неверная ставка.")
        return
    mines_count = 3
    if len(parts) >= 3:
        try:
            mines_count = int(parts[2])
            if mines_count < 1 or mines_count > 5:
                mines_count = 3
        except:
            pass
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    ok, _ = reserve_bet(message.from_user.id, bet)
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    cells = list(range(1, 10))
    mines = set(random.sample(cells, mines_count))
    MINES_GAMES[message.from_user.id] = {"bet": bet, "mines_count": mines_count, "mines": mines, "opened": set()}
    await message.answer(mines_text(MINES_GAMES[message.from_user.id]), reply_markup=mines_kb(MINES_GAMES[message.from_user.id]))

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("очко"))
async def ochko_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) != 2:
        await message.answer("❌ Формат: `очко ставка`\nПример: `очко 1000`")
        return
    try:
        bet = parse_amount(parts[1])
    except:
        await message.answer("❌ Неверная ставка.")
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    await state.update_data(bet=bet)
    await state.set_state(OchkoStates.waiting_confirm)
    await message.answer(
        f"🎴 **Очко**\n\nСтавка: **{fmt_money(bet)}**\n\n"
        f"Желаешь начать игру?",
        reply_markup=ochko_confirm_kb()
    )

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("ламы"))
async def llama_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) != 2:
        await message.answer("❌ Формат: `ламы ставка`\nПример: `ламы 1000`")
        return
    try:
        bet = parse_amount(parts[1])
    except:
        await message.answer("❌ Неверная ставка.")
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    ok, _ = reserve_bet(message.from_user.id, bet)
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    LLAMA_GAMES[message.from_user.id] = {"bet": bet, "level": 0}
    await message.answer(llama_text(LLAMA_GAMES[message.from_user.id]), reply_markup=llama_kb())

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("рул"))
async def roulette_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 2:
        await message.answer("❌ Формат: `рул ставка [красное/черное/чет/нечет/зеро]`\nПример: `рул 1000 чет`")
        return
    try:
        bet = parse_amount(parts[1])
    except:
        await message.answer("❌ Неверная ставка.")
        return
    choice = None
    if len(parts) >= 3:
        c = parts[2]
        if c in ["красное", "красный", "кра", "red"]:
            choice = "red"
        elif c in ["черное", "черный", "чер", "black"]:
            choice = "black"
        elif c in ["чет", "четное", "even"]:
            choice = "even"
        elif c in ["нечет", "нечетное", "odd"]:
            choice = "odd"
        elif c in ["зеро", "zero", "0"]:
            choice = "zero"
    if not choice:
        await state.update_data(bet=bet)
        await state.set_state(RouletteStates.waiting_choice)
        await message.answer(f"🎡 **Рулетка**\n\nСтавка: {fmt_money(bet)}\n\nВыбери сектор:", reply_markup=roulette_choice_kb())
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    win, payout, result_text, number = await play_roulette_with_sticker(message, bet, choice)
    ok, balance = settle_instant_bet(message.from_user.id, bet, payout, f"roulette:{choice}", f"number={number}")
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    await message.answer(
        f"{result_text}\n\n"
        f"Результат: **{'Победа' if win else 'Поражение'}**\n"
        f"Выплата: **{fmt_money(payout)}**\n"
        f"Баланс: **{fmt_money(balance)}**"
    )

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("краш"))
async def crash_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 3:
        await message.answer("❌ Формат: `краш ставка множитель`\nПример: `краш 1000 2.5`")
        return
    try:
        bet = parse_amount(parts[1])
        target = float(parts[2].replace(",", "."))
    except:
        await message.answer("❌ Неверные данные.")
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    if target < 1.01 or target > 10:
        await message.answer("❌ Множитель должен быть от 1.01 до 10")
        return
    win, crash_multiplier = await crash_roll_with_animation(message, target)
    payout = round(bet * target, 2) if win else 0.0
    ok, balance = settle_instant_bet(message.from_user.id, bet, payout, f"crash:{target}", f"crash={crash_multiplier}")
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    await message.answer(
        f"📈 **Краш**\n\n"
        f"Твой множитель: **x{target}**\n"
        f"Множитель игры: **x{crash_multiplier}**\n"
        f"Результат: **{'Победа' if win else 'Поражение'}**\n"
        f"Выплата: **{fmt_money(payout)}**\n"
        f"Баланс: **{fmt_money(balance)}**"
    )

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("кубик"))
async def cube_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 3:
        await message.answer("❌ Формат: `кубик ставка 1-6`\nПример: `кубик 1000 5`")
        return
    try:
        bet = parse_amount(parts[1])
        guess = int(parts[2])
    except:
        await message.answer("❌ Неверные данные.")
        return
    if guess < 1 or guess > 6:
        await message.answer("❌ Число должно быть от 1 до 6")
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    dice_msg = await message.answer_dice(emoji="🎲")
    rolled = int(dice_msg.dice.value)
    win = guess == rolled
    payout = round(bet * 5.8, 2) if win else 0.0
    ok, balance = settle_instant_bet(message.from_user.id, bet, payout, f"cube:{guess}", f"rolled={rolled}")
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    await message.answer(
        f"🎲 **Кубик**\n\n"
        f"Твой выбор: **{guess}**\n"
        f"Выпало: **{rolled}**\n"
        f"Результат: **{'Победа' if win else 'Поражение'}**\n"
        f"Выплата: **{fmt_money(payout)}**\n"
        f"Баланс: **{fmt_money(balance)}**"
    )

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("кости"))
async def dice_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 3:
        await message.answer("❌ Формат: `кости ставка м/б/равно`\nПример: `кости 1000 м`")
        return
    try:
        bet = parse_amount(parts[1])
        choice = parts[2]
    except:
        await message.answer("❌ Неверные данные.")
        return
    if choice not in ["м", "б", "равно"]:
        await message.answer("❌ Выбери: `м` (меньше 7), `б` (больше 7) или `равно`")
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    d1_msg = await message.answer_dice(emoji="🎲")
    d2_msg = await message.answer_dice(emoji="🎲")
    d1 = int(d1_msg.dice.value)
    d2 = int(d2_msg.dice.value)
    total = d1 + d2
    win = False
    mult = 0.0
    if choice == "м" and total < 7:
        win, mult = True, 2.25
    elif choice == "б" and total > 7:
        win, mult = True, 2.25
    elif choice == "равно" and total == 7:
        win, mult = True, 5.0
    payout = round(bet * mult, 2) if win else 0.0
    ok, balance = settle_instant_bet(message.from_user.id, bet, payout, f"dice:{choice}", f"{d1}+{d2}={total}")
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    relation = "меньше 7" if total < 7 else ("больше 7" if total > 7 else "равно 7")
    await message.answer(
        f"🎯 **Кости**\n\n"
        f"Кубики: **{d1}** + **{d2}** = **{total}** ({relation})\n"
        f"Результат: **{'Победа' if win else 'Поражение'}**\n"
        f"Выплата: **{fmt_money(payout)}**\n"
        f"Баланс: **{fmt_money(balance)}**"
    )

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("футбол"))
async def football_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 2:
        await message.answer("❌ Формат: `футбол ставка [гол/мимо]`\nПример: `футбол 1000 гол`")
        return
    try:
        bet = parse_amount(parts[1])
    except:
        await message.answer("❌ Неверная ставка.")
        return
    choice = None
    if len(parts) >= 3:
        c = parts[2]
        if c in ["гол", "gol", "goal"]:
            choice = "gol"
        elif c in ["мимо", "mimo", "miss"]:
            choice = "mimo"
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    dice_msg = await message.answer_dice(emoji="⚽")
    value = int(dice_msg.dice.value)
    outcome = "мимо" if value <= 2 else "гол"
    win = False
    if choice:
        win = outcome == choice
        payout = round(bet * 1.85, 2) if win else 0.0
        ok, balance = settle_instant_bet(message.from_user.id, bet, payout, f"football:{choice}", f"result={outcome}")
    else:
        win = value >= 4
        payout = round(bet * 1.85, 2) if win else 0.0
        ok, balance = settle_instant_bet(message.from_user.id, bet, payout, "football", f"value={value}")
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    await message.answer(
        f"⚽ **Футбол**\n\n"
        f"Итог удара: **{'ГОЛ!' if outcome == 'гол' else 'Мимо'}**\n"
        f"Результат: **{'Победа' if win else 'Поражение'}**\n"
        f"Выплата: **{fmt_money(payout)}**\n"
        f"Баланс: **{fmt_money(balance)}**"
    )

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("баскет"))
async def basket_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 2:
        await message.answer("❌ Формат: `баскет ставка`\nПример: `баскет 1000`")
        return
    try:
        bet = parse_amount(parts[1])
    except:
        await message.answer("❌ Неверная ставка.")
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    dice_msg = await message.answer_dice(emoji="🏀")
    value = int(dice_msg.dice.value)
    win = value in {4, 5}
    payout = round(bet * 2.2, 2) if win else 0.0
    ok, balance = settle_instant_bet(message.from_user.id, bet, payout, "basketball", f"value={value}")
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    await message.answer(
        f"🏀 **Баскет**\n\n"
        f"Итог броска: **{'ПОПАДАНИЕ!' if win else 'Промах'}**\n"
        f"Результат: **{'Победа' if win else 'Поражение'}**\n"
        f"Выплата: **{fmt_money(payout)}**\n"
        f"Баланс: **{fmt_money(balance)}**"
    )

@dp.message(StateFilter(None), lambda m: (m.text or "").lower().startswith("дартс"))
async def darts_start(message: Message, state: FSMContext):
    await state.clear()
    parts = message.text.lower().split()
    if len(parts) < 3:
        await message.answer("❌ Формат: `дартс ставка красное/черное/чет/нечет/зеро`\nПример: `дартс 1000 красное`")
        return
    try:
        bet = parse_amount(parts[1])
        choice_text = parts[2]
    except:
        await message.answer("❌ Неверные данные.")
        return
    if choice_text in ["красное", "красный", "кра", "red"]:
        choice = "red"
    elif choice_text in ["черное", "черный", "чер", "black"]:
        choice = "black"
    elif choice_text in ["чет", "четное", "even"]:
        choice = "even"
    elif choice_text in ["нечет", "нечетное", "odd"]:
        choice = "odd"
    elif choice_text in ["зеро", "zero", "0"]:
        choice = "zero"
    else:
        await message.answer("❌ Выбери: красное, черное, чет, нечет или зеро")
        return
    if bet == -1:
        user = get_user(message.from_user.id)
        bet = float(user["coins"] or 0)
    if bet < MIN_BET:
        await message.answer(f"❌ Минимальная ставка: {MIN_BET}")
        return
    if bet > MAX_BET:
        await message.answer(f"❌ Максимальная ставка: {fmt_money(MAX_BET)}")
        return
    number = random.randint(0, 20)
    color = "green" if number == 0 else ("red" if number % 2 == 1 else "black")
    parity = "zero" if number == 0 else ("even" if number % 2 == 0 else "odd")
    win = False
    multiplier = 0.0
    if choice == "red" and color == "red":
        win, multiplier = True, 2.0
    elif choice == "black" and color == "black":
        win, multiplier = True, 2.0
    elif choice == "even" and parity == "even":
        win, multiplier = True, 2.0
    elif choice == "odd" and parity == "odd":
        win, multiplier = True, 2.0
    elif choice == "zero" and number == 0:
        win, multiplier = True, 20.0
    payout = round(bet * multiplier, 2) if win else 0.0
    ok, balance = settle_instant_bet(message.from_user.id, bet, payout, f"darts:{choice}", f"number={number}")
    if not ok:
        await message.answer("❌ Недостаточно средств.")
        return
    color_emoji = {"red": "🔴", "black": "⚫", "green": "🟢"}[color]
    color_text = {"red": "красное", "black": "чёрное", "green": "зелёное (яблочко)"}[color]
    await message.answer(
        f"🎯 **Дартс**\n\n"
        f"{color_emoji} Выпало **{number}** ({color_text})\n"
        f"Результат: **{'Победа' if win else 'Поражение'}**\n"
        f"Выплата: **{fmt_money(payout)}**\n"
        f"Баланс: **{fmt_money(balance)}**"
    )

# ========== КНОПКИ ИГР ==========
@dp.callback_query(F.data == "game:tower")
async def game_tower_cb(query: CallbackQuery):
    await query.message.answer("🗼 **Башня**\nВведи ставку и количество мин (1-3):\nПример: `башня 1000 2`")
    await query.answer()

@dp.callback_query(F.data == "game:gold")
async def game_gold_cb(query: CallbackQuery):
    await query.message.answer("🥇 **Золото**\nВведи ставку:\nПример: `золото 1000`")
    await query.answer()

@dp.callback_query(F.data == "game:diamonds")
async def game_diamonds_cb(query: CallbackQuery):
    await query.message.answer("💎 **Алмазы**\nВведи ставку:\nПример: `алмазы 1000`")
    await query.answer()

@dp.callback_query(F.data == "game:mines")
async def game_mines_cb(query: CallbackQuery):
    await query.message.answer("💣 **Мины**\nВведи ставку и количество мин (1-5):\nПример: `мины 1000 3`")
    await query.answer()

@dp.callback_query(F.data == "game:ochko")
async def game_ochko_cb(query: CallbackQuery):
    await query.message.answer("🎴 **Очко**\nВведи ставку:\nПример: `очко 1000`")
    await query.answer()

@dp.callback_query(F.data == "game:llama")
async def game_llama_cb(query: CallbackQuery):
    await query.message.answer("🦙 **Ламы**\nВведи ставку:\nПример: `ламы 1000`")
    await query.answer()

@dp.callback_query(F.data == "game:roulette")
async def game_roulette_cb(query: CallbackQuery):
    await query.message.answer("🎡 **Рулетка**\nВведи ставку и сектор:\nПример: `рул 1000 чет`")
    await query.answer()

@dp.callback_query(F.data == "game:crash")
async def game_crash_cb(query: CallbackQuery):
    await query.message.answer("📈 **Краш**\nВведи ставку и множитель:\nПример: `краш 1000 2.5`")
    await query.answer()

@dp.callback_query(F.data == "game:cube")
async def game_cube_cb(query: CallbackQuery):
    await query.message.answer("🎲 **Кубик**\nВведи ставку и число (1-6):\nПример: `кубик 1000 5`")
    await query.answer()

@dp.callback_query(F.data == "game:dice")
async def game_dice_cb(query: CallbackQuery):
    await query.message.answer("🎯 **Кости**\nВведи ставку и выбор (м/б/равно):\nПример: `кости 1000 м`")
    await query.answer()

@dp.callback_query(F.data == "game:football")
async def game_football_cb(query: CallbackQuery):
    await query.message.answer("⚽ **Футбол**\nВведи ставку и выбор (гол/мимо):\nПример: `футбол 1000 гол`")
    await query.answer()

@dp.callback_query(F.data == "game:basket")
async def game_basket_cb(query: CallbackQuery):
    await query.message.answer("🏀 **Баскет**\nВведи ставку:\nПример: `баскет 1000`")
    await query.answer()

@dp.callback_query(F.data == "game:darts")
async def game_darts_cb(query: CallbackQuery):
    await query.message.answer("🎯 **Дартс**\nВведи ставку и сектор:\nПример: `дартс 1000 красное`")
    await query.answer()

# ========== КОЛБЭКИ ИГР ==========
@dp.callback_query(F.data.startswith("tower:"))
async def tower_callback(query: CallbackQuery):
    user_id = query.from_user.id
    game = TOWER_GAMES.get(user_id)
    if not game:
        await query.answer("❌ Нет активной игры", show_alert=True)
        return
    action = query.data.split(":")[1]
    if action == "pick":
        chosen = int(query.data.split(":")[2])
        safe = random.randint(1, 3)
        if chosen != safe:
            bet = float(game["bet"])
            balance = finalize_reserved_bet(user_id, bet, 0.0, "tower", f"lose_at_{game['level']}")
            TOWER_GAMES.pop(user_id, None)
            await query.message.edit_text(f"💥 **Башня**\nЛовушка была в {safe}. Ты выбрал {chosen}.\nПотеряно: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        game["level"] += 1
        if game["level"] >= len(TOWER_MULTIPLIERS):
            bet = float(game["bet"])
            payout = round(bet * TOWER_MULTIPLIERS[-1], 2)
            balance = finalize_reserved_bet(user_id, bet, payout, "tower", "completed")
            TOWER_GAMES.pop(user_id, None)
            await query.message.edit_text(f"🏁 **Башня пройдена!**\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        await query.message.edit_text(tower_text(game), reply_markup=tower_kb())
        await query.answer("✅ Успех!")
    elif action == "cash":
        if game["level"] == 0:
            await query.answer("❌ Сначала сделай ход!", show_alert=True)
            return
        bet = float(game["bet"])
        mult = TOWER_MULTIPLIERS[game["level"] - 1]
        payout = round(bet * mult, 2)
        balance = finalize_reserved_bet(user_id, bet, payout, "tower", f"cashout_{game['level']}")
        TOWER_GAMES.pop(user_id, None)
        await query.message.edit_text(f"✅ **Ты забрал выигрыш!**\nУровень: {game['level']}\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
        await query.answer()
    elif action == "cancel":
        if game["level"] > 0:
            await query.answer("❌ Нельзя отменить после хода", show_alert=True)
            return
        bet = float(game["bet"])
        balance = finalize_reserved_bet(user_id, bet, bet, "tower", "cancelled")
        TOWER_GAMES.pop(user_id, None)
        await query.message.edit_text(f"❌ Игра отменена. Возвращено: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
        await query.answer()

@dp.callback_query(F.data.startswith("gold:"))
async def gold_callback(query: CallbackQuery):
    user_id = query.from_user.id
    game = GOLD_GAMES.get(user_id)
    if not game:
        await query.answer("❌ Нет активной игры", show_alert=True)
        return
    action = query.data.split(":")[1]
    if action == "pick":
        chosen = int(query.data.split(":")[2])
        trap = random.randint(1, 4)
        if chosen == trap:
            bet = float(game["bet"])
            balance = finalize_reserved_bet(user_id, bet, 0.0, "gold", f"lose_at_{game['step']}")
            GOLD_GAMES.pop(user_id, None)
            await query.message.edit_text(f"💥 **Золото**\nЛовушка была в {trap}. Ты выбрал {chosen}.\nПотеряно: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        game["step"] += 1
        if game["step"] >= len(GOLD_MULTIPLIERS):
            bet = float(game["bet"])
            payout = round(bet * GOLD_MULTIPLIERS[-1], 2)
            balance = finalize_reserved_bet(user_id, bet, payout, "gold", "completed")
            GOLD_GAMES.pop(user_id, None)
            await query.message.edit_text(f"🏁 **Золото пройдено!**\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        await query.message.edit_text(gold_text(game), reply_markup=gold_kb())
        await query.answer("✅ Успех!")
    elif action == "cash":
        if game["step"] == 0:
            await query.answer("❌ Сначала сделай ход!", show_alert=True)
            return
        bet = float(game["bet"])
        mult = GOLD_MULTIPLIERS[game["step"] - 1]
        payout = round(bet * mult, 2)
        balance = finalize_reserved_bet(user_id, bet, payout, "gold", f"cashout_{game['step']}")
        GOLD_GAMES.pop(user_id, None)
        await query.message.edit_text(f"✅ **Ты забрал выигрыш!**\nРаунд: {game['step']}\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
        await query.answer()
    elif action == "cancel":
        if game["step"] > 0:
            await query.answer("❌ Нельзя отменить после хода", show_alert=True)
            return
        bet = float(game["bet"])
        balance = finalize_reserved_bet(user_id, bet, bet, "gold", "cancelled")
        GOLD_GAMES.pop(user_id, None)
        await query.message.edit_text(f"❌ Игра отменена. Возвращено: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
        await query.answer()

@dp.callback_query(F.data.startswith("diamond:"))
async def diamond_callback(query: CallbackQuery):
    user_id = query.from_user.id
    game = DIAMOND_GAMES.get(user_id)
    if not game:
        await query.answer("❌ Нет активной игры", show_alert=True)
        return
    action = query.data.split(":")[1]
    if action == "pick":
        chosen = int(query.data.split(":")[2])
        trap = random.randint(1, 5)
        if chosen == trap:
            bet = float(game["bet"])
            balance = finalize_reserved_bet(user_id, bet, 0.0, "diamonds", f"lose_at_{game['step']}")
            DIAMOND_GAMES.pop(user_id, None)
            await query.message.edit_text(f"💥 **Алмазы**\nБракованный кристалл был {trap}. Ты выбрал {chosen}.\nПотеряно: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        game["step"] += 1
        if game["step"] >= len(DIAMOND_MULTIPLIERS):
            bet = float(game["bet"])
            payout = round(bet * DIAMOND_MULTIPLIERS[-1], 2)
            balance = finalize_reserved_bet(user_id, bet, payout, "diamonds", "completed")
            DIAMOND_GAMES.pop(user_id, None)
            await query.message.edit_text(f"🏁 **Алмазы пройдены!**\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        await query.message.edit_text(diamond_text(game), reply_markup=diamond_kb())
        await query.answer("✅ Успех!")
    elif action == "cash":
        if game["step"] == 0:
            await query.answer("❌ Сначала сделай ход!", show_alert=True)
            return
        bet = float(game["bet"])
        mult = DIAMOND_MULTIPLIERS[game["step"] - 1]
        payout = round(bet * mult, 2)
        balance = finalize_reserved_bet(user_id, bet, payout, "diamonds", f"cashout_{game['step']}")
        DIAMOND_GAMES.pop(user_id, None)
        await query.message.edit_text(f"✅ **Ты забрал выигрыш!**\nШаг: {game['step']}\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
        await query.answer()
    elif action == "cancel":
        if game["step"] > 0:
            await query.answer("❌ Нельзя отменить после хода", show_alert=True)
            return
        bet = float(game["bet"])
        balance = finalize_reserved_bet(user_id, bet, bet, "diamonds", "cancelled")
        DIAMOND_GAMES.pop(user_id, None)
        await query.message.edit_text(f"❌ Игра отменена. Возвращено: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
        await query.answer()

@dp.callback_query(F.data == "mines:noop")
async def mines_noop(query: CallbackQuery):
    await query.answer()

@dp.callback_query(F.data.startswith("mines:"))
async def mines_callback(query: CallbackQuery):
    user_id = query.from_user.id
    game = MINES_GAMES.get(user_id)
    if not game:
        await query.answer("❌ Нет активной игры", show_alert=True)
        return
    action = query.data.split(":")[1]
    if action == "cell":
        idx = int(query.data.split(":")[2])
        if idx in game["opened"]:
            await query.answer("Уже открыто", show_alert=True)
            return
        if idx in game["mines"]:
            bet = float(game["bet"])
            balance = finalize_reserved_bet(user_id, bet, 0.0, "mines", "explode")
            MINES_GAMES.pop(user_id, None)
            await query.message.edit_text(f"💥 **Мины**\nТы попал на мину в клетке {idx}!\nПотеряно: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        game["opened"].add(idx)
        safe_opened = len(game["opened"])
        safe_total = 9 - game["mines_count"]
        if safe_opened >= safe_total:
            bet = float(game["bet"])
            mult = mines_multiplier(safe_opened, game["mines_count"])
            payout = round(bet * mult, 2)
            balance = finalize_reserved_bet(user_id, bet, payout, "mines", "cleared")
            MINES_GAMES.pop(user_id, None)
            await query.message.edit_text(f"🏁 **Мины**\nВсе безопасные клетки открыты!\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        await query.message.edit_text(mines_text(game), reply_markup=mines_kb(game))
        await query.answer("✅ Безопасно")
    elif action == "cash":
        if len(game["opened"]) == 0:
            await query.answer("❌ Сначала открой клетку!", show_alert=True)
            return
        bet = float(game["bet"])
        mult = mines_multiplier(len(game["opened"]), game["mines_count"])
        payout = round(bet * mult, 2)
        balance = finalize_reserved_bet(user_id, bet, payout, "mines", f"cashout_{len(game['opened'])}")
        MINES_GAMES.pop(user_id, None)
        await query.message.edit_text(f"✅ **Ты забрал выигрыш!**\nОткрыто: {len(game['opened'])}\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
        await query.answer()
    elif action == "cancel":
        if len(game["opened"]) > 0:
            await query.answer("❌ Нельзя отменить после хода", show_alert=True)
            return
        bet = float(game["bet"])
        balance = finalize_reserved_bet(user_id, bet, bet, "mines", "cancelled")
        MINES_GAMES.pop(user_id, None)
        await query.message.edit_text(f"❌ Игра отменена. Возвращено: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
        await query.answer()

@dp.callback_query(F.data.startswith("llama:"))
async def llama_callback(query: CallbackQuery):
    user_id = query.from_user.id
    game = LLAMA_GAMES.get(user_id)
    if not game:
        await query.answer("❌ Нет активной игры", show_alert=True)
        return
    action = query.data.split(":")[1]
    if action == "pick":
        chosen = int(query.data.split(":")[2])
        safe = random.randint(1, 4)
        if chosen != safe:
            bet = float(game["bet"])
            balance = finalize_reserved_bet(user_id, bet, 0.0, "llama", f"lose_at_{game['level']}")
            LLAMA_GAMES.pop(user_id, None)
            await query.message.edit_text(f"💥 **Ламы**\nЗлая лама была в {safe}. Ты выбрал {chosen}.\nПотеряно: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        game["level"] += 1
        if game["level"] >= len(LLAMA_MULTIPLIERS):
            bet = float(game["bet"])
            payout = round(bet * LLAMA_MULTIPLIERS[-1], 2)
            balance = finalize_reserved_bet(user_id, bet, payout, "llama", "completed")
            LLAMA_GAMES.pop(user_id, None)
            await query.message.edit_text(f"🏁 **Ламы пройдены!**\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
            await query.answer()
            return
        await query.message.edit_text(llama_text(game), reply_markup=llama_kb())
        await query.answer("✅ Лама довольна!")
    elif action == "cash":
        if game["level"] == 0:
            await query.answer("❌ Сначала сделай ход!", show_alert=True)
            return
        bet = float(game["bet"])
        mult = LLAMA_MULTIPLIERS[game["level"] - 1]
        payout = round(bet * mult, 2)
        balance = finalize_reserved_bet(user_id, bet, payout, "llama", f"cashout_{game['level']}")
        LLAMA_GAMES.pop(user_id, None)
        await query.message.edit_text(f"✅ **Ты забрал выигрыш!**\nУровень: {game['level']}\nВыигрыш: {fmt_money(payout)}\nБаланс: {fmt_money(balance)}")
        await query.answer()
    elif action == "cancel":
        if game["level"] > 0:
            await query.answer("❌ Нельзя отменить после хода", show_alert=True)
            return
        bet = float(game["bet"])
        balance = finalize_reserved_bet(user_id, bet, bet, "llama", "cancelled")
        LLAMA_GAMES.pop(user_id, None)
        await query.message.edit_text(f"❌ Игра отменена. Возвращено: {fmt_money(bet)}\nБаланс: {fmt_money(balance)}")
        await query.answer()

@dp.callback_query(F.data.startswith("roulette:"))
async def roulette_callback(query: CallbackQuery, state: FSMContext):
    action = query.data.split(":")[1]
    if action == "choice":
        choice = query.data.split(":")[2]
        data = await state.get_data()
        bet = float(data.get("bet", 0))
        if bet <= 0:
            await query.answer("❌ Ошибка, начни заново", show_alert=True)
            await state.clear()
            return
        await state.clear()
        win, payout, result_text, number = await play_roulette_with_sticker(query.message, bet, choice)
        ok, balance = settle_instant_bet(query.from_user.id, bet, payout, f"roulette:{choice}", f"number={number}")
        if not ok:
            await query.message.answer("❌ Недостаточно средств.")
            await query.answer()
            return
        await query.message.answer(
            f"{result_text}\n\n"
            f"Результат: **{'Победа' if win else 'Поражение'}**\n"
            f"Выплата: **{fmt_money(payout)}**\n"
            f"Баланс: **{fmt_money(balance)}**"
        )
        await query.answer()
    elif action == "cancel":
        await state.clear()
        await query.message.edit_text("❌ Рулетка отменена.", reply_markup=games_kb())
        await query.answer()

@dp.callback_query(F.data.startswith("ochko:"))
async def ochko_callback(query: CallbackQuery, state: FSMContext):
    action = query.data.split(":")[1]
    if action == "start":
        data = await state.get_data()
        bet = float(data.get("bet", 0))
        if bet <= 0:
            await query.answer("❌ Ошибка", show_alert=True)
            await state.clear()
            return
        await state.clear()
        ok, _ = reserve_bet(query.from_user.id, bet)
        if not ok:
            await query.message.answer("❌ Недостаточно средств.")
            await query.answer()
            return
        deck = make_deck()
        player = [deck.pop(), deck.pop()]
        dealer = [deck.pop(), deck.pop()]
        OCHKO_GAMES[query.from_user.id] = {"bet": bet, "deck": deck, "player": player, "dealer": dealer}
        game = OCHKO_GAMES[query.from_user.id]
        player_value = hand_value(player)
        dealer_value = hand_value(dealer)
        if player_value == 21:
            if dealer_value == 21:
                payout = bet
                outcome = "push"
                text = "Ничья!"
            else:
                payout = round(bet * 2.5, 2)
                outcome = "win"
                text = "BLACKJACK!"
            balance = finalize_reserved_bet(query.from_user.id, bet, payout, "ochko", outcome)
            OCHKO_GAMES.pop(query.from_user.id, None)
            await query.message.edit_text(
                f"{render_ochko_table(game, reveal_dealer=True)}\n\n{text}\n"
                f"Выплата: **{fmt_money(payout)}**\n"
                f"Баланс: **{fmt_money(balance)}**"
            )
            await query.answer()
            return
        await query.message.edit_text(render_ochko_table(game, reveal_dealer=False), reply_markup=ochko_kb())
        await query.answer()
    elif action == "cancel":
        await state.clear()
        await query.message.edit_text("❌ Очко отменено.", reply_markup=games_kb())
        await query.answer()

@dp.callback_query(F.data == "ochko:hit")
async def ochko_hit_callback(query: CallbackQuery):
    user_id = query.from_user.id
    game = OCHKO_GAMES.get(user_id)
    if not game:
        await query.answer("❌ Нет активной игры", show_alert=True)
        return
    deck = game["deck"]
    if not deck:
        deck = make_deck()
        game["deck"] = deck
    game["player"].append(deck.pop())
    value = hand_value(game["player"])
    if value > 21:
        bet = float(game["bet"])
        balance = finalize_reserved_bet(user_id, bet, 0.0, "ochko", "bust")
        OCHKO_GAMES.pop(user_id, None)
        await query.message.edit_text(
            f"{render_ochko_table(game, reveal_dealer=True)}\n\n"
            f"**Перебор! Ты проиграл.**\n"
            f"Баланс: **{fmt_money(balance)}**"
        )
        await query.answer()
        return
    await query.message.edit_text(render_ochko_table(game, reveal_dealer=False), reply_markup=ochko_kb())
    await query.answer()

@dp.callback_query(F.data == "ochko:stand")
async def ochko_stand_callback(query: CallbackQuery):
    user_id = query.from_user.id
    game = OCHKO_GAMES.get(user_id)
    if not game:
        await query.answer("❌ Нет активной игры", show_alert=True)
        return
    deck = game["deck"]
    while hand_value(game["dealer"]) < 17:
        if not deck:
            deck = make_deck()
            game["deck"] = deck
        game["dealer"].append(deck.pop())
    player_value = hand_value(game["player"])
    dealer_value = hand_value(game["dealer"])
    bet = float(game["bet"])
    if dealer_value > 21 or player_value > dealer_value:
        payout = round(bet * 2.0, 2)
        result_text = "Победа!"
        outcome = "win"
    elif dealer_value == player_value:
        payout = bet
        result_text = "Ничья"
        outcome = "push"
    else:
        payout = 0.0
        result_text = "Поражение"
        outcome = "lose"
    balance = finalize_reserved_bet(user_id, bet, payout, "ochko", outcome)
    OCHKO_GAMES.pop(user_id, None)
    await query.message.edit_text(
        f"{render_ochko_table(game, reveal_dealer=True)}\n\n"
        f"Результат: **{result_text}**\n"
        f"Выплата: **{fmt_money(payout)}**\n"
        f"Баланс: **{fmt_money(balance)}**"
    )
    await query.answer()

@dp.callback_query(F.data == "bank:term:cancel")
async def bank_term_cancel_cb(query: CallbackQuery, state: FSMContext):
    await state.clear()
    await query.message.edit_text("❌ Депозит отменён.", reply_markup=bank_kb())
    await query.answer()

@dp.callback_query(F.data.startswith("bank:term:"))
async def bank_term_cb(query: CallbackQuery, state: FSMContext):
    raw = query.data.split(":")[-1]
    if raw == "cancel":
        return
    data = await state.get_data()
    amount = float(data.get("amount", 0))
    if amount <= 0:
        await query.answer("❌ Ошибка, начни заново", show_alert=True)
        return
    term_days = int(raw)
    ok, msg = add_deposit(query.from_user.id, amount, term_days)
    await state.clear()
    if not ok:
        await query.message.answer(f"❌ {msg}")
    else:
        rate = BANK_TERMS[term_days]
        await query.message.answer(
            f"✅ **Депозит открыт!**\n\n"
            f"Сумма: {fmt_money(amount)}\n"
            f"Срок: {term_days} дней\n"
            f"Доход: +{int(rate*100)}%",
            reply_markup=bank_kb()
        )
    await query.answer()

@dp.callback_query(F.data == "bank:list")
async def bank_list_cb(query: CallbackQuery):
    rows = list_user_deposits(query.from_user.id)
    if not rows:
        await query.message.answer("📜 У тебя нет депозитов.")
        await query.answer()
        return
    text = "📜 **Твои депозиты**\n\n"
    now = now_ts()
    for row in rows[:10]:
        opened_at = row["opened_at"]
        term_days = row["term_days"]
        unlock_ts = opened_at + term_days * 86400
        left = unlock_ts - now
        if row["status"] == "active":
            status = "⏳ активен" if left > 0 else "✅ готов к выводу"
        else:
            status = "✅ закрыт"
        text += f"#{row['id']} | {fmt_money(row['principal'])} | {term_days}д | +{int(row['rate']*100)}% | {status}\n"
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:bank")]
    ]))
    await query.answer()

@dp.callback_query(F.data == "bank:withdraw")
async def bank_withdraw_cb(query: CallbackQuery):
    closed, payout = withdraw_matured_deposits(query.from_user.id)
    if closed == 0:
        await query.message.answer("❌ Нет депозитов, готовых к выводу.")
    else:
        await query.message.answer(f"✅ Выведено {closed} депозитов на сумму {fmt_money(payout)}")
    await query.answer()

@dp.callback_query(F.data == "checks:create")
async def checks_create_cb(query: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(CheckCreateStates.waiting_amount)
    await query.message.edit_text(
        "🧾 **Создание чека**\n\n"
        "Введи сумму на 1 активацию (мин. 10):\n(Комиссия 6%)",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:checks")]
        ])
    )
    await query.answer()

@dp.message(CheckCreateStates.waiting_amount)
async def checks_create_amount(message: Message, state: FSMContext):
    try:
        amount = parse_amount(message.text)
        if amount == -1:
            await message.answer("❌ Нельзя использовать 'все' для создания чека.")
            return
    except:
        await message.answer("❌ Введи корректную сумму.")
        return
    if amount < 10:
        await message.answer("❌ Минимальная сумма на чек: 10")
        return
    await state.update_data(amount=amount)
    await state.set_state(CheckCreateStates.waiting_count)
    await message.answer("Сколько активаций? (1-100)")

@dp.message(CheckCreateStates.waiting_count)
async def checks_create_count(message: Message, state: FSMContext):
    try:
        count = int(message.text)
    except:
        await message.answer("❌ Введи целое число.")
        return
    if not 1 <= count <= 100:
        await message.answer("❌ Количество от 1 до 100.")
        return
    data = await state.get_data()
    amount = float(data.get("amount", 0))
    ok, result = create_check_atomic(message.from_user.id, amount, count)
    await state.clear()
    if not ok:
        await message.answer(f"❌ {result}")
        return
    await message.answer(
        f"✅ **Чек создан!**\n\n"
        f"Код: `{result}`\n"
        f"Сумма: {fmt_money(amount)}\n"
        f"Активаций: {count}\n"
        f"Комиссия: {fmt_money(amount * count * CHECK_FEE_PERCENT)}",
        reply_markup=main_menu_kb()
    )

@dp.callback_query(F.data == "checks:claim")
async def checks_claim_cb(query: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(CheckClaimStates.waiting_code)
    await query.message.edit_text(
        "💸 **Активация чека**\n\nВведи код чека:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:checks")]
        ])
    )
    await query.answer()

@dp.message(CheckClaimStates.waiting_code)
async def checks_claim_code(message: Message, state: FSMContext):
    code = message.text.strip().upper()
    ok, msg, reward = claim_check_atomic(message.from_user.id, code)
    await state.clear()
    if not ok:
        await message.answer(f"❌ {msg}")
        return
    await message.answer(f"✅ {msg}\n+{fmt_money(reward)}", reply_markup=main_menu_kb())

@dp.callback_query(F.data == "checks:my")
async def checks_my_cb(query: CallbackQuery):
    rows = list_my_checks(query.from_user.id)
    if not rows:
        await query.message.answer("📄 У тебя нет созданных чеков.")
        await query.answer()
        return
    text = "📄 **Твои чеки**\n\n"
    for row in rows:
        text += f"🔹 `{row['code']}` | {fmt_money(row['per_user'])} | осталось: {row['remaining']}\n"
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:checks")]
    ]))
    await query.answer()

@dp.callback_query(F.data == "help:rules")
async def help_rules_cb(query: CallbackQuery):
    await query.message.answer(
        "📜 **Правила WILLD GRAMM**\n\n"
        f"1. Минимальная ставка: **{MIN_BET}**\n"
        f"2. Максимальная ставка: **{fmt_money(MAX_BET)}**\n"
        f"3. Вывод средств: комиссия **{WITHDRAW_FEE*100}%**\n"
        f"4. Создание чека: комиссия **{CHECK_FEE_PERCENT*100}%**\n"
        f"5. Ежедневный бонус: **{fmt_money(DAILY_BONUS)}**\n"
        f"6. Запрещены мультиаккаунты\n"
        f"7. Запрещены споры с администрацией\n\n"
        f"Нарушение правил = блокировка аккаунта.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:help")]
        ])
    )
    await query.answer()

@dp.callback_query(F.data == "help:howto")
async def help_howto_cb(query: CallbackQuery):
    await query.message.answer(
        "🎮 **Как играть в WILLD GRAMM**\n\n"
        "**Команды:**\n"
        "• `башня 1000 2` — ставка 1000, 2 мины\n"
        "• `золото 1000` — ставка 1000\n"
        "• `алмазы 1000` — ставка 1000\n"
        "• `мины 1000 3` — ставка 1000, 3 мины\n"
        "• `рул 1000 чет` — ставка 1000, на чёт\n"
        "• `краш 1000 2.5` — ставка 1000, множитель 2.5\n"
        "• `кубик 1000 5` — ставка 1000, угадать 5\n"
        "• `кости 1000 м` — ставка 1000, меньше 7\n"
        "• `очко 1000` — ставка 1000\n"
        "• `футбол 1000 гол` — ставка 1000, на гол\n"
        "• `баскет 1000` — ставка 1000\n"
        "• `ламы 1000` — ставка 1000\n"
        "• `дартс 1000 красное` — ставка 1000, на красное\n\n"
        "💡 **Вместо суммы можно написать `все`** — поставишь весь баланс!",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:help")]
        ])
    )
    await query.answer()

# ========== ЗАПУСК ==========
async def main():
    print("✅ Бот WILLD GRAMM запущен!")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
