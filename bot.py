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

# ========== ВЫВОД СРЕДСТВ (ИСПРАВЛЕННЫЙ) ==========
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

# ========== КЛАВИАТУРЫ ==========
def main_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Профиль", callback_data="menu:profile")],
        [InlineKeyboardButton(text="🎮 Игры", callback_data="menu:games")],
        [InlineKeyboardButton(text="🏦 Банк", callback_data="menu:bank")],
        [InlineKeyboardButton(text="⭐ Пополнить", callback_data="menu:deposit"), InlineKeyboardButton(text="💸 Вывести", callback_data="menu:withdraw")],
        [InlineKeyboardButton(text="🎁 Бонус", callback_data="menu:bonus"), InlineKeyboardButton(text="🏆 Топ", callback_data="menu:top")],
        [InlineKeyboardButton(text="🧾 Чеки", callback_data="menu:checks"), InlineKeyboardButton(text="🎟 Промо", callback_data="menu:promo")],
        [InlineKeyboardButton(text="❓ Помощь", callback_data="menu:help"), InlineKeyboardButton(text="🆘 Поддержка", callback_data="menu:support")],
    ])

def profile_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Пополнить", callback_data="menu:deposit"), InlineKeyboardButton(text="💸 Вывести", callback_data="menu:withdraw")],
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
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:profile")],
    ])

def deposit_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Через Stars", callback_data="menu:donate")],
        [InlineKeyboardButton(text="💳 Ручное пополнение", callback_data="deposit:manual")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:profile")],
    ])

def withdraw_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Вывести все", callback_data="withdraw:all")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:profile")],
    ])

def checks_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать чек", callback_data="checks:create")],
        [InlineKeyboardButton(text="💸 Активировать чек", callback_data="checks:claim")],
        [InlineKeyboardButton(text="📄 Мои чеки", callback_data="checks:my")],
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

def roulette_choice_kb(bet):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔴 Красное (x2)", callback_data="roulette:choice:red"), InlineKeyboardButton(text="⚫ Черное (x2)", callback_data="roulette:choice:black")],
        [InlineKeyboardButton(text="2️⃣ Чет (x2)", callback_data="roulette:choice:even"), InlineKeyboardButton(text="1️⃣ Нечет (x2)", callback_data="roulette:choice:odd")],
        [InlineKeyboardButton(text="0️⃣ Зеро (x36)", callback_data="roulette:choice:zero")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="roulette:cancel")],
    ])

def darts_choice_kb(bet):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎯 Красное (x2)", callback_data="darts:choice:red"), InlineKeyboardButton(text="🎯 Черное (x2)", callback_data="darts:choice:black")],
        [InlineKeyboardButton(text="🎯 Чет (x2)", callback_data="darts:choice:even"), InlineKeyboardButton(text="🎯 Нечет (x2)", callback_data="darts:choice:odd")],
        [InlineKeyboardButton(text="🎯 Зеро (x36)", callback_data="darts:choice:zero")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="darts:cancel")],
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

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Выдать монеты", callback_data="admin:give")],
        [InlineKeyboardButton(text="👑 Выдать статус", callback_data="admin:setstatus")],
        [InlineKeyboardButton(text="🎟 Создать промо", callback_data="admin:createpromo")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="📋 Все заявки на вывод", callback_data="admin:withdraws")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])

def admin_status_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Игрок (0)", callback_data="admin:status:0")],
        [InlineKeyboardButton(text="🛡️ Помощник (1)", callback_data="admin:status:1")],
        [InlineKeyboardButton(text="👑 Создатель (2)", callback_data="admin:status:2")],
        [InlineKeyboardButton(text="⭐ VIP (3)", callback_data="admin:status:3")],
        [InlineKeyboardButton(text="🎲 Хайроллер (4)", callback_data="admin:status:4")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")],
    ])

def help_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📜 Правила", callback_data="help:rules")],
        [InlineKeyboardButton(text="🎮 Как играть", callback_data="help:howto")],
        [InlineKeyboardButton(text="📢 Наш канал", url=CHANNEL_LINK)],
        [InlineKeyboardButton(text="🆘 Поддержка", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")],
    ])

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
    await message.answer(f"🦙 **Добро пожаловать в WILLD GRAMM!**\n\nТвой баланс: **{fmt_money(get_user(user.id)['coins'])}**\nТвой статус: {get_user_status_text(get_user(user.id)['status'])}\n\nИспользуй кнопки ниже:", reply_markup=main_menu_kb())

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
    await message.answer(f"👤 **Твой профиль**\n\nID: `{message.from_user.id}`\nСтатус: {get_user_status_text(stats['status'])}\n\n💰 Баланс: **{fmt_money(stats['coins'])}**\n⭐ Stars: **{stats['stars']}**\n\n🎮 Сыграно: **{stats['total_games']}**\n🏆 Побед: **{stats['total_wins']}**\n🏦 Депозитов: **{stats['active_deposits']}**", reply_markup=profile_kb())

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
    text = "🏆 **ТОП ИГРОКОВ**\n\n💰 **По балансу:**\n"
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
    await message.answer(f"❓ **Помощь WILLD GRAMM**\n\n**Правила:**\n• Ставки: от {MIN_BET} до {MAX_BET}\n• Вывод: комиссия 5%\n• Чек: комиссия 6%\n\n**Игры:**\n`башня 1000 2`\n`золото 1000`\n`алмазы 1000`\n`мины 1000 3`\n`рул 1000 чет`\n`краш 1000 2.5`\n`кубик 1000 5`\n`кости 1000 м`\n`очко 1000`\n`футбол 1000 гол`\n`баскет 1000`\n`ламы 1000`\n`дартс 1000 красное`\n\n💡 Вместо суммы можно писать `все`\n\n📢 {CHANNEL_LINK}\n🆘 {SUPPORT_USERNAME} / {SUPPORT_USERNAME2}", reply_markup=help_kb())

# ========== КНОПКИ МЕНЮ ==========
@dp.callback_query(F.data == "menu:main")
async def menu_main_cb(query: CallbackQuery, state: FSMContext):
    await state.clear()
    await query.message.edit_text("📱 **Главное меню**", reply_markup=main_menu_kb())
    await query.answer()

@dp.callback_query(F.data == "menu:profile")
async def menu_profile_cb(query: CallbackQuery):
    stats = get_profile_stats(query.from_user.id)
    await query.message.edit_text(f"👤 **Твой профиль**\n\nID: `{query.from_user.id}`\nСтатус: {get_user_status_text(stats['status'])}\n\n💰 Баланс: **{fmt_money(stats['coins'])}**\n⭐ Stars: **{stats['stars']}**\n\n🎮 Сыграно: **{stats['total_games']}**\n🏆 Побед: **{stats['total_wins']}**\n🏦 Депозитов: **{stats['active_deposits']}**", reply_markup=profile_kb())
    await query.answer()

@dp.callback_query(F.data == "menu:games")
async def menu_games_cb(query: CallbackQuery):
    await query.message.edit_text("🎮 **Выбери игру**", reply_markup=games_kb())
    await query.answer()

@dp.callback_query(F.data == "menu:bank")
async def menu_bank_cb(query: CallbackQuery):
    summary = get_bank_summary(query.from_user.id)
    await query.message.edit_text(f"🏦 **Банк**\n\n💰 Баланс: **{fmt_money(summary['coins'])}**\n📊 Активных депозитов: **{summary['count_active']}**\n💵 Сумма в депозитах: **{fmt_money(summary['active_sum'])}**\n\n**Доходность:**\n• 7 дней: +3%\n• 14 дней: +7%\n• 30 дней: +18%", reply_markup=bank_kb())
    await query.answer()

@dp.callback_query(F.data == "menu:deposit")
async def menu_deposit_cb(query: CallbackQuery):
    await query.message.edit_text(f"⭐ **Пополнение баланса**\n\n**Способ 1: Telegram Stars**\n1 Star = {STARS_RATE} {CURRENCY_NAME}\nМин: {MIN_STARS} Stars, Макс: {MAX_STARS} Stars\n\n**Способ 2: Ручное пополнение**\nПерейдите в чат: {MANUAL_DEPOSIT_CHAT}\nСледуйте инструкции в закреплённом сообщении", reply_markup=deposit_kb())
    await query.answer()

@dp.callback_query(F.data == "menu:donate")
async def menu_donate_cb(query: CallbackQuery):
    await query.message.edit_text(f"⭐ **Пополнение через Stars**\n\nКурс: 1 Star = {STARS_RATE} {CURRENCY_NAME}\nМин: {MIN_STARS} Stars\nМакс: {MAX_STARS} Stars\n\nВыбери сумму:", reply_markup=donate_kb())
    await query.answer()

@dp.callback_query(F.data == "deposit:manual")
async def deposit_manual_cb(query: CallbackQuery):
    await query.message.answer(f"💳 **Ручное пополнение**\n\n1. Перейдите в чат: {MANUAL_DEPOSIT_CHAT}\n2. Прочитайте закреплённое сообщение\n3. Следуйте инструкции\n\nПосле перевода средства поступят в течение пары минут.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📱 Перейти в чат", url=MANUAL_DEPOSIT_CHAT)], [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:deposit")]]))
    await query.answer()

@dp.callback_query(F.data == "menu:withdraw")
async def menu_withdraw_cb(query: CallbackQuery, state: FSMContext):
    await state.set_state(WithdrawStates.waiting_amount)
    await query.message.edit_text("💸 **Вывод средств**\n\nКомиссия: 5%\nМинимальная сумма: 100 WILLD GRAMM\n\nВведи сумму для вывода (или напиши `все`):", reply_markup=withdraw_kb())
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
    text = "🏆 **ТОП ИГРОКОВ**\n\n💰 **По балансу:**\n"
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
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")]]))
    await query.answer()

@dp.callback_query(F.data == "menu:checks")
async def menu_checks_cb(query: CallbackQuery):
    await query.message.edit_text("🧾 **Чеки**\n\nСоздание чека: комиссия 6%\nАктивация чека: бесплатно\n\nВыбери действие:", reply_markup=checks_kb())
    await query.answer()

@dp.callback_query(F.data == "menu:promo")
async def menu_promo_cb(query: CallbackQuery, state: FSMContext):
    await state.set_state(PromoStates.waiting_code)
    await query.message.edit_text("🎟 **Активация промокода**\n\nВведи код промокода:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")]]))
    await query.answer()

@dp.callback_query(F.data == "menu:help")
async def menu_help_cb(query: CallbackQuery):
    await query.message.edit_text(f"❓ **Помощь**\n\n**Правила:**\n• Ставки: от {MIN_BET} до {MAX_BET}\n• Вывод: комиссия 5%\n• Чек: комиссия 6%\n\n**Как играть:**\n• Введи название игры и ставку\n• Пример: `башня 500 2`\n• Вместо суммы можно написать `все`\n\n**Канал:** {CHANNEL_LINK}\n**Поддержка:** {SUPPORT_USERNAME}", reply_markup=help_kb())
    await query.answer()

@dp.callback_query(F.data == "menu:support")
async def menu_support_cb(query: CallbackQuery):
    await query.message.answer(f"🆘 **Поддержка WILLD GRAMM**\n\nПо всем вопросам:\n{SUPPORT_USERNAME}\n{SUPPORT_USERNAME2}\n\n📢 Наш канал: {CHANNEL_LINK}", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📢 Канал", url=CHANNEL_LINK)], [InlineKeyboardButton(text="💬 Написать поддержке", url=f"https://t.me/{SUPPORT_USERNAME[1:]}")], [InlineKeyboardButton(text="◀️ Назад", callback_data="menu:main")]]))
    await query.answer()

# ========== ЧЕКИ ==========
@dp.callback_query(F.data == "checks:create")
async def checks_create_cb(query: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(CheckCreateStates.waiting_amount)
    await query.message.edit_text("🧾 **Создание чека**\n\nВведи сумму на 1 активацию (комиссия 6%):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu:checks")]]))
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
    await message.answer(f"✅ **Чек создан!**\n\nКод: `{result}`\nСумма: {fmt_money(amount)}\nАктиваций: {count}\nКомиссия: {fmt_money(amount * count * CHECK_FEE_PERCENT)}", reply_markup=main_menu_kb())

@dp.callback_query(F.data == "checks:claim")
async def checks_claim_cb(query: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(CheckClaimStates.waiting_code)
    await query.message.edit_text("💸 **Активация чека**\n\nВведи код чека:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu:checks")]]))
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
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu:checks")]]))
    await query.answer()

# ========== ПРОМОКОДЫ ==========
@dp.message(PromoStates.waiting_code)
async def promo_code_input(message: Message, state: FSMContext):
    code = message.text.strip()
    ok, msg, reward = redeem_promo_atomic(message.from_user.id, code)
    await state.clear()
    if not ok:
        await message.answer(f"❌ {msg}")
        return
    await message.answer(f"✅ {msg}\n+{fmt_money(reward)}", reply_markup=main_menu_kb())

# ========== БАНК ==========
@dp.callback_query(F.data == "bank:open")
async def bank_open_cb(query: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(BankStates.waiting_amount)
    await query.message.edit_text("🏦 **Открытие депозита**\n\nВведи сумму депозита (мин. 100):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu:bank")]]))
    await query.answer()

@dp.message(BankStates.waiting_amount)
async def bank_amount(message: Message, state: FSMContext):
    try:
        amount = parse_amount(message.text)
        if amount == -1:
            user = get_user(message.from_user.id)
            amount = float(user["coins"] or 0)
    except:
        await message.answer("❌ Введи корректную сумму.")
        return
    if amount < 100:
        await message.answer("❌ Минимальный депозит: 100")
        return
    await state.update_data(amount=amount)
    await message.answer("📅 Выбери срок депозита:", reply_markup=bank_terms_kb())

@dp.callback_query(F.data.startswith("bank:term:"))
async def bank_term_cb(query: CallbackQuery, state: FSMContext):
    raw = query.data.split(":")[-1]
    if raw == "cancel":
        await state.clear()
        await query.message.edit_text("❌ Депозит отменён.", reply_markup=bank_kb())
        await query.answer()
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
        await query.message.answer(f"✅ **Депозит открыт!**\n\nСумма: {fmt_money(amount)}\nСрок: {term_days} дней\nДоход: +{int(rate*100)}%", reply_markup=bank_kb())
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
    for row in rows:
        opened_at = row["opened_at"]
        term_days = row["term_days"]
        unlock_ts = opened_at + term_days * 86400
        left = unlock_ts - now
        if row["status"] == "active":
            status = "⏳ активен" if left > 0 else "✅ готов к выводу"
        else:
            status = "✅ закрыт"
        text += f"#{row['id']} | {fmt_money(row['principal'])} | {term_days}д | +{int(row['rate']*100)}% | {status}\n"
    await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu:bank")]]))
    await query.answer()

@dp.callback_query(F.data == "bank:withdraw")
async def bank_withdraw_cb(query: CallbackQuery):
    closed, payout = withdraw_matured_deposits(query.from_user.id)
    if closed == 0:
        await query.message.answer("❌ Нет депозитов, готовых к выводу.")
    else:
        await query.message.answer(f"✅ Выведено {closed} депозитов на сумму {fmt_money(payout)}")
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
    await state.set_state(NewPromoStates.waiting_code)
    await query.message.edit_text("💰 **Выдать монеты**\n\nВведи ID пользователя и сумму через пробел:\nПример: `8478884644 1000`", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]]))
    await query.answer()

@dp.callback_query(F.data == "admin:setstatus")
async def admin_setstatus_cb(query: CallbackQuery, state: FSMContext):
    if not is_admin_user(query.from_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return
    await state.set_state(SetStatusStates.waiting_user)
    await query.message.edit_text("👑 **Выдать статус**\n\nВведи ID пользователя или @username:\nПример: `8478884644` или `@username`", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]]))
    await query.answer()

@dp.callback_query(F.data == "admin:createpromo")
async def admin_createpromo_cb(query: CallbackQuery, state: FSMContext):
    if not is_admin_user(query.from_user.id):
        await query.answer("⛔ Доступ запрещён", show_alert=True)
        return
    await state.set_state(NewPromoStates.waiting_code)
    await query.message.edit_text("🎟 **Создание промокода**\n\nВведи код промокода (3-24 символа, A-Z, 0-9, _-):", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]]))
    await query.answer()

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
    await query.message.edit_text(f"📊 **Статистика бота**\n\n👥 Пользователей: **{total_users}**\n💰 Всего монет: **{fmt_money(total_coins)}**\n🎮 Всего ставок: **{total_bets}**\n🎟 Промокодов: **{total_promo}**\n🧾 Чеков: **{total_checks}**\n🏦 Депозитов: **{total_deposits}**", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]]))
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
        await query.message.edit_text("📋 **Заявки на вывод**\n\nНет активных заявок.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]]))
    else:
        text = "📋 **Заявки на вывод**\n\n"
        for user_id, username, w in pending[:10]:
            text += f"👤 {mention_user(int(user_id), username)}\n💰 Сумма: {fmt_money(w['amount'])}\n💸 К выдаче: {fmt_money(w['amount_net'])}\n📅 {fmt_dt(w['ts'])}\n➡️ /approve_withdraw {user_id}\n\n"
        await query.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="admin:menu")]]))
    await query.answer()

# ========== ДОНАТ ==========
@dp.callback_query(F.data.startswith("donate:"))
async def donate_callback(query: CallbackQuery):
    stars = int(query.data.split(":")[1])
    if stars < MIN_STARS or stars > MAX_STARS:
        await query.answer(f"❌ Минимум {MIN_STARS} Stars, максимум {MAX_STARS}", show_alert=True)
        return
    prices = [LabeledPrice(label=f"{stars} Telegram Stars", amount=stars)]
    await query.bot.send_invoice(chat_id=query.from_user.id, title="Пополнение WILLD GRAMM", description=f"Получи {fmt_money(stars * STARS_RATE)} за {stars} Stars", payload=f"donate_{stars}_{query.from_user.id}", provider_token="", currency="XTR", prices=prices, start_parameter="donate")
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
        await message.answer(f"✅ **Пополнение успешно!**\n\n⭐ {stars} Stars → {fmt_money(stars * STARS_RATE)}\n\nБаланс обновлён. Приятной игры!", reply_markup=main_menu_kb())
    except:
        await message.answer("❌ Ошибка при обработке платежа.")

# ========== ИГРЫ ==========
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
    await query.message.answer("🎡 **Рулетка**\nВведи ставку:\nПример: `рул 1000 чет`")
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

# ========== ЗАПУСК ==========
async def main():
    print("✅ Бот WILLD GRAMM запущен!")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
