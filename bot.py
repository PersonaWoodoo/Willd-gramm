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

# ========== КОНФИГ ==========
BOT_TOKEN = "8200340859:AAFziC0Vk2KH71AwnCPvQBkyCfBl50eVMrs"
ADMIN_IDS = [8478884644]
SUPPORT_CHAT = "https://t.me/+5Xm6srsM9GI2NDky"
CHANNEL_LINK = "https://t.me/+CJ7BZGR0FAY4YTky"

CURRENCY = "WG"
START_BALANCE = 100.0
MIN_BET = 1.0
MAX_BET = 10000.0
DAILY_BONUS = 250.0
CHECK_FEE = 0.06
STARS_RATE = 2200
MIN_STARS = 1
MAX_STARS = 1500
WITHDRAW_FEE = 0.05

BANK_TERMS = {7: 0.03, 14: 0.07, 30: 0.18}

RED_NUMBERS = {1,3,5,7,9,12,14,16,18,19,21,23,25,27,30,32,34,36}

TOWER_MULT = [1.20,1.48,1.86,2.35,2.95,3.75,4.85,6.15]
GOLD_MULT = [1.15,1.35,1.62,2.0,2.55,3.25,4.2]
DIAMOND_MULT = [1.12,1.28,1.48,1.72,2.02,2.4,2.92,3.6]
LLAMA_MULT = [1.25,1.56,1.95,2.44,3.05,3.81,4.77,5.96,7.45,9.31]

STATUSES = {0:"👤 Игрок",1:"🛡️ Помощник",2:"👑 Создатель",3:"⭐ VIP",4:"🎲 Хайроллер"}

# ========== БД ==========
DB_PATH = "data.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY, username TEXT, coins REAL DEFAULT 100, stars INT DEFAULT 0, games INT DEFAULT 0, wins INT DEFAULT 0, status INT DEFAULT 0, checks TEXT, withdraws TEXT, deposit_stars INT DEFAULT 0)")
    c.execute("CREATE TABLE IF NOT EXISTS bets (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, bet REAL, choice TEXT, outcome TEXT, win INT, payout REAL, ts INT)")
    c.execute("CREATE TABLE IF NOT EXISTS checks (code TEXT PRIMARY KEY, creator TEXT, per_user REAL, remaining INT, claimed TEXT, created INT)")
    c.execute("CREATE TABLE IF NOT EXISTS promos (name TEXT PRIMARY KEY, reward REAL, claimed TEXT, remaining INT, created_by TEXT)")
    c.execute("CREATE TABLE IF NOT EXISTS bank (id INTEGER PRIMARY KEY AUTOINCREMENT, user_id TEXT, amount REAL, rate REAL, days INT, opened INT, status TEXT, closed INT)")
    c.execute("CREATE TABLE IF NOT EXISTS json_data (key TEXT PRIMARY KEY, value TEXT)")
    for col in ['username','stars','games','wins','withdraws','deposit_stars']:
        try: c.execute(f"ALTER TABLE users ADD COLUMN {col}")
        except: pass
    conn.commit()
    conn.close()

init_db()

# ========== СОСТОЯНИЯ ==========
class GameStates(StatesGroup):
    bet = State()
    choice = State()
    mines = State()

# ========== ФУНКЦИИ ==========
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def now():
    return int(time.time())

def fmt(n):
    n = round(float(n),2)
    if n >= 1e6: return f"{n/1e6:.1f}M {CURRENCY}"
    if n >= 1e3: return f"{n/1e3:.1f}K {CURRENCY}"
    return f"{n:.2f} {CURRENCY}"

def parse_amount(t):
    t = str(t).strip().lower().replace(" ", "").replace(",",".")
    if t in ["все","всё","all"]: return -1
    m = 1
    if t.endswith(("к","k")): t,m = t[:-1],1000
    if t.endswith(("м","m")): t,m = t[:-1],1e6
    v = float(t)*m
    if v <= 0: raise ValueError
    return round(v,2)

def mention(uid,name=None):
    return f'<a href="tg://user?id={uid}">{html.escape(name or f"User{uid}")}</a>'

def is_admin(uid): return uid in ADMIN_IDS

def ensure_user(uid, name=None):
    conn = get_db()
    conn.execute("INSERT OR IGNORE INTO users (id,username,coins,checks,withdraws) VALUES (?,?,?,?,?)", (str(uid), name or "", START_BALANCE, "[]", "[]"))
    if name: conn.execute("UPDATE users SET username = ? WHERE id = ?", (name, str(uid)))
    conn.commit()
    conn.close()

def get_user(uid):
    conn = get_db()
    ensure_user(uid)
    u = conn.execute("SELECT * FROM users WHERE id = ?", (str(uid),)).fetchone()
    conn.close()
    return u

def add_coins(uid, amount):
    conn = get_db()
    conn.execute("BEGIN")
    conn.execute("UPDATE users SET coins = coins + ? WHERE id = ?", (round(amount,2), str(uid)))
    conn.commit()
    row = conn.execute("SELECT coins FROM users WHERE id = ?", (str(uid),)).fetchone()
    conn.close()
    return row["coins"]

def add_stars(uid, stars):
    conn = get_db()
    conn.execute("BEGIN")
    conn.execute("UPDATE users SET stars = stars + ?, deposit_stars = deposit_stars + ? WHERE id = ?", (stars, stars, str(uid)))
    conn.execute("UPDATE users SET coins = coins + ? WHERE id = ?", (stars * STARS_RATE, str(uid)))
    conn.commit()
    conn.close()

def reserve_bet(uid, bet):
    conn = get_db()
    conn.execute("BEGIN")
    u = conn.execute("SELECT coins FROM users WHERE id = ?", (str(uid),)).fetchone()
    coins = u["coins"]
    if bet == -1: bet = coins
    if coins < bet:
        conn.rollback()
        conn.close()
        return False, coins
    conn.execute("UPDATE users SET coins = coins - ? WHERE id = ?", (bet, str(uid)))
    conn.commit()
    conn.close()
    return True, coins - bet

def finalize_bet(uid, bet, payout, choice, outcome):
    payout = max(0, round(payout,2))
    conn = get_db()
    conn.execute("BEGIN")
    if payout > 0:
        conn.execute("UPDATE users SET coins = coins + ? WHERE id = ?", (payout, str(uid)))
    conn.execute("INSERT INTO bets (user_id,bet,choice,outcome,win,payout,ts) VALUES (?,?,?,?,?,?,?)",
                 (str(uid), round(bet,2), choice, outcome, 1 if payout>0 else 0, payout, now()))
    if payout > 0:
        conn.execute("UPDATE users SET wins = wins + 1 WHERE id = ?", (str(uid),))
    conn.execute("UPDATE users SET games = games + 1 WHERE id = ?", (str(uid),))
    conn.commit()
    bal = conn.execute("SELECT coins FROM users WHERE id = ?", (str(uid),)).fetchone()["coins"]
    conn.close()
    return bal

def instant_bet(uid, bet, payout, choice, outcome):
    payout = max(0, round(payout,2))
    conn = get_db()
    conn.execute("BEGIN")
    u = conn.execute("SELECT coins FROM users WHERE id = ?", (str(uid),)).fetchone()
    coins = u["coins"]
    if bet == -1: bet = coins
    if coins < bet:
        conn.rollback()
        conn.close()
        return False, coins
    new_bal = round(coins - bet + payout, 2)
    conn.execute("UPDATE users SET coins = ? WHERE id = ?", (new_bal, str(uid)))
    conn.execute("INSERT INTO bets (user_id,bet,choice,outcome,win,payout,ts) VALUES (?,?,?,?,?,?,?)",
                 (str(uid), round(bet,2), choice, outcome, 1 if payout>0 else 0, payout, now()))
    if payout > 0:
        conn.execute("UPDATE users SET wins = wins + 1 WHERE id = ?", (str(uid),))
    conn.execute("UPDATE users SET games = games + 1 WHERE id = ?", (str(uid),))
    conn.commit()
    conn.close()
    return True, new_bal

def get_stats(uid):
    u = get_user(uid)
    conn = get_db()
    dep = conn.execute("SELECT COUNT(*) as cnt, COALESCE(SUM(amount),0) as sum FROM bank WHERE user_id = ? AND status = 'active'", (str(uid),)).fetchone()
    conn.close()
    return {
        "coins": u["coins"], "stars": u["stars"] or 0, "status": u["status"] or 0,
        "games": u["games"] or 0, "wins": u["wins"] or 0,
        "deposits": dep["cnt"], "deposit_sum": dep["sum"]
    }

def top_balance():
    conn = get_db()
    rows = conn.execute("SELECT id, username, coins FROM users ORDER BY coins DESC LIMIT 10").fetchall()
    conn.close()
    return rows

# ========== КЛАВИАТУРЫ ==========
def main_kb():
    kb = [[InlineKeyboardButton(text="💰 Профиль", callback_data="profile")]]
    kb.append([InlineKeyboardButton(text="🎮 Игры", callback_data="games")])
    kb.append([InlineKeyboardButton(text="🏦 Банк", callback_data="bank")])
    kb.append([InlineKeyboardButton(text="⭐ Пополнить", callback_data="deposit"), InlineKeyboardButton(text="💸 Вывести", callback_data="withdraw")])
    kb.append([InlineKeyboardButton(text="🎁 Бонус", callback_data="bonus"), InlineKeyboardButton(text="🏆 Топ", callback_data="top")])
    kb.append([InlineKeyboardButton(text="🧾 Чеки", callback_data="checks"), InlineKeyboardButton(text="🎟 Промо", callback_data="promo")])
    kb.append([InlineKeyboardButton(text="❓ Помощь", callback_data="help")])
    if is_admin(8478884644):
        kb.append([InlineKeyboardButton(text="🛠️ Админ", callback_data="admin")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def games_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗼 Башня", callback_data="game:tower"), InlineKeyboardButton(text="🥇 Золото", callback_data="game:gold")],
        [InlineKeyboardButton(text="💎 Алмазы", callback_data="game:diamonds"), InlineKeyboardButton(text="💣 Мины", callback_data="game:mines")],
        [InlineKeyboardButton(text="🎡 Рулетка", callback_data="game:roulette"), InlineKeyboardButton(text="📈 Краш", callback_data="game:crash")],
        [InlineKeyboardButton(text="🎲 Кубик", callback_data="game:cube"), InlineKeyboardButton(text="🎯 Кости", callback_data="game:dice")],
        [InlineKeyboardButton(text="🎴 Очко", callback_data="game:ochko"), InlineKeyboardButton(text="🦙 Ламы", callback_data="game:llama")],
        [InlineKeyboardButton(text="⚽ Футбол", callback_data="game:football"), InlineKeyboardButton(text="🏀 Баскет", callback_data="game:basket")],
        [InlineKeyboardButton(text="🎯 Дартс", callback_data="game:darts")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")]
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Выдать", callback_data="admin:give")],
        [InlineKeyboardButton(text="👑 Статус", callback_data="admin:status")],
        [InlineKeyboardButton(text="🎟 Промо", callback_data="admin:promo")],
        [InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton(text="📋 Выводы", callback_data="admin:withdraws")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")]
    ])

# ========== БОТ ==========
dp = Dispatcher(storage=MemoryStorage())
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

@dp.message(CommandStart())
async def start(m: Message):
    ensure_user(m.from_user.id, m.from_user.username)
    await m.answer(
        f"🦙 <b>WILLD GRAMM</b>\n\n"
        f"💰 Баланс: {fmt(get_user(m.from_user.id)['coins'])}\n"
        f"👤 Статус: {STATUSES[get_user(m.from_user.id)['status']]}\n\n"
        f"🎮 <b>Игры:</b> башня, золото, алмазы, мины, рул, краш, кубик, кости, очко, футбол, баскет, ламы, дартс\n"
        f"💡 Пример: <code>башня 500 2</code> или <code>все</code> вместо суммы\n\n"
        f"⬇️ <b>Меню:</b>",
        reply_markup=main_kb()
    )

@dp.message(Command("admin"))
async def admin_cmd(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("⛔ Доступ запрещён")
    await m.answer("🛠️ Админ-панель", reply_markup=admin_kb())

@dp.callback_query(F.data == "menu")
async def menu_cb(c: CallbackQuery):
    await c.message.edit_text("📱 Главное меню", reply_markup=main_kb())
    await c.answer()

@dp.callback_query(F.data == "profile")
async def profile_cb(c: CallbackQuery):
    s = get_stats(c.from_user.id)
    await c.message.edit_text(
        f"👤 <b>Профиль</b>\n\n"
        f"🆔 ID: <code>{c.from_user.id}</code>\n"
        f"👤 Статус: {STATUSES[s['status']]}\n\n"
        f"💰 Баланс: {fmt(s['coins'])}\n"
        f"⭐ Stars: {s['stars']}\n\n"
        f"🎮 Игр: {s['games']}\n"
        f"🏆 Побед: {s['wins']}\n"
        f"📊 WR: {(s['wins']/max(1,s['games'])*100):.1f}%\n\n"
        f"🏦 Депозитов: {s['deposits']} | {fmt(s['deposit_sum'])}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")]
        ])
    )
    await c.answer()

@dp.callback_query(F.data == "games")
async def games_cb(c: CallbackQuery):
    await c.message.edit_text("🎮 <b>Выбери игру</b>", reply_markup=games_kb())
    await c.answer()

@dp.callback_query(F.data == "bonus")
async def bonus_cb(c: CallbackQuery):
    uid = c.from_user.id
    key = f"bonus_{uid}"
    conn = get_db()
    last = conn.execute("SELECT value FROM json_data WHERE key = ?", (key,)).fetchone()
    last_ts = int(json.loads(last["value"])) if last else 0
    now_ts = now()
    conn.close()
    if now_ts - last_ts < 86400:
        left = 86400 - (now_ts - last_ts)
        await c.answer(f"Бонус через {left//3600}ч {(left%3600)//60}м", show_alert=True)
        return
    ok, bal = instant_bet(uid, 0, DAILY_BONUS, "bonus", "daily")
    if ok:
        conn = get_db()
        conn.execute("INSERT OR REPLACE INTO json_data (key, value) VALUES (?, ?)", (key, json.dumps(now_ts)))
        conn.commit()
        conn.close()
        await c.message.answer(f"🎁 +{fmt(DAILY_BONUS)}\n💰 Баланс: {fmt(bal)}")
    await c.answer()

@dp.callback_query(F.data == "top")
async def top_cb(c: CallbackQuery):
    rows = top_balance()
    text = "🏆 <b>Топ по балансу</b>\n\n"
    medals = ["🥇","🥈","🥉"]
    for i, r in enumerate(rows[:10]):
        medal = medals[i] if i<3 else f"{i+1}."
        name = r["username"] or f"User{r['id'][:6]}"
        text += f"{medal} {html.escape(name)} — {fmt(r['coins'])}\n"
    await c.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад", callback_data="menu")]]))
    await c.answer()

@dp.callback_query(F.data == "help")
async def help_cb(c: CallbackQuery):
    await c.message.edit_text(
        f"❓ <b>Помощь</b>\n\n"
        f"📜 <b>Правила:</b>\n"
        f"• Ставки: {MIN_BET}-{MAX_BET}\n"
        f"• Вывод: комиссия {WITHDRAW_FEE*100}%\n"
        f"• Чек: комиссия {CHECK_FEE*100}%\n\n"
        f"🎮 <b>Игры:</b>\n"
        f"<code>башня 1000 2</code>\n"
        f"<code>золото 1000</code>\n"
        f"<code>алмазы 1000</code>\n"
        f"<code>мины 1000 3</code>\n"
        f"<code>рул 1000 чет</code>\n"
        f"<code>краш 1000 2.5</code>\n"
        f"<code>кубик 1000 5</code>\n"
        f"<code>кости 1000 м</code>\n"
        f"<code>очко 1000</code>\n"
        f"<code>футбол 1000 гол</code>\n"
        f"<code>баскет 1000</code>\n"
        f"<code>ламы 1000</code>\n"
        f"<code>дартс 1000 красное</code>\n\n"
        f"💡 Вместо суммы можно написать <code>все</code>\n\n"
        f"📢 <a href='{CHANNEL_LINK}'>Канал</a> | 🆘 Поддержка: @CashOverseer",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📢 Канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton(text="🆘 Поддержка", url="https://t.me/CashOverseer")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu")]
        ])
    )
    await c.answer()

# ========== ИГРЫ ==========
active_games = {}

@dp.message(lambda m: m.text and m.text.lower().startswith("башня"))
async def tower_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 2:
        return await m.answer("❌ Формат: `башня ставка [мины 1-3]`")
    try:
        bet = parse_amount(parts[1])
    except:
        return await m.answer("❌ Неверная ставка")
    mines = 1
    if len(parts) >= 3:
        try: mines = max(1, min(3, int(parts[2])))
        except: pass
    if bet == -1:
        bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET:
        return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET:
        return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    ok, _ = reserve_bet(m.from_user.id, bet)
    if not ok:
        return await m.answer("❌ Недостаточно средств")
    active_games[m.from_user.id] = {"game":"tower","bet":bet,"level":0,"mines":mines}
    await m.answer(
        f"🗼 <b>Башня</b>\nСтавка: {fmt(bet)}\nЭтаж: 0\nМножитель: x1\n\nВыбери секцию:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="tower:1"), InlineKeyboardButton(text="2", callback_data="tower:2"), InlineKeyboardButton(text="3", callback_data="tower:3")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="tower:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="tower:cancel")]
        ])
    )

@dp.callback_query(F.data.startswith("tower:"))
async def tower_cb(c: CallbackQuery):
    uid = c.from_user.id
    game = active_games.get(uid)
    if not game or game["game"] != "tower":
        return await c.answer("Нет активной игры", show_alert=True)
    action = c.data.split(":")[1]
    if action == "cash":
        if game["level"] == 0:
            return await c.answer("Сначала сделай ход", show_alert=True)
        mult = TOWER_MULT[game["level"]-1]
        payout = round(game["bet"] * mult, 2)
        bal = finalize_bet(uid, game["bet"], payout, "tower", f"cashout_{game['level']}")
        active_games.pop(uid)
        await c.message.edit_text(f"✅ Забрал {fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    if action == "cancel":
        if game["level"] > 0:
            return await c.answer("Нельзя отменить после хода", show_alert=True)
        bal = finalize_bet(uid, game["bet"], game["bet"], "tower", "cancel")
        active_games.pop(uid)
        await c.message.edit_text(f"❌ Отмена. Возвращено {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    # pick
    chosen = int(action)
    safe = random.randint(1,3)
    if chosen != safe:
        bal = finalize_bet(uid, game["bet"], 0, "tower", f"lose_at_{game['level']}")
        active_games.pop(uid)
        await c.message.edit_text(f"💥 Ловушка в {safe}! Ты выбрал {chosen}\nПотеряно: {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    game["level"] += 1
    if game["level"] >= len(TOWER_MULT):
        payout = round(game["bet"] * TOWER_MULT[-1], 2)
        bal = finalize_bet(uid, game["bet"], payout, "tower", "completed")
        active_games.pop(uid)
        await c.message.edit_text(f"🏁 Башня пройдена! +{fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    mult = TOWER_MULT[game["level"]-1]
    await c.message.edit_text(
        f"🗼 <b>Башня</b>\nСтавка: {fmt(game['bet'])}\nЭтаж: {game['level']}\nМножитель: x{mult}\nВыигрыш: {fmt(game['bet']*mult)}\n\nВыбери секцию:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="tower:1"), InlineKeyboardButton(text="2", callback_data="tower:2"), InlineKeyboardButton(text="3", callback_data="tower:3")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="tower:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="tower:cancel")]
        ])
    )
    await c.answer("✅ Успех!")

# Золото
@dp.message(lambda m: m.text and m.text.lower().startswith("золото"))
async def gold_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 2:
        return await m.answer("❌ Формат: `золото ставка`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    ok, _ = reserve_bet(m.from_user.id, bet)
    if not ok: return await m.answer("❌ Недостаточно средств")
    active_games[m.from_user.id] = {"game":"gold","bet":bet,"step":0}
    await m.answer(
        f"🥇 <b>Золото</b>\nСтавка: {fmt(bet)}\nРаунд: 0\n\nВыбери плитку 1-4:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="gold:1"), InlineKeyboardButton(text="2", callback_data="gold:2"), InlineKeyboardButton(text="3", callback_data="gold:3"), InlineKeyboardButton(text="4", callback_data="gold:4")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="gold:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="gold:cancel")]
        ])
    )

@dp.callback_query(F.data.startswith("gold:"))
async def gold_cb(c: CallbackQuery):
    uid = c.from_user.id
    game = active_games.get(uid)
    if not game or game["game"] != "gold":
        return await c.answer("Нет активной игры", show_alert=True)
    action = c.data.split(":")[1]
    if action == "cash":
        if game["step"] == 0:
            return await c.answer("Сначала сделай ход", show_alert=True)
        mult = GOLD_MULT[game["step"]-1]
        payout = round(game["bet"] * mult, 2)
        bal = finalize_bet(uid, game["bet"], payout, "gold", f"cashout_{game['step']}")
        active_games.pop(uid)
        await c.message.edit_text(f"✅ Забрал {fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    if action == "cancel":
        if game["step"] > 0:
            return await c.answer("Нельзя отменить после хода", show_alert=True)
        bal = finalize_bet(uid, game["bet"], game["bet"], "gold", "cancel")
        active_games.pop(uid)
        await c.message.edit_text(f"❌ Отмена. Возвращено {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    chosen = int(action)
    trap = random.randint(1,4)
    if chosen == trap:
        bal = finalize_bet(uid, game["bet"], 0, "gold", f"lose_at_{game['step']}")
        active_games.pop(uid)
        await c.message.edit_text(f"💥 Ловушка в {trap}! Ты выбрал {chosen}\nПотеряно: {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    game["step"] += 1
    if game["step"] >= len(GOLD_MULT):
        payout = round(game["bet"] * GOLD_MULT[-1], 2)
        bal = finalize_bet(uid, game["bet"], payout, "gold", "completed")
        active_games.pop(uid)
        await c.message.edit_text(f"🏁 Золото пройдено! +{fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    mult = GOLD_MULT[game["step"]-1]
    await c.message.edit_text(
        f"🥇 <b>Золото</b>\nСтавка: {fmt(game['bet'])}\nРаунд: {game['step']}\nМножитель: x{mult}\nВыигрыш: {fmt(game['bet']*mult)}\n\nВыбери плитку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="gold:1"), InlineKeyboardButton(text="2", callback_data="gold:2"), InlineKeyboardButton(text="3", callback_data="gold:3"), InlineKeyboardButton(text="4", callback_data="gold:4")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="gold:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="gold:cancel")]
        ])
    )
    await c.answer("✅ Успех!")

# Алмазы (аналогично, сокращённо)
@dp.message(lambda m: m.text and m.text.lower().startswith("алмазы"))
async def diamonds_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 2:
        return await m.answer("❌ Формат: `алмазы ставка`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    ok, _ = reserve_bet(m.from_user.id, bet)
    if not ok: return await m.answer("❌ Недостаточно средств")
    active_games[m.from_user.id] = {"game":"diamonds","bet":bet,"step":0}
    await m.answer(
        f"💎 <b>Алмазы</b>\nСтавка: {fmt(bet)}\nШаг: 0\n\nВыбери кристалл 1-5:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="diamond:1"), InlineKeyboardButton(text="2", callback_data="diamond:2"), InlineKeyboardButton(text="3", callback_data="diamond:3"), InlineKeyboardButton(text="4", callback_data="diamond:4"), InlineKeyboardButton(text="5", callback_data="diamond:5")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="diamond:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="diamond:cancel")]
        ])
    )

@dp.callback_query(F.data.startswith("diamond:"))
async def diamond_cb(c: CallbackQuery):
    uid = c.from_user.id
    game = active_games.get(uid)
    if not game or game["game"] != "diamonds":
        return await c.answer("Нет активной игры", show_alert=True)
    action = c.data.split(":")[1]
    if action == "cash":
        if game["step"] == 0: return await c.answer("Сначала сделай ход", show_alert=True)
        mult = DIAMOND_MULT[game["step"]-1]
        payout = round(game["bet"] * mult, 2)
        bal = finalize_bet(uid, game["bet"], payout, "diamonds", f"cashout_{game['step']}")
        active_games.pop(uid)
        await c.message.edit_text(f"✅ Забрал {fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    if action == "cancel":
        if game["step"] > 0: return await c.answer("Нельзя отменить после хода", show_alert=True)
        bal = finalize_bet(uid, game["bet"], game["bet"], "diamonds", "cancel")
        active_games.pop(uid)
        await c.message.edit_text(f"❌ Отмена. Возвращено {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    chosen = int(action)
    trap = random.randint(1,5)
    if chosen == trap:
        bal = finalize_bet(uid, game["bet"], 0, "diamonds", f"lose_at_{game['step']}")
        active_games.pop(uid)
        await c.message.edit_text(f"💥 Бракованный кристалл {trap}! Ты выбрал {chosen}\nПотеряно: {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    game["step"] += 1
    if game["step"] >= len(DIAMOND_MULT):
        payout = round(game["bet"] * DIAMOND_MULT[-1], 2)
        bal = finalize_bet(uid, game["bet"], payout, "diamonds", "completed")
        active_games.pop(uid)
        await c.message.edit_text(f"🏁 Алмазы пройдены! +{fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    mult = DIAMOND_MULT[game["step"]-1]
    await c.message.edit_text(
        f"💎 <b>Алмазы</b>\nСтавка: {fmt(game['bet'])}\nШаг: {game['step']}\nМножитель: x{mult}\nВыигрыш: {fmt(game['bet']*mult)}\n\nВыбери кристалл:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="diamond:1"), InlineKeyboardButton(text="2", callback_data="diamond:2"), InlineKeyboardButton(text="3", callback_data="diamond:3"), InlineKeyboardButton(text="4", callback_data="diamond:4"), InlineKeyboardButton(text="5", callback_data="diamond:5")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="diamond:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="diamond:cancel")]
        ])
    )
    await c.answer("✅ Успех!")

# Рулетка
@dp.message(lambda m: m.text and m.text.lower().startswith("рул"))
async def roulette_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 3:
        return await m.answer("❌ Формат: `рул ставка красное/черное/чет/нечет/зеро`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    choice_map = {"красное":"red","черное":"black","чет":"even","нечет":"odd","зеро":"zero"}
    choice = choice_map.get(parts[2])
    if not choice:
        return await m.answer("❌ Выбери: красное, черное, чет, нечет, зеро")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    number = random.randint(0,36)
    color = "green" if number==0 else ("red" if number in RED_NUMBERS else "black")
    parity = "zero" if number==0 else ("even" if number%2==0 else "odd")
    win = False
    mult = 0
    if choice=="red" and color=="red": win,mult = True,2
    elif choice=="black" and color=="black": win,mult = True,2
    elif choice=="even" and parity=="even": win,mult = True,2
    elif choice=="odd" and parity=="odd": win,mult = True,2
    elif choice=="zero" and number==0: win,mult = True,36
    payout = round(bet * mult, 2) if win else 0
    ok, bal = instant_bet(m.from_user.id, bet, payout, f"roulette:{choice}", f"num={number}")
    if not ok:
        return await m.answer("❌ Недостаточно средств")
    color_emoji = {"red":"🔴","black":"⚫","green":"🟢"}[color]
    await m.answer(f"{color_emoji} Выпало {number}\nРезультат: {'✅ Победа' if win else '❌ Поражение'}\nВыплата: {fmt(payout)}\n💰 Баланс: {fmt(bal)}")

# Краш
@dp.message(lambda m: m.text and m.text.lower().startswith("краш"))
async def crash_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 3:
        return await m.answer("❌ Формат: `краш ставка множитель`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    try: target = float(parts[2].replace(",","."))
    except: return await m.answer("❌ Неверный множитель")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    if target < 1.01 or target > 10:
        return await m.answer("❌ Множитель от 1.01 до 10")
    msg = await m.answer("🚀 Ракета взлетает...")
    await asyncio.sleep(1)
    await msg.edit_text("🚀🚀 Набирает высоту...")
    await asyncio.sleep(1)
    await msg.edit_text("🚀🚀🚀 В космосе!")
    await asyncio.sleep(0.8)
    r = random.random()
    if r<0.06: crash = 1.00
    elif r<0.55: crash = round(random.uniform(1.01,1.80),2)
    elif r<0.80: crash = round(random.uniform(1.81,2.80),2)
    elif r<0.93: crash = round(random.uniform(2.81,4.50),2)
    elif r<0.985: crash = round(random.uniform(4.51,9.50),2)
    else: crash = round(random.uniform(9.51,10.0),2)
    await msg.edit_text(f"💥 КРАШ! x{crash}")
    win = target <= crash
    payout = round(bet * target, 2) if win else 0
    ok, bal = instant_bet(m.from_user.id, bet, payout, f"crash:{target}", f"crash={crash}")
    if not ok:
        return await m.answer("❌ Недостаточно средств")
    await m.answer(f"📈 Твой множитель: x{target}\nИгра: x{crash}\nРезультат: {'✅ Победа' if win else '❌ Поражение'}\nВыплата: {fmt(payout)}\n💰 Баланс: {fmt(bal)}")

# Кубик
@dp.message(lambda m: m.text and m.text.lower().startswith("кубик"))
async def cube_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 3:
        return await m.answer("❌ Формат: `кубик ставка 1-6`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    try: guess = int(parts[2])
    except: return await m.answer("❌ Неверное число")
    if guess<1 or guess>6:
        return await m.answer("❌ Число от 1 до 6")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    dice = await m.answer_dice(emoji="🎲")
    rolled = dice.dice.value
    win = guess == rolled
    payout = round(bet * 5.8, 2) if win else 0
    ok, bal = instant_bet(m.from_user.id, bet, payout, f"cube:{guess}", f"rolled={rolled}")
    if not ok:
        return await m.answer("❌ Недостаточно средств")
    await m.answer(f"🎲 Выпало {rolled}\nРезультат: {'✅ Победа' if win else '❌ Поражение'}\nВыплата: {fmt(payout)}\n💰 Баланс: {fmt(bal)}")

# Кости
@dp.message(lambda m: m.text and m.text.lower().startswith("кости"))
async def dice_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 3:
        return await m.answer("❌ Формат: `кости ставка м/б/равно`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    choice = parts[2]
    if choice not in ["м","б","равно"]:
        return await m.answer("❌ Выбери: м (меньше 7), б (больше 7), равно")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    d1 = await m.answer_dice(emoji="🎲")
    d2 = await m.answer_dice(emoji="🎲")
    total = d1.dice.value + d2.dice.value
    win = False
    mult = 0
    if choice == "м" and total < 7: win,mult = True,2.25
    elif choice == "б" and total > 7: win,mult = True,2.25
    elif choice == "равно" and total == 7: win,mult = True,5
    payout = round(bet * mult, 2) if win else 0
    ok, bal = instant_bet(m.from_user.id, bet, payout, f"dice:{choice}", f"sum={total}")
    if not ok:
        return await m.answer("❌ Недостаточно средств")
    await m.answer(f"🎲 {d1.dice.value} + {d2.dice.value} = {total}\nРезультат: {'✅ Победа' if win else '❌ Поражение'}\nВыплата: {fmt(payout)}\n💰 Баланс: {fmt(bal)}")

# Футбол
@dp.message(lambda m: m.text and m.text.lower().startswith("футбол"))
async def football_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 2:
        return await m.answer("❌ Формат: `футбол ставка [гол/мимо]`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    choice = parts[2] if len(parts)>2 else None
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    dice = await m.answer_dice(emoji="⚽")
    value = dice.dice.value
    outcome = "гол" if value >= 3 else "мимо"
    if choice:
        win = outcome == choice
        payout = round(bet * 1.85, 2) if win else 0
        ok, bal = instant_bet(m.from_user.id, bet, payout, f"football:{choice}", outcome)
    else:
        win = value >= 4
        payout = round(bet * 1.85, 2) if win else 0
        ok, bal = instant_bet(m.from_user.id, bet, payout, "football", f"value={value}")
    if not ok:
        return await m.answer("❌ Недостаточно средств")
    await m.answer(f"⚽ {outcome.upper()}!\nРезультат: {'✅ Победа' if win else '❌ Поражение'}\nВыплата: {fmt(payout)}\n💰 Баланс: {fmt(bal)}")

# Баскет
@dp.message(lambda m: m.text and m.text.lower().startswith("баскет"))
async def basket_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 2:
        return await m.answer("❌ Формат: `баскет ставка`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    dice = await m.answer_dice(emoji="🏀")
    value = dice.dice.value
    win = value in [4,5]
    payout = round(bet * 2.2, 2) if win else 0
    ok, bal = instant_bet(m.from_user.id, bet, payout, "basket", f"value={value}")
    if not ok:
        return await m.answer("❌ Недостаточно средств")
    await m.answer(f"🏀 {'ПОПАДАНИЕ!' if win else 'Промах'}\nРезультат: {'✅ Победа' if win else '❌ Поражение'}\nВыплата: {fmt(payout)}\n💰 Баланс: {fmt(bal)}")

# Ламы
@dp.message(lambda m: m.text and m.text.lower().startswith("ламы"))
async def llama_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 2:
        return await m.answer("❌ Формат: `ламы ставка`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    ok, _ = reserve_bet(m.from_user.id, bet)
    if not ok: return await m.answer("❌ Недостаточно средств")
    active_games[m.from_user.id] = {"game":"llama","bet":bet,"level":0}
    await m.answer(
        f"🦙 <b>Ламы</b>\nСтавка: {fmt(bet)}\nУровень: 0\n\nВыбери ламу 1-4:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="llama:1"), InlineKeyboardButton(text="2", callback_data="llama:2"), InlineKeyboardButton(text="3", callback_data="llama:3"), InlineKeyboardButton(text="4", callback_data="llama:4")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="llama:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="llama:cancel")]
        ])
    )

@dp.callback_query(F.data.startswith("llama:"))
async def llama_cb(c: CallbackQuery):
    uid = c.from_user.id
    game = active_games.get(uid)
    if not game or game["game"] != "llama":
        return await c.answer("Нет активной игры", show_alert=True)
    action = c.data.split(":")[1]
    if action == "cash":
        if game["level"] == 0: return await c.answer("Сначала сделай ход", show_alert=True)
        mult = LLAMA_MULT[game["level"]-1]
        payout = round(game["bet"] * mult, 2)
        bal = finalize_bet(uid, game["bet"], payout, "llama", f"cashout_{game['level']}")
        active_games.pop(uid)
        await c.message.edit_text(f"✅ Забрал {fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    if action == "cancel":
        if game["level"] > 0: return await c.answer("Нельзя отменить после хода", show_alert=True)
        bal = finalize_bet(uid, game["bet"], game["bet"], "llama", "cancel")
        active_games.pop(uid)
        await c.message.edit_text(f"❌ Отмена. Возвращено {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    chosen = int(action)
    safe = random.randint(1,4)
    if chosen != safe:
        bal = finalize_bet(uid, game["bet"], 0, "llama", f"lose_at_{game['level']}")
        active_games.pop(uid)
        await c.message.edit_text(f"💥 Злая лама в {safe}! Ты выбрал {chosen}\nПотеряно: {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    game["level"] += 1
    if game["level"] >= len(LLAMA_MULT):
        payout = round(game["bet"] * LLAMA_MULT[-1], 2)
        bal = finalize_bet(uid, game["bet"], payout, "llama", "completed")
        active_games.pop(uid)
        await c.message.edit_text(f"🏁 Ламы пройдены! +{fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    mult = LLAMA_MULT[game["level"]-1]
    await c.message.edit_text(
        f"🦙 <b>Ламы</b>\nСтавка: {fmt(game['bet'])}\nУровень: {game['level']}\nМножитель: x{mult}\nВыигрыш: {fmt(game['bet']*mult)}\n\nВыбери ламу:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="llama:1"), InlineKeyboardButton(text="2", callback_data="llama:2"), InlineKeyboardButton(text="3", callback_data="llama:3"), InlineKeyboardButton(text="4", callback_data="llama:4")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="llama:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="llama:cancel")]
        ])
    )
    await c.answer("✅ Лама добрая!")

# Дартс
@dp.message(lambda m: m.text and m.text.lower().startswith("дартс"))
async def darts_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 3:
        return await m.answer("❌ Формат: `дартс ставка красное/черное/чет/нечет/зеро`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    choice_map = {"красное":"red","черное":"black","чет":"even","нечет":"odd","зеро":"zero"}
    choice = choice_map.get(parts[2])
    if not choice:
        return await m.answer("❌ Выбери: красное, черное, чет, нечет, зеро")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    number = random.randint(0,20)
    color = "green" if number==0 else ("red" if number%2==1 else "black")
    parity = "zero" if number==0 else ("even" if number%2==0 else "odd")
    win = False
    mult = 0
    if choice=="red" and color=="red": win,mult = True,2
    elif choice=="black" and color=="black": win,mult = True,2
    elif choice=="even" and parity=="even": win,mult = True,2
    elif choice=="odd" and parity=="odd": win,mult = True,2
    elif choice=="zero" and number==0: win,mult = True,20
    payout = round(bet * mult, 2) if win else 0
    ok, bal = instant_bet(m.from_user.id, bet, payout, f"darts:{choice}", f"num={number}")
    if not ok:
        return await m.answer("❌ Недостаточно средств")
    color_emoji = {"red":"🔴","black":"⚫","green":"🟢"}[color]
    await m.answer(f"{color_emoji} Выпало {number}\nРезультат: {'✅ Победа' if win else '❌ Поражение'}\nВыплата: {fmt(payout)}\n💰 Баланс: {fmt(bal)}")

# Очко (Blackjack)
@dp.message(lambda m: m.text and m.text.lower().startswith("очко"))
async def ochko_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 2:
        return await m.answer("❌ Формат: `очко ставка`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    ok, _ = reserve_bet(m.from_user.id, bet)
    if not ok: return await m.answer("❌ Недостаточно средств")
    ranks = ["2","3","4","5","6","7","8","9","10","J","Q","K","A"]
    suits = ["♠","♥","♦","♣"]
    deck = [(r,s) for r in ranks for s in suits]
    random.shuffle(deck)
    player = [deck.pop(), deck.pop()]
    dealer = [deck.pop(), deck.pop()]
    active_games[m.from_user.id] = {"game":"ochko","bet":bet,"deck":deck,"player":player,"dealer":dealer}
    pv = sum(10 if r in ["J","Q","K"] else (11 if r=="A" else int(r)) for r,_ in player)
    # Aces adjustment
    aces = sum(1 for r,_ in player if r=="A")
    while pv > 21 and aces > 0:
        pv -= 10
        aces -= 1
    dv = sum(10 if r in ["J","Q","K"] else (11 if r=="A" else int(r)) for r,_ in dealer)
    aces = sum(1 for r,_ in dealer if r=="A")
    while dv > 21 and aces > 0:
        dv -= 10
        aces -= 1
    if pv == 21:
        if dv == 21:
            bal = finalize_bet(m.from_user.id, bet, bet, "ochko", "push")
            active_games.pop(m.from_user.id)
            await m.answer(f"🎴 Очко\n{player[0][0]}{player[0][1]} {player[1][0]}{player[1][1]} = 21\nДилер: {dealer[0][0]}{dealer[0][1]} ?\nНичья! Возврат {fmt(bet)}\n💰 Баланс: {fmt(bal)}")
        else:
            bal = finalize_bet(m.from_user.id, bet, bet*2.5, "ochko", "blackjack")
            active_games.pop(m.from_user.id)
            await m.answer(f"🎴 BLACKJACK! +{fmt(bet*2.5)}\n💰 Баланс: {fmt(bal)}")
        return
    await m.answer(
        f"🎴 <b>Очко</b>\nСтавка: {fmt(bet)}\n\nДилер: {dealer[0][0]}{dealer[0][1]} ?\nТы: {player[0][0]}{player[0][1]} {player[1][0]}{player[1][1]} = {pv}\n\nВыбери действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="➕ Взять", callback_data="ochko:hit"), InlineKeyboardButton(text="✋ Стоп", callback_data="ochko:stand")]
        ])
    )

@dp.callback_query(F.data.startswith("ochko:"))
async def ochko_cb(c: CallbackQuery):
    uid = c.from_user.id
    game = active_games.get(uid)
    if not game or game["game"] != "ochko":
        return await c.answer("Нет активной игры", show_alert=True)
    action = c.data.split(":")[1]
    if action == "hit":
        game["player"].append(game["deck"].pop())
        pv = sum(10 if r in ["J","Q","K"] else (11 if r=="A" else int(r)) for r,_ in game["player"])
        aces = sum(1 for r,_ in game["player"] if r=="A")
        while pv > 21 and aces > 0:
            pv -= 10
            aces -= 1
        if pv > 21:
            bal = finalize_bet(uid, game["bet"], 0, "ochko", "bust")
            active_games.pop(uid)
            await c.message.edit_text(f"🎴 Перебор! {pv}\n💰 Баланс: {fmt(bal)}")
            return await c.answer()
        cards = " ".join(f"{r}{s}" for r,s in game["player"])
        await c.message.edit_text(
            f"🎴 <b>Очко</b>\nСтавка: {fmt(game['bet'])}\n\nДилер: {game['dealer'][0][0]}{game['dealer'][0][1]} ?\nТы: {cards} = {pv}\n\nВыбери действие:",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Взять", callback_data="ochko:hit"), InlineKeyboardButton(text="✋ Стоп", callback_data="ochko:stand")]
            ])
        )
        await c.answer()
    elif action == "stand":
        # Dealer turn
        dv = sum(10 if r in ["J","Q","K"] else (11 if r=="A" else int(r)) for r,_ in game["dealer"])
        aces = sum(1 for r,_ in game["dealer"] if r=="A")
        while dv > 21 and aces > 0:
            dv -= 10
            aces -= 1
        while dv < 17:
            game["dealer"].append(game["deck"].pop())
            dv = sum(10 if r in ["J","Q","K"] else (11 if r=="A" else int(r)) for r,_ in game["dealer"])
            aces = sum(1 for r,_ in game["dealer"] if r=="A")
            while dv > 21 and aces > 0:
                dv -= 10
                aces -= 1
        pv = sum(10 if r in ["J","Q","K"] else (11 if r=="A" else int(r)) for r,_ in game["player"])
        aces = sum(1 for r,_ in game["player"] if r=="A")
        while pv > 21 and aces > 0:
            pv -= 10
            aces -= 1
        if dv > 21 or pv > dv:
            payout = game["bet"] * 2
            result = "Победа"
        elif pv == dv:
            payout = game["bet"]
            result = "Ничья"
        else:
            payout = 0
            result = "Поражение"
        bal = finalize_bet(uid, game["bet"], payout, "ochko", result.lower())
        active_games.pop(uid)
        player_cards = " ".join(f"{r}{s}" for r,s in game["player"])
        dealer_cards = " ".join(f"{r}{s}" for r,s in game["dealer"])
        await c.message.edit_text(
            f"🎴 <b>Очко</b>\n\nДилер: {dealer_cards} = {dv}\nТы: {player_cards} = {pv}\n\nРезультат: {result}\nВыплата: {fmt(payout)}\n💰 Баланс: {fmt(bal)}"
        )
        await c.answer()

# Мины
@dp.message(lambda m: m.text and m.text.lower().startswith("мины"))
async def mines_game(m: Message):
    parts = m.text.lower().split()
    if len(parts) < 2:
        return await m.answer("❌ Формат: `мины ставка [мины 1-5]`")
    try: bet = parse_amount(parts[1])
    except: return await m.answer("❌ Неверная ставка")
    mines = 3
    if len(parts) >= 3:
        try: mines = max(1, min(5, int(parts[2])))
        except: pass
    if bet == -1: bet = get_user(m.from_user.id)["coins"]
    if bet < MIN_BET: return await m.answer(f"❌ Мин. ставка: {MIN_BET}")
    if bet > MAX_BET: return await m.answer(f"❌ Макс. ставка: {fmt(MAX_BET)}")
    ok, _ = reserve_bet(m.from_user.id, bet)
    if not ok: return await m.answer("❌ Недостаточно средств")
    cells = list(range(1,10))
    mine_positions = set(random.sample(cells, mines))
    active_games[m.from_user.id] = {"game":"mines","bet":bet,"mines":mines,"positions":mine_positions,"opened":set()}
    await m.answer(
        f"💣 <b>Мины</b>\nСтавка: {fmt(bet)}\nМин: {mines}\nОткрыто: 0\nМножитель: x1\n\nВыбери клетку 1-9:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="mines:1"), InlineKeyboardButton(text="2", callback_data="mines:2"), InlineKeyboardButton(text="3", callback_data="mines:3")],
            [InlineKeyboardButton(text="4", callback_data="mines:4"), InlineKeyboardButton(text="5", callback_data="mines:5"), InlineKeyboardButton(text="6", callback_data="mines:6")],
            [InlineKeyboardButton(text="7", callback_data="mines:7"), InlineKeyboardButton(text="8", callback_data="mines:8"), InlineKeyboardButton(text="9", callback_data="mines:9")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="mines:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="mines:cancel")]
        ])
    )

def mines_mult(opened, mines):
    if opened == 0: return 1
    safe = 9 - mines
    base = 9 / max(1, safe)
    return round((base ** opened) * 0.95, 2)

@dp.callback_query(F.data.startswith("mines:"))
async def mines_cb(c: CallbackQuery):
    uid = c.from_user.id
    game = active_games.get(uid)
    if not game or game["game"] != "mines":
        return await c.answer("Нет активной игры", show_alert=True)
    action = c.data.split(":")[1]
    if action == "cash":
        if len(game["opened"]) == 0:
            return await c.answer("Сначала открой клетку", show_alert=True)
        mult = mines_mult(len(game["opened"]), game["mines"])
        payout = round(game["bet"] * mult, 2)
        bal = finalize_bet(uid, game["bet"], payout, "mines", f"cashout_{len(game['opened'])}")
        active_games.pop(uid)
        await c.message.edit_text(f"✅ Забрал {fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    if action == "cancel":
        if len(game["opened"]) > 0:
            return await c.answer("Нельзя отменить после хода", show_alert=True)
        bal = finalize_bet(uid, game["bet"], game["bet"], "mines", "cancel")
        active_games.pop(uid)
        await c.message.edit_text(f"❌ Отмена. Возвращено {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    cell = int(action)
    if cell in game["opened"]:
        return await c.answer("Уже открыто", show_alert=True)
    if cell in game["positions"]:
        bal = finalize_bet(uid, game["bet"], 0, "mines", f"explode_at_{cell}")
        active_games.pop(uid)
        await c.message.edit_text(f"💥 Ты попал на мину в клетке {cell}!\nПотеряно: {fmt(game['bet'])}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    game["opened"].add(cell)
    opened = len(game["opened"])
    safe_total = 9 - game["mines"]
    if opened >= safe_total:
        mult = mines_mult(opened, game["mines"])
        payout = round(game["bet"] * mult, 2)
        bal = finalize_bet(uid, game["bet"], payout, "mines", "cleared")
        active_games.pop(uid)
        await c.message.edit_text(f"🏁 Все безопасные клетки открыты! +{fmt(payout)}\n💰 Баланс: {fmt(bal)}")
        return await c.answer()
    mult = mines_mult(opened, game["mines"])
    await c.message.edit_text(
        f"💣 <b>Мины</b>\nСтавка: {fmt(game['bet'])}\nМин: {game['mines']}\nОткрыто: {opened}\nМножитель: x{mult}\nПотенциал: {fmt(game['bet']*mult)}\n\nВыбери клетку:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="1", callback_data="mines:1"), InlineKeyboardButton(text="2", callback_data="mines:2"), InlineKeyboardButton(text="3", callback_data="mines:3")],
            [InlineKeyboardButton(text="4", callback_data="mines:4"), InlineKeyboardButton(text="5", callback_data="mines:5"), InlineKeyboardButton(text="6", callback_data="mines:6")],
            [InlineKeyboardButton(text="7", callback_data="mines:7"), InlineKeyboardButton(text="8", callback_data="mines:8"), InlineKeyboardButton(text="9", callback_data="mines:9")],
            [InlineKeyboardButton(text="💰 Забрать", callback_data="mines:cash"), InlineKeyboardButton(text="❌ Сдаться", callback_data="mines:cancel")]
        ])
    )
    await c.answer("✅ Безопасно!")

# ========== ЗАПУСК ==========
async def main():
    print("✅ WILLD GRAMM запущен!")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
