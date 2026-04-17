import asyncio
import html
import json
import logging
import os
import re
import sqlite3
import time
from statistics import mean
from typing import Any, Optional
from urllib.parse import unquote

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw.types import InputBotAppShortName, InputUser

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "mrkt_session").strip()
OWNER_CHAT_ID = int(os.getenv("OWNER_CHAT_ID", "0") or 0)
CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "15"))
DB_PATH = os.getenv("DB_PATH", "alerts.db").strip()
MRKT_BOT_USERNAME = os.getenv("MRKT_BOT_USERNAME", "mrkt").strip()
MRKT_APP_SHORT_NAME = os.getenv("MRKT_APP_SHORT_NAME", "app").strip()
MRKT_PLATFORM = os.getenv("MRKT_PLATFORM", "android").strip()
TOKEN_REFRESH_SECONDS = int(os.getenv("TOKEN_REFRESH_SECONDS", "3600"))
MARKET_API_URL = "https://api.tgmrkt.io/api/v1"
MRKT_CDN_REFERER = "https://cdn.tgmrkt.io/"
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "20"))
STARTUP_TEST = os.getenv("SEND_STARTUP_TEST", "0").strip() == "1"

SUPPORTED_MARKETS = ["MRKT", "Tonnel", "GetGems", "Portals", "xGift"]

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("mrktbot")


def esc(v: Any) -> str:
    return html.escape("" if v is None else str(v))


def slugify(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", name or "")


def to_float(value: Any) -> Optional[float]:
    if value in (None, "", "None"):
        return None
    try:
        return float(value)
    except Exception:
        return None


def to_ton(value: Any) -> Optional[float]:
    x = to_float(value)
    if x is None:
        return None
    if x > 1_000_000:
        return x / 1_000_000_000
    return x


def fmt_ton(value: Any) -> str:
    x = to_ton(value)
    if x is None:
        return "—"
    return f"{x:.2f} TON"


def fmt_percent(value: Any) -> str:
    x = to_float(value)
    if x is None:
        return "—"
    if 0 < x < 1:
        x *= 100
    return f"{x:.2f}".rstrip("0").rstrip(".") + "%"


def build_tme_url(gift_name: str, gift_number: Any) -> Optional[str]:
    if not gift_name or gift_number is None:
        return None
    return f"https://t.me/nft/{slugify(gift_name)}-{gift_number}"


class InputStates(StatesGroup):
    gift = State()
    model = State()
    backdrop = State()
    min_price = State()
    max_price = State()


class DB:
    def __init__(self, path: str):
        self.path = path

    def conn(self):
        return sqlite3.connect(self.path)

    def init(self):
        conn = self.conn()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY,
                is_active INTEGER NOT NULL DEFAULT 1,
                gift_filter TEXT,
                model_filter TEXT,
                backdrop_filter TEXT,
                min_price REAL,
                max_price REAL,
                markets_json TEXT NOT NULL DEFAULT '[]',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_listings (
                uniq TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
            )
            """
        )
        conn.commit()
        conn.close()

    def ensure_user(self, chat_id: int):
        now = int(time.time())
        conn = self.conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users(chat_id, is_active, created_at, updated_at)
            VALUES(?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET is_active=1, updated_at=excluded.updated_at
            """,
            (chat_id, now, now),
        )
        conn.commit()
        conn.close()

    def get_user(self, chat_id: int) -> dict:
        conn = self.conn()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE chat_id=?", (chat_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            self.ensure_user(chat_id)
            return self.get_user(chat_id)
        data = dict(row)
        try:
            data["markets"] = json.loads(data.get("markets_json") or "[]")
        except Exception:
            data["markets"] = []
        return data

    def set_field(self, chat_id: int, field: str, value: Any):
        allowed = {"gift_filter", "model_filter", "backdrop_filter", "min_price", "max_price", "markets_json", "is_active"}
        if field not in allowed:
            raise ValueError("bad field")
        conn = self.conn()
        cur = conn.cursor()
        cur.execute(f"UPDATE users SET {field}=?, updated_at=? WHERE chat_id=?", (value, int(time.time()), chat_id))
        conn.commit()
        conn.close()

    def toggle_market(self, chat_id: int, market: str):
        user = self.get_user(chat_id)
        markets = set(user.get("markets") or [])
        if market in markets:
            markets.remove(market)
        else:
            markets.add(market)
        self.set_field(chat_id, "markets_json", json.dumps(sorted(markets), ensure_ascii=False))

    def clear_filters(self, chat_id: int):
        conn = self.conn()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE users
            SET gift_filter=NULL, model_filter=NULL, backdrop_filter=NULL,
                min_price=NULL, max_price=NULL, markets_json='[]', updated_at=?
            WHERE chat_id=?
            """,
            (int(time.time()), chat_id),
        )
        conn.commit()
        conn.close()

    def subscribers(self) -> list[dict]:
        conn = self.conn()
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute("SELECT * FROM users WHERE is_active=1")
        rows = [dict(r) for r in cur.fetchall()]
        conn.close()
        out = []
        for r in rows:
            try:
                r["markets"] = json.loads(r.get("markets_json") or "[]")
            except Exception:
                r["markets"] = []
            out.append(r)
        if OWNER_CHAT_ID and all(r["chat_id"] != OWNER_CHAT_ID for r in out):
            out.append({
                "chat_id": OWNER_CHAT_ID,
                "gift_filter": None,
                "model_filter": None,
                "backdrop_filter": None,
                "min_price": None,
                "max_price": None,
                "markets": [],
            })
        return out

    def seen(self, uniq: str) -> bool:
        conn = self.conn()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM seen_listings WHERE uniq=?", (uniq,))
        ok = cur.fetchone() is not None
        conn.close()
        return ok

    def mark_seen(self, uniq: str):
        conn = self.conn()
        cur = conn.cursor()
        cur.execute("INSERT OR IGNORE INTO seen_listings(uniq, created_at) VALUES(?, ?)", (uniq, int(time.time())))
        conn.commit()
        conn.close()


db = DB(DB_PATH)


class MrktApi:
    def __init__(self):
        self.token: Optional[str] = None
        self.token_received_at = 0.0
        self.http: Optional[AsyncSession] = None
        self.tg: Optional[Client] = None

    async def __aenter__(self):
        self.http = AsyncSession(
            impersonate="chrome124",
            timeout=30,
            headers={
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
                "Content-Type": "application/json;charset=UTF-8",
                "Origin": "https://cdn.tgmrkt.io",
                "Referer": MRKT_CDN_REFERER,
                "User-Agent": "Mozilla/5.0",
            },
        )
        self.tg = Client(SESSION_NAME, API_ID, API_HASH, no_updates=True)
        await self.tg.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.tg:
            await self.tg.stop()
        if self.http:
            await self.http.close()

    async def get_init_data(self) -> str:
        assert self.tg is not None
        bot_peer = await self.tg.resolve_peer(MRKT_BOT_USERNAME)
        bot = InputUser(user_id=bot_peer.user_id, access_hash=bot_peer.access_hash)
        bot_app = InputBotAppShortName(bot_id=bot, short_name=MRKT_APP_SHORT_NAME)
        web_view = await self.tg.invoke(
            RequestAppWebView(peer=bot_peer, app=bot_app, platform=MRKT_PLATFORM)
        )
        url = web_view.url
        if "tgWebAppData=" not in url:
            raise RuntimeError("tgWebAppData not found")
        return unquote(url.split("tgWebAppData=", 1)[1].split("&tgWebAppVersion", 1)[0])

    async def refresh_token(self):
        assert self.http is not None
        init_data = await self.get_init_data()
        resp = await self.http.post(f"{MARKET_API_URL}/auth", json={"data": init_data})
        resp.raise_for_status()
        data = resp.json()
        token = data.get("token") if isinstance(data, dict) else None
        if not token:
            raise RuntimeError(f"No token in auth response: {data}")
        self.token = token
        self.token_received_at = time.time()
        logger.info("MRKT token refreshed")

    async def ensure_token(self):
        if self.token and time.time() - self.token_received_at < TOKEN_REFRESH_SECONDS:
            return
        await self.refresh_token()

    async def post(self, path: str, payload: dict) -> dict:
        assert self.http is not None
        await self.ensure_token()
        headers = {"Authorization": self.token or "", "Referer": MRKT_CDN_REFERER, "Origin": "https://cdn.tgmrkt.io"}
        resp = await self.http.post(f"{MARKET_API_URL}{path}", headers=headers, json=payload)
        if resp.status_code == 401:
            await self.refresh_token()
            headers["Authorization"] = self.token or ""
            resp = await self.http.post(f"{MARKET_API_URL}{path}", headers=headers, json=payload)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"Bad response: {data}")
        return data

    async def fetch_saling(self, count: int = 20, gift: Optional[str] = None, model: Optional[str] = None, backdrop: Optional[str] = None,
                           min_price_ton: Optional[float] = None, max_price_ton: Optional[float] = None, cursor: str = "") -> dict:
        payload = {
            "collectionNames": [gift] if gift else [],
            "modelNames": [model] if model else [],
            "backdropNames": [backdrop] if backdrop else [],
            "symbolNames": [],
            "ordering": "None",
            "lowToHigh": False,
            "maxPrice": int(max_price_ton * 1_000_000_000) if max_price_ton else None,
            "minPrice": int(min_price_ton * 1_000_000_000) if min_price_ton else None,
            "mintable": None,
            "number": None,
            "count": min(max(count, 1), 20),
            "cursor": cursor,
            "query": None,
            "promotedFirst": False,
        }
        logger.info("MRKT /gifts/saling payload: %s", payload)
        return await self.post("/gifts/saling", payload)


def extract_from_mrkt(item: dict) -> dict:
    gift_name = item.get("collectionName") or item.get("name") or item.get("giftName") or "Unknown Gift"
    gift_number = item.get("giftNum") or item.get("number")
    model = item.get("modelName") or item.get("model") or "Unknown"
    backdrop = item.get("backdropName") or item.get("backdrop") or "Unknown"
    symbol = item.get("symbolName") or item.get("symbol") or "Unknown"
    price = to_ton(item.get("priceTon") or item.get("salePriceTon") or item.get("price") or item.get("salePrice"))
    return {
        "provider": "MRKT",
        "uniq": f"mrkt:{item.get('id') or item.get('saleId') or gift_name}:{gift_number}:{price}",
        "gift_name": gift_name,
        "gift_number": gift_number,
        "model_name": model,
        "model_percent": item.get("modelRarityPercent") or item.get("modelRarity"),
        "backdrop_name": backdrop,
        "backdrop_percent": item.get("backdropRarityPercent") or item.get("backgroundRarityPercent") or item.get("backdropRarity"),
        "symbol_name": symbol,
        "symbol_percent": item.get("symbolRarityPercent") or item.get("symbolRarity"),
        "price_ton": price,
        "avg_buy_ton": None,
        "avg_sell_ton": None,
        "market_url": item.get("url") or item.get("saleUrl") or item.get("marketUrl"),
        "tme_url": build_tme_url(gift_name, gift_number),
        "raw": item,
    }


async def calc_avg_sell_mrkt(api: MrktApi, listing: dict) -> Optional[float]:
    try:
        resp = await api.fetch_saling(
            count=10,
            gift=listing["gift_name"] if listing.get("gift_name") else None,
            model=listing["model_name"] if listing.get("model_name") and listing["model_name"] != "Unknown" else None,
            backdrop=listing["backdrop_name"] if listing.get("backdrop_name") and listing["backdrop_name"] != "Unknown" else None,
        )
        gifts = resp.get("gifts", []) or []
        vals = [to_ton((g.get("priceTon") or g.get("salePriceTon") or g.get("price") or g.get("salePrice"))) for g in gifts]
        vals = [v for v in vals if v is not None]
        return round(mean(vals), 4) if vals else None
    except Exception as e:
        logger.warning("avg sell calc failed: %s", e)
        return None


def user_matches(user: dict, listing: dict) -> bool:
    markets = set(user.get("markets") or [])
    if markets and listing["provider"] not in markets:
        return False
    gift_filter = (user.get("gift_filter") or "").strip().lower()
    if gift_filter and gift_filter not in (listing.get("gift_name") or "").lower():
        return False
    model_filter = (user.get("model_filter") or "").strip().lower()
    if model_filter and model_filter not in (listing.get("model_name") or "").lower():
        return False
    backdrop_filter = (user.get("backdrop_filter") or "").strip().lower()
    if backdrop_filter and backdrop_filter not in (listing.get("backdrop_name") or "").lower():
        return False
    min_price = user.get("min_price")
    if min_price is not None and listing.get("price_ton") is not None and listing["price_ton"] < float(min_price):
        return False
    max_price = user.get("max_price")
    if max_price is not None and listing.get("price_ton") is not None and listing["price_ton"] > float(max_price):
        return False
    return True


def settings_text(user: dict) -> str:
    markets = ", ".join(user.get("markets") or []) or "Все"
    return (
        "<b>Это бот для уведомлений о выходах подарков</b>\n\n"
        f"<b>Подарок:</b> {esc(user.get('gift_filter') or 'Все')}\n"
        f"<b>Модель:</b> {esc(user.get('model_filter') or 'Все')}\n"
        f"<b>Фон:</b> {esc(user.get('backdrop_filter') or 'Все')}\n"
        f"<b>Мин. цена:</b> {esc(user.get('min_price') if user.get('min_price') is not None else '—')}\n"
        f"<b>Макс. цена:</b> {esc(user.get('max_price') if user.get('max_price') is not None else '—')}\n"
        f"<b>Маркеты:</b> {esc(markets)}"
    )


def main_menu(chat_id: int) -> InlineKeyboardMarkup:
    user = db.get_user(chat_id)
    kb = InlineKeyboardBuilder()
    kb.button(text="🎁 Подарок", callback_data="set:gift")
    kb.button(text="🧬 Модель", callback_data="set:model")
    kb.button(text="🖼 Фон", callback_data="set:backdrop")
    kb.button(text="💸 Мин. цена", callback_data="set:min_price")
    kb.button(text="💰 Макс. цена", callback_data="set:max_price")
    kb.button(text="🏪 Маркеты", callback_data="menu:markets")
    kb.button(text="🗑 Сбросить фильтры", callback_data="reset:all")
    kb.adjust(2, 2, 2, 1)
    return kb.as_markup()


def markets_menu(chat_id: int) -> InlineKeyboardMarkup:
    user = db.get_user(chat_id)
    selected = set(user.get("markets") or [])
    kb = InlineKeyboardBuilder()
    for market in SUPPORTED_MARKETS:
        mark = "✅" if market in selected else "☑️"
        kb.button(text=f"{mark} {market}", callback_data=f"toggle_market:{market}")
    kb.button(text="⬅️ Назад", callback_data="menu:main")
    kb.adjust(2, 2, 1)
    return kb.as_markup()


def listing_text(item: dict) -> str:
    return (
        f"<b>{esc(item['gift_name'])} #{esc(item['gift_number'])}</b>\n"
        f"<b>Маркет:</b> {esc(item['provider'])}\n\n"
        f"- Model: {esc(item['model_name'])} ({fmt_percent(item.get('model_percent'))})\n"
        f"- Symbol: {esc(item['symbol_name'])} ({fmt_percent(item.get('symbol_percent'))})\n"
        f"- Backdrop: {esc(item['backdrop_name'])} ({fmt_percent(item.get('backdrop_percent'))})\n\n"
        f"🪙 Price: {fmt_ton(item.get('price_ton'))}\n"
        f"📉 Avg buy: {fmt_ton(item.get('avg_buy_ton'))}\n"
        f"📈 Avg sell: {fmt_ton(item.get('avg_sell_ton'))}"
    )


bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()


@dp.message(CommandStart())
async def cmd_start(message: Message):
    db.ensure_user(message.chat.id)
    await message.answer(settings_text(db.get_user(message.chat.id)), reply_markup=main_menu(message.chat.id))


@dp.callback_query(F.data == "menu:main")
async def cb_menu_main(call: CallbackQuery):
    await call.message.edit_text(settings_text(db.get_user(call.message.chat.id)), reply_markup=main_menu(call.message.chat.id))
    await call.answer()


@dp.callback_query(F.data == "menu:markets")
async def cb_markets(call: CallbackQuery):
    await call.message.edit_text("Выбери маркеты для уведомлений:", reply_markup=markets_menu(call.message.chat.id))
    await call.answer()


@dp.callback_query(F.data.startswith("toggle_market:"))
async def cb_toggle_market(call: CallbackQuery):
    market = call.data.split(":", 1)[1]
    db.ensure_user(call.message.chat.id)
    db.toggle_market(call.message.chat.id, market)
    await call.message.edit_reply_markup(reply_markup=markets_menu(call.message.chat.id))
    await call.answer("Сохранено")


@dp.callback_query(F.data == "reset:all")
async def cb_reset(call: CallbackQuery):
    db.clear_filters(call.message.chat.id)
    await call.message.edit_text(settings_text(db.get_user(call.message.chat.id)), reply_markup=main_menu(call.message.chat.id))
    await call.answer("Фильтры сброшены")


@dp.callback_query(F.data == "set:gift")
async def cb_set_gift(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.gift)
    await call.message.answer("Введи подарок вручную. Пример: <code>Jester Hat</code>\nЧтобы убрать фильтр — отправь <code>-</code>")
    await call.answer()


@dp.callback_query(F.data == "set:model")
async def cb_set_model(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.model)
    await call.message.answer("Введи модель вручную. Пример: <code>Funster</code>\nЧтобы убрать фильтр — отправь <code>-</code>")
    await call.answer()


@dp.callback_query(F.data == "set:backdrop")
async def cb_set_backdrop(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.backdrop)
    await call.message.answer("Введи фон вручную. Пример: <code>Silver Blue</code>\nЧтобы убрать фильтр — отправь <code>-</code>")
    await call.answer()


@dp.callback_query(F.data == "set:min_price")
async def cb_set_min_price(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.min_price)
    await call.message.answer("Введи минимальную цену в TON. Пример: <code>1.5</code>\nЧтобы убрать фильтр — отправь <code>-</code>")
    await call.answer()


@dp.callback_query(F.data == "set:max_price")
async def cb_set_max_price(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.max_price)
    await call.message.answer("Введи максимальную цену в TON. Пример: <code>10</code>\nЧтобы убрать фильтр — отправь <code>-</code>")
    await call.answer()


@dp.message(InputStates.gift)
async def st_gift(message: Message, state: FSMContext):
    db.ensure_user(message.chat.id)
    db.set_field(message.chat.id, "gift_filter", None if message.text.strip() == "-" else message.text.strip())
    await state.clear()
    await message.answer(settings_text(db.get_user(message.chat.id)), reply_markup=main_menu(message.chat.id))


@dp.message(InputStates.model)
async def st_model(message: Message, state: FSMContext):
    db.ensure_user(message.chat.id)
    db.set_field(message.chat.id, "model_filter", None if message.text.strip() == "-" else message.text.strip())
    await state.clear()
    await message.answer(settings_text(db.get_user(message.chat.id)), reply_markup=main_menu(message.chat.id))


@dp.message(InputStates.backdrop)
async def st_backdrop(message: Message, state: FSMContext):
    db.ensure_user(message.chat.id)
    db.set_field(message.chat.id, "backdrop_filter", None if message.text.strip() == "-" else message.text.strip())
    await state.clear()
    await message.answer(settings_text(db.get_user(message.chat.id)), reply_markup=main_menu(message.chat.id))


@dp.message(InputStates.min_price)
async def st_min_price(message: Message, state: FSMContext):
    db.ensure_user(message.chat.id)
    value = None if message.text.strip() == "-" else to_float(message.text.strip())
    db.set_field(message.chat.id, "min_price", value)
    await state.clear()
    await message.answer(settings_text(db.get_user(message.chat.id)), reply_markup=main_menu(message.chat.id))


@dp.message(InputStates.max_price)
async def st_max_price(message: Message, state: FSMContext):
    db.ensure_user(message.chat.id)
    value = None if message.text.strip() == "-" else to_float(message.text.strip())
    db.set_field(message.chat.id, "max_price", value)
    await state.clear()
    await message.answer(settings_text(db.get_user(message.chat.id)), reply_markup=main_menu(message.chat.id))


async def send_listing_to_user(chat_id: int, item: dict):
    buttons = []
    if item.get("market_url"):
        buttons.append([InlineKeyboardButton(text=f"Открыть на {item['provider']}", url=item["market_url"])])
    if item.get("tme_url"):
        buttons.append([InlineKeyboardButton(text="Открыть подарок", url=item["tme_url"])])
    markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None
    await bot.send_message(chat_id, listing_text(item), reply_markup=markup, disable_web_page_preview=False)


async def poll_mrkt(api: MrktApi):
    try:
        result = await api.fetch_saling(count=PAGE_LIMIT)
    except Exception as e:
        logger.exception("MRKT fetch failed: %s", e)
        return []
    gifts = result.get("gifts", []) or []
    out = []
    for raw in reversed(gifts):
        item = extract_from_mrkt(raw)
        if db.seen(item["uniq"]):
            continue
        item["avg_sell_ton"] = await calc_avg_sell_mrkt(api, item)
        out.append(item)
    return out


async def monitor_loop():
    if not BOT_TOKEN or not API_ID or not API_HASH:
        raise RuntimeError("Заполни BOT_TOKEN, API_ID, API_HASH в .env")
    db.init()
    logger.info("No paid API mode started. Interval=%s sec", CHECK_INTERVAL)
    async with MrktApi() as api:
        if STARTUP_TEST and OWNER_CHAT_ID:
            await bot.send_message(OWNER_CHAT_ID, "Бот запущен в режиме без платного API.")
        while True:
            subscribers = db.subscribers()
            if not subscribers:
                logger.info("Подписчиков пока нет")
            items = await poll_mrkt(api)
            if items:
                logger.info("Новых листингов: %s", len(items))
            for item in items:
                for user in subscribers:
                    if user_matches(user, item):
                        try:
                            await send_listing_to_user(int(user["chat_id"]), item)
                        except Exception as e:
                            logger.warning("send failed to %s: %s", user['chat_id'], e)
                db.mark_seen(item["uniq"])
            await asyncio.sleep(CHECK_INTERVAL)


async def main():
    monitor = asyncio.create_task(monitor_loop())
    await dp.start_polling(bot)
    await monitor


if __name__ == "__main__":
    asyncio.run(main())
