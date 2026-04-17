
import asyncio
import html
import logging
import os
import sqlite3
import time
from pathlib import Path
from typing import Any, Optional

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw.types import InputBotAppShortName, InputUser
from curl_cffi.requests import AsyncSession
from urllib.parse import unquote


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_CHAT_ID = int(os.getenv("OWNER_CHAT_ID", "0") or 0)

API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "mrkt_session").strip()

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "15") or 15)
DB_PATH = os.getenv("DB_PATH", "alerts.db").strip()

MRKT_BOT_USERNAME = os.getenv("MRKT_BOT_USERNAME", "mrkt").strip()
MRKT_APP_SHORT_NAME = os.getenv("MRKT_APP_SHORT_NAME", "app").strip()
MRKT_PLATFORM = os.getenv("MRKT_PLATFORM", "android").strip()
MRKT_STATIC_TOKEN = os.getenv("MRKT_STATIC_TOKEN", "").strip()

TOKEN_REFRESH_SECONDS = int(os.getenv("TOKEN_REFRESH_SECONDS", "3600") or 3600)

MARKET_API_URL = "https://api.tgmrkt.io/api/v1"
MRKT_CDN_REFERER = "https://cdn.tgmrkt.io/"

MARKETS = ["mrkt", "tonnel", "getgems", "portals"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("mrktbot")


class InputStates(StatesGroup):
    gift = State()
    model = State()
    backdrop = State()
    min_price = State()
    max_price = State()


class DB:
    def __init__(self, path: str):
        self.path = path

    def connect(self):
        return sqlite3.connect(self.path)

    def init(self):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS filters (
                chat_id INTEGER PRIMARY KEY,
                gift_name TEXT DEFAULT '',
                model_name TEXT DEFAULT '',
                backdrop_name TEXT DEFAULT '',
                min_price REAL,
                max_price REAL,
                markets TEXT DEFAULT 'mrkt',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS seen_items (
                unique_id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
            )
        """)
        conn.commit()
        conn.close()

    def upsert_user(self, chat_id: int):
        now = int(time.time())
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users(chat_id, is_active, created_at, updated_at)
            VALUES(?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET is_active=1, updated_at=excluded.updated_at
        """, (chat_id, now, now))
        cur.execute("""
            INSERT INTO filters(chat_id, created_at, updated_at)
            VALUES(?, ?, ?)
            ON CONFLICT(chat_id) DO NOTHING
        """, (chat_id, now, now))
        conn.commit()
        conn.close()

    def list_active_users(self) -> list[int]:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("SELECT chat_id FROM users WHERE is_active = 1")
        rows = [r[0] for r in cur.fetchall()]
        conn.close()
        return rows

    def get_filter(self, chat_id: int) -> dict:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("""
            SELECT gift_name, model_name, backdrop_name, min_price, max_price, markets
            FROM filters WHERE chat_id = ?
        """, (chat_id,))
        row = cur.fetchone()
        conn.close()
        if not row:
            now = int(time.time())
            conn = self.connect()
            cur = conn.cursor()
            cur.execute("""
                INSERT OR IGNORE INTO filters(chat_id, created_at, updated_at, markets)
                VALUES(?, ?, ?, 'mrkt')
            """, (chat_id, now, now))
            conn.commit()
            conn.close()
            return {
                "gift_name": "",
                "model_name": "",
                "backdrop_name": "",
                "min_price": None,
                "max_price": None,
                "markets": "mrkt",
            }
        return {
            "gift_name": row[0] or "",
            "model_name": row[1] or "",
            "backdrop_name": row[2] or "",
            "min_price": row[3],
            "max_price": row[4],
            "markets": row[5] or "mrkt",
        }

    def update_filter_value(self, chat_id: int, field: str, value: Any):
        now = int(time.time())
        conn = self.connect()
        cur = conn.cursor()
        cur.execute(f"""
            UPDATE filters SET {field} = ?, updated_at = ? WHERE chat_id = ?
        """, (value, now, chat_id))
        conn.commit()
        conn.close()

    def toggle_market(self, chat_id: int, market: str):
        data = self.get_filter(chat_id)
        markets = set([m for m in (data["markets"] or "").split(",") if m])
        if market in markets:
            markets.remove(market)
        else:
            markets.add(market)
        if not markets:
            markets.add("mrkt")
        self.update_filter_value(chat_id, "markets", ",".join(sorted(markets)))

    def reset_filters(self, chat_id: int):
        now = int(time.time())
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("""
            UPDATE filters
            SET gift_name='',
                model_name='',
                backdrop_name='',
                min_price=NULL,
                max_price=NULL,
                markets='mrkt',
                updated_at=?
            WHERE chat_id=?
        """, (now, chat_id))
        conn.commit()
        conn.close()

    def is_seen(self, unique_id: str) -> bool:
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM seen_items WHERE unique_id = ?", (unique_id,))
        row = cur.fetchone()
        conn.close()
        return row is not None

    def mark_seen(self, unique_id: str):
        conn = self.connect()
        cur = conn.cursor()
        cur.execute("""
            INSERT OR IGNORE INTO seen_items(unique_id, created_at) VALUES(?, ?)
        """, (unique_id, int(time.time())))
        conn.commit()
        conn.close()


db = DB(DB_PATH)


def esc(v: Any) -> str:
    return html.escape("" if v is None else str(v))


def fmt_price(v: Any) -> str:
    if v is None:
        return "—"
    return f"{float(v):.2f} TON"


def fmt_percent(v: Any) -> str:
    if v is None:
        return "—"
    try:
        x = float(v)
        if 0 < x < 1:
            x *= 100
        return f"{x:.2f}".rstrip("0").rstrip(".") + "%"
    except Exception:
        return "—"


def build_tme_url(gift_name: str, gift_number: Any) -> Optional[str]:
    if not gift_name or gift_number is None:
        return None
    slug = "".join(ch for ch in gift_name if ch.isalnum())
    return f"https://t.me/nft/{slug}-{gift_number}"


def extract_unique_id(item: dict) -> str:
    return str(item.get("saleId") or item.get("id") or f'{item.get("collectionName","gift")}-{item.get("giftNum","0")}-{item.get("price","0")}')

def extract_gift_name(item: dict) -> str:
    return item.get("collectionName") or item.get("name") or "Unknown Gift"

def extract_gift_number(item: dict) -> Any:
    return item.get("giftNum") or item.get("number") or "—"

def extract_price(item: dict) -> Optional[float]:
    price = item.get("priceTon")
    if price is None:
        price = item.get("price")
    try:
        if price is None:
            return None
        val = float(price)
        if val > 1_000_000:
            return val / 1_000_000_000
        return val
    except Exception:
        return None

def extract_model(item: dict) -> str:
    return item.get("modelName") or item.get("model") or "—"

def extract_model_percent(item: dict) -> Any:
    return item.get("modelRarityPercent") or item.get("model_rarity_percent") or item.get("modelRarity")

def extract_backdrop(item: dict) -> str:
    return item.get("backdropName") or item.get("backdrop") or "—"

def extract_backdrop_percent(item: dict) -> Any:
    return (
        item.get("backdropRarityPercent")
        or item.get("backdrop_rarity_percent")
        or item.get("backgroundRarityPercent")
        or item.get("background_rarity_percent")
        or item.get("backdropRarity")
    )

def extract_symbol(item: dict) -> str:
    return item.get("symbolName") or item.get("symbol") or "—"

def extract_symbol_percent(item: dict) -> Any:
    return item.get("symbolRarityPercent") or item.get("symbol_rarity_percent") or item.get("symbolRarity")


def parse_markets(value: str) -> list[str]:
    return [m for m in (value or "").split(",") if m]


def filter_matches(item: dict, f: dict) -> bool:
    gift_name = f["gift_name"].strip().lower()
    model_name = f["model_name"].strip().lower()
    backdrop_name = f["backdrop_name"].strip().lower()
    min_price = f["min_price"]
    max_price = f["max_price"]
    markets = set(parse_markets(f["markets"]))

    item_market = "mrkt"
    if markets and item_market not in markets:
        return False

    if gift_name and gift_name not in extract_gift_name(item).lower():
        return False
    if model_name and model_name not in extract_model(item).lower():
        return False
    if backdrop_name and backdrop_name not in extract_backdrop(item).lower():
        return False

    price = extract_price(item)
    if price is None:
        return False
    if min_price is not None and price < float(min_price):
        return False
    if max_price is not None and price > float(max_price):
        return False

    return True


def main_menu(chat_id: int) -> InlineKeyboardMarkup:
    f = db.get_filter(chat_id)
    markets = set(parse_markets(f["markets"]))
    rows = [
        [InlineKeyboardButton(text="🎁 Подарок", callback_data="set:gift"),
         InlineKeyboardButton(text="🧩 Модель", callback_data="set:model")],
        [InlineKeyboardButton(text="🖼 Фон", callback_data="set:backdrop"),
         InlineKeyboardButton(text="💰 Цена", callback_data="set:price")],
        [InlineKeyboardButton(text=f'{"✅" if "mrkt" in markets else "⬜"} MRKT', callback_data="market:mrkt"),
         InlineKeyboardButton(text=f'{"✅" if "tonnel" in markets else "⬜"} Tonnel', callback_data="market:tonnel")],
        [InlineKeyboardButton(text=f'{"✅" if "getgems" in markets else "⬜"} GetGems', callback_data="market:getgems"),
         InlineKeyboardButton(text=f'{"✅" if "portals" in markets else "⬜"} Portals', callback_data="market:portals")],
        [InlineKeyboardButton(text="📋 Мои фильтры", callback_data="show:filters"),
         InlineKeyboardButton(text="🧹 Сбросить", callback_data="reset:filters")],
    ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def filters_text(chat_id: int) -> str:
    f = db.get_filter(chat_id)
    return (
        "Текущие фильтры:\n\n"
        f"🎁 Подарок: {f['gift_name'] or '—'}\n"
        f"🧩 Модель: {f['model_name'] or '—'}\n"
        f"🖼 Фон: {f['backdrop_name'] or '—'}\n"
        f"💰 Мин. цена: {f['min_price'] if f['min_price'] is not None else '—'}\n"
        f"💰 Макс. цена: {f['max_price'] if f['max_price'] is not None else '—'}\n"
        f"🏪 Маркеты: {', '.join(parse_markets(f['markets'])) or 'mrkt'}"
    )


class MrktApi:
    def __init__(self):
        self.token: Optional[str] = MRKT_STATIC_TOKEN or None
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
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
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
            raise RuntimeError(f"tgWebAppData not found: {url}")
        init_data = unquote(url.split("tgWebAppData=", 1)[1].split("&tgWebAppVersion", 1)[0])
        if not init_data:
            raise RuntimeError("init_data empty")
        return init_data

    async def refresh_token(self):
        assert self.http is not None
        init_data = await self.get_init_data()
        resp = await self.http.post(f"{MARKET_API_URL}/auth", json={"data": init_data})
        resp.raise_for_status()
        data = resp.json()
        token = data.get("token") if isinstance(data, dict) else None
        if not token:
            raise RuntimeError(f"MRKT auth failed: {data}")
        self.token = token
        self.token_received_at = time.time()
        logger.info("MRKT token refreshed")

    async def ensure_token(self):
        if self.token and (time.time() - self.token_received_at) < TOKEN_REFRESH_SECONDS:
            return
        await self.refresh_token()

    async def post(self, path: str, payload: dict) -> dict:
        assert self.http is not None
        await self.ensure_token()
        headers = {
            "Authorization": self.token or "",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "Origin": "https://cdn.tgmrkt.io",
            "Referer": MRKT_CDN_REFERER,
        }
        resp = await self.http.post(f"{MARKET_API_URL}{path}", headers=headers, json=payload)
        if resp.status_code == 401:
            await self.refresh_token()
            headers["Authorization"] = self.token or ""
            resp = await self.http.post(f"{MARKET_API_URL}{path}", headers=headers, json=payload)
        if resp.status_code >= 400:
            body = getattr(resp, "text", "")
            raise RuntimeError(f"MRKT HTTP {resp.status_code} | path={path} | payload={payload} | body={body}")
        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"bad response: {data}")
        return data

    async def fetch_saling(self, count: int = 20) -> list[dict]:
        payload = {
            "collectionNames": [],
            "modelNames": [],
            "backdropNames": [],
            "symbolNames": [],
            "count": count,
            "cursor": "",
            "promotedFirst": False,
            "lowToHigh": False,
            "ordering": "Date"
        }
        try:
            data = await self.post("/gifts/saling", payload)
        except Exception:
            payload["ordering"] = "Price"
            payload["lowToHigh"] = True
            data = await self.post("/gifts/saling", payload)
        gifts = data.get("gifts", [])
        return gifts if isinstance(gifts, list) else []


async def send_item(bot: Bot, chat_id: int, item: dict):
    gift_name = extract_gift_name(item)
    gift_number = extract_gift_number(item)
    price = extract_price(item)
    model = extract_model(item)
    model_p = extract_model_percent(item)
    backdrop = extract_backdrop(item)
    backdrop_p = extract_backdrop_percent(item)
    symbol = extract_symbol(item)
    symbol_p = extract_symbol_percent(item)
    tme = build_tme_url(gift_name, gift_number)

    text = (
        f"<b>{esc(gift_name)} #{esc(gift_number)}</b>\n\n"
        f"- Model: {esc(model)} ({fmt_percent(model_p)})\n"
        f"- Symbol: {esc(symbol)} ({fmt_percent(symbol_p)})\n"
        f"- Backdrop: {esc(backdrop)} ({fmt_percent(backdrop_p)})\n\n"
        f"🪙 Price: {fmt_price(price)}\n"
        f"📉 Avg buy: —\n"
        f"📈 Avg sell: —\n\n"
        f"{esc(tme) if tme else ''}"
    )
    kb = []
    if tme:
        kb.append([InlineKeyboardButton(text="Открыть подарок", url=tme)])
    markup = InlineKeyboardMarkup(inline_keyboard=kb) if kb else None
    await bot.send_message(chat_id, text, reply_markup=markup, disable_web_page_preview=False)


async def monitor_loop(bot: Bot):
    if OWNER_CHAT_ID:
        db.upsert_user(OWNER_CHAT_ID)

    async with MrktApi() as api:
        logger.info("MRKT monitor started. Interval=%s sec", CHECK_INTERVAL)
        while True:
            try:
                users = db.list_active_users()
                if not users:
                    logger.info("Подписчиков пока нет")
                    await asyncio.sleep(CHECK_INTERVAL)
                    continue

                gifts = await api.fetch_saling(count=20)
                logger.info("Получено подарков: %s", len(gifts))
                for item in reversed(gifts):
                    uid = extract_unique_id(item)
                    if db.is_seen(uid):
                        continue
                    db.mark_seen(uid)

                    for chat_id in users:
                        f = db.get_filter(chat_id)
                        if filter_matches(item, f):
                            try:
                                await send_item(bot, chat_id, item)
                            except Exception as e:
                                logger.warning("Не удалось отправить %s: %s", chat_id, e)

            except Exception as e:
                logger.exception("Ошибка мониторинга: %s", e)

            await asyncio.sleep(CHECK_INTERVAL)


router = Router()


@router.message(Command("start"))
async def cmd_start(message: Message):
    db.upsert_user(message.chat.id)
    text = (
        "Это бот для уведомлений о выходах подарков.\n\n"
        "Фильтры настраиваются через меню ниже:\n"
        "— подарок\n"
        "— модель\n"
        "— фон\n"
        "— цена\n"
        "— маркеты\n\n"
        "Сейчас реально активен источник MRKT.\n"
        "Tonnel / GetGems / Portals уже добавлены в меню как фильтры-интерфейс, "
        "но без стабильных публичных эндпоинтов я не могу честно обещать рабочий парсинг всех этих маркетов в этой версии."
    )
    await message.answer(text, reply_markup=main_menu(message.chat.id))


@router.message(Command("menu"))
async def cmd_menu(message: Message):
    db.upsert_user(message.chat.id)
    await message.answer(filters_text(message.chat.id), reply_markup=main_menu(message.chat.id))


@router.callback_query(F.data == "show:filters")
async def cb_show_filters(call: CallbackQuery):
    await call.message.edit_text(filters_text(call.message.chat.id), reply_markup=main_menu(call.message.chat.id))
    await call.answer()


@router.callback_query(F.data == "reset:filters")
async def cb_reset_filters(call: CallbackQuery):
    db.reset_filters(call.message.chat.id)
    await call.message.edit_text("Фильтры сброшены.\n\n" + filters_text(call.message.chat.id), reply_markup=main_menu(call.message.chat.id))
    await call.answer("Сброшено")


@router.callback_query(F.data.startswith("market:"))
async def cb_market(call: CallbackQuery):
    market = call.data.split(":", 1)[1]
    db.toggle_market(call.message.chat.id, market)
    await call.message.edit_text(filters_text(call.message.chat.id), reply_markup=main_menu(call.message.chat.id))
    await call.answer("Обновлено")


@router.callback_query(F.data == "set:gift")
async def cb_set_gift(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.gift)
    await call.message.answer("Введи подарок вручную.\nПример: Jester Hat")
    await call.answer()

@router.callback_query(F.data == "set:model")
async def cb_set_model(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.model)
    await call.message.answer("Введи модель вручную.\nПример: Lava Viper")
    await call.answer()

@router.callback_query(F.data == "set:backdrop")
async def cb_set_backdrop(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.backdrop)
    await call.message.answer("Введи фон вручную.\nПример: Copper")
    await call.answer()

@router.callback_query(F.data == "set:price")
async def cb_set_price(call: CallbackQuery):
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Мин. цена", callback_data="set:min_price"),
         InlineKeyboardButton(text="Макс. цена", callback_data="set:max_price")]
    ])
    await call.message.answer("Что хочешь ввести?", reply_markup=kb)
    await call.answer()

@router.callback_query(F.data == "set:min_price")
async def cb_set_min_price(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.min_price)
    await call.message.answer("Введи минимальную цену в TON. Пример: 2.5\nНапиши 0 чтобы убрать.")
    await call.answer()

@router.callback_query(F.data == "set:max_price")
async def cb_set_max_price(call: CallbackQuery, state: FSMContext):
    await state.set_state(InputStates.max_price)
    await call.message.answer("Введи максимальную цену в TON. Пример: 10\nНапиши 0 чтобы убрать.")
    await call.answer()

@router.message(InputStates.gift)
async def state_gift(message: Message, state: FSMContext):
    db.update_filter_value(message.chat.id, "gift_name", message.text.strip())
    await state.clear()
    await message.answer("Фильтр по подарку обновлён.\n\n" + filters_text(message.chat.id), reply_markup=main_menu(message.chat.id))

@router.message(InputStates.model)
async def state_model(message: Message, state: FSMContext):
    db.update_filter_value(message.chat.id, "model_name", message.text.strip())
    await state.clear()
    await message.answer("Фильтр по модели обновлён.\n\n" + filters_text(message.chat.id), reply_markup=main_menu(message.chat.id))

@router.message(InputStates.backdrop)
async def state_backdrop(message: Message, state: FSMContext):
    db.update_filter_value(message.chat.id, "backdrop_name", message.text.strip())
    await state.clear()
    await message.answer("Фильтр по фону обновлён.\n\n" + filters_text(message.chat.id), reply_markup=main_menu(message.chat.id))

@router.message(InputStates.min_price)
async def state_min_price(message: Message, state: FSMContext):
    text = message.text.strip().replace(",", ".")
    val = None if text == "0" else float(text)
    db.update_filter_value(message.chat.id, "min_price", val)
    await state.clear()
    await message.answer("Минимальная цена обновлена.\n\n" + filters_text(message.chat.id), reply_markup=main_menu(message.chat.id))

@router.message(InputStates.max_price)
async def state_max_price(message: Message, state: FSMContext):
    text = message.text.strip().replace(",", ".")
    val = None if text == "0" else float(text)
    db.update_filter_value(message.chat.id, "max_price", val)
    await state.clear()
    await message.answer("Максимальная цена обновлена.\n\n" + filters_text(message.chat.id), reply_markup=main_menu(message.chat.id))


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN пустой")
    if not API_ID or not API_HASH:
        raise RuntimeError("API_ID/API_HASH пустые")
    db.init()

    bot = Bot(BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    monitor = asyncio.create_task(monitor_loop(bot))
    try:
        await dp.start_polling(bot)
    finally:
        monitor.cancel()
        with contextlib.suppress(Exception):
            await monitor


if __name__ == "__main__":
    import contextlib
    asyncio.run(main())
