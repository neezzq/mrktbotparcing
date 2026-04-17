import asyncio
import html
import logging
import os
import re
import sqlite3
import time
from statistics import mean
from typing import Any, Optional
from urllib.parse import unquote

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw.types import InputBotAppShortName, InputUser
from curl_cffi.requests import AsyncSession

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = int(os.getenv("CHAT_ID", "0"))

API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "mrkt_session").strip()

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "15"))
DB_PATH = os.getenv("DB_PATH", "alerts.db").strip()

MAX_PRICE_TON = float(os.getenv("MAX_PRICE_TON", "999999"))
POLL_COUNT = int(os.getenv("POLL_COUNT", "20"))
AVG_SAMPLE_SIZE = int(os.getenv("AVG_SAMPLE_SIZE", "10"))
TOKEN_REFRESH_SECONDS = int(os.getenv("TOKEN_REFRESH_SECONDS", "3600"))

FILTER_COLLECTIONS = [x.strip() for x in os.getenv("FILTER_COLLECTIONS", "").split(",") if x.strip()]
FILTER_MODELS = [x.strip() for x in os.getenv("FILTER_MODELS", "").split(",") if x.strip()]
FILTER_BACKDROPS = [x.strip() for x in os.getenv("FILTER_BACKDROPS", "").split(",") if x.strip()]
FILTER_SYMBOLS = [x.strip() for x in os.getenv("FILTER_SYMBOLS", "").split(",") if x.strip()]

MRKT_BOT_USERNAME = os.getenv("MRKT_BOT_USERNAME", "mrkt").strip()
MRKT_APP_SHORT_NAME = os.getenv("MRKT_APP_SHORT_NAME", "app").strip()
MRKT_PLATFORM = os.getenv("MRKT_PLATFORM", "android").strip()
MRKT_STATIC_TOKEN = os.getenv("MRKT_STATIC_TOKEN", "").strip()

MARKET_API_URL = "https://api.tgmrkt.io/api/v1"
MRKT_CDN_REFERER = "https://cdn.tgmrkt.io/"

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("mrktbot")


def require_env() -> None:
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not CHAT_ID:
        missing.append("CHAT_ID")
    if not API_ID:
        missing.append("API_ID")
    if not API_HASH:
        missing.append("API_HASH")
    if missing:
        raise RuntimeError(f"Не заполнены переменные: {', '.join(missing)}")


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_items (
            unique_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def is_seen(unique_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM seen_items WHERE unique_id = ?", (unique_id,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_seen(unique_id: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO seen_items(unique_id, created_at) VALUES(?, ?)",
        (unique_id, int(time.time())),
    )
    conn.commit()
    conn.close()


def safe_get(obj: Any, *keys: str, default=None):
    if obj is None:
        return default
    for key in keys:
        if isinstance(obj, dict) and key in obj:
            val = obj.get(key)
            if val is not None:
                return val
        if hasattr(obj, key):
            val = getattr(obj, key)
            if val is not None:
                return val
    return default


def to_float(value: Any) -> Optional[float]:
    if value is None:
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
    text = f"{x:.2f}".rstrip("0").rstrip(".")
    return f"{text}%"


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def slugify_gift_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", name or "")


def build_tme_url(gift_name: str, gift_number: Any) -> Optional[str]:
    if not gift_name or gift_number is None:
        return None
    slug = slugify_gift_name(gift_name)
    return f"https://t.me/nft/{slug}-{gift_number}"


def first_not_none(*values):
    for v in values:
        if v is not None:
            return v
    return None


def extract_unique_id(item: dict) -> str:
    return str(
        first_not_none(
            safe_get(item, "saleId", "sale_id", "id"),
            f"{safe_get(item, 'giftNum', 'gift_num', 'number', default='unknown')}-"
            f"{safe_get(item, 'collectionName', 'collection_name', 'name', default='gift')}-"
            f"{safe_get(item, 'price', 'salePrice', 'sale_price', 'salePriceTon', 'sale_price_ton', default='noprice')}",
        )
    )


def extract_gift_name(item: dict) -> str:
    return str(first_not_none(safe_get(item, "collectionName", "collection_name", "name", "giftName", "gift_name"), "Unknown Gift"))


def extract_gift_number(item: dict) -> Any:
    return first_not_none(safe_get(item, "giftNum", "gift_num", "number"), "—")


def extract_price_ton(item: dict) -> Optional[float]:
    return to_ton(first_not_none(safe_get(item, "priceTon", "price_ton", "salePriceTon", "sale_price_ton"), safe_get(item, "price", "salePrice", "sale_price")))


def extract_model_name(item: dict) -> str:
    return str(first_not_none(safe_get(item, "modelName", "model_name", "model"), "Unknown"))


def extract_model_percent(item: dict) -> Any:
    return first_not_none(safe_get(item, "modelRarityPercent", "model_rarity_percent"), safe_get(item, "modelRarity", "model_rarity"))


def extract_backdrop_name(item: dict) -> str:
    return str(first_not_none(safe_get(item, "backdropName", "backdrop_name", "backdrop"), "Unknown"))


def extract_backdrop_percent(item: dict) -> Any:
    return first_not_none(
        safe_get(item, "backdropRarityPercent", "backdrop_rarity_percent"),
        safe_get(item, "backgroundRarityPercent", "background_rarity_percent"),
        safe_get(item, "backdropRarity", "backdrop_rarity", "backgroundRarity", "background_rarity"),
    )


def extract_symbol_name(item: dict) -> str:
    return str(first_not_none(safe_get(item, "symbolName", "symbol_name", "symbol"), "Unknown"))


def extract_symbol_percent(item: dict) -> Any:
    return first_not_none(safe_get(item, "symbolRarityPercent", "symbol_rarity_percent"), safe_get(item, "symbolRarity", "symbol_rarity"))


def extract_market_url(item: dict) -> Optional[str]:
    return first_not_none(safe_get(item, "url", "saleUrl", "sale_url", "marketUrl", "market_url"), None)


def item_matches_filters(item: dict) -> bool:
    collection_name = extract_gift_name(item)
    model_name = extract_model_name(item)
    backdrop_name = extract_backdrop_name(item)
    symbol_name = extract_symbol_name(item)
    if FILTER_COLLECTIONS and collection_name not in FILTER_COLLECTIONS:
        return False
    if FILTER_MODELS and model_name not in FILTER_MODELS:
        return False
    if FILTER_BACKDROPS and backdrop_name not in FILTER_BACKDROPS:
        return False
    if FILTER_SYMBOLS and symbol_name not in FILTER_SYMBOLS:
        return False
    return True


class MrktApi:
    def __init__(self) -> None:
        self.token: Optional[str] = MRKT_STATIC_TOKEN or None
        self.token_received_at: float = 0.0
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
        self.tg = Client(SESSION_NAME, API_ID, API_HASH)
        await self.tg.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self.tg:
            await self.tg.stop()
        if self.http:
            await self.http.close()

    async def ensure_token(self) -> None:
        if self.token and (time.time() - self.token_received_at) < TOKEN_REFRESH_SECONDS:
            return
        await self.refresh_token()

    async def get_init_data(self) -> str:
        assert self.tg is not None

        bot_peer = await self.tg.resolve_peer(MRKT_BOT_USERNAME)
        bot = InputUser(user_id=bot_peer.user_id, access_hash=bot_peer.access_hash)
        peer = bot_peer
        bot_app = InputBotAppShortName(bot_id=bot, short_name=MRKT_APP_SHORT_NAME)

        web_view = await self.tg.invoke(
            RequestAppWebView(
                peer=peer,
                app=bot_app,
                platform=MRKT_PLATFORM,
            )
        )

        url = web_view.url
        if "tgWebAppData=" not in url:
            raise RuntimeError(f"tgWebAppData not found: {url}")

        init_data = unquote(url.split("tgWebAppData=", 1)[1].split("&tgWebAppVersion", 1)[0])
        if not init_data:
            raise RuntimeError("init_data empty")
        return init_data

    async def refresh_token(self) -> None:
        assert self.http is not None
        init_data = await self.get_init_data()
        payload = {"data": init_data}
        resp = await self.http.post(f"{MARKET_API_URL}/auth", json=payload)
        resp.raise_for_status()
        data = resp.json()
        token = data.get("token") if isinstance(data, dict) else None
        if not token:
            raise RuntimeError(f"MRKT auth не вернул token. Ответ: {data}")
        self.token = token
        self.token_received_at = time.time()
        logger.info("MRKT token refreshed")

    async def post(self, path: str, json_data: dict) -> dict:
        assert self.http is not None
        await self.ensure_token()

        headers = {
            "Referer": MRKT_CDN_REFERER,
            "Origin": "https://cdn.tgmrkt.io",
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
        }
        if self.token:
            headers["Authorization"] = self.token

        resp = await self.http.post(f"{MARKET_API_URL}{path}", headers=headers, json=json_data)

        if resp.status_code == 401:
            logger.warning("MRKT token устарел, обновляю и повторяю запрос")
            await self.refresh_token()
            if self.token:
                headers["Authorization"] = self.token
            resp = await self.http.post(f"{MARKET_API_URL}{path}", headers=headers, json=json_data)

        if resp.status_code >= 400:
            try:
                body = resp.text
            except Exception:
                body = "<no body>"
            raise RuntimeError(f"MRKT HTTP {resp.status_code} | path={path} | payload={json_data} | body={body}")

        data = resp.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"Некорректный ответ MRKT: {data}")
        return data

    async def fetch_saling(
        self,
        collection_names: Optional[list[str]] = None,
        model_names: Optional[list[str]] = None,
        backdrop_names: Optional[list[str]] = None,
        symbol_names: Optional[list[str]] = None,
        ordering: Optional[str] = None,
        low_to_high: bool = False,
        max_price: Optional[float] = None,
        min_price: Optional[float] = None,
        count: int = 20,
        cursor: Optional[str] = None,
    ) -> dict:
        payload = {"count": min(max(count, 1), 20), "lowToHigh": low_to_high}
        if cursor:
            payload["cursor"] = cursor
        if collection_names:
            payload["collectionNames"] = collection_names
        if model_names:
            payload["modelNames"] = model_names
        if backdrop_names:
            payload["backdropNames"] = backdrop_names
        if symbol_names:
            payload["symbolNames"] = symbol_names
        if ordering:
            payload["ordering"] = ordering
        if max_price is not None:
            payload["maxPrice"] = max_price
        if min_price is not None:
            payload["minPrice"] = min_price
        logger.info("MRKT /gifts/saling payload: %s", payload)
        return await self.post("/gifts/saling", payload)


async def calc_avg_sell(api: MrktApi, item: dict) -> Optional[float]:
    try:
        collection_name = extract_gift_name(item)
        model_name = extract_model_name(item)
        backdrop_name = extract_backdrop_name(item)
        symbol_name = extract_symbol_name(item)
        result = await api.fetch_saling(
            collection_names=[collection_name] if collection_name else None,
            model_names=[model_name] if model_name and model_name != "Unknown" else None,
            backdrop_names=[backdrop_name] if backdrop_name and backdrop_name != "Unknown" else None,
            symbol_names=[symbol_name] if symbol_name and symbol_name != "Unknown" else None,
            ordering="Price",
            low_to_high=True,
            count=min(AVG_SAMPLE_SIZE, 20),
        )
        gifts = result.get("gifts", []) or []
        prices = [extract_price_ton(x) for x in gifts]
        prices = [x for x in prices if x is not None]
        return round(mean(prices), 4) if prices else None
    except Exception as e:
        logger.warning("Не удалось посчитать avg sell: %s", e)
        return None


async def calc_avg_buy(api: MrktApi, item: dict, avg_sell: Optional[float]) -> Optional[float]:
    for key in [
        "avgBuyPriceTon",
        "avg_buy_price_ton",
        "averageBuyPriceTon",
        "average_buy_price_ton",
        "avgBuyTon",
        "avg_buy_ton",
    ]:
        ton = to_ton(safe_get(item, key))
        if ton is not None:
            return round(ton, 4)
    return None


def build_message(item: dict, avg_buy: Optional[float], avg_sell: Optional[float]) -> str:
    gift_name = extract_gift_name(item)
    gift_number = extract_gift_number(item)
    price_ton = extract_price_ton(item)
    model_name = extract_model_name(item)
    model_percent = extract_model_percent(item)
    symbol_name = extract_symbol_name(item)
    symbol_percent = extract_symbol_percent(item)
    backdrop_name = extract_backdrop_name(item)
    backdrop_percent = extract_backdrop_percent(item)
    tme_url = build_tme_url(gift_name, gift_number)

    text = (
        f"<b>{esc(gift_name)} #{esc(gift_number)}</b>\n\n"
        f"- Model: {esc(model_name)} ({fmt_percent(model_percent)})\n"
        f"- Symbol: {esc(symbol_name)} ({fmt_percent(symbol_percent)})\n"
        f"- Backdrop: {esc(backdrop_name)} ({fmt_percent(backdrop_percent)})\n\n"
        f"🪙 Price: {fmt_ton(price_ton)}\n"
        f"📉 Avg buy: {fmt_ton(avg_buy)}\n"
        f"📈 Avg sell: {fmt_ton(avg_sell)}"
    )
    if tme_url:
        text += f"\n\n{esc(tme_url)}"
    return text


async def send_alert(bot: Bot, item: dict, avg_buy: Optional[float], avg_sell: Optional[float]) -> None:
    gift_name = extract_gift_name(item)
    gift_number = extract_gift_number(item)
    tme_url = build_tme_url(gift_name, gift_number)
    market_url = extract_market_url(item)

    buttons = []
    if market_url:
        buttons.append([InlineKeyboardButton(text="Открыть на MRKT", url=str(market_url))])
    if tme_url:
        buttons.append([InlineKeyboardButton(text="Открыть подарок", url=tme_url)])
    markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

    await bot.send_message(
        chat_id=CHAT_ID,
        text=build_message(item, avg_buy, avg_sell),
        parse_mode="HTML",
        disable_web_page_preview=False,
        reply_markup=markup,
    )


async def process_market_page(bot: Bot, api: MrktApi) -> None:
    result = await api.fetch_saling(
        collection_names=FILTER_COLLECTIONS or None,
        model_names=FILTER_MODELS or None,
        backdrop_names=FILTER_BACKDROPS or None,
        symbol_names=FILTER_SYMBOLS or None,
        low_to_high=False,
        max_price=MAX_PRICE_TON if MAX_PRICE_TON < 999999 else None,
        count=POLL_COUNT,
    )

    gifts = result.get("gifts", []) or []
    logger.info("Получено подарков: %s", len(gifts))

    for item in reversed(gifts):
        try:
            if not item_matches_filters(item):
                continue
            unique_id = extract_unique_id(item)
            if is_seen(unique_id):
                continue
            price_ton = extract_price_ton(item)
            if price_ton is None:
                logger.warning("Пропускаю листинг без цены: %s", item)
                continue
            if price_ton > MAX_PRICE_TON:
                continue
            avg_sell = await calc_avg_sell(api, item)
            avg_buy = await calc_avg_buy(api, item, avg_sell)
            await send_alert(bot, item, avg_buy, avg_sell)
            mark_seen(unique_id)
            logger.info("Отправлено: %s #%s | %s", extract_gift_name(item), extract_gift_number(item), fmt_ton(price_ton))
        except Exception as e:
            logger.exception("Ошибка обработки листинга: %s", e)


async def monitor_loop() -> None:
    require_env()
    init_db()
    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    async with MrktApi() as api:
        logger.info("MRKT bot started. Interval=%s sec", CHECK_INTERVAL)
        while True:
            try:
                await process_market_page(bot, api)
            except Exception as e:
                logger.exception("Ошибка основного цикла: %s", e)
            await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(monitor_loop())
