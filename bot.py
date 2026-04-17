import asyncio
import html
import logging
import os
import re
import sqlite3
import time
from statistics import mean
from typing import Any, Optional

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv

# pip install amrkt aiogram python-dotenv
from amrkt import MarketClient


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CHAT_ID = int(os.getenv("CHAT_ID", "0"))
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "mrkt_session").strip()

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "15"))
DB_PATH = os.getenv("DB_PATH", "alerts.db").strip()
MAX_PRICE_TON = float(os.getenv("MAX_PRICE_TON", "999999"))
AVG_SAMPLE_SIZE = int(os.getenv("AVG_SAMPLE_SIZE", "20"))

# Фильтры (можно оставить пустыми)
FILTER_COLLECTIONS = [x.strip() for x in os.getenv("FILTER_COLLECTIONS", "").split(",") if x.strip()]
FILTER_MODELS = [x.strip() for x in os.getenv("FILTER_MODELS", "").split(",") if x.strip()]
FILTER_BACKDROPS = [x.strip() for x in os.getenv("FILTER_BACKDROPS", "").split(",") if x.strip()]
FILTER_SYMBOLS = [x.strip() for x in os.getenv("FILTER_SYMBOLS", "").split(",") if x.strip()]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("mrkt-bot")


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
        raise RuntimeError(f"Не заполнены переменные окружения: {', '.join(missing)}")


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS seen_items (
            item_id TEXT PRIMARY KEY,
            created_at INTEGER NOT NULL
        )
    """)
    conn.commit()
    conn.close()


def is_seen(item_id: str) -> bool:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM seen_items WHERE item_id = ?", (item_id,))
    row = cur.fetchone()
    conn.close()
    return row is not None


def mark_seen(item_id: str) -> None:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO seen_items(item_id, created_at) VALUES(?, ?)",
        (item_id, int(time.time()))
    )
    conn.commit()
    conn.close()


def slugify_gift_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", name or "")


def safe_get(obj: Any, *names: str, default=None):
    for name in names:
        if isinstance(obj, dict) and name in obj:
            value = obj.get(name)
            if value is not None:
                return value
        if hasattr(obj, name):
            value = getattr(obj, name)
            if value is not None:
                return value
    return default


def to_ton(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        value = float(value)
    except Exception:
        return None

    # если вдруг число пришло в nanoTON
    if value > 1_000_000:
        return value / 1_000_000_000
    return value


def fmt_percent(value: Any) -> str:
    if value is None:
        return "—"
    try:
        x = float(value)
        text = f"{x:.2f}".rstrip("0").rstrip(".")
        return f"{text}%"
    except Exception:
        return "—"


def fmt_ton(value: Any) -> str:
    val = to_ton(value)
    if val is None:
        return "—"
    return f"{val:.2f} TON"


def escape(s: Any) -> str:
    return html.escape("" if s is None else str(s))


def build_tme_url(gift_title: str, gift_number: Any) -> Optional[str]:
    if not gift_title or gift_number is None:
        return None
    slug = slugify_gift_name(gift_title)
    return f"https://t.me/nft/{slug}-{gift_number}"


def item_unique_id(feed_item: Any, gift: Any) -> Optional[str]:
    return str(
        safe_get(feed_item, "id")
        or safe_get(feed_item, "sale_id")
        or safe_get(gift, "id")
        or safe_get(gift, "gift_id")
        or ""
    ) or None


def gift_matches_filters(gift: Any) -> bool:
    collection_name = str(safe_get(gift, "collection_name", "collectionName", "title", default="") or "")
    model_name = str(safe_get(gift, "model_name", "modelName", default="") or "")
    backdrop_name = str(safe_get(gift, "backdrop_name", "backdropName", default="") or "")
    symbol_name = str(safe_get(gift, "symbol_name", "symbolName", default="") or "")

    if FILTER_COLLECTIONS and collection_name not in FILTER_COLLECTIONS:
        return False
    if FILTER_MODELS and model_name not in FILTER_MODELS:
        return False
    if FILTER_BACKDROPS and backdrop_name not in FILTER_BACKDROPS:
        return False
    if FILTER_SYMBOLS and symbol_name not in FILTER_SYMBOLS:
        return False
    return True


async def get_listing_details(client: MarketClient, gift_id: str) -> Any:
    try:
        return await client.get_gift(gift_id)
    except Exception as e:
        logger.warning("Не удалось получить детали gift_id=%s: %s", gift_id, e)
        return None


async def calc_avg_sell(client: MarketClient, gift: Any) -> Optional[float]:
    """
    Средняя цена продажи = среднее по текущим активным листингам похожих подарков.
    Это нормальная practical-метрика для алерта.
    """
    try:
        collection_name = safe_get(gift, "collection_name", "collectionName")
        if not collection_name:
            return None

        result = await client.search_gifts(
            collection_names=[collection_name],
            count=min(AVG_SAMPLE_SIZE, 20),
            low_to_high=True,
            ordering="Price"
        )

        items = safe_get(result, "items", default=[]) or []
        prices = []

        for it in items:
            p = to_ton(safe_get(it, "sale_price_ton", "price_ton", "sale_price", "price"))
            if p is not None:
                prices.append(p)

        if not prices:
            return None

        return round(mean(prices), 4)
    except Exception as e:
        logger.warning("Не удалось посчитать avg sell: %s", e)
        return None


async def calc_avg_buy(client: MarketClient, gift: Any) -> Optional[float]:
    """
    Средняя цена покупки = среднее по recent sale событиям из feed.
    Это best-effort, потому что публично найденная документация не дает
    отдельного стабильного history endpoint для точного расчета.
    """
    try:
        collection_name = str(safe_get(gift, "collection_name", "collectionName", default="") or "")
        if not collection_name:
            return None

        feed = await client.get_feed()
        items = safe_get(feed, "items", default=[]) or []

        prices = []
        for row in items:
            row_type = str(safe_get(row, "type", default="") or "").lower()
            if row_type != "sale":
                continue

            row_gift = safe_get(row, "gift", default=None)
            if not row_gift:
                continue

            row_collection = str(
                safe_get(row_gift, "collection_name", "collectionName", "title", default="") or ""
            )
            if row_collection != collection_name:
                continue

            amount = to_ton(safe_get(row, "amount_ton", "amount", "price_ton", "price"))
            if amount is not None:
                prices.append(amount)

            if len(prices) >= AVG_SAMPLE_SIZE:
                break

        if not prices:
            return None

        return round(mean(prices), 4)
    except Exception as e:
        logger.warning("Не удалось посчитать avg buy: %s", e)
        return None


def build_message(gift: Any, price_ton: Optional[float], avg_buy: Optional[float], avg_sell: Optional[float]) -> str:
    gift_title = safe_get(gift, "name", "title", default="Unknown Gift")
    gift_number = safe_get(gift, "number", default="—")

    model_name = safe_get(gift, "model_name", "modelName", default="Unknown")
    model_percent = safe_get(gift, "model_rarity_percent", "modelRarityPercent", default=None)

    backdrop_name = safe_get(gift, "backdrop_name", "backdropName", default="Unknown")
    backdrop_percent = safe_get(gift, "backdrop_rarity_percent", "backdropRarityPercent", default=None)

    symbol_name = safe_get(gift, "symbol_name", "symbolName", default="Unknown")
    symbol_percent = safe_get(gift, "symbol_rarity_percent", "symbolRarityPercent", default=None)

    tme_url = build_tme_url(gift_title, gift_number)

    text = (
        f"<b>{escape(gift_title)} #{escape(gift_number)}</b>\n\n"
        f"- <b>Model:</b> {escape(model_name)} ({fmt_percent(model_percent)})\n"
        f"- <b>Symbol:</b> {escape(symbol_name)} ({fmt_percent(symbol_percent)})\n"
        f"- <b>Backdrop:</b> {escape(backdrop_name)} ({fmt_percent(backdrop_percent)})\n\n"
        f"🪙 <b>Price:</b> {fmt_ton(price_ton)}\n"
        f"📉 <b>Avg buy:</b> {fmt_ton(avg_buy)}\n"
        f"📈 <b>Avg sell:</b> {fmt_ton(avg_sell)}"
    )

    if tme_url:
        text += f"\n\n{escape(tme_url)}"

    return text


async def send_alert(bot: Bot, gift: Any, price_ton: Optional[float], avg_buy: Optional[float], avg_sell: Optional[float]) -> None:
    gift_title = safe_get(gift, "name", "title", default="Unknown Gift")
    gift_number = safe_get(gift, "number", default="—")
    tme_url = build_tme_url(gift_title, gift_number)

    market_url = (
        safe_get(gift, "url")
        or safe_get(gift, "market_url")
        or safe_get(gift, "sale_url")
    )

    buttons = []
    if market_url:
        buttons.append([InlineKeyboardButton(text="Открыть на MRKT", url=str(market_url))])
    if tme_url:
        buttons.append([InlineKeyboardButton(text="Открыть подарок", url=tme_url)])

    markup = InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

    await bot.send_message(
        chat_id=CHAT_ID,
        text=build_message(gift, price_ton, avg_buy, avg_sell),
        parse_mode="HTML",
        disable_web_page_preview=False,
        reply_markup=markup
    )


async def process_feed_item(bot: Bot, client: MarketClient, feed_item: Any) -> None:
    row_type = str(safe_get(feed_item, "type", default="") or "").lower()
    if row_type != "listing":
        return

    row_gift = safe_get(feed_item, "gift", default=None)
    if not row_gift:
        return

    gift_id = str(safe_get(row_gift, "id", "gift_id", default="") or "")
    if not gift_id:
        logger.warning("У listing нет gift_id, пропускаю")
        return

    gift = await get_listing_details(client, gift_id)
    if gift is None:
        gift = row_gift

    if not gift_matches_filters(gift):
        return

    uid = item_unique_id(feed_item, gift)
    if not uid:
        uid = f"gift:{gift_id}:listing"

    if is_seen(uid):
        return

    price_ton = to_ton(
        safe_get(
            gift,
            "sale_price_ton",
            "price_ton",
            "sale_price",
            "price",
            default=safe_get(feed_item, "amount_ton", "amount", default=None),
        )
    )

    if price_ton is None:
        logger.warning("Не удалось определить цену для gift_id=%s", gift_id)
        return

    if price_ton > MAX_PRICE_TON:
        return

    avg_sell = await calc_avg_sell(client, gift)
    avg_buy = await calc_avg_buy(client, gift)

    await send_alert(bot, gift, price_ton, avg_buy, avg_sell)
    mark_seen(uid)

    logger.info(
        "Отправлено: %s #%s | %.2f TON",
        safe_get(gift, "name", "title", default="Unknown"),
        safe_get(gift, "number", default="—"),
        price_ton,
    )


async def monitor_loop() -> None:
    require_env()
    init_db()

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode="HTML")
    )

    # amrkt сам работает через пользовательскую TG-сессию
    async with MarketClient(
        api_id=API_ID,
        api_hash=API_HASH,
        session_name=SESSION_NAME,
    ) as client:
        logger.info("MRKT bot started. Interval=%s sec", CHECK_INTERVAL)

        while True:
            try:
                feed = await client.get_feed()
                items = safe_get(feed, "items", default=[]) or []
                logger.info("feed items: %s", len(items))

                for row in items:
                    try:
                        await process_feed_item(bot, client, row)
                    except Exception as e:
                        logger.exception("Ошибка обработки feed item: %s", e)

            except Exception as e:
                logger.exception("Ошибка основного цикла: %s", e)

            await asyncio.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    asyncio.run(monitor_loop())