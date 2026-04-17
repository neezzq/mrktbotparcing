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

from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv
from pyrogram import Client
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw.types import InputBotAppShortName, InputUser


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_CHAT_ID = int(os.getenv("OWNER_CHAT_ID", "0") or "0")
API_ID = int(os.getenv("API_ID", "0") or "0")
API_HASH = os.getenv("API_HASH", "").strip()
SESSION_NAME = os.getenv("SESSION_NAME", "mrkt_session").strip()

CHECK_INTERVAL = int(os.getenv("CHECK_INTERVAL", "5") or "5")
DB_PATH = os.getenv("DB_PATH", "alerts.db").strip()
POLL_COUNT = int(os.getenv("POLL_COUNT", "20") or "20")
AVG_SAMPLE_SIZE = int(os.getenv("AVG_SAMPLE_SIZE", "10") or "10")
TOKEN_REFRESH_SECONDS = int(os.getenv("TOKEN_REFRESH_SECONDS", "3600") or "3600")
MAX_PRICE_TON = float(os.getenv("MAX_PRICE_TON", "0") or "0")
MIN_PRICE_TON = float(os.getenv("MIN_PRICE_TON", "0") or "0")
MRKT_ORDERING = (os.getenv("MRKT_ORDERING", "Date") or "Date").strip()
SEND_STARTUP_TEST = os.getenv("SEND_STARTUP_TEST", "0").strip() == "1"

MRKT_BOT_USERNAME = os.getenv("MRKT_BOT_USERNAME", "mrkt").strip()
MRKT_APP_SHORT_NAME = os.getenv("MRKT_APP_SHORT_NAME", "app").strip()
MRKT_PLATFORM = os.getenv("MRKT_PLATFORM", "android").strip()
MRKT_STATIC_TOKEN = os.getenv("MRKT_STATIC_TOKEN", "").strip()

MARKET_API_URL = "https://api.tgmrkt.io/api/v1"
MRKT_CDN_REFERER = "https://cdn.tgmrkt.io/"

START_TEXT = (
    "Это бот для уведомлений о выходах подарков.\n\n"
    "Команды:\n"
    "/gift Название подарка — добавить фильтр\n"
    "/ungift Название подарка — убрать фильтр\n"
    "/mygifts — показать мои фильтры\n"
    "/allgifts — получать все подарки\n"
    "/help — помощь\n\n"
    "Пример: /gift Jester Hat\n\n"
    "После /start бот добавит тебя в подписчики."
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("mrktbot")


class MrktHttpError(RuntimeError):
    def __init__(self, status_code: int, path: str, payload: dict, body: str):
        super().__init__(f"MRKT HTTP {status_code} | path={path} | payload={payload} | body={body}")
        self.status_code = status_code
        self.path = path
        self.payload = payload
        self.body = body


class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init(self) -> None:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS seen_items (
                unique_id TEXT PRIMARY KEY,
                created_at INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                chat_id INTEGER PRIMARY KEY,
                username TEXT,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS user_filters (
                chat_id INTEGER NOT NULL,
                collection_name TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                PRIMARY KEY (chat_id, collection_name)
            )
            """
        )
        conn.commit()
        conn.close()

    def remember_user(self, chat_id: int, username: str | None) -> None:
        now = int(time.time())
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO users(chat_id, username, is_active, created_at, updated_at)
            VALUES(?, ?, 1, ?, ?)
            ON CONFLICT(chat_id) DO UPDATE SET
                username = excluded.username,
                is_active = 1,
                updated_at = excluded.updated_at
            """,
            (chat_id, username, now, now),
        )
        conn.commit()
        conn.close()

    def deactivate_user(self, chat_id: int) -> None:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("UPDATE users SET is_active = 0, updated_at = ? WHERE chat_id = ?", (int(time.time()), chat_id))
        conn.commit()
        conn.close()

    def add_filter(self, chat_id: int, collection_name: str) -> None:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO user_filters(chat_id, collection_name, created_at) VALUES(?, ?, ?)",
            (chat_id, collection_name, int(time.time())),
        )
        conn.commit()
        conn.close()

    def remove_filter(self, chat_id: int, collection_name: str) -> bool:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM user_filters WHERE chat_id = ? AND collection_name = ?", (chat_id, collection_name))
        changed = cur.rowcount > 0
        conn.commit()
        conn.close()
        return changed

    def clear_filters(self, chat_id: int) -> None:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute("DELETE FROM user_filters WHERE chat_id = ?", (chat_id,))
        conn.commit()
        conn.close()

    def get_filters(self, chat_id: int) -> list[str]:
        conn = self._conn()
        cur = conn.cursor()
        rows = cur.execute(
            "SELECT collection_name FROM user_filters WHERE chat_id = ? ORDER BY collection_name ASC",
            (chat_id,),
        ).fetchall()
        conn.close()
        return [row[0] for row in rows]

    def get_active_users(self) -> list[int]:
        conn = self._conn()
        cur = conn.cursor()
        rows = cur.execute("SELECT chat_id FROM users WHERE is_active = 1 ORDER BY chat_id ASC").fetchall()
        conn.close()
        return [int(row[0]) for row in rows]

    def get_user_filters_map(self) -> dict[int, list[str]]:
        conn = self._conn()
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT u.chat_id, uf.collection_name
            FROM users u
            LEFT JOIN user_filters uf ON u.chat_id = uf.chat_id
            WHERE u.is_active = 1
            ORDER BY u.chat_id ASC, uf.collection_name ASC
            """
        ).fetchall()
        conn.close()
        result: dict[int, list[str]] = {}
        for row in rows:
            chat_id = int(row[0])
            collection_name = row[1]
            result.setdefault(chat_id, [])
            if collection_name:
                result[chat_id].append(str(collection_name))
        return result

    def is_seen(self, unique_id: str) -> bool:
        conn = self._conn()
        cur = conn.cursor()
        row = cur.execute("SELECT 1 FROM seen_items WHERE unique_id = ?", (unique_id,)).fetchone()
        conn.close()
        return row is not None

    def mark_seen(self, unique_id: str) -> None:
        conn = self._conn()
        cur = conn.cursor()
        cur.execute(
            "INSERT OR IGNORE INTO seen_items(unique_id, created_at) VALUES(?, ?)",
            (unique_id, int(time.time())),
        )
        conn.commit()
        conn.close()


storage = Storage(DB_PATH)
router = Router()


def ensure_owner_subscribed() -> None:
    if OWNER_CHAT_ID > 0:
        storage.remember_user(OWNER_CHAT_ID, None)



def require_env() -> None:
    missing = []
    if not BOT_TOKEN:
        missing.append("BOT_TOKEN")
    if not API_ID:
        missing.append("API_ID")
    if not API_HASH:
        missing.append("API_HASH")
    if missing:
        raise RuntimeError(f"Не заполнены переменные: {', '.join(missing)}")


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


def walk_values(node: Any):
    if isinstance(node, dict):
        for key, value in node.items():
            yield key, value
            yield from walk_values(value)
    elif isinstance(node, list):
        for item in node:
            yield from walk_values(item)


def find_first_by_keys(data: Any, keys: list[str]) -> Any:
    wanted = {k.lower() for k in keys}
    for key, value in walk_values(data):
        if str(key).lower() in wanted and value not in (None, ""):
            return value
    return None


def find_dict_with_keys(data: Any, keys: list[str]) -> Optional[dict]:
    wanted = {k.lower() for k in keys}
    if isinstance(data, dict):
        present = {str(k).lower() for k in data.keys()}
        if wanted.issubset(present):
            return data
        for _, value in data.items():
            found = find_dict_with_keys(value, keys)
            if found:
                return found
    elif isinstance(data, list):
        for item in data:
            found = find_dict_with_keys(item, keys)
            if found:
                return found
    return None


def normalize_name(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").strip())


def normalize_case_insensitive(name: str) -> str:
    return normalize_name(name).casefold()


def first_not_none(*values):
    for value in values:
        if value is not None:
            return value
    return None


def to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.replace(",", ".").replace("%", "").strip()
    try:
        return float(value)
    except Exception:
        return None


def to_ton(value: Any) -> Optional[float]:
    num = to_float(value)
    if num is None:
        return None
    if num > 1_000_000:
        return num / 1_000_000_000
    return num


def ton_to_nano(value: float) -> int:
    return int(round(value * 1_000_000_000))


def fmt_ton(value: Any) -> str:
    ton = to_ton(value)
    if ton is None:
        return "—"
    return f"{ton:.2f} TON"


def fmt_percent(value: Any) -> str:
    num = to_float(value)
    if num is None:
        return "—"
    if 0 < num < 1:
        num *= 100
    return f"{num:.2f}".rstrip("0").rstrip(".") + "%"


def esc(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def slugify_gift_name(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9]", "", name or "")


def build_tme_url(gift_name: str, gift_number: Any) -> Optional[str]:
    if not gift_name or gift_number in (None, "", "—"):
        return None
    return f"https://t.me/nft/{slugify_gift_name(gift_name)}-{gift_number}"


def extract_unique_id(item: dict) -> str:
    return str(first_not_none(
        safe_get(item, "saleId", "sale_id", "id"),
        f"{extract_gift_name(item)}|{extract_gift_number(item)}|{extract_price_ton(item) or 'noprice'}",
    ))


def extract_gift_name(item: dict) -> str:
    return normalize_name(str(first_not_none(
        safe_get(item, "collectionName", "collection_name", "name", "giftName", "gift_name", "title"),
        "Unknown Gift",
    )))


def extract_gift_number(item: dict) -> Any:
    return first_not_none(safe_get(item, "giftNum", "gift_num", "number"), "—")


def extract_price_ton(item: dict) -> Optional[float]:
    return to_ton(first_not_none(
        safe_get(item, "priceTon", "price_ton", "salePriceTon", "sale_price_ton"),
        safe_get(item, "price", "salePrice", "sale_price", "amount"),
    ))


def extract_model_name(item: dict) -> str:
    return normalize_name(str(first_not_none(safe_get(item, "modelName", "model_name", "model"), "Unknown")))


def extract_symbol_name(item: dict) -> str:
    return normalize_name(str(first_not_none(safe_get(item, "symbolName", "symbol_name", "symbol"), "Unknown")))


def extract_backdrop_name(item: dict) -> str:
    return normalize_name(str(first_not_none(safe_get(item, "backdropName", "backdrop_name", "backdrop", "backgroundName", "background_name"), "Unknown")))


def extract_market_url(item: dict) -> Optional[str]:
    return first_not_none(
        safe_get(item, "url", "saleUrl", "sale_url", "marketUrl", "market_url"),
        None,
    )


def extract_percent(item: dict, primary_keys: list[str], fallback_nested_names: list[str]) -> Any:
    direct = find_first_by_keys(item, primary_keys)
    if direct is not None:
        return direct
    for nested_name in fallback_nested_names:
        node = safe_get(item, nested_name)
        if isinstance(node, dict):
            direct = find_first_by_keys(node, ["percent", "rarityPercent", "rarity_percent", "chance", "share"])
            if direct is not None:
                return direct
    return None


def extract_model_percent(item: dict) -> Any:
    return extract_percent(item, ["modelRarityPercent", "model_rarity_percent", "modelPercent"], ["model"])


def extract_symbol_percent(item: dict) -> Any:
    return extract_percent(item, ["symbolRarityPercent", "symbol_rarity_percent", "symbolPercent"], ["symbol"])


def extract_backdrop_percent(item: dict) -> Any:
    return extract_percent(item, ["backdropRarityPercent", "backdrop_rarity_percent", "backgroundRarityPercent", "background_rarity_percent", "backdropPercent", "backgroundPercent"], ["backdrop", "background"])


def parse_statistics_response(item: dict, stats: Any) -> dict:
    enriched = dict(item)
    avg_buy = find_first_by_keys(stats, [
        "avgBuy", "avg_buy", "avgBuyTon", "avg_buy_ton", "avgBuyPrice", "avgBuyPriceTon",
        "averageBuy", "averageBuyTon", "averageBuyPrice", "averageBuyPriceTon",
        "buyAvg", "buyAverage",
    ])
    avg_sell = find_first_by_keys(stats, [
        "avgSell", "avg_sell", "avgSellTon", "avg_sell_ton", "avgSellPrice", "avgSellPriceTon",
        "averageSell", "averageSellTon", "averageSellPrice", "averageSellPriceTon",
        "sellAvg", "sellAverage",
    ])
    if avg_buy is not None:
        enriched["avg_buy_ton"] = avg_buy
    if avg_sell is not None:
        enriched["avg_sell_ton"] = avg_sell

    model_block = find_dict_with_keys(stats, ["name", "percent"])
    if model_block and normalize_case_insensitive(str(model_block.get("name"))) == normalize_case_insensitive(extract_model_name(item)):
        enriched.setdefault("modelRarityPercent", model_block.get("percent"))

    # Better targeted trait lookup
    for trait_key, name_key, percent_key in [
        ("model", extract_model_name(item), "modelRarityPercent"),
        ("symbol", extract_symbol_name(item), "symbolRarityPercent"),
        ("backdrop", extract_backdrop_name(item), "backdropRarityPercent"),
        ("background", extract_backdrop_name(item), "backdropRarityPercent"),
    ]:
        found = None
        for key, value in walk_values(stats):
            if str(key).lower() == trait_key and isinstance(value, dict):
                found = value
                break
        if found is None:
            # search generic lists of dicts with name+percent and matching current trait name
            current_name = normalize_case_insensitive(name_key)
            for _, value in walk_values(stats):
                if isinstance(value, list):
                    for item2 in value:
                        if isinstance(item2, dict):
                            nm = first_not_none(item2.get("name"), item2.get("title"), item2.get("value"))
                            pct = first_not_none(item2.get("percent"), item2.get("rarityPercent"), item2.get("rarity_percent"), item2.get("share"))
                            if nm and pct is not None and normalize_case_insensitive(str(nm)) == current_name:
                                enriched.setdefault(percent_key, pct)
                                break
                if percent_key in enriched:
                    break
        else:
            pct = first_not_none(found.get("percent"), found.get("rarityPercent"), found.get("rarity_percent"), found.get("share"))
            if pct is not None:
                enriched.setdefault(percent_key, pct)

    return enriched


class MrktApi:
    def __init__(self) -> None:
        self.token: Optional[str] = MRKT_STATIC_TOKEN or None
        self.token_received_at = 0.0
        self.http: Optional[AsyncSession] = None
        self.tg: Optional[Client] = None
        self.stats_cache: dict[str, tuple[float, dict]] = {}

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

    async def ensure_token(self) -> None:
        if self.token and (time.time() - self.token_received_at) < TOKEN_REFRESH_SECONDS:
            return
        await self.refresh_token()

    async def get_init_data(self) -> str:
        assert self.tg is not None
        bot_peer = await self.tg.resolve_peer(MRKT_BOT_USERNAME)
        bot = InputUser(user_id=bot_peer.user_id, access_hash=bot_peer.access_hash)
        bot_app = InputBotAppShortName(bot_id=bot, short_name=MRKT_APP_SHORT_NAME)
        web_view = await self.tg.invoke(RequestAppWebView(peer=bot_peer, app=bot_app, platform=MRKT_PLATFORM))
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
        resp = await self.http.post(f"{MARKET_API_URL}/auth", json={"data": init_data})
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
            headers["Authorization"] = self.token or ""
            resp = await self.http.post(f"{MARKET_API_URL}{path}", headers=headers, json=json_data)
        if resp.status_code >= 400:
            body = resp.text if hasattr(resp, "text") else "<no body>"
            raise MrktHttpError(resp.status_code, path, json_data, body)
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
        max_price_ton: Optional[float] = None,
        min_price_ton: Optional[float] = None,
        count: int = 20,
        cursor: str = "",
    ) -> dict:
        collection_names = collection_names or []
        model_names = model_names or []
        backdrop_names = backdrop_names or []
        symbol_names = symbol_names or []
        ordering = ordering or MRKT_ORDERING or "Date"

        # front-like request object
        req_lower = {
            "collectionNames": collection_names,
            "modelNames": model_names,
            "backdropNames": backdrop_names,
            "symbolNames": symbol_names,
            "lowToHigh": low_to_high,
            "count": min(max(int(count), 1), 20),
            "cursor": cursor,
            "promotedFirst": False,
            "query": None,
            "mintable": None,
            "number": None,
            "maxPrice": ton_to_nano(max_price_ton) if max_price_ton and max_price_ton > 0 else None,
            "minPrice": ton_to_nano(min_price_ton) if min_price_ton and min_price_ton > 0 else None,
        }
        req_upper = {
            "CollectionNames": collection_names,
            "ModelNames": model_names,
            "BackdropNames": backdrop_names,
            "SymbolNames": symbol_names,
            "lowToHigh": low_to_high,
            "count": min(max(int(count), 1), 20),
            "cursor": cursor,
            "promotedFirst": False,
            "query": None,
            "mintable": None,
            "number": None,
            "maxPrice": ton_to_nano(max_price_ton) if max_price_ton and max_price_ton > 0 else None,
            "minPrice": ton_to_nano(min_price_ton) if min_price_ton and min_price_ton > 0 else None,
        }
        compact_lower = {k: v for k, v in req_lower.items() if v is not None}
        compact_upper = {k: v for k, v in req_upper.items() if v is not None}

        candidates = [
            compact_lower | {"ordering": ordering},
            {"req": compact_lower, "ordering": ordering},
            compact_upper | {"ordering": ordering},
            {"req": compact_upper, "ordering": ordering},
        ]

        last_error: Optional[Exception] = None
        for payload in candidates:
            try:
                logger.info("MRKT /gifts/saling payload: %s", payload)
                data = await self.post("/gifts/saling", payload)
                if any(k in data for k in ("gifts", "items", "data", "results")):
                    return data
                # if endpoint returns ok wrapper
                return data
            except MrktHttpError as exc:
                last_error = exc
                continue
        if last_error:
            raise last_error
        raise RuntimeError("Не удалось получить /gifts/saling")

    async def fetch_gift_statistics(self, item: dict) -> dict:
        cache_key = f"{extract_gift_name(item)}|{extract_gift_number(item)}"
        now = time.time()
        cached = self.stats_cache.get(cache_key)
        if cached and now - cached[0] < 600:
            return cached[1]

        payloads = [
            {"collectionName": extract_gift_name(item), "giftNum": extract_gift_number(item)},
            {"collectionName": extract_gift_name(item), "number": extract_gift_number(item)},
            {"giftNum": extract_gift_number(item), "collectionName": extract_gift_name(item), "language": "ru"},
            {"req": {"collectionName": extract_gift_name(item), "giftNum": extract_gift_number(item)}},
            {"req": {"collectionName": extract_gift_name(item), "number": extract_gift_number(item)}},
        ]
        paths = ["/gift-statistics", "/gifts/statistics"]
        last_error = None
        for path in paths:
            for payload in payloads:
                try:
                    data = await self.post(path, payload)
                    self.stats_cache[cache_key] = (now, data)
                    return data
                except Exception as exc:
                    last_error = exc
                    continue
        logger.warning("Не удалось получить gift statistics для %s #%s: %s", extract_gift_name(item), extract_gift_number(item), last_error)
        return {}

    async def enrich_item(self, item: dict) -> dict:
        stats = await self.fetch_gift_statistics(item)
        enriched = parse_statistics_response(item, stats)
        return enriched


async def calc_avg_sell(api: MrktApi, item: dict) -> Optional[float]:
    direct = to_ton(safe_get(item, "avg_sell_ton", "avgSellTon", "avgSellPriceTon", "averageSellPriceTon"))
    if direct is not None:
        return round(direct, 4)
    try:
        result = await api.fetch_saling(
            collection_names=[extract_gift_name(item)],
            model_names=[extract_model_name(item)] if extract_model_name(item) != "Unknown" else [],
            backdrop_names=[extract_backdrop_name(item)] if extract_backdrop_name(item) != "Unknown" else [],
            symbol_names=[extract_symbol_name(item)] if extract_symbol_name(item) != "Unknown" else [],
            ordering="Price",
            low_to_high=True,
            count=min(AVG_SAMPLE_SIZE, 20),
        )
        gifts = extract_items_list(result)
        prices = [extract_price_ton(x) for x in gifts]
        prices = [x for x in prices if x is not None]
        if prices:
            return round(mean(prices), 4)
    except Exception as exc:
        logger.warning("Не удалось посчитать avg sell: %s", exc)
    return None


async def calc_avg_buy(api: MrktApi, item: dict) -> Optional[float]:
    direct = to_ton(safe_get(item, "avg_buy_ton", "avgBuyTon", "avgBuyPriceTon", "averageBuyPriceTon"))
    if direct is not None:
        return round(direct, 4)
    stats = await api.fetch_gift_statistics(item)
    found = find_first_by_keys(stats, [
        "avgBuy", "avg_buy", "avgBuyTon", "avg_buy_ton", "avgBuyPrice", "avgBuyPriceTon",
        "averageBuy", "averageBuyTon", "averageBuyPrice", "averageBuyPriceTon",
        "buyAvg", "buyAverage",
    ])
    ton = to_ton(found)
    return round(ton, 4) if ton is not None else None


def extract_items_list(data: dict) -> list[dict]:
    for key in ("gifts", "items", "results", "data"):
        value = data.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
        if isinstance(value, dict):
            for subkey in ("items", "gifts", "results"):
                subval = value.get(subkey)
                if isinstance(subval, list):
                    return [x for x in subval if isinstance(x, dict)]
    # generic recursive fallback
    if isinstance(data, dict):
        for _, value in data.items():
            if isinstance(value, list) and value and isinstance(value[0], dict):
                sample = value[0]
                if extract_gift_name(sample) != "Unknown Gift" or extract_price_ton(sample) is not None:
                    return value
    return []


def build_message(item: dict, avg_buy: Optional[float], avg_sell: Optional[float]) -> str:
    gift_name = extract_gift_name(item)
    gift_number = extract_gift_number(item)
    return (
        f"<b>{esc(gift_name)} #{esc(gift_number)}</b>\n\n"
        f"- Model: {esc(extract_model_name(item))} ({fmt_percent(extract_model_percent(item))})\n"
        f"- Symbol: {esc(extract_symbol_name(item))} ({fmt_percent(extract_symbol_percent(item))})\n"
        f"- Backdrop: {esc(extract_backdrop_name(item))} ({fmt_percent(extract_backdrop_percent(item))})\n\n"
        f"🪙 Price: {fmt_ton(extract_price_ton(item))}\n"
        f"📉 Avg buy: {fmt_ton(avg_buy)}\n"
        f"📈 Avg sell: {fmt_ton(avg_sell)}"
        + (f"\n\n{esc(build_tme_url(gift_name, gift_number))}" if build_tme_url(gift_name, gift_number) else "")
    )


async def send_alert(bot: Bot, chat_id: int, item: dict, avg_buy: Optional[float], avg_sell: Optional[float]) -> None:
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
        chat_id=chat_id,
        text=build_message(item, avg_buy, avg_sell),
        reply_markup=markup,
        disable_web_page_preview=False,
    )


@router.message(CommandStart())
async def cmd_start(message: Message):
    storage.remember_user(message.chat.id, message.from_user.username if message.from_user else None)
    await message.answer(START_TEXT)


@router.message(Command("help"))
async def cmd_help(message: Message):
    storage.remember_user(message.chat.id, message.from_user.username if message.from_user else None)
    await message.answer(START_TEXT)


@router.message(Command("gift"))
async def cmd_gift(message: Message):
    storage.remember_user(message.chat.id, message.from_user.username if message.from_user else None)
    text = message.text or ""
    gift_name = normalize_name(text.partition(" ")[2])
    if not gift_name:
        await message.answer("Напиши так: /gift Jester Hat")
        return
    storage.add_filter(message.chat.id, gift_name)
    await message.answer(f"Фильтр добавлен: <b>{esc(gift_name)}</b>")


@router.message(Command("ungift"))
async def cmd_ungift(message: Message):
    storage.remember_user(message.chat.id, message.from_user.username if message.from_user else None)
    text = message.text or ""
    gift_name = normalize_name(text.partition(" ")[2])
    if not gift_name:
        await message.answer("Напиши так: /ungift Jester Hat")
        return
    removed = storage.remove_filter(message.chat.id, gift_name)
    if removed:
        await message.answer(f"Фильтр удалён: <b>{esc(gift_name)}</b>")
    else:
        await message.answer("Такого фильтра у тебя нет.")


@router.message(Command("mygifts"))
async def cmd_mygifts(message: Message):
    storage.remember_user(message.chat.id, message.from_user.username if message.from_user else None)
    filters = storage.get_filters(message.chat.id)
    if not filters:
        await message.answer("Сейчас фильтров нет. Ты получаешь все подарки.")
        return
    await message.answer("Твои фильтры:\n" + "\n".join(f"- {esc(x)}" for x in filters))


@router.message(Command("allgifts"))
async def cmd_allgifts(message: Message):
    storage.remember_user(message.chat.id, message.from_user.username if message.from_user else None)
    storage.clear_filters(message.chat.id)
    await message.answer("Фильтры очищены. Теперь будут приходить все подарки.")


def user_wants_item(filters: list[str], item: dict) -> bool:
    if not filters:
        return True
    item_name = normalize_case_insensitive(extract_gift_name(item))
    filter_names = {normalize_case_insensitive(x) for x in filters}
    return item_name in filter_names


async def process_items_for_users(bot: Bot, api: MrktApi, items: list[dict]) -> None:
    users_map = storage.get_user_filters_map()
    if not users_map:
        return

    for item in items:
        unique_id = extract_unique_id(item)
        if storage.is_seen(unique_id):
            continue

        enriched = await api.enrich_item(item)
        avg_buy = await calc_avg_buy(api, enriched)
        avg_sell = await calc_avg_sell(api, enriched)

        sent_to_any = False
        for chat_id, filters in users_map.items():
            if not user_wants_item(filters, enriched):
                continue
            try:
                await send_alert(bot, chat_id, enriched, avg_buy, avg_sell)
                sent_to_any = True
            except Exception as exc:
                logger.warning("Не удалось отправить уведомление в chat_id=%s: %s", chat_id, exc)
                storage.deactivate_user(chat_id)

        if sent_to_any:
            storage.mark_seen(unique_id)
            logger.info("Отправлено: %s #%s | %s", extract_gift_name(enriched), extract_gift_number(enriched), fmt_ton(extract_price_ton(enriched)))


async def process_market_page(bot: Bot, api: MrktApi) -> None:
    users_map = storage.get_user_filters_map()
    if not users_map:
        logger.info("Подписчиков пока нет")
        return

    unique_collections = sorted({name for filters in users_map.values() for name in filters if name})
    need_global = any(len(filters) == 0 for filters in users_map.values())

    batches: list[dict] = []
    if need_global:
        batches.append(
            await api.fetch_saling(
                ordering=MRKT_ORDERING,
                low_to_high=False,
                max_price_ton=MAX_PRICE_TON if MAX_PRICE_TON > 0 else None,
                min_price_ton=MIN_PRICE_TON if MIN_PRICE_TON > 0 else None,
                count=POLL_COUNT,
            )
        )
    for collection_name in unique_collections:
        batches.append(
            await api.fetch_saling(
                collection_names=[collection_name],
                ordering=MRKT_ORDERING,
                low_to_high=False,
                max_price_ton=MAX_PRICE_TON if MAX_PRICE_TON > 0 else None,
                min_price_ton=MIN_PRICE_TON if MIN_PRICE_TON > 0 else None,
                count=POLL_COUNT,
            )
        )

    merged: dict[str, dict] = {}
    for batch in batches:
        items = extract_items_list(batch)
        logger.info("Получено подарков: %s", len(items))
        for item in items:
            merged[extract_unique_id(item)] = item

    ordered = list(merged.values())
    ordered.sort(key=lambda x: str(extract_unique_id(x)))
    await process_items_for_users(bot, api, ordered)


async def monitor_loop(bot: Bot) -> None:
    async with MrktApi() as api:
        logger.info("MRKT monitor started. Interval=%s sec", CHECK_INTERVAL)
        if SEND_STARTUP_TEST:
            for chat_id in storage.get_active_users():
                try:
                    await bot.send_message(chat_id, "✅ Мониторинг MRKT запущен")
                except Exception:
                    pass
        while True:
            try:
                await process_market_page(bot, api)
            except Exception as exc:
                logger.exception("Ошибка основного цикла: %s", exc)
            await asyncio.sleep(CHECK_INTERVAL)


async def main() -> None:
    require_env()
    storage.init()
    ensure_owner_subscribed()

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp = Dispatcher()
    dp.include_router(router)

    monitor_task = asyncio.create_task(monitor_loop(bot))
    try:
        await dp.start_polling(bot)
    finally:
        monitor_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await monitor_task


if __name__ == "__main__":
    import contextlib

    asyncio.run(main())
