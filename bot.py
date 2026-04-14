import asyncio
import atexit
import base64
import io
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path

import discord
from aiohttp import ClientSession, ClientTimeout, web
from discord import app_commands
from discord.ext import commands


def load_env_file(env_path: str | None = None) -> None:
    base_dir = Path(__file__).resolve().parent
    file_path = Path(env_path) if env_path else base_dir / ".env"
    if not file_path.exists():
        return

    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip().lstrip("\ufeff")
        value = value.strip()
        if key:
            os.environ[key] = value


load_env_file()

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
CLIENT_ID = os.getenv("CLIENT_ID", "").strip()
GUILD_ID = os.getenv("GUILD_ID", "").strip()
LOJA_CHANNEL_ID = os.getenv("LOJA_CHANNEL_ID", "").strip()
LOJA_CHANNEL_ID_2 = os.getenv("LOJA_CHANNEL_ID_2", "").strip()
LOJA_CHANNEL_ID_3 = os.getenv("LOJA_CHANNEL_ID_3", "").strip()
LOJA_CHANNEL_ID_4 = os.getenv("LOJA_CHANNEL_ID_4", "").strip()
LOJA_CHANNEL_ID_5 = os.getenv("LOJA_CHANNEL_ID_5", "").strip()
TICKETS_CATEGORY_ID = os.getenv("TICKETS_CATEGORY_ID", "").strip()
LOG_CHANNEL_ID = os.getenv("LOG_CHANNEL_ID", "").strip()
LOG_TICKET_CHANNEL_ID = os.getenv("LOG_TICKET_CHANNEL_ID", "").strip()
LOG_CHECKOUT_CHANNEL_ID = os.getenv("LOG_CHECKOUT_CHANNEL_ID", "").strip()
LOG_PAYMENT_CHANNEL_ID = os.getenv("LOG_PAYMENT_CHANNEL_ID", "").strip()
LOG_ROLE_CHANNEL_ID = os.getenv("LOG_ROLE_CHANNEL_ID", "").strip()
STOCK_ALERT_CHANNEL_ID = os.getenv("STOCK_ALERT_CHANNEL_ID", "").strip()
AUTO_ROLE_ID = os.getenv("AUTO_ROLE_ID", "").strip()
POSTAR_ROLE_ID = os.getenv("POSTAR_ROLE_ID", "").strip()
ASSUMIR_TICKET_ROLE_ID = os.getenv("ASSUMIR_TICKET_ROLE_ID", "").strip()
MANAGE_ROLE_COMMAND_ROLE_ID = os.getenv("MANAGE_ROLE_COMMAND_ROLE_ID", "").strip()
MP_ACCESS_TOKEN = os.getenv("MP_ACCESS_TOKEN", "").strip()
MP_WEBHOOK_URL = os.getenv("MP_WEBHOOK_URL", "").strip()
OWNER_USER_ID = os.getenv("OWNER_USER_ID", "").strip()
WEBHOOK_PORT = int(os.getenv("WEBHOOK_PORT", "8080").strip() or "8080")
ENABLE_MESSAGE_CONTENT = (
    os.getenv("ENABLE_MESSAGE_CONTENT", "false").strip().lower() == "true"
)
ENABLE_MEMBERS_INTENT = (
    os.getenv("ENABLE_MEMBERS_INTENT", "false").strip().lower() == "true"
)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper() or "INFO"
LOG_FILE = os.getenv("LOG_FILE", "logs/bot.log").strip() or "logs/bot.log"
MP_BLOCKED_PROVIDERS = [
    item.strip().lower()
    for item in os.getenv("MP_BLOCKED_PROVIDERS", "picpay").split(",")
    if item.strip()
]


def parse_env_int(value: str, key: str, required: bool = False) -> int | None:
    cleaned = (value or "").strip()
    # Accept common .env formats like quoted IDs or inline comments.
    if "#" in cleaned:
        cleaned = cleaned.split("#", 1)[0].strip()
    cleaned = cleaned.strip("\"'").strip()
    if not cleaned:
        if required:
            raise RuntimeError(f"Variavel obrigatoria ausente no .env: {key}")
        return None

    if not cleaned.isdigit():
        if required:
            raise RuntimeError(f"Variavel obrigatoria invalida no .env: {key}")
        # Use getLogger directly so this is safe to call before the module-level
        # LOGGER is assigned (e.g. during early module initialisation).
        logging.getLogger("bot_loja").warning(
            "Variavel opcional invalida no .env ignorada: %s=%s", key, cleaned
        )
        return None

    return int(cleaned)


def setup_logger() -> logging.Logger:
    logs_path = Path(LOG_FILE)
    logs_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("bot_loja")
    if logger.handlers:
        return logger

    logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler = RotatingFileHandler(
        logs_path,
        maxBytes=2 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    logger.propagate = False
    return logger


def detect_blocked_payment_provider(payment_details: dict) -> str | None:
    if not MP_BLOCKED_PROVIDERS:
        return None

    # Try direct fields first, then fallback to full payload scan.
    candidates: list[str] = []
    payment_method_id = str(payment_details.get("payment_method_id") or "")
    payment_type_id = str(payment_details.get("payment_type_id") or "")
    issuer_id = str(payment_details.get("issuer_id") or "")
    candidates.extend([payment_method_id, payment_type_id, issuer_id])

    payer = payment_details.get("payer")
    if isinstance(payer, dict):
        candidates.extend(
            [
                str(payer.get("first_name") or ""),
                str(payer.get("last_name") or ""),
                str(payer.get("email") or ""),
            ]
        )

    poi = payment_details.get("point_of_interaction")
    if isinstance(poi, dict):
        tx_data = poi.get("transaction_data")
        if isinstance(tx_data, dict):
            bank_info = tx_data.get("bank_info")
            if isinstance(bank_info, dict):
                candidates.extend(
                    [
                        str(bank_info.get("payer") or ""),
                        str(bank_info.get("collector") or ""),
                        str(bank_info.get("bank_name") or ""),
                    ]
                )

    raw_payload = "\n".join(candidates).lower()
    full_payload = json.dumps(payment_details, ensure_ascii=False).lower()

    for provider in MP_BLOCKED_PROVIDERS:
        if provider in raw_payload or provider in full_payload:
            return provider

    return None


LOGGER = setup_logger()

REQUIRED_ENV_VALUES = {
    "BOT_TOKEN": BOT_TOKEN,
    "CLIENT_ID": CLIENT_ID,
    "GUILD_ID": GUILD_ID,
}
MISSING_REQUIRED_ENV = [
    key for key, value in REQUIRED_ENV_VALUES.items() if not (value or "").strip()
]

if MISSING_REQUIRED_ENV:
    missing_list = ", ".join(MISSING_REQUIRED_ENV)
    raise RuntimeError(
        "Variaveis obrigatorias ausentes: "
        f"{missing_list}. "
        "No Railway, configure essas chaves em Variables."
    )

GUILD_ID_INT = parse_env_int(GUILD_ID, "GUILD_ID", required=True)
LOJA_CHANNEL_ID_INT = parse_env_int(LOJA_CHANNEL_ID, "LOJA_CHANNEL_ID")
LOJA_CHANNEL_ID_2_INT = parse_env_int(LOJA_CHANNEL_ID_2, "LOJA_CHANNEL_ID_2")
LOJA_CHANNEL_ID_3_INT = parse_env_int(LOJA_CHANNEL_ID_3, "LOJA_CHANNEL_ID_3")
LOJA_CHANNEL_ID_4_INT = parse_env_int(LOJA_CHANNEL_ID_4, "LOJA_CHANNEL_ID_4")
LOJA_CHANNEL_ID_5_INT = parse_env_int(LOJA_CHANNEL_ID_5, "LOJA_CHANNEL_ID_5")
TICKETS_CATEGORY_ID_INT = parse_env_int(TICKETS_CATEGORY_ID, "TICKETS_CATEGORY_ID")
LOG_CHANNEL_ID_INT = parse_env_int(LOG_CHANNEL_ID, "LOG_CHANNEL_ID")
LOG_TICKET_CHANNEL_ID_INT = parse_env_int(LOG_TICKET_CHANNEL_ID, "LOG_TICKET_CHANNEL_ID")
LOG_CHECKOUT_CHANNEL_ID_INT = parse_env_int(
    LOG_CHECKOUT_CHANNEL_ID, "LOG_CHECKOUT_CHANNEL_ID"
)
LOG_PAYMENT_CHANNEL_ID_INT = parse_env_int(
    LOG_PAYMENT_CHANNEL_ID, "LOG_PAYMENT_CHANNEL_ID"
)
LOG_ROLE_CHANNEL_ID_INT = parse_env_int(LOG_ROLE_CHANNEL_ID, "LOG_ROLE_CHANNEL_ID")
STOCK_ALERT_CHANNEL_ID_INT = parse_env_int(
    STOCK_ALERT_CHANNEL_ID, "STOCK_ALERT_CHANNEL_ID"
)
AUTO_ROLE_ID_INT = parse_env_int(AUTO_ROLE_ID, "AUTO_ROLE_ID")
POSTAR_ROLE_ID_INT = parse_env_int(POSTAR_ROLE_ID, "POSTAR_ROLE_ID")
ASSUMIR_TICKET_ROLE_ID_INT = parse_env_int(
    ASSUMIR_TICKET_ROLE_ID, "ASSUMIR_TICKET_ROLE_ID"
)
MANAGE_ROLE_COMMAND_ROLE_ID_INT = parse_env_int(
    MANAGE_ROLE_COMMAND_ROLE_ID, "MANAGE_ROLE_COMMAND_ROLE_ID"
)
OWNER_USER_ID_INT = parse_env_int(OWNER_USER_ID, "OWNER_USER_ID")
PROCESSED_PAYMENTS: set[str] = set()
DISCOUNT_CODE = os.getenv("DISCOUNT_CODE", "PHANTOM1K").strip().upper()
DISCOUNT_RATE = 0.10
DISCOUNT_USAGE_FILE = Path(__file__).resolve().parent / "logs" / "discount_usage.json"
ACTIVE_DISCOUNT_BY_CHANNEL: dict[int, int] = {}
DELIVERY_USAGE_FILE = Path(__file__).resolve().parent / "logs" / "delivery_usage.json"
CHECKOUT_CREATION_LOCKS: dict[tuple[int, int, str], asyncio.Lock] = {}
CHECKOUT_LOCK_DIR = Path(__file__).resolve().parent / "logs" / "checkout_locks"
CHECKOUT_BURST_GUARD_DIR = Path(__file__).resolve().parent / "logs" / "checkout_burst"
PRODUCT_POST_LOCK_DIR = Path(__file__).resolve().parent / "logs" / "post_locks"
STOCK_MESSAGE_FILE = Path(__file__).resolve().parent / "logs" / "stock_message.json"
PRODUCT_MESSAGES_FILE = Path(__file__).resolve().parent / "logs" / "product_messages.json"
PAYMENT_TRACKING_FILE = Path(__file__).resolve().parent / "logs" / "payment_tracking.json"
BOT_INSTANCE_LOCK_FILE = Path(__file__).resolve().parent / "logs" / "bot_instance.lock"
BOT_INSTANCE_LOCK_FD: int | None = None
CHECKOUT_BURST_WINDOW_SECONDS = 12.0
POST_COMMAND_WINDOW_SECONDS = 8.0
INTERACTION_DEDUPE_WINDOW_SECONDS = 60.0
CHECKOUT_INTERACTION_WINDOW_SECONDS = 45.0
RECENT_POST_INTERACTIONS: dict[int, float] = {}
RECENT_POST_REQUESTS: dict[tuple[int, int, str], float] = {}
RECENT_CHECKOUT_INTERACTIONS: dict[tuple[int, int], float] = {}


def get_checkout_type_from_topic(topic: str | None) -> str | None:
    if not topic:
        return None

    match = re.match(r"^(checkout(?:2|3|4|5)?):([0-9]+)$", topic.strip())
    if not match:
        return None

    return match.group(1)


def is_checkout_topic_for_user(
    topic: str | None,
    user_id: int,
    checkout_type: str | None = None,
) -> bool:
    if not topic:
        return False

    topic_checkout_type = get_checkout_type_from_topic(topic)
    if topic_checkout_type is None:
        return False

    if checkout_type and topic_checkout_type != checkout_type:
        return False

    return topic.endswith(f":{user_id}")


def extract_checkout_user_id(topic: str | None) -> int | None:
    if not topic:
        return None

    match = re.match(r"^checkout(?:2|3|4|5)?:([0-9]+)$", topic.strip())
    if not match:
        return None

    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


async def find_open_checkouts_for_user(
    guild: discord.Guild,
    user_id: int,
    checkout_type: str | None = None,
) -> list[discord.TextChannel]:
    by_id: dict[int, discord.TextChannel] = {}

    # First try cache for performance.
    for channel in guild.channels:
        if isinstance(channel, discord.TextChannel) and is_checkout_topic_for_user(
            channel.topic,
            user_id,
            checkout_type,
        ):
            by_id[channel.id] = channel

    # Then fetch from API to avoid race conditions with stale cache.
    try:
        channels = await guild.fetch_channels()
    except Exception:
        return list(by_id.values())

    for channel in channels:
        if isinstance(channel, discord.TextChannel) and is_checkout_topic_for_user(
            channel.topic,
            user_id,
            checkout_type,
        ):
            by_id[channel.id] = channel

    return list(by_id.values())


async def find_open_checkout_for_user(
    guild: discord.Guild,
    user_id: int,
    checkout_type: str | None = None,
) -> discord.TextChannel | None:
    channels = await find_open_checkouts_for_user(guild, user_id, checkout_type)
    if not channels:
        return None

    channels.sort(key=lambda item: item.id)
    return channels[0]


async def enforce_single_checkout_for_user(
    guild: discord.Guild,
    user_id: int,
    checkout_type: str,
) -> discord.TextChannel | None:
    channels = await find_open_checkouts_for_user(guild, user_id, checkout_type)
    if not channels:
        return None

    channels.sort(key=lambda item: item.id)
    primary_checkout = channels[0]
    for duplicate_checkout in channels[1:]:
        try:
            await duplicate_checkout.delete(
                reason=f"Checkout duplicado para user {user_id}",
            )
        except Exception:
            LOGGER.warning(
                "Falha ao remover checkout duplicado. channel_id=%s user_id=%s",
                duplicate_checkout.id,
                user_id,
            )

    return primary_checkout


def acquire_checkout_file_lock(
    guild_id: int,
    user_id: int,
    checkout_type: str,
) -> Path | None:
    CHECKOUT_LOCK_DIR.mkdir(parents=True, exist_ok=True)
    safe_checkout_type = re.sub(r"[^a-z0-9_-]", "", checkout_type.lower()) or "checkout"
    lock_path = CHECKOUT_LOCK_DIR / f"{guild_id}_{user_id}_{safe_checkout_type}.lock"

    for _ in range(2):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
                lock_file.write(f"pid={os.getpid()} ts={int(time.time())}\n")
            return lock_path
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
                if age > 30:
                    lock_path.unlink(missing_ok=True)
                    continue
            except Exception:
                pass
            return None

    return None


def release_checkout_file_lock(lock_path: Path | None) -> None:
    if not lock_path:
        return
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def acquire_checkout_burst_guard(
    guild_id: int,
    user_id: int,
    checkout_type: str,
) -> bool:
    CHECKOUT_BURST_GUARD_DIR.mkdir(parents=True, exist_ok=True)
    safe_checkout_type = re.sub(r"[^a-z0-9_-]", "", checkout_type.lower()) or "checkout"
    guard_path = CHECKOUT_BURST_GUARD_DIR / f"{guild_id}_{user_id}_{safe_checkout_type}.cooldown"

    for _ in range(2):
        try:
            fd = os.open(str(guard_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as guard_file:
                guard_file.write(f"pid={os.getpid()} ts={int(time.time())}\n")
            return True
        except FileExistsError:
            try:
                age = time.time() - guard_path.stat().st_mtime
                if age > CHECKOUT_BURST_WINDOW_SECONDS:
                    guard_path.unlink(missing_ok=True)
                    continue
            except Exception:
                pass
            return False

    return False


def acquire_product_post_file_lock(
    guild_id: int,
    channel_id: int,
    product_id: str,
) -> Path | None:
    PRODUCT_POST_LOCK_DIR.mkdir(parents=True, exist_ok=True)
    safe_product_id = re.sub(r"[^a-z0-9_-]", "", product_id.lower()) or "produto"
    lock_path = PRODUCT_POST_LOCK_DIR / f"{guild_id}_{channel_id}_{safe_product_id}.lock"

    for _ in range(2):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
                lock_file.write(f"pid={os.getpid()} ts={int(time.time())}\n")
            return lock_path
        except FileExistsError:
            try:
                age = time.time() - lock_path.stat().st_mtime
                if age > 90:
                    lock_path.unlink(missing_ok=True)
                    continue
            except Exception:
                pass
            return None

    return None


def release_product_post_file_lock(lock_path: Path | None) -> None:
    if not lock_path:
        return
    try:
        lock_path.unlink(missing_ok=True)
    except Exception:
        pass


def is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False

    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def release_bot_instance_lock() -> None:
    global BOT_INSTANCE_LOCK_FD

    if BOT_INSTANCE_LOCK_FD is not None:
        try:
            os.close(BOT_INSTANCE_LOCK_FD)
        except OSError:
            pass
        BOT_INSTANCE_LOCK_FD = None

    try:
        BOT_INSTANCE_LOCK_FILE.unlink(missing_ok=True)
    except Exception:
        pass


def acquire_bot_instance_lock() -> None:
    global BOT_INSTANCE_LOCK_FD

    BOT_INSTANCE_LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)

    for _ in range(2):
        try:
            fd = os.open(
                str(BOT_INSTANCE_LOCK_FILE),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            os.write(fd, str(os.getpid()).encode("utf-8"))
            BOT_INSTANCE_LOCK_FD = fd
            atexit.register(release_bot_instance_lock)
            return
        except FileExistsError:
            lock_age = 0.0
            try:
                lock_age = time.time() - BOT_INSTANCE_LOCK_FILE.stat().st_mtime
            except Exception:
                pass

            try:
                raw_pid = BOT_INSTANCE_LOCK_FILE.read_text(encoding="utf-8").strip()
                existing_pid = int(raw_pid) if raw_pid else 0
            except Exception:
                existing_pid = 0

            if is_pid_running(existing_pid):
                raise RuntimeError(
                    f"Outra instancia do bot ja esta em execucao (pid={existing_pid})."
                )

            # Evita corrida de startup: se o lock e recente, nao remover.
            if lock_age <= 30:
                raise RuntimeError(
                    "Outra instancia do bot pode estar iniciando agora; tente novamente em alguns segundos."
                )

            BOT_INSTANCE_LOCK_FILE.unlink(missing_ok=True)

    raise RuntimeError("Nao foi possivel obter lock de instancia unica do bot.")


def load_discount_usage() -> set[int]:
    if not DISCOUNT_USAGE_FILE.exists():
        return set()

    try:
        raw = json.loads(DISCOUNT_USAGE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return set()

    if not isinstance(raw, list):
        return set()

    users: set[int] = set()
    for value in raw:
        try:
            users.add(int(value))
        except (TypeError, ValueError):
            continue
    return users


def save_discount_usage(users: set[int]) -> None:
    DISCOUNT_USAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = sorted(users)
    DISCOUNT_USAGE_FILE.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


DISCOUNT_USED_USERS: set[int] = load_discount_usage()


def load_delivery_usage() -> set[str]:
    if not DELIVERY_USAGE_FILE.exists():
        return set()

    try:
        raw = json.loads(DELIVERY_USAGE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return set()

    if not isinstance(raw, list):
        return set()

    entries: set[str] = set()
    for value in raw:
        if isinstance(value, str) and value.strip():
            entries.add(value.strip())
    return entries


def save_delivery_usage(entries: set[str]) -> None:
    DELIVERY_USAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = sorted(entries)
    DELIVERY_USAGE_FILE.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


DELIVERY_SENT_ENTRIES: set[str] = load_delivery_usage()


def load_stock_message_ref() -> dict[str, int] | None:
    if not STOCK_MESSAGE_FILE.exists():
        return None

    try:
        raw = json.loads(STOCK_MESSAGE_FILE.read_text(encoding="utf-8"))
    except Exception:
        return None

    if not isinstance(raw, dict):
        return None

    try:
        channel_id = int(raw.get("channel_id"))
        message_id = int(raw.get("message_id"))
    except (TypeError, ValueError):
        return None

    return {"channel_id": channel_id, "message_id": message_id}


def save_stock_message_ref(channel_id: int, message_id: int) -> None:
    global STOCK_MESSAGE_REF

    STOCK_MESSAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "channel_id": channel_id,
        "message_id": message_id,
    }
    STOCK_MESSAGE_FILE.write_text(
        json.dumps(payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    STOCK_MESSAGE_REF = payload


def clear_stock_message_ref() -> None:
    global STOCK_MESSAGE_REF

    STOCK_MESSAGE_REF = None
    try:
        STOCK_MESSAGE_FILE.unlink(missing_ok=True)
    except Exception:
        pass


STOCK_MESSAGE_REF: dict[str, int] | None = load_stock_message_ref()


def load_product_message_refs() -> dict[str, int]:
    if not PRODUCT_MESSAGES_FILE.exists():
        return {}

    try:
        raw = json.loads(PRODUCT_MESSAGES_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

    if not isinstance(raw, dict):
        return {}

    refs: dict[str, int] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        try:
            refs[key] = int(value)
        except (TypeError, ValueError):
            continue
    return refs


def save_product_message_refs() -> None:
    PRODUCT_MESSAGES_FILE.parent.mkdir(parents=True, exist_ok=True)
    PRODUCT_MESSAGES_FILE.write_text(
        json.dumps(PRODUCT_MESSAGE_REFS, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def get_product_message_ref_key(guild_id: int, channel_id: int, product_id: str) -> str:
    return f"{guild_id}:{channel_id}:{product_id}"


def get_product_message_ref(guild_id: int, channel_id: int, product_id: str) -> int | None:
    key = get_product_message_ref_key(guild_id, channel_id, product_id)
    return PRODUCT_MESSAGE_REFS.get(key)


def set_product_message_ref(
    guild_id: int,
    channel_id: int,
    product_id: str,
    message_id: int,
) -> None:
    key = get_product_message_ref_key(guild_id, channel_id, product_id)
    PRODUCT_MESSAGE_REFS[key] = message_id
    save_product_message_refs()


def clear_product_message_ref(guild_id: int, channel_id: int, product_id: str) -> None:
    key = get_product_message_ref_key(guild_id, channel_id, product_id)
    if key in PRODUCT_MESSAGE_REFS:
        PRODUCT_MESSAGE_REFS.pop(key, None)
        save_product_message_refs()


PRODUCT_MESSAGE_REFS: dict[str, int] = load_product_message_refs()


def load_payment_tracking() -> dict[str, dict[str, object]]:
    if not PAYMENT_TRACKING_FILE.exists():
        return {}

    try:
        raw = json.loads(PAYMENT_TRACKING_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}

    if not isinstance(raw, dict):
        return {}

    records: dict[str, dict[str, object]] = {}
    for payment_id, value in raw.items():
        if not isinstance(payment_id, str) or not payment_id.strip():
            continue
        if not isinstance(value, dict):
            continue
        records[payment_id.strip()] = value
    return records


def save_payment_tracking() -> None:
    PAYMENT_TRACKING_FILE.parent.mkdir(parents=True, exist_ok=True)
    PAYMENT_TRACKING_FILE.write_text(
        json.dumps(PAYMENT_TRACKING, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )


def upsert_payment_tracking(payment_id: str, **fields: object) -> None:
    normalized_payment_id = str(payment_id or "").strip()
    if not normalized_payment_id:
        return

    record = dict(PAYMENT_TRACKING.get(normalized_payment_id, {}))
    if "created_at" not in record:
        record["created_at"] = int(time.time())

    for key, value in fields.items():
        if value is None:
            continue
        record[key] = value

    record["updated_at"] = int(time.time())
    PAYMENT_TRACKING[normalized_payment_id] = record
    save_payment_tracking()


def get_payment_tracking_record(payment_id: str) -> dict[str, object]:
    normalized_payment_id = str(payment_id or "").strip()
    if not normalized_payment_id:
        return {}
    return dict(PAYMENT_TRACKING.get(normalized_payment_id, {}))


def iter_recent_payment_records(limit: int = 25) -> list[tuple[str, dict[str, object]]]:
    entries: list[tuple[str, dict[str, object]]] = list(PAYMENT_TRACKING.items())
    entries.sort(
        key=lambda item: int(item[1].get("updated_at", item[1].get("created_at", 0)) or 0),
        reverse=True,
    )
    return entries[: max(1, limit)]


def iter_reconcile_candidates(limit: int = 30) -> list[tuple[str, dict[str, object]]]:
    candidates: list[tuple[str, dict[str, object]]] = []
    for payment_id, record in iter_recent_payment_records(limit=200):
        status = str(record.get("status") or "").strip().lower()
        if status in {"pending", "processing", "error_lookup"}:
            candidates.append((payment_id, record))
        if len(candidates) >= limit:
            break
    return candidates


def build_orders_dashboard_embed(limit: int = 10) -> discord.Embed:
    total = len(PAYMENT_TRACKING)
    status_counts: dict[str, int] = {}
    for record in PAYMENT_TRACKING.values():
        status = str(record.get("status") or "unknown").lower()
        status_counts[status] = status_counts.get(status, 0) + 1

    pending_count = (
        status_counts.get("pending", 0)
        + status_counts.get("processing", 0)
        + status_counts.get("error_lookup", 0)
        + status_counts.get("approved_no_stock", 0)
    )

    lines: list[str] = [
        f"**Total rastreado:** {total}",
        f"**Pendencias:** {pending_count}",
        f"**Aprovados:** {status_counts.get('approved', 0)}",
        f"**Bloqueados:** {status_counts.get('blocked_provider', 0)}",
    ]

    recent_lines: list[str] = []
    for payment_id, record in iter_recent_payment_records(limit=limit):
        user_id = record.get("user_id")
        product_id = str(record.get("product_id") or "?")
        status = str(record.get("status") or "unknown")
        if isinstance(user_id, int):
            who = f"<@{user_id}>"
        else:
            who = "desconhecido"
        recent_lines.append(f"`{payment_id}` • {status} • {product_id} • {who}")

    embed = discord.Embed(
        title="📊 Painel de Pedidos",
        description="\n".join(lines),
        color=discord.Color.blue(),
    )
    embed.add_field(
        name="Ultimos pedidos",
        value="\n".join(recent_lines) if recent_lines else "Nenhum pedido rastreado ainda.",
        inline=False,
    )
    embed.timestamp = discord.utils.utcnow()
    return embed


PAYMENT_TRACKING: dict[str, dict[str, object]] = load_payment_tracking()


def cleanup_recent_post_trackers(now: float | None = None) -> None:
    current = now if now is not None else time.monotonic()

    expired_interactions = [
        key
        for key, ts in RECENT_POST_INTERACTIONS.items()
        if current - ts > INTERACTION_DEDUPE_WINDOW_SECONDS
    ]
    for key in expired_interactions:
        RECENT_POST_INTERACTIONS.pop(key, None)

    expired_requests = [
        key
        for key, ts in RECENT_POST_REQUESTS.items()
        if current - ts > POST_COMMAND_WINDOW_SECONDS
    ]
    for key in expired_requests:
        RECENT_POST_REQUESTS.pop(key, None)


def cleanup_recent_checkout_trackers(now: float | None = None) -> None:
    current = now if now is not None else time.monotonic()
    expired = [
        key
        for key, ts in RECENT_CHECKOUT_INTERACTIONS.items()
        if current - ts > CHECKOUT_INTERACTION_WINDOW_SECONDS
    ]
    for key in expired:
        RECENT_CHECKOUT_INTERACTIONS.pop(key, None)


def parse_delivery_usage_entry(entry: str) -> tuple[int | None, str | None]:
    parts = entry.split(":")
    if len(parts) < 3:
        return None, None

    try:
        user_id = int(parts[-2])
    except (TypeError, ValueError):
        return None, None

    product_id = parts[-1].strip()
    if not product_id:
        return None, None

    return user_id, product_id


def clear_delivery_usage_entries(
    user_id: int,
    product_id: str | None = None,
) -> int:
    removed_entries: list[str] = []

    for entry in DELIVERY_SENT_ENTRIES:
        entry_user_id, entry_product_id = parse_delivery_usage_entry(entry)
        if entry_user_id != user_id:
            continue
        if product_id and entry_product_id != product_id:
            continue
        removed_entries.append(entry)

    if not removed_entries:
        return 0

    for entry in removed_entries:
        DELIVERY_SENT_ENTRIES.discard(entry)

    save_delivery_usage(DELIVERY_SENT_ENTRIES)
    return len(removed_entries)


def get_postar_role_id() -> int | None:
    # Recarrega o .env para refletir alteracoes sem depender de restart.
    load_env_file()
    return parse_env_int(os.getenv("POSTAR_ROLE_ID", "").strip(), "POSTAR_ROLE_ID")


def get_assumir_ticket_role_id() -> int | None:
    load_env_file()
    return parse_env_int(
        os.getenv("ASSUMIR_TICKET_ROLE_ID", "").strip(),
        "ASSUMIR_TICKET_ROLE_ID",
    )


def get_manage_role_command_role_id() -> int | None:
    load_env_file()
    return parse_env_int(
        os.getenv("MANAGE_ROLE_COMMAND_ROLE_ID", "").strip(),
        "MANAGE_ROLE_COMMAND_ROLE_ID",
    )


def get_loja_channel_id_2() -> int | None:
    load_env_file()
    return parse_env_int(os.getenv("LOJA_CHANNEL_ID_2", "").strip(), "LOJA_CHANNEL_ID_2")


def get_loja_channel_id_3() -> int | None:
    load_env_file()
    return parse_env_int(os.getenv("LOJA_CHANNEL_ID_3", "").strip(), "LOJA_CHANNEL_ID_3")


def get_loja_channel_id_4() -> int | None:
    load_env_file()
    return parse_env_int(os.getenv("LOJA_CHANNEL_ID_4", "").strip(), "LOJA_CHANNEL_ID_4")


def get_loja_channel_id_5() -> int | None:
    load_env_file()
    return parse_env_int(os.getenv("LOJA_CHANNEL_ID_5", "").strip(), "LOJA_CHANNEL_ID_5")


def get_stock_alert_channel_id() -> int | None:
    load_env_file()
    return parse_env_int(
        os.getenv("STOCK_ALERT_CHANNEL_ID", "").strip(), "STOCK_ALERT_CHANNEL_ID"
    )


async def user_can_post_products(
    interaction: discord.Interaction, role_id: int | None
) -> bool:
    if not role_id:
        return False

    guild = interaction.guild
    if guild is None:
        return False

    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None:
        member = guild.get_member(interaction.user.id)

    if member is None:
        try:
            member = await guild.fetch_member(interaction.user.id)
        except Exception:
            return False

    return any(role.id == role_id for role in member.roles)


@dataclass(frozen=True)
class Product:
    product_id: str
    nome: str
    preco: float
    estoque: int
    descricao: list[str]
    imagem: str


PRODUCT = Product(
    product_id="discord_nitro",
    nome="Discord Nitro 1 mês",
    preco=7.50,
    estoque=20,
    descricao=[
        "Nitro 1 tem duração de 1 mês",
        "Divirta-se usando seu nitro!",
        "Receba a key instantaneamente.",
        "Entrega imediata após a compra.",
    ],
        imagem="https://cdn.discordapp.com/attachments/1492989727458984006/1492989866651422871/b6ceec78-18c0-4158-a32b-452214ac24d6.png?ex=69dd56aa&is=69dc052a&hm=e3679bf280e337d483a5999045e225e594a7a762154a2e4bf0dba3eec9483c8f&",
)

PRODUCT2 = Product(
    product_id="discord_nitro_3",
    nome="Discord Nitro 3 meses",
    preco=12.50,
    estoque=20,
    descricao=[
        "Nitro tem duração de 3 meses",
        "Divirta-se usando seu nitro!",
        "Receba a key instantaneamente.",
        "Entrega imediata após a compra.",
    ],
        imagem="https://cdn.discordapp.com/attachments/1492989727458984006/1492989866651422871/b6ceec78-18c0-4158-a32b-452214ac24d6.png?ex=69dd56aa&is=69dc052a&hm=e3679bf280e337d483a5999045e225e594a7a762154a2e4bf0dba3eec9483c8f&",
)

PRODUCT3 = Product(
    product_id="conta_nitrada",
    nome="Conta nitrada",
    preco=10.00,
    estoque=20,
    descricao=[
        "Conta pronta para uso imediato.",
        "Acesso rapido e sem complicacao.",
        "Dados enviados logo apos a compra.",
        "Entrega imediata após a compra.",
    ],
        imagem="https://cdn.discordapp.com/attachments/1492989727458984006/1492989866651422871/b6ceec78-18c0-4158-a32b-452214ac24d6.png?ex=69dd56aa&is=69dc052a&hm=e3679bf280e337d483a5999045e225e594a7a762154a2e4bf0dba3eec9483c8f&",
)

# Produto 4: edite nome, preco, estoque, descricao e imagem aqui.
PRODUCT4 = Product(
    product_id="conta_nitrada_3_meses",
    nome="Conta nitrada 3 meses",
    preco=15.00,
    estoque=20,
    descricao=[
        "Conta nitrada com duracao de 3 meses.",
        "Conta pronta para uso imediato.",
        "Dados de acesso enviados logo apos a compra.",
        "Entrega imediata após a compra.",
    ],
        imagem="https://cdn.discordapp.com/attachments/1492989727458984006/1492989866651422871/b6ceec78-18c0-4158-a32b-452214ac24d6.png?ex=69dd56aa&is=69dc052a&hm=e3679bf280e337d483a5999045e225e594a7a762154a2e4bf0dba3eec9483c8f&",
)

PRODUCT5 = Product(
    product_id="teste_1_real",
    nome="Produto teste",
    preco=1.00,
    estoque=20,
    descricao=[
        "Produto de teste com valor simbólico.",
        "Fluxo completo de checkout e pagamento.",
        "Use para validar automacoes da loja.",
        "Entrega após confirmação de pagamento.",
    ],
    imagem="https://cdn.discordapp.com/attachments/1492989727458984006/1492989866651422871/b6ceec78-18c0-4158-a32b-452214ac24d6.png?ex=69dd56aa&is=69dc052a&hm=e3679bf280e337d483a5999045e225e594a7a762154a2e4bf0dba3eec9483c8f&",
)


def format_brl(valor: float) -> str:
    return f"R${valor:.2f}"


def get_discounted_amount(valor: float) -> float:
    return round(valor * (1 - DISCOUNT_RATE), 2)


def get_delivery_env_key(product: Product) -> str:
    return f"DELIVERY_CODE_{product.product_id.upper()}"


def parse_delivery_codes(raw_value: str) -> list[str]:
    if not raw_value:
        return []

    cleaned = raw_value.strip().strip("\"'").replace("\\n", "\n")
    if not cleaned:
        return []

    if "\n" in cleaned:
        parts = cleaned.splitlines()
    elif "||" in cleaned:
        parts = cleaned.split("||")
    else:
        parts = [cleaned]

    codes: list[str] = []
    for item in parts:
        value = item.strip()
        if value:
            codes.append(value)
    return codes


def serialize_delivery_codes(codes: list[str]) -> str:
    return "\\n".join(codes)


def read_env_value(key: str) -> tuple[str, bool]:
    env_file = Path(__file__).resolve().parent / ".env"
    if not env_file.exists():
        return "", False

    value = ""
    found = False
    try:
        for raw_line in env_file.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            current_key, current_value = line.split("=", 1)
            if current_key.strip().lstrip("\ufeff") == key:
                value = current_value.strip()
                found = True
    except Exception:
        return "", False

    return value, found


def get_env_file_path() -> Path:
    return Path(__file__).resolve().parent / ".env"


def log_missing_delivery_codes(product: Product, env_key: str) -> None:
    LOGGER.warning(
        "Chave de estoque ausente ou vazia no .env. product_id=%s env_key=%s",
        product.product_id,
        env_key,
    )


def get_delivery_codes(product: Product) -> list[str]:
    env_key = get_delivery_env_key(product)
    raw_value, found_in_file = read_env_value(env_key)
    if found_in_file:
        codes = parse_delivery_codes(raw_value)
        if not codes:
            log_missing_delivery_codes(product, env_key)
        return codes

    if not get_env_file_path().exists():
        load_env_file()
        return parse_delivery_codes(os.getenv(env_key, ""))

    log_missing_delivery_codes(product, env_key)
    return []


def get_delivery_stock(product: Product) -> int:
    return len(get_delivery_codes(product))


def update_delivery_codes_in_env(product: Product, codes: list[str]) -> None:
    env_key = get_delivery_env_key(product)
    base_dir = Path(__file__).resolve().parent
    env_file = base_dir / ".env"
    
    if not env_file.exists():
        return
    
    try:
        lines = env_file.read_text(encoding="utf-8").splitlines()
        new_lines = []
        serialized = serialize_delivery_codes(codes)
        replaced = False
        
        for line in lines:
            stripped = line.strip()
            if stripped.startswith(f"{env_key}="):
                new_lines.append(f"{env_key}={serialized}")
                replaced = True
            else:
                new_lines.append(line)

        if not replaced:
            new_lines.append(f"{env_key}={serialized}")
        
        env_file.write_text("\n".join(new_lines) + "\n", encoding="utf-8")
        os.environ[env_key] = serialized
        LOGGER.info(
            "Estoque atualizado no .env. product_id=%s quantidade=%s",
            product.product_id,
            len(codes),
        )
    except Exception as error:
        LOGGER.warning(
            "Erro ao atualizar codigos no .env. product_id=%s erro=%s",
            product.product_id,
            error,
        )


def consume_delivery_code(product: Product) -> str:
    codes = get_delivery_codes(product)
    if not codes:
        return ""

    delivery_code = codes.pop(0)
    update_delivery_codes_in_env(product, codes)
    return delivery_code


def get_product_by_id(product_id: str | None) -> Product | None:
    if not product_id:
        return None

    for product in (PRODUCT, PRODUCT2, PRODUCT3, PRODUCT4, PRODUCT5):
        if product.product_id == product_id:
            return product
    return None


async def send_product_delivery_dm(
    bot: commands.Bot,
    user_id: int,
    product: Product,
    payment_id: str,
) -> str:
    delivery_key = f"{payment_id}:{user_id}:{product.product_id}"
    if delivery_key in DELIVERY_SENT_ENTRIES:
        LOGGER.info(
            "Entrega manual ja registrada para este pagamento. payment_id=%s user_id=%s product_id=%s",
            payment_id,
            user_id,
            product.product_id,
        )
        return "already_sent"

    delivery_code = consume_delivery_code(product)
    if not delivery_code:
        LOGGER.info(
            "Entrega manual sem codigo configurado. user_id=%s product_id=%s",
            user_id,
            product.product_id,
        )
        owner_alert = discord.Embed(
            title="🚨 Estoque de codigos esgotado",
            description=(
                f"**Cliente:** <@{user_id}>\n"
                f"**Produto:** {product.nome}\n"
                f"**ID do pagamento:** `{payment_id}`\n"
                "**Alerta:** Os codigos deste produto acabaram."
            ),
            color=discord.Color.orange(),
        )
        owner_alert.timestamp = discord.utils.utcnow()
        owner_notified = await send_dm_to_owner(bot, owner_alert)
        if not owner_notified:
            await send_log(bot, owner_alert, channel_id=LOG_PAYMENT_CHANNEL_ID_INT)

        return "missing_code"

    owner_delivery_embed = discord.Embed(
        title="📦 Entrega Manual Necessaria",
        description=(
            f"**Cliente:** <@{user_id}>\n"
            f"**Produto:** {product.nome}\n"
            f"**ID do pagamento:** `{payment_id}`\n"
            "**Prazo informado ao cliente:** ate 2 horas"
        ),
        color=discord.Color.blue(),
    )
    owner_delivery_embed.add_field(
        name="Codigo para enviar manualmente",
        value=f"```{delivery_code[:1000]}```",
        inline=False,
    )
    owner_delivery_embed.timestamp = discord.utils.utcnow()

    owner_notified = await send_dm_to_owner(bot, owner_delivery_embed)
    if not owner_notified:
        await send_log(bot, owner_delivery_embed, channel_id=LOG_PAYMENT_CHANNEL_ID_INT)

    DELIVERY_SENT_ENTRIES.add(delivery_key)
    save_delivery_usage(DELIVERY_SENT_ENTRIES)
    refresh_stock_messages = getattr(bot, "refresh_all_product_stock_messages", None)
    if callable(refresh_stock_messages):
        await refresh_stock_messages()

    return "manual_ready"


async def send_log(
    bot: commands.Bot,
    embed: discord.Embed,
    channel_id: int | None = None,
) -> None:
    target_channel_id = channel_id or LOG_CHANNEL_ID_INT
    if not target_channel_id:
        return
    channel = bot.get_channel(target_channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(target_channel_id)
        except Exception:
            LOGGER.warning("Canal de log nao encontrado. id=%s", target_channel_id)
            return
    if isinstance(channel, discord.TextChannel):
        try:
            await channel.send(embed=embed)
        except Exception:
            LOGGER.warning("Falha ao enviar log para o canal %s", target_channel_id)


async def send_dm_to_owner(
    bot: commands.Bot,
    embed: discord.Embed,
) -> bool:
    if not OWNER_USER_ID_INT:
        return False

    try:
        user = await bot.fetch_user(OWNER_USER_ID_INT)
        await user.send(embed=embed)
        return True
    except Exception as error:
        LOGGER.warning("Falha ao enviar DM para dono. user_id=%s erro=%s", OWNER_USER_ID_INT, error)
        return False


async def send_stock_alert_message(bot: commands.Bot, message: str) -> None:
    target_channel_id = get_stock_alert_channel_id() or STOCK_ALERT_CHANNEL_ID_INT
    if not target_channel_id:
        LOGGER.warning("STOCK_ALERT_CHANNEL_ID nao configurado para alerta de estoque")
        return

    channel = bot.get_channel(target_channel_id)
    if channel is None:
        try:
            channel = await bot.fetch_channel(target_channel_id)
        except Exception:
            LOGGER.warning("Canal de alerta de estoque nao encontrado. id=%s", target_channel_id)
            return

    if channel is None or not hasattr(channel, "send"):
        LOGGER.warning(
            "Canal de alerta sem suporte a envio de mensagem. id=%s",
            target_channel_id,
        )
        return

    try:
        await channel.send(message)
    except Exception:
        LOGGER.warning("Falha ao enviar alerta de estoque no canal %s", target_channel_id)
        fallback_embed = discord.Embed(
            title="⚠️ Falha no canal de alerta de estoque",
            description=(
                f"Nao foi possivel enviar alerta no canal `{target_channel_id}`.\n"
                "Verifique permissao de envio de mensagens do bot nesse canal."
            ),
            color=discord.Color.orange(),
        )
        fallback_embed.timestamp = discord.utils.utcnow()
        await send_log(bot, fallback_embed, channel_id=LOG_PAYMENT_CHANNEL_ID_INT)


def build_product_embed() -> discord.Embed:
    estoque = get_delivery_stock(PRODUCT)
    descricao = [
        "**Produto premium para Discord**",
        *[f"- {linha}" for linha in PRODUCT.descricao],
        "",
        f"**Nome:** {PRODUCT.nome}",
        f"**Preco:** {format_brl(PRODUCT.preco)}",
        f"**Estoque:** {estoque}",
    ]
    embed = discord.Embed(
        title="Phantom Vendas | Produto",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(31, 35, 40),
    )
    embed.set_image(url=PRODUCT.imagem)
    return embed


def build_resumo_embed(user_id: int) -> discord.Embed:
    descricao = [
        f"**Cliente:** <@{user_id}>",
        f"**Produto:** {PRODUCT.nome}",
        f"**Valor unitario:** {format_brl(PRODUCT.preco)}",
        "**Quantidade:** 1",
        f"**Total:** {format_brl(PRODUCT.preco)}",
        "",
        "**Produtos no carrinho:** 1",
        f"**Valor a pagar:** {format_brl(PRODUCT.preco)}",
        "**Cupom adicionado:** Sem cupom",
    ]
    return discord.Embed(
        title="Phantom Vendas | Resumo da Compra",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(47, 49, 54),
    )


def build_product_embed_2() -> discord.Embed:
    estoque = get_delivery_stock(PRODUCT2)
    descricao = [
        "**Produto premium**",
        *[f"- {linha}" for linha in PRODUCT2.descricao],
        "",
        f"**Nome:** {PRODUCT2.nome}",
        f"**Preco:** {format_brl(PRODUCT2.preco)}",
        f"**Estoque:** {estoque}",
    ]
    embed = discord.Embed(
        title="Phantom Vendas | Produto",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(31, 35, 40),
    )
    embed.set_image(url=PRODUCT2.imagem)
    return embed


def build_resumo_embed_2(user_id: int) -> discord.Embed:
    descricao = [
        f"**Cliente:** <@{user_id}>",
        f"**Produto:** {PRODUCT2.nome}",
        f"**Valor unitario:** {format_brl(PRODUCT2.preco)}",
        "**Quantidade:** 1",
        f"**Total:** {format_brl(PRODUCT2.preco)}",
        "",
        "**Produtos no carrinho:** 1",
        f"**Valor a pagar:** {format_brl(PRODUCT2.preco)}",
        "**Cupom adicionado:** Sem cupom",
    ]
    return discord.Embed(
        title="Phantom Vendas | Resumo da Compra",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(47, 49, 54),
    )


def build_product_embed_3() -> discord.Embed:
    estoque = get_delivery_stock(PRODUCT3)
    descricao = [
        "**Produto premium**",
        *[f"- {linha}" for linha in PRODUCT3.descricao],
        "",
        f"**Nome:** {PRODUCT3.nome}",
        f"**Preco:** {format_brl(PRODUCT3.preco)}",
        f"**Estoque:** {estoque}",
    ]
    embed = discord.Embed(
        title="Phantom Vendas | Produto",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(31, 35, 40),
    )
    embed.set_image(url=PRODUCT3.imagem)
    return embed


def build_resumo_embed_3(user_id: int) -> discord.Embed:
    descricao = [
        f"**Cliente:** <@{user_id}>",
        f"**Produto:** {PRODUCT3.nome}",
        f"**Valor unitario:** {format_brl(PRODUCT3.preco)}",
        "**Quantidade:** 1",
        f"**Total:** {format_brl(PRODUCT3.preco)}",
        "",
        "**Produtos no carrinho:** 1",
        f"**Valor a pagar:** {format_brl(PRODUCT3.preco)}",
        "**Cupom adicionado:** Sem cupom",
    ]
    return discord.Embed(
        title="Phantom Vendas | Resumo da Compra",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(47, 49, 54),
    )


def build_product_embed_4() -> discord.Embed:
    estoque = get_delivery_stock(PRODUCT4)
    descricao = [
        "**Produto premium**",
        *[f"- {linha}" for linha in PRODUCT4.descricao],
        "",
        f"**Nome:** {PRODUCT4.nome}",
        f"**Preco:** {format_brl(PRODUCT4.preco)}",
        f"**Estoque:** {estoque}",
    ]
    embed = discord.Embed(
        title="Phantom Vendas | Produto",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(31, 35, 40),
    )
    embed.set_image(url=PRODUCT4.imagem)
    return embed


def build_product_embed_5() -> discord.Embed:
    estoque = get_delivery_stock(PRODUCT5)
    descricao = [
        "**Produto de teste**",
        *[f"- {linha}" for linha in PRODUCT5.descricao],
        "",
        f"**Nome:** {PRODUCT5.nome}",
        f"**Preco:** {format_brl(PRODUCT5.preco)}",
        f"**Estoque:** {estoque}",
    ]
    embed = discord.Embed(
        title="Phantom Vendas | Produto",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(31, 35, 40),
    )
    embed.set_image(url=PRODUCT5.imagem)
    return embed


def get_loja_channel_id_for_product(product: Product) -> int | None:
    if product.product_id == PRODUCT.product_id:
        return LOJA_CHANNEL_ID_INT
    if product.product_id == PRODUCT2.product_id:
        return get_loja_channel_id_2() or LOJA_CHANNEL_ID_2_INT
    if product.product_id == PRODUCT3.product_id:
        return get_loja_channel_id_3() or LOJA_CHANNEL_ID_3_INT
    if product.product_id == PRODUCT4.product_id:
        return get_loja_channel_id_4() or LOJA_CHANNEL_ID_4_INT
    if product.product_id == PRODUCT5.product_id:
        return get_loja_channel_id_5() or LOJA_CHANNEL_ID_5_INT or LOJA_CHANNEL_ID_INT
    return None


def get_product_embed_builder(product: Product):
    if product.product_id == PRODUCT.product_id:
        return build_product_embed
    if product.product_id == PRODUCT2.product_id:
        return build_product_embed_2
    if product.product_id == PRODUCT3.product_id:
        return build_product_embed_3
    if product.product_id == PRODUCT4.product_id:
        return build_product_embed_4
    return build_product_embed_5


def build_product_view_for(product: Product, bot: commands.Bot) -> discord.ui.View:
    if product.product_id == PRODUCT.product_id:
        return ProductView(bot)
    if product.product_id == PRODUCT2.product_id:
        return ProductView2(bot)
    if product.product_id == PRODUCT3.product_id:
        return ProductView3(bot)
    if product.product_id == PRODUCT4.product_id:
        return ProductView4(bot)
    return ProductView5(bot)


class ProductPostInProgressError(RuntimeError):
    pass


class ProductCardSyncError(RuntimeError):
    pass


def message_has_button_custom_id(message: discord.Message, custom_id: str) -> bool:
    for row in message.components:
        children = getattr(row, "children", [])
        for component in children:
            if getattr(component, "custom_id", None) == custom_id:
                return True
    return False


def message_is_product_card_for(message: discord.Message, product: Product) -> bool:
    if not message.embeds:
        return False

    embed = message.embeds[0]
    description = embed.description or ""
    return product.nome in description and "Phantom Vendas | Produto" in (embed.title or "")


async def find_existing_product_card_message(
    channel: discord.TextChannel,
    product: Product,
    bot_user_id: int | None,
) -> discord.Message | None:
    target_custom_id = f"comprar_{product.product_id}"

    async for message in channel.history(limit=400):
        if bot_user_id and message.author.id != bot_user_id:
            continue
        if not message.embeds:
            continue
        if message_has_button_custom_id(message, target_custom_id) or message_is_product_card_for(
            message,
            product,
        ):
            return message

    return None


async def post_or_update_product_card(
    guild: discord.Guild,
    channel: discord.TextChannel,
    product: Product,
    embed: discord.Embed,
    view: discord.ui.View,
    bot_user_id: int | None,
) -> tuple[str, discord.Message]:
    lock_path = acquire_product_post_file_lock(guild.id, channel.id, product.product_id)
    if lock_path is None:
        raise ProductPostInProgressError("post_in_progress")

    try:
        stored_message_id = get_product_message_ref(guild.id, channel.id, product.product_id)
        had_stored_ref = bool(stored_message_id)
        if stored_message_id:
            try:
                stored_message = await channel.fetch_message(stored_message_id)
                if not bot_user_id or stored_message.author.id == bot_user_id:
                    await stored_message.edit(embed=embed, view=view)
                    return "updated", stored_message
            except Exception:
                clear_product_message_ref(guild.id, channel.id, product.product_id)

        existing_message = await find_existing_product_card_message(channel, product, bot_user_id)
        if existing_message:
            set_product_message_ref(
                guild.id,
                channel.id,
                product.product_id,
                existing_message.id,
            )
            await existing_message.edit(embed=embed, view=view)

            # Cleanup old duplicates, keeping the most recent card.
            duplicates_removed = 0
            async for message in channel.history(limit=None):
                if message.id == existing_message.id:
                    continue
                if bot_user_id and message.author.id != bot_user_id:
                    continue
                if not message.embeds:
                    continue
                if message_has_button_custom_id(message, f"comprar_{product.product_id}") or message_is_product_card_for(
                    message,
                    product,
                ):
                    try:
                        await message.delete(reason="Remocao automatica de card duplicado")
                        duplicates_removed += 1
                    except Exception:
                        continue
            if duplicates_removed:
                LOGGER.warning(
                    "Cards duplicados removidos. product_id=%s canal=%s removidos=%s",
                    product.product_id,
                    channel.id,
                    duplicates_removed,
                )
            return "updated", existing_message

        # Modo estrito: se havia referencia salva e o card nao foi encontrado,
        # bloqueia criacao nesta tentativa para evitar avalanche de duplicados em retry.
        if had_stored_ref:
            raise ProductCardSyncError("stored_card_missing")

        sent_message = await channel.send(embed=embed, view=view)
        set_product_message_ref(guild.id, channel.id, product.product_id, sent_message.id)
        return "created", sent_message
    finally:
        release_product_post_file_lock(lock_path)


def build_stock_overview_embed() -> discord.Embed:
    lines = [
        f"**{PRODUCT.nome}:** {get_delivery_stock(PRODUCT)}",
        f"**{PRODUCT2.nome}:** {get_delivery_stock(PRODUCT2)}",
        f"**{PRODUCT3.nome}:** {get_delivery_stock(PRODUCT3)}",
        f"**{PRODUCT4.nome}:** {get_delivery_stock(PRODUCT4)}",
        f"**{PRODUCT5.nome}:** {get_delivery_stock(PRODUCT5)}",
    ]

    embed = discord.Embed(
        title="📦 Estoque Atual",
        description="\n".join(lines),
        color=discord.Color.blue(),
    )
    embed.set_footer(text="Atualiza automaticamente a cada compra")
    embed.timestamp = discord.utils.utcnow()
    return embed


def build_resumo_embed_4(user_id: int) -> discord.Embed:
    descricao = [
        f"**Cliente:** <@{user_id}>",
        f"**Produto:** {PRODUCT4.nome}",
        f"**Valor unitario:** {format_brl(PRODUCT4.preco)}",
        "**Quantidade:** 1",
        f"**Total:** {format_brl(PRODUCT4.preco)}",
        "",
        "**Produtos no carrinho:** 1",
        f"**Valor a pagar:** {format_brl(PRODUCT4.preco)}",
        "**Cupom adicionado:** Sem cupom",
    ]
    return discord.Embed(
        title="Phantom Vendas | Resumo da Compra",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(47, 49, 54),
    )


def build_resumo_embed_5(user_id: int) -> discord.Embed:
    descricao = [
        f"**Cliente:** <@{user_id}>",
        f"**Produto:** {PRODUCT5.nome}",
        f"**Valor unitario:** {format_brl(PRODUCT5.preco)}",
        "**Quantidade:** 1",
        f"**Total:** {format_brl(PRODUCT5.preco)}",
        "",
        "**Produtos no carrinho:** 1",
        f"**Valor a pagar:** {format_brl(PRODUCT5.preco)}",
        "**Cupom adicionado:** Sem cupom",
    ]
    return discord.Embed(
        title="Phantom Vendas | Resumo da Compra",
        description="\n".join(descricao),
        color=discord.Color.from_rgb(47, 49, 54),
    )


def sanitize_channel_name(username: str, user_id: int) -> str:
    base = f"compra-{username}".lower()
    cleaned = re.sub(r"[^a-z0-9-]", "", base)[:20]
    return cleaned or f"compra-{str(user_id)[:5]}"


def build_redirect_view(channel_id: int) -> discord.ui.View:
    view = discord.ui.View(timeout=120)
    view.add_item(
        discord.ui.Button(
            label="Abrir Ticket",
            style=discord.ButtonStyle.link,
            url=f"https://discord.com/channels/{GUILD_ID_INT}/{channel_id}",
        )
    )
    return view


def build_ticket_url_view(url: str) -> discord.ui.View:
    view = discord.ui.View(timeout=300)
    view.add_item(
        discord.ui.Button(
            label="Abrir comprovante no Mercado Pago",
            style=discord.ButtonStyle.link,
            url=url,
        )
    )
    return view


def parse_checkout_reference(reference: str) -> tuple[int | None, int | None, str | None]:
    if not reference.startswith("checkout"):
        return None, None, None

    parts = reference.split(":")
    if len(parts) < 4:
        return None, None, None

    try:
        channel_id = int(parts[1])
        user_id = int(parts[2])
    except ValueError:
        return None, None, None

    return channel_id, user_id, parts[3]


def parse_support_ticket_topic(topic: str) -> tuple[str | None, int | None, int | None]:
    if not topic.startswith("support_ticket:"):
        return None, None, None

    parts = topic.split(":")
    if len(parts) != 4:
        return None, None, None

    ticket_type = parts[1]
    try:
        owner_id = int(parts[2])
        assignee_id = int(parts[3])
    except ValueError:
        return None, None, None

    return ticket_type, owner_id, assignee_id


def sanitize_ticket_channel_name(prefix: str, suffix: str) -> str:
    clean_prefix = re.sub(r"[^a-z0-9-]", "", prefix.lower())[:12] or "ticket"
    clean_suffix = re.sub(r"[^a-z0-9-]", "", suffix.lower())[:20] or "canal"
    return f"{clean_prefix}-{clean_suffix}"[:32]


def build_ticket_panel_embed() -> discord.Embed:
    embed = discord.Embed(
        title="Abrir Ticket",
        description=(
            "Escolha abaixo o tipo de atendimento que voce precisa.\n"
            "Ao clicar, seu ticket sera aberto e voce sera redirecionado."
        ),
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name="Duvida",
        value=(
            "💬 **SUPORTE DE DUVIDAS**\n\n"
            "Precisa de ajuda? Abra um ticket e nossa equipe ira te atender o mais rapido possivel 🤝."
        ),
        inline=False,
    )
    embed.add_field(
        name="Suporte",
        value=(
            "🛠️ **SUPORTE**\n\n"
            "Precisa de suporte em algo? Abra um ticket e nossa equipe ira te atender o mais rapido possivel 🤝."
        ),
        inline=False,
    )
    embed.add_field(
        name="🛒 Comprar",
        value=(
            "🛒 **COMPRAR**\n\n"
            "🎫 Suporte / Duvidas\n\n"
            "Ficou com alguma duvida ou teve algum problema na sua compra? Nao se preocupe!\n\n"
            "Nossa equipe esta pronta para te ajudar o mais rapido possivel. 💬"
        ),
        inline=False,
    )
    return embed


def build_ticket_created_embed(ticket_type: str, user_id: int) -> discord.Embed:
    embed = discord.Embed(
        title="🎟️ TICKET ABERTO 🚀",
        description="Fala! Seu ticket foi criado com sucesso 👑🔥\n\nNossa equipe já foi notificada e em breve alguém irá te atender. Enquanto isso, descreva sua dúvida ou problema com o **máximo de detalhes possível** 📩\n\n⚡ Quanto mais informações você enviar, mais rápido e eficiente será o atendimento!\n\n💬 Aguarde um membro da staff e evite marcar sem necessidade.\n\n🚀 Obrigado pela paciência, vamos resolver isso juntos!",
        color=discord.Color.from_rgb(47, 49, 54),
    )
    embed.add_field(
        name="📋 Informações",
        value=f"**Tipo:** {ticket_type.title()}\n**Cliente:** <@{user_id}>",
        inline=False,
    )
    return embed


class SupportTicketManageView(discord.ui.View):
    def __init__(self, bot: commands.Bot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Assumir Ticket",
        style=discord.ButtonStyle.success,
        custom_id="support_assumir_ticket",
    )
    async def assumir_ticket(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        role_id = get_assumir_ticket_role_id()
        if role_id and not await user_can_post_products(interaction, role_id):
            await interaction.response.send_message(
                "Apenas o cargo configurado para assumir tickets pode usar este botao.",
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "Este botao so funciona em canais de ticket.",
                ephemeral=True,
            )
            return

        ticket_type, owner_id, assignee_id = parse_support_ticket_topic(
            interaction.channel.topic or ""
        )
        if not ticket_type or not owner_id:
            await interaction.response.send_message(
                "Canal de ticket invalido.",
                ephemeral=True,
            )
            return

        if assignee_id and assignee_id != interaction.user.id:
            await interaction.response.send_message(
                f"Este ticket ja foi assumido por <@{assignee_id}>.",
                ephemeral=True,
            )
            return

        new_name = sanitize_ticket_channel_name("atendimento", interaction.user.name)
        new_topic = f"support_ticket:{ticket_type}:{owner_id}:{interaction.user.id}"
        await interaction.channel.edit(name=new_name, topic=new_topic)
        await interaction.channel.send(
            f"✅ Ticket assumido por {interaction.user.mention}."
        )
        await interaction.response.send_message("Ticket assumido com sucesso.", ephemeral=True)

    @discord.ui.button(
        label="Excluir Ticket",
        style=discord.ButtonStyle.danger,
        custom_id="support_excluir_ticket",
    )
    async def excluir_ticket(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        role_id = get_assumir_ticket_role_id()
        if role_id and not await user_can_post_products(interaction, role_id):
            await interaction.response.send_message(
                "Apenas o cargo configurado para tickets pode excluir tickets.",
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "Este botao so funciona em canais de ticket.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            "Ticket sera excluido em 3 segundos.", ephemeral=True
        )
        await asyncio.sleep(3)
        await interaction.channel.delete(reason=f"Ticket excluido por {interaction.user}")


class SupportTicketPanelView(discord.ui.View):
    def __init__(self, bot: commands.Bot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    async def _open_ticket(
        self, interaction: discord.Interaction, ticket_type: str
    ) -> None:
        guild = interaction.guild
        if guild is None:
            await interaction.response.send_message(
                "Este botao so funciona em servidor.",
                ephemeral=True,
            )
            return

        existing_ticket = discord.utils.find(
            lambda c: isinstance(c, discord.TextChannel)
            and (c.topic or "").startswith(
                f"support_ticket:{ticket_type}:{interaction.user.id}:"
            ),
            guild.channels,
        )
        if existing_ticket:
            await interaction.response.send_message(
                f"Voce ja possui ticket aberto em {existing_ticket.mention}.",
                ephemeral=True,
                view=build_redirect_view(existing_ticket.id),
            )
            return

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            interaction.user: discord.PermissionOverwrite(
                view_channel=True, send_messages=True, read_message_history=True
            ),
            guild.me: discord.PermissionOverwrite(
                view_channel=True,
                send_messages=True,
                read_message_history=True,
                manage_channels=True,
            ),
        }

        role_id = get_assumir_ticket_role_id()
        if role_id:
            staff_role = guild.get_role(role_id)
            if staff_role:
                overwrites[staff_role] = discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    manage_messages=True,
                )

        category = guild.get_channel(TICKETS_CATEGORY_ID_INT) if TICKETS_CATEGORY_ID_INT else None
        channel_name = sanitize_ticket_channel_name(ticket_type, str(interaction.user.id)[-5:])
        
        try:
            ticket_channel = await guild.create_text_channel(
                name=channel_name,
                category=category if isinstance(category, discord.CategoryChannel) else None,
                topic=f"support_ticket:{ticket_type}:{interaction.user.id}:0",
                overwrites=overwrites,
                reason=f"Ticket de suporte ({ticket_type}) para {interaction.user}",
            )

            await ticket_channel.send(
                embed=build_ticket_created_embed(ticket_type, interaction.user.id),
                view=SupportTicketManageView(self.bot),
            )

            log_embed = discord.Embed(
                title="🎟️ Ticket de Suporte Aberto",
                description=(
                    f"**Tipo:** {ticket_type.title()}\n"
                    f"**Cliente:** <@{interaction.user.id}>\n"
                    f"**Canal:** {ticket_channel.mention}"
                ),
                color=discord.Color.blurple(),
            )
            log_embed.timestamp = discord.utils.utcnow()
            await send_log(
                self.bot,
                log_embed,
                channel_id=LOG_TICKET_CHANNEL_ID_INT,
            )

            # Send the question embed
            question_embed = discord.Embed(
                description=(
                    f"{interaction.user.mention}\n\n"
                    "🚀 **PRECISA DE ALGUMA COISA?**\n\n"
                    "**Nossa equipe já está a caminho! Aguarde um instante, em breve um membro da staff ira te atender 🤝**\n\n"
                    "**Para agilizar o atendimento, ja explique aqui o que voce precisa, com o maximo de detalhes possivel 📩**"
                ),
                color=discord.Color.from_rgb(88, 166, 255),
            )
            question_embed.set_footer(text=f"Aguardando resposta...", icon_url=interaction.user.display_avatar.url)
            await ticket_channel.send(embed=question_embed)

            await interaction.response.send_message(
                f"Ticket criado em {ticket_channel.mention}.",
                ephemeral=True,
                view=build_redirect_view(ticket_channel.id),
            )
        except Exception as e:
            LOGGER.error(f"Erro ao criar ticket: {e}")
            await interaction.response.send_message(
                f"Erro ao criar ticket: {str(e)}",
                ephemeral=True,
            )

    @discord.ui.button(
        label="❓ 𝘿u𝙫𝙞𝙙𝙖",
        style=discord.ButtonStyle.primary,
        custom_id="support_abrir_duvida",
    )
    async def abrir_duvida(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._open_ticket(interaction, "duvida")

    @discord.ui.button(
        label="🛠️ 𝙨𝙪𝙥𝙤𝙧𝙩𝙚",
        style=discord.ButtonStyle.success,
        custom_id="support_abrir_suporte",
    )
    async def abrir_suporte(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._open_ticket(interaction, "suporte")

    @discord.ui.button(
        label="🛒 𝙘𝙤𝙢𝙥𝙧𝙖𝙧",
        style=discord.ButtonStyle.secondary,
        custom_id="support_abrir_comprar",
    )
    async def abrir_comprar(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await self._open_ticket(interaction, "comprar")


class PaymentMethodView(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Pagar com PIX",
        style=discord.ButtonStyle.success,
        custom_id="pagamento_pix",
    )
    async def pagar_pix(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not MP_ACCESS_TOKEN:
            await interaction.response.send_message(
                "Erro: MP_ACCESS_TOKEN nao configurado no .env.",
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "Este botao so funciona em um canal de checkout.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        apply_discount = (
            isinstance(interaction.channel, discord.TextChannel)
            and ACTIVE_DISCOUNT_BY_CHANNEL.get(interaction.channel.id)
            == interaction.user.id
        )

        try:
            payment = await self.bot.create_pix_payment(
                channel_id=interaction.channel.id,
                user_id=interaction.user.id,
                apply_discount=apply_discount,
            )
        except Exception as error:
            LOGGER.exception(
                "Falha ao gerar PIX. channel_id=%s user_id=%s",
                interaction.channel.id,
                interaction.user.id,
            )
            await interaction.followup.send(
                f"Erro ao gerar PIX no Mercado Pago: {error}",
                ephemeral=True,
            )
            return

        transaction_data = (
            payment.get("point_of_interaction", {})
            .get("transaction_data", {})
        )
        qr_code = transaction_data.get("qr_code", "")
        qr_code_base64 = transaction_data.get("qr_code_base64", "")
        ticket_url = transaction_data.get("ticket_url", "")
        payment_id = payment.get("id")
        payment_id_str = str(payment_id or "").strip()
        LOGGER.info(
            "PIX gerado com sucesso. payment_id=%s channel_id=%s user_id=%s",
            payment_id,
            interaction.channel.id,
            interaction.user.id,
        )

        if payment_id_str:
            upsert_payment_tracking(
                payment_id_str,
                status="pending",
                user_id=interaction.user.id,
                channel_id=interaction.channel.id,
                product_id=PRODUCT.product_id,
                amount=float(payment.get("transaction_amount", PRODUCT.preco)),
            )

        paid_amount = float(payment.get("transaction_amount", PRODUCT.preco))
        preco_formatted = format_brl(paid_amount)
        embed = discord.Embed(
            title="Phantom Vendas | Pagamento PIX",
            description=(
                f"**Valor:** {preco_formatted}\n"
                f"**ID do pagamento:** `{payment_id}`\n\n"
                "Pague com o QR code abaixo. A confirmacao e automatica."
            ),
            color=discord.Color.from_rgb(31, 35, 40),
        )
        embed.set_footer(text="Apos pagar, aguarde alguns segundos.")

        view = None
        if ticket_url:
            view = build_ticket_url_view(ticket_url)

        if qr_code_base64:
            qr_bytes = base64.b64decode(qr_code_base64)
            file = discord.File(io.BytesIO(qr_bytes), filename="pix-qrcode.png")
            embed.set_image(url="attachment://pix-qrcode.png")

            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )

            await interaction.channel.send(embed=embed, file=file, view=view)
        else:
            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )
            await interaction.channel.send(embed=embed, view=view)

        await interaction.followup.send(
            "PIX gerado com sucesso. Enviei os dados no canal de checkout.",
            ephemeral=True,
        )


class CheckoutView(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Ir para o Pagamento",
        style=discord.ButtonStyle.success,
        custom_id="checkout_pagamento",
    )
    async def pagamento(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        payment_view = PaymentMethodView(self.bot)
        await interaction.response.send_message(
            "**Escolha o metodo de pagamento:**",
            view=payment_view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Adicionar Cupom de Desconto",
        style=discord.ButtonStyle.primary,
        custom_id="checkout_cupom",
    )
    async def cupom(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(CouponModal(PRODUCT))

    @discord.ui.button(
        label="Cancelar Compra",
        style=discord.ButtonStyle.danger,
        custom_id="checkout_cancelar",
    )
    async def cancelar(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await interaction.response.send_message(
            "Compra cancelada. Este canal sera apagado em 5 segundos.",
            ephemeral=True,
        )
        canal_nome = interaction.channel.name if interaction.channel else "desconhecido"
        user_id = interaction.user.id
        await asyncio.sleep(5)
        if interaction.channel and isinstance(interaction.channel, discord.TextChannel):
            log_embed = discord.Embed(
                title="❌ Compra Cancelada",
                description=(
                    f"**Produto:** {PRODUCT.nome}\n"
                    f"**Cliente:** <@{user_id}>\n"
                    f"**Canal:** {canal_nome}"
                ),
                color=discord.Color.red(),
            )
            log_embed.timestamp = discord.utils.utcnow()
            await send_log(self.bot, log_embed, channel_id=LOG_CHECKOUT_CHANNEL_ID_INT)
            await interaction.channel.delete(reason="Checkout cancelado pelo usuario")


async def handle_checkout_click(
    bot: commands.Bot,
    interaction: discord.Interaction,
    product: Product,
    checkout_type: str,
    resumo_embed: discord.Embed,
    checkout_view: discord.ui.View,
) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            "Este botao so funciona em servidor.", ephemeral=True
        )
        return

    now = time.monotonic()
    cleanup_recent_checkout_trackers(now)
    interaction_id = int(getattr(interaction, "id", 0) or 0)
    if interaction_id:
        dedupe_key = (guild.id, interaction_id)
        last_ts = RECENT_CHECKOUT_INTERACTIONS.get(dedupe_key)
        if last_ts and (now - last_ts) <= CHECKOUT_INTERACTION_WINDOW_SECONDS:
            await interaction.response.send_message(
                "Clique duplicado detectado. Aguarde alguns segundos.",
                ephemeral=True,
            )
            return
        RECENT_CHECKOUT_INTERACTIONS[dedupe_key] = now

    await interaction.response.defer(ephemeral=True, thinking=True)

    if get_delivery_stock(product) <= 0:
        await interaction.followup.send(
            "Produto sem estoque no momento. Aguarde reposicao de codigos.",
            ephemeral=True,
        )
        return

    if not acquire_checkout_burst_guard(guild.id, interaction.user.id, checkout_type):
        existing_checkout = await find_open_checkout_for_user(
            guild,
            interaction.user.id,
            checkout_type,
        )
        if existing_checkout:
            await interaction.followup.send(
                f"Voce ja possui checkout aberto em {existing_checkout.mention}.",
                ephemeral=True,
                view=build_redirect_view(existing_checkout.id),
            )
        else:
            await interaction.followup.send(
                "Aguarde alguns segundos antes de clicar novamente em Comprar.",
                ephemeral=True,
            )
        return

    lock_key = (guild.id, interaction.user.id, checkout_type)
    lock = CHECKOUT_CREATION_LOCKS.setdefault(lock_key, asyncio.Lock())
    async with lock:
        file_lock = acquire_checkout_file_lock(
            guild.id,
            interaction.user.id,
            checkout_type,
        )
        if file_lock is None:
            await asyncio.sleep(0.5)
            existing_checkout = await find_open_checkout_for_user(
                guild,
                interaction.user.id,
                checkout_type,
            )
            if existing_checkout:
                await interaction.followup.send(
                    f"Voce ja possui checkout aberto em {existing_checkout.mention}.",
                    ephemeral=True,
                    view=build_redirect_view(existing_checkout.id),
                )
            else:
                await interaction.followup.send(
                    "Seu checkout ja esta sendo criado. Aguarde 2 segundos e tente novamente.",
                    ephemeral=True,
                )
            return

        try:
            existing_checkouts = await find_open_checkouts_for_user(
                guild,
                interaction.user.id,
                checkout_type,
            )
            if existing_checkouts:
                existing_checkouts.sort(key=lambda item: item.id)
                primary_checkout = existing_checkouts[0]
                for duplicate_checkout in existing_checkouts[1:]:
                    try:
                        await duplicate_checkout.delete(
                            reason=f"Checkout duplicado para user {interaction.user.id}",
                        )
                    except Exception:
                        LOGGER.warning(
                            "Falha ao remover checkout duplicado. channel_id=%s user_id=%s",
                            duplicate_checkout.id,
                            interaction.user.id,
                        )

                LOGGER.info(
                    "Checkout ja existente. channel_id=%s user_id=%s tipo=%s",
                    primary_checkout.id,
                    interaction.user.id,
                    checkout_type,
                )
                await interaction.followup.send(
                    f"Voce ja possui checkout aberto em {primary_checkout.mention}.",
                    ephemeral=True,
                    view=build_redirect_view(primary_checkout.id),
                )
                return

            channel_name = sanitize_channel_name(interaction.user.name, interaction.user.id)
            topic_value = f"{checkout_type}:{interaction.user.id}"

            overwrites = {
                guild.default_role: discord.PermissionOverwrite(view_channel=False),
                interaction.user: discord.PermissionOverwrite(
                    view_channel=True, send_messages=True, read_message_history=True
                ),
                guild.me: discord.PermissionOverwrite(
                    view_channel=True,
                    send_messages=True,
                    read_message_history=True,
                    manage_channels=True,
                ),
            }

            category = guild.get_channel(TICKETS_CATEGORY_ID_INT) if TICKETS_CATEGORY_ID_INT else None
            compra_channel = await guild.create_text_channel(
                name=channel_name,
                category=category if isinstance(category, discord.CategoryChannel) else None,
                topic=topic_value,
                overwrites=overwrites,
                reason=f"Checkout ({checkout_type}) para {interaction.user}",
            )

            primary_checkout = await enforce_single_checkout_for_user(
                guild,
                interaction.user.id,
                checkout_type,
            )
            if primary_checkout and primary_checkout.id != compra_channel.id:
                await interaction.followup.send(
                    f"Voce ja possui checkout aberto em {primary_checkout.mention}.",
                    ephemeral=True,
                    view=build_redirect_view(primary_checkout.id),
                )
                return

            await compra_channel.send(embed=resumo_embed, view=checkout_view)
            await compra_channel.send(f"{interaction.user.mention} seu ticket foi aberto.")
            LOGGER.info(
                "Checkout criado. channel_id=%s user_id=%s guild_id=%s tipo=%s",
                compra_channel.id,
                interaction.user.id,
                interaction.guild_id,
                checkout_type,
            )
            log_embed = discord.Embed(
                title="🎫 Ticket Aberto",
                description=(
                    f"**Produto:** {product.nome}\n"
                    f"**Cliente:** <@{interaction.user.id}>\n"
                    f"**Canal:** {compra_channel.mention}\n"
                    f"**Valor:** {format_brl(product.preco)}"
                ),
                color=discord.Color.blurple(),
            )
            log_embed.timestamp = discord.utils.utcnow()
            await send_log(bot, log_embed, channel_id=LOG_CHECKOUT_CHANNEL_ID_INT)
            await interaction.followup.send(
                f"Seu checkout foi criado em {compra_channel.mention}. Clique no botao para abrir.",
                ephemeral=True,
                view=build_redirect_view(compra_channel.id),
            )
        finally:
            release_checkout_file_lock(file_lock)


class ProductView(discord.ui.View):
    def __init__(self, bot: commands.Bot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Comprar",
        style=discord.ButtonStyle.success,
        custom_id=f"comprar_{PRODUCT.product_id}",
    )
    async def comprar(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await handle_checkout_click(
            bot=self.bot,
            interaction=interaction,
            product=PRODUCT,
            checkout_type="checkout",
            resumo_embed=build_resumo_embed(interaction.user.id),
            checkout_view=CheckoutView(self.bot),
        )


class PaymentMethodView2(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Pagar com PIX",
        style=discord.ButtonStyle.success,
        custom_id="pagamento_pix_2",
    )
    async def pagar_pix(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not MP_ACCESS_TOKEN:
            await interaction.response.send_message(
                "Erro: MP_ACCESS_TOKEN nao configurado no .env.",
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "Este botao so funciona em um canal de checkout.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        apply_discount = (
            isinstance(interaction.channel, discord.TextChannel)
            and ACTIVE_DISCOUNT_BY_CHANNEL.get(interaction.channel.id)
            == interaction.user.id
        )

        try:
            payment = await self.bot.create_pix_payment_2(
                channel_id=interaction.channel.id,
                user_id=interaction.user.id,
                apply_discount=apply_discount,
            )
        except Exception as error:
            LOGGER.exception(
                "Falha ao gerar PIX (produto 2). channel_id=%s user_id=%s",
                interaction.channel.id,
                interaction.user.id,
            )
            await interaction.followup.send(
                f"Erro ao gerar PIX no Mercado Pago: {error}",
                ephemeral=True,
            )
            return

        transaction_data = (
            payment.get("point_of_interaction", {})
            .get("transaction_data", {})
        )
        qr_code = transaction_data.get("qr_code", "")
        qr_code_base64 = transaction_data.get("qr_code_base64", "")
        ticket_url = transaction_data.get("ticket_url", "")
        payment_id = payment.get("id")
        payment_id_str = str(payment_id or "").strip()
        LOGGER.info(
            "PIX gerado (produto 2). payment_id=%s channel_id=%s user_id=%s",
            payment_id,
            interaction.channel.id,
            interaction.user.id,
        )

        if payment_id_str:
            upsert_payment_tracking(
                payment_id_str,
                status="pending",
                user_id=interaction.user.id,
                channel_id=interaction.channel.id,
                product_id=PRODUCT2.product_id,
                amount=float(payment.get("transaction_amount", PRODUCT2.preco)),
            )

        paid_amount = float(payment.get("transaction_amount", PRODUCT2.preco))
        preco_formatted = format_brl(paid_amount)
        embed = discord.Embed(
            title="Phantom Vendas | Pagamento PIX",
            description=(
                f"**Valor:** {preco_formatted}\n"
                f"**ID do pagamento:** `{payment_id}`\n\n"
                "Pague com o QR code abaixo. A confirmacao e automatica."
            ),
            color=discord.Color.from_rgb(31, 35, 40),
        )
        embed.set_footer(text="Apos pagar, aguarde alguns segundos.")

        view = None
        if ticket_url:
            view = build_ticket_url_view(ticket_url)

        if qr_code_base64:
            qr_bytes = base64.b64decode(qr_code_base64)
            file = discord.File(io.BytesIO(qr_bytes), filename="pix-qrcode.png")
            embed.set_image(url="attachment://pix-qrcode.png")

            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )

            await interaction.channel.send(embed=embed, file=file, view=view)
        else:
            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )
            await interaction.channel.send(embed=embed, view=view)

        await interaction.followup.send(
            "PIX gerado com sucesso. Enviei os dados no canal de checkout.",
            ephemeral=True,
        )


class CheckoutView2(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Ir para o Pagamento",
        style=discord.ButtonStyle.success,
        custom_id="checkout_pagamento_2",
    )
    async def pagamento(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        payment_view = PaymentMethodView2(self.bot)
        await interaction.response.send_message(
            "**Escolha o metodo de pagamento:**",
            view=payment_view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Adicionar Cupom de Desconto",
        style=discord.ButtonStyle.primary,
        custom_id="checkout_cupom_2",
    )
    async def cupom(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(CouponModal(PRODUCT2))


class CouponModal(discord.ui.Modal):
    def __init__(self, product: Product) -> None:
        super().__init__(title="Aplicar Cupom")
        self.product = product

    coupon_code = discord.ui.TextInput(
        label="Digite o cupom",
        placeholder="Ex.: DESCONTO10",
        min_length=3,
        max_length=30,
        required=True,
    )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "Cupom so pode ser aplicado no canal de checkout.",
                ephemeral=True,
            )
            return

        if interaction.user.id in DISCOUNT_USED_USERS:
            await interaction.response.send_message(
                "Voce ja usou este cupom anteriormente. Ele so pode ser usado 1 vez por pessoa.",
                ephemeral=True,
            )
            return

        code = str(self.coupon_code).strip().upper()
        if code != DISCOUNT_CODE:
            await interaction.response.send_message(
                "Cupom invalido.",
                ephemeral=True,
            )
            return

        if ACTIVE_DISCOUNT_BY_CHANNEL.get(interaction.channel.id) == interaction.user.id:
            await interaction.response.send_message(
                "Este checkout ja esta com o cupom aplicado.",
                ephemeral=True,
            )
            return

        ACTIVE_DISCOUNT_BY_CHANNEL[interaction.channel.id] = interaction.user.id
        DISCOUNT_USED_USERS.add(interaction.user.id)
        save_discount_usage(DISCOUNT_USED_USERS)

        original = self.product.preco
        discounted = get_discounted_amount(original)
        await interaction.response.send_message(
            (
                f"Cupom aplicado com sucesso: **{DISCOUNT_CODE}**\n"
                f"Valor original: {format_brl(original)}\n"
                f"Valor com 10% OFF: {format_brl(discounted)}"
            ),
            ephemeral=True,
        )

    @discord.ui.button(
        label="Cancelar Compra",
        style=discord.ButtonStyle.danger,
        custom_id="checkout_cancelar_2",
    )
    async def cancelar(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await interaction.response.send_message(
            "Compra cancelada. Este canal sera apagado em 5 segundos.",
            ephemeral=True,
        )
        canal_nome = interaction.channel.name if interaction.channel else "desconhecido"
        user_id = interaction.user.id
        await asyncio.sleep(5)
        if interaction.channel and isinstance(interaction.channel, discord.TextChannel):
            log_embed = discord.Embed(
                title="❌ Compra Cancelada",
                description=(
                    f"**Produto:** {PRODUCT2.nome}\n"
                    f"**Cliente:** <@{user_id}>\n"
                    f"**Canal:** {canal_nome}"
                ),
                color=discord.Color.red(),
            )
            log_embed.timestamp = discord.utils.utcnow()
            await send_log(self.bot, log_embed, channel_id=LOG_CHECKOUT_CHANNEL_ID_INT)
            await interaction.channel.delete(reason="Checkout cancelado pelo usuario")


class ProductView2(discord.ui.View):
    def __init__(self, bot: commands.Bot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Comprar",
        style=discord.ButtonStyle.success,
        custom_id=f"comprar_{PRODUCT2.product_id}",
    )
    async def comprar(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await handle_checkout_click(
            bot=self.bot,
            interaction=interaction,
            product=PRODUCT2,
            checkout_type="checkout2",
            resumo_embed=build_resumo_embed_2(interaction.user.id),
            checkout_view=CheckoutView2(self.bot),
        )


class PaymentMethodView3(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Pagar com PIX",
        style=discord.ButtonStyle.success,
        custom_id="pagamento_pix_3",
    )
    async def pagar_pix(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not MP_ACCESS_TOKEN:
            await interaction.response.send_message(
                "Erro: MP_ACCESS_TOKEN nao configurado no .env.",
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "Este botao so funciona em um canal de checkout.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        apply_discount = (
            isinstance(interaction.channel, discord.TextChannel)
            and ACTIVE_DISCOUNT_BY_CHANNEL.get(interaction.channel.id)
            == interaction.user.id
        )

        try:
            payment = await self.bot.create_pix_payment_3(
                channel_id=interaction.channel.id,
                user_id=interaction.user.id,
                apply_discount=apply_discount,
            )
        except Exception as error:
            LOGGER.exception(
                "Falha ao gerar PIX (produto 3). channel_id=%s user_id=%s",
                interaction.channel.id,
                interaction.user.id,
            )
            await interaction.followup.send(
                f"Erro ao gerar PIX no Mercado Pago: {error}",
                ephemeral=True,
            )
            return

        transaction_data = (
            payment.get("point_of_interaction", {})
            .get("transaction_data", {})
        )
        qr_code = transaction_data.get("qr_code", "")
        qr_code_base64 = transaction_data.get("qr_code_base64", "")
        ticket_url = transaction_data.get("ticket_url", "")
        payment_id = payment.get("id")
        payment_id_str = str(payment_id or "").strip()
        LOGGER.info(
            "PIX gerado (produto 3). payment_id=%s channel_id=%s user_id=%s",
            payment_id,
            interaction.channel.id,
            interaction.user.id,
        )

        if payment_id_str:
            upsert_payment_tracking(
                payment_id_str,
                status="pending",
                user_id=interaction.user.id,
                channel_id=interaction.channel.id,
                product_id=PRODUCT3.product_id,
                amount=float(payment.get("transaction_amount", PRODUCT3.preco)),
            )

        paid_amount = float(payment.get("transaction_amount", PRODUCT3.preco))
        preco_formatted = format_brl(paid_amount)
        embed = discord.Embed(
            title="Phantom Vendas | Pagamento PIX",
            description=(
                f"**Valor:** {preco_formatted}\n"
                f"**ID do pagamento:** `{payment_id}`\n\n"
                "Pague com o QR code abaixo. A confirmacao e automatica."
            ),
            color=discord.Color.from_rgb(31, 35, 40),
        )
        embed.set_footer(text="Apos pagar, aguarde alguns segundos.")

        view = None
        if ticket_url:
            view = build_ticket_url_view(ticket_url)

        if qr_code_base64:
            qr_bytes = base64.b64decode(qr_code_base64)
            file = discord.File(io.BytesIO(qr_bytes), filename="pix-qrcode.png")
            embed.set_image(url="attachment://pix-qrcode.png")

            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )

            await interaction.channel.send(embed=embed, file=file, view=view)
        else:
            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )
            await interaction.channel.send(embed=embed, view=view)

        await interaction.followup.send(
            "PIX gerado com sucesso. Enviei os dados no canal de checkout.",
            ephemeral=True,
        )


class CheckoutView3(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Ir para o Pagamento",
        style=discord.ButtonStyle.success,
        custom_id="checkout_pagamento_3",
    )
    async def pagamento(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        payment_view = PaymentMethodView3(self.bot)
        await interaction.response.send_message(
            "**Escolha o metodo de pagamento:**",
            view=payment_view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Adicionar Cupom de Desconto",
        style=discord.ButtonStyle.primary,
        custom_id="checkout_cupom_3",
    )
    async def cupom(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(CouponModal(PRODUCT3))

    @discord.ui.button(
        label="Cancelar Compra",
        style=discord.ButtonStyle.danger,
        custom_id="checkout_cancelar_3",
    )
    async def cancelar(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await interaction.response.send_message(
            "Compra cancelada. Este canal sera apagado em 5 segundos.",
            ephemeral=True,
        )
        canal_nome = interaction.channel.name if interaction.channel else "desconhecido"
        user_id = interaction.user.id
        await asyncio.sleep(5)
        if interaction.channel and isinstance(interaction.channel, discord.TextChannel):
            log_embed = discord.Embed(
                title="❌ Compra Cancelada",
                description=(
                    f"**Produto:** {PRODUCT3.nome}\n"
                    f"**Cliente:** <@{user_id}>\n"
                    f"**Canal:** {canal_nome}"
                ),
                color=discord.Color.red(),
            )
            log_embed.timestamp = discord.utils.utcnow()
            await send_log(self.bot, log_embed, channel_id=LOG_CHECKOUT_CHANNEL_ID_INT)
            await interaction.channel.delete(reason="Checkout cancelado pelo usuario")


class ProductView3(discord.ui.View):
    def __init__(self, bot: commands.Bot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Comprar",
        style=discord.ButtonStyle.success,
        custom_id=f"comprar_{PRODUCT3.product_id}",
    )
    async def comprar(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await handle_checkout_click(
            bot=self.bot,
            interaction=interaction,
            product=PRODUCT3,
            checkout_type="checkout3",
            resumo_embed=build_resumo_embed_3(interaction.user.id),
            checkout_view=CheckoutView3(self.bot),
        )


class PaymentMethodView4(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Pagar com PIX",
        style=discord.ButtonStyle.success,
        custom_id="pagamento_pix_4",
    )
    async def pagar_pix(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not MP_ACCESS_TOKEN:
            await interaction.response.send_message(
                "Erro: MP_ACCESS_TOKEN nao configurado no .env.",
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "Este botao so funciona em um canal de checkout.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        apply_discount = (
            isinstance(interaction.channel, discord.TextChannel)
            and ACTIVE_DISCOUNT_BY_CHANNEL.get(interaction.channel.id)
            == interaction.user.id
        )

        try:
            payment = await self.bot.create_pix_payment_4(
                channel_id=interaction.channel.id,
                user_id=interaction.user.id,
                apply_discount=apply_discount,
            )
        except Exception as error:
            LOGGER.exception(
                "Falha ao gerar PIX (produto 4). channel_id=%s user_id=%s",
                interaction.channel.id,
                interaction.user.id,
            )
            await interaction.followup.send(
                f"Erro ao gerar PIX no Mercado Pago: {error}",
                ephemeral=True,
            )
            return

        transaction_data = (
            payment.get("point_of_interaction", {})
            .get("transaction_data", {})
        )
        qr_code = transaction_data.get("qr_code", "")
        qr_code_base64 = transaction_data.get("qr_code_base64", "")
        ticket_url = transaction_data.get("ticket_url", "")
        payment_id = payment.get("id")
        payment_id_str = str(payment_id or "").strip()
        LOGGER.info(
            "PIX gerado (produto 4). payment_id=%s channel_id=%s user_id=%s",
            payment_id,
            interaction.channel.id,
            interaction.user.id,
        )

        if payment_id_str:
            upsert_payment_tracking(
                payment_id_str,
                status="pending",
                user_id=interaction.user.id,
                channel_id=interaction.channel.id,
                product_id=PRODUCT4.product_id,
                amount=float(payment.get("transaction_amount", PRODUCT4.preco)),
            )

        paid_amount = float(payment.get("transaction_amount", PRODUCT4.preco))
        preco_formatted = format_brl(paid_amount)
        embed = discord.Embed(
            title="Phantom Vendas | Pagamento PIX",
            description=(
                f"**Valor:** {preco_formatted}\n"
                f"**ID do pagamento:** `{payment_id}`\n\n"
                "Pague com o QR code abaixo. A confirmacao e automatica."
            ),
            color=discord.Color.from_rgb(31, 35, 40),
        )
        embed.set_footer(text="Apos pagar, aguarde alguns segundos.")

        view = None
        if ticket_url:
            view = build_ticket_url_view(ticket_url)

        if qr_code_base64:
            qr_bytes = base64.b64decode(qr_code_base64)
            file = discord.File(io.BytesIO(qr_bytes), filename="pix-qrcode.png")
            embed.set_image(url="attachment://pix-qrcode.png")

            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )

            await interaction.channel.send(embed=embed, file=file, view=view)
        else:
            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )
            await interaction.channel.send(embed=embed, view=view)

        await interaction.followup.send(
            "PIX gerado com sucesso. Enviei os dados no canal de checkout.",
            ephemeral=True,
        )


class CheckoutView4(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Ir para o Pagamento",
        style=discord.ButtonStyle.success,
        custom_id="checkout_pagamento_4",
    )
    async def pagamento(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        payment_view = PaymentMethodView4(self.bot)
        await interaction.response.send_message(
            "**Escolha o metodo de pagamento:**",
            view=payment_view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Adicionar Cupom de Desconto",
        style=discord.ButtonStyle.primary,
        custom_id="checkout_cupom_4",
    )
    async def cupom(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(CouponModal(PRODUCT4))

    @discord.ui.button(
        label="Cancelar Compra",
        style=discord.ButtonStyle.danger,
        custom_id="checkout_cancelar_4",
    )
    async def cancelar(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await interaction.response.send_message(
            "Compra cancelada. Este canal sera apagado em 5 segundos.",
            ephemeral=True,
        )
        canal_nome = interaction.channel.name if interaction.channel else "desconhecido"
        user_id = interaction.user.id
        await asyncio.sleep(5)
        if interaction.channel and isinstance(interaction.channel, discord.TextChannel):
            log_embed = discord.Embed(
                title="❌ Compra Cancelada",
                description=(
                    f"**Produto:** {PRODUCT4.nome}\n"
                    f"**Cliente:** <@{user_id}>\n"
                    f"**Canal:** {canal_nome}"
                ),
                color=discord.Color.red(),
            )
            log_embed.timestamp = discord.utils.utcnow()
            await send_log(self.bot, log_embed, channel_id=LOG_CHECKOUT_CHANNEL_ID_INT)
            await interaction.channel.delete(reason="Checkout cancelado pelo usuario")


class ProductView4(discord.ui.View):
    def __init__(self, bot: commands.Bot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Comprar",
        style=discord.ButtonStyle.success,
        custom_id=f"comprar_{PRODUCT4.product_id}",
    )
    async def comprar(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await handle_checkout_click(
            bot=self.bot,
            interaction=interaction,
            product=PRODUCT4,
            checkout_type="checkout4",
            resumo_embed=build_resumo_embed_4(interaction.user.id),
            checkout_view=CheckoutView4(self.bot),
        )


class PaymentMethodView5(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Pagar com PIX",
        style=discord.ButtonStyle.success,
        custom_id="pagamento_pix_5",
    )
    async def pagar_pix(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        if not MP_ACCESS_TOKEN:
            await interaction.response.send_message(
                "Erro: MP_ACCESS_TOKEN nao configurado no .env.",
                ephemeral=True,
            )
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await interaction.response.send_message(
                "Este botao so funciona em um canal de checkout.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        apply_discount = (
            isinstance(interaction.channel, discord.TextChannel)
            and ACTIVE_DISCOUNT_BY_CHANNEL.get(interaction.channel.id)
            == interaction.user.id
        )

        try:
            payment = await self.bot.create_pix_payment_5(
                channel_id=interaction.channel.id,
                user_id=interaction.user.id,
                apply_discount=apply_discount,
            )
        except Exception as error:
            LOGGER.exception(
                "Falha ao gerar PIX (produto 5). channel_id=%s user_id=%s",
                interaction.channel.id,
                interaction.user.id,
            )
            await interaction.followup.send(
                f"Erro ao gerar PIX no Mercado Pago: {error}",
                ephemeral=True,
            )
            return

        transaction_data = (
            payment.get("point_of_interaction", {})
            .get("transaction_data", {})
        )
        qr_code = transaction_data.get("qr_code", "")
        qr_code_base64 = transaction_data.get("qr_code_base64", "")
        ticket_url = transaction_data.get("ticket_url", "")
        payment_id = payment.get("id")
        payment_id_str = str(payment_id or "").strip()
        LOGGER.info(
            "PIX gerado (produto 5). payment_id=%s channel_id=%s user_id=%s",
            payment_id,
            interaction.channel.id,
            interaction.user.id,
        )

        if payment_id_str:
            upsert_payment_tracking(
                payment_id_str,
                status="pending",
                user_id=interaction.user.id,
                channel_id=interaction.channel.id,
                product_id=PRODUCT5.product_id,
                amount=float(payment.get("transaction_amount", PRODUCT5.preco)),
            )

        paid_amount = float(payment.get("transaction_amount", PRODUCT5.preco))
        preco_formatted = format_brl(paid_amount)
        embed = discord.Embed(
            title="Phantom Vendas | Pagamento PIX",
            description=(
                f"**Valor:** {preco_formatted}\n"
                f"**ID do pagamento:** `{payment_id}`\n\n"
                "Pague com o QR code abaixo. A confirmacao e automatica."
            ),
            color=discord.Color.from_rgb(31, 35, 40),
        )
        embed.set_footer(text="Apos pagar, aguarde alguns segundos.")

        view = None
        if ticket_url:
            view = build_ticket_url_view(ticket_url)

        if qr_code_base64:
            qr_bytes = base64.b64decode(qr_code_base64)
            file = discord.File(io.BytesIO(qr_bytes), filename="pix-qrcode.png")
            embed.set_image(url="attachment://pix-qrcode.png")

            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )

            await interaction.channel.send(embed=embed, file=file, view=view)
        else:
            if qr_code:
                embed.add_field(
                    name="PIX copia e cola",
                    value=f"```{qr_code[:1000]}```",
                    inline=False,
                )
            await interaction.channel.send(embed=embed, view=view)

        await interaction.followup.send(
            "PIX gerado com sucesso. Enviei os dados no canal de checkout.",
            ephemeral=True,
        )


class CheckoutView5(discord.ui.View):
    def __init__(self, bot: "LojaBot") -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Ir para o Pagamento",
        style=discord.ButtonStyle.success,
        custom_id="checkout_pagamento_5",
    )
    async def pagamento(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        payment_view = PaymentMethodView5(self.bot)
        await interaction.response.send_message(
            "**Escolha o metodo de pagamento:**",
            view=payment_view,
            ephemeral=True,
        )

    @discord.ui.button(
        label="Adicionar Cupom de Desconto",
        style=discord.ButtonStyle.primary,
        custom_id="checkout_cupom_5",
    )
    async def cupom(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await interaction.response.send_modal(CouponModal(PRODUCT5))

    @discord.ui.button(
        label="Cancelar Compra",
        style=discord.ButtonStyle.danger,
        custom_id="checkout_cancelar_5",
    )
    async def cancelar(
        self, interaction: discord.Interaction, button: discord.ui.Button
    ) -> None:
        await interaction.response.send_message(
            "Compra cancelada. Este canal sera apagado em 5 segundos.",
            ephemeral=True,
        )
        canal_nome = interaction.channel.name if interaction.channel else "desconhecido"
        user_id = interaction.user.id
        await asyncio.sleep(5)
        if interaction.channel and isinstance(interaction.channel, discord.TextChannel):
            log_embed = discord.Embed(
                title="❌ Compra Cancelada",
                description=(
                    f"**Produto:** {PRODUCT5.nome}\n"
                    f"**Cliente:** <@{user_id}>\n"
                    f"**Canal:** {canal_nome}"
                ),
                color=discord.Color.red(),
            )
            log_embed.timestamp = discord.utils.utcnow()
            await send_log(self.bot, log_embed, channel_id=LOG_CHECKOUT_CHANNEL_ID_INT)
            await interaction.channel.delete(reason="Checkout cancelado pelo usuario")


class ProductView5(discord.ui.View):
    def __init__(self, bot: commands.Bot) -> None:
        super().__init__(timeout=None)
        self.bot = bot

    @discord.ui.button(
        label="Comprar",
        style=discord.ButtonStyle.success,
        custom_id=f"comprar_{PRODUCT5.product_id}",
    )
    async def comprar(self, interaction: discord.Interaction, button: discord.ui.Button) -> None:
        await handle_checkout_click(
            bot=self.bot,
            interaction=interaction,
            product=PRODUCT5,
            checkout_type="checkout5",
            resumo_embed=build_resumo_embed_5(interaction.user.id),
            checkout_view=CheckoutView5(self.bot),
        )


class LojaBot(commands.Bot):
    def __init__(self) -> None:
        intents = discord.Intents.default()
        intents.message_content = ENABLE_MESSAGE_CONTENT
        intents.members = ENABLE_MEMBERS_INTENT
        super().__init__(command_prefix="!", intents=intents)
        self.http_session: ClientSession | None = None
        self.web_runner: web.AppRunner | None = None
        self.stock_sync_task: asyncio.Task | None = None
        self.last_env_mtime: float | None = None

    async def setup_hook(self) -> None:
        self.http_session = ClientSession(timeout=ClientTimeout(total=30))
        await self.start_webhook_server()
        await self.cleanup_duplicate_checkout_channels()
        await self.reconcile_pending_payments(limit=40)
        await self.refresh_all_product_stock_messages()
        self.last_env_mtime = self.get_env_mtime()
        self.stock_sync_task = asyncio.create_task(self.stock_sync_loop())
        self.add_view(ProductView(self))
        self.add_view(CheckoutView(self))
        self.add_view(PaymentMethodView(self))
        self.add_view(ProductView2(self))
        self.add_view(CheckoutView2(self))
        self.add_view(PaymentMethodView2(self))
        self.add_view(ProductView3(self))
        self.add_view(CheckoutView3(self))
        self.add_view(PaymentMethodView3(self))
        self.add_view(ProductView4(self))
        self.add_view(CheckoutView4(self))
        self.add_view(PaymentMethodView4(self))
        self.add_view(ProductView5(self))
        self.add_view(CheckoutView5(self))
        self.add_view(PaymentMethodView5(self))
        self.add_view(SupportTicketPanelView(self))
        self.add_view(SupportTicketManageView(self))
        guild_obj = discord.Object(id=GUILD_ID_INT)
        self.tree.add_command(postar_produto, guild=guild_obj)
        self.tree.add_command(postar_produto2, guild=guild_obj)
        self.tree.add_command(postar_produto3, guild=guild_obj)
        self.tree.add_command(postar_produto4, guild=guild_obj)
        self.tree.add_command(postar_produto5, guild=guild_obj)
        self.tree.add_command(ticket, guild=guild_obj)
        self.tree.add_command(aceitar_compra, guild=guild_obj)
        self.tree.add_command(resetar_entrega, guild=guild_obj)
        self.tree.add_command(ver_estoque, guild=guild_obj)
        self.tree.add_command(postar_estoque, guild=guild_obj)
        self.tree.add_command(painel_pedidos, guild=guild_obj)
        await self.tree.sync(guild=guild_obj)

    async def close(self) -> None:
        if self.stock_sync_task:
            self.stock_sync_task.cancel()
            self.stock_sync_task = None

        if self.web_runner:
            await self.web_runner.cleanup()
            self.web_runner = None

        if self.http_session and not self.http_session.closed:
            await self.http_session.close()
            self.http_session = None

        await super().close()

    def get_env_mtime(self) -> float | None:
        env_file = Path(__file__).resolve().parent / ".env"
        try:
            return env_file.stat().st_mtime
        except OSError:
            return None

    async def stock_sync_loop(self) -> None:
        while not self.is_closed():
            try:
                current_mtime = self.get_env_mtime()
                if current_mtime is not None and current_mtime != self.last_env_mtime:
                    self.last_env_mtime = current_mtime
                    await self.refresh_all_product_stock_messages()
            except Exception as error:
                LOGGER.warning("Falha na sincronizacao automatica de estoque: %s", error)

            await asyncio.sleep(12)

    async def reconcile_pending_payments(self, limit: int = 30) -> None:
        candidates = iter_reconcile_candidates(limit=limit)
        if not candidates:
            return

        reconciled = 0
        for payment_id, record in candidates:
            try:
                details = await self.get_payment_details(payment_id)
            except Exception:
                upsert_payment_tracking(payment_id, status="error_lookup")
                continue

            status = str(details.get("status") or "").lower()
            if status != "approved":
                upsert_payment_tracking(payment_id, status=f"mp_{status or 'unknown'}")
                continue

            external_reference = str(details.get("external_reference") or "")
            channel_id, user_id, product_id = parse_checkout_reference(external_reference)
            if not channel_id:
                try:
                    channel_id = int(record.get("channel_id") or 0)
                except (TypeError, ValueError):
                    channel_id = None
            if not user_id:
                try:
                    user_id = int(record.get("user_id") or 0)
                except (TypeError, ValueError):
                    user_id = None
            if not product_id:
                product_id = str(record.get("product_id") or "")

            produto = get_product_by_id(product_id) or PRODUCT
            upsert_payment_tracking(
                payment_id,
                status="processing",
                user_id=user_id,
                channel_id=channel_id,
                product_id=produto.product_id,
                amount=float(details.get("transaction_amount", produto.preco)),
            )

            if not user_id:
                upsert_payment_tracking(payment_id, status="error_missing_user")
                continue

            if channel_id and not bool(record.get("approval_notified")):
                channel = self.get_channel(channel_id)
                if channel is None:
                    try:
                        channel = await self.fetch_channel(channel_id)
                    except Exception:
                        channel = None

                if isinstance(channel, discord.TextChannel):
                    amount = float(details.get("transaction_amount", produto.preco))
                    reconcile_embed = discord.Embed(
                        description=(
                            "✅ Pagamento aprovado detectado na reconciliacao. "
                            "Seu produto sera tratado manualmente."
                        ),
                        color=discord.Color.green(),
                    )
                    reconcile_embed.set_footer(
                        text=f"ID do pagamento: {payment_id} • Valor: {format_brl(amount)}"
                    )
                    reconcile_embed.timestamp = discord.utils.utcnow()
                    try:
                        await channel.send(content=f"<@{user_id}>", embed=reconcile_embed)
                        upsert_payment_tracking(payment_id, approval_notified=True)
                    except Exception:
                        pass

            delivery_status = await send_product_delivery_dm(self, user_id, produto, payment_id)
            if delivery_status == "missing_code":
                upsert_payment_tracking(
                    payment_id,
                    status="approved_no_stock",
                    delivery_status=delivery_status,
                )
            else:
                upsert_payment_tracking(
                    payment_id,
                    status="approved",
                    delivery_status=delivery_status,
                )

            PROCESSED_PAYMENTS.add(payment_id)
            reconciled += 1

        if reconciled:
            LOGGER.warning("Reconciliacao processou %s pagamento(s) pendente(s)", reconciled)

    async def refresh_product_stock_message(self, product: Product) -> None:
        channel_id = get_loja_channel_id_for_product(product)
        if not channel_id:
            return

        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except Exception:
                return

        if not isinstance(channel, discord.TextChannel):
            return

        target_custom_id = f"comprar_{product.product_id}"
        bot_user_id = self.user.id if self.user else None
        embed_builder = get_product_embed_builder(product)
        new_embed = embed_builder()
        new_view = build_product_view_for(product, self)

        try:
            updated_any = False
            stored_message_id = get_product_message_ref(
                channel.guild.id,
                channel.id,
                product.product_id,
            )
            if stored_message_id:
                try:
                    stored_message = await channel.fetch_message(stored_message_id)
                    if bot_user_id is None or stored_message.author.id == bot_user_id:
                        current_description = stored_message.embeds[0].description if stored_message.embeds else ""
                        if current_description != (new_embed.description or ""):
                            await stored_message.edit(embed=new_embed, view=new_view)
                        updated_any = True
                except Exception:
                    clear_product_message_ref(channel.guild.id, channel.id, product.product_id)

            latest_message_id: int | None = None
            async for message in channel.history(limit=None):
                if bot_user_id is not None and message.author.id != bot_user_id:
                    continue
                if not message.embeds:
                    continue
                if not message_has_button_custom_id(
                    message,
                    target_custom_id,
                ) and not message_is_product_card_for(message, product):
                    continue

                if latest_message_id is None:
                    latest_message_id = message.id

                current_description = message.embeds[0].description or ""
                if current_description != (new_embed.description or ""):
                    await message.edit(embed=new_embed, view=new_view)
                updated_any = True

            if latest_message_id is not None:
                set_product_message_ref(
                    channel.guild.id,
                    channel.id,
                    product.product_id,
                    latest_message_id,
                )
            elif stored_message_id and not updated_any:
                clear_product_message_ref(channel.guild.id, channel.id, product.product_id)
        except Exception as error:
            LOGGER.warning(
                "Falha ao atualizar card de estoque. product_id=%s erro=%s",
                product.product_id,
                error,
            )

    async def refresh_all_product_stock_messages(self) -> None:
        for product in (PRODUCT, PRODUCT2, PRODUCT3, PRODUCT4, PRODUCT5):
            await self.refresh_product_stock_message(product)
        await self.refresh_public_stock_message()

    async def refresh_public_stock_message(self) -> None:
        if not STOCK_MESSAGE_REF:
            return

        channel_id = STOCK_MESSAGE_REF.get("channel_id")
        message_id = STOCK_MESSAGE_REF.get("message_id")
        if not channel_id or not message_id:
            clear_stock_message_ref()
            return

        channel = self.get_channel(channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(channel_id)
            except Exception:
                clear_stock_message_ref()
                return

        if not isinstance(channel, discord.TextChannel):
            clear_stock_message_ref()
            return

        try:
            message = await channel.fetch_message(message_id)
        except Exception:
            clear_stock_message_ref()
            return

        try:
            await message.edit(embed=build_stock_overview_embed())
        except Exception as error:
            LOGGER.warning("Falha ao atualizar painel publico de estoque: %s", error)

    async def start_webhook_server(self) -> None:
        app = web.Application()
        app.router.add_get("/webhook/mercadopago", self.webhook_health)
        app.router.add_post("/webhook/mercadopago", self.webhook_mercadopago)

        self.web_runner = web.AppRunner(app)
        await self.web_runner.setup()

        candidate_ports = [WEBHOOK_PORT, 9090, 8081, 10080, 11080, 12080, 13080, 14080]
        last_error: Exception | None = None
        for port in candidate_ports:
            try:
                site = web.TCPSite(self.web_runner, "0.0.0.0", port)
                await site.start()
                if port != WEBHOOK_PORT:
                    LOGGER.warning(
                        "Porta %s ocupada. Webhook iniciado na porta alternativa %s",
                        WEBHOOK_PORT,
                        port,
                    )
                LOGGER.info("Webhook Mercado Pago ativo na porta %s", port)
                return
            except OSError as error:
                last_error = error
                continue

        LOGGER.error(
            "Nao foi possivel iniciar webhook em nenhuma porta; bot seguira online sem servidor webhook. erro=%s",
            last_error,
        )
        return

    async def cleanup_duplicate_checkout_channels(self) -> None:
        if not GUILD_ID_INT:
            return

        guild = self.get_guild(GUILD_ID_INT)
        if guild is None:
            try:
                guild = await self.fetch_guild(GUILD_ID_INT)
            except Exception:
                LOGGER.warning("Nao foi possivel obter guild para limpar checkouts duplicados.")
                return

        try:
            channels = await guild.fetch_channels()
        except Exception:
            LOGGER.warning("Nao foi possivel listar canais para limpeza de duplicados.")
            return

        channels_by_user_and_type: dict[tuple[int, str], list[discord.TextChannel]] = {}
        for channel in channels:
            if not isinstance(channel, discord.TextChannel):
                continue
            checkout_user_id = extract_checkout_user_id(channel.topic)
            if checkout_user_id is None:
                continue
            checkout_type = get_checkout_type_from_topic(channel.topic)
            if checkout_type is None:
                continue
            channels_by_user_and_type.setdefault(
                (checkout_user_id, checkout_type),
                [],
            ).append(channel)

        removed_count = 0
        for (user_id, checkout_type), user_channels in channels_by_user_and_type.items():
            if len(user_channels) <= 1:
                continue

            user_channels.sort(key=lambda item: item.id)
            for duplicate_channel in user_channels[1:]:
                try:
                    await duplicate_channel.delete(
                        reason=(
                            "Limpeza automatica de checkout duplicado "
                            f"para user {user_id} tipo {checkout_type}"
                        ),
                    )
                    removed_count += 1
                except Exception:
                    LOGGER.warning(
                        (
                            "Falha ao remover checkout duplicado na inicializacao. "
                            "channel_id=%s user_id=%s checkout_type=%s"
                        ),
                        duplicate_channel.id,
                        user_id,
                        checkout_type,
                    )

        if removed_count:
            LOGGER.warning("Limpeza de checkout: %s canal(is) duplicado(s) removido(s)", removed_count)
        else:
            LOGGER.info("Limpeza de checkout: nenhum canal duplicado encontrado")

    async def webhook_health(self, request: web.Request) -> web.Response:
        return web.json_response({"ok": True})

    async def webhook_mercadopago(self, request: web.Request) -> web.Response:
        try:
            payload = await request.json(content_type=None)
        except Exception:
            payload = {}

        query = request.rel_url.query
        payment_id = query.get("data.id")

        if not payment_id:
            data = payload.get("data") if isinstance(payload, dict) else None
            if isinstance(data, dict):
                payment_id = data.get("id")

        if not payment_id and isinstance(payload, dict):
            event_type = str(payload.get("type") or payload.get("topic") or "").lower()
            if event_type == "payment":
                payment_id = payload.get("id")

        if not payment_id:
            LOGGER.warning("Webhook recebido sem payment_id")
            return web.json_response({"ok": True, "ignored": "sem payment id"})

        payment_id_str = str(payment_id)
        if payment_id_str in PROCESSED_PAYMENTS:
            LOGGER.info("Webhook duplicado ignorado. payment_id=%s", payment_id_str)
            return web.json_response({"ok": True, "duplicate": True})

        try:
            details = await self.get_payment_details(payment_id_str)
        except Exception as error:
            LOGGER.exception(
                "Falha ao consultar pagamento. payment_id=%s erro=%s",
                payment_id_str,
                error,
            )
            upsert_payment_tracking(payment_id_str, status="error_lookup")
            return web.json_response({"ok": True, "error": "consulta falhou"})

        status = str(details.get("status", "")).lower()
        if status != "approved":
            LOGGER.info(
                "Webhook processado com status nao aprovado. payment_id=%s status=%s",
                payment_id_str,
                status,
            )
            upsert_payment_tracking(
                payment_id_str,
                status=f"mp_{status or 'unknown'}",
            )
            return web.json_response({"ok": True, "status": status})

        blocked_provider = detect_blocked_payment_provider(details)
        if blocked_provider:
            LOGGER.info(
                "Pagamento bloqueado por provedor. payment_id=%s provider=%s",
                payment_id_str,
                blocked_provider,
            )
            external_reference = str(details.get("external_reference") or "")
            channel_id, user_id, _ = parse_checkout_reference(external_reference)
            if channel_id:
                channel = self.get_channel(channel_id)
                if channel is None:
                    try:
                        channel = await self.fetch_channel(channel_id)
                    except Exception:
                        channel = None
                if isinstance(channel, discord.TextChannel):
                    await channel.send(
                        content=(
                            f"<@{user_id}> pagamento recusado: o app/banco **{blocked_provider}** nao e aceito. "
                            "Use outro banco/app para pagar via PIX."
                        )
                    )
            upsert_payment_tracking(
                payment_id_str,
                status="blocked_provider",
                provider=blocked_provider,
            )
            PROCESSED_PAYMENTS.add(payment_id_str)
            return web.json_response({"ok": True, "status": "blocked-provider"})

        PROCESSED_PAYMENTS.add(payment_id_str)

        external_reference = str(details.get("external_reference") or "")
        channel_id, user_id, product_id = parse_checkout_reference(external_reference)
        produto = get_product_by_id(product_id) or PRODUCT
        upsert_payment_tracking(
            payment_id_str,
            status="processing",
            user_id=user_id,
            channel_id=channel_id,
            product_id=produto.product_id,
            amount=float(details.get("transaction_amount", produto.preco)),
        )

        if channel_id:
            channel = self.get_channel(channel_id)
            if channel is None:
                try:
                    channel = await self.fetch_channel(channel_id)
                except Exception:
                    channel = None

            if isinstance(channel, discord.TextChannel):
                amount = details.get("transaction_amount", produto.preco)
                pagamento_embed = discord.Embed(
                    description=(
                        "✅ Pagamento aprovado! "
                        "Seu produto sera entregue manualmente em ate 2 horas."
                    ),
                    color=discord.Color.green(),
                )
                pagamento_embed.set_footer(text=f"ID do pagamento: {payment_id_str} • Valor: {format_brl(float(amount))}")
                pagamento_embed.timestamp = discord.utils.utcnow()
                await channel.send(content=f"<@{user_id}>", embed=pagamento_embed)
                LOGGER.info(
                    "Pagamento aprovado enviado ao canal. payment_id=%s channel_id=%s user_id=%s",
                    payment_id_str,
                    channel_id,
                    user_id,
                )
                log_embed = discord.Embed(
                    title="✅ Pagamento Aprovado",
                    description=(
                        f"**Cliente:** <@{user_id}>\n"
                        f"**ID:** `{payment_id_str}`\n"
                        f"**Valor:** {format_brl(float(amount))}"
                    ),
                    color=discord.Color.green(),
                )
                log_embed.timestamp = discord.utils.utcnow()
                await send_log(self, log_embed, channel_id=LOG_PAYMENT_CHANNEL_ID_INT)

                delivery_status = await send_product_delivery_dm(
                    self, user_id, produto, payment_id_str
                )
                if delivery_status == "missing_code":
                    upsert_payment_tracking(
                        payment_id_str,
                        status="approved_no_stock",
                        delivery_status=delivery_status,
                    )
                    await send_stock_alert_message(
                        self,
                        (
                            "🚨 Alerta de estoque de codigos\n"
                            f"Cliente: <@{user_id}>\n"
                            f"Produto: {produto.nome}\n"
                            f"Pagamento: `{payment_id_str}`\n"
                            "Acao: entrega manual em ate 2 horas"
                        ),
                    )
                elif delivery_status == "already_sent":
                    upsert_payment_tracking(
                        payment_id_str,
                        status="approved",
                        delivery_status=delivery_status,
                    )
                    already_sent_embed = discord.Embed(
                        title="📦 Entrega ja registrada",
                        description=(
                            f"**Cliente:** <@{user_id}>\n"
                            f"**Produto:** {produto.nome}\n"
                            f"**Pagamento:** `{payment_id_str}`\n"
                            "**Acao:** codigo nao reenviado"
                        ),
                        color=discord.Color.orange(),
                    )
                    already_sent_embed.timestamp = discord.utils.utcnow()
                    await send_log(self, already_sent_embed, channel_id=LOG_PAYMENT_CHANNEL_ID_INT)
                else:
                    upsert_payment_tracking(
                        payment_id_str,
                        status="approved",
                        delivery_status=delivery_status,
                    )

        return web.json_response({"ok": True, "status": "approved"})

    async def get_payment_details(self, payment_id: str) -> dict:
        if not self.http_session:
            raise RuntimeError("Sessao HTTP indisponivel")

        if not MP_ACCESS_TOKEN:
            raise RuntimeError("MP_ACCESS_TOKEN nao configurado")

        headers = {
            "Authorization": f"Bearer {MP_ACCESS_TOKEN}",
        }

        async with self.http_session.get(
            f"https://api.mercadopago.com/v1/payments/{payment_id}",
            headers=headers,
        ) as response:
            data = await response.json(content_type=None)
            if response.status >= 400:
                raise RuntimeError(str(data))
            return data

    async def create_pix_payment(
        self, channel_id: int, user_id: int, apply_discount: bool = False
    ) -> dict:
        if not self.http_session:
            raise RuntimeError("Sessao HTTP indisponivel")

        if not MP_ACCESS_TOKEN:
            raise RuntimeError("MP_ACCESS_TOKEN nao configurado")

        idempotency_key = f"pix-{channel_id}-{user_id}-{int(time.time() * 1000)}"
        external_reference = f"checkout:{channel_id}:{user_id}:{PRODUCT.product_id}"

        amount = get_discounted_amount(PRODUCT.preco) if apply_discount else PRODUCT.preco
        description = PRODUCT.nome
        if apply_discount:
            description = f"{PRODUCT.nome} ({DISCOUNT_CODE})"

        payload = {
            "transaction_amount": amount,
            "description": description,
            "payment_method_id": "pix",
            "external_reference": external_reference,
            "payer": {
                "email": f"cliente{user_id}@example.com",
            },
        }
        if MP_WEBHOOK_URL:
            payload["notification_url"] = MP_WEBHOOK_URL

        headers = {
            "Authorization": f"Bearer {MP_ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "X-Idempotency-Key": idempotency_key,
        }

        async with self.http_session.post(
            "https://api.mercadopago.com/v1/payments",
            json=payload,
            headers=headers,
        ) as response:
            data = await response.json(content_type=None)
            if response.status >= 400:
                LOGGER.error(
                    "Erro ao criar PIX no Mercado Pago. status=%s response=%s",
                    response.status,
                    data,
                )
                raise RuntimeError(str(data))
            return data

    async def create_pix_payment_2(
        self, channel_id: int, user_id: int, apply_discount: bool = False
    ) -> dict:
        if not self.http_session:
            raise RuntimeError("Sessao HTTP indisponivel")

        if not MP_ACCESS_TOKEN:
            raise RuntimeError("MP_ACCESS_TOKEN nao configurado")

        idempotency_key = f"pix2-{channel_id}-{user_id}-{int(time.time() * 1000)}"
        external_reference = f"checkout2:{channel_id}:{user_id}:{PRODUCT2.product_id}"

        amount = get_discounted_amount(PRODUCT2.preco) if apply_discount else PRODUCT2.preco
        description = PRODUCT2.nome
        if apply_discount:
            description = f"{PRODUCT2.nome} ({DISCOUNT_CODE})"

        payload = {
            "transaction_amount": amount,
            "description": description,
            "payment_method_id": "pix",
            "external_reference": external_reference,
            "payer": {
                "email": f"cliente{user_id}@example.com",
            },
        }
        if MP_WEBHOOK_URL:
            payload["notification_url"] = MP_WEBHOOK_URL

        headers = {
            "Authorization": f"Bearer {MP_ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "X-Idempotency-Key": idempotency_key,
        }

        async with self.http_session.post(
            "https://api.mercadopago.com/v1/payments",
            json=payload,
            headers=headers,
        ) as response:
            data = await response.json(content_type=None)
            if response.status >= 400:
                LOGGER.error(
                    "Erro ao criar PIX (produto 2). status=%s response=%s",
                    response.status,
                    data,
                )
                raise RuntimeError(str(data))
            return data

    async def create_pix_payment_3(
        self, channel_id: int, user_id: int, apply_discount: bool = False
    ) -> dict:
        if not self.http_session:
            raise RuntimeError("Sessao HTTP indisponivel")

        if not MP_ACCESS_TOKEN:
            raise RuntimeError("MP_ACCESS_TOKEN nao configurado")

        idempotency_key = f"pix3-{channel_id}-{user_id}-{int(time.time() * 1000)}"
        external_reference = f"checkout3:{channel_id}:{user_id}:{PRODUCT3.product_id}"

        amount = get_discounted_amount(PRODUCT3.preco) if apply_discount else PRODUCT3.preco
        description = PRODUCT3.nome
        if apply_discount:
            description = f"{PRODUCT3.nome} ({DISCOUNT_CODE})"

        payload = {
            "transaction_amount": amount,
            "description": description,
            "payment_method_id": "pix",
            "external_reference": external_reference,
            "payer": {
                "email": f"cliente{user_id}@example.com",
            },
        }
        if MP_WEBHOOK_URL:
            payload["notification_url"] = MP_WEBHOOK_URL

        headers = {
            "Authorization": f"Bearer {MP_ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "X-Idempotency-Key": idempotency_key,
        }

        async with self.http_session.post(
            "https://api.mercadopago.com/v1/payments",
            json=payload,
            headers=headers,
        ) as response:
            data = await response.json(content_type=None)
            if response.status >= 400:
                LOGGER.error(
                    "Erro ao criar PIX (produto 3). status=%s response=%s",
                    response.status,
                    data,
                )
                raise RuntimeError(str(data))
            return data

    async def create_pix_payment_4(
        self, channel_id: int, user_id: int, apply_discount: bool = False
    ) -> dict:
        if not self.http_session:
            raise RuntimeError("Sessao HTTP indisponivel")

        if not MP_ACCESS_TOKEN:
            raise RuntimeError("MP_ACCESS_TOKEN nao configurado")

        idempotency_key = f"pix4-{channel_id}-{user_id}-{int(time.time() * 1000)}"
        external_reference = f"checkout4:{channel_id}:{user_id}:{PRODUCT4.product_id}"

        amount = get_discounted_amount(PRODUCT4.preco) if apply_discount else PRODUCT4.preco
        description = PRODUCT4.nome
        if apply_discount:
            description = f"{PRODUCT4.nome} ({DISCOUNT_CODE})"

        payload = {
            "transaction_amount": amount,
            "description": description,
            "payment_method_id": "pix",
            "external_reference": external_reference,
            "payer": {
                "email": f"cliente{user_id}@example.com",
            },
        }
        if MP_WEBHOOK_URL:
            payload["notification_url"] = MP_WEBHOOK_URL

        headers = {
            "Authorization": f"Bearer {MP_ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "X-Idempotency-Key": idempotency_key,
        }

        async with self.http_session.post(
            "https://api.mercadopago.com/v1/payments",
            json=payload,
            headers=headers,
        ) as response:
            data = await response.json(content_type=None)
            if response.status >= 400:
                LOGGER.error(
                    "Erro ao criar PIX (produto 4). status=%s response=%s",
                    response.status,
                    data,
                )
                raise RuntimeError(str(data))
            return data

    async def create_pix_payment_5(
        self, channel_id: int, user_id: int, apply_discount: bool = False
    ) -> dict:
        if not self.http_session:
            raise RuntimeError("Sessao HTTP indisponivel")

        if not MP_ACCESS_TOKEN:
            raise RuntimeError("MP_ACCESS_TOKEN nao configurado")

        idempotency_key = f"pix5-{channel_id}-{user_id}-{int(time.time() * 1000)}"
        external_reference = f"checkout5:{channel_id}:{user_id}:{PRODUCT5.product_id}"

        amount = get_discounted_amount(PRODUCT5.preco) if apply_discount else PRODUCT5.preco
        description = PRODUCT5.nome
        if apply_discount:
            description = f"{PRODUCT5.nome} ({DISCOUNT_CODE})"

        payload = {
            "transaction_amount": amount,
            "description": description,
            "payment_method_id": "pix",
            "external_reference": external_reference,
            "payer": {
                "email": f"cliente{user_id}@example.com",
            },
        }
        if MP_WEBHOOK_URL:
            payload["notification_url"] = MP_WEBHOOK_URL

        headers = {
            "Authorization": f"Bearer {MP_ACCESS_TOKEN}",
            "Content-Type": "application/json",
            "X-Idempotency-Key": idempotency_key,
        }

        async with self.http_session.post(
            "https://api.mercadopago.com/v1/payments",
            json=payload,
            headers=headers,
        ) as response:
            data = await response.json(content_type=None)
            if response.status >= 400:
                LOGGER.error(
                    "Erro ao criar PIX (produto 5). status=%s response=%s",
                    response.status,
                    data,
                )
                raise RuntimeError(str(data))
            return data


bot = LojaBot()


async def handle_postar_produto(
    interaction: discord.Interaction,
    product: Product,
    loja_channel_id: int | None,
    canal_invalido_msg: str,
    sucesso_msg: str,
) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    now = time.monotonic()
    cleanup_recent_post_trackers(now)

    interaction_id = int(getattr(interaction, "id", 0) or 0)
    if interaction_id:
        previous_ts = RECENT_POST_INTERACTIONS.get(interaction_id)
        if previous_ts and (now - previous_ts) <= INTERACTION_DEDUPE_WINDOW_SECONDS:
            await interaction.followup.send(
                "Este comando ja foi processado. Aguarde alguns segundos.",
                ephemeral=True,
            )
            return
        RECENT_POST_INTERACTIONS[interaction_id] = now

    postar_role_id = get_postar_role_id()
    if postar_role_id and not await user_can_post_products(interaction, postar_role_id):
        await interaction.followup.send(
            "Apenas o cargo configurado pode usar este comando.", ephemeral=True
        )
        return

    guild = interaction.guild
    if guild is None:
        await interaction.followup.send(
            "Comando disponivel apenas em servidor.", ephemeral=True
        )
        return

    request_key = (guild.id, interaction.user.id, product.product_id)
    previous_request_ts = RECENT_POST_REQUESTS.get(request_key)
    if previous_request_ts and (now - previous_request_ts) <= POST_COMMAND_WINDOW_SECONDS:
        remaining = max(1, int(POST_COMMAND_WINDOW_SECONDS - (now - previous_request_ts)))
        await interaction.followup.send(
            f"Comando repetido muito rapido. Aguarde {remaining}s e tente novamente.",
            ephemeral=True,
        )
        return
    RECENT_POST_REQUESTS[request_key] = now

    if not loja_channel_id:
        await interaction.followup.send(
            "Canal da loja nao configurado no .env.", ephemeral=True
        )
        return

    loja_channel = guild.get_channel(loja_channel_id)
    if loja_channel is None:
        try:
            fetched = await guild.fetch_channel(loja_channel_id)
            loja_channel = fetched if isinstance(fetched, discord.TextChannel) else None
        except Exception:
            loja_channel = None

    if not isinstance(loja_channel, discord.TextChannel):
        await interaction.followup.send(canal_invalido_msg, ephemeral=True)
        return

    try:
        action, message = await post_or_update_product_card(
            guild,
            loja_channel,
            product,
            get_product_embed_builder(product)(),
            build_product_view_for(product, bot),
            bot.user.id if bot.user else None,
        )
    except ProductPostInProgressError:
        await interaction.followup.send(
            "Uma postagem deste produto ja esta em andamento. Tente novamente em 2 segundos.",
            ephemeral=True,
        )
        return
    except ProductCardSyncError:
        await interaction.followup.send(
            (
                "Card anterior nao foi encontrado agora. Bloqueei esta tentativa para evitar duplicados. "
                "Execute o comando novamente para recriar com seguranca."
            ),
            ephemeral=True,
        )
        return
    except Exception:
        LOGGER.exception(
            "Falha inesperada ao postar produto. guild_id=%s user_id=%s product_id=%s",
            guild.id,
            interaction.user.id,
            product.product_id,
        )
        await interaction.followup.send(
            "Erro inesperado ao postar o card. Tente novamente em alguns segundos.",
            ephemeral=True,
        )
        return

    if action == "updated":
        await interaction.followup.send(
            (
                "Produto ja estava postado; card atualizado com sucesso. "
                f"Mensagem: https://discord.com/channels/{guild.id}/{loja_channel.id}/{message.id}"
            ),
            ephemeral=True,
        )
    else:
        await interaction.followup.send(sucesso_msg, ephemeral=True)


@app_commands.command(
    name="postar_produto", description="Posta o card do produto com botao de compra"
)
async def postar_produto(interaction: discord.Interaction) -> None:
    await handle_postar_produto(
        interaction,
        PRODUCT,
        LOJA_CHANNEL_ID_INT,
        "Canal da loja invalido no .env.",
        "Produto postado com sucesso.",
    )


@app_commands.command(
    name="postar_produto2", description="Posta o card do produto 2 com botao de compra"
)
async def postar_produto2(interaction: discord.Interaction) -> None:
    await handle_postar_produto(
        interaction,
        PRODUCT2,
        get_loja_channel_id_2(),
        "Canal da loja 2 invalido no .env.",
        "Produto 2 postado com sucesso.",
    )


@app_commands.command(
    name="postar_produto3", description="Posta o card do produto 3 com botao de compra"
)
async def postar_produto3(interaction: discord.Interaction) -> None:
    await handle_postar_produto(
        interaction,
        PRODUCT3,
        get_loja_channel_id_3(),
        "Canal da loja 3 invalido no .env.",
        "Produto 3 postado com sucesso.",
    )


@app_commands.command(
    name="postar_produto4", description="Posta o card do produto 4 com botao de compra"
)
async def postar_produto4(interaction: discord.Interaction) -> None:
    await handle_postar_produto(
        interaction,
        PRODUCT4,
        get_loja_channel_id_4(),
        "Canal da loja 4 invalido no .env.",
        "Produto 4 postado com sucesso.",
    )


@app_commands.command(
    name="postar_produto5", description="Posta o card do produto 5 (teste) com botao de compra"
)
async def postar_produto5(
    interaction: discord.Interaction,
    canal: discord.TextChannel | None = None,
) -> None:
    target_channel_id = canal.id if canal else get_loja_channel_id_5()
    await handle_postar_produto(
        interaction,
        PRODUCT5,
        target_channel_id,
        "Canal da loja 5 invalido no .env.",
        "Produto 5 postado com sucesso.",
    )


@app_commands.command(
    name="ticket", description="Posta o painel de abertura de tickets de suporte"
)
async def ticket(interaction: discord.Interaction) -> None:
    postar_role_id = get_postar_role_id()
    if postar_role_id and not await user_can_post_products(interaction, postar_role_id):
        await interaction.response.send_message(
            "Apenas o cargo configurado pode postar o painel de ticket.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(
            "Use este comando em um canal de texto do servidor.",
            ephemeral=True,
        )
        return

    await interaction.channel.send(
        embed=build_ticket_panel_embed(),
        view=SupportTicketPanelView(bot),
    )
    await interaction.response.send_message(
        "Painel de ticket postado com sucesso.", ephemeral=True
    )


@app_commands.command(
    name="aceitarcompra",
    description="Aprova manualmente uma compra no canal de checkout atual",
)
async def aceitar_compra(interaction: discord.Interaction) -> None:
    if interaction.guild is None or not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(
            "Use este comando em um canal de checkout do servidor.",
            ephemeral=True,
        )
        return

    member = interaction.user if isinstance(interaction.user, discord.Member) else None
    if member is None:
        await interaction.response.send_message(
            "Nao consegui validar seu usuario neste servidor.",
            ephemeral=True,
        )
        return

    has_dev_dc_role = any(role.name.casefold() == "dev dc" for role in member.roles)
    if not has_dev_dc_role:
        await interaction.response.send_message(
            "Apenas quem possui o cargo DEV DC pode usar este comando.",
            ephemeral=True,
        )
        return

    topic = interaction.channel.topic or ""
    if not (
        topic.startswith("checkout:")
        or topic.startswith("checkout2:")
        or topic.startswith("checkout3:")
        or topic.startswith("checkout4:")
        or topic.startswith("checkout5:")
    ):
        await interaction.response.send_message(
            "Este comando so funciona dentro de um canal de checkout.",
            ephemeral=True,
        )
        return

    try:
        user_id = int(topic.split(":", 1)[1])
    except (ValueError, IndexError):
        await interaction.response.send_message(
            "Nao consegui identificar o cliente deste checkout.",
            ephemeral=True,
        )
        return

    produto = PRODUCT
    if topic.startswith("checkout2:"):
        produto = PRODUCT2
    elif topic.startswith("checkout3:"):
        produto = PRODUCT3
    elif topic.startswith("checkout4:"):
        produto = PRODUCT4
    elif topic.startswith("checkout5:"):
        produto = PRODUCT5
    payment_id = (
        f"manual:{interaction.channel.id}:{user_id}:{produto.product_id}:{int(time.time())}"
    )
    upsert_payment_tracking(
        payment_id,
        status="manual_approved",
        user_id=user_id,
        channel_id=interaction.channel.id,
        product_id=produto.product_id,
        amount=float(produto.preco),
        approval_notified=True,
    )

    await interaction.response.send_message(
        "📦 Seu produto será enviado na sua DM em até 1 hora."
    )

    LOGGER.info(
        "Aprovacao manual executada. channel_id=%s admin_id=%s user_id=%s payment_id=%s",
        interaction.channel.id,
        interaction.user.id,
        user_id,
        payment_id,
    )
    log_embed = discord.Embed(
        title="✅ Pagamento Aprovado (Manual)",
        description=(
            f"**Cliente:** <@{user_id}>\n"
            f"**ID:** `{payment_id}`\n"
            f"**Valor:** {format_brl(produto.preco)}\n"
            f"**Admin:** <@{interaction.user.id}>"
        ),
        color=discord.Color.green(),
    )
    log_embed.timestamp = discord.utils.utcnow()
    await send_log(bot, log_embed, channel_id=LOG_PAYMENT_CHANNEL_ID_INT)

    delivery_status = await send_product_delivery_dm(bot, user_id, produto, payment_id)

    if delivery_status == "missing_code":
        upsert_payment_tracking(
            payment_id,
            status="approved_no_stock",
            delivery_status=delivery_status,
        )
        await send_stock_alert_message(
            bot,
            (
                "🚨 Alerta de estoque de codigos\n"
                f"Cliente: <@{user_id}>\n"
                f"Produto: {produto.nome}\n"
                f"Pagamento: `{payment_id}`\n"
                "Acao: entrega manual em ate 2 horas"
            ),
        )
    elif delivery_status == "already_sent" and isinstance(interaction.channel, discord.TextChannel):
        upsert_payment_tracking(
            payment_id,
            status="approved",
            delivery_status=delivery_status,
        )
        already_sent_embed = discord.Embed(
            title="📦 Entrega ja registrada",
            description=(
                f"**Cliente:** <@{user_id}>\n"
                f"**Produto:** {produto.nome}\n"
                f"**Pagamento:** `{payment_id}`\n"
                "**Acao:** codigo nao reenviado"
            ),
            color=discord.Color.orange(),
        )
        already_sent_embed.timestamp = discord.utils.utcnow()
        await send_log(bot, already_sent_embed, channel_id=LOG_PAYMENT_CHANNEL_ID_INT)
    else:
        upsert_payment_tracking(
            payment_id,
            status="approved",
            delivery_status=delivery_status,
        )


@app_commands.command(
    name="resetarentrega",
    description="Libera reenvio de key para um usuario (e produto opcional)",
)
@app_commands.describe(
    usuario="Usuario que tera o bloqueio de entrega removido",
    produto="Produto especifico para resetar (opcional)",
)
@app_commands.choices(
    produto=[
        app_commands.Choice(name=PRODUCT.nome, value=PRODUCT.product_id),
        app_commands.Choice(name=PRODUCT2.nome, value=PRODUCT2.product_id),
        app_commands.Choice(name=PRODUCT3.nome, value=PRODUCT3.product_id),
        app_commands.Choice(name=PRODUCT4.nome, value=PRODUCT4.product_id),
    ]
)
async def resetar_entrega(
    interaction: discord.Interaction,
    usuario: discord.Member,
    produto: app_commands.Choice[str] | None = None,
) -> None:
    role_id = get_manage_role_command_role_id()
    if role_id and not await user_can_post_products(interaction, role_id):
        await interaction.response.send_message(
            "Apenas o cargo configurado pode usar este comando.",
            ephemeral=True,
        )
        return

    target_product_id = produto.value if produto else None
    removed_count = clear_delivery_usage_entries(usuario.id, target_product_id)

    if removed_count == 0:
        await interaction.response.send_message(
            "Nenhum bloqueio de entrega encontrado para os filtros informados.",
            ephemeral=True,
        )
        return

    product_text = target_product_id or "todos os produtos"
    await interaction.response.send_message(
        (
            f"Reset concluido para <@{usuario.id}>. "
            f"Entradas removidas: {removed_count}. "
            f"Filtro de produto: {product_text}."
        ),
        ephemeral=True,
    )

    log_embed = discord.Embed(
        title="🔄 Reset de Entrega",
        description=(
            f"**Cliente:** <@{usuario.id}>\n"
            f"**Produto:** {product_text}\n"
            f"**Entradas removidas:** {removed_count}\n"
            f"**Executado por:** <@{interaction.user.id}>"
        ),
        color=discord.Color.orange(),
    )
    log_embed.timestamp = discord.utils.utcnow()
    await send_log(bot, log_embed, channel_id=LOG_PAYMENT_CHANNEL_ID_INT)


@app_commands.command(
    name="verestoque",
    description="Mostra o estoque atual de codigos por produto",
)
async def ver_estoque(interaction: discord.Interaction) -> None:
    role_id = get_manage_role_command_role_id()
    if role_id and not await user_can_post_products(interaction, role_id):
        await interaction.response.send_message(
            "Apenas o cargo configurado pode usar este comando.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        embed=build_stock_overview_embed(),
        ephemeral=True,
    )


@app_commands.command(
    name="postarestoque",
    description="Posta um painel publico de estoque com atualizacao automatica",
)
async def postar_estoque(interaction: discord.Interaction) -> None:
    role_id = get_manage_role_command_role_id()
    if role_id and not await user_can_post_products(interaction, role_id):
        await interaction.response.send_message(
            "Apenas o cargo configurado pode usar este comando.",
            ephemeral=True,
        )
        return

    if not isinstance(interaction.channel, discord.TextChannel):
        await interaction.response.send_message(
            "Use este comando em um canal de texto.",
            ephemeral=True,
        )
        return

    stock_message = await interaction.channel.send(embed=build_stock_overview_embed())
    save_stock_message_ref(interaction.channel.id, stock_message.id)

    await interaction.response.send_message(
        f"Painel de estoque postado em {interaction.channel.mention}.",
        ephemeral=True,
    )


@app_commands.command(
    name="painelpedidos",
    description="Mostra resumo de pedidos rastreados e pendencias",
)
async def painel_pedidos(interaction: discord.Interaction) -> None:
    role_id = get_manage_role_command_role_id()
    if role_id and not await user_can_post_products(interaction, role_id):
        await interaction.response.send_message(
            "Apenas o cargo configurado pode usar este comando.",
            ephemeral=True,
        )
        return

    await interaction.response.send_message(
        embed=build_orders_dashboard_embed(limit=12),
        ephemeral=True,
    )


def user_has_role(member: discord.Member, role_id: int | None) -> bool:
    if not role_id:
        return False
    return any(role.id == role_id for role in member.roles)


async def send_temp_reply(ctx: commands.Context, content: str) -> None:
    reply_message = await ctx.reply(content)

    async def cleanup_messages() -> None:
        await asyncio.sleep(7)
        try:
            await reply_message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass

        try:
            await ctx.message.delete()
        except (discord.Forbidden, discord.NotFound):
            pass

    asyncio.create_task(cleanup_messages())


@bot.command(name="addcargo")
@commands.guild_only()
async def addcargo(ctx: commands.Context, membro: discord.Member, cargo: discord.Role) -> None:
    role_id = get_manage_role_command_role_id()

    if not isinstance(ctx.author, discord.Member) or ctx.guild is None:
        await send_temp_reply(ctx, "Comando disponivel apenas em servidor.")
        return

    if role_id and not user_has_role(ctx.author, role_id):
        await send_temp_reply(ctx, "Voce nao tem permissao para usar este comando.")
        return

    bot_member = ctx.guild.me
    if bot_member is None:
        await send_temp_reply(ctx, "Nao consegui validar a hierarquia do bot neste servidor.")
        return

    if cargo >= bot_member.top_role:
        await send_temp_reply(ctx, "Nao consigo gerenciar este cargo por hierarquia.")
        return

    if cargo in membro.roles:
        await send_temp_reply(ctx, f"{membro.mention} ja possui o cargo {cargo.mention}.")
        return

    try:
        await membro.add_roles(cargo, reason=f"Comando !addcargo por {ctx.author}")
        await ctx.reply(f"Cargo {cargo.mention} adicionado para {membro.mention}.")
        log_embed = discord.Embed(
            title="✅ Cargo Adicionado",
            description=(
                f"**Membro:** {membro.mention}\n"
                f"**Cargo:** {cargo.mention}\n"
                f"**Executado por:** {ctx.author.mention}"
            ),
            color=discord.Color.green(),
        )
        log_embed.timestamp = discord.utils.utcnow()
        await send_log(
            bot,
            log_embed,
            channel_id=LOG_ROLE_CHANNEL_ID_INT or LOG_CHANNEL_ID_INT,
        )
    except discord.Forbidden:
        await send_temp_reply(ctx, "Sem permissao para adicionar este cargo.")
    except Exception as error:
        await send_temp_reply(ctx, f"Erro ao adicionar cargo: {error}")


@addcargo.error
async def addcargo_error(ctx: commands.Context, error: commands.CommandError) -> None:
    if isinstance(error, (commands.MissingRequiredArgument, commands.BadArgument)):
        await send_temp_reply(
            ctx,
            "Uso correto: !addcargo @membro @cargo",
        )
        return
    raise error


@bot.command(name="remcargo")
@commands.guild_only()
async def remcargo(ctx: commands.Context, membro: discord.Member, cargo: discord.Role) -> None:
    role_id = get_manage_role_command_role_id()

    if not isinstance(ctx.author, discord.Member) or ctx.guild is None:
        await send_temp_reply(ctx, "Comando disponivel apenas em servidor.")
        return

    if role_id and not user_has_role(ctx.author, role_id):
        await send_temp_reply(ctx, "Voce nao tem permissao para usar este comando.")
        return

    bot_member = ctx.guild.me
    if bot_member is None:
        await send_temp_reply(ctx, "Nao consegui validar a hierarquia do bot neste servidor.")
        return

    if cargo >= bot_member.top_role:
        await send_temp_reply(ctx, "Nao consigo gerenciar este cargo por hierarquia.")
        return

    if cargo not in membro.roles:
        await send_temp_reply(ctx, f"{membro.mention} nao possui o cargo {cargo.mention}.")
        return

    try:
        await membro.remove_roles(cargo, reason=f"Comando !remcargo por {ctx.author}")
        await ctx.reply(f"Cargo {cargo.mention} removido de {membro.mention}.")
        log_embed = discord.Embed(
            title="🗑️ Cargo Removido",
            description=(
                f"**Membro:** {membro.mention}\n"
                f"**Cargo:** {cargo.mention}\n"
                f"**Executado por:** {ctx.author.mention}"
            ),
            color=discord.Color.orange(),
        )
        log_embed.timestamp = discord.utils.utcnow()
        await send_log(
            bot,
            log_embed,
            channel_id=LOG_ROLE_CHANNEL_ID_INT or LOG_CHANNEL_ID_INT,
        )
    except discord.Forbidden:
        await send_temp_reply(ctx, "Sem permissao para remover este cargo.")
    except Exception as error:
        await send_temp_reply(ctx, f"Erro ao remover cargo: {error}")


@remcargo.error
async def remcargo_error(ctx: commands.Context, error: commands.CommandError) -> None:
    if isinstance(error, (commands.MissingRequiredArgument, commands.BadArgument)):
        await send_temp_reply(
            ctx,
            "Uso correto: !remcargo @membro @cargo",
        )
        return
    raise error


@bot.event
async def on_ready() -> None:
    LOGGER.info("Bot online como %s", bot.user)
    try:
        await bot.refresh_all_product_stock_messages()
    except Exception as error:
        LOGGER.warning("Falha ao atualizar cards de estoque no on_ready: %s", error)

    if AUTO_ROLE_ID_INT and not ENABLE_MEMBERS_INTENT:
        LOGGER.warning(
            "AUTO_ROLE_ID configurado, mas ENABLE_MEMBERS_INTENT=false. "
            "Ative o intent no portal do Discord e no .env para funcionar."
        )


@bot.event
async def on_member_join(member: discord.Member) -> None:
    if member.guild.id != GUILD_ID_INT:
        return

    if not AUTO_ROLE_ID_INT:
        return

    role = member.guild.get_role(AUTO_ROLE_ID_INT)
    if role is None:
        LOGGER.warning("Cargo automatico nao encontrado. role_id=%s", AUTO_ROLE_ID_INT)
        return

    try:
        await member.add_roles(role, reason="Cargo automatico ao entrar no servidor")
        LOGGER.info(
            "Cargo automatico adicionado. user_id=%s role_id=%s guild_id=%s",
            member.id,
            role.id,
            member.guild.id,
        )
        log_embed = discord.Embed(
            title="👤 Novo Membro",
            description=(
                f"**Usuario:** {member.mention}\n"
                f"**Cargo adicionado:** {role.mention}"
            ),
            color=discord.Color.green(),
        )
        log_embed.timestamp = discord.utils.utcnow()
        await send_log(bot, log_embed)
    except discord.Forbidden:
        LOGGER.error(
            "Sem permissao para adicionar cargo automatico. user_id=%s role_id=%s",
            member.id,
            AUTO_ROLE_ID_INT,
        )
    except Exception as error:
        LOGGER.exception(
            "Erro ao adicionar cargo automatico. user_id=%s role_id=%s erro=%s",
            member.id,
            AUTO_ROLE_ID_INT,
            error,
        )

try:
    acquire_bot_instance_lock()
except RuntimeError as error:
    LOGGER.error(str(error))
    sys.exit(0)

bot.run(BOT_TOKEN)
