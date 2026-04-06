import os
import re
import random
import logging
import asyncio
import time
import uuid
import datetime
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlparse
from functools import wraps

from psycopg_pool import ConnectionPool
import redis
from telegram import (
    Update, InlineKeyboardMarkup, InlineKeyboardButton,
    BotCommand, BotCommandScopeDefault, BotCommandScopeAllPrivateChats, BotCommandScopeChat
)
from telegram.error import BadRequest
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackQueryHandler
)

# =========================
# ENV
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram/webhook").strip()
PORT = int(os.getenv("PORT", "8080"))

# ✅ 白名单群组：逗号分隔的群组ID，如 -1001234567890,-1009876543210
ALLOWED_CHAT_IDS: set[int] = {
    int(x.strip())
    for x in os.getenv("ALLOWED_CHAT_IDS", "").split(",")
    if x.strip().lstrip("-").isdigit()
}

MIN_TEXT_LEN = int(os.getenv("MIN_TEXT_LEN", "3"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "20"))
DAILY_MAX_MILLI = int(os.getenv("DAILY_MAX_MILLI", "50000"))
ENABLE_SAME_TEXT_BLOCK = os.getenv("ENABLE_SAME_TEXT_BLOCK", "1") == "1"
PER_MINUTE_CAP = int(os.getenv("PER_MINUTE_CAP", "2"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not BOT_TOKEN or not DATABASE_URL or not REDIS_URL:
    raise RuntimeError("缺少 BOT_TOKEN / DATABASE_URL / REDIS_URL")

if not ALLOWED_CHAT_IDS:
    logger.warning("⚠️  ALLOWED_CHAT_IDS 未配置，机器人将不响应任何群组！")

# =========================
# Utils
# =========================
def coin_to_milli(s: str) -> int:
    d = Decimal(str(s)).quantize(Decimal("0.001"), rounding=ROUND_DOWN)
    return int(d * 1000)

def milli_to_coin(m: int) -> str:
    return f"{m / 1000:.3f}".rstrip("0").rstrip(".")

def parse_redis_url(url: str):
    u = urlparse(url)
    return {
        "host": u.hostname,
        "port": u.port or 6379,
        "db": int((u.path or "/0").replace("/", "") or 0),
        "password": u.password,
        "decode_responses": True,
    }

def safe_int(x, d=0):
    try:
        return int(x)
    except Exception:
        return d

def safe_float(x, d=0.0):
    try:
        return float(x)
    except Exception:
        return d

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def valid_text_basic(text: str, min_len: int) -> bool:
    if not text:
        return False
    t = text.strip()
    if len(t) < min_len:
        return False
    if t.isdigit():
        return False
    if re.fullmatch(r"[\W_]+", t):
        return False
    return True

def fmt_display_name(first: str, last: str, username: str, uid: int) -> str:
    name = ""
    if first:
        name = first
        if last:
            name += f" {last}"
    elif username:
        name = f"@{username}"
    else:
        name = str(uid)
    return name

# =========================
# 白名单校验
# =========================
def is_allowed_chat(chat_id: int) -> bool:
    """群组是否在白名单内"""
    return chat_id in ALLOWED_CHAT_IDS

# =========================
# Telegram 管理员实时校验
# =========================
async def is_group_admin(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    """
    实时调用 Telegram API 验证用户是否是指定群组的管理员或群主
    结果缓存 60 秒到 Redis，避免频繁 API 调用
    """
    cache_key = f"tg_admin:{chat_id}:{user_id}"
    cached = rds.get(cache_key)
    if cached is not None:
        return cached == "1"
    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        result = member.status in ("administrator", "creator")
        rds.setex(cache_key, 60, "1" if result else "0")
        return result
    except Exception:
        logger.exception(f"is_group_admin failed chat={chat_id} user={user_id}")
        return False

async def has_manage_admins_permission(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    user_id: int
) -> bool:
    """
    是否拥有“添加/管理管理员”权限（Promote Members）
    - creator: True
    - administrator: 仅当 can_promote_members=True
    """
    cache_key = f"tg_promote:{chat_id}:{user_id}"
    cached = rds.get(cache_key)
    if cached is not None:
        return cached == "1"

    try:
        member = await context.bot.get_chat_member(chat_id, user_id)
        ok = False
        if member.status == "creator":
            ok = True
        elif member.status == "administrator":
            ok = bool(getattr(member, "can_promote_members", False))

        rds.setex(cache_key, 60, "1" if ok else "0")
        return ok
    except Exception:
        logger.exception(f"has_manage_admins_permission failed chat={chat_id} user={user_id}")
        return False

async def get_admin_chat_ids(context: ContextTypes.DEFAULT_TYPE, user_id: int) -> list[tuple[int, str]]:
    """
    返回用户在白名单群组中拥有“添加管理员权限”的群组列表 [(chat_id, title), ...]
    """
    result = []
    for chat_id in ALLOWED_CHAT_IDS:
        try:
            if await has_manage_admins_permission(context, chat_id, user_id):
                chat = await context.bot.get_chat(chat_id)
                result.append((chat_id, chat.title or str(chat_id)))
        except Exception:
            pass
    return result

# =========================
# DB / Redis
# =========================
pg_pool = ConnectionPool(
    conninfo=DATABASE_URL,
    min_size=1,
    max_size=20,
    kwargs={"autocommit": False},
    open=True
)
rds = redis.Redis(**parse_redis_url(REDIS_URL))
rds.ping()

def with_conn(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        with pg_pool.connection() as conn:
            try:
                out = fn(conn, *args, **kwargs)
                conn.commit()
                return out
            except Exception:
                conn.rollback()
                raise
    return wrapper

# =========================
# Schema（移除 chat_admins 表）
# =========================
@with_conn
def init_db(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS chats (
          chat_id BIGINT PRIMARY KEY,
          title TEXT NOT NULL DEFAULT '',
          active BOOLEAN NOT NULL DEFAULT TRUE,
          created_at TIMESTAMP NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
          chat_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          balance_milli BIGINT NOT NULL DEFAULT 0,
          display_name TEXT NOT NULL DEFAULT '',
          updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
          PRIMARY KEY (chat_id, user_id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS drop_rules (
          id BIGSERIAL PRIMARY KEY,
          chat_id BIGINT NOT NULL,
          name TEXT NOT NULL,
          probability DOUBLE PRECISION NOT NULL,
          min_milli BIGINT NOT NULL,
          max_milli BIGINT NOT NULL,
          enabled BOOLEAN NOT NULL DEFAULT TRUE,
          priority INT NOT NULL DEFAULT 100
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS shop_items (
          id BIGSERIAL PRIMARY KEY,
          chat_id BIGINT NOT NULL,
          title TEXT NOT NULL,
          price_milli BIGINT NOT NULL,
          enabled BOOLEAN NOT NULL DEFAULT TRUE,
          stock INT NULL,
          description TEXT NOT NULL DEFAULT '',
          created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS redeem_orders (
          id BIGSERIAL PRIMARY KEY,
          chat_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          item_id BIGINT NOT NULL,
          price_milli BIGINT NOT NULL,
          status TEXT NOT NULL DEFAULT 'approved',
          created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS coin_logs (
          id BIGSERIAL PRIMARY KEY,
          chat_id BIGINT NOT NULL,
          operator_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          delta_milli BIGINT NOT NULL,
          reason TEXT NOT NULL DEFAULT '',
          log_type TEXT NOT NULL DEFAULT 'admin',
          created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_settings (
          chat_id BIGINT PRIMARY KEY,
          rank_keywords TEXT NOT NULL DEFAULT '排行榜,排名,积分榜',
          shop_keywords TEXT NOT NULL DEFAULT '商城,兑换,商店',
          redeem_notice TEXT NOT NULL DEFAULT '',
          rank_delete_seconds INT NOT NULL DEFAULT 120,
          updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_chat ON drop_rules(chat_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_shop_chat ON shop_items(chat_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_logs_chat ON coin_logs(chat_id, id DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_logs_type ON coin_logs(chat_id, log_type, created_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_redeem_orders_chat ON redeem_orders(chat_id, created_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_wallets_chat ON wallets(chat_id, balance_milli DESC);")

        # ── 红包主表 ──
        cur.execute("""
        CREATE TABLE IF NOT EXISTS redpacks (
          pack_id      TEXT PRIMARY KEY,
          chat_id      BIGINT NOT NULL,
          sender_id    BIGINT NOT NULL,
          sender_name  TEXT NOT NULL DEFAULT '',
          total_milli  BIGINT NOT NULL,
          remain_milli BIGINT NOT NULL,
          total_count  INT NOT NULL,
          remain_count INT NOT NULL,
          status       TEXT NOT NULL DEFAULT 'open',
          created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
          expire_at    TIMESTAMP NOT NULL
        );
        """)
        # ── 红包抢记录 ──
        cur.execute("""
        CREATE TABLE IF NOT EXISTS redpack_records (
          id           BIGSERIAL PRIMARY KEY,
          pack_id      TEXT NOT NULL,
          chat_id      BIGINT NOT NULL,
          user_id      BIGINT NOT NULL,
          user_name    TEXT NOT NULL DEFAULT '',
          amount_milli BIGINT NOT NULL,
          grabbed_at   TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)
        # ── PC28 期表 ──
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pc28_periods (
          id       BIGSERIAL PRIMARY KEY,
          chat_id  BIGINT NOT NULL,
          period   INT NOT NULL,
          open_at  TIMESTAMP NOT NULL,
          status   TEXT NOT NULL DEFAULT 'betting',
          result   TEXT
        );
        """)

        # ── PC28 注单表 ──
        cur.execute("""
        CREATE TABLE IF NOT EXISTS pc28_bets (
          id           BIGSERIAL PRIMARY KEY,
          chat_id      BIGINT NOT NULL,
          period_id    BIGINT NOT NULL,
          user_id      BIGINT NOT NULL,
          user_name    TEXT NOT NULL DEFAULT '',
          bet_type     TEXT NOT NULL,
          amount_milli BIGINT NOT NULL,
          status       TEXT NOT NULL DEFAULT 'pending',
          payout_milli BIGINT NOT NULL DEFAULT 0
        );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rp_chat ON redpacks(chat_id, status);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rpr_pack ON redpack_records(pack_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pc28_period ON pc28_periods(chat_id, status);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_pc28_bets ON pc28_bets(period_id, chat_id);")

@with_conn
def migrate_db(conn):
    with conn.cursor() as cur:
        cur.execute("UPDATE drop_rules SET name='普通红包' WHERE lower(name) IN ('common','normal')")
        cur.execute("UPDATE drop_rules SET name='稀有红包' WHERE lower(name)='rare'")
        cur.execute("UPDATE drop_rules SET name='史诗红包' WHERE lower(name)='epic'")
        # 补列兼容旧库
        for ddl in [
            """DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='coin_logs' AND column_name='log_type') THEN
               ALTER TABLE coin_logs ADD COLUMN log_type TEXT NOT NULL DEFAULT 'admin'; END IF; END $$;""",
            """DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='shop_items' AND column_name='description') THEN
               ALTER TABLE shop_items ADD COLUMN description TEXT NOT NULL DEFAULT ''; END IF; END $$;""",
            """DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='chat_settings' AND column_name='rank_delete_seconds') THEN
               ALTER TABLE chat_settings ADD COLUMN rank_delete_seconds INT NOT NULL DEFAULT 120; END IF; END $$;""",
            """DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='chat_settings' AND column_name='shop_keywords') THEN
               ALTER TABLE chat_settings ADD COLUMN shop_keywords TEXT NOT NULL DEFAULT '商城,兑换,商店'; END IF; END $$;""",
            """DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name='wallets' AND column_name='display_name') THEN
               ALTER TABLE wallets ADD COLUMN display_name TEXT NOT NULL DEFAULT ''; END IF; END $$;""",
            """DO $$ BEGIN
               IF EXISTS (
                 SELECT 1 FROM information_schema.table_constraints
                 WHERE table_name='pc28_periods'
                 AND constraint_name='pc28_periods_chat_id_period_key'
               ) THEN
                 ALTER TABLE pc28_periods DROP CONSTRAINT pc28_periods_chat_id_period_key;
               END IF;
             END $$;""",
        ]:
            cur.execute(ddl)

# =========================
# Chat upsert
# =========================
@with_conn
def upsert_chat(conn, chat_id: int, title: str):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO chats(chat_id,title,active,created_at,updated_at)
        VALUES(%s,%s,TRUE,NOW(),NOW())
        ON CONFLICT(chat_id)
        DO UPDATE SET title=EXCLUDED.title,updated_at=NOW(),active=TRUE
        """, (chat_id, title or ""))

# =========================
# Chat Settings
# =========================
@with_conn
def get_chat_settings(conn, chat_id: int) -> dict:
    with conn.cursor() as cur:
        cur.execute("""
        SELECT rank_keywords, redeem_notice, rank_delete_seconds, shop_keywords
        FROM chat_settings WHERE chat_id=%s
        """, (chat_id,))
        row = cur.fetchone()
        if row:
            return {
                "rank_keywords": row[0],
                "redeem_notice": row[1],
                "rank_delete_seconds": int(row[2]) if row[2] else 120,
                "shop_keywords": row[3] if row[3] else "商城,兑换,商店",
            }
        return {
            "rank_keywords": "排行榜,排名,积分榜",
            "redeem_notice": "",
            "rank_delete_seconds": 120,
            "shop_keywords": "商城,兑换,商店",
        }

@with_conn
def set_chat_setting(conn, chat_id: int, key: str, value):
    with conn.cursor() as cur:
        allowed_keys = {"rank_keywords", "shop_keywords", "redeem_notice", "rank_delete_seconds"}
        if key not in allowed_keys:
            return
        if key == "rank_delete_seconds":
            value = int(value)
        cur.execute(f"""
        INSERT INTO chat_settings(chat_id,{key},updated_at)
        VALUES(%s,%s,NOW())
        ON CONFLICT(chat_id) DO UPDATE SET {key}=EXCLUDED.{key},updated_at=NOW()
        """, (chat_id, value))

# =========================
# Drop Rules
# =========================
@with_conn
def ensure_default_rules(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM drop_rules WHERE chat_id=%s", (chat_id,))
        if cur.fetchone()[0] == 0:
            cur.execute("""
            INSERT INTO drop_rules(chat_id,name,probability,min_milli,max_milli,enabled,priority)
            VALUES
            (%s,'普通红包',0.10,100,1000,TRUE,100),
            (%s,'惊喜红包',0.01,1000,5000,TRUE,90),
            (%s,'锦鲤红包',0.001,10000,50000,TRUE,80)
            """, (chat_id, chat_id, chat_id))

@with_conn
def list_rules(conn, chat_id: int, limit=20, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,name,probability,min_milli,max_milli,enabled,priority
        FROM drop_rules WHERE chat_id=%s
        ORDER BY priority ASC,id ASC LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def get_rule(conn, chat_id: int, rid: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,name,probability,min_milli,max_milli,enabled,priority
        FROM drop_rules WHERE chat_id=%s AND id=%s
        """, (chat_id, rid))
        return cur.fetchone()

@with_conn
def update_rule(conn, chat_id: int, rid: int, p: float, mn: int, mx: int, en: bool, name: str):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE drop_rules SET probability=%s,min_milli=%s,max_milli=%s,enabled=%s,name=%s
        WHERE chat_id=%s AND id=%s
        """, (p, mn, mx, en, name, chat_id, rid))

@with_conn
def rules_count(conn, chat_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM drop_rules WHERE chat_id=%s", (chat_id,))
        return int(cur.fetchone()[0])

# =========================
# Wallet
# =========================
@with_conn
def wallet_get(conn, chat_id: int, user_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT balance_milli FROM wallets WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id))
        r = cur.fetchone()
        return int(r[0]) if r else 0

@with_conn
def wallet_add(conn, chat_id: int, user_id: int, delta: int,
               reason: str = "drop", display_name: str = ""):
    with conn.cursor() as cur:
        if delta < 0:
            raise ValueError("wallet_add delta 必须 >= 0")
        cur.execute("""
        INSERT INTO wallets(chat_id,user_id,balance_milli,display_name,updated_at)
        VALUES(%s,%s,%s,%s,NOW())
        ON CONFLICT(chat_id,user_id)
        DO UPDATE SET
          balance_milli=wallets.balance_milli+EXCLUDED.balance_milli,
          display_name=CASE WHEN EXCLUDED.display_name!='' THEN EXCLUDED.display_name ELSE wallets.display_name END,
          updated_at=NOW()
        """, (chat_id, user_id, delta, display_name or ""))
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,0,%s,%s,%s,'drop')
        """, (chat_id, user_id, delta, reason))

@with_conn
def wallet_adjust_admin(conn, chat_id: int, operator_id: int,
                        user_id: int, delta: int, reason: str):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT balance_milli FROM wallets WHERE chat_id=%s AND user_id=%s FOR UPDATE
        """, (chat_id, user_id))
        row = cur.fetchone()
        old = int(row[0]) if row else 0
        new = old + delta
        if new < 0:
            return False, f"余额不足，当前 {milli_to_coin(old)} 金币"
        cur.execute("""
        INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
        VALUES(%s,%s,%s,NOW())
        ON CONFLICT(chat_id,user_id) DO UPDATE SET balance_milli=%s,updated_at=NOW()
        """, (chat_id, user_id, new, new))
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,%s,%s,%s,%s,'admin')
        """, (chat_id, operator_id, user_id, delta, reason or "panel_adjust"))
        return True, f"用户 {user_id} 变动 {milli_to_coin(delta)} 金币，新余额 {milli_to_coin(new)} 金币"

@with_conn
def wallet_transfer_atomic(conn, chat_id: int, from_user_id: int, to_user_id: int, amount_milli: int):
    """
    群内用户转账（原子）
    - 同一 chat_id 内转账，不可跨群
    - 扣减转出方，增加转入方
    - 写 transfer 日志（双边）
    """
    if amount_milli <= 0:
        return False, "金额必须大于 0"
    if from_user_id == to_user_id:
        return False, "不能给自己转账"

    with conn.cursor() as cur:
        # 锁定转出方
        cur.execute("""
        SELECT balance_milli FROM wallets
        WHERE chat_id=%s AND user_id=%s
        FOR UPDATE
        """, (chat_id, from_user_id))
        row_from = cur.fetchone()
        from_bal = int(row_from[0]) if row_from else 0

        if from_bal < amount_milli:
            return False, f"余额不足（当前 {milli_to_coin(from_bal)} 金币）"

        # 锁定转入方（若不存在后续 UPSERT 创建）
        cur.execute("""
        SELECT balance_milli FROM wallets
        WHERE chat_id=%s AND user_id=%s
        FOR UPDATE
        """, (chat_id, to_user_id))
        row_to = cur.fetchone()
        to_bal = int(row_to[0]) if row_to else 0

        new_from = from_bal - amount_milli
        new_to = to_bal + amount_milli

        # 更新转出方
        cur.execute("""
        INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
        VALUES(%s,%s,%s,NOW())
        ON CONFLICT(chat_id,user_id)
        DO UPDATE SET balance_milli=%s,updated_at=NOW()
        """, (chat_id, from_user_id, new_from, new_from))

        # 更新转入方
        cur.execute("""
        INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
        VALUES(%s,%s,%s,NOW())
        ON CONFLICT(chat_id,user_id)
        DO UPDATE SET balance_milli=%s,updated_at=NOW()
        """, (chat_id, to_user_id, new_to, new_to))

        # 日志：转出（负）
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,%s,%s,%s,%s,'transfer')
        """, (chat_id, from_user_id, from_user_id, -amount_milli, f"to:{to_user_id}"))

        # 日志：转入（正）
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,%s,%s,%s,%s,'transfer')
        """, (chat_id, from_user_id, to_user_id, amount_milli, f"from:{from_user_id}"))

        return True, (new_from, new_to)
@with_conn
def get_all_wallets(conn, chat_id: int, limit=15, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT user_id,balance_milli,updated_at,display_name FROM wallets WHERE chat_id=%s
        ORDER BY balance_milli DESC LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def get_all_wallets_count(conn, chat_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wallets WHERE chat_id=%s", (chat_id,))
        return int(cur.fetchone()[0])

# =========================
# Coin Logs
# =========================
@with_conn
def coin_logs_page(conn, chat_id: int, limit=8, offset=0, log_type: str = None):
    with conn.cursor() as cur:
        if log_type and log_type != "all":
            cur.execute("""
            SELECT id,operator_id,user_id,delta_milli,reason,created_at,log_type
            FROM coin_logs WHERE chat_id=%s AND log_type=%s
            ORDER BY id DESC LIMIT %s OFFSET %s
            """, (chat_id, log_type, limit, offset))
        else:
            cur.execute("""
            SELECT id,operator_id,user_id,delta_milli,reason,created_at,log_type
            FROM coin_logs WHERE chat_id=%s
            ORDER BY id DESC LIMIT %s OFFSET %s
            """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def coin_logs_count(conn, chat_id: int, log_type: str = None) -> int:
    with conn.cursor() as cur:
        if log_type and log_type != "all":
            cur.execute("SELECT COUNT(*) FROM coin_logs WHERE chat_id=%s AND log_type=%s",
                        (chat_id, log_type))
        else:
            cur.execute("SELECT COUNT(*) FROM coin_logs WHERE chat_id=%s", (chat_id,))
        return int(cur.fetchone()[0])

@with_conn
def user_coin_logs_page(conn, chat_id: int, user_id: int, limit=8, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,operator_id,user_id,delta_milli,reason,created_at,log_type
        FROM coin_logs WHERE chat_id=%s AND user_id=%s
        ORDER BY id DESC LIMIT %s OFFSET %s
        """, (chat_id, user_id, limit, offset))
        return cur.fetchall()

@with_conn
def user_coin_logs_count(conn, chat_id: int, user_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM coin_logs WHERE chat_id=%s AND user_id=%s",
                    (chat_id, user_id))
        return int(cur.fetchone()[0])

# =========================
# Shop
# =========================
@with_conn
def shop_page(conn, chat_id: int, limit=6, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,title,price_milli,stock,enabled,description
        FROM shop_items WHERE chat_id=%s
        ORDER BY id ASC LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def shop_page_enabled(conn, chat_id: int, limit=6, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,title,price_milli,stock,enabled,description
        FROM shop_items
        WHERE chat_id=%s AND enabled=TRUE
        ORDER BY id ASC
        LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()
    
@with_conn
def get_item_by_display_index(conn, chat_id: int, display_idx: int):
    with conn.cursor() as cur:
        if display_idx <= 0:
            return None
        cur.execute("""
        SELECT id,title,price_milli,stock,enabled,description
        FROM shop_items
        WHERE chat_id=%s AND enabled=TRUE
        ORDER BY id ASC
        LIMIT 1 OFFSET %s
        """, (chat_id, display_idx - 1))
        return cur.fetchone()
    
@with_conn
def get_display_index_by_item_id(conn, chat_id: int, item_id: int, enabled_only: bool = False):
    with conn.cursor() as cur:
        if enabled_only:
            cur.execute("""
            SELECT COUNT(*) + 1
            FROM shop_items
            WHERE chat_id=%s AND enabled=TRUE AND id < %s
            """, (chat_id, item_id))
        else:
            cur.execute("""
            SELECT COUNT(*) + 1
            FROM shop_items
            WHERE chat_id=%s AND id < %s
            """, (chat_id, item_id))
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else 0
   
@with_conn
def shop_count(conn, chat_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM shop_items WHERE chat_id=%s", (chat_id,))
        return int(cur.fetchone()[0])

@with_conn
def shop_count_enabled(conn, chat_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM shop_items WHERE chat_id=%s AND enabled=TRUE",
                    (chat_id,))
        return int(cur.fetchone()[0])

@with_conn
def get_item(conn, chat_id: int, item_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,title,price_milli,stock,enabled,description
        FROM shop_items WHERE chat_id=%s AND id=%s
        """, (chat_id, item_id))
        return cur.fetchone()

@with_conn
def add_item(conn, chat_id: int, title: str, price_milli: int,
             stock, description: str = ""):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO shop_items(chat_id,title,price_milli,enabled,stock,description)
        VALUES(%s,%s,%s,TRUE,%s,%s)
        """, (chat_id, title, price_milli, stock, description or ""))

@with_conn
def update_item(conn, chat_id: int, item_id: int, title: str, price_milli: int,
                stock, enabled: bool, description: str = ""):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE shop_items SET title=%s,price_milli=%s,stock=%s,enabled=%s,description=%s
        WHERE chat_id=%s AND id=%s
        """, (title, price_milli, stock, enabled, description or "", chat_id, item_id))

@with_conn
def buy_item_atomic(conn, chat_id: int, user_id: int, item_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,title,price_milli,enabled,stock
        FROM shop_items WHERE chat_id=%s AND id=%s FOR UPDATE
        """, (chat_id, item_id))
        item = cur.fetchone()
        if not item:
            return False, "商品不存在"
        _id, title, price, enabled, stock = item
        if not enabled:
            return False, "商品已下架"
        if stock is not None and stock <= 0:
            return False, "库存不足"
        cur.execute("""
        SELECT balance_milli FROM wallets WHERE chat_id=%s AND user_id=%s FOR UPDATE
        """, (chat_id, user_id))
        w = cur.fetchone()
        bal = int(w[0]) if w else 0
        if bal < price:
            return False, f"金币不足（需要 {milli_to_coin(price)}，当前 {milli_to_coin(bal)}）"
        cur.execute("""
        UPDATE wallets SET balance_milli=balance_milli-%s,updated_at=NOW()
        WHERE chat_id=%s AND user_id=%s
        """, (price, chat_id, user_id))
        if stock is not None:
            cur.execute("UPDATE shop_items SET stock=stock-1 WHERE id=%s", (item_id,))
        cur.execute("""
        INSERT INTO redeem_orders(chat_id,user_id,item_id,price_milli,status)
        VALUES(%s,%s,%s,%s,'approved')
        """, (chat_id, user_id, item_id, price))
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,%s,%s,%s,%s,'redeem')
        """, (chat_id, user_id, user_id, -int(price), f"buy:{item_id}:{title}"))
        return True, f"✅ 兑换成功：{title}（-{milli_to_coin(price)} 金币）"
    
# =========================
# ██████╗ ███████╗██████╗ ██████╗  █████╗  ██████╗██╗  ██╗
# ██╔══██╗██╔════╝██╔══██╗██╔══██╗██╔══██╗██╔════╝██║ ██╔╝
# ██████╔╝█████╗  ██║  ██║██████╔╝███████║██║     █████╔╝
# ██╔══██╗██╔══╝  ██║  ██║██╔═══╝ ██╔══██║██║     ██╔═██╗
# ██║  ██║███████╗██████╔╝██║     ██║  ██║╚██████╗██║  ██╗
# 红包功能
# =========================

REDPACK_EXPIRE_SECONDS = 120   # 红包有效期（秒）
REDPACK_MIN_MILLI      = 10    # 单个红包最小金额（0.01金币）
REDPACK_MAX_COUNT      = 100   # 单次最多几个红包
REDPACK_MAX_TOTAL      = 100_000_000  # 单次最多总金额（milli）

# ---------- DB ----------

@with_conn
def rp_create(conn, pack_id: str, chat_id: int, sender_id: int,
              sender_name: str, total_milli: int, count: int, expire_seconds: int):
    with conn.cursor() as cur:
        # 先扣钱（原子）
        cur.execute("""
        SELECT balance_milli FROM wallets
        WHERE chat_id=%s AND user_id=%s FOR UPDATE
        """, (chat_id, sender_id))
        row = cur.fetchone()
        bal = int(row[0]) if row else 0
        if bal < total_milli:
            return False, f"余额不足（当前 {milli_to_coin(bal)} 金币）"
        cur.execute("""
        UPDATE wallets SET balance_milli=balance_milli-%s, updated_at=NOW()
        WHERE chat_id=%s AND user_id=%s
        """, (total_milli, chat_id, sender_id))
        # 写日志
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,%s,%s,%s,%s,'redpack')
        """, (chat_id, sender_id, sender_id, -total_milli, f"send_redpack:{pack_id}"))
        # 创建红包
        cur.execute("""
        INSERT INTO redpacks(pack_id,chat_id,sender_id,sender_name,
          total_milli,remain_milli,total_count,remain_count,status,expire_at)
        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,'open', NOW() + %s * INTERVAL '1 second')
        """, (pack_id, chat_id, sender_id, sender_name,
              total_milli, total_milli, count, count, expire_seconds))
        return True, "ok"

@with_conn
def rp_grab(conn, pack_id: str, chat_id: int, user_id: int, user_name: str):
    """
    抢红包原子操作
    返回 (ok: bool, msg: str, amount_milli: int)
    """
    with conn.cursor() as cur:
        # 锁定红包行
        cur.execute("""
        SELECT remain_milli, remain_count, status, expire_at, sender_id
        FROM redpacks WHERE pack_id=%s AND chat_id=%s FOR UPDATE
        """, (pack_id, chat_id))
        row = cur.fetchone()
        if not row:
            return False, "红包不存在", 0
        remain_milli, remain_count, status, expire_at, sender_id = row

        if status != 'open':
            return False, "红包已结束", 0
        # 过期检查（完全在数据库内比较，避免时区问题）
        cur.execute("SELECT NOW() > expire_at FROM redpacks WHERE pack_id=%s", (pack_id,))
        is_expired = cur.fetchone()[0]
        if is_expired:
            cur.execute("UPDATE redpacks SET status='expired' WHERE pack_id=%s", (pack_id,))
            return False, "红包已过期", 0
        if remain_count <= 0 or remain_milli <= 0:
            cur.execute("UPDATE redpacks SET status='finished' WHERE pack_id=%s", (pack_id,))
            return False, "红包已抢完", 0

        # 是否已抢过
        cur.execute("""
        SELECT 1 FROM redpack_records
        WHERE pack_id=%s AND user_id=%s
        """, (pack_id, user_id))
        if cur.fetchone():
            return False, "你已经抢过这个红包了", 0

        # 不能抢自己的（可选，注释掉则允许）
        if user_id == sender_id:
            return False, "不能抢自己发的红包", 0

        # 随机金额：最后一个全拿剩余
        if remain_count == 1:
            amount = remain_milli
        else:
            # 保证每个剩余红包至少有 REDPACK_MIN_MILLI
            max_grab = remain_milli - REDPACK_MIN_MILLI * (remain_count - 1)
            max_grab = max(REDPACK_MIN_MILLI, max_grab)
            amount = random.randint(REDPACK_MIN_MILLI, max_grab)

        new_remain_milli = remain_milli - amount
        new_remain_count = remain_count - 1
        new_status = 'finished' if new_remain_count == 0 else 'open'

        cur.execute("""
        UPDATE redpacks
        SET remain_milli=%s, remain_count=%s, status=%s
        WHERE pack_id=%s
        """, (new_remain_milli, new_remain_count, new_status, pack_id))

        # 给抢红包者加钱
        cur.execute("""
        INSERT INTO wallets(chat_id,user_id,balance_milli,display_name,updated_at)
        VALUES(%s,%s,%s,%s,NOW())
        ON CONFLICT(chat_id,user_id)
        DO UPDATE SET
          balance_milli=wallets.balance_milli+EXCLUDED.balance_milli,
          display_name=CASE WHEN EXCLUDED.display_name!='' THEN EXCLUDED.display_name ELSE wallets.display_name END,
          updated_at=NOW()
        """, (chat_id, user_id, amount, user_name))

        # 写记录
        cur.execute("""
        INSERT INTO redpack_records(pack_id,chat_id,user_id,user_name,amount_milli)
        VALUES(%s,%s,%s,%s,%s)
        """, (pack_id, chat_id, user_id, user_name, amount))

        # 写日志
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,%s,%s,%s,%s,'redpack')
        """, (chat_id, sender_id, user_id, amount, f"grab_redpack:{pack_id}"))

        return True, new_status, amount

@with_conn
def rp_expire_refund(conn, pack_id: str, chat_id: int):
    """超时退款"""
    with conn.cursor() as cur:
        cur.execute("""
        SELECT remain_milli, status, sender_id FROM redpacks
        WHERE pack_id=%s AND chat_id=%s FOR UPDATE
        """, (pack_id, chat_id))
        row = cur.fetchone()
        if not row:
            return False, 0
        remain_milli, status, sender_id = row
        if status not in ('open', 'expired'):
            return False, 0
        cur.execute("""
        UPDATE redpacks SET status='expired', remain_milli=0 WHERE pack_id=%s
        """, (pack_id,))
        if remain_milli > 0:
            cur.execute("""
            UPDATE wallets SET balance_milli=balance_milli+%s, updated_at=NOW()
            WHERE chat_id=%s AND user_id=%s
            """, (remain_milli, chat_id, sender_id))
            cur.execute("""
            INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
            VALUES(%s,%s,%s,%s,%s,'redpack')
            """, (chat_id, 0, sender_id, remain_milli, f"refund_redpack:{pack_id}"))
        return True, remain_milli

@with_conn
def rp_get(conn, pack_id: str, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT pack_id,sender_id,sender_name,total_milli,remain_milli,
               total_count,remain_count,status,expire_at
        FROM redpacks WHERE pack_id=%s AND chat_id=%s
        """, (pack_id, chat_id))
        return cur.fetchone()

@with_conn
def rp_records(conn, pack_id: str):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT user_id,user_name,amount_milli,grabbed_at
        FROM redpack_records WHERE pack_id=%s
        ORDER BY grabbed_at ASC
        """, (pack_id,))
        return cur.fetchall()

# ---------- 红包消息文本 ----------

def rp_text(pack_info, records) -> str:
    pack_id, sender_id, sender_name, total_milli, remain_milli, \
        total_count, remain_count, status, expire_at = pack_info

    grabbed_count = total_count - remain_count
    grabbed_milli = total_milli - remain_milli

    status_map = {
        'open':     '🟢 抢红包中',
        'finished': '🔴 已抢完',
        'expired':  '⏰ 已过期',
    }
    status_str = status_map.get(status, status)

    lines = [
        f"🧧 <b>{sender_name}</b> 发了一个红包！",
        f"💰 总金额：{milli_to_coin(total_milli)} 金币  共 {total_count} 个",
        f"📊 状态：{status_str}  已抢 {grabbed_count}/{total_count} 个",
        "",
    ]
    if records:
        lines.append("🎉 抢红包记录：")
        for uid, uname, amt, ts in records:
            lines.append(f"  • {uname}：+{milli_to_coin(amt)} 金币")
    if status == 'open':
        lines.append("")
        lines.append("👇 点击下方按钮抢红包")
    return "\n".join(lines)

def rp_keyboard(pack_id: str, status: str) -> InlineKeyboardMarkup:
    if status == 'open':
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("🧧 抢红包", callback_data=f"rp:grab:{pack_id}"),
            InlineKeyboardButton("📋 查看详情", callback_data=f"rp:detail:{pack_id}"),
        ]])
    else:
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("📋 查看详情", callback_data=f"rp:detail:{pack_id}"),
        ]])

# ---------- 红包 Handler ----------

async def handle_redpack_send(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str):
    """
    触发格式：发红包 <金额> <个数>
    例：发红包 10 5
    """
    chat_id = update.effective_chat.id
    user = update.effective_user
    uid = user.id
    display_name = fmt_display_name(user.first_name, user.last_name, user.username, uid)

    # 解析参数
    parts = text.strip().split()
    # parts[0] = '发红包', parts[1] = 金额, parts[2] = 个数
    if len(parts) < 3:
        await update.message.reply_text(
            "❌ 格式错误\n用法：发红包 <金额> <个数>\n例：发红包 10 5"
        )
        return

    try:
        total_milli = coin_to_milli(parts[1])
        count = int(parts[2])
    except Exception:
        await update.message.reply_text("❌ 金额或个数格式错误")
        return

    if total_milli <= 0:
        await update.message.reply_text("❌ 金额必须大于 0")
        return
    if count <= 0 or count > REDPACK_MAX_COUNT:
        await update.message.reply_text(f"❌ 个数必须在 1~{REDPACK_MAX_COUNT} 之间")
        return
    if total_milli > REDPACK_MAX_TOTAL:
        await update.message.reply_text(f"❌ 单次红包总金额不能超过 {milli_to_coin(REDPACK_MAX_TOTAL)} 金币")
        return
    # 保证每个红包至少 REDPACK_MIN_MILLI
    if total_milli < count * REDPACK_MIN_MILLI:
        await update.message.reply_text(
            f"❌ 金额太少，{count} 个红包至少需要 {milli_to_coin(count * REDPACK_MIN_MILLI)} 金币"
        )
        return

    pack_id = uuid.uuid4().hex[:12].upper()
    ok, msg = rp_create(pack_id, chat_id, uid, display_name,
                        total_milli, count, REDPACK_EXPIRE_SECONDS)
    if not ok:
        await update.message.reply_text(f"❌ 发红包失败：{msg}")
        return

    pack_info = rp_get(pack_id, chat_id)
    records = rp_records(pack_id)
    bot_msg = await update.message.reply_text(
        rp_text(pack_info, records),
        reply_markup=rp_keyboard(pack_id, 'open'),
        parse_mode="HTML"
    )

    # 记录消息ID，用于超时更新
    context.bot_data[f"rp_msg_{chat_id}_{pack_id}"] = bot_msg.message_id

    # 超时自动退款任务
    async def _expire_task():
        await asyncio.sleep(REDPACK_EXPIRE_SECONDS)
        ok2, refund = rp_expire_refund(pack_id, chat_id)
        pack_info2 = rp_get(pack_id, chat_id)
        records2 = rp_records(pack_id)
        mid = context.bot_data.get(f"rp_msg_{chat_id}_{pack_id}")
        if mid and pack_info2:
            try:
                expire_text = rp_text(pack_info2, records2)
                if ok2 and refund > 0:
                    expire_text += f"\n\n💸 已退回未抢金额 {milli_to_coin(refund)} 金币给发包人"
                await context.bot.edit_message_text(
                    chat_id=chat_id, message_id=mid,
                    text=expire_text,
                    reply_markup=rp_keyboard(pack_id, 'expired'),
                    parse_mode="HTML"
                )
            except Exception:
                pass

    asyncio.create_task(_expire_task())


async def cb_redpack(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """红包回调：抢红包 / 查看详情"""
    q = update.callback_query
    if not q or not update.effective_user or not q.message:
        return

    chat_id = q.message.chat_id
    if not is_allowed_chat(chat_id):
        await q.answer()
        return

    uid = update.effective_user.id
    user = update.effective_user
    display_name = fmt_display_name(user.first_name, user.last_name, user.username, uid)
    data = q.data or ""

    if data.startswith("rp:grab:"):
        pack_id = data[len("rp:grab:"):]
        ok, result, amount = rp_grab(pack_id, chat_id, uid, display_name)

        if not ok:
            await q.answer(f"❌ {result}", show_alert=True)
            # 若红包已结束，刷新消息
            if result in ("红包已结束", "红包已抢完", "红包已过期"):
                pack_info = rp_get(pack_id, chat_id)
                records = rp_records(pack_id)
                if pack_info:
                    try:
                        await q.edit_message_text(
                            rp_text(pack_info, records),
                            reply_markup=rp_keyboard(pack_id, pack_info[7]),
                            parse_mode="HTML"
                        )
                    except Exception:
                        pass
            return

        await q.answer(f"🎉 抢到 {milli_to_coin(amount)} 金币！", show_alert=True)

        # 刷新红包消息
        pack_info = rp_get(pack_id, chat_id)
        records = rp_records(pack_id)
        if pack_info:
            try:
                await q.edit_message_text(
                    rp_text(pack_info, records),
                    reply_markup=rp_keyboard(pack_id, pack_info[7]),
                    parse_mode="HTML"
                )
            except BadRequest as e:
                if "Message is not modified" not in str(e):
                    logger.exception("rp edit failed")
        return

    if data.startswith("rp:detail:"):
        pack_id = data[len("rp:detail:"):]
        pack_info = rp_get(pack_id, chat_id)
        records = rp_records(pack_id)
        if not pack_info:
            await q.answer("红包不存在", show_alert=True)
            return
        try:
            await q.edit_message_text(
                rp_text(pack_info, records),
                reply_markup=rp_keyboard(pack_id, pack_info[7]),
                parse_mode="HTML"
            )
        except BadRequest as e:
            if "Message is not modified" not in str(e):
                pass
        await q.answer()
        return

    await q.answer()


# =========================
# PC28 游戏
# =========================

PC28_INTERVAL   = 120    # 每期秒数（2分钟）
PC28_BET_CLOSE  = 20     # 开奖前N秒停止下注
PC28_ODDS       = 1.9    # 大小单双赔率
PC28_MIN_BET    = 10     # 最小下注（milli）
PC28_MAX_BET    = 10_000_000  # 最大下注（milli）

# 合法投注类型
PC28_BET_TYPES = {"大", "小", "单", "双"}

# ---------- PC28 开关（Redis）----------

def pc28_is_enabled(chat_id: int) -> bool:
    """检查该群PC28是否已由管理员开启"""
    return rds.get(f"pc28_on:{chat_id}") == "1"

def pc28_set_enabled(chat_id: int, enabled: bool):
    """管理员开启/关闭PC28"""
    if enabled:
        rds.set(f"pc28_on:{chat_id}", "1")
    else:
        rds.delete(f"pc28_on:{chat_id}")

# ---------- PC28 DB ----------

# 修改后
@with_conn
def pc28_get_or_create_period(conn, chat_id: int) -> tuple:
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id, period, open_at, status, result
        FROM pc28_periods
        WHERE chat_id=%s AND status IN ('betting','closed')
        AND DATE(open_at) = CURRENT_DATE
        ORDER BY period DESC LIMIT 1
        """, (chat_id,))
        row = cur.fetchone()
        if row:
            return row
        # 创建第一期：当日期数从1开始
        cur.execute("""
        SELECT COALESCE(MAX(period),0)+1 FROM pc28_periods
        WHERE chat_id=%s AND DATE(open_at) = CURRENT_DATE
        """, (chat_id,))
        next_period = cur.fetchone()[0]
        cur.execute("""
        INSERT INTO pc28_periods(chat_id,period,open_at,status)
        VALUES(%s, %s, NOW() + %s * INTERVAL '1 second', 'betting')
        RETURNING id,period,open_at,status,result
        """, (chat_id, next_period, PC28_INTERVAL))
        return cur.fetchone()

@with_conn
def pc28_get_current(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,period,open_at,status,result
        FROM pc28_periods
        WHERE chat_id=%s AND status IN ('betting','closed')
        AND DATE(open_at) = CURRENT_DATE
        ORDER BY period DESC LIMIT 1
        """, (chat_id,))
        return cur.fetchone()

@with_conn
def pc28_place_bet(conn, chat_id: int, period_id: int, user_id: int,
                   user_name: str, bet_type: str, amount_milli: int):
    with conn.cursor() as cur:
        # 检查期状态
        cur.execute("SELECT status FROM pc28_periods WHERE id=%s FOR UPDATE", (period_id,))
        row = cur.fetchone()
        if not row or row[0] != 'betting':
            return False, "当前期已停止下注"

        # 扣钱
        cur.execute("""
        SELECT balance_milli FROM wallets
        WHERE chat_id=%s AND user_id=%s FOR UPDATE
        """, (chat_id, user_id))
        wrow = cur.fetchone()
        bal = int(wrow[0]) if wrow else 0
        if bal < amount_milli:
            return False, f"余额不足（当前 {milli_to_coin(bal)} 金币）"

        cur.execute("""
        UPDATE wallets SET balance_milli=balance_milli-%s, updated_at=NOW()
        WHERE chat_id=%s AND user_id=%s
        """, (amount_milli, chat_id, user_id))

        # 写注单
        cur.execute("""
        INSERT INTO pc28_bets(chat_id,period_id,user_id,user_name,bet_type,amount_milli,status)
        VALUES(%s,%s,%s,%s,%s,%s,'pending')
        """, (chat_id, period_id, user_id, user_name, bet_type, amount_milli))

        # 写日志
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,%s,%s,%s,%s,'pc28')
        """, (chat_id, user_id, user_id, -amount_milli, f"pc28_bet:{bet_type}:{period_id}"))

        return True, "ok"

@with_conn
def pc28_settle(conn, chat_id: int, period_id: int, result_str: str):
    """
    开奖结算
    result_str: 如 "14:大:双"  (和值:大小:单双)
    """
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE pc28_periods SET status='settled', result=%s
        WHERE id=%s AND chat_id=%s
        """, (result_str, period_id, chat_id))

        # 取所有待结算注单
        cur.execute("""
        SELECT id,user_id,user_name,bet_type,amount_milli
        FROM pc28_bets WHERE period_id=%s AND chat_id=%s AND status='pending'
        FOR UPDATE
        """, (period_id, chat_id))
        bets = cur.fetchall()

        # 解析结果
        parts = result_str.split(":")
        winning_types = set(parts[1:])  # {'大','双'} 或 {'小','单'} 等

        winners = []
        for bid, uid, uname, btype, amt in bets:
            if btype in winning_types:
                payout = int(amt * PC28_ODDS)
                cur.execute("""
                UPDATE pc28_bets SET status='win', payout_milli=%s WHERE id=%s
                """, (payout, bid))
                # 返还本金+奖励
                cur.execute("""
                INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
                VALUES(%s,%s,%s,NOW())
                ON CONFLICT(chat_id,user_id)
                DO UPDATE SET balance_milli=wallets.balance_milli+EXCLUDED.balance_milli,updated_at=NOW()
                """, (chat_id, uid, payout))
                cur.execute("""
                INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
                VALUES(%s,0,%s,%s,%s,'pc28')
                """, (chat_id, uid, payout, f"pc28_win:{btype}:{period_id}"))
                winners.append((uid, uname, btype, amt, payout))
            else:
                cur.execute("""
                UPDATE pc28_bets SET status='lose', payout_milli=0 WHERE id=%s
                """, (bid,))
        return winners, bets

@with_conn
def pc28_get_bets(conn, chat_id: int, period_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT user_id,user_name,bet_type,amount_milli,status,payout_milli
        FROM pc28_bets
        WHERE period_id=%s AND chat_id=%s AND status != 'refund'
        ORDER BY id ASC
        """, (period_id, chat_id))
        return cur.fetchall()
    
@with_conn
def pc28_close_betting(conn, chat_id: int, period_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE pc28_periods SET status='closed'
        WHERE id=%s AND chat_id=%s AND status='betting'
        """, (period_id, chat_id))

@with_conn
def pc28_create_next(conn, chat_id: int) -> tuple:
    with conn.cursor() as cur:
        # 当日期数自增
        cur.execute("""
        SELECT COALESCE(MAX(period),0)+1 FROM pc28_periods
        WHERE chat_id=%s AND DATE(open_at) = CURRENT_DATE
        """, (chat_id,))
        next_period = cur.fetchone()[0]
        cur.execute("""
        INSERT INTO pc28_periods(chat_id,period,open_at,status)
        VALUES(%s, %s, NOW() + %s * INTERVAL '1 second', 'betting')
        RETURNING id,period,open_at,status,result
        """, (chat_id, next_period, PC28_INTERVAL))
        return cur.fetchone()

# ── 新增：停止时退款当前未结算注单 ──
@with_conn
def pc28_refund_pending(conn, chat_id: int) -> int:
    """退还当前未结算期的所有pending注单，返回退款笔数"""
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id FROM pc28_periods
        WHERE chat_id=%s AND status IN ('betting','closed')
        ORDER BY period DESC LIMIT 1
        """, (chat_id,))
        row = cur.fetchone()
        if not row:
            return 0
        period_id = row[0]
        cur.execute("""
        SELECT id, user_id, amount_milli FROM pc28_bets
        WHERE period_id=%s AND chat_id=%s AND status='pending'
        FOR UPDATE
        """, (period_id, chat_id))
        bets = cur.fetchall()
        for bid, uid, amt in bets:
            cur.execute("""
            UPDATE pc28_bets SET status='refund' WHERE id=%s
            """, (bid,))
            cur.execute("""
            INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
            VALUES(%s,%s,%s,NOW())
            ON CONFLICT(chat_id,user_id)
            DO UPDATE SET balance_milli=wallets.balance_milli+EXCLUDED.balance_milli,
                          updated_at=NOW()
            """, (chat_id, uid, amt))
            cur.execute("""
            INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
            VALUES(%s,0,%s,%s,'pc28_stop_refund','pc28')
            """, (chat_id, uid, amt))
        cur.execute("""
        UPDATE pc28_periods SET status='cancelled'
        WHERE id=%s
        """, (period_id,))
        return len(bets)

async def _pc28_refund_current(context: ContextTypes.DEFAULT_TYPE, chat_id: int):
    """异步退款任务，退完后发群通知"""
    count = pc28_refund_pending(chat_id)
    if count > 0:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"💸 PC28已停止，共退还 <b>{count}</b> 笔注单金币给各下注用户",
                parse_mode="HTML"
            )
        except Exception:
            logger.exception("_pc28_refund_current send failed")

# ---------- PC28 开奖逻辑 ----------

def pc28_draw() -> tuple:
    """
    模拟PC28开奖：3个骰子（0-9）求和
    返回 (n1, n2, n3, total, size, parity)
    size: 大(>=14) / 小(<14)
    parity: 单 / 双
    """
    n1, n2, n3 = random.randint(0, 9), random.randint(0, 9), random.randint(0, 9)
    total = n1 + n2 + n3
    size = "大" if total >= 14 else "小"
    parity = "双" if total % 2 == 0 else "单"
    return n1, n2, n3, total, size, parity

def pc28_period_text(period_row, bets=None, is_current=True) -> str:
    pid, period_no, open_at, status, result = period_row

    # 倒计时
    if status == 'betting':
        try:
            now = datetime.datetime.now(tz=open_at.tzinfo)
            remain = max(0, int((open_at - now).total_seconds()))
            close_remain = max(0, remain - PC28_BET_CLOSE)
        except Exception:
            remain = 0
            close_remain = 0
        time_line = f"⏳ 距开奖：{remain}秒  距停止下注：{close_remain}秒"
        status_str = "🟢 投注中"
    elif status == 'closed':
        time_line = "🔒 已停止下注，等待开奖..."
        status_str = "🔒 停止下注"
    elif status == 'settled':
        time_line = ""
        status_str = "✅ 已开奖"
    else:
        time_line = ""
        status_str = status

    date_str = open_at.strftime("%m月%d日") if open_at else ""
    lines = [
        f"🎲 <b>PC28 {date_str} 第 {period_no} 期</b>",
        f"📌 状态：{status_str}",
    ]
    if time_line:
        lines.append(time_line)

    if result:
        parts = result.split(":")
        total_val = parts[0]
        tags = " ".join(parts[1:])
        lines.append(f"🎯 开奖结果：{total_val}  [{tags}]")

    lines.append("")
    lines.append("📊 赔率：大/小/单/双 均为 1.9 倍")
    lines.append("💬 投注格式：押大 10  押小 5  押单 3  押双 8")

    if bets:
        lines.append("")
        lines.append("📋 本期注单：")
        # 按类型汇总
        summary: dict = {}
        for uid, uname, btype, amt, st, payout in bets:
            summary.setdefault(btype, []).append((uname, amt, st, payout))
        for btype, items in summary.items():
            total_bet = sum(a for _, a, _, _ in items)
            lines.append(f"  {btype}：共 {len(items)} 注 / {milli_to_coin(total_bet)} 金币")

    return "\n".join(lines)

# ---------- PC28 定时任务 ----------

# 全局：每个群的当前期信息缓存 {chat_id: (period_id, open_at)}
_pc28_tasks: dict[int, asyncio.Task] = {}

async def pc28_run_period(context: ContextTypes.DEFAULT_TYPE, chat_id: int,
                          period_row: tuple, announce_msg_id: int | None = None):
    """
    管理单期生命周期：
    1. 等待到停止下注时间 → 更新消息
    2. 等待到开奖时间 → 开奖 → 发结果 → 创建下一期
    """
    pid, period_no, open_at, status, _ = period_row

    try:
        # ── 检查开关，若已关闭则直接退出 ──
        if not pc28_is_enabled(chat_id):
            return

        # 计算剩余时间
        now = datetime.datetime.now(tz=open_at.tzinfo)
        total_remain = max(0, (open_at - now).total_seconds())
        close_remain = max(0, total_remain - PC28_BET_CLOSE)

        # ── 阶段1：等到停止下注 ──
        if close_remain > 0:
            await asyncio.sleep(close_remain)

        pc28_close_betting(chat_id, pid)

        # 更新公告消息
        if announce_msg_id:
            period_row2 = pc28_get_current(chat_id)
            if period_row2:
                try:
                    bets = pc28_get_bets(chat_id, pid)
                    await context.bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=announce_msg_id,
                        text=pc28_period_text(period_row2, bets, is_current=True),
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

        # ── 阶段2：等到开奖 ──
        await asyncio.sleep(PC28_BET_CLOSE)

        # 开奖
        n1, n2, n3, total, size, parity = pc28_draw()
        result_str = f"{total}:{size}:{parity}"
        winners, all_bets = pc28_settle(chat_id, pid, result_str)

        # 构造开奖消息
        open_at_row = period_row[2]
        date_str = open_at_row.strftime("%m月%d日") if open_at_row else ""
        result_lines = [
            f"🎲 <b>PC28 {date_str} 第 {period_no} 期 开奖结果</b>",
            f"🎯 号码：{n1} + {n2} + {n3} = <b>{total}</b>",
            f"📌 结果：<b>{size} {parity}</b>",
            "",
        ]
        if winners:
            result_lines.append("🏆 中奖名单：")
            for uid, uname, btype, amt, payout in winners:
                profit = payout - amt
                result_lines.append(
                    f"  🎉 {uname}  押{btype} {milli_to_coin(amt)}金币"
                    f"  → +{milli_to_coin(profit)} 金币"
                )
        else:
            result_lines.append("😔 本期无人中奖")

        result_lines.append("")
        result_lines.append(f"⏰ 下一期即将开始...")

        result_msg = await context.bot.send_message(
            chat_id=chat_id,
            text="\n".join(result_lines),
            parse_mode="HTML"
        )
        # 开奖结果30秒后自毁
        auto_delete_pair(context, chat_id,
            result_msg.message_id, result_msg.message_id, 30)

        # 更新公告消息为已结束
        if announce_msg_id:
            try:
                await context.bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=announce_msg_id,
                    text="\n".join(result_lines),
                    parse_mode="HTML"
                )
            except Exception:
                pass

        # ── 创建下一期前再次检查开关 ──
        if not pc28_is_enabled(chat_id):
            return

        # ── 创建下一期 ──
        await asyncio.sleep(3)
        next_row = pc28_create_next(chat_id)
        bets_next = []
        new_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=pc28_period_text(next_row, bets_next, is_current=True),
            parse_mode="HTML"
        )
        # 递归启动下一期
        task = asyncio.create_task(
            pc28_run_period(context, chat_id, next_row, new_msg.message_id)
        )
        _pc28_tasks[chat_id] = task

    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception(f"pc28_run_period error chat={chat_id}")


async def handle_pc28_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    关键词 '28开奖' → 仅群管理员可开启
    关键词 '28停止' → 仅群管理员可停止
    """
    chat_id = update.effective_chat.id
    uid = update.effective_user.id
    msg_text = (update.message.text or "").strip()

    # ── 停止指令 ──
    if msg_text == "28停止":
        if not await is_group_admin(context, chat_id, uid):
            await update.message.reply_text("❌ 仅群管理员可停止 PC28 游戏")
            return
        task = _pc28_tasks.pop(chat_id, None)
        if task and not task.done():
            task.cancel()
        pc28_set_enabled(chat_id, False)
        bot_msg = await update.message.reply_text(
            "🛑 PC28 游戏已停止\n正在退还当前期未开奖注单..."
        )
        auto_delete_pair(context, chat_id,
            update.message.message_id, bot_msg.message_id, 30)
        asyncio.create_task(_pc28_refund_current(context, chat_id))
        return

    # ── 开启指令（28开奖）──
    if not await is_group_admin(context, chat_id, uid):
        await update.message.reply_text("❌ 仅群管理员可开启 PC28 游戏")
        return

    if pc28_is_enabled(chat_id):
        # 已开启，只刷新显示当前期状态
        period_row = pc28_get_or_create_period(chat_id)
        bets = pc28_get_bets(chat_id, period_row[0])
        bot_msg = await update.message.reply_text(
            pc28_period_text(period_row, bets, is_current=True),
            parse_mode="HTML"
        )
        # 只删触发指令消息，公告消息不自毁
        auto_delete_pair(context, chat_id,
            update.message.message_id, update.message.message_id, 10)
        # 若任务意外停了则重启
        existing = _pc28_tasks.get(chat_id)
        if existing is None or existing.done():
            task = asyncio.create_task(
                pc28_run_period(context, chat_id, period_row, None)
            )
            _pc28_tasks[chat_id] = task
        return

    # 首次开启
    pc28_set_enabled(chat_id, True)
    period_row = pc28_get_or_create_period(chat_id)
    bets = pc28_get_bets(chat_id, period_row[0])
    msg = await update.message.reply_text(
        pc28_period_text(period_row, bets, is_current=True),
        parse_mode="HTML"
    )
    # 只删触发指令消息，公告消息不自毁（由游戏流程管理）
    auto_delete_pair(context, chat_id,
        update.message.message_id, update.message.message_id, 10)
    existing = _pc28_tasks.get(chat_id)
    if existing is None or existing.done():
        task = asyncio.create_task(
            pc28_run_period(context, chat_id, period_row, msg.message_id)
        )
        _pc28_tasks[chat_id] = task

async def handle_pc28_bet(update: Update, context: ContextTypes.DEFAULT_TYPE,
                          bet_type: str, text: str):
    """
    关键词：押大/押小/押单/押双 <金额>
    """
    chat_id = update.effective_chat.id
    user = update.effective_user
    uid = user.id
    display_name = fmt_display_name(user.first_name, user.last_name, user.username, uid)

    # ── 新增：检查PC28是否已由管理员开启 ──
    if not pc28_is_enabled(chat_id):
        await update.message.reply_text("❌ PC28 游戏未开启，请等待管理员开启")
        return

    parts = text.strip().split()
    if len(parts) < 2:
        await update.message.reply_text(f"❌ 格式：押{bet_type} <金额>\n例：押{bet_type} 10")
        return

    try:
        amount_milli = coin_to_milli(parts[1])
    except Exception:
        await update.message.reply_text("❌ 金额格式错误")
        return

    if amount_milli < PC28_MIN_BET:
        await update.message.reply_text(f"❌ 最小下注 {milli_to_coin(PC28_MIN_BET)} 金币")
        return
    if amount_milli > PC28_MAX_BET:
        await update.message.reply_text(f"❌ 最大下注 {milli_to_coin(PC28_MAX_BET)} 金币")
        return

    period_row = pc28_get_current(chat_id)
    if not period_row:
        await update.message.reply_text("❌ 当前没有进行中的期，请先发送「28开奖」开启游戏")
        return

    pid, period_no, open_at, status, _ = period_row
    if status != 'betting':
        await update.message.reply_text("❌ 当前期已停止下注，请等待下一期")
        return

    ok, msg = pc28_place_bet(chat_id, pid, uid, display_name, bet_type, amount_milli)
    if not ok:
        await update.message.reply_text(f"❌ 下注失败：{msg}")
        return

    bal = wallet_get(chat_id, uid)
    bot_msg = await update.message.reply_text(
        f"✅ 下注成功！\n"
        f"第 {period_no} 期  押 <b>{bet_type}</b>  {milli_to_coin(amount_milli)} 金币\n"
        f"赔率：{PC28_ODDS}x  中奖可得：{milli_to_coin(int(amount_milli * PC28_ODDS))} 金币\n"
        f"剩余余额：{milli_to_coin(bal)} 金币",
        parse_mode="HTML"
    )
    auto_delete_pair(context, chat_id,
        update.message.message_id, bot_msg.message_id, 30)

# =========================
# Statistics
# =========================
@with_conn
def get_chat_stats(conn, chat_id: int) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wallets WHERE chat_id=%s", (chat_id,))
        total_users = int(cur.fetchone()[0])
        cur.execute("SELECT COALESCE(SUM(balance_milli),0) FROM wallets WHERE chat_id=%s",
                    (chat_id,))
        total_balance = int(cur.fetchone()[0])
        cur.execute("""SELECT COALESCE(SUM(delta_milli),0) FROM coin_logs
            WHERE chat_id=%s AND log_type='drop' AND created_at>=CURRENT_DATE""", (chat_id,))
        today_drop = int(cur.fetchone()[0])
        cur.execute("""SELECT COALESCE(SUM(ABS(delta_milli)),0) FROM coin_logs
            WHERE chat_id=%s AND log_type='redeem' AND created_at>=CURRENT_DATE""", (chat_id,))
        today_redeem = int(cur.fetchone()[0])
        cur.execute("""SELECT COALESCE(SUM(delta_milli),0) FROM coin_logs
            WHERE chat_id=%s AND log_type='drop'
            AND created_at>=DATE_TRUNC('month',CURRENT_DATE)""", (chat_id,))
        month_drop = int(cur.fetchone()[0])
        cur.execute("""SELECT COALESCE(SUM(ABS(delta_milli)),0) FROM coin_logs
            WHERE chat_id=%s AND log_type='redeem'
            AND created_at>=DATE_TRUNC('month',CURRENT_DATE)""", (chat_id,))
        month_redeem = int(cur.fetchone()[0])
        cur.execute("""SELECT COUNT(*) FROM redeem_orders
            WHERE chat_id=%s AND created_at>=CURRENT_DATE""", (chat_id,))
        today_orders = int(cur.fetchone()[0])
        cur.execute("""SELECT COUNT(*) FROM redeem_orders
            WHERE chat_id=%s AND created_at>=DATE_TRUNC('month',CURRENT_DATE)""", (chat_id,))
        month_orders = int(cur.fetchone()[0])
        cur.execute("""SELECT COALESCE(SUM(delta_milli),0) FROM coin_logs
            WHERE chat_id=%s AND log_type='admin' AND delta_milli>0
            AND created_at>=CURRENT_DATE""", (chat_id,))
        today_admin_add = int(cur.fetchone()[0])
        return {
            "total_users": total_users,
            "total_balance": total_balance,
            "today_drop": today_drop,
            "today_redeem": today_redeem,
            "month_drop": month_drop,
            "month_redeem": month_redeem,
            "today_orders": today_orders,
            "month_orders": month_orders,
            "today_admin_add": today_admin_add,
        }

@with_conn
def get_wallet_rank(conn, chat_id: int, limit=15, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT user_id,balance_milli,display_name FROM wallets
        WHERE chat_id=%s AND balance_milli>0
        ORDER BY balance_milli DESC LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def get_wallet_rank_count(conn, chat_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wallets WHERE chat_id=%s AND balance_milli>0",
                    (chat_id,))
        return int(cur.fetchone()[0])

# =========================
# Redis 风控
# =========================
def can_reward(chat_id: int, user_id: int, text: str) -> bool:
    k_pm = f"pm:{chat_id}:{user_id}"
    n = rds.incr(k_pm)
    if n == 1:
        rds.expire(k_pm, 60)
    if n > PER_MINUTE_CAP:
        return False
    k_cd = f"cd:{chat_id}:{user_id}"
    if not rds.set(k_cd, "1", ex=COOLDOWN_SECONDS, nx=True):
        return False
    if ENABLE_SAME_TEXT_BLOCK:
        t = (text or "").strip().lower()
        if t:
            k_lt = f"lt:{chat_id}:{user_id}"
            old = rds.get(k_lt)
            if old == t:
                return False
            rds.setex(k_lt, 120, t)
    daily = int(rds.get(f"daily:{chat_id}:{user_id}") or 0)
    if daily >= DAILY_MAX_MILLI:
        return False
    return True

def add_daily(chat_id: int, user_id: int, amount: int):
    k = f"daily:{chat_id}:{user_id}"
    p = rds.pipeline()
    p.incrby(k, amount)
    p.expire(k, 86400)
    p.execute()

# =========================
# UI Constants
# =========================
SHOP_SIZE = 6
RULE_SIZE = 6
LOG_SIZE = 8
WALLET_PAGE_SIZE = 15
RANK_SIZE = 15

# =========================
# UI Helpers
# =========================
def fmt_rule_row(r):
    rid, name, p, mn, mx, en, pr = r
    return f"{'✅' if en else '❌'} {name}｜{p*100:.4f}%｜{milli_to_coin(mn)}~{milli_to_coin(mx)}"

def selected_chat_id(context: ContextTypes.DEFAULT_TYPE):
    return safe_int(context.user_data.get("sel_chat_id"), 0)

def selected_chat_title(context: ContextTypes.DEFAULT_TYPE):
    return context.user_data.get("sel_chat_title", "")

def clear_pending_state(context: ContextTypes.DEFAULT_TYPE):
    for k in [
        "await_target_input", "await_rule_name", "await_item_title",
        "await_add_item_input", "await_rule_prob", "await_rule_min",
        "await_rule_max", "await_item_price", "await_item_stock",
        "await_item_desc", "await_rank_keywords", "await_shop_keywords",
        "await_redeem_notice", "await_adm_add", "await_adm_sub",
        "await_rank_delete_seconds",
    ]:
        context.user_data.pop(k, None)

def console_text(context: ContextTypes.DEFAULT_TYPE) -> str:
    title = selected_chat_title(context) or str(selected_chat_id(context))
    return (
        f"🏠 管理控制台\n"
        f"📌 当前群组：{title}\n"
        f"所有操作均作用于上方群组，切换群组请点击底部按钮。"
    )

def adm_user_text(chat_id: int, target: int) -> str:
    bal = wallet_get(chat_id, target)
    return (
        f"💰 金币管理\n"
        f"目标用户：{target}\n"
        f"当前余额：{milli_to_coin(bal)} 金币\n"
        f"📖 说明：\n"
        f"• 先点「选择用户」输入用户ID\n"
        f"• 再点「增加」或「扣除」输入金额\n"
        f"• 支持小数，如 0.5 = 0.5金币"
    )

def stats_text(chat_id: int) -> str:
    s = get_chat_stats(chat_id)
    return (
        "📊 数据统计\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"👥 总参与用户：{s['total_users']} 人\n"
        f"💰 全群流通金币：{milli_to_coin(s['total_balance'])}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📅 今日\n"
        f"  🎁 发放：{milli_to_coin(s['today_drop'])}\n"
        f"  🛒 兑换：{milli_to_coin(s['today_redeem'])}\n"
        f"  📦 兑换笔数：{s['today_orders']} 笔\n"
        f"  🔧 管理员增发：{milli_to_coin(s['today_admin_add'])}\n"
        "━━━━━━━━━━━━━━━━━━\n"
        "📆 本月\n"
        f"  🎁 发放：{milli_to_coin(s['month_drop'])}\n"
        f"  🛒 兑换：{milli_to_coin(s['month_redeem'])}\n"
        f"  📦 兑换笔数：{s['month_orders']} 笔\n"
    )

def rank_text(chat_id: int, page: int) -> tuple:
    total = get_wallet_rank_count(chat_id)
    max_page = max(0, (total - 1) // RANK_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    rows = get_wallet_rank(chat_id, RANK_SIZE, page * RANK_SIZE)
    medals = ["🥇", "🥈", "🥉"]
    lines = [f"🏆 金币排行榜（第 {page+1}/{max_page+1} 页）\n"]
    for i, (uid, bal, display_name) in enumerate(rows):
        rank_num = page * RANK_SIZE + i + 1
        medal = medals[rank_num - 1] if rank_num <= 3 else f"#{rank_num}"
        name = display_name if display_name else str(uid)
        lines.append(
            f"{medal} <a href=\"tg://user?id={uid}\">{name}</a> — {milli_to_coin(bal)} 金币"
        )
    txt = "\n".join(lines) if rows else "🏆 暂无排行数据"
    return txt, max_page

def item_detail_text(it, display_idx: int | None = None) -> str:
    _id, title, price, stock, enabled, desc = it
    no_line = f"序号：{display_idx}" if display_idx else f"编号：{_id}"
    lines = [
        "🎁 商品详情",
        no_line,
        f"名称：{title}",
        f"价格：{milli_to_coin(price)} 金币",
        f"库存：{'∞' if stock is None else stock}",
        f"状态：{'✅ 上架中' if enabled else '❌ 已下架'}",
    ]
    if desc:
        lines.append(f"描述：{desc}")
    return "\n".join(lines)

def rule_detail_text(r) -> str:
    _id, name, p, mn, mx, en, pr = r
    return (
        f"⚙️ 规则详情\n"
        f"名称：{name}\n"
        f"概率：{p*100:.4f}%\n"
        f"金额范围：{milli_to_coin(mn)} ~ {milli_to_coin(mx)} 金币\n"
        f"状态：{'✅ 开启' if en else '❌ 关闭'}\n"
        f"📖 概率格式：输入 5 或 5% 均表示5%"
    )

def settings_text(chat_id: int) -> str:
    s = get_chat_settings(chat_id)
    kw = s.get("rank_keywords", "排行榜,排名,积分榜")
    skw = s.get("shop_keywords", "商城,兑换,商店")
    notice = s.get("redeem_notice", "") or "（未设置）"
    rd = s.get("rank_delete_seconds", 120)
    return (
        f"⚙️ 群组设置\n"
        f"🔑 排行榜关键词：{kw}\n"
        f"🛒 商城关键词：{skw}\n"
        f"⏱ 排行榜自毁时间：{rd} 秒\n"
        f"📢 兑换通知文本：{notice}\n"
        f"📖 说明：\n"
        f"• 排行榜/商城关键词：逗号分隔，群内发送即触发\n"
        f"• 自毁时间：排行榜消息多少秒后自动删除（0=不删除）\n"
        f"• 兑换通知：兑换成功后附加在通知末尾的说明"
    )

def user_panel_text(chat_id: int, user_id: int) -> str:
    bal = wallet_get(chat_id, user_id)
    return (
        f"🛍️ 我的钱包\n"
        f"用户：<a href=\"tg://user?id={user_id}\">{user_id}</a>\n"
        f"余额：{milli_to_coin(bal)} 金币\n"
        f"点击下方按钮兑换商品或查看记录"
    )

def kb_cancel(back_data: str = "v4:admin_home"):
    return [InlineKeyboardButton("❌ 取消", callback_data=f"v4:cancel_input:{back_data}")]

# =========================
# Keyboards
# =========================
def kb_home():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗂️ 选择管理群组", callback_data="v4:groups:0")],
        [InlineKeyboardButton("🆔 查看我的ID", callback_data="v4:show_myid")],
    ])

def kb_groups(page: int, chats: list):
    """chats: [(chat_id, title), ...]"""
    total = len(chats)
    max_page = max(0, (total - 1) // 8) if total > 0 else 0
    page = clamp(page, 0, max_page)
    rows = []
    for cid, title in chats[page*8:(page+1)*8]:
        rows.append([InlineKeyboardButton(
            f"📌 {title or cid}",
            callback_data=f"v4:selgroup:{cid}"
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v4:groups:{max(0,page-1)}"),
        InlineKeyboardButton(f"{page+1}/{max_page+1}", callback_data="v4:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v4:groups:{min(max_page,page+1)}")
    ])
    rows.append([InlineKeyboardButton("🔄 刷新列表", callback_data="v4:groups:0:refresh")])
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="v4:home")])
    return InlineKeyboardMarkup(rows)

def kb_console():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 金币管理", callback_data="v4:adm_user"),
         InlineKeyboardButton("⚙️ 抽奖规则", callback_data="v4:adm_rules:0")],
        [InlineKeyboardButton("🎁 商品管理", callback_data="v4:adm_shop:0"),
         InlineKeyboardButton("🧾 操作日志", callback_data="v4:logs:0:all")],
        [InlineKeyboardButton("📊 数据统计", callback_data="v4:stats"),
         InlineKeyboardButton("🏆 排行榜", callback_data="v4:rank:0")],
        [InlineKeyboardButton("💼 用户余额", callback_data="v4:wallets:0"),
         InlineKeyboardButton("⚙️ 群组设置", callback_data="v4:chat_settings")],
        [InlineKeyboardButton("🔁 切换群组", callback_data="v4:groups:0")],
    ])

def kb_adm_user():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 选择目标用户", callback_data="v4:adm_target")],
        [InlineKeyboardButton("➕ 增加金币", callback_data="v4:adm_add_input"),
         InlineKeyboardButton("➖ 扣除金币", callback_data="v4:adm_sub_input")],
        [InlineKeyboardButton("🔍 查询余额", callback_data="v4:adm_qbal")],
        [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")],
    ])

def kb_adm_rules(chat_id: int, page: int):
    total = rules_count(chat_id)
    max_page = max(0, (total - 1) // RULE_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    rs = list_rules(chat_id, RULE_SIZE, page * RULE_SIZE)
    rows = []
    for r in rs:
        rows.append([InlineKeyboardButton(
            fmt_rule_row(r), callback_data=f"v4:adm_rule:{r[0]}"
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v4:adm_rules:{max(0,page-1)}"),
        InlineKeyboardButton(f"{page+1}/{max_page+1}", callback_data="v4:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v4:adm_rules:{min(max_page,page+1)}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")])
    return InlineKeyboardMarkup(rows)

def kb_rule_edit(rid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ 修改名称", callback_data=f"v4:r:name:{rid}"),
         InlineKeyboardButton("🔁 开启/关闭", callback_data=f"v4:r:toggle:{rid}")],
        [InlineKeyboardButton("📊 修改概率", callback_data=f"v4:r:prob_input:{rid}")],
        [InlineKeyboardButton("📉 修改最小金额", callback_data=f"v4:r:min_input:{rid}"),
         InlineKeyboardButton("📈 修改最大金额", callback_data=f"v4:r:max_input:{rid}")],
        [InlineKeyboardButton("🔙 返回规则列表", callback_data="v4:adm_rules:0")],
    ])

def kb_logs(chat_id: int, page: int, log_type: str = "all"):
    total = coin_logs_count(chat_id, log_type if log_type != "all" else None)
    max_page = max(0, (total - 1) // LOG_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    def _btn(label, lt):
        marker = "●" if log_type == lt else ""
        return InlineKeyboardButton(f"{marker}{label}", callback_data=f"v4:logs:0:{lt}")
    return InlineKeyboardMarkup([
        [_btn("全部", "all"), _btn("发放", "drop"), _btn("兑换", "redeem")],
        [_btn("管理", "admin"), _btn("转账", "transfer"),
         _btn("🧧红包", "redpack"), _btn("🎲PC28", "pc28")],
        [InlineKeyboardButton("⬅️", callback_data=f"v4:logs:{max(0,page-1)}:{log_type}"),
         InlineKeyboardButton(f"{page+1}/{max_page+1}", callback_data="v4:noop"),
         InlineKeyboardButton("➡️", callback_data=f"v4:logs:{min(max_page,page+1)}:{log_type}")],
        [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")],
    ])

def kb_adm_shop(chat_id: int, page: int):
    total = shop_count(chat_id)
    max_page = max(0, (total - 1) // SHOP_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    items = shop_page(chat_id, SHOP_SIZE, page * SHOP_SIZE)
    rows = []
    for i, (item_id, title, price, stock, enabled, desc) in enumerate(items):
        idx = page * SHOP_SIZE + i + 1  # 群内展示序号（从1开始）
        rows.append([InlineKeyboardButton(
            f"{'✅' if enabled else '❌'} {idx}. {title[:12]}｜-{milli_to_coin(price)}金币｜{'∞' if stock is None else stock}件",
            callback_data=f"v4:adm_item:{item_id}"   # 仍然使用真实ID
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v4:adm_shop:{max(0,page-1)}"),
        InlineKeyboardButton(f"{page+1}/{max_page+1}", callback_data="v4:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v4:adm_shop:{min(max_page,page+1)}")
    ])
    rows.append([InlineKeyboardButton("➕ 新增商品", callback_data="v4:adm_additem_start")])
    rows.append([InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")])
    return InlineKeyboardMarkup(rows)

def kb_item_edit(item_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔁 上架/下架", callback_data=f"v4:i:toggle:{item_id}")],
        [InlineKeyboardButton("✏️ 修改名称", callback_data=f"v4:i:title:{item_id}"),
         InlineKeyboardButton("📋 修改描述", callback_data=f"v4:i:desc:{item_id}")],
        [InlineKeyboardButton("💰 修改价格", callback_data=f"v4:i:price_input:{item_id}"),
         InlineKeyboardButton("📦 修改库存", callback_data=f"v4:i:stock_input:{item_id}")],
        [InlineKeyboardButton("♾️ 库存设为无限", callback_data=f"v4:i:stockinf:{item_id}")],
        [InlineKeyboardButton("🔙 返回商品列表", callback_data="v4:adm_shop:0")],
    ])

def kb_user_panel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🛒 兑换商品", callback_data="v4:u:shop:0"),
         InlineKeyboardButton("📋 金币记录", callback_data="v4:u:logs:0")],
        [InlineKeyboardButton("🔄 刷新余额", callback_data="v4:u:refresh")],
    ])

def kb_user_shop(chat_id: int, page: int):
    total = shop_count_enabled(chat_id)
    max_page = max(0, (total - 1) // SHOP_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    items = shop_page_enabled(chat_id, SHOP_SIZE, page * SHOP_SIZE)  # 仅启用商品
    rows = []
    for i, (item_id, title, price, stock, enabled, desc) in enumerate(items):
        idx = page * SHOP_SIZE + i + 1  # 展示序号（从1开始，连续）
        stock_str = "∞" if stock is None else str(stock)
        rows.append([InlineKeyboardButton(
            f"{idx}. {title[:14]}  -{milli_to_coin(price)}金币  库存:{stock_str}",
            callback_data=f"v4:u:buy:{item_id}"   # 回调仍走真实ID，安全
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v4:u:shop:{max(0,page-1)}"),
        InlineKeyboardButton(f"{page+1}/{max_page+1}", callback_data="v4:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v4:u:shop:{min(max_page,page+1)}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回钱包", callback_data="v4:u:back")])
    return InlineKeyboardMarkup(rows)

def kb_wallets(chat_id: int, page: int):
    total = get_all_wallets_count(chat_id)
    max_page = max(0, (total - 1) // WALLET_PAGE_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️", callback_data=f"v4:wallets:{max(0,page-1)}"),
         InlineKeyboardButton(f"{page+1}/{max_page+1}", callback_data="v4:noop"),
         InlineKeyboardButton("➡️", callback_data=f"v4:wallets:{min(max_page,page+1)}")],
        [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")],
    ])

def kb_chat_settings():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 修改排行榜关键词", callback_data="v4:set_rank_kw")],
        [InlineKeyboardButton("🛒 修改商城关键词", callback_data="v4:set_shop_kw")],
        [InlineKeyboardButton("⏱ 修改排行榜自毁时间", callback_data="v4:set_rank_del")],
        [InlineKeyboardButton("📢 修改兑换通知文本", callback_data="v4:set_redeem_notice")],
        [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")],
    ])

# =========================
# 命令刷新
# =========================
async def post_init(app):
    try:
        await app.bot.set_my_commands(
            commands=[
                BotCommand("start", "打开面板"),
                BotCommand("myid", "查看我的ID"),
                BotCommand("buy", "兑换商品：/buy 商品编号"),
                BotCommand("pay", "转账：回复用户消息后 /pay 金额"),  # 新增
                BotCommand("cancel", "取消当前输入"),
            ],
            scope=BotCommandScopeDefault()
        )
        await app.bot.set_my_commands(
            commands=[
                BotCommand("start", "打开管理面板"),
                BotCommand("myid", "查看我的ID"),
                BotCommand("pay", "转账：回复用户消息后 /pay 金额"),  # 新增
                BotCommand("cancel", "取消当前输入"),
            ],
            scope=BotCommandScopeAllPrivateChats()
        )
    except Exception:
        logger.exception("post_init set_my_commands failed")
# =========================
# 兑换通知
# =========================
async def notify_purchase(context: ContextTypes.DEFAULT_TYPE,
                          chat_id: int, buyer_id: int, item_id: int):
    try:
        it = get_item(chat_id, item_id)
        if not it:
            return
        _id, title, price, stock, enabled, desc = it
        bal = wallet_get(chat_id, buyer_id)
        settings = get_chat_settings(chat_id)
        notice_extra = settings.get("redeem_notice", "").strip()

        # 群内公开通知
        group_text = (
            f"🧾 兑换通知\n"
            f"用户：<a href=\"tg://user?id={buyer_id}\">{buyer_id}</a>\n"
            f"商品：{title}\n"
            f"花费：-{milli_to_coin(price)} 金币\n"
            f"剩余：{milli_to_coin(bal)} 金币"
        )
        if notice_extra:
            group_text += f"\n\n📌 {notice_extra}"
        await context.bot.send_message(
            chat_id=chat_id, text=group_text, parse_mode="HTML"
        )

        # 私信所有群管理员（通过 Telegram API 获取）
        try:
            admins = await context.bot.get_chat_administrators(chat_id)
            admin_text = (
                f"🛒 有新的兑换订单！\n"
                f"群组：{chat_id}\n"
                f"买家：<a href=\"tg://user?id={buyer_id}\">{buyer_id}</a>\n"
                f"商品：{title}\n"
                f"花费：-{milli_to_coin(price)} 金币\n"
                f"买家余额：{milli_to_coin(bal)} 金币\n"
                f"库存剩余：{'∞' if stock is None else max(0, stock)}"
            )
            if desc:
                admin_text += f"\n描述：{desc}"
            if notice_extra:
                admin_text += f"\n\n📌 {notice_extra}"
            for admin_member in admins:
                if admin_member.user.is_bot:
                    continue
                try:
                    await context.bot.send_message(
                        chat_id=admin_member.user.id,
                        text=admin_text,
                        parse_mode="HTML"
                    )
                except Exception:
                    pass
        except Exception:
            logger.exception("notify_purchase get_chat_administrators failed")
    except Exception:
        logger.exception("notify_purchase failed")

# =========================
# auto_delete / safe_edit
# =========================
# 修改后
def auto_delete_pair(context, chat_id, trigger_mid, bot_mid, delay=120):
    if delay <= 0:
        return
    async def _del():
        await asyncio.sleep(delay)
        for mid in set([trigger_mid, bot_mid]):  # 用 set 去重，避免同一条消息删两次
            try:
                await context.bot.delete_message(chat_id=chat_id, message_id=mid)
            except Exception:
                pass
    asyncio.create_task(_del())

async def safe_edit(q, text, reply_markup=None, parse_mode=None):
    try:
        await q.edit_message_text(
            text=text, reply_markup=reply_markup, parse_mode=parse_mode
        )
    except BadRequest as e:
        if "Message is not modified" in str(e):
            return
        try:
            await q.message.reply_text(
                text=text, reply_markup=reply_markup, parse_mode=parse_mode
            )
        except Exception:
            logger.exception("safe_edit fallback failed")
    except Exception:
        logger.exception("safe_edit failed")
        try:
            await q.message.reply_text(
                text=text, reply_markup=reply_markup, parse_mode=parse_mode
            )
        except Exception:
            pass

async def on_error(update, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled exception", exc_info=context.error)

# =========================
# ✅ 核心权限守卫（续）
# =========================
def guard_group(update: Update) -> bool:  # ✅ 改为同步函数，内部无 await 不需要 async
    """群组消息：必须在白名单内，否则静默忽略"""
    chat = update.effective_chat
    if not chat:
        return False
    if chat.type in ("group", "supergroup"):
        return is_allowed_chat(chat.id)
    return True  # 私聊始终放行

async def guard_admin(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
) -> tuple[bool, int]:
    """
    私聊管理操作守卫：
    1. 必须是私聊
    2. 必须已选择群组（sel_chat_id）
    3. 所选群组必须在白名单内
    4. 用户必须是该群的 Telegram 管理员（实时验证）
    返回 (True, chat_id) 或 (False, 0)
    """
    if not update.effective_user:
        return False, 0
    if not update.effective_chat or update.effective_chat.type != "private":
        return False, 0

    uid = update.effective_user.id
    cid = selected_chat_id(context)

    if cid == 0:
        return False, 0
    if not is_allowed_chat(cid):
        # 所选群已不在白名单，清除缓存
        context.user_data.pop("sel_chat_id", None)
        context.user_data.pop("sel_chat_title", None)
        return False, 0
    if not await has_manage_admins_permission(context, cid, uid):
        # 权限已失效，清除缓存
        context.user_data.pop("sel_chat_id", None)
        context.user_data.pop("sel_chat_title", None)
        return False, 0

    return True, cid

# =========================
# Commands
# =========================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """私聊 /start：管理面板入口"""
    if not update.effective_user or not update.message:
        return
    if update.effective_chat.type != "private":
        return

    uid = update.effective_user.id
    clear_pending_state(context)

    admin_chats = await get_admin_chat_ids(context, uid)

    if not admin_chats:
        await update.message.reply_text(
            "👋 欢迎使用金币机器人！\n\n"
            "⚠️ 你目前不具备任何已授权群组的“添加管理员”权限。\n"
            "请确认：\n"
            "• 机器人已被拉入群组\n"
            "• 你在该群组担任管理员\n"
            "• 该群组已由部署者加入白名单",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🆔 查看我的ID", callback_data="v4:show_myid")]
            ])
        )
        return

    cid = selected_chat_id(context)
    if cid and is_allowed_chat(cid) and await has_manage_admins_permission(context, cid, uid):
        await update.message.reply_text(
            console_text(context),
            reply_markup=kb_console()
        )
    else:
        # ✅ 只保留一个 else，同时写入时间戳
        context.user_data["admin_chats_cache"] = admin_chats
        context.user_data["admin_chats_cache_ts"] = time.time()
        await update.message.reply_text(
            "👋 欢迎回来！请选择要管理的群组：",
            reply_markup=kb_groups(0, admin_chats)
        )

async def cmd_myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message:
        return
    uid = update.effective_user.id
    chat_id = update.effective_chat.id
    await update.message.reply_text(
        f"🆔 你的用户ID：<code>{uid}</code>\n"
        f"💬 当前会话ID：<code>{chat_id}</code>",
        parse_mode="HTML"
    )

async def cmd_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    clear_pending_state(context)
    await update.message.reply_text("✅ 已取消当前操作")

async def cmd_buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message or not update.effective_chat:
        return
    # ✅ 先判断是否私聊，给出提示
    if update.effective_chat.type == "private":
        await update.message.reply_text("请在群组内使用 /buy 命令")
        return
    # ✅ 再做群组白名单检查（此时已确认是群组，直接用 is_allowed_chat）
    if not is_allowed_chat(update.effective_chat.id):
        return

    chat_id = update.effective_chat.id
    uid = update.effective_user.id
    args = context.args
    if not args:
        await update.message.reply_text("用法：/buy 商品编号\n例如：/buy 1")
        return

    display_idx = safe_int(args[0], 0)
    if display_idx <= 0:
        await update.message.reply_text("❌ 无效的商品编号")
        return

    # 按“展示序号”映射到真实 item_id（仅上架商品）
    it = get_item_by_display_index(chat_id, display_idx)
    if not it:
        await update.message.reply_text("❌ 商品不存在或已下架")
        return

    real_item_id = int(it[0])
    ok, msg = buy_item_atomic(chat_id, uid, real_item_id)
    await update.message.reply_text(msg)
    if ok:
        await notify_purchase(context, chat_id, uid, real_item_id)

async def cmd_pay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    仅支持：回复某人消息 /pay 10
    规则：
    - 仅群组可用
    - 仅白名单群可用
    - 不能给自己转
    - 金额必须 > 0
    - 转账不跨群（按当前 chat_id 钱包记账）
    """
    if not update.effective_user or not update.message or not update.effective_chat:
        return

    chat = update.effective_chat
    # 私聊不支持
    if chat.type == "private":
        await update.message.reply_text("请在群组内回复对方消息使用：/pay 金额")
        return

    chat_id = chat.id
    if not is_allowed_chat(chat_id):
        return

    # 必须回复某人的消息
    reply = update.message.reply_to_message
    if not reply or not reply.from_user:
        await update.message.reply_text("❌ 请先回复对方消息，再发送：/pay 金额\n例如：/pay 10")
        return

    from_uid = update.effective_user.id
    to_uid = reply.from_user.id

    # 禁止给机器人转（可选但建议）
    if reply.from_user.is_bot:
        await update.message.reply_text("❌ 不能给机器人转账")
        return

    if from_uid == to_uid:
        await update.message.reply_text("❌ 不能给自己转账")
        return

    # 解析金额：只取第一个参数
    args = context.args or []
    if len(args) < 1:
        await update.message.reply_text("❌ 请输入金额\n例如：/pay 10")
        return

    try:
        amount_milli = coin_to_milli(args[0])
        if amount_milli <= 0:
            raise ValueError
    except Exception:
        await update.message.reply_text("❌ 金额格式错误，请输入正数（如 10 或 0.5）")
        return

    ok, result = wallet_transfer_atomic(chat_id, from_uid, to_uid, amount_milli)
    if not ok:
        await update.message.reply_text(f"❌ {result}")
        return

    new_from, new_to = result
    await update.message.reply_text(
        f"💸 转账成功\n"
        f"转给 <a href=\"tg://user?id={to_uid}\">{to_uid}</a>：{milli_to_coin(amount_milli)} 金币\n"
        f"你的余额：{milli_to_coin(new_from)} 金币\n"
        f"对方余额：{milli_to_coin(new_to)} 金币",
        parse_mode="HTML"
    )

# =========================
# Callback Handler（管理面板）
# =========================
async def cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not update.effective_user:
        return
    uid = update.effective_user.id
    data = q.data or ""

    # 管理回调只允许私聊
    if update.effective_chat and update.effective_chat.type != "private":
        await q.answer("❌ 管理操作仅限私聊使用", show_alert=True)
        return

    if data == "v4:noop":
        await q.answer()
        return

    # ── 取消输入 ──
    if data.startswith("v4:cancel_input:"):
        await q.answer()
        clear_pending_state(context)
        back = data[len("v4:cancel_input:"):]
        ok, chat_id = await guard_admin(update, context)
        if not ok:
            await safe_edit(q,
                "⚠️ 管理权限已失效，请重新选择群组",
                reply_markup=kb_home())
            return
        if back == "v4:admin_home":
            await safe_edit(q, console_text(context), reply_markup=kb_console())
        elif back.startswith("v4:adm_rules"):
            await safe_edit(q, "⚙️ 抽奖规则管理\n点击规则可编辑",
                reply_markup=kb_adm_rules(chat_id, 0))
        elif back.startswith("v4:adm_shop"):
            await safe_edit(q, "🎁 商品管理", reply_markup=kb_adm_shop(chat_id, 0))
        elif back == "v4:chat_settings":
            await safe_edit(q, settings_text(chat_id), reply_markup=kb_chat_settings())
        elif back == "v4:adm_user":
            t = context.user_data.get("adm_target", uid)
            await safe_edit(q, adm_user_text(chat_id, t), reply_markup=kb_adm_user())
        else:
            await safe_edit(q, console_text(context), reply_markup=kb_console())
        return

    # ── 首页 ──
    if data == "v4:home":
        await q.answer()
        clear_pending_state(context)
        await safe_edit(q, "🏠 首页", reply_markup=kb_home())
        return

    if data == "v4:show_myid":
        await q.answer(f"你的ID：{uid}", show_alert=True)
        return

    # ── 群组列表（支持刷新）──
    # cb() 函数内，── 群组列表 ── 分支
    if data.startswith("v4:groups:"):
        await q.answer()
        parts = data.split(":")
        page = safe_int(parts[2], 0)
        refresh = len(parts) > 3 and parts[3] == "refresh"

        # ✅ 新增：超过 5 分钟自动过期强制刷新
        cache_ts = context.user_data.get("admin_chats_cache_ts", 0)
        cache_expired = (time.time() - cache_ts) > 300

        if refresh or "admin_chats_cache" not in context.user_data or cache_expired:
            admin_chats = await get_admin_chat_ids(context, uid)
            context.user_data["admin_chats_cache"] = admin_chats
            context.user_data["admin_chats_cache_ts"] = time.time()  # ✅ 更新时间戳
        else:
            admin_chats = context.user_data["admin_chats_cache"]

        if not admin_chats:
            await safe_edit(q,
                "⚠️ 你目前不是任何已授权群组的管理员\n\n"
                "请确认机器人已在群内且你担任管理员",
                reply_markup=kb_home())
            return
        await safe_edit(q, "🗂️ 请选择要管理的群组：",
            reply_markup=kb_groups(page, admin_chats))
        return


    # ── 选择群组 ──
    # cb() 函数内，── 选择群组 ── 分支
    if data.startswith("v4:selgroup:"):
        cid = safe_int(data.split(":")[2], 0)

        if not is_allowed_chat(cid):
            await q.answer("❌ 该群组未在白名单内", show_alert=True)
            return
        if not await has_manage_admins_permission(context, cid, uid):
            await q.answer("❌ 你没有该群的“添加管理员”权限", show_alert=True)
            return

        # ✅ 以下全部在 if 块内，正确缩进
        await q.answer()
        context.user_data["sel_chat_id"] = cid
        try:
            chat = await context.bot.get_chat(cid)
            context.user_data["sel_chat_title"] = chat.title or str(cid)
        except Exception:
            context.user_data["sel_chat_title"] = str(cid)

        clear_pending_state(context)
        upsert_chat(cid, context.user_data["sel_chat_title"])
        await safe_edit(q, console_text(context), reply_markup=kb_console())
        return

    # ── 控制台 ──  ✅ 现在可以正常到达
    if data == "v4:admin_home":
        await q.answer()
        ok, _ = await guard_admin(update, context)
        if not ok:
            await safe_edit(q,
                "⚠️ 管理权限已失效，请重新选择群组",
                reply_markup=kb_home())
            return
        await safe_edit(q, console_text(context), reply_markup=kb_console())
        return

    # ── 以下所有操作统一经过 guard_admin ──
    ok, chat_id = await guard_admin(update, context)
    if not ok:
        await q.answer("⚠️ 权限验证失败，请重新 /start 选择群组", show_alert=True)
        return

    # ── 金币管理 ──
    if data == "v4:adm_user":
        await q.answer()
        context.user_data.setdefault("adm_target", uid)
        t = context.user_data["adm_target"]
        await safe_edit(q, adm_user_text(chat_id, t), reply_markup=kb_adm_user())
        return

    if data == "v4:adm_target":
        await q.answer()
        context.user_data["await_target_input"] = True
        await safe_edit(q,
            "🎯 请输入目标用户的 Telegram ID（纯数字）\n\n"
            "💡 提示：用户可发送 /myid 获取自己的ID",
            reply_markup=InlineKeyboardMarkup([kb_cancel("v4:adm_user")]))
        return

    if data == "v4:adm_add_input":
        await q.answer()
        context.user_data["await_adm_add"] = True
        t = context.user_data.get("adm_target", uid)
        bal = wallet_get(chat_id, t)
        await safe_edit(q,
            f"➕ 增加金币\n目标用户：{t}\n当前余额：{milli_to_coin(bal)} 金币\n"
            f"请输入要增加的金币数量（支持小数，如：10 或 0.5）",
            reply_markup=InlineKeyboardMarkup([kb_cancel("v4:adm_user")]))
        return

    if data == "v4:adm_sub_input":
        await q.answer()
        context.user_data["await_adm_sub"] = True
        t = context.user_data.get("adm_target", uid)
        bal = wallet_get(chat_id, t)
        await safe_edit(q,
            f"➖ 扣除金币\n目标用户：{t}\n当前余额：{milli_to_coin(bal)} 金币\n"
            f"请输入要扣除的金币数量（支持小数，如：10 或 0.5）",
            reply_markup=InlineKeyboardMarkup([kb_cancel("v4:adm_user")]))
        return

    if data == "v4:adm_qbal":
        t = context.user_data.get("adm_target", uid)
        bal = wallet_get(chat_id, t)
        await q.answer(f"用户 {t} 余额：{milli_to_coin(bal)} 金币", show_alert=True)
        return

    # ── 规则管理 ──
    if data.startswith("v4:adm_rules:"):
        await q.answer()
        page = safe_int(data.split(":")[2], 0)
        ensure_default_rules(chat_id)
        await safe_edit(q,
            "⚙️ 抽奖规则管理\n点击规则可编辑，✅=开启 ❌=关闭",
            reply_markup=kb_adm_rules(chat_id, page))
        return

    if data.startswith("v4:adm_rule:"):
        await q.answer()
        rid = safe_int(data.split(":")[2], 0)
        r = get_rule(chat_id, rid)
        if not r:
            await q.answer("规则不存在", show_alert=True)
            return
        await safe_edit(q, rule_detail_text(r), reply_markup=kb_rule_edit(rid))
        return

    if data.startswith("v4:r:toggle:"):
        await q.answer()
        rid = safe_int(data.split(":")[3], 0)
        r = get_rule(chat_id, rid)
        if not r:
            await q.answer("规则不存在", show_alert=True)
            return
        _id, name, p, mn, mx, en, pr = r
        update_rule(chat_id, rid, float(p), int(mn), int(mx), not bool(en), name)
        nr = get_rule(chat_id, rid)
        await safe_edit(q,
            f"✅ 规则已{'开启' if not en else '关闭'}\n\n" + rule_detail_text(nr),
            reply_markup=kb_rule_edit(rid))
        return

    if data.startswith("v4:r:name:"):
        await q.answer()
        rid = safe_int(data.split(":")[3], 0)
        context.user_data["await_rule_name"] = rid
        await safe_edit(q,
            f"✏️ 修改规则名称\n规则ID：{rid}\n\n请输入新名称：",
            reply_markup=InlineKeyboardMarkup([kb_cancel(f"v4:adm_rule:{rid}")]))
        return

    if data.startswith("v4:r:prob_input:"):
        await q.answer()
        rid = safe_int(data.split(":")[3], 0)
        r = get_rule(chat_id, rid)
        await safe_edit(q,
            f"📊 修改触发概率\n规则：{r[1] if r else rid}\n"
            f"当前概率：{float(r[2])*100:.4f}%\n\n"
            f"请输入新概率（如：5 或 5% → 5%，0.5 → 0.5%）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel(f"v4:adm_rule:{rid}")]))
        context.user_data["await_rule_prob"] = rid
        return

    if data.startswith("v4:r:min_input:"):
        await q.answer()
        rid = safe_int(data.split(":")[3], 0)
        r = get_rule(chat_id, rid)
        await safe_edit(q,
            f"📉 修改最小金额\n规则：{r[1] if r else rid}\n"
            f"当前最小：{milli_to_coin(int(r[3])) if r else '?'} 金币\n\n"
            f"请输入新的最小金额（如：0.5 或 1）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel(f"v4:adm_rule:{rid}")]))
        context.user_data["await_rule_min"] = rid
        return

    if data.startswith("v4:r:max_input:"):
        await q.answer()
        rid = safe_int(data.split(":")[3], 0)
        r = get_rule(chat_id, rid)
        await safe_edit(q,
            f"📈 修改最大金额\n规则：{r[1] if r else rid}\n"
            f"当前最大：{milli_to_coin(int(r[4])) if r else '?'} 金币\n\n"
            f"请输入新的最大金额（如：5 或 10）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel(f"v4:adm_rule:{rid}")]))
        context.user_data["await_rule_max"] = rid
        return

    # ── 日志 ──
    if data.startswith("v4:logs:"):
        await q.answer()
        parts = data.split(":")
        page = safe_int(parts[2], 0)
        log_type = parts[3] if len(parts) > 3 else "all"
        lt_filter = log_type if log_type != "all" else None
        logs = coin_logs_page(chat_id, LOG_SIZE, page * LOG_SIZE, lt_filter)
        type_icons = {"drop": "🎁", "redeem": "🛒", "admin": "🔧", "transfer": "💸", "redpack": "🧧", "pc28": "🎲"}
        lines = [f"🧾 操作日志（{log_type}）\n"]
        for row in logs:
            lid, op, tgt, delta, reason, ts, lt = row
            icon = type_icons.get(lt, "📝")
            sign = "+" if delta >= 0 else ""
            lines.append(
                f"{icon} {str(ts)[:16]} | 用户:{tgt} | {sign}{milli_to_coin(delta)}"
            )
        txt = "\n".join(lines) if logs else "暂无日志"
        await safe_edit(q, txt, reply_markup=kb_logs(chat_id, page, log_type))
        return

    # ── 统计 ──
    if data == "v4:stats":
        await q.answer()
        await safe_edit(q, stats_text(chat_id),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 刷新", callback_data="v4:stats")],
                [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")]
            ]))
        return

    # ── 排行榜（管理端）──
    if data.startswith("v4:rank:"):
        await q.answer()
        page = safe_int(data.split(":")[2], 0)
        txt, max_page = rank_text(chat_id, page)
        page = clamp(page, 0, max_page)
        await safe_edit(q, txt,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️", callback_data=f"v4:rank:{max(0,page-1)}"),
                 InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
                 InlineKeyboardButton("➡️", callback_data=f"v4:rank:{min(max_page,page+1)}")],
                [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")]
            ]), parse_mode="HTML")
        return

    # ── 用户余额列表 ──
    if data.startswith("v4:wallets:"):
        await q.answer()
        page = safe_int(data.split(":")[2], 0)
        rows = get_all_wallets(chat_id, WALLET_PAGE_SIZE, page * WALLET_PAGE_SIZE)
        lines = [f"💼 用户余额列表\n"]
        for i, (wuid, bal, upd, dname) in enumerate(rows):
            rank = page * WALLET_PAGE_SIZE + i + 1
            name = dname if dname else str(wuid)
            lines.append(
                f"#{rank} <a href=\"tg://user?id={wuid}\">{name}</a>"
                f" — {milli_to_coin(bal)} 金币"
            )
        txt = "\n".join(lines) if rows else "暂无数据"
        await safe_edit(q, txt,
            reply_markup=kb_wallets(chat_id, page), parse_mode="HTML")
        return

    # ── 商品管理 ──
    if data.startswith("v4:adm_shop:"):
        await q.answer()
        page = safe_int(data.split(":")[2], 0)
        await safe_edit(q,
            "🎁 商品管理\n✅=上架 ❌=下架\n格式：序号. 名称｜-价格金币｜库存件",
            reply_markup=kb_adm_shop(chat_id, page))
        return

    if data.startswith("v4:adm_item:"):
        await q.answer()
        item_id = safe_int(data.split(":")[2], 0)
        it = get_item(chat_id, item_id)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        idx = get_display_index_by_item_id(chat_id, item_id, enabled_only=False)
        await safe_edit(q, item_detail_text(it, idx), reply_markup=kb_item_edit(item_id))
        return


    if data == "v4:adm_additem_start":
        await q.answer()
        context.user_data["await_add_item_input"] = True
        await safe_edit(q,
            "➕ 新增商品\n━━━━━━━━━━━━━━━━━━\n"
            "请按以下格式发送：\n\n"
            "  标题|价格|库存|描述\n\n"
            "📖 示例：\n"
            "  会员资格|10|100|有效期30天\n"
            "  无限库存商品|5|∞\n\n"
            "• 库存填 ∞ 或 0 表示无限\n• 描述可省略",
            reply_markup=InlineKeyboardMarkup([kb_cancel("v4:adm_shop:0")]))
        return

    if data.startswith("v4:i:toggle:"):
        await q.answer()
        item_id = safe_int(data.split(":")[3], 0)
        it = get_item(chat_id, item_id)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled, desc = it
        update_item(chat_id, item_id, title, price, stock, not enabled, desc)
        nit = get_item(chat_id, item_id)
        await safe_edit(q,
            f"✅ 商品已{'上架' if not enabled else '下架'}\n\n" + item_detail_text(nit),
            reply_markup=kb_item_edit(item_id))
        return

    if data.startswith("v4:i:title:"):
        await q.answer()
        item_id = safe_int(data.split(":")[3], 0)
        context.user_data["await_item_title"] = item_id
        await safe_edit(q,
            f"✏️ 修改商品名称\n商品ID：{item_id}\n\n请输入新名称：",
            reply_markup=InlineKeyboardMarkup([kb_cancel(f"v4:adm_item:{item_id}")]))
        return

    if data.startswith("v4:i:desc:"):
        await q.answer()
        item_id = safe_int(data.split(":")[3], 0)
        context.user_data["await_item_desc"] = item_id
        await safe_edit(q,
            f"📋 修改商品描述\n商品ID：{item_id}\n\n"
            f"请输入新描述（发送「清空」可删除描述）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel(f"v4:adm_item:{item_id}")]))
        return

    if data.startswith("v4:i:price_input:"):
        await q.answer()
        item_id = safe_int(data.split(":")[3], 0)
        it = get_item(chat_id, item_id)
        await safe_edit(q,
            f"💰 修改商品价格\n商品：{it[1] if it else item_id}\n"
            f"当前价格：{milli_to_coin(it[2]) if it else '?'} 金币\n\n"
            f"请输入新价格（如：5 或 0.5）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel(f"v4:adm_item:{item_id}")]))
        context.user_data["await_item_price"] = item_id
        return

    if data.startswith("v4:i:stock_input:"):
        await q.answer()
        item_id = safe_int(data.split(":")[3], 0)
        it = get_item(chat_id, item_id)
        await safe_edit(q,
            f"📦 修改商品库存\n商品：{it[1] if it else item_id}\n"
            f"当前库存：{'∞' if it and it[3] is None else (it[3] if it else '?')}\n\n"
            f"请输入新库存数量（整数）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel(f"v4:adm_item:{item_id}")]))
        context.user_data["await_item_stock"] = item_id
        return

    if data.startswith("v4:i:stockinf:"):
        await q.answer()
        item_id = safe_int(data.split(":")[3], 0)
        it = get_item(chat_id, item_id)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled, desc = it
        update_item(chat_id, item_id, title, price, None, enabled, desc)
        nit = get_item(chat_id, item_id)
        await safe_edit(q,
            f"✅ 库存已设为无限\n\n" + item_detail_text(nit),
            reply_markup=kb_item_edit(item_id))
        return

    # ── 群组设置 ──
    if data == "v4:chat_settings":
        await q.answer()
        await safe_edit(q, settings_text(chat_id), reply_markup=kb_chat_settings())
        return

    if data == "v4:set_rank_kw":
        await q.answer()
        context.user_data["await_rank_keywords"] = True
        s = get_chat_settings(chat_id)
        await safe_edit(q,
            f"🔑 修改排行榜关键词\n当前：{s.get('rank_keywords','排行榜,排名,积分榜')}\n\n"
            f"请输入新关键词（多个用逗号分隔）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel("v4:chat_settings")]))
        return

    if data == "v4:set_shop_kw":
        await q.answer()
        context.user_data["await_shop_keywords"] = True
        s = get_chat_settings(chat_id)
        await safe_edit(q,
            f"🛒 修改商城关键词\n当前：{s.get('shop_keywords','商城,兑换,商店')}\n\n"
            f"请输入新关键词（多个用逗号分隔）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel("v4:chat_settings")]))
        return

    if data == "v4:set_rank_del":
        await q.answer()
        context.user_data["await_rank_delete_seconds"] = True
        s = get_chat_settings(chat_id)
        await safe_edit(q,
            f"⏱ 修改排行榜自毁时间\n当前：{s.get('rank_delete_seconds', 120)} 秒\n\n"
            f"请输入新的自毁时间（秒，整数，0=不删除）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel("v4:chat_settings")]))
        return

    if data == "v4:set_redeem_notice":
        await q.answer()
        context.user_data["await_redeem_notice"] = True
        s = get_chat_settings(chat_id)
        cur_notice = s.get("redeem_notice", "") or "（未设置）"
        await safe_edit(q,
            f"📢 修改兑换通知文本\n当前：{cur_notice}\n\n"
            f"请输入新的通知文本（发送「清空」可删除）：",
            reply_markup=InlineKeyboardMarkup([kb_cancel("v4:chat_settings")]))
        return

# =========================
# 群内公开排行榜回调
# =========================
async def cb_pub_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.message or not update.effective_user:
        return

    chat_id = q.message.chat_id
    # 白名单检查
    if not is_allowed_chat(chat_id):
        await q.answer()
        return

    uid = update.effective_user.id
    msg_id = q.message.message_id
    owner = context.bot_data.get(f"rank_owner_{chat_id}_{msg_id}")
    if owner is not None and uid != owner:
        await q.answer("❌ 只有发送排行榜的用户才能翻页", show_alert=True)
        return

    await q.answer()
    data = q.data or ""
    page = max(0, safe_int(data.split(":")[2], 0))
    txt, max_page = rank_text(chat_id, page)
    page = clamp(page, 0, max_page)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️", callback_data=f"v4:pub_rank:{max(0,page-1)}"),
         InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
         InlineKeyboardButton("➡️", callback_data=f"v4:pub_rank:{min(max_page,page+1)}")]
    ])
    try:
        await q.edit_message_text(text=txt, reply_markup=kb, parse_mode="HTML")
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.exception("cb_pub_rank edit failed")

# =========================
# 用户面板回调
# =========================
# cb_user() 函数开头
async def cb_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not update.effective_user or not q.message:
        return

    chat_id = q.message.chat_id
    chat_type = q.message.chat.type if q.message.chat else "private"

    # ✅ 只有群组消息才做白名单检查，私聊场景不限制
    if chat_type in ("group", "supergroup") and not is_allowed_chat(chat_id):
        await q.answer()
        return

    uid = update.effective_user.id
    msg_id = q.message.message_id
    data = q.data or ""

    # 群内消息只有唤起人才能操作
    if chat_type in ("group", "supergroup"):
        owner = context.bot_data.get(f"shop_owner_{chat_id}_{msg_id}")
        if owner is not None and uid != owner:
            await q.answer("❌ 只有发送该消息的用户才能操作", show_alert=True)
            return

    if data == "v4:u:back":
        await q.answer()
        await safe_edit(q, user_panel_text(chat_id, uid),
            reply_markup=kb_user_panel(), parse_mode="HTML")
        return

    if data == "v4:u:refresh":
        await q.answer("✅ 已刷新")
        await safe_edit(q, user_panel_text(chat_id, uid),
            reply_markup=kb_user_panel(), parse_mode="HTML")
        return

    if data.startswith("v4:u:shop:"):
        await q.answer()
        page = safe_int(data.split(":")[3], 0)
        total = shop_count_enabled(chat_id)
        if total == 0:
            await q.answer("🛒 暂无可兑换商品", show_alert=True)
            return
        bal = wallet_get(chat_id, uid)
        await safe_edit(q,
            f"🛒 兑换商品\n💰 我的余额：<b>{milli_to_coin(bal)}</b> 金币\n点击商品按钮即可兑换",
            reply_markup=kb_user_shop(chat_id, page),
            parse_mode="HTML")
        return

    if data.startswith("v4:u:buy:"):
        await q.answer()
        item_id = safe_int(data.split(":")[3], 0)
        it = get_item(chat_id, item_id)
        if not it:
            await q.answer("❌ 商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled, desc = it
        bal = wallet_get(chat_id, uid)
        confirm_text = (
            f"🛒 确认兑换\n商品：{title}\n价格：-{milli_to_coin(price)} 金币\n"
            f"库存：{'∞' if stock is None else stock}\n你的余额：{milli_to_coin(bal)} 金币\n"
        )
        if desc:
            confirm_text += f"描述：{desc}\n"
        confirm_text += "\n确认兑换吗？"
        await safe_edit(q, confirm_text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ 确认兑换", callback_data=f"v4:u:confirm:{item_id}"),
                 InlineKeyboardButton("❌ 取消", callback_data="v4:u:back")]
            ]))
        return

    if data.startswith("v4:u:confirm:"):
        await q.answer()
        item_id = safe_int(data.split(":")[3], 0)
        ok, msg = buy_item_atomic(chat_id, uid, item_id)
        if ok:
            await notify_purchase(context, chat_id, uid, item_id)
            bal = wallet_get(chat_id, uid)
            await safe_edit(q,
                f"✅ 兑换成功！\n{msg}\n剩余余额：{milli_to_coin(bal)} 金币",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🛒 继续兑换", callback_data="v4:u:shop:0"),
                     InlineKeyboardButton("🔙 返回钱包", callback_data="v4:u:back")]
                ]))
        else:
            await safe_edit(q,
                f"❌ 兑换失败：{msg}",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔙 返回", callback_data="v4:u:back")]
                ]))
        return

    if data.startswith("v4:u:logs:"):
        await q.answer()
        page = safe_int(data.split(":")[3], 0)
        total = user_coin_logs_count(chat_id, uid)
        max_page = max(0, (total - 1) // LOG_SIZE) if total > 0 else 0
        page = clamp(page, 0, max_page)
        logs = user_coin_logs_page(chat_id, uid, LOG_SIZE, page * LOG_SIZE)
        type_icons = {"drop": "🎁", "redeem": "🛒", "admin": "🔧", "transfer": "💸", "redpack": "🧧", "pc28": "🎲"}
        lines = [f"📋 我的金币记录\n"]
        for row in logs:
            lid, op, tgt, delta, reason, ts, lt = row
            icon = type_icons.get(lt, "📝")
            sign = "+" if delta >= 0 else ""
            lines.append(f"{icon} {str(ts)[:16]} {sign}{milli_to_coin(delta)}")
        txt = "\n".join(lines) if logs else "暂无记录"
        await safe_edit(q, txt,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️", callback_data=f"v4:u:logs:{max(0,page-1)}"),
                 InlineKeyboardButton(f"{page+1}/{max_page+1}", callback_data="v4:noop"),
                 InlineKeyboardButton("➡️", callback_data=f"v4:u:logs:{min(max_page,page+1)}")],
                [InlineKeyboardButton("🔙 返回钱包", callback_data="v4:u:back")]
            ]))
        return

# =========================
# on_text（私聊输入 + 群组发言）
# =========================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user or not update.effective_chat:
        return

    user_id = update.effective_user.id
    chat_type = update.effective_chat.type
    text = (update.message.text or "").strip()

    # ── 私聊：管理员输入流程 ──
    if chat_type == "private":
        ok, chat_id = await guard_admin(update, context)
        v = chat_id if ok else 0

        if context.user_data.get("await_target_input"):
            target = safe_int(text, 0)
            if not target:
                await update.message.reply_text("❌ 格式错误，请输入纯数字用户ID")
                return
            context.user_data.pop("await_target_input", None)
            context.user_data["adm_target"] = target
            if ok:
                await update.message.reply_text(
                    f"✅ 目标用户已设置为：{target}\n\n" + adm_user_text(v, target),
                    reply_markup=kb_adm_user())
            else:
                await update.message.reply_text(f"✅ 目标用户已设置为：{target}")
            return

        if not ok:
            return

        if context.user_data.get("await_adm_add"):
            try:
                delta = coin_to_milli(text)
                if delta <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("❌ 格式错误，请输入正数\n如：10 或 0.5")
                return
            t = context.user_data.get("adm_target", user_id)
            ok2, msg = wallet_adjust_admin(v, user_id, t, delta, "panel_add")
            context.user_data.pop("await_adm_add", None)
            await update.message.reply_text(
                f"{'✅' if ok2 else '❌'} {msg}\n\n" + adm_user_text(v, t),
                reply_markup=kb_adm_user())
            return

        if context.user_data.get("await_adm_sub"):
            try:
                delta = coin_to_milli(text)
                if delta <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("❌ 格式错误，请输入正数\n如：10 或 0.5")
                return
            t = context.user_data.get("adm_target", user_id)
            ok2, msg = wallet_adjust_admin(v, user_id, t, -delta, "panel_sub")
            context.user_data.pop("await_adm_sub", None)
            await update.message.reply_text(
                f"{'✅' if ok2 else '❌'} {msg}\n\n" + adm_user_text(v, t),
                reply_markup=kb_adm_user())
            return

        if context.user_data.get("await_rule_name"):
            rid = context.user_data.pop("await_rule_name")
            r = get_rule(v, rid)
            if not r:
                await update.message.reply_text("❌ 规则不存在")
                return
            _id, name, p, mn, mx, en, pr = r
            update_rule(v, rid, float(p), int(mn), int(mx), bool(en), text)
            nr = get_rule(v, rid)
            await update.message.reply_text(
                f"✅ 规则名称已更新为：{text}\n\n" + rule_detail_text(nr),
                reply_markup=kb_rule_edit(rid))
            return

        if context.user_data.get("await_rule_prob"):
            rid = context.user_data.pop("await_rule_prob")
            r = get_rule(v, rid)
            if not r:
                await update.message.reply_text("❌ 规则不存在")
                return
            raw = text.strip().rstrip("%")
            try:
                pf = float(raw)
                if pf < 0 or pf > 100:
                    raise ValueError
                prob_val = pf / 100.0
            except Exception:
                await update.message.reply_text("❌ 格式错误，请输入 0~100 之间的数字")
                return
            _id, name, p, mn, mx, en, pr = r
            update_rule(v, rid, prob_val, int(mn), int(mx), bool(en), name)
            nr = get_rule(v, rid)
            await update.message.reply_text(
                f"✅ 概率已更新为：{prob_val*100:.4f}%\n\n" + rule_detail_text(nr),
                reply_markup=kb_rule_edit(rid))
            return

        if context.user_data.get("await_rule_min"):
            rid = context.user_data.pop("await_rule_min")
            r = get_rule(v, rid)
            if not r:
                await update.message.reply_text("❌ 规则不存在")
                return
            try:
                new_min = coin_to_milli(text)
                if new_min <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("❌ 格式错误，请输入正数")
                return
            _id, name, p, mn, mx, en, pr = r
            if new_min > int(mx):
                await update.message.reply_text(
                    f"❌ 最小值不能大于最大值（当前最大：{milli_to_coin(int(mx))}）")
                context.user_data["await_rule_min"] = rid
                return
            update_rule(v, rid, float(p), new_min, int(mx), bool(en), name)
            nr = get_rule(v, rid)
            await update.message.reply_text(
                f"✅ 最小金额已更新为：{milli_to_coin(new_min)}\n\n" + rule_detail_text(nr),
                reply_markup=kb_rule_edit(rid))
            return

        if context.user_data.get("await_rule_max"):
            rid = context.user_data.pop("await_rule_max")
            r = get_rule(v, rid)
            if not r:
                await update.message.reply_text("❌ 规则不存在")
                return
            try:
                new_max = coin_to_milli(text)
                if new_max <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("❌ 格式错误，请输入正数")
                return
            _id, name, p, mn, mx, en, pr = r
            if new_max < int(mn):
                await update.message.reply_text(
                    f"❌ 最大值不能小于最小值（当前最小：{milli_to_coin(int(mn))}）")
                context.user_data["await_rule_max"] = rid
                return
            update_rule(v, rid, float(p), int(mn), new_max, bool(en), name)
            nr = get_rule(v, rid)
            await update.message.reply_text(
                f"✅ 最大金额已更新为：{milli_to_coin(new_max)}\n\n" + rule_detail_text(nr),
                reply_markup=kb_rule_edit(rid))
            return

        if context.user_data.get("await_add_item_input"):
            context.user_data.pop("await_add_item_input", None)
            parts = [p.strip() for p in text.split("|")]
            if len(parts) < 2:
                await update.message.reply_text("❌ 格式错误\n正确格式：标题|价格|库存|描述")
                context.user_data["await_add_item_input"] = True
                return
            title = parts[0]
            try:
                price = coin_to_milli(parts[1])
                if price <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("❌ 价格格式错误，请输入正数")
                context.user_data["await_add_item_input"] = True
                return
            stock_raw = parts[2].strip() if len(parts) > 2 else "∞"
            stock = None if stock_raw in ("∞", "0", "") else safe_int(stock_raw, None)
            description = parts[3].strip() if len(parts) > 3 else ""
            add_item(v, title, price, stock, description)
            await update.message.reply_text(
                f"✅ 商品添加成功！\n名称：{title}\n价格：-{milli_to_coin(price)} 金币\n"
                f"库存：{'∞' if stock is None else stock}\n"
                + (f"描述：{description}\n" if description else "")
                + "\n🎁 商品管理",
                reply_markup=kb_adm_shop(v, 0))
            return

        if context.user_data.get("await_item_title"):
            item_id = context.user_data.pop("await_item_title")
            it = get_item(v, item_id)
            if not it:
                await update.message.reply_text("❌ 商品不存在")
                return
            _id, title, price, stock, enabled, desc = it
            update_item(v, item_id, text, price, stock, enabled, desc)
            nit = get_item(v, item_id)
            await update.message.reply_text(
                f"✅ 商品名称已更新为：{text}\n\n" + item_detail_text(nit),
                reply_markup=kb_item_edit(item_id))
            return

        if context.user_data.get("await_item_desc"):
            item_id = context.user_data.pop("await_item_desc")
            it = get_item(v, item_id)
            if not it:
                await update.message.reply_text("❌ 商品不存在")
                return
            _id, title, price, stock, enabled, desc = it
            new_desc = "" if text == "清空" else text
            update_item(v, item_id, title, price, stock, enabled, new_desc)
            nit = get_item(v, item_id)
            await update.message.reply_text(
                f"✅ 商品描述已{'清空' if not new_desc else f'更新为：{new_desc}'}\n\n"
                + item_detail_text(nit),
                reply_markup=kb_item_edit(item_id))
            return

        if context.user_data.get("await_item_price"):
            item_id = context.user_data.pop("await_item_price")
            it = get_item(v, item_id)
            if not it:
                await update.message.reply_text("❌ 商品不存在")
                return
            try:
                new_price = coin_to_milli(text)
                if new_price <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("❌ 格式错误，请输入正数")
                context.user_data["await_item_price"] = item_id
                return
            _id, title, price, stock, enabled, desc = it
            update_item(v, item_id, title, new_price, stock, enabled, desc)
            nit = get_item(v, item_id)
            await update.message.reply_text(
                f"✅ 价格已更新为：-{milli_to_coin(new_price)} 金币\n\n" + item_detail_text(nit),
                reply_markup=kb_item_edit(item_id))
            return

        if context.user_data.get("await_item_stock"):
            item_id = context.user_data.pop("await_item_stock")
            it = get_item(v, item_id)
            if not it:
                await update.message.reply_text("❌ 商品不存在")
                return
            new_stock = safe_int(text, -1)
            if new_stock < 0:
                await update.message.reply_text("❌ 请输入非负整数")
                context.user_data["await_item_stock"] = item_id
                return
            _id, title, price, stock, enabled, desc = it
            update_item(v, item_id, title, price, new_stock, enabled, desc)
            nit = get_item(v, item_id)
            await update.message.reply_text(
                f"✅ 库存已更新为：{new_stock}\n\n" + item_detail_text(nit),
                reply_markup=kb_item_edit(item_id))
            return

        if context.user_data.get("await_rank_keywords"):
            context.user_data.pop("await_rank_keywords", None)
            kw = text.strip()
            if not kw:
                await update.message.reply_text("❌ 关键词不能为空")
                context.user_data["await_rank_keywords"] = True
                return
            set_chat_setting(v, "rank_keywords", kw)
            await update.message.reply_text(
                f"✅ 排行榜关键词已更新\n\n" + settings_text(v),
                reply_markup=kb_chat_settings())
            return

        if context.user_data.get("await_shop_keywords"):
            context.user_data.pop("await_shop_keywords", None)
            kw = text.strip()
            if not kw:
                await update.message.reply_text("❌ 关键词不能为空")
                context.user_data["await_shop_keywords"] = True
                return
            set_chat_setting(v, "shop_keywords", kw)
            await update.message.reply_text(
                f"✅ 商城关键词已更新\n\n" + settings_text(v),
                reply_markup=kb_chat_settings())
            return

        if context.user_data.get("await_rank_delete_seconds"):
            context.user_data.pop("await_rank_delete_seconds", None)
            secs = safe_int(text, -1)
            if secs < 0:
                await update.message.reply_text("❌ 请输入非负整数（秒）")
                context.user_data["await_rank_delete_seconds"] = True
                return
            set_chat_setting(v, "rank_delete_seconds", secs)
            await update.message.reply_text(
                f"✅ 排行榜自毁时间已更新为：{secs} 秒\n\n" + settings_text(v),
                reply_markup=kb_chat_settings())
            return

        if context.user_data.get("await_redeem_notice"):
            context.user_data.pop("await_redeem_notice", None)
            new_notice = "" if text == "清空" else text
            set_chat_setting(v, "redeem_notice", new_notice)
            await update.message.reply_text(
                f"✅ 兑换通知文本已{'清空' if not new_notice else '更新'}\n\n" + settings_text(v),
                reply_markup=kb_chat_settings())
            return

        return  # 私聊无其他处理

    # ── 群组：白名单检查 + 关键词触发 + 金币发放 ──
    if chat_type in ("group", "supergroup"):
        chat_id = update.effective_chat.id

        # 白名单检查
        if not is_allowed_chat(chat_id):
            return

        msg_text = update.message.text or ""
        settings = get_chat_settings(chat_id)

        # 排行榜关键词触发
        kw_list = [k.strip() for k in
                   settings.get("rank_keywords", "排行榜,排名,积分榜").split(",") if k.strip()]
        if msg_text.strip() in kw_list:
            txt, max_page = rank_text(chat_id, 0)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️", callback_data="v4:pub_rank:0"),
                 InlineKeyboardButton(f"第1/{max_page+1}页", callback_data="v4:noop"),
                 InlineKeyboardButton("➡️", callback_data=f"v4:pub_rank:{min(1, max_page)}")]
            ])
            bot_msg = await update.message.reply_text(txt, reply_markup=kb, parse_mode="HTML")
            context.bot_data[f"rank_owner_{chat_id}_{bot_msg.message_id}"] = user_id
            rd = settings.get("rank_delete_seconds", 120)
            auto_delete_pair(context, chat_id,
                update.message.message_id, bot_msg.message_id, rd)
            return

        # 商城关键词触发
        skw_list = [k.strip() for k in
                    settings.get("shop_keywords", "商城,兑换,商店").split(",") if k.strip()]
        if msg_text.strip() in skw_list:
            total = shop_count_enabled(chat_id)
            if total == 0:
                bot_msg = await update.message.reply_text("🛒 暂时没有可兑换的商品")
            else:
                bal = wallet_get(chat_id, user_id)
                bot_msg = await update.message.reply_text(
                    f"🛒 兑换商品\n💰 我的余额：<b>{milli_to_coin(bal)}</b> 金币\n"
                    f"点击商品按钮即可兑换",
                    reply_markup=kb_user_shop(chat_id, 0),
                    parse_mode="HTML"
                )
            context.bot_data[f"shop_owner_{chat_id}_{bot_msg.message_id}"] = user_id
            auto_delete_pair(context, chat_id,
                update.message.message_id, bot_msg.message_id, 60)
            return
        
        # ── 红包关键词触发 ──
        if msg_text.startswith("发红包"):
            await handle_redpack_send(update, context, msg_text)
            return

        # ── PC28 关键词触发 ──
        if msg_text.strip() in ("28开奖", "28停止"):
            await handle_pc28_query(update, context)
            return

        # 押注关键词：押大 / 押小 / 押单 / 押双
        for _bt in ("大", "小", "单", "双"):
            if msg_text.startswith(f"押{_bt}"):
                await handle_pc28_bet(update, context, _bt, msg_text)
                return

        # 金币发放
        if not valid_text_basic(msg_text, MIN_TEXT_LEN):
            return
        u = update.effective_user
        display_name = fmt_display_name(u.first_name, u.last_name, u.username, user_id)
        if not can_reward(chat_id, user_id, msg_text):
            return
        rules = list_rules(chat_id)
        if not rules:
            ensure_default_rules(chat_id)
            rules = list_rules(chat_id)
        won = None
        for r in sorted(rules, key=lambda x: x[6]):
            rid, name, prob, mn, mx, en, pr = r
            if not en:
                continue
            if random.random() < float(prob):
                won = r
                break
        if not won:
            return
        rid, name, prob, mn, mx, en, pr = won
        amount = random.randint(int(mn), int(mx))
        wallet_add(chat_id, user_id, amount, display_name=display_name)
        add_daily(chat_id, user_id, amount)
        bal_after = wallet_get(chat_id, user_id)
        try:
            shop_kw_hint = settings.get("shop_keywords", "商城,兑换,商店").split(",")[0].strip()
            win_msg = (
                f"🧧 恭喜 <a href=\"tg://user?id={user_id}\">{display_name}</a> 中奖！\n"
                f"🎖 等级：{name}\n"
                f"💰 获得：+{milli_to_coin(amount)} 金币\n"
                f"👜 余额：{milli_to_coin(bal_after)} 金币\n"
                f"💡 发送「{shop_kw_hint}」可兑换商品"
            )
            await update.message.reply_text(win_msg, parse_mode="HTML")
        except Exception:
            pass

# =========================
# 群内 /start → 用户钱包面板
# =========================
async def cmd_start_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_user or not update.message or not update.effective_chat:
        return

    # 私聊走管理面板
    if update.effective_chat.type == "private":
        await cmd_start(update, context)
        return

    chat_id = update.effective_chat.id
    # 白名单检查
    if not is_allowed_chat(chat_id):
        return

    uid = update.effective_user.id
    bot_msg = await update.message.reply_text(
        user_panel_text(chat_id, uid),
        reply_markup=kb_user_panel(),
        parse_mode="HTML"
    )
    context.bot_data[f"shop_owner_{chat_id}_{bot_msg.message_id}"] = uid

# =========================
# main
# =========================
def main():
    init_db()
    migrate_db()

    app = (
        ApplicationBuilder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .build()
    )

    app.add_error_handler(on_error)

    app.add_handler(CommandHandler("start", cmd_start_group))
    app.add_handler(CommandHandler("myid", cmd_myid))
    app.add_handler(CommandHandler("cancel", cmd_cancel))
    app.add_handler(CommandHandler("buy", cmd_buy))
    app.add_handler(CommandHandler("pay", cmd_pay))

    app.add_handler(CallbackQueryHandler(cb_redpack, pattern=r"^rp:"))
    app.add_handler(CallbackQueryHandler(cb_pub_rank, pattern=r"^v4:pub_rank:"))
    app.add_handler(CallbackQueryHandler(cb_user,     pattern=r"^v4:u:"))
    app.add_handler(CallbackQueryHandler(cb,          pattern=r"^v4:"))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    if WEBHOOK_URL:
        app.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            url_path=WEBHOOK_PATH,
            webhook_url=f"{WEBHOOK_URL.rstrip('/')}{WEBHOOK_PATH}",
        )
    else:
        app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
