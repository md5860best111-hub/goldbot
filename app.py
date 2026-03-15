import os
import re
import random
import logging
import asyncio
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

ROOT_ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ROOT_ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
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

def is_root_admin(user_id: int) -> bool:
    return user_id in ROOT_ADMIN_IDS

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

_initialized_chats: set = set()

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
# Schema
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
        CREATE TABLE IF NOT EXISTS chat_admins (
          chat_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          role TEXT NOT NULL DEFAULT 'admin',
          created_at TIMESTAMP NOT NULL DEFAULT NOW(),
          PRIMARY KEY (chat_id, user_id)
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS wallets (
          chat_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          balance_milli BIGINT NOT NULL DEFAULT 0,
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
          created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS chat_settings (
          chat_id BIGINT PRIMARY KEY,
          rank_keywords TEXT NOT NULL DEFAULT '排行榜,排名,积分榜',
          redeem_notice TEXT NOT NULL DEFAULT '',
          updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)

        # ⚠️ 先补列，再建依赖这些列的索引
        cur.execute("""
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='coin_logs' AND column_name='log_type'
          ) THEN
            ALTER TABLE coin_logs ADD COLUMN log_type TEXT NOT NULL DEFAULT 'admin';
          END IF;
        END $$;
        """)
        cur.execute("""
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='shop_items' AND column_name='description'
          ) THEN
            ALTER TABLE shop_items ADD COLUMN description TEXT NOT NULL DEFAULT '';
          END IF;
        END $$;
        """)

        # 建索引（列已确保存在）
        cur.execute("CREATE INDEX IF NOT EXISTS idx_chat_admins_user ON chat_admins(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_chat ON drop_rules(chat_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_shop_chat ON shop_items(chat_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_logs_chat ON coin_logs(chat_id, id DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_logs_type ON coin_logs(chat_id, log_type, created_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_redeem_orders_chat ON redeem_orders(chat_id, created_at DESC);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_wallets_chat ON wallets(chat_id, balance_milli DESC);")

# =========================
# Migration
# =========================
@with_conn
def migrate_db(conn):
    with conn.cursor() as cur:
        cur.execute("UPDATE drop_rules SET name='普通红包' WHERE lower(name) IN ('common','normal')")
        cur.execute("UPDATE drop_rules SET name='稀有红包' WHERE lower(name)='rare'")
        cur.execute("UPDATE drop_rules SET name='史诗红包' WHERE lower(name)='epic'")
        cur.execute("""
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='coin_logs' AND column_name='log_type'
          ) THEN
            ALTER TABLE coin_logs ADD COLUMN log_type TEXT NOT NULL DEFAULT 'admin';
          END IF;
        END $$;
        """)
        cur.execute("""
        DO $$ BEGIN
          IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_name='shop_items' AND column_name='description'
          ) THEN
            ALTER TABLE shop_items ADD COLUMN description TEXT NOT NULL DEFAULT '';
          END IF;
        END $$;
        """)

# =========================
# Chat / Admin
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

@with_conn
def add_chat_admin(conn, chat_id: int, user_id: int, role: str = "admin"):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO chat_admins(chat_id,user_id,role)
        VALUES(%s,%s,%s)
        ON CONFLICT(chat_id,user_id) DO NOTHING
        """, (chat_id, user_id, role))

@with_conn
def del_chat_admin(conn, chat_id: int, user_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM chat_admins WHERE chat_id=%s", (chat_id,))
        c = cur.fetchone()[0]
        cur.execute("SELECT 1 FROM chat_admins WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        if not cur.fetchone():
            return False, "该用户不是此群管理员"
        if c <= 1:
            return False, "至少保留1位群管理员"
        cur.execute("DELETE FROM chat_admins WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        return True, "已移除群管理员"

@with_conn
def is_chat_admin(conn, chat_id: int, user_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM chat_admins WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        return cur.fetchone() is not None

@with_conn
def list_user_admin_chats(conn, user_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT c.chat_id,c.title FROM chat_admins a
        JOIN chats c ON c.chat_id=a.chat_id
        WHERE a.user_id=%s AND c.active=TRUE
        ORDER BY c.updated_at DESC
        """, (user_id,))
        return cur.fetchall()

@with_conn
def list_chat_admins(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT user_id,role,created_at FROM chat_admins
        WHERE chat_id=%s ORDER BY created_at ASC
        """, (chat_id,))
        return cur.fetchall()

@with_conn
def list_chat_admin_ids(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT user_id FROM chat_admins WHERE chat_id=%s ORDER BY created_at ASC", (chat_id,))
        return [int(r[0]) for r in cur.fetchall()]

# =========================
# Chat Settings
# =========================
@with_conn
def get_chat_settings(conn, chat_id: int) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT rank_keywords,redeem_notice FROM chat_settings WHERE chat_id=%s", (chat_id,))
        row = cur.fetchone()
        if row:
            return {"rank_keywords": row[0], "redeem_notice": row[1]}
        return {"rank_keywords": "排行榜,排名,积分榜", "redeem_notice": ""}

@with_conn
def set_chat_setting(conn, chat_id: int, key: str, value: str):
    with conn.cursor() as cur:
        if key == "rank_keywords":
            cur.execute("""
            INSERT INTO chat_settings(chat_id,rank_keywords,updated_at)
            VALUES(%s,%s,NOW())
            ON CONFLICT(chat_id) DO UPDATE SET rank_keywords=EXCLUDED.rank_keywords,updated_at=NOW()
            """, (chat_id, value))
        elif key == "redeem_notice":
            cur.execute("""
            INSERT INTO chat_settings(chat_id,redeem_notice,updated_at)
            VALUES(%s,%s,NOW())
            ON CONFLICT(chat_id) DO UPDATE SET redeem_notice=EXCLUDED.redeem_notice,updated_at=NOW()
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
        cur.execute("SELECT balance_milli FROM wallets WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        r = cur.fetchone()
        return int(r[0]) if r else 0

@with_conn
def wallet_add(conn, chat_id: int, user_id: int, delta: int, reason: str = "drop"):
    """发放红包，同时写入 coin_logs（log_type=drop）"""
    with conn.cursor() as cur:
        if delta < 0:
            raise ValueError("wallet_add delta 必须 >= 0")
        cur.execute("""
        INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
        VALUES(%s,%s,%s,NOW())
        ON CONFLICT(chat_id,user_id)
        DO UPDATE SET balance_milli=wallets.balance_milli+EXCLUDED.balance_milli,updated_at=NOW()
        """, (chat_id, user_id, delta))
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,0,%s,%s,%s,'drop')
        """, (chat_id, user_id, delta, reason))

@with_conn
def wallet_adjust_admin(conn, chat_id: int, operator_id: int, user_id: int, delta: int, reason: str):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT balance_milli FROM wallets WHERE chat_id=%s AND user_id=%s FOR UPDATE
        """, (chat_id, user_id))
        row = cur.fetchone()
        old = int(row[0]) if row else 0
        new = old + delta
        if new < 0:
            return False, f"余额不足，当前 {milli_to_coin(old)}"
        cur.execute("""
        INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
        VALUES(%s,%s,%s,NOW())
        ON CONFLICT(chat_id,user_id) DO UPDATE SET balance_milli=%s,updated_at=NOW()
        """, (chat_id, user_id, new, new))
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason,log_type)
        VALUES(%s,%s,%s,%s,%s,'admin')
        """, (chat_id, operator_id, user_id, delta, reason or "panel_adjust"))
        return True, f"成功：用户 {user_id} 变动 {milli_to_coin(delta)}，新余额 {milli_to_coin(new)}"

@with_conn
def get_all_wallets(conn, chat_id: int, limit=15, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT user_id,balance_milli,updated_at FROM wallets WHERE chat_id=%s
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
            cur.execute("SELECT COUNT(*) FROM coin_logs WHERE chat_id=%s AND log_type=%s", (chat_id, log_type))
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
        cur.execute("SELECT COUNT(*) FROM coin_logs WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
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
        ORDER BY id DESC LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def shop_count(conn, chat_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM shop_items WHERE chat_id=%s", (chat_id,))
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
def add_item(conn, chat_id: int, title: str, price_milli: int, stock, description: str = ""):
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
            return False, "金币不足"
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
        """, (chat_id, user_id, user_id, -int(price), f"buy_item:{item_id}:{title}"))
        return True, f"购买成功：{title}，扣除 {milli_to_coin(price)} 金币"

# =========================
# Statistics
# =========================
@with_conn
def get_chat_stats(conn, chat_id: int) -> dict:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wallets WHERE chat_id=%s", (chat_id,))
        total_users = int(cur.fetchone()[0])
        cur.execute("SELECT COALESCE(SUM(balance_milli),0) FROM wallets WHERE chat_id=%s", (chat_id,))
        total_balance = int(cur.fetchone()[0])
        cur.execute("""
        SELECT COALESCE(SUM(delta_milli),0) FROM coin_logs
        WHERE chat_id=%s AND log_type='drop' AND created_at>=CURRENT_DATE
        """, (chat_id,))
        today_drop = int(cur.fetchone()[0])
        cur.execute("""
        SELECT COALESCE(SUM(ABS(delta_milli)),0) FROM coin_logs
        WHERE chat_id=%s AND log_type='redeem' AND created_at>=CURRENT_DATE
        """, (chat_id,))
        today_redeem = int(cur.fetchone()[0])
        cur.execute("""
        SELECT COALESCE(SUM(delta_milli),0) FROM coin_logs
        WHERE chat_id=%s AND log_type='drop'
        AND created_at>=DATE_TRUNC('month',CURRENT_DATE)
        """, (chat_id,))
        month_drop = int(cur.fetchone()[0])
        cur.execute("""
        SELECT COALESCE(SUM(ABS(delta_milli)),0) FROM coin_logs
        WHERE chat_id=%s AND log_type='redeem'
        AND created_at>=DATE_TRUNC('month',CURRENT_DATE)
        """, (chat_id,))
        month_redeem = int(cur.fetchone()[0])
        cur.execute("""
        SELECT COUNT(*) FROM redeem_orders WHERE chat_id=%s AND created_at>=CURRENT_DATE
        """, (chat_id,))
        today_orders = int(cur.fetchone()[0])
        cur.execute("""
        SELECT COUNT(*) FROM redeem_orders
        WHERE chat_id=%s AND created_at>=DATE_TRUNC('month',CURRENT_DATE)
        """, (chat_id,))
        month_orders = int(cur.fetchone()[0])
        cur.execute("""
        SELECT COALESCE(SUM(delta_milli),0) FROM coin_logs
        WHERE chat_id=%s AND log_type='admin' AND delta_milli>0 AND created_at>=CURRENT_DATE
        """, (chat_id,))
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
        SELECT user_id,balance_milli FROM wallets WHERE chat_id=%s AND balance_milli>0
        ORDER BY balance_milli DESC LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def get_wallet_rank_count(conn, chat_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM wallets WHERE chat_id=%s AND balance_milli>0", (chat_id,))
        return int(cur.fetchone()[0])

# =========================
# Redis 风控（原子操作修复）
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
STEP_OPTIONS = [100, 500, 1000, 5000, 10000]

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

def ensure_admin_selected_chat(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    cid = selected_chat_id(context)
    if cid == 0:
        return False, "请先选择管理群组"
    if not is_chat_admin(cid, user_id):
        return False, "你不在该群管理员列表"
    return True, cid

def can_use_admin_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.effective_chat or not update.effective_user:
        return False, "上下文无效"
    if update.effective_chat.type != "private":
        return False, "管理员命令仅可在私聊使用"
    uid = update.effective_user.id
    ok, cid_or_msg = ensure_admin_selected_chat(uid, context)
    if not ok:
        return False, f"{cid_or_msg}\n\n请先私聊执行：/start -> 选择管理群组"
    return True, cid_or_msg

def clear_pending_state(context: ContextTypes.DEFAULT_TYPE):
    for k in [
        "await_target_input", "await_rule_name", "await_item_title",
        "await_add_item_input", "await_rule_prob", "await_rule_min",
        "await_rule_max", "await_item_price", "await_item_stock",
        "await_item_desc", "await_rank_keywords", "await_redeem_notice",
    ]:
        context.user_data.pop(k, None)

def console_text(context: ContextTypes.DEFAULT_TYPE, uid: int):
    cid = selected_chat_id(context)
    title = selected_chat_title(context) or str(cid)
    role = "root+群管理员" if is_root_admin(uid) else "群管理员"
    return (
        "✅ 已进入管理控制台\n"
        f"当前管理群：{title} ({cid})\n"
        f"你的身份：{role}\n\n"
        "你现在的所有管理操作都只作用于这个群。"
    )

def stats_text(chat_id: int) -> str:
    s = get_chat_stats(chat_id)
    return (
        "📊 数据统计（当前群）\n"
        "─────────────────\n"
        f"👥 总参与用户：{s['total_users']} 人\n"
        f"💰 全群流通金币：{milli_to_coin(s['total_balance'])}\n"
        "─────────────────\n"
        "📅 今日数据\n"
        f"  🎁 发放金币：{milli_to_coin(s['today_drop'])}\n"
        f"  🛒 兑换金币：{milli_to_coin(s['today_redeem'])}\n"
        f"  📦 兑换笔数：{s['today_orders']} 笔\n"
        f"  🔧 管理员增发：{milli_to_coin(s['today_admin_add'])}\n"
        "─────────────────\n"
        "📆 本月数据\n"
        f"  🎁 发放金币：{milli_to_coin(s['month_drop'])}\n"
        f"  🛒 兑换金币：{milli_to_coin(s['month_redeem'])}\n"
        f"  📦 兑换笔数：{s['month_orders']} 笔\n"
    )

def rank_text(chat_id: int, page: int) -> tuple:
    """返回 (text, max_page)"""
    total = get_wallet_rank_count(chat_id)
    max_page = max(0, (total - 1) // RANK_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    rows = get_wallet_rank(chat_id, RANK_SIZE, page * RANK_SIZE)
    medals = ["🥇", "🥈", "🥉"]
    lines = [f"🏆 金币排行榜（第{page+1}/{max_page+1}页）\n"]
    for i, (uid, bal) in enumerate(rows):
        rank_num = page * RANK_SIZE + i + 1
        medal = medals[rank_num - 1] if rank_num <= 3 else f"#{rank_num}"
        lines.append(f"{medal} {uid} — {milli_to_coin(bal)} 金币")
    txt = "\n".join(lines) if rows else "🏆 暂无排行数据"
    return txt, max_page

def item_detail_text(it) -> str:
    _id, title, price, stock, enabled, desc = it
    lines = [
        f"🎁 编辑商品（当前群）",
        f"ID：{_id}",
        f"标题：{title}",
        f"价格：{milli_to_coin(price)} 金币",
        f"库存：{'∞' if stock is None else stock}",
        f"状态：{'✅ 上架' if enabled else '❌ 下架'}",
    ]
    if desc:
        lines.append(f"描述：{desc}")
    return "\n".join(lines)

def rule_detail_text(r) -> str:
    _id, name, p, mn, mx, en, pr = r
    return (
        f"⚙️ 编辑规则（当前群）\n"
        f"名称：{name}\n"
        f"概率：{p*100:.4f}%\n"
        f"最小：{milli_to_coin(mn)} 金币\n"
        f"最大：{milli_to_coin(mx)} 金币\n"
        f"状态：{'✅ 开启' if en else '❌ 关闭'}\n\n"
        f"💡 直接输入概率支持格式：0.5 或 0.5%"
    )

# =========================
# Keyboards
# =========================
def kb_home():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🗂️ 选择管理群组", callback_data="v4:groups:0")],
        [InlineKeyboardButton("🆔 我的ID", callback_data="v4:show_myid")]
    ])

def kb_groups(user_id: int, page: int):
    chats = list_user_admin_chats(user_id)
    total = len(chats)
    max_page = max(0, (total - 1) // 8) if total > 0 else 0
    page = clamp(page, 0, max_page)
    rows = []
    for cid, title in chats[page*8:(page+1)*8]:
        rows.append([InlineKeyboardButton(f"{title or cid}", callback_data=f"v4:selgroup:{cid}")])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v4:groups:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v4:groups:{min(max_page,page+1)}")
    ])
    rows.append([
        InlineKeyboardButton("🆔 我的ID", callback_data="v4:show_myid"),
        InlineKeyboardButton("🔄 刷新", callback_data=f"v4:groups:{page}")
    ])
    rows.append([InlineKeyboardButton("🏠 返回首页", callback_data="v4:home")])
    return InlineKeyboardMarkup(rows)

def kb_console():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 金币管理", callback_data="v4:adm_user"),
         InlineKeyboardButton("⚙️ 规则管理", callback_data="v4:adm_rules:0")],
        [InlineKeyboardButton("🎁 商品管理", callback_data="v4:adm_shop:0"),
         InlineKeyboardButton("🧾 操作日志", callback_data="v4:logs:0:all")],
        [InlineKeyboardButton("📊 数据统计", callback_data="v4:stats"),
         InlineKeyboardButton("🏆 排行榜管理", callback_data="v4:rank_mgr")],
        [InlineKeyboardButton("💰 用户余额", callback_data="v4:wallets:0"),
         InlineKeyboardButton("⚙️ 群组设置", callback_data="v4:chat_settings")],
        [InlineKeyboardButton("🛡️ 群管理员", callback_data="v4:adm_list")],
        [InlineKeyboardButton("🔁 切换管理群组", callback_data="v4:groups:0")]
    ])

def kb_adm_user():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 选择目标用户", callback_data="v4:adm_target"),
         InlineKeyboardButton("💱 切换步进", callback_data="v4:adm_step")],
        [InlineKeyboardButton("➕ 加金币", callback_data="v4:adm_add"),
         InlineKeyboardButton("➖ 扣金币", callback_data="v4:adm_sub")],
        [InlineKeyboardButton("📦 查余额", callback_data="v4:adm_qbal")],
        [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")]
    ])

def kb_adm_rules(chat_id: int, page: int):
    total = rules_count(chat_id)
    max_page = max(0, (total - 1) // RULE_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    rs = list_rules(chat_id, RULE_SIZE, page * RULE_SIZE)
    rows = []
    for r in rs:
        rows.append([InlineKeyboardButton(fmt_rule_row(r), callback_data=f"v4:adm_rule:{r[0]}")])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v4:adm_rules:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v4:adm_rules:{min(max_page,page+1)}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")])
    return InlineKeyboardMarkup(rows)

def kb_rule_edit(rid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ 改名称", callback_data=f"v4:r:name:{rid}"),
         InlineKeyboardButton("🔁 开关切换", callback_data=f"v4:r:toggle:{rid}")],
        [InlineKeyboardButton("📝 直接输入概率", callback_data=f"v4:r:prob_input:{rid}")],
        [InlineKeyboardButton("概率 -0.1%", callback_data=f"v4:r:p:-0.001:{rid}"),
         InlineKeyboardButton("概率 +0.1%", callback_data=f"v4:r:p:+0.001:{rid}")],
        [InlineKeyboardButton("概率 -1%", callback_data=f"v4:r:p:-0.01:{rid}"),
         InlineKeyboardButton("概率 +1%", callback_data=f"v4:r:p:+0.01:{rid}")],
        [InlineKeyboardButton("📝 直接输入最小值", callback_data=f"v4:r:min_input:{rid}"),
         InlineKeyboardButton("📝 直接输入最大值", callback_data=f"v4:r:max_input:{rid}")],
        [InlineKeyboardButton("最小 -0.1", callback_data=f"v4:r:min:-100:{rid}"),
         InlineKeyboardButton("最小 +0.1", callback_data=f"v4:r:min:+100:{rid}")],
        [InlineKeyboardButton("最大 -0.1", callback_data=f"v4:r:max:-100:{rid}"),
         InlineKeyboardButton("最大 +0.1", callback_data=f"v4:r:max:+100:{rid}")],
        [InlineKeyboardButton("🔙 返回规则列表", callback_data="v4:adm_rules:0")]
    ])

def kb_logs(chat_id: int, page: int, log_type: str = "all"):
    total = coin_logs_count(chat_id, log_type if log_type != "all" else None)
    max_page = max(0, (total - 1) // LOG_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)

    def _btn(label, lt):
        marker = "●" if log_type == lt else ""
        return InlineKeyboardButton(f"{marker}{label}", callback_data=f"v4:logs:0:{lt}")

    return InlineKeyboardMarkup([
        [_btn("全部", "all"), _btn("发放", "drop"),
         _btn("兑换", "redeem"), _btn("管理", "admin")],
        [InlineKeyboardButton("⬅️", callback_data=f"v4:logs:{max(0,page-1)}:{log_type}"),
         InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
         InlineKeyboardButton("➡️", callback_data=f"v4:logs:{min(max_page,page+1)}:{log_type}")],
        [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")]
    ])

def kb_adm_shop(chat_id: int, page: int):
    total = shop_count(chat_id)
    max_page = max(0, (total - 1) // SHOP_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    items = shop_page(chat_id, SHOP_SIZE, page * SHOP_SIZE)
    rows = []
    for item_id, title, price, stock, enabled, desc in items:
        rows.append([InlineKeyboardButton(
            f"{'✅' if enabled else '❌'} ID{item_id} {title[:10]}｜{milli_to_coin(price)}｜{'∞' if stock is None else stock}",
            callback_data=f"v4:adm_item:{item_id}"
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v4:adm_shop:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v4:adm_shop:{min(max_page,page+1)}")
    ])
    rows.append([InlineKeyboardButton("➕ 新增商品", callback_data="v4:adm_additem_start")])
    rows.append([InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")])
    return InlineKeyboardMarkup(rows)

def kb_item_edit(item_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔁 上/下架", callback_data=f"v4:i:toggle:{item_id}")],
        [InlineKeyboardButton("📝 直接输入价格", callback_data=f"v4:i:price_input:{item_id}"),
         InlineKeyboardButton("📝 直接输入库存", callback_data=f"v4:i:stock_input:{item_id}")],
        [InlineKeyboardButton("价格 -0.5", callback_data=f"v4:i:price:-500:{item_id}"),
         InlineKeyboardButton("价格 +0.5", callback_data=f"v4:i:price:+500:{item_id}")],
        [InlineKeyboardButton("价格 -1", callback_data=f"v4:i:price:-1000:{item_id}"),
         InlineKeyboardButton("价格 +1", callback_data=f"v4:i:price:+1000:{item_id}")],
        [InlineKeyboardButton("库存 -1", callback_data=f"v4:i:stock:-1:{item_id}"),
         InlineKeyboardButton("库存 +1", callback_data=f"v4:i:stock:+1:{item_id}")],
        [InlineKeyboardButton("库存设∞", callback_data=f"v4:i:stockinf:{item_id}")],
        [InlineKeyboardButton("✏️ 改标题", callback_data=f"v4:i:title:{item_id}"),
         InlineKeyboardButton("📋 改描述", callback_data=f"v4:i:desc:{item_id}")],
        [InlineKeyboardButton("🔙 返回商品列表", callback_data="v4:adm_shop:0")]
    ])

def kb_user_panel():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎁 兑换商品", callback_data="v4:u:shop:0"),
         InlineKeyboardButton("🧾 金币记录", callback_data="v4:u:logs:0")],
        [InlineKeyboardButton("🔄 刷新余额", callback_data="v4:u:refresh")]
    ])

def kb_wallets(chat_id: int, page: int):
    total = get_all_wallets_count(chat_id)
    max_page = max(0, (total - 1) // WALLET_PAGE_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️", callback_data=f"v4:wallets:{max(0,page-1)}"),
         InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
         InlineKeyboardButton("➡️", callback_data=f"v4:wallets:{min(max_page,page+1)}")],
        [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")]
    ])

def kb_rank_nav(chat_id: int, page: int):
    total = get_wallet_rank_count(chat_id)
    max_page = max(0, (total - 1) // RANK_SIZE) if total > 0 else 0
    page = clamp(page, 0, max_page)
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️", callback_data=f"v4:rank:{max(0,page-1)}"),
         InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
         InlineKeyboardButton("➡️", callback_data=f"v4:rank:{min(max_page,page+1)}")]
    ])

def kb_chat_settings():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔑 修改排行榜关键词", callback_data="v4:set_rank_kw")],
        [InlineKeyboardButton("📢 修改兑换通知文本", callback_data="v4:set_redeem_notice")],
        [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")]
    ])

def kb_rank_mgr():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🏆 查看排行榜", callback_data="v4:rank:0")],
        [InlineKeyboardButton("🔑 修改排行榜关键词", callback_data="v4:set_rank_kw")],
        [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")]
    ])

def user_panel_text(chat_id: int, user_id: int):
    bal = wallet_get(chat_id, user_id)
    return (
        "🛍️ 用户面板（当前群）\n"
        f"用户ID：{user_id}\n"
        f"当前余额：{milli_to_coin(bal)} 金币\n\n"
        "可进行兑换、查看金币记录。"
    )

# =========================
# 命令刷新
# =========================
async def refresh_private_commands_for_user(app, user_id: int):
    try:
        chats = list_user_admin_chats(user_id)
        base_cmds = [
            BotCommand("start", "打开面板"),
            BotCommand("myid", "查看我的ID"),
            BotCommand("whoami", "查看当前身份"),
            BotCommand("cancel", "取消当前输入"),
        ]
        admin_cmds = [
            BotCommand("additem", "新增商品：/additem 标题 | 价格 | 库存"),
            BotCommand("bind_admin", "群内授权管理员：/bind_admin 用户ID"),
            BotCommand("unbind_admin", "群内移除管理员：/unbind_admin 用户ID"),
        ]
        cmds = base_cmds + (admin_cmds if chats else [])
        await app.bot.set_my_commands(commands=cmds, scope=BotCommandScopeChat(chat_id=user_id))
    except Exception:
        logger.exception("refresh_private_commands_for_user failed: user_id=%s", user_id)

async def post_init(app):
    try:
        await app.bot.set_my_commands(
            commands=[
                BotCommand("start", "打开面板"),
                BotCommand("myid", "查看我的ID"),
                BotCommand("whoami", "查看当前身份"),
                BotCommand("buy", "群内兑换：/buy 商品ID"),
                BotCommand("cancel", "取消当前输入"),
            ],
            scope=BotCommandScopeDefault()
        )
        await app.bot.set_my_commands(
            commands=[
                BotCommand("start", "打开面板并选择管理群"),
                BotCommand("myid", "查看我的ID"),
                BotCommand("whoami", "查看当前身份"),
                BotCommand("cancel", "取消当前输入"),
            ],
            scope=BotCommandScopeAllPrivateChats()
        )
        seen = set()
        with pg_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT user_id FROM chat_admins")
                for (uid,) in cur.fetchall():
                    uid = int(uid)
                    if uid not in seen:
                        seen.add(uid)
                        await refresh_private_commands_for_user(app, uid)
    except Exception:
        logger.exception("set_my_commands failed")

# =========================
# 兑换通知（含自定义文本+用户主页链接）
# =========================
async def notify_purchase(context: ContextTypes.DEFAULT_TYPE, chat_id: int, buyer_id: int, item_id: int):
    try:
        it = get_item(chat_id, item_id)
        if not it:
            return
        _id, title, price, stock, enabled, desc = it
        bal = wallet_get(chat_id, buyer_id)
        settings = get_chat_settings(chat_id)
        notice_extra = settings.get("redeem_notice", "").strip()

        # 群内通知
        group_text = (
            "🧾 兑换成功通知\n"
            f"用户：{buyer_id}\n"
            f"商品：{title}\n"
            f"花费：{milli_to_coin(price)} 金币"
        )
        if notice_extra:
            group_text += f"\n\n📌 {notice_extra}"
        try:
            await context.bot.send_message(chat_id=chat_id, text=group_text)
        except Exception:
            logger.exception("notify group failed: chat_id=%s", chat_id)

        # 管理员私聊通知（含用户主页按钮）
        admin_ids = list_chat_admin_ids(chat_id)
        if not admin_ids:
            return
        admin_text = (
            "🛒 用户兑换通知\n"
            f"群ID：{chat_id}\n"
            f"用户ID：{buyer_id}\n"
            f"商品：ID{item_id} {title}\n"
            f"价格：{milli_to_coin(price)} 金币\n"
            f"用户余额：{milli_to_coin(bal)} 金币\n"
            f"库存剩余：{'∞' if stock is None else stock}"
        )
        if notice_extra:
            admin_text += f"\n\n📌 兑奖说明：{notice_extra}"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton(f"👤 打开用户主页 {buyer_id}", url=f"tg://user?id={buyer_id}")]
        ])
        for aid in admin_ids:
            try:
                await context.bot.send_message(chat_id=aid, text=admin_text, reply_markup=kb)
            except Exception:
                logger.exception("notify admin failed: admin_id=%s", aid)
    except Exception:
        logger.exception("notify_purchase failed")

# =========================
# Misc helpers
# =========================
# 接在 safe_edit 的 except Exception 块之后

async def safe_edit(q, text, reply_markup=None):
    try:
        await q.edit_message_text(text=text, reply_markup=reply_markup)
    except BadRequest as e:
        msg = str(e)
        if "Message is not modified" in msg:
            return
        try:
            await q.message.reply_text(text=text, reply_markup=reply_markup)
        except Exception:
            logger.exception("safe_edit fallback failed")
    except Exception:
        logger.exception("safe_edit failed")
        try:
            await q.message.reply_text(text=text, reply_markup=reply_markup)
        except Exception:
            pass

async def _job_delete_msg(context: ContextTypes.DEFAULT_TYPE):
    job = context.job
    if not job:
        return
    data = job.data or {}
    chat_id = data.get("chat_id")
    message_id = data.get("message_id")
    if not chat_id or not message_id:
        return
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def _del_after(app, chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try:
        await app.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception:
        pass

async def auto_delete_pair(context: ContextTypes.DEFAULT_TYPE, chat_id: int,
                            trigger_mid: int, bot_mid: int, delay: int = 60):
    try:
        jq = context.application.job_queue
        if jq is not None:
            jq.run_once(_job_delete_msg, when=delay,
                        data={"chat_id": chat_id, "message_id": trigger_mid},
                        name=f"del_trigger_{chat_id}_{trigger_mid}")
            jq.run_once(_job_delete_msg, when=delay,
                        data={"chat_id": chat_id, "message_id": bot_mid},
                        name=f"del_bot_{chat_id}_{bot_mid}")
        else:
            asyncio.create_task(_del_after(context.application, chat_id, trigger_mid, delay))
            asyncio.create_task(_del_after(context.application, chat_id, bot_mid, delay))
    except Exception:
        logger.exception("schedule auto delete pair failed")

async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    logger.exception("Unhandled error: %s", context.error)
    try:
        if isinstance(update, Update) and update.effective_message:
            await update.effective_message.reply_text("⚠️ 内部错误，请稍后重试。")
    except Exception:
        pass

# =========================
# Commands
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    # 不清除已选群组，仅清除输入状态，提升管理员体验
    clear_pending_state(context)
    if update.effective_user:
        await refresh_private_commands_for_user(context.application, update.effective_user.id)
    await update.message.reply_text(
        "📋 管理面板\n请先选择你要管理的群组：",
        reply_markup=kb_home()
    )

async def myid_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    await update.message.reply_text(f"你的用户ID：{update.effective_user.id}")

async def whoami_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user or not update.effective_chat:
        return
    uid = update.effective_user.id
    chat = update.effective_chat
    cid = chat.id
    root = is_root_admin(uid)
    cadm = is_chat_admin(cid, uid) if chat.type in ("group", "supergroup") else False
    sel = selected_chat_id(context)
    await update.message.reply_text(
        f"user_id={uid}\nchat_id={cid}\nchat_type={chat.type}\n"
        f"is_root={root}\nis_chat_admin_here={cadm}\nselected_chat_id={sel}"
    )

async def cancel_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    clear_pending_state(context)
    await update.message.reply_text("已取消当前输入流程。")

async def additem_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message:
        return
    ok, cid_or_msg = can_use_admin_command(update, context)
    if not ok:
        await update.message.reply_text(cid_or_msg)
        return
    chat_id = cid_or_msg
    raw = update.message.text.replace("/additem", "", 1).strip()
    parts = [x.strip() for x in raw.split("|")]
    if len(parts) < 2:
        await update.message.reply_text("用法：/additem 标题 | 价格 | 库存(可选) | 描述(可选)")
        return
    title = parts[0]
    try:
        price = coin_to_milli(parts[1])
    except Exception:
        await update.message.reply_text("价格格式错误")
        return
    stock = None
    if len(parts) >= 3 and parts[2]:
        try:
            stock = int(parts[2])
            if stock < 0:
                await update.message.reply_text("库存不能小于0")
                return
        except Exception:
            await update.message.reply_text("库存必须是整数")
            return
    description = parts[3] if len(parts) >= 4 else ""
    add_item(chat_id, title, price, stock, description)
    await update.message.reply_text(f"✅ 已添加商品：{title}｜{milli_to_coin(price)}｜库存{'∞' if stock is None else stock}")

async def buy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type not in ("group", "supergroup"):
        await update.message.reply_text("请在群里使用 /buy")
        return
    chat_id = update.effective_chat.id
    if not context.args:
        await update.message.reply_text("用法：/buy 商品ID")
        return
    item_id = safe_int(context.args[0], 0)
    if item_id <= 0:
        await update.message.reply_text("商品ID错误")
        return
    ok, msg = buy_item_atomic(chat_id, update.effective_user.id, item_id)
    await update.message.reply_text(msg)
    if ok:
        try:
            await notify_purchase(context, chat_id, update.effective_user.id, item_id)
        except Exception:
            logger.exception("notify purchase failed in /buy")

async def bind_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type not in ("group", "supergroup"):
        await update.message.reply_text("请在目标群里使用")
        return
    chat_id = update.effective_chat.id
    chat_title = update.effective_chat.title or str(chat_id)
    upsert_chat(chat_id, chat_title)
    ensure_default_rules(chat_id)
    operator = update.effective_user.id
    if not (is_root_admin(operator) or is_chat_admin(chat_id, operator)):
        await update.message.reply_text("无权限：仅本群已绑定管理员可授权。")
        return
    if not context.args:
        await update.message.reply_text("用法：/bind_admin 用户ID")
        return
    target = safe_int(context.args[0], 0)
    if target <= 0:
        await update.message.reply_text("用户ID错误")
        return
    add_chat_admin(chat_id, target, "admin")
    await update.message.reply_text(f"✅ 已授权 {target} 为本群管理员")
    await refresh_private_commands_for_user(context.application, target)

async def unbind_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type not in ("group", "supergroup"):
        await update.message.reply_text("请在目标群里使用")
        return
    chat_id = update.effective_chat.id
    operator = update.effective_user.id
    if not (is_root_admin(operator) or is_chat_admin(chat_id, operator)):
        await update.message.reply_text("无权限。")
        return
    if not context.args:
        await update.message.reply_text("用法：/unbind_admin 用户ID")
        return
    target = safe_int(context.args[0], 0)
    if target <= 0:
        await update.message.reply_text("用户ID错误")
        return
    ok, msg = del_chat_admin(chat_id, target)
    await update.message.reply_text(msg)
    if ok:
        await refresh_private_commands_for_user(context.application, target)

# =========================
# Callback Handler
# =========================
async def cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.message or not update.effective_user:
        return
    try:
        data = q.data or ""
        uid = update.effective_user.id
        logger.info("CB data=%s uid=%s", data, uid)

        if data == "v4:noop":
            await q.answer()
            return

        if data == "v4:show_myid":
            await q.answer(f"你的ID: {uid}", show_alert=True)
            try:
                await q.message.reply_text(f"🆔 你的用户ID：{uid}")
            except Exception:
                pass
            return

        if data == "v4:home":
            await q.answer()
            clear_pending_state(context)
            await safe_edit(q, "📋 管理面板\n请先选择你要管理的群组：", reply_markup=kb_home())
            return

        if data.startswith("v4:groups:"):
            await q.answer()
            clear_pending_state(context)
            page = max(0, safe_int(data.split(":")[2], 0))
            chats = list_user_admin_chats(uid)
            if not chats:
                await safe_edit(q,
                    "🗂️ 你还没有可管理的群\n\n"
                    "请在目标群由 root 或现管理员执行：\n"
                    "/bind_admin 你的用户ID\n\n先发 /myid 获取你的ID",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🆔 我的ID", callback_data="v4:show_myid")],
                        [InlineKeyboardButton("🔄 刷新", callback_data="v4:groups:0")],
                        [InlineKeyboardButton("🏠 返回首页", callback_data="v4:home")]
                    ]))
                return
            await safe_edit(q, "🗂️ 请选择管理群组：", reply_markup=kb_groups(uid, page))
            return

        if data.startswith("v4:selgroup:"):
            cid = safe_int(data.split(":")[2], 0)
            if cid == 0 or not is_chat_admin(cid, uid):
                await q.answer("无权限选择该群", show_alert=True)
                return
            await q.answer()
            clear_pending_state(context)
            context.user_data["sel_chat_id"] = cid
            chats = {c[0]: c[1] for c in list_user_admin_chats(uid)}
            context.user_data["sel_chat_title"] = chats.get(cid, str(cid))
            ensure_default_rules(cid)
            await safe_edit(q, console_text(context, uid), reply_markup=kb_console())
            return

        if data == "v4:admin_home":
            await q.answer()
            clear_pending_state(context)
            ok, v = ensure_admin_selected_chat(uid, context)
            if not ok:
                await safe_edit(q, "请先选择管理群组：",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton("🗂️ 选择管理群组", callback_data="v4:groups:0")]
                    ]))
                return
            await safe_edit(q, console_text(context, uid), reply_markup=kb_console())
            return

        # ===== 用户面板（无需管理员权限）=====
        if data == "v4:u:refresh":
            await q.answer()
            chat_id = q.message.chat_id
            await safe_edit(q, user_panel_text(chat_id, uid), reply_markup=kb_user_panel())
            return

        if data.startswith("v4:u:shop:"):
            await q.answer()
            chat_id = q.message.chat_id
            # 限制：只有消息发起者可翻页
            orig_uid = context.user_data.get(f"shop_owner_{q.message.message_id}")
            if orig_uid is None:
                context.user_data[f"shop_owner_{q.message.message_id}"] = uid
                orig_uid = uid
            if uid != orig_uid:
                await q.answer("请自己发送「商城」关键词打开专属面板", show_alert=True)
                return
            page = max(0, safe_int(data.split(":")[3], 0))
            total = shop_count(chat_id)
            max_page = max(0, (total - 1) // SHOP_SIZE) if total > 0 else 0
            page = clamp(page, 0, max_page)
            items = shop_page(chat_id, SHOP_SIZE, page * SHOP_SIZE)
            buy_rows = []
            lines = ["🎁 商城（当前群）"]
            for item_id, title, price, stock, enabled, desc in items:
                if not enabled:
                    continue
                stock_str = "∞" if stock is None else str(stock)
                line = f"ID{item_id}｜{title}｜{milli_to_coin(price)}｜库存{stock_str}"
                if desc:
                    line += f"\n  📝 {desc}"
                lines.append(line)
                buy_rows.append([InlineKeyboardButton(
                    f"🛒 购买 ID{item_id} {title[:8]}",
                    callback_data=f"v4:u:buy:{item_id}:{page}"
                )])
            txt = "\n".join(lines) if len(lines) > 1 else "🎁 商城（当前群）\n暂无可兑换商品"
            kb = InlineKeyboardMarkup(buy_rows + [
                [InlineKeyboardButton("⬅️", callback_data=f"v4:u:shop:{max(0,page-1)}"),
                 InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
                 InlineKeyboardButton("➡️", callback_data=f"v4:u:shop:{min(max_page,page+1)}")],
                [InlineKeyboardButton("🔙 返回用户面板", callback_data="v4:u:refresh")]
            ])
            await safe_edit(q, txt, reply_markup=kb)
            return

        if data.startswith("v4:u:logs:"):
            await q.answer()
            chat_id = q.message.chat_id
            page = max(0, safe_int(data.split(":")[3], 0))
            total = user_coin_logs_count(chat_id, uid)
            max_page = max(0, (total - 1) // LOG_SIZE) if total > 0 else 0
            page = clamp(page, 0, max_page)
            rows = user_coin_logs_page(chat_id, uid, LOG_SIZE, page * LOG_SIZE)
            if not rows:
                txt = "🧾 你的金币记录（当前群）\n暂无记录"
            else:
                type_emoji = {"drop": "🎁", "redeem": "🛒", "admin": "🔧"}
                lines = ["🧾 你的金币记录（当前群）"]
                for lid, op, u, d, reason, ct, lt in rows:
                    sign = "+" if d >= 0 else ""
                    emoji = type_emoji.get(lt, "•")
                    lines.append(f"{emoji} #{lid} | {sign}{milli_to_coin(d)} | {reason}")
                txt = "\n".join(lines)
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️", callback_data=f"v4:u:logs:{max(0,page-1)}"),
                 InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
                 InlineKeyboardButton("➡️", callback_data=f"v4:u:logs:{min(max_page,page+1)}")],
                [InlineKeyboardButton("🔙 返回用户面板", callback_data="v4:u:refresh")]
            ])
            await safe_edit(q, txt[:3900], reply_markup=kb)
            return

        if data.startswith("v4:u:buy:"):
            chat_id = q.message.chat_id
            parts = data.split(":")
            item_id = safe_int(parts[3], 0)
            page = safe_int(parts[4], 0) if len(parts) > 4 else 0
            ok_buy, msg = buy_item_atomic(chat_id, uid, item_id)
            await q.answer(msg[:180], show_alert=True)
            if ok_buy:
                try:
                    await notify_purchase(context, chat_id, uid, item_id)
                except Exception:
                    logger.exception("notify purchase failed")
            # 刷新商城列表
            total = shop_count(chat_id)
            max_page = max(0, (total - 1) // SHOP_SIZE) if total > 0 else 0
            page = clamp(page, 0, max_page)
            items = shop_page(chat_id, SHOP_SIZE, page * SHOP_SIZE)
            buy_rows = []
            lines = ["🎁 商城（当前群）"]
            for iid, title, price, stock, enabled, desc in items:
                if not enabled:
                    continue
                lines.append(f"ID{iid}｜{title}｜{milli_to_coin(price)}｜库存{'∞' if stock is None else stock}")
                buy_rows.append([InlineKeyboardButton(
                    f"🛒 购买 ID{iid} {title[:8]}",
                    callback_data=f"v4:u:buy:{iid}:{page}"
                )])
            txt = "\n".join(lines) if len(lines) > 1 else "🎁 商城（当前群）\n暂无可兑换商品"
            kb = InlineKeyboardMarkup(buy_rows + [
                [InlineKeyboardButton("⬅️", callback_data=f"v4:u:shop:{max(0,page-1)}"),
                 InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
                 InlineKeyboardButton("➡️", callback_data=f"v4:u:shop:{min(max_page,page+1)}")],
                [InlineKeyboardButton("🔙 返回用户面板", callback_data="v4:u:refresh")]
            ])
            await safe_edit(q, txt, reply_markup=kb)
            return

        # ===== 管理员区鉴权 =====
        ok, v = ensure_admin_selected_chat(uid, context)
        if not ok:
            await q.answer(v, show_alert=True)
            return
        chat_id = v

        # ----- 数据统计 -----
        if data == "v4:stats":
            await q.answer()
            await safe_edit(q, stats_text(chat_id),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 刷新", callback_data="v4:stats")],
                    [InlineKeyboardButton("🔙 返回控制台", callback_data="v4:admin_home")]
                ]))
            return

        # ----- 排行榜管理 -----
        if data == "v4:rank_mgr":
            await q.answer()
            settings = get_chat_settings(chat_id)
            kw = settings.get("rank_keywords", "排行榜,排名,积分榜")
            await safe_edit(q,
                f"🏆 排行榜管理\n当前触发关键词：{kw}\n\n"
                "用户在群内发送关键词即可查看排行榜",
                reply_markup=kb_rank_mgr())
            return

        if data.startswith("v4:rank:"):
            await q.answer()
            page = max(0, safe_int(data.split(":")[2], 0))
            txt, max_page = rank_text(chat_id, page)
            page = clamp(page, 0, max_page)
            await safe_edit(q, txt, reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️", callback_data=f"v4:rank:{max(0,page-1)}"),
                 InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
                 InlineKeyboardButton("➡️", callback_data=f"v4:rank:{min(max_page,page+1)}")],
                [InlineKeyboardButton("🔙 返回排行榜管理", callback_data="v4:rank_mgr")]
            ]))
            return

        # ----- 群组设置 -----
        if data == "v4:chat_settings":
            await q.answer()
            settings = get_chat_settings(chat_id)
            kw = settings.get("rank_keywords", "排行榜,排名,积分榜")
            notice = settings.get("redeem_notice", "") or "（未设置）"
            await safe_edit(q,
                f"⚙️ 群组设置\n\n"
                f"🔑 排行榜关键词：{kw}\n\n"
                f"📢 兑换通知文本：{notice}",
                reply_markup=kb_chat_settings())
            return

        if data == "v4:set_rank_kw":
            await q.answer()
            context.user_data["await_rank_keywords"] = True
            await safe_edit(q,
                "🔑 请输入排行榜触发关键词\n"
                "多个关键词用英文逗号分隔\n"
                "例如：排行榜,排名,积分榜,top\n\n"
                "发送 /cancel 可取消")
            return

        if data == "v4:set_redeem_notice":
            await q.answer()
            context.user_data["await_redeem_notice"] = True
            await safe_edit(q,
                "📢 请输入兑换成功后的通知文本\n"
                "此文本会附加在兑换通知末尾，告知用户如何联系兑奖\n"
                "例如：请联系 @admin 进行兑奖，备注订单号\n\n"
                "发送「清空」可删除通知文本\n"
                "发送 /cancel 可取消")
            return

        # ----- 用户余额列表 -----
        if data.startswith("v4:wallets:"):
            await q.answer()
            page = max(0, safe_int(data.split(":")[2], 0))
            total = get_all_wallets_count(chat_id)
            max_page = max(0, (total - 1) // WALLET_PAGE_SIZE) if total > 0 else 0
            page = clamp(page, 0, max_page)
            rows = get_all_wallets(chat_id, WALLET_PAGE_SIZE, page * WALLET_PAGE_SIZE)
            if not rows:
                txt = "💰 用户余额列表\n暂无数据"
            else:
                lines = [f"💰 用户余额列表（第{page+1}/{max_page+1}页）\n"]
                for i, (wuid, bal, upd) in enumerate(rows):
                    rank = page * WALLET_PAGE_SIZE + i + 1
                    lines.append(f"#{rank} {wuid} — {milli_to_coin(bal)} 金币")
                txt = "\n".join(lines)
            await safe_edit(q, txt, reply_markup=kb_wallets(chat_id, page))
            return

        # ----- 管理员列表 -----
        if data == "v4:adm_list":
            await q.answer()
            rows = list_chat_admins(chat_id)
            txt = ("🛡️ 本群管理员列表：\n" +
                   "\n".join([f"- {x[0]} ({x[1]})" for x in rows])) if rows else "暂无管理员"
            await safe_edit(q, txt, reply_markup=kb_console())
            return

        # ----- 金币管理 -----
        if data == "v4:adm_user":
            await q.answer()
            context.user_data.setdefault("adm_target", uid)
            context.user_data.setdefault("adm_step", 1000)
            t = context.user_data["adm_target"]
            s = context.user_data["adm_step"]
            bal = wallet_get(chat_id, t)
            await safe_edit(q,
                f"👤 金币管理（仅当前群）\n目标用户：{t}\n步进：{milli_to_coin(s)}\n目标余额：{milli_to_coin(bal)}",
                reply_markup=kb_adm_user())
            return

        if data == "v4:adm_target":
            await q.answer()
            context.user_data["await_target_input"] = True
            await safe_edit(q, "请输入目标用户ID（纯数字）\n发送 /cancel 可取消")
            return

        if data == "v4:adm_step":
            await q.answer()
            cur_step = context.user_data.get("adm_step", 1000)
            idx = STEP_OPTIONS.index(cur_step) if cur_step in STEP_OPTIONS else 2
            idx = (idx + 1) % len(STEP_OPTIONS)
            context.user_data["adm_step"] = STEP_OPTIONS[idx]
            t = context.user_data.get("adm_target", uid)
            bal = wallet_get(chat_id, t)
            await safe_edit(q,
                f"👤 金币管理（仅当前群）\n目标用户：{t}\n步进：{milli_to_coin(context.user_data['adm_step'])}\n目标余额：{milli_to_coin(bal)}",
                reply_markup=kb_adm_user())
            return

        if data == "v4:adm_qbal":
            t = context.user_data.get("adm_target", uid)
            bal = wallet_get(chat_id, t)
            await q.answer(f"用户 {t} 当前群余额：{milli_to_coin(bal)}", show_alert=True)
            return

        if data in ("v4:adm_add", "v4:adm_sub"):
            t = context.user_data.get("adm_target", uid)
            s = context.user_data.get("adm_step", 1000)
            delta = s if data == "v4:adm_add" else -s
            ok2, msg = wallet_adjust_admin(chat_id, uid, t, delta, "panel_adjust")
            await q.answer(msg[:180], show_alert=True)
            bal = wallet_get(chat_id, t)
            await safe_edit(q,
                f"👤 金币管理（仅当前群）\n目标用户：{t}\n步进：{milli_to_coin(s)}\n目标余额：{milli_to_coin(bal)}",
                reply_markup=kb_adm_user())
            return

        # ----- 日志 -----
        if data.startswith("v4:logs:"):
            await q.answer()
            parts = data.split(":")
            page = max(0, safe_int(parts[2], 0))
            log_type = parts[3] if len(parts) > 3 else "all"
            lt_filter = None if log_type == "all" else log_type
            total = coin_logs_count(chat_id, lt_filter)
            max_page = max(0, (total - 1) // LOG_SIZE) if total > 0 else 0
            page = clamp(page, 0, max_page)
            rows = coin_logs_page(chat_id, LOG_SIZE, page * LOG_SIZE, lt_filter)
            type_emoji = {"drop": "🎁", "redeem": "🛒", "admin": "🔧"}
            type_label = {"all": "全部", "drop": "发放", "redeem": "兑换", "admin": "管理"}
            if not rows:
                txt = f"🧾 {type_label.get(log_type,'全部')}日志\n暂无记录"
            else:
                lines = [f"🧾 {type_label.get(log_type,'全部')}日志（第{page+1}/{max_page+1}页）"]
                for lid, op, u, d, reason, ct, lt in rows:
                    sign = "+" if d >= 0 else ""
                    emoji = type_emoji.get(lt, "•")
                    lines.append(f"{emoji} #{lid} | {op}→{u} | {sign}{milli_to_coin(d)} | {reason}")
                txt = "\n".join(lines)
            await safe_edit(q, txt[:3900], reply_markup=kb_logs(chat_id, page, log_type))
            return

        # ----- 规则管理 -----
        if data.startswith("v4:adm_rules:"):
            await q.answer()
            page = max(0, safe_int(data.split(":")[2], 0))
            ensure_default_rules(chat_id)
            await safe_edit(q, "⚙️ 规则管理（仅当前群）\n说明：概率与区间只影响当前群。",
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

        if data.startswith("v4:r:name:"):
            await q.answer()
            rid = safe_int(data.split(":")[3], 0)
            context.user_data["await_rule_name"] = rid
            await safe_edit(q, f"请输入规则 ID{rid} 的新名称（1~32字）\n发送 /cancel 可取消")
            return

        if data.startswith("v4:r:prob_input:"):
            await q.answer()
            rid = safe_int(data.split(":")[3], 0)
            context.user_data["await_rule_prob"] = rid
            await safe_edit(q,
                f"请输入规则 ID{rid} 的新概率\n"
                "支持格式：\n"
                "  • 直接输入小数：0.05（表示5%）\n"
                "  • 带百分号：5%（表示5%）\n"
                "范围：0% ~ 100%\n\n"
                "发送 /cancel 可取消")
            return

        if data.startswith("v4:r:min_input:"):
            await q.answer()
            rid = safe_int(data.split(":")[3], 0)
            context.user_data["await_rule_min"] = rid
            await safe_edit(q, f"请输入规则 ID{rid} 的最小金币数（如：0.5 或 1）\n发送 /cancel 可取消")
            return

        if data.startswith("v4:r:max_input:"):
            await q.answer()
            rid = safe_int(data.split(":")[3], 0)
            context.user_data["await_rule_max"] = rid
            await safe_edit(q, f"请输入规则 ID{rid} 的最大金币数（如：10 或 50.5）\n发送 /cancel 可取消")
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
            await safe_edit(q, rule_detail_text(nr), reply_markup=kb_rule_edit(rid))
            return

        if data.startswith("v4:r:p:") or data.startswith("v4:r:min:") or data.startswith("v4:r:max:"):
            await q.answer()
            parts = data.split(":")
            _, _, field, delta_s, rid_s = parts
            rid = safe_int(rid_s, 0)
            r = get_rule(chat_id, rid)
            if not r:
                await q.answer("规则不存在", show_alert=True)
                return
            _id, name, p, mn, mx, en, pr = r
            np2, nmn, nmx = float(p), int(mn), int(mx)
            if field == "p":
                np2 = round(clamp(np2 + float(delta_s), 0.0, 1.0), 6)
            elif field == "min":
                nmn = max(1, nmn + safe_int(delta_s, 0))
                if nmn > nmx:
                    nmn = nmx
            elif field == "max":
                nmx = max(nmn, nmx + safe_int(delta_s, 0))
            update_rule(chat_id, rid, np2, nmn, nmx, bool(en), name)
            nr = get_rule(chat_id, rid)
            await safe_edit(q, rule_detail_text(nr), reply_markup=kb_rule_edit(rid))
            return

        # ----- 商品管理 -----
        if data.startswith("v4:adm_shop:"):
            await q.answer()
            page = max(0, safe_int(data.split(":")[2], 0))
            await safe_edit(q, "🎁 商品管理（仅当前群）", reply_markup=kb_adm_shop(chat_id, page))
            return

        if data == "v4:adm_additem_start":
            await q.answer()
            context.user_data["await_add_item_input"] = True
            await safe_edit(q,
                "🧩 请输入新商品信息：\n"
                "格式：标题 | 价格 | 库存(可选) | 描述(可选)\n"
                "例如：周边徽章 | 9.9 | 20 | 限量款周边\n"
                "不填库存则为∞\n\n"
                "发送 /cancel 可取消")
            return

        if data.startswith("v4:adm_item:"):
            await q.answer()
            item_id = safe_int(data.split(":")[2], 0)
            it = get_item(chat_id, item_id)
            if not it:
                await q.answer("商品不存在", show_alert=True)
                return
            await safe_edit(q, item_detail_text(it), reply_markup=kb_item_edit(item_id))
            return

        if data.startswith("v4:i:toggle:"):
            await q.answer()
            item_id = safe_int(data.split(":")[3], 0)
            it = get_item(chat_id, item_id)
            if not it:
                await q.answer("商品不存在", show_alert=True)
                return
            _id, title, price, stock, enabled, desc = it
            update_item(chat_id, item_id, title, int(price), stock, not bool(enabled), desc)
            nit = get_item(chat_id, item_id)
            await safe_edit(q, item_detail_text(nit), reply_markup=kb_item_edit(item_id))
            return

        if data.startswith("v4:i:price_input:"):
            await q.answer()
            item_id = safe_int(data.split(":")[3], 0)
            context.user_data["await_item_price"] = item_id
            await safe_edit(q, f"请输入商品 ID{item_id} 的新价格（如：9.9 或 100）\n发送 /cancel 可取消")
            return

        if data.startswith("v4:i:stock_input:"):
            await q.answer()
            item_id = safe_int(data.split(":")[3], 0)
            context.user_data["await_item_stock"] = item_id
            await safe_edit(q, f"请输入商品 ID{item_id} 的新库存（整数，0表示无货）\n发送 /cancel 可取消")
            return

        if data.startswith("v4:i:price:"):
            await q.answer()
            parts = data.split(":")
            _, _, _, delta_s, item_id_s = parts
            item_id = safe_int(item_id_s, 0)
            it = get_item(chat_id, item_id)
            if not it:
                await q.answer("商品不存在", show_alert=True)
                return
            _id, title, price, stock, enabled, desc = it
            nprice = max(1, int(price) + safe_int(delta_s, 0))
            update_item(chat_id, item_id, title, nprice, stock, bool(enabled), desc)
            nit = get_item(chat_id, item_id)
            await safe_edit(q, item_detail_text(nit), reply_markup=kb_item_edit(item_id))
            return

        if data.startswith("v4:i:stock:"):
            await q.answer()
            parts = data.split(":")
            _, _, _, delta_s, item_id_s = parts
            item_id = safe_int(item_id_s, 0)
            it = get_item(chat_id, item_id)
            if not it:
                await q.answer("商品不存在", show_alert=True)
                return
            _id, title, price, stock, enabled, desc = it
            cur_stock = 0 if stock is None else int(stock)
            nstock = max(0, cur_stock + safe_int(delta_s, 0))
            update_item(chat_id, item_id, title, int(price), nstock, bool(enabled), desc)
            nit = get_item(chat_id, item_id)
            await safe_edit(q, item_detail_text(nit), reply_markup=kb_item_edit(item_id))
            return

        if data.startswith("v4:i:stockinf:"):
            await q.answer()
            item_id = safe_int(data.split(":")[3], 0)
            it = get_item(chat_id, item_id)
            if not it:
                await q.answer("商品不存在", show_alert=True)
                return
            _id, title, price, stock, enabled, desc = it
            update_item(chat_id, item_id, title, int(price), None, bool(enabled), desc)
            nit = get_item(chat_id, item_id)
            await safe_edit(q, item_detail_text(nit), reply_markup=kb_item_edit(item_id))
            return

        if data.startswith("v4:i:title:"):
            await q.answer()
            item_id = safe_int(data.split(":")[3], 0)
            context.user_data["await_item_title"] = item_id
            await safe_edit(q, f"请输入商品 ID{item_id} 新标题（1~40字）\n发送 /cancel 可取消")
            return

        if data.startswith("v4:i:desc:"):
            await q.answer()
            item_id = safe_int(data.split(":")[3], 0)
            context.user_data["await_item_desc"] = item_id
            await safe_edit(q,
                f"请输入商品 ID{item_id} 的描述（最多100字）\n"
                "发送「清空」可删除描述\n"
                "发送 /cancel 可取消")
            return

        await q.answer("未识别操作", show_alert=False)

    except Exception:
        logger.exception("cb handler crashed")
        try:
            await q.message.reply_text("⚠️ 按钮处理失败，请重试。")
        except Exception:
            pass

# =========================
# Message Handler
# =========================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    text = (update.message.text or "").strip()
    if not text:
        return

    chat = update.effective_chat
    user = update.effective_user
    chat_id = chat.id
    user_id = user.id

    # 群组初始化缓存
    if chat.type in ("group", "supergroup") and chat_id not in _initialized_chats:
        upsert_chat(chat_id, chat.title or str(chat_id))
        ensure_default_rules(chat_id)
        _initialized_chats.add(chat_id)

    # ===== 私聊输入流程 =====
    if chat.type == "private":

        if context.user_data.get("await_target_input"):
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            target = safe_int(text, 0)
            if target <= 0:
                await update.message.reply_text("用户ID格式错误，发送 /cancel 可取消")
                return
            context.user_data["adm_target"] = target
            context.user_data.pop("await_target_input", None)
            bal = wallet_get(v, target)
            await update.message.reply_text(
                f"✅ 已设置目标用户：{target}\n当前群余额：{milli_to_coin(bal)}",
                reply_markup=kb_adm_user())
            return

        if context.user_data.get("await_rule_name"):
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            rid = safe_int(context.user_data.get("await_rule_name"), 0)
            name = text[:32].strip()
            if not name:
                await update.message.reply_text("名称不能为空，发送 /cancel 可取消")
                return
            r = get_rule(v, rid)
            if not r:
                context.user_data.pop("await_rule_name", None)
                await update.message.reply_text("规则不存在")
                return
            _id, old_name, p, mn, mx, en, pr = r
            update_rule(v, rid, float(p), int(mn), int(mx), bool(en), name)
            context.user_data.pop("await_rule_name", None)
            await update.message.reply_text(f"✅ 规则 ID{rid} 名称已更新为：{name}")
            return

        if context.user_data.get("await_rule_prob") is not None:
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            rid = safe_int(context.user_data.get("await_rule_prob"), 0)
            # 解析概率：支持 "5%" 或 "0.05"
            raw_prob = text.strip().rstrip("%")
            prob_val = safe_float(raw_prob, -1)
            if prob_val < 0:
                await update.message.reply_text("格式错误，请输入如 0.05 或 5%，发送 /cancel 可取消")
                return
            # 如果输入的是百分比形式（>1 视为百分比）
            if "%" in text or prob_val > 1:
                prob_val = prob_val / 100.0
            prob_val = clamp(prob_val, 0.0, 1.0)
            r = get_rule(v, rid)
            if not r:
                context.user_data.pop("await_rule_prob", None)
                await update.message.reply_text("规则不存在")
                return
            _id, name, p, mn, mx, en, pr = r
            update_rule(v, rid, round(prob_val, 6), int(mn), int(mx), bool(en), name)
            context.user_data.pop("await_rule_prob", None)
            await update.message.reply_text(
                f"✅ 规则 ID{rid} 概率已更新为：{prob_val*100:.4f}%")
            return

        if context.user_data.get("await_rule_min") is not None:
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            rid = safe_int(context.user_data.get("await_rule_min"), 0)
            try:
                new_min = coin_to_milli(text.strip())
                if new_min <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("格式错误，请输入正数，发送 /cancel 可取消")
                return
            r = get_rule(v, rid)
            if not r:
                context.user_data.pop("await_rule_min", None)
                await update.message.reply_text("规则不存在")
                return
            _id, name, p, mn, mx, en, pr = r
            if new_min > int(mx):
                new_min = int(mx)
            update_rule(v, rid, float(p), new_min, int(mx), bool(en), name)
            context.user_data.pop("await_rule_min", None)
            await update.message.reply_text(f"✅ 规则 ID{rid} 最小值已更新为：{milli_to_coin(new_min)}")
            return

        if context.user_data.get("await_rule_max") is not None:
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            rid = safe_int(context.user_data.get("await_rule_max"), 0)
            try:
                new_max = coin_to_milli(text.strip())
                if new_max <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("格式错误，请输入正数，发送 /cancel 可取消")
                return
            r = get_rule(v, rid)
            if not r:
                context.user_data.pop("await_rule_max", None)
                await update.message.reply_text("规则不存在")
                return
            _id, name, p, mn, mx, en, pr = r
            new_max = max(int(mn), new_max)
            update_rule(v, rid, float(p), int(mn), new_max, bool(en), name)
            context.user_data.pop("await_rule_max", None)
            await update.message.reply_text(f"✅ 规则 ID{rid} 最大值已更新为：{milli_to_coin(new_max)}")
            return

        if context.user_data.get("await_add_item_input"):
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            parts = [x.strip() for x in text.split("|")]
            if len(parts) < 2:
                await update.message.reply_text("格式错误，请用：标题 | 价格 | 库存(可选) | 描述(可选)")
                return
            title = parts[0][:40].strip()
            if not title:
                await update.message.reply_text("标题不能为空")
                return
            try:
                price = coin_to_milli(parts[1])
                if price <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("价格格式错误或必须大于0")
                return
            stock = None
            if len(parts) >= 3 and parts[2]:
                try:
                    stock = int(parts[2])
                    if stock < 0:
                        await update.message.reply_text("库存不能小于0")
                        return
                except Exception:
                    await update.message.reply_text("库存必须是整数")
                    return
            description = parts[3][:100].strip() if len(parts) >= 4 else ""
            add_item(v, title, price, stock, description)
            context.user_data.pop("await_add_item_input", None)
            await update.message.reply_text(
                f"✅ 已添加商品：{title}｜{milli_to_coin(price)}｜库存{'∞' if stock is None else stock}"
                + (f"\n描述：{description}" if description else ""))
            return

        if context.user_data.get("await_item_title"):
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            item_id = safe_int(context.user_data.get("await_item_title"), 0)
            title = text[:40].strip()
            if not title:
                await update.message.reply_text("标题不能为空，发送 /cancel 可取消")
                return
            it = get_item(v, item_id)
            if not it:
                context.user_data.pop("await_item_title", None)
                await update.message.reply_text("商品不存在")
                return
            _id, old_title, price, stock, enabled, desc = it
            update_item(v, item_id, title, int(price), stock, bool(enabled), desc)
            context.user_data.pop("await_item_title", None)
            await update.message.reply_text(f"✅ 商品 ID{item_id} 标题已更新为：{title}")
            return

        if context.user_data.get("await_item_price") is not None:
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            item_id = safe_int(context.user_data.get("await_item_price"), 0)
            try:
                new_price = coin_to_milli(text.strip())
                if new_price <= 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("价格格式错误或必须大于0，发送 /cancel 可取消")
                return
            it = get_item(v, item_id)
            if not it:
                context.user_data.pop("await_item_price", None)
                await update.message.reply_text("商品不存在")
                return
            _id, title, price, stock, enabled, desc = it
            update_item(v, item_id, title, new_price, stock, bool(enabled), desc)
            context.user_data.pop("await_item_price", None)
            await update.message.reply_text(f"✅ 商品 ID{item_id} 价格已更新为：{milli_to_coin(new_price)}")
            return

        if context.user_data.get("await_item_stock") is not None:
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            item_id = safe_int(context.user_data.get("await_item_stock"), 0)
            try:
                new_stock = int(text.strip())
                if new_stock < 0:
                    raise ValueError
            except Exception:
                await update.message.reply_text("库存必须是非负整数，发送 /cancel 可取消")
                return
            it = get_item(v, item_id)
            if not it:
                context.user_data.pop("await_item_stock", None)
                await update.message.reply_text("商品不存在")
                return
            _id, title, price, stock, enabled, desc = it
            update_item(v, item_id, title, int(price), new_stock, bool(enabled), desc)
            context.user_data.pop("await_item_stock", None)
            await update.message.reply_text(f"✅ 商品 ID{item_id} 库存已更新为：{new_stock}")
            return

        if context.user_data.get("await_item_desc") is not None:
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            item_id = safe_int(context.user_data.get("await_item_desc"), 0)
            it = get_item(v, item_id)
            if not it:
                context.user_data.pop("await_item_desc", None)
                await update.message.reply_text("商品不存在")
                return
            _id, title, price, stock, enabled, old_desc = it
            new_desc = "" if text.strip() == "清空" else text[:100].strip()
            update_item(v, item_id, title, int(price), stock, bool(enabled), new_desc)
            context.user_data.pop("await_item_desc", None)
            await update.message.reply_text(
                f"✅ 商品 ID{item_id} 描述已{'清空' if not new_desc else f'更新为：{new_desc}'}")
            return

        if context.user_data.get("await_rank_keywords"):
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            kw = text.strip()[:200]
            if not kw:
                await update.message.reply_text("关键词不能为空，发送 /cancel 可取消")
                return
            set_chat_setting(v, "rank_keywords", kw)
            context.user_data.pop("await_rank_keywords", None)
            await update.message.reply_text(f"✅ 排行榜关键词已更新为：{kw}")
            return


        if context.user_data.get("await_redeem_notice"):
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            new_notice = "" if text.strip() == "清空" else text.strip()[:300]
            set_chat_setting(v, "redeem_notice", new_notice)
            context.user_data.pop("await_redeem_notice", None)
            await update.message.reply_text(
                f"✅ 兑换通知文本已{'清空' if not new_notice else f'更新为：\n{new_notice}'}")
            return

    # ===== 群组消息处理 =====
    if chat.type in ("group", "supergroup") and not user.is_bot:

        # 商城关键词
        if text in ("商城", "商店", "兑换"):
            bot_msg = await update.message.reply_text(
                user_panel_text(chat_id, user_id),
                reply_markup=kb_user_panel()
            )
            # 记录消息归属，用于翻页鉴权
            context.user_data[f"shop_owner_{bot_msg.message_id}"] = user_id
            try:
                await auto_delete_pair(
                    context=context,
                    chat_id=chat_id,
                    trigger_mid=update.message.message_id,
                    bot_mid=bot_msg.message_id,
                    delay=60
                )
            except Exception:
                logger.exception("auto_delete_pair failed")
            return

        # 排行榜关键词（从群组设置读取）
        settings = get_chat_settings(chat_id)
        rank_kw_list = [k.strip() for k in settings.get("rank_keywords", "排行榜,排名,积分榜").split(",") if k.strip()]
        if text in rank_kw_list:
            txt, max_page = rank_text(chat_id, 0)
            total = get_wallet_rank_count(chat_id)
            mp = max(0, (total - 1) // RANK_SIZE) if total > 0 else 0
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️", callback_data="v4:pub_rank:0"),
                 InlineKeyboardButton(f"第1/{mp+1}页", callback_data="v4:noop"),
                 InlineKeyboardButton("➡️", callback_data=f"v4:pub_rank:1")]
            ]) if mp > 0 else None
            bot_msg = await update.message.reply_text(txt, reply_markup=kb)
            try:
                await auto_delete_pair(
                    context=context,
                    chat_id=chat_id,
                    trigger_mid=update.message.message_id,
                    bot_mid=bot_msg.message_id,
                    delay=120
                )
            except Exception:
                logger.exception("auto_delete_pair rank failed")
            return

        # 命令跳过
        if text.startswith("/"):
            return

        # 基础文本校验
        if not valid_text_basic(text, MIN_TEXT_LEN):
            return

        # 风控
        if not can_reward(chat_id, user_id, text):
            return

        # 红包抽奖
        rules = list_rules(chat_id, 100, 0)
        total_amt = 0
        hits = []
        for rid, name, p, mn, mx, en, pr in rules:
            if not en:
                continue
            if random.random() < float(p):
                amt = random.randint(int(mn), int(mx))
                total_amt += amt
                hits.append((name, amt))

        if total_amt <= 0:
            return

        # 每日上限截断
        got = int(rds.get(f"daily:{chat_id}:{user_id}") or 0)
        allow = max(0, DAILY_MAX_MILLI - got)
        grant = min(total_amt, allow)
        if grant <= 0:
            return

        # 写入钱包 + 日志
        hit_names = "+".join([n for n, _ in hits[:3]])
        reason = f"drop:{hit_names}" if hit_names else "drop"
        wallet_add(chat_id, user_id, grant, reason)
        add_daily(chat_id, user_id, grant)

        # 构建回复
        if hits:
            top_name, _ = max(hits, key=lambda x: x[1])
        else:
            top_name = "神秘红包"

        flair = "🎉"
        if "锦鲤" in top_name:
            flair = "🐉✨"
        elif "惊喜" in top_name:
            flair = "🎊💥"
        elif "普通" in top_name:
            flair = "🧧"

        detail = " + ".join([f"{n}:{milli_to_coin(a)}" for n, a in hits[:3]])
        if len(hits) > 3:
            detail += " + ..."

        await update.message.reply_text(
            f"{flair} 恭喜 {user.first_name} 中奖！\n"
            f"🏆 命中档次：{top_name}\n"
            f"💰 本次获得：{milli_to_coin(grant)} 金币\n"
            f"📦 命中明细：{detail if detail else '-'}"
        )


# 群内公开排行榜翻页回调（无需管理员权限）
async def cb_pub_rank(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.message:
        return
    await q.answer()
    data = q.data or ""
    chat_id = q.message.chat_id
    page = max(0, safe_int(data.split(":")[2], 0))
    txt, max_page = rank_text(chat_id, page)
    page = clamp(page, 0, max_page)
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️", callback_data=f"v4:pub_rank:{max(0,page-1)}"),
         InlineKeyboardButton(f"第{page+1}/{max_page+1}页", callback_data="v4:noop"),
         InlineKeyboardButton("➡️", callback_data=f"v4:pub_rank:{min(max_page,page+1)}")]
    ])
    try:
        await q.edit_message_text(text=txt, reply_markup=kb)
    except BadRequest as e:
        if "Message is not modified" not in str(e):
            logger.exception("cb_pub_rank edit failed")


# =========================
# Main
# =========================
def main():
    init_db()
    migrate_db()
    logger.info("ROOT_ADMIN_IDS loaded: %s", sorted(list(ROOT_ADMIN_IDS)))

    app = ApplicationBuilder().token(BOT_TOKEN).post_init(post_init).build()

    # 命令
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("myid", myid_cmd))
    app.add_handler(CommandHandler("whoami", whoami_cmd))
    app.add_handler(CommandHandler("cancel", cancel_cmd))
    app.add_handler(CommandHandler("buy", buy_cmd))
    app.add_handler(CommandHandler("additem", additem_cmd))
    app.add_handler(CommandHandler("bind_admin", bind_admin_cmd))
    app.add_handler(CommandHandler("unbind_admin", unbind_admin_cmd))

    # 回调
    app.add_handler(CallbackQueryHandler(cb_pub_rank, pattern=r"^v4:pub_rank:"))
    app.add_handler(CallbackQueryHandler(cb, pattern=r"^v4:"))

    # 消息
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(on_error)

    try:
        if WEBHOOK_URL:
            full_url = WEBHOOK_URL.rstrip("/") + WEBHOOK_PATH
            logger.info("Webhook mode: %s", full_url)
            app.run_webhook(
                listen="0.0.0.0",
                port=PORT,
                webhook_url=full_url,
                url_path=WEBHOOK_PATH.lstrip("/")
            )
        else:
            logger.warning("WEBHOOK_URL 未设置，回退 polling 模式")
            app.run_polling()
    finally:
        try:
            pg_pool.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
