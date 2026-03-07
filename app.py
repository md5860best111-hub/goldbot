import os
import re
import random
import logging
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlparse
from functools import wraps
from datetime import datetime, timezone

from psycopg_pool import ConnectionPool
import redis

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
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

# 超级维护员（可跨群授权管理员），逗号分隔
# 例如：ROOT_ADMIN_IDS=123456,987654
ROOT_ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ROOT_ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

# 默认风控
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

def clamp(v, lo, hi):
    return max(lo, min(hi, v))

def now_utc_date_str():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def valid_text_basic(text: str, min_len: int) -> bool:
    if not text:
        return False
    t = text.strip()
    if len(t) < min_len:
        return False
    if t.isdigit():
        return False
    # 过滤纯符号/纯表情（简单版）
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
        # 群信息
        cur.execute("""
        CREATE TABLE IF NOT EXISTS chats (
          chat_id BIGINT PRIMARY KEY,
          title TEXT NOT NULL DEFAULT '',
          active BOOLEAN NOT NULL DEFAULT TRUE,
          created_at TIMESTAMP NOT NULL DEFAULT NOW(),
          updated_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)

        # 按群管理员（核心）
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

        cur.execute("CREATE INDEX IF NOT EXISTS idx_chat_admins_user ON chat_admins(user_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_chat ON drop_rules(chat_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_shop_chat ON shop_items(chat_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_logs_chat ON coin_logs(chat_id, id DESC);")

# =========================
# Chat/Admin Access
# =========================
@with_conn
def upsert_chat(conn, chat_id: int, title: str):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO chats(chat_id, title, active, created_at, updated_at)
        VALUES(%s,%s,TRUE,NOW(),NOW())
        ON CONFLICT(chat_id)
        DO UPDATE SET title=EXCLUDED.title, updated_at=NOW(), active=TRUE
        """, (chat_id, title or ""))

@with_conn
def add_chat_admin(conn, chat_id: int, user_id: int, role: str = "admin"):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO chat_admins(chat_id, user_id, role)
        VALUES(%s,%s,%s)
        ON CONFLICT(chat_id, user_id) DO NOTHING
        """, (chat_id, user_id, role))

@with_conn
def del_chat_admin(conn, chat_id: int, user_id: int):
    with conn.cursor() as cur:
        # 至少保留1个管理员
        cur.execute("SELECT COUNT(*) FROM chat_admins WHERE chat_id=%s", (chat_id,))
        c = cur.fetchone()[0]
        cur.execute("SELECT 1 FROM chat_admins WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        e = cur.fetchone()
        if not e:
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
        SELECT c.chat_id, c.title
        FROM chat_admins a
        JOIN chats c ON c.chat_id=a.chat_id
        WHERE a.user_id=%s AND c.active=TRUE
        ORDER BY c.updated_at DESC
        """, (user_id,))
        return cur.fetchall()

@with_conn
def list_chat_admins(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT user_id, role, created_at
        FROM chat_admins
        WHERE chat_id=%s
        ORDER BY created_at ASC
        """, (chat_id,))
        return cur.fetchall()

# =========================
# Business DB
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
        FROM drop_rules
        WHERE chat_id=%s
        ORDER BY priority ASC, id ASC
        LIMIT %s OFFSET %s
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
        UPDATE drop_rules
        SET probability=%s,min_milli=%s,max_milli=%s,enabled=%s,name=%s
        WHERE chat_id=%s AND id=%s
        """, (p, mn, mx, en, name, chat_id, rid))

@with_conn
def wallet_get(conn, chat_id: int, user_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT balance_milli FROM wallets WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        r = cur.fetchone()
        return int(r[0]) if r else 0

@with_conn
def wallet_add(conn, chat_id: int, user_id: int, delta: int):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
        VALUES(%s,%s,%s,NOW())
        ON CONFLICT(chat_id,user_id)
        DO UPDATE SET balance_milli=wallets.balance_milli+EXCLUDED.balance_milli, updated_at=NOW()
        """, (chat_id, user_id, delta))

@with_conn
def wallet_adjust_admin(conn, chat_id: int, operator_id: int, user_id: int, delta: int, reason: str):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT balance_milli FROM wallets
        WHERE chat_id=%s AND user_id=%s
        FOR UPDATE
        """, (chat_id, user_id))
        row = cur.fetchone()
        old = int(row[0]) if row else 0
        new = old + delta
        if new < 0:
            return False, f"余额不足，当前 {milli_to_coin(old)}"

        if row:
            cur.execute("""
            UPDATE wallets SET balance_milli=%s, updated_at=NOW()
            WHERE chat_id=%s AND user_id=%s
            """, (new, chat_id, user_id))
        else:
            cur.execute("""
            INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
            VALUES(%s,%s,%s,NOW())
            """, (chat_id, user_id, new))

        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason)
        VALUES(%s,%s,%s,%s,%s)
        """, (chat_id, operator_id, user_id, delta, reason or "panel_adjust"))

        return True, f"成功：用户 {user_id} 变动 {milli_to_coin(delta)}，新余额 {milli_to_coin(new)}"

@with_conn
def coin_logs_page(conn, chat_id: int, limit=8, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,operator_id,user_id,delta_milli,reason,created_at
        FROM coin_logs
        WHERE chat_id=%s
        ORDER BY id DESC
        LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def shop_page(conn, chat_id: int, limit=6, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,title,price_milli,stock,enabled
        FROM shop_items
        WHERE chat_id=%s
        ORDER BY id DESC
        LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def get_item(conn, chat_id: int, item_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,title,price_milli,stock,enabled
        FROM shop_items
        WHERE chat_id=%s AND id=%s
        """, (chat_id, item_id))
        return cur.fetchone()

@with_conn
def add_item(conn, chat_id: int, title: str, price_milli: int, stock):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO shop_items(chat_id,title,price_milli,enabled,stock)
        VALUES(%s,%s,%s,TRUE,%s)
        """, (chat_id, title, price_milli, stock))

@with_conn
def update_item(conn, chat_id: int, item_id: int, title: str, price_milli: int, stock, enabled: bool):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE shop_items
        SET title=%s,price_milli=%s,stock=%s,enabled=%s
        WHERE chat_id=%s AND id=%s
        """, (title, price_milli, stock, enabled, chat_id, item_id))

@with_conn
def buy_item_atomic(conn, chat_id: int, user_id: int, item_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,title,price_milli,enabled,stock
        FROM shop_items
        WHERE chat_id=%s AND id=%s
        FOR UPDATE
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
        SELECT balance_milli FROM wallets
        WHERE chat_id=%s AND user_id=%s
        FOR UPDATE
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
        return True, f"购买成功：{title}，扣除 {milli_to_coin(price)} 金币"

# =========================
# Redis风控
# =========================
def can_reward(chat_id: int, user_id: int, text: str) -> bool:
    # 每分钟计数限制
    k_pm = f"pm:{chat_id}:{user_id}"
    n = rds.incr(k_pm)
    if n == 1:
        rds.expire(k_pm, 60)
    if n > PER_MINUTE_CAP:
        return False

    # 冷却
    k_cd = f"cd:{chat_id}:{user_id}"
    if rds.exists(k_cd):
        return False
    rds.setex(k_cd, COOLDOWN_SECONDS, "1")

    # 重复文本拦截
    if ENABLE_SAME_TEXT_BLOCK:
        t = (text or "").strip().lower()
        if t:
            k_lt = f"lt:{chat_id}:{user_id}"
            old = rds.get(k_lt)
            if old == t:
                return False
            rds.setex(k_lt, 120, t)

    # 日上限
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
# UI
# =========================
SHOP_SIZE = 6
RULE_SIZE = 6
LOG_SIZE = 8
STEP_OPTIONS = [100, 500, 1000, 5000, 10000]

def fmt_rule_row(r):
    rid, name, p, mn, mx, en, pr = r
    return f"{'✅' if en else '❌'} {name}｜{p*100:.3f}%｜{milli_to_coin(mn)}~{milli_to_coin(mx)}"

def kb_home():
    rows = [
        [InlineKeyboardButton("💰 我的金币", callback_data="v3:me"),
         InlineKeyboardButton("🛒 商店", callback_data="v3:shop:0")],
        [InlineKeyboardButton("🎯 掉落规则", callback_data="v3:rules_read:0")],
        [InlineKeyboardButton("🗂️ 选择管理群组", callback_data="v3:groups:0")],
        [InlineKeyboardButton("⚙️ 当前群管理面板", callback_data="v3:admin_home")]
    ]
    return InlineKeyboardMarkup(rows)

def kb_groups(user_id: int, page: int):
    rows = []
    chats = list_user_admin_chats(user_id)
    start = page * 8
    part = chats[start:start+8]
    for cid, title in part:
        rows.append([InlineKeyboardButton(f"{title or cid}", callback_data=f"v3:selgroup:{cid}")])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v3:groups:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}页", callback_data="v3:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v3:groups:{page+1}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回", callback_data="v3:home")])
    return InlineKeyboardMarkup(rows)

def kb_admin_home():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 金币管理", callback_data="v3:adm_user"),
         InlineKeyboardButton("🧾 操作日志", callback_data="v3:logs:0")],
        [InlineKeyboardButton("⚙️ 规则管理", callback_data="v3:adm_rules:0"),
         InlineKeyboardButton("🎁 商品管理", callback_data="v3:adm_shop:0")],
        [InlineKeyboardButton("🛡️ 群管理员", callback_data="v3:adm_list")],
        [InlineKeyboardButton("🔙 返回", callback_data="v3:home")]
    ])

def kb_shop(chat_id: int, page: int):
    items = shop_page(chat_id, SHOP_SIZE, page * SHOP_SIZE)
    rows = []
    for item_id, title, price, stock, enabled in items:
        if not enabled:
            continue
        rows.append([InlineKeyboardButton(
            f"{title[:14]}｜{milli_to_coin(price)}｜库存{'∞' if stock is None else stock}",
            callback_data=f"v3:buy:{item_id}"
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v3:shop:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}页", callback_data="v3:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v3:shop:{page+1}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回", callback_data="v3:home")])
    return InlineKeyboardMarkup(rows)

def kb_rules_read(chat_id: int, page: int):
    rs = list_rules(chat_id, RULE_SIZE, page * RULE_SIZE)
    rows = []
    for r in rs:
        rid = r[0]
        rows.append([InlineKeyboardButton(fmt_rule_row(r), callback_data=f"v3:rule_view:{rid}")])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v3:rules_read:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}页", callback_data="v3:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v3:rules_read:{page+1}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回", callback_data="v3:home")])
    return InlineKeyboardMarkup(rows)

def kb_adm_user():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("选择目标用户", callback_data="v3:adm_target"),
         InlineKeyboardButton("💱切换步进", callback_data="v3:adm_step")],
        [InlineKeyboardButton("➕加金币", callback_data="v3:adm_add"),
         InlineKeyboardButton("➖扣金币", callback_data="v3:adm_sub")],
        [InlineKeyboardButton("📦查余额", callback_data="v3:adm_qbal")],
        [InlineKeyboardButton("🔙 返回", callback_data="v3:admin_home")]
    ])

def kb_adm_rules(chat_id: int, page: int):
    rs = list_rules(chat_id, RULE_SIZE, page * RULE_SIZE)
    rows = []
    for r in rs:
        rid = r[0]
        rows.append([InlineKeyboardButton(fmt_rule_row(r), callback_data=f"v3:adm_rule:{rid}")])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v3:adm_rules:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}页", callback_data="v3:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v3:adm_rules:{page+1}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回", callback_data="v3:admin_home")])
    return InlineKeyboardMarkup(rows)

def kb_rule_edit(rid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️改名称", callback_data=f"v3:r:name:{rid}"),
         InlineKeyboardButton("🔁开关切换", callback_data=f"v3:r:toggle:{rid}")],
        [InlineKeyboardButton("概率 -0.1%", callback_data=f"v3:r:p:-0.001:{rid}"),
         InlineKeyboardButton("概率 +0.1%", callback_data=f"v3:r:p:+0.001:{rid}")],
        [InlineKeyboardButton("最小 -0.1", callback_data=f"v3:r:min:-100:{rid}"),
         InlineKeyboardButton("最小 +0.1", callback_data=f"v3:r:min:+100:{rid}")],
        [InlineKeyboardButton("最大 -0.1", callback_data=f"v3:r:max:-100:{rid}"),
         InlineKeyboardButton("最大 +0.1", callback_data=f"v3:r:max:+100:{rid}")],
        [InlineKeyboardButton("🔙 返回规则列表", callback_data="v3:adm_rules:0")]
    ])

def kb_logs(page: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️", callback_data=f"v3:logs:{max(0,page-1)}"),
         InlineKeyboardButton(f"第{page+1}页", callback_data="v3:noop"),
         InlineKeyboardButton("➡️", callback_data=f"v3:logs:{page+1}")],
        [InlineKeyboardButton("🔙 返回", callback_data="v3:admin_home")]
    ])

def kb_adm_shop(chat_id: int, page: int):
    items = shop_page(chat_id, SHOP_SIZE, page * SHOP_SIZE)
    rows = []
    for item_id, title, price, stock, enabled in items:
        rows.append([InlineKeyboardButton(
            f"{'✅' if enabled else '❌'} ID{item_id} {title[:10]}｜{milli_to_coin(price)}｜库存{'∞' if stock is None else stock}",
            callback_data=f"v3:adm_item:{item_id}"
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"v3:adm_shop:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}页", callback_data="v3:noop"),
        InlineKeyboardButton("➡️", callback_data=f"v3:adm_shop:{page+1}")
    ])
    rows.append([InlineKeyboardButton("➕新增商品（/additem 标题|价格|库存）", callback_data="v3:noop")])
    rows.append([InlineKeyboardButton("🔙 返回", callback_data="v3:admin_home")])
    return InlineKeyboardMarkup(rows)

def kb_item_edit(item_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔁上/下架", callback_data=f"v3:i:toggle:{item_id}")],
        [InlineKeyboardButton("价格 -0.5", callback_data=f"v3:i:price:-500:{item_id}"),
         InlineKeyboardButton("价格 +0.5", callback_data=f"v3:i:price:+500:{item_id}")],
        [InlineKeyboardButton("库存 -1", callback_data=f"v3:i:stock:-1:{item_id}"),
         InlineKeyboardButton("库存 +1", callback_data=f"v3:i:stock:+1:{item_id}")],
        [InlineKeyboardButton("库存设∞", callback_data=f"v3:i:stockinf:{item_id}"),
         InlineKeyboardButton("✏️改标题", callback_data=f"v3:i:title:{item_id}")],
        [InlineKeyboardButton("🔙 返回商品列表", callback_data="v3:adm_shop:0")]
    ])

def selected_chat_id(context: ContextTypes.DEFAULT_TYPE):
    return safe_int(context.user_data.get("sel_chat_id"), 0)

def ensure_admin_selected_chat(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    cid = selected_chat_id(context)
    if cid <= 0:
        return False, "请先在“选择管理群组”里选一个群"
    if not is_chat_admin(cid, user_id):
        return False, "你不在该群的管理员列表中"
    return True, cid

# =========================
# Commands
# =========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    uid = update.effective_user.id
    chats = list_user_admin_chats(uid)
    if chats and selected_chat_id(context) <= 0:
        context.user_data["sel_chat_id"] = chats[0][0]
        context.user_data["sel_chat_title"] = chats[0][1]
    await update.message.reply_text(
        "📋 控制面板\n说明：请先点击“选择管理群组”，后续管理数据仅限该群。",
        reply_markup=kb_home()
    )

async def additem_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 私聊管理员执行，作用于已选择群
    if not update.message or not update.effective_user:
        return
    uid = update.effective_user.id
    ok, cid_or_msg = ensure_admin_selected_chat(uid, context)
    if not ok:
        await update.message.reply_text(cid_or_msg)
        return
    chat_id = cid_or_msg

    raw = update.message.text.replace("/additem", "", 1).strip()
    parts = [x.strip() for x in raw.split("|")]
    if len(parts) < 2:
        await update.message.reply_text("用法：/additem 标题 | 价格 | 库存(可选)")
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
        except Exception:
            await update.message.reply_text("库存必须是整数")
            return

    add_item(chat_id, title, price, stock)
    await update.message.reply_text(f"已添加商品到当前群：{title}")

async def buy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 用户在群里用 /buy（用当前聊天群）
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

async def bind_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # 在群内执行：/bind_admin user_id
    # 权限：ROOT_ADMIN_IDS 或 已是该群管理员
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
    allowed = is_root_admin(operator) or is_chat_admin(chat_id, operator)
    if not allowed:
        await update.message.reply_text("无权限：仅根管理员或本群管理员可授权")
        return

    if not context.args:
        await update.message.reply_text("用法：/bind_admin 用户ID")
        return
    target = safe_int(context.args[0], 0)
    if target <= 0:
        await update.message.reply_text("用户ID错误")
        return
    add_chat_admin(chat_id, target, "admin")
    await update.message.reply_text(f"已授权 {target} 为本群管理员")

async def unbind_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type not in ("group", "supergroup"):
        await update.message.reply_text("请在目标群里使用")
        return
    chat_id = update.effective_chat.id
    operator = update.effective_user.id
    allowed = is_root_admin(operator) or is_chat_admin(chat_id, operator)
    if not allowed:
        await update.message.reply_text("无权限：仅根管理员或本群管理员可移除")
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

# =========================
# Callback
# =========================
async def cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.message or not update.effective_user:
        return
    await q.answer()
    data = q.data or ""
    uid = update.effective_user.id

    if data == "v3:noop":
        return
    if data == "v3:home":
        await q.edit_message_text(
            "📋 控制面板\n说明：请先点击“选择管理群组”，后续管理数据仅限该群。",
            reply_markup=kb_home()
        )
        return

    # 用户功能（在私聊里需依赖选中群；在群里则用当前群）
    if data == "v3:me":
        # 优先当前聊天群（若是群），否则用已选群
        chat_id = q.message.chat_id if q.message.chat.type in ("group", "supergroup") else selected_chat_id(context)
        if chat_id <= 0:
            await q.edit_message_text("请先选择管理群组后再查看（或在群里打开）", reply_markup=kb_home())
            return
        bal = wallet_get(chat_id, uid)
        got = int(rds.get(f"daily:{chat_id}:{uid}") or 0)
        await q.edit_message_text(
            f"💰 你的余额：{milli_to_coin(bal)}\n今日已获得：{milli_to_coin(got)} / {milli_to_coin(DAILY_MAX_MILLI)}",
            reply_markup=kb_home()
        )
        return

    if data.startswith("v3:shop:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        chat_id = q.message.chat_id if q.message.chat.type in ("group", "supergroup") else selected_chat_id(context)
        if chat_id <= 0:
            await q.edit_message_text("请先选择管理群组（或在群里打开）", reply_markup=kb_home())
            return
        await q.edit_message_text("🛒 商店（点击购买）", reply_markup=kb_shop(chat_id, page))
        return

    if data.startswith("v3:buy:"):
        item_id = safe_int(data.split(":")[2], 0)
        chat_id = q.message.chat_id if q.message.chat.type in ("group", "supergroup") else selected_chat_id(context)
        if chat_id <= 0:
            await q.answer("请先选择管理群组", show_alert=True)
            return
        ok, msg = buy_item_atomic(chat_id, uid, item_id)
        await q.answer(msg[:180], show_alert=True)
        return

    if data.startswith("v3:rules_read:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        chat_id = q.message.chat_id if q.message.chat.type in ("group", "supergroup") else selected_chat_id(context)
        if chat_id <= 0:
            await q.edit_message_text("请先选择管理群组（或在群里打开）", reply_markup=kb_home())
            return
        await q.edit_message_text("🎯 掉落规则（中文说明）", reply_markup=kb_rules_read(chat_id, page))
        return

    if data.startswith("v3:rule_view:"):
        rid = safe_int(data.split(":")[2], 0)
        chat_id = q.message.chat_id if q.message.chat.type in ("group", "supergroup") else selected_chat_id(context)
        if chat_id <= 0:
            await q.answer("请先选择群组", show_alert=True)
            return
        r = get_rule(chat_id, rid)
        if not r:
            await q.answer("规则不存在", show_alert=True)
            return
        _id, name, p, mn, mx, en, pr = r
        txt = (
            f"🎯 规则详情\n"
            f"名称：{name}\n"
            f"概率：{p*100:.3f}%\n"
            f"奖励区间：{milli_to_coin(mn)} ~ {milli_to_coin(mx)} 金币\n"
            f"状态：{'开启' if en else '关闭'}"
        )
        await q.edit_message_text(txt, reply_markup=kb_rules_read(chat_id, 0))
        return

    # 管理员一级：群组选择
    if data.startswith("v3:groups:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        await q.edit_message_text(
            "🗂️ 请选择管理群组（仅显示你有权限的群）\n"
            "如果列表为空，请先在目标群执行 /bind_admin 你的ID",
            reply_markup=kb_groups(uid, page)
        )
        return

    if data.startswith("v3:selgroup:"):
        cid = safe_int(data.split(":")[2], 0)
        if cid <= 0 or not is_chat_admin(cid, uid):
            await q.answer("无权限选择该群", show_alert=True)
            return
        # 保存会话中的当前群
        context.user_data["sel_chat_id"] = cid
        # 取标题
        chats = {c[0]: c[1] for c in list_user_admin_chats(uid)}
        context.user_data["sel_chat_title"] = chats.get(cid, str(cid))
        ensure_default_rules(cid)
        await q.edit_message_text(
            f"✅ 已切换管理群：{context.user_data['sel_chat_title']}\n"
            f"后续管理数据均为该群，互不相通。",
            reply_markup=kb_admin_home()
        )
        return

    if data == "v3:admin_home":
        ok, v = ensure_admin_selected_chat(uid, context)
        if not ok:
            await q.edit_message_text(
                f"⚠️ {v}\n请先点击“选择管理群组”。",
                reply_markup=kb_home()
            )
            return
        cid = v
        title = context.user_data.get("sel_chat_title", str(cid))
        await q.edit_message_text(
            f"⚙️ 当前管理群：{title}\n请选择管理功能：",
            reply_markup=kb_admin_home()
        )
        return

    # 以下管理功能都走 selected_chat_id 严格隔离
    ok, v = ensure_admin_selected_chat(uid, context)
    if not ok:
        await q.answer(v, show_alert=True)
        return
    chat_id = v

    if data == "v3:adm_list":
        rows = list_chat_admins(chat_id)
        txt = "🛡️ 本群管理员列表：\n" + "\n".join([f"- {x[0]} ({x[1]})" for x in rows]) if rows else "暂无管理员"
        await q.edit_message_text(txt, reply_markup=kb_admin_home())
        return

    if data == "v3:adm_user":
        context.user_data.setdefault("adm_target", uid)
        context.user_data.setdefault("adm_step", 1000)
        t = context.user_data["adm_target"]
        s = context.user_data["adm_step"]
        bal = wallet_get(chat_id, t)
        await q.edit_message_text(
            f"👤 金币管理（仅当前群）\n目标用户：{t}\n步进：{milli_to_coin(s)}\n目标余额：{milli_to_coin(bal)}",
            reply_markup=kb_adm_user()
        )
        return

    if data == "v3:adm_target":
        context.user_data["await_target_input"] = True
        await q.edit_message_text("请输入目标用户ID（纯数字）")
        return

    if data == "v3:adm_step":
        cur = context.user_data.get("adm_step", 1000)
        idx = STEP_OPTIONS.index(cur) if cur in STEP_OPTIONS else 2
        idx = (idx + 1) % len(STEP_OPTIONS)
        context.user_data["adm_step"] = STEP_OPTIONS[idx]
        t = context.user_data.get("adm_target", uid)
        bal = wallet_get(chat_id, t)
        await q.edit_message_text(
            f"👤 金币管理（仅当前群）\n目标用户：{t}\n步进：{milli_to_coin(context.user_data['adm_step'])}\n目标余额：{milli_to_coin(bal)}",
            reply_markup=kb_adm_user()
        )
        return

    if data == "v3:adm_qbal":
        t = context.user_data.get("adm_target", uid)
        bal = wallet_get(chat_id, t)
        await q.answer(f"用户 {t} 当前群余额：{milli_to_coin(bal)}", show_alert=True)
        return

    if data in ("v3:adm_add", "v3:adm_sub"):
        t = context.user_data.get("adm_target", uid)
        s = context.user_data.get("adm_step", 1000)
        delta = s if data == "v3:adm_add" else -s
        ok2, msg = wallet_adjust_admin(chat_id, uid, t, delta, "panel_adjust")
        await q.answer(msg[:180], show_alert=True)
        bal = wallet_get(chat_id, t)
        await q.edit_message_text(
            f"👤 金币管理（仅当前群）\n目标用户：{t}\n步进：{milli_to_coin(s)}\n目标余额：{milli_to_coin(bal)}",
            reply_markup=kb_adm_user()
        )
        return

    if data.startswith("v3:logs:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        rows = coin_logs_page(chat_id, LOG_SIZE, page * LOG_SIZE)
        if not rows:
            txt = "🧾 本群暂无管理员操作日志"
        else:
            lines = ["🧾 本群管理员操作日志："]
            for lid, op, u, d, reason, ct in rows:
                sign = "+" if d >= 0 else ""
                lines.append(f"#{lid} | {op}->{u} | {sign}{milli_to_coin(d)} | {reason}")
            txt = "\n".join(lines)
        await q.edit_message_text(txt[:3900], reply_markup=kb_logs(page))
        return

    if data.startswith("v3:adm_rules:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        ensure_default_rules(chat_id)
        await q.edit_message_text(
            "⚙️ 规则管理（仅当前群）\n说明：概率与区间只影响当前群。",
            reply_markup=kb_adm_rules(chat_id, page)
        )
        return

    if data.startswith("v3:adm_rule:"):
        rid = safe_int(data.split(":")[2], 0)
        r = get_rule(chat_id, rid)
        if not r:
            await q.answer("规则不存在", show_alert=True)
            return
        _id, name, p, mn, mx, en, pr = r
        txt = (
            f"⚙️ 编辑规则（当前群）\n"
            f"名称：{name}\n概率：{p*100:.3f}%\n"
            f"最小：{milli_to_coin(mn)}\n最大：{milli_to_coin(mx)}\n"
            f"状态：{'开启' if en else '关闭'}"
        )
        await q.edit_message_text(txt, reply_markup=kb_rule_edit(rid))
        return

    if data.startswith("v3:r:name:"):
        rid = safe_int(data.split(":")[3], 0)
        context.user_data["await_rule_name"] = rid
        await q.edit_message_text(f"请输入规则 ID{rid} 的新中文名称（1~32字）")
        return

    if data.startswith("v3:r:toggle:"):
        rid = safe_int(data.split(":")[3], 0)
        r = get_rule(chat_id, rid)
        if not r:
            await q.answer("规则不存在", show_alert=True)
            return
        _id, name, p, mn, mx, en, pr = r
        update_rule(chat_id, rid, float(p), int(mn), int(mx), not bool(en), name)
        nr = get_rule(chat_id, rid)
        _id, name, p, mn, mx, en, pr = nr
        txt = f"名称：{name}\n概率：{p*100:.3f}%\n最小：{milli_to_coin(mn)}\n最大：{milli_to_coin(mx)}\n状态：{'开启' if en else '关闭'}"
        await q.edit_message_text(txt, reply_markup=kb_rule_edit(rid))
        return

    if data.startswith("v3:r:p:") or data.startswith("v3:r:min:") or data.startswith("v3:r:max:"):
        # v3:r:{field}:{delta}:{rid}
        _, _, field, delta_s, rid_s = data.split(":")
        rid = safe_int(rid_s, 0)
        r = get_rule(chat_id, rid)
        if not r:
            await q.answer("规则不存在", show_alert=True)
            return
        _id, name, p, mn, mx, en, pr = r
        np, nmn, nmx = float(p), int(mn), int(mx)

        if field == "p":
            np = round(clamp(np + float(delta_s), 0.0, 1.0), 6)
        elif field == "min":
            nmn = max(1, nmn + safe_int(delta_s, 0))
            if nmn > nmx:
                nmn = nmx
        elif field == "max":
            nmx = max(nmn, nmx + safe_int(delta_s, 0))

        update_rule(chat_id, rid, np, nmn, nmx, bool(en), name)
        nr = get_rule(chat_id, rid)
        _id, name, p, mn, mx, en, pr = nr
        txt = f"名称：{name}\n概率：{p*100:.3f}%\n最小：{milli_to_coin(mn)}\n最大：{milli_to_coin(mx)}\n状态：{'开启' if en else '关闭'}"
        await q.edit_message_text(txt, reply_markup=kb_rule_edit(rid))
        return

    if data.startswith("v3:adm_shop:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        await q.edit_message_text("🎁 商品管理（仅当前群）", reply_markup=kb_adm_shop(chat_id, page))
        return

    if data.startswith("v3:adm_item:"):
        item_id = safe_int(data.split(":")[2], 0)
        it = get_item(chat_id, item_id)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        txt = (
            f"🎁 编辑商品（当前群）\n"
            f"标题：{title}\n价格：{milli_to_coin(price)}\n"
            f"库存：{'∞' if stock is None else stock}\n"
            f"状态：{'上架' if enabled else '下架'}"
        )
        await q.edit_message_text(txt, reply_markup=kb_item_edit(item_id))
        return

    if data.startswith("v3:i:toggle:"):
        item_id = safe_int(data.split(":")[3], 0)
        it = get_item(chat_id, item_id)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        update_item(chat_id, item_id, title, int(price), stock, not bool(enabled))
        nit = get_item(chat_id, item_id)
        _id, title, price, stock, enabled = nit
        txt = f"标题：{title}\n价格：{milli_to_coin(price)}\n库存：{'∞' if stock is None else stock}\n状态：{'上架' if enabled else '下架'}"
        await q.edit_message_text(txt, reply_markup=kb_item_edit(item_id))
        return

    if data.startswith("v3:i:price:"):
        _, _, _, delta_s, item_id_s = data.split(":")
        item_id = safe_int(item_id_s, 0)
        it = get_item(chat_id, item_id)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        nprice = max(1, int(price) + safe_int(delta_s, 0))
        update_item(chat_id, item_id, title, nprice, stock, bool(enabled))
        nit = get_item(chat_id, item_id)
        _id, title, price, stock, enabled = nit
        txt = f"标题：{title}\n价格：{milli_to_coin(price)}\n库存：{'∞' if stock is None else stock}\n状态：{'上架' if enabled else '下架'}"
        await q.edit_message_text(txt, reply_markup=kb_item_edit(item_id))
        return

    if data.startswith("v3:i:stock:"):
        _, _, _, delta_s, item_id_s = data.split(":")
        item_id = safe_int(item_id_s, 0)
        it = get_item(chat_id, item_id)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        cur = 0 if stock is None else int(stock)
        nstock = max(0, cur + safe_int(delta_s, 0))
        update_item(chat_id, item_id, title, int(price), nstock, bool(enabled))
        nit = get_item(chat_id, item_id)
        _id, title, price, stock, enabled = nit
        txt = f"标题：{title}\n价格：{milli_to_coin(price)}\n库存：{'∞' if stock is None else stock}\n状态：{'上架' if enabled else '下架'}"
        await q.edit_message_text(txt, reply_markup=kb_item_edit(item_id))
        return

    if data.startswith("v3:i:stockinf:"):
        item_id = safe_int(data.split(":")[3], 0)
        it = get_item(chat_id, item_id)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        update_item(chat_id, item_id, title, int(price), None, bool(enabled))
        nit = get_item(chat_id, item_id)
        _id, title, price, stock, enabled = nit
        txt = f"标题：{title}\n价格：{milli_to_coin(price)}\n库存：{'∞' if stock is None else stock}\n状态：{'上架' if enabled else '下架'}"
        await q.edit_message_text(txt, reply_markup=kb_item_edit(item_id))
        return

    if data.startswith("v3:i:title:"):
        item_id = safe_int(data.split(":")[3], 0)
        context.user_data["await_item_title"] = item_id
        await q.edit_message_text(f"请输入商品 ID{item_id} 新标题（1~40字）")
        return

# =========================
# Message handlers
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

    # 记录群并自动初始化
    if chat.type in ("group", "supergroup"):
        upsert_chat(chat_id, chat.title or str(chat_id))
        ensure_default_rules(chat_id)

    # 管理员输入态（私聊）
    if chat.type == "private":
        # 目标用户ID输入
        if context.user_data.get("await_target_input"):
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            target = safe_int(text, 0)
            if target <= 0:
                await update.message.reply_text("用户ID格式错误")
                return
            context.user_data["adm_target"] = target
            context.user_data["await_target_input"] = False
            bal = wallet_get(v, target)
            await update.message.reply_text(
                f"已设置目标用户：{target}\n当前群余额：{milli_to_coin(bal)}",
                reply_markup=kb_adm_user()
            )
            return

        # 规则名输入
        if context.user_data.get("await_rule_name"):
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            rid = safe_int(context.user_data.get("await_rule_name"), 0)
            name = text[:32].strip()
            if not name:
                await update.message.reply_text("名称不能为空")
                return
            r = get_rule(v, rid)
            if not r:
                context.user_data.pop("await_rule_name", None)
                await update.message.reply_text("规则不存在")
                return
            _id, old_name, p, mn, mx, en, pr = r
            update_rule(v, rid, float(p), int(mn), int(mx), bool(en), name)
            context.user_data.pop("await_rule_name", None)
            await update.message.reply_text(f"规则 ID{rid} 名称已更新为：{name}")
            return

        # 商品标题输入
        if context.user_data.get("await_item_title"):
            ok, v = ensure_admin_selected_chat(user_id, context)
            if not ok:
                await update.message.reply_text(v)
                return
            item_id = safe_int(context.user_data.get("await_item_title"), 0)
            title = text[:40].strip()
            if not title:
                await update.message.reply_text("标题不能为空")
                return
            it = get_item(v, item_id)
            if not it:
                context.user_data.pop("await_item_title", None)
                await update.message.reply_text("商品不存在")
                return
            _id, old_title, price, stock, enabled = it
            update_item(v, item_id, title, int(price), stock, bool(enabled))
            context.user_data.pop("await_item_title", None)
            await update.message.reply_text(f"商品 ID{item_id} 标题已更新为：{title}")
            return

    # 群聊掉落逻辑
    if chat.type in ("group", "supergroup") and not user.is_bot:
        if text.startswith("/"):
            return
        if not valid_text_basic(text, MIN_TEXT_LEN):
            return
        if not can_reward(chat_id, user_id, text):
            return

        rules = list_rules(chat_id, 100, 0)
        total = 0
        for rid, name, p, mn, mx, en, pr in rules:
            if not en:
                continue
            if random.random() < float(p):
                total += random.randint(int(mn), int(mx))
        if total <= 0:
            return

        got = int(rds.get(f"daily:{chat_id}:{user_id}") or 0)
        allow = max(0, DAILY_MAX_MILLI - got)
        grant = min(total, allow)
        if grant <= 0:
            return

        wallet_add(chat_id, user_id, grant)
        add_daily(chat_id, user_id, grant)
        await update.message.reply_text(f"🎉 {user.first_name} 获得 {milli_to_coin(grant)} 金币")

# =========================
# Main
# =========================
def main():
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # /start 直接进面板；移除 /panel
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("buy", buy_cmd))
    app.add_handler(CommandHandler("additem", additem_cmd))
    app.add_handler(CommandHandler("bind_admin", bind_admin_cmd))
    app.add_handler(CommandHandler("unbind_admin", unbind_admin_cmd))

    app.add_handler(CallbackQueryHandler(cb, pattern=r"^v3:"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

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
            logger.warning("WEBHOOK_URL 未设置，回退 polling")
            app.run_polling()
    finally:
        try:
            pg_pool.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
