import os
import random
import logging
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlparse
from functools import wraps

from psycopg_pool import ConnectionPool
import redis

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters, CallbackQueryHandler
)

# -----------------------
# ENV
# -----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram/webhook").strip()
PORT = int(os.getenv("PORT", "8080"))

DEFAULT_ADMIN_IDS = "631234269,6376186830"

MIN_TEXT_LEN = int(os.getenv("MIN_TEXT_LEN", "5"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "20"))
DAILY_MAX_MILLI = int(os.getenv("DAILY_MAX_MILLI", "50000"))
ENABLE_SAME_TEXT_BLOCK = os.getenv("ENABLE_SAME_TEXT_BLOCK", "1") == "1"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not BOT_TOKEN or not DATABASE_URL or not REDIS_URL:
    raise RuntimeError("缺少 BOT_TOKEN / DATABASE_URL / REDIS_URL")

# -----------------------
# Helpers
# -----------------------
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

# -----------------------
# DB/Redis
# -----------------------
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

# -----------------------
# Schema
# -----------------------
@with_conn
def init_db(conn):
    with conn.cursor() as cur:
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
        CREATE TABLE IF NOT EXISTS bot_admins (
          user_id BIGINT PRIMARY KEY,
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
        cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_chat ON drop_rules(chat_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_shop_chat ON shop_items(chat_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_logs_chat ON coin_logs(chat_id, id DESC);")

        # 预置管理员
        aids = [x.strip() for x in os.getenv("ADMIN_IDS", DEFAULT_ADMIN_IDS).split(",") if x.strip()]
        for aid in aids:
            cur.execute("""
            INSERT INTO bot_admins(user_id) VALUES(%s)
            ON CONFLICT(user_id) DO NOTHING
            """, (int(aid),))

@with_conn
def is_admin(conn, uid: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM bot_admins WHERE user_id=%s", (uid,))
        return cur.fetchone() is not None

@with_conn
def list_admins(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, created_at FROM bot_admins ORDER BY created_at ASC")
        return cur.fetchall()

@with_conn
def add_admin(conn, operator: int, target: int):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM bot_admins WHERE user_id=%s", (operator,))
        if not cur.fetchone():
            return False, "只有管理员可操作"
        cur.execute("INSERT INTO bot_admins(user_id) VALUES(%s) ON CONFLICT(user_id) DO NOTHING", (target,))
        return True, "已添加管理员"

@with_conn
def del_admin(conn, operator: int, target: int):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM bot_admins WHERE user_id=%s", (operator,))
        if not cur.fetchone():
            return False, "只有管理员可操作"
        cur.execute("SELECT COUNT(*) FROM bot_admins")
        total = cur.fetchone()[0]
        cur.execute("SELECT 1 FROM bot_admins WHERE user_id=%s", (target,))
        if not cur.fetchone():
            return False, "目标不是管理员"
        if total <= 1:
            return False, "至少保留1名管理员"
        cur.execute("DELETE FROM bot_admins WHERE user_id=%s", (target,))
        return True, "已删除管理员"

@with_conn
def wallet_get(conn, chat_id: int, uid: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT balance_milli FROM wallets WHERE chat_id=%s AND user_id=%s", (chat_id, uid))
        r = cur.fetchone()
        return int(r[0]) if r else 0

@with_conn
def wallet_add(conn, chat_id: int, uid: int, delta: int):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at)
        VALUES(%s,%s,%s,NOW())
        ON CONFLICT(chat_id,user_id)
        DO UPDATE SET balance_milli=wallets.balance_milli+EXCLUDED.balance_milli, updated_at=NOW()
        """, (chat_id, uid, delta))

@with_conn
def wallet_adjust_admin(conn, chat_id: int, operator_id: int, target_id: int, delta: int, reason: str):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT balance_milli FROM wallets
        WHERE chat_id=%s AND user_id=%s
        FOR UPDATE
        """, (chat_id, target_id))
        row = cur.fetchone()
        old = int(row[0]) if row else 0
        new = old + delta
        if new < 0:
            return False, f"余额不足，当前 {milli_to_coin(old)}"
        if row:
            cur.execute("UPDATE wallets SET balance_milli=%s, updated_at=NOW() WHERE chat_id=%s AND user_id=%s",
                        (new, chat_id, target_id))
        else:
            cur.execute("INSERT INTO wallets(chat_id,user_id,balance_milli,updated_at) VALUES(%s,%s,%s,NOW())",
                        (chat_id, target_id, new))
        cur.execute("""
        INSERT INTO coin_logs(chat_id,operator_id,user_id,delta_milli,reason)
        VALUES(%s,%s,%s,%s,%s)
        """, (chat_id, operator_id, target_id, delta, reason or "panel"))
        return True, f"成功：{target_id} 变动 {milli_to_coin(delta)}，新余额 {milli_to_coin(new)}"

@with_conn
def coin_logs_page(conn, chat_id: int, limit=10, offset=0):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id, operator_id, user_id, delta_milli, reason, created_at
        FROM coin_logs
        WHERE chat_id=%s
        ORDER BY id DESC
        LIMIT %s OFFSET %s
        """, (chat_id, limit, offset))
        return cur.fetchall()

@with_conn
def ensure_default_rules(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM drop_rules WHERE chat_id=%s", (chat_id,))
        if cur.fetchone()[0] == 0:
            cur.execute("""
            INSERT INTO drop_rules(chat_id,name,probability,min_milli,max_milli,enabled,priority)
            VALUES
            (%s,'common',0.01,100,1000,TRUE,100),
            (%s,'rare',0.001,1000,2000,TRUE,90),
            (%s,'epic',0.0001,2000,10000,TRUE,80)
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
        FROM drop_rules
        WHERE chat_id=%s AND id=%s
        """, (chat_id, rid))
        return cur.fetchone()

@with_conn
def update_rule(conn, chat_id: int, rid: int, p: float, mn: int, mx: int, en: bool, name: str):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE drop_rules
        SET probability=%s, min_milli=%s, max_milli=%s, enabled=%s, name=%s
        WHERE chat_id=%s AND id=%s
        """, (p, mn, mx, en, name, chat_id, rid))
        return cur.rowcount

@with_conn
def shop_page(conn, chat_id: int, limit=10, offset=0):
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
        SET title=%s, price_milli=%s, stock=%s, enabled=%s
        WHERE chat_id=%s AND id=%s
        """, (title, price_milli, stock, enabled, chat_id, item_id))
        return cur.rowcount

@with_conn
def buy_item_atomic(conn, chat_id: int, uid: int, item_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id,title,price_milli,enabled,stock
        FROM shop_items
        WHERE chat_id=%s AND id=%s
        FOR UPDATE
        """, (chat_id, item_id))
        it = cur.fetchone()
        if not it:
            return False, "商品不存在"
        _id, title, price, enabled, stock = it
        if not enabled:
            return False, "商品已下架"
        if stock is not None and stock <= 0:
            return False, "库存不足"

        cur.execute("""
        SELECT balance_milli FROM wallets
        WHERE chat_id=%s AND user_id=%s
        FOR UPDATE
        """, (chat_id, uid))
        w = cur.fetchone()
        bal = int(w[0]) if w else 0
        if bal < price:
            return False, "金币不足"

        cur.execute("UPDATE wallets SET balance_milli=balance_milli-%s, updated_at=NOW() WHERE chat_id=%s AND user_id=%s",
                    (price, chat_id, uid))
        if stock is not None:
            cur.execute("UPDATE shop_items SET stock=stock-1 WHERE id=%s", (item_id,))
        cur.execute("""
        INSERT INTO redeem_orders(chat_id,user_id,item_id,price_milli,status)
        VALUES(%s,%s,%s,%s,'approved')
        """, (chat_id, uid, item_id, price))
        return True, f"购买成功：{title}，扣除 {milli_to_coin(price)}"

# -----------------------
# 风控
# -----------------------
def can_reward(chat_id: int, uid: int, text: str) -> bool:
    k_cd = f"cd:{chat_id}:{uid}"
    if rds.exists(k_cd):
        return False
    rds.setex(k_cd, COOLDOWN_SECONDS, "1")

    if ENABLE_SAME_TEXT_BLOCK:
        t = (text or "").strip().lower()
        if t:
            k = f"lt:{chat_id}:{uid}"
            old = rds.get(k)
            if old == t:
                return False
            rds.setex(k, 120, t)

    got = int(rds.get(f"daily:{chat_id}:{uid}") or 0)
    return got < DAILY_MAX_MILLI

def add_daily(chat_id: int, uid: int, amt: int):
    k = f"daily:{chat_id}:{uid}"
    p = rds.pipeline()
    p.incrby(k, amt)
    p.expire(k, 86400)
    p.execute()

# -----------------------
# UI
# -----------------------
SHOP_SIZE = 6
RULE_SIZE = 6
LOG_SIZE = 8
STEP_OPTIONS = [100, 500, 1000, 5000, 10000]  # 0.1/0.5/1/5/10

def kb_panel(isadm: bool):
    rows = [
        [InlineKeyboardButton("💰 我的金币", callback_data="x:me"),
         InlineKeyboardButton("🛒 商店", callback_data="x:shop:0")],
        [InlineKeyboardButton("🎯 掉落说明", callback_data="x:rules_read:0")]
    ]
    if isadm:
        rows += [
            [InlineKeyboardButton("👤 金币管理", callback_data="x:adm_user"),
             InlineKeyboardButton("🧾 操作日志", callback_data="x:logs:0")],
            [InlineKeyboardButton("⚙️ 规则管理", callback_data="x:adm_rules:0"),
             InlineKeyboardButton("🎁 商品管理", callback_data="x:adm_shop:0")],
            [InlineKeyboardButton("🛡️ 管理员列表", callback_data="x:adm_list")]
        ]
    return InlineKeyboardMarkup(rows)

def kb_shop(chat_id: int, page: int):
    rows = []
    items = shop_page(chat_id, SHOP_SIZE, page * SHOP_SIZE)
    for item_id, title, price, stock, enabled in items:
        if not enabled:
            continue
        st = "∞" if stock is None else str(stock)
        rows.append([InlineKeyboardButton(
            f"{title[:12]} | {milli_to_coin(price)} | 库存{st}",
            callback_data=f"x:buy:{item_id}"
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"x:shop:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}页", callback_data="x:noop"),
        InlineKeyboardButton("➡️", callback_data=f"x:shop:{page+1}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回", callback_data="x:home")])
    return InlineKeyboardMarkup(rows)

def kb_rules_read(chat_id: int, page: int):
    rows = []
    rs = list_rules(chat_id, RULE_SIZE, page * RULE_SIZE)
    for rid, name, p, mn, mx, en, pr in rs:
        rows.append([InlineKeyboardButton(
            f"{'✅' if en else '❌'} ID{rid} {name} p={p}",
            callback_data=f"x:rule_view:{rid}"
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"x:rules_read:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}页", callback_data="x:noop"),
        InlineKeyboardButton("➡️", callback_data=f"x:rules_read:{page+1}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回", callback_data="x:home")])
    return InlineKeyboardMarkup(rows)

def kb_admin_user():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("选择目标用户", callback_data="x:adm_target"),
         InlineKeyboardButton("💱切换步进", callback_data="x:adm_step")],
        [InlineKeyboardButton("➕加金币", callback_data="x:adm_add"),
         InlineKeyboardButton("➖扣金币", callback_data="x:adm_sub")],
        [InlineKeyboardButton("📦查余额", callback_data="x:adm_qbal")],
        [InlineKeyboardButton("🔙 返回", callback_data="x:home")]
    ])

def kb_admin_rules(chat_id: int, page: int):
    rs = list_rules(chat_id, RULE_SIZE, page * RULE_SIZE)
    rows = []
    for rid, name, p, mn, mx, en, pr in rs:
        rows.append([InlineKeyboardButton(
            f"{'✅' if en else '❌'} ID{rid} {name} p={p}",
            callback_data=f"x:adm_rule:{rid}"
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"x:adm_rules:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}页", callback_data="x:noop"),
        InlineKeyboardButton("➡️", callback_data=f"x:adm_rules:{page+1}")
    ])
    rows.append([InlineKeyboardButton("🔙 返回", callback_data="x:home")])
    return InlineKeyboardMarkup(rows)

def kb_rule_edit(rid: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("名称编辑", callback_data=f"x:r:name:{rid}"),
         InlineKeyboardButton("开关切换", callback_data=f"x:r:toggle:{rid}")],
        [InlineKeyboardButton("p -0.001", callback_data=f"x:r:p:-0.001:{rid}"),
         InlineKeyboardButton("p +0.001", callback_data=f"x:r:p:+0.001:{rid}")],
        [InlineKeyboardButton("min -0.1", callback_data=f"x:r:min:-100:{rid}"),
         InlineKeyboardButton("min +0.1", callback_data=f"x:r:min:+100:{rid}")],
        [InlineKeyboardButton("max -0.1", callback_data=f"x:r:max:-100:{rid}"),
         InlineKeyboardButton("max +0.1", callback_data=f"x:r:max:+100:{rid}")],
        [InlineKeyboardButton("🔙 返回规则列表", callback_data="x:adm_rules:0")]
    ])

def kb_admin_shop(chat_id: int, page: int):
    items = shop_page(chat_id, SHOP_SIZE, page * SHOP_SIZE)
    rows = []
    for item_id, title, price, stock, enabled in items:
        st = "∞" if stock is None else str(stock)
        rows.append([InlineKeyboardButton(
            f"{'✅' if enabled else '❌'} ID{item_id} {title[:10]} {milli_to_coin(price)} 库存{st}",
            callback_data=f"x:adm_item:{item_id}"
        )])
    rows.append([
        InlineKeyboardButton("⬅️", callback_data=f"x:adm_shop:{max(0,page-1)}"),
        InlineKeyboardButton(f"第{page+1}页", callback_data="x:noop"),
        InlineKeyboardButton("➡️", callback_data=f"x:adm_shop:{page+1}")
    ])
    rows.append([InlineKeyboardButton("➕ 新增商品(命令 /additem)", callback_data="x:noop")])
    rows.append([InlineKeyboardButton("🔙 返回", callback_data="x:home")])
    return InlineKeyboardMarkup(rows)

def kb_item_edit(item_id: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("上/下架切换", callback_data=f"x:i:toggle:{item_id}")],
        [InlineKeyboardButton("价格 -0.5", callback_data=f"x:i:price:-500:{item_id}"),
         InlineKeyboardButton("价格 +0.5", callback_data=f"x:i:price:+500:{item_id}")],
        [InlineKeyboardButton("库存 -1", callback_data=f"x:i:stock:-1:{item_id}"),
         InlineKeyboardButton("库存 +1", callback_data=f"x:i:stock:+1:{item_id}")],
        [InlineKeyboardButton("库存设为∞", callback_data=f"x:i:stockinf:{item_id}")],
        [InlineKeyboardButton("标题编辑", callback_data=f"x:i:title:{item_id}")],
        [InlineKeyboardButton("🔙 返回商品列表", callback_data="x:adm_shop:0")]
    ])

def kb_logs(page: int):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⬅️", callback_data=f"x:logs:{max(0,page-1)}"),
         InlineKeyboardButton(f"第{page+1}页", callback_data="x:noop"),
         InlineKeyboardButton("➡️", callback_data=f"x:logs:{page+1}")],
        [InlineKeyboardButton("🔙 返回", callback_data="x:home")]
    ])

# -----------------------
# Commands
# -----------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text("已启动，输入 /panel 打开面板")

async def panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    adm = is_admin(update.effective_user.id)
    if adm:
        context.user_data.setdefault("adm_target", update.effective_user.id)
        context.user_data.setdefault("adm_step", 1000)
    await update.message.reply_text("📋 控制面板", reply_markup=kb_panel(adm))

async def buy_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if not context.args:
        await update.message.reply_text("用法：/buy 商品ID")
        return
    iid = safe_int(context.args[0], 0)
    if iid <= 0:
        await update.message.reply_text("ID错误")
        return
    ok, msg = buy_item_atomic(update.effective_chat.id, update.effective_user.id, iid)
    await update.message.reply_text(msg)

async def additem_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("你不是管理员")
        return
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
    add_item(update.effective_chat.id, title, price, stock)
    await update.message.reply_text("商品已添加")

async def addadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("只有管理员可操作")
        return
    uid = safe_int(context.args[0], 0) if context.args else 0
    if uid <= 0:
        await update.message.reply_text("用法：/addadmin 用户ID")
        return
    ok, msg = add_admin(update.effective_user.id, uid)
    await update.message.reply_text(msg if ok else f"失败：{msg}")

async def deladmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("只有管理员可操作")
        return
    uid = safe_int(context.args[0], 0) if context.args else 0
    if uid <= 0:
        await update.message.reply_text("用法：/deladmin 用户ID")
        return
    ok, msg = del_admin(update.effective_user.id, uid)
    await update.message.reply_text(msg if ok else f"失败：{msg}")

# -----------------------
# Callback
# -----------------------
async def cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if not q or not q.message or not update.effective_user:
        return
    data = q.data or ""
    await q.answer()

    uid = update.effective_user.id
    chat_id = q.message.chat_id
    adm = is_admin(uid)

    if data == "x:noop":
        return
    if data == "x:home":
        await q.edit_message_text("📋 控制面板", reply_markup=kb_panel(adm))
        return
    if data == "x:me":
        bal = wallet_get(chat_id, uid)
        got = int(rds.get(f"daily:{chat_id}:{uid}") or 0)
        await q.edit_message_text(
            f"💰 余额：{milli_to_coin(bal)}\n今日获得：{milli_to_coin(got)} / {milli_to_coin(DAILY_MAX_MILLI)}",
            reply_markup=kb_panel(adm)
        )
        return
    if data.startswith("x:shop:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        await q.edit_message_text("🛒 商店（点击即购买）", reply_markup=kb_shop(chat_id, page))
        return
    if data.startswith("x:buy:"):
        iid = safe_int(data.split(":")[2], 0)
        ok, msg = buy_item_atomic(chat_id, uid, iid)
        await q.answer(msg[:180], show_alert=True)
        return
    if data.startswith("x:rules_read:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        await q.edit_message_text("🎯 掉落规则", reply_markup=kb_rules_read(chat_id, page))
        return
    if data.startswith("x:rule_view:"):
        rid = safe_int(data.split(":")[2], 0)
        r = get_rule(chat_id, rid)
        if not r:
            await q.answer("规则不存在", show_alert=True)
            return
        _id, name, p, mn, mx, en, pr = r
        txt = f"ID{rid} {name}\n概率: {p}\n区间: {milli_to_coin(mn)}~{milli_to_coin(mx)}\n状态: {'开启' if en else '关闭'}"
        await q.edit_message_text(txt, reply_markup=kb_rules_read(chat_id, 0))
        return

    # admin gate
    if not adm:
        await q.answer("无权限", show_alert=True)
        return

    if data == "x:adm_list":
        rows = list_admins()
        txt = "🛡️ 管理员列表\n" + "\n".join([f"- {x[0]}" for x in rows]) if rows else "暂无管理员"
        await q.edit_message_text(txt, reply_markup=kb_panel(True))
        return

    # 金币管理
    if data == "x:adm_user":
        context.user_data.setdefault("adm_target", uid)
        context.user_data.setdefault("adm_step", 1000)
        t = context.user_data["adm_target"]
        s = context.user_data["adm_step"]
        bal = wallet_get(chat_id, t)
        await q.edit_message_text(
            f"👤 金币管理\n目标: {t}\n步进: {milli_to_coin(s)}\n目标余额: {milli_to_coin(bal)}",
            reply_markup=kb_admin_user()
        )
        return
    if data == "x:adm_target":
        context.user_data["await_target_input"] = True
        await q.edit_message_text("请输入目标用户ID（纯数字）")
        return
    if data == "x:adm_step":
        cur = context.user_data.get("adm_step", 1000)
        idx = STEP_OPTIONS.index(cur) if cur in STEP_OPTIONS else 2
        idx = (idx + 1) % len(STEP_OPTIONS)
        context.user_data["adm_step"] = STEP_OPTIONS[idx]
        t = context.user_data.get("adm_target", uid)
        bal = wallet_get(chat_id, t)
        await q.edit_message_text(
            f"👤 金币管理\n目标: {t}\n步进: {milli_to_coin(context.user_data['adm_step'])}\n目标余额: {milli_to_coin(bal)}",
            reply_markup=kb_admin_user()
        )
        return
    if data == "x:adm_qbal":
        t = context.user_data.get("adm_target", uid)
        bal = wallet_get(chat_id, t)
        await q.answer(f"{t} 余额 {milli_to_coin(bal)}", show_alert=True)
        return
    if data in ("x:adm_add", "x:adm_sub"):
        t = context.user_data.get("adm_target", uid)
        s = context.user_data.get("adm_step", 1000)
        delta = s if data == "x:adm_add" else -s
        ok, msg = wallet_adjust_admin(chat_id, uid, t, delta, "panel_adjust")
        await q.answer(msg[:180], show_alert=True)
        bal = wallet_get(chat_id, t)
        await q.edit_message_text(
            f"👤 金币管理\n目标: {t}\n步进: {milli_to_coin(s)}\n目标余额: {milli_to_coin(bal)}",
            reply_markup=kb_admin_user()
        )
        return

    # 日志
    if data.startswith("x:logs:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        rows = coin_logs_page(chat_id, LOG_SIZE, page * LOG_SIZE)
        if not rows:
            txt = "🧾 暂无日志"
        else:
            lines = ["🧾 管理员操作日志："]
            for lid, op, tu, d, reason, ct in rows:
                sign = "+" if d >= 0 else ""
                lines.append(f"#{lid} {ct} | {op} -> {tu} | {sign}{milli_to_coin(d)} | {reason}")
            txt = "\n".join(lines)
        await q.edit_message_text(txt[:3900], reply_markup=kb_logs(page))
        return

    # 规则管理
    if data.startswith("x:adm_rules:"):
        ensure_default_rules(chat_id)
        page = max(0, safe_int(data.split(":")[2], 0))
        await q.edit_message_text("⚙️ 规则管理", reply_markup=kb_admin_rules(chat_id, page))
        return
    if data.startswith("x:adm_rule:"):
        rid = safe_int(data.split(":")[2], 0)
        r = get_rule(chat_id, rid)
        if not r:
            await q.answer("规则不存在", show_alert=True)
            return
        _id, name, p, mn, mx, en, pr = r
        txt = f"⚙️ 编辑规则 ID{rid} {name}\n概率:{p}\n最小:{milli_to_coin(mn)}\n最大:{milli_to_coin(mx)}\n状态:{'开' if en else '关'}"
        await q.edit_message_text(txt, reply_markup=kb_rule_edit(rid))
        return
    if data.startswith("x:r:name:"):
        rid = safe_int(data.split(":")[3], 0)
        context.user_data["await_rule_name"] = rid
        await q.edit_message_text(f"请输入规则 ID{rid} 新名称（1~32字符）")
        return
    if data.startswith("x:r:toggle:"):
        rid = safe_int(data.split(":")[3], 0)
        r = get_rule(chat_id, rid)
        if not r:
            await q.answer("规则不存在", show_alert=True)
            return
        _id, name, p, mn, mx, en, pr = r
        update_rule(chat_id, rid, float(p), int(mn), int(mx), not bool(en), name)
        nr = get_rule(chat_id, rid)
        _id, name, p, mn, mx, en, pr = nr
        txt = f"⚙️ 编辑规则 ID{rid} {name}\n概率:{p}\n最小:{milli_to_coin(mn)}\n最大:{milli_to_coin(mx)}\n状态:{'开' if en else '关'}"
        await q.edit_message_text(txt, reply_markup=kb_rule_edit(rid))
        return
    if data.startswith("x:r:p:") or data.startswith("x:r:min:") or data.startswith("x:r:max:"):
        # x:r:{field}:{delta}:{rid}
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
        txt = f"⚙️ 编辑规则 ID{rid} {name}\n概率:{p}\n最小:{milli_to_coin(mn)}\n最大:{milli_to_coin(mx)}\n状态:{'开' if en else '关'}"
        await q.edit_message_text(txt, reply_markup=kb_rule_edit(rid))
        return

    # 商品管理
    if data.startswith("x:adm_shop:"):
        page = max(0, safe_int(data.split(":")[2], 0))
        await q.edit_message_text("🎁 商品管理", reply_markup=kb_admin_shop(chat_id, page))
        return
    if data.startswith("x:adm_item:"):
        iid = safe_int(data.split(":")[2], 0)
        it = get_item(chat_id, iid)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        st = "∞" if stock is None else str(stock)
        txt = f"🎁 编辑商品 ID{iid}\n标题:{title}\n价格:{milli_to_coin(price)}\n库存:{st}\n状态:{'上架' if enabled else '下架'}"
        await q.edit_message_text(txt, reply_markup=kb_item_edit(iid))
        return
    if data.startswith("x:i:toggle:"):
        iid = safe_int(data.split(":")[3], 0)
        it = get_item(chat_id, iid)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        update_item(chat_id, iid, title, int(price), stock, not bool(enabled))
        nit = get_item(chat_id, iid)
        _id, title, price, stock, enabled = nit
        st = "∞" if stock is None else str(stock)
        txt = f"🎁 编辑商品 ID{iid}\n标题:{title}\n价格:{milli_to_coin(price)}\n库存:{st}\n状态:{'上架' if enabled else '下架'}"
        await q.edit_message_text(txt, reply_markup=kb_item_edit(iid))
        return
    if data.startswith("x:i:price:"):
        # x:i:price:{delta}:{id}
        _, _, _, delta_s, iid_s = data.split(":")
        iid = safe_int(iid_s, 0)
        it = get_item(chat_id, iid)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        nprice = max(1, int(price) + safe_int(delta_s, 0))
        update_item(chat_id, iid, title, nprice, stock, bool(enabled))
        nit = get_item(chat_id, iid)
        _id, title, price, stock, enabled = nit
        st = "∞" if stock is None else str(stock)
        txt = f"🎁 编辑商品 ID{iid}\n标题:{title}\n价格:{milli_to_coin(price)}\n库存:{st}\n状态:{'上架' if enabled else '下架'}"
        await q.edit_message_text(txt, reply_markup=kb_item_edit(iid))
        return
    if data.startswith("x:i:stock:"):
        # x:i:stock:{delta}:{id}
        _, _, _, delta_s, iid_s = data.split(":")
        iid = safe_int(iid_s, 0)
        it = get_item(chat_id, iid)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        cur = 0 if stock is None else int(stock)
        nstock = max(0, cur + safe_int(delta_s, 0))
        update_item(chat_id, iid, title, int(price), nstock, bool(enabled))
        nit = get_item(chat_id, iid)
        _id, title, price, stock, enabled = nit
        st = "∞" if stock is None else str(stock)
        txt = f"🎁 编辑商品 ID{iid}\n标题:{title}\n价格:{milli_to_coin(price)}\n库存:{st}\n状态:{'上架' if enabled else '下架'}"
        await q.edit_message_text(txt, reply_markup=kb_item_edit(iid))
        return
    if data.startswith("x:i:stockinf:"):
        iid = safe_int(data.split(":")[3], 0)
        it = get_item(chat_id, iid)
        if not it:
            await q.answer("商品不存在", show_alert=True)
            return
        _id, title, price, stock, enabled = it
        update_item(chat_id, iid, title, int(price), None, bool(enabled))
        nit = get_item(chat_id, iid)
        _id, title, price, stock, enabled = nit
        st = "∞" if stock is None else str(stock)
        txt = f"🎁 编辑商品 ID{iid}\n标题:{title}\n价格:{milli_to_coin(price)}\n库存:{st}\n状态:{'上架' if enabled else '下架'}"
        await q.edit_message_text(txt, reply_markup=kb_item_edit(iid))
        return
    if data.startswith("x:i:title:"):
        iid = safe_int(data.split(":")[3], 0)
        context.user_data["await_item_title"] = iid
        await q.edit_message_text(f"请输入商品 ID{iid} 新标题（1~40字符）")
        return

# -----------------------
# Text handler
# -----------------------
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    text = (update.message.text or "").strip()
    if not text:
        return

    chat_id = update.effective_chat.id
    uid = update.effective_user.id
    adm = is_admin(uid)

    # 管理员输入目标用户ID
    if adm and context.user_data.get("await_target_input") and not text.startswith("/"):
        target = safe_int(text, 0)
        if target <= 0:
            await update.message.reply_text("用户ID格式错误")
            return
        context.user_data["adm_target"] = target
        context.user_data["await_target_input"] = False
        step = context.user_data.get("adm_step", 1000)
        bal = wallet_get(chat_id, target)
        await update.message.reply_text(
            f"目标用户已设置：{target}\n步进：{milli_to_coin(step)}\n当前余额：{milli_to_coin(bal)}",
            reply_markup=kb_admin_user()
        )
        return

    # 规则名称输入
    if adm and context.user_data.get("await_rule_name") and not text.startswith("/"):
        rid = safe_int(context.user_data.get("await_rule_name"), 0)
        name = text[:32].strip()
        if len(name) < 1:
            await update.message.reply_text("名称不能为空")
            return
        r = get_rule(chat_id, rid)
        if not r:
            context.user_data.pop("await_rule_name", None)
            await update.message.reply_text("规则不存在")
            return
        _id, old_name, p, mn, mx, en, pr = r
        update_rule(chat_id, rid, float(p), int(mn), int(mx), bool(en), name)
        context.user_data.pop("await_rule_name", None)
        await update.message.reply_text(f"规则 ID{rid} 名称已更新为：{name}")
        return

    # 商品标题输入
    if adm and context.user_data.get("await_item_title") and not text.startswith("/"):
        iid = safe_int(context.user_data.get("await_item_title"), 0)
        title = text[:40].strip()
        if len(title) < 1:
            await update.message.reply_text("标题不能为空")
            return
        it = get_item(chat_id, iid)
        if not it:
            context.user_data.pop("await_item_title", None)
            await update.message.reply_text("商品不存在")
            return
        _id, old_title, price, stock, enabled = it
        update_item(chat_id, iid, title, int(price), stock, bool(enabled))
        context.user_data.pop("await_item_title", None)
        await update.message.reply_text(f"商品 ID{iid} 标题已更新为：{title}")
        return

    # 群聊掉落
    if update.effective_chat.type in ["group", "supergroup"] and not update.effective_user.is_bot:
        if text.startswith("/") or len(text) < MIN_TEXT_LEN:
            return
        ensure_default_rules(chat_id)
        if not can_reward(chat_id, uid, text):
            return
        rs = list_rules(chat_id, 100, 0)
        total = 0
        for rid, name, p, mn, mx, en, pr in rs:
            if not en:
                continue
            if random.random() < float(p):
                total += random.randint(int(mn), int(mx))
        if total <= 0:
            return
        got = int(rds.get(f"daily:{chat_id}:{uid}") or 0)
        allow = max(0, DAILY_MAX_MILLI - got)
        grant = min(total, allow)
        if grant <= 0:
            return
        wallet_add(chat_id, uid, grant)
        add_daily(chat_id, uid, grant)
        await update.message.reply_text(f"🎉 {update.effective_user.first_name} 获得 {milli_to_coin(grant)} 金币")

# -----------------------
# main
# -----------------------
def main():
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("panel", panel))
    app.add_handler(CommandHandler("buy", buy_cmd))
    app.add_handler(CommandHandler("additem", additem_cmd))
    app.add_handler(CommandHandler("addadmin", addadmin_cmd))
    app.add_handler(CommandHandler("deladmin", deladmin_cmd))

    app.add_handler(CallbackQueryHandler(cb, pattern=r"^x:"))
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
            logger.warning("WEBHOOK_URL 未设置，使用 polling")
            app.run_polling()
    finally:
        try:
            pg_pool.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
