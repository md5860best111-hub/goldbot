import os
import random
import logging
from decimal import Decimal, ROUND_DOWN
from urllib.parse import urlparse
from functools import wraps

import psycopg
from psycopg_pool import ConnectionPool
import redis

from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler,
    ContextTypes, filters
)

# -----------------------
# 环境变量
# -----------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()

WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()  # 例如 https://xxx.zeabur.app
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/telegram/webhook").strip()
PORT = int(os.getenv("PORT", "8080"))

# 默认超级管理员（首次初始化用）
DEFAULT_OWNER_IDS = "631234269,6376186830"

# 风控
MIN_TEXT_LEN = int(os.getenv("MIN_TEXT_LEN", "5"))
COOLDOWN_SECONDS = int(os.getenv("COOLDOWN_SECONDS", "20"))
DAILY_MAX_MILLI = int(os.getenv("DAILY_MAX_MILLI", "50000"))  # 50金币/天
ENABLE_SAME_TEXT_BLOCK = os.getenv("ENABLE_SAME_TEXT_BLOCK", "1") == "1"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if not BOT_TOKEN:
    raise RuntimeError("缺少 BOT_TOKEN")
if not DATABASE_URL:
    raise RuntimeError("缺少 DATABASE_URL")
if not REDIS_URL:
    raise RuntimeError("缺少 REDIS_URL")


# -----------------------
# 工具函数
# -----------------------
def coin_to_milli(s: str) -> int:
    d = Decimal(s).quantize(Decimal("0.001"), rounding=ROUND_DOWN)
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


# -----------------------
# 连接池（psycopg3）
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
                result = fn(conn, *args, **kwargs)
                conn.commit()
                return result
            except Exception:
                conn.rollback()
                raise
    return wrapper


# -----------------------
# 数据库初始化
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

        # 管理员表（支持手动增删）
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_admins (
          user_id BIGINT PRIMARY KEY,
          is_owner BOOLEAN NOT NULL DEFAULT FALSE,
          created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
        """)

        cur.execute("CREATE INDEX IF NOT EXISTS idx_rules_chat ON drop_rules(chat_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_shop_chat ON shop_items(chat_id);")

        # 初始化默认 owner
        owner_ids = [x.strip() for x in os.getenv("OWNER_IDS", DEFAULT_OWNER_IDS).split(",") if x.strip()]
        for oid in owner_ids:
            cur.execute("""
            INSERT INTO bot_admins(user_id, is_owner)
            VALUES(%s, TRUE)
            ON CONFLICT(user_id) DO NOTHING
            """, (int(oid),))


@with_conn
def is_admin(conn, user_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM bot_admins WHERE user_id=%s", (user_id,))
        return cur.fetchone() is not None

@with_conn
def is_owner(conn, user_id: int) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT is_owner FROM bot_admins WHERE user_id=%s", (user_id,))
        row = cur.fetchone()
        return bool(row and row[0])

@with_conn
def list_admins(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT user_id, is_owner, created_at FROM bot_admins ORDER BY is_owner DESC, created_at ASC")
        return cur.fetchall()

@with_conn
def add_admin(conn, operator_id: int, new_admin_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT is_owner FROM bot_admins WHERE user_id=%s", (operator_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            return False, "只有 owner 可以添加管理员"

        cur.execute("""
        INSERT INTO bot_admins(user_id, is_owner)
        VALUES(%s, FALSE)
        ON CONFLICT(user_id) DO NOTHING
        """, (new_admin_id,))
        return True, "已添加管理员"

@with_conn
def del_admin(conn, operator_id: int, target_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT is_owner FROM bot_admins WHERE user_id=%s", (operator_id,))
        row = cur.fetchone()
        if not row or not row[0]:
            return False, "只有 owner 可以删除管理员"

        cur.execute("SELECT is_owner FROM bot_admins WHERE user_id=%s", (target_id,))
        t = cur.fetchone()
        if not t:
            return False, "目标不在管理员列表"
        if t[0]:
            return False, "不能删除 owner"

        cur.execute("DELETE FROM bot_admins WHERE user_id=%s", (target_id,))
        return True, "已删除管理员"


@with_conn
def ensure_default_rules(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM drop_rules WHERE chat_id=%s", (chat_id,))
        c = cur.fetchone()[0]
        if c == 0:
            cur.execute("""
            INSERT INTO drop_rules(chat_id, name, probability, min_milli, max_milli, enabled, priority)
            VALUES
            (%s,'common',0.01,100,1000,TRUE,100),
            (%s,'rare',0.001,1000,2000,TRUE,90),
            (%s,'epic',0.0001,2000,10000,TRUE,80)
            """, (chat_id, chat_id, chat_id))

@with_conn
def get_rules(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("""
          SELECT id, name, probability, min_milli, max_milli, enabled, priority
          FROM drop_rules
          WHERE chat_id=%s AND enabled=TRUE
          ORDER BY priority ASC, id ASC
        """, (chat_id,))
        return cur.fetchall()

@with_conn
def wallet_get(conn, chat_id: int, user_id: int) -> int:
    with conn.cursor() as cur:
        cur.execute("SELECT balance_milli FROM wallets WHERE chat_id=%s AND user_id=%s", (chat_id, user_id))
        row = cur.fetchone()
        return int(row[0]) if row else 0

@with_conn
def wallet_add(conn, chat_id: int, user_id: int, delta: int):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO wallets(chat_id, user_id, balance_milli, updated_at)
        VALUES(%s,%s,%s,NOW())
        ON CONFLICT(chat_id, user_id)
        DO UPDATE SET
          balance_milli = wallets.balance_milli + EXCLUDED.balance_milli,
          updated_at = NOW()
        """, (chat_id, user_id, delta))

@with_conn
def shop_list(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id, title, price_milli, stock
        FROM shop_items
        WHERE chat_id=%s AND enabled=TRUE
        ORDER BY id DESC
        LIMIT 30
        """, (chat_id,))
        return cur.fetchall()

@with_conn
def add_item(conn, chat_id: int, title: str, price_milli: int, stock):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO shop_items(chat_id, title, price_milli, enabled, stock)
        VALUES(%s,%s,%s,TRUE,%s)
        """, (chat_id, title, price_milli, stock))

@with_conn
def buy_item_atomic(conn, chat_id: int, user_id: int, item_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id, title, price_milli, enabled, stock
        FROM shop_items
        WHERE id=%s AND chat_id=%s
        FOR UPDATE
        """, (item_id, chat_id))
        item = cur.fetchone()
        if not item:
            return False, "商品不存在"
        _id, title, price, enabled, stock = item
        if not enabled:
            return False, "商品已下架"
        if stock is not None and stock <= 0:
            return False, "商品已售罄"

        cur.execute("""
        SELECT balance_milli FROM wallets
        WHERE chat_id=%s AND user_id=%s
        FOR UPDATE
        """, (chat_id, user_id))
        row = cur.fetchone()
        bal = int(row[0]) if row else 0
        if bal < price:
            return False, "金币不足"

        cur.execute("""
        UPDATE wallets SET balance_milli=balance_milli-%s, updated_at=NOW()
        WHERE chat_id=%s AND user_id=%s
        """, (price, chat_id, user_id))

        if stock is not None:
            cur.execute("UPDATE shop_items SET stock=stock-1 WHERE id=%s", (item_id,))

        cur.execute("""
        INSERT INTO redeem_orders(chat_id, user_id, item_id, price_milli, status)
        VALUES(%s,%s,%s,%s,'approved')
        """, (chat_id, user_id, item_id, price))

        return True, f"购买成功：{title}，扣除 {milli_to_coin(price)} 金币"

@with_conn
def list_rules(conn, chat_id: int):
    with conn.cursor() as cur:
        cur.execute("""
        SELECT id, name, probability, min_milli, max_milli, enabled, priority
        FROM drop_rules
        WHERE chat_id=%s
        ORDER BY priority ASC, id ASC
        """, (chat_id,))
        return cur.fetchall()

@with_conn
def set_rule(conn, chat_id: int, rule_id: int, probability: float, min_milli: int, max_milli: int, enabled: bool):
    with conn.cursor() as cur:
        cur.execute("""
        UPDATE drop_rules
        SET probability=%s, min_milli=%s, max_milli=%s, enabled=%s
        WHERE id=%s AND chat_id=%s
        """, (probability, min_milli, max_milli, enabled, rule_id, chat_id))
        return cur.rowcount


# -----------------------
# Redis 风控
# -----------------------
def can_reward(chat_id: int, user_id: int, text: str) -> bool:
    cooldown_key = f"cd:{chat_id}:{user_id}"
    if rds.exists(cooldown_key):
        return False
    rds.setex(cooldown_key, COOLDOWN_SECONDS, "1")

    if ENABLE_SAME_TEXT_BLOCK:
        t = (text or "").strip().lower()
        if t:
            k = f"lt:{chat_id}:{user_id}"
            old = rds.get(k)
            if old == t:
                return False
            rds.setex(k, 120, t)

    daily_key = f"daily:{chat_id}:{user_id}"
    got = int(rds.get(daily_key) or 0)
    if got >= DAILY_MAX_MILLI:
        return False

    return True

def add_daily_reward(chat_id: int, user_id: int, amount: int):
    daily_key = f"daily:{chat_id}:{user_id}"
    p = rds.pipeline()
    p.incrby(daily_key, amount)
    p.expire(daily_key, 86400)
    p.execute()


# -----------------------
# 命令
# -----------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.message:
        await update.message.reply_text(
            "金币机器人已启动。\n"
            "用户命令：/coins /shop /buy\n"
            "管理员：/additem /listrules /setrule\n"
            "Owner：/addadmin /deladmin /listadmins"
        )

async def coins(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    bal = wallet_get(chat_id, user_id)
    await update.message.reply_text(f"你的余额：{milli_to_coin(bal)} 金币")

async def shop(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    rows = shop_list(chat_id)
    if not rows:
        await update.message.reply_text("商店暂无商品")
        return
    lines = ["🛒 商店："]
    for _id, title, price, stock in rows:
        stock_txt = "∞" if stock is None else str(stock)
        lines.append(f"ID {_id} | {title} | {milli_to_coin(price)} 金币 | 库存:{stock_txt}")
    lines.append("\n购买：/buy 商品ID")
    await update.message.reply_text("\n".join(lines))

async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    chat_id = update.effective_chat.id
    user_id = update.effective_user.id
    if not context.args:
        await update.message.reply_text("用法：/buy 商品ID")
        return
    try:
        item_id = int(context.args[0])
    except Exception:
        await update.message.reply_text("商品ID必须是数字")
        return
    ok, msg = buy_item_atomic(chat_id, user_id, item_id)
    await update.message.reply_text(msg)

async def additem_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_admin(user_id):
        await update.message.reply_text("你不是管理员")
        return

    raw = update.message.text.replace("/additem", "", 1).strip()
    parts = [x.strip() for x in raw.split("|")]
    if len(parts) < 2:
        await update.message.reply_text("用法：/additem 商品名 | 价格(如 2.5) | 库存(可选)")
        return

    title = parts[0]
    try:
        price_milli = coin_to_milli(parts[1])
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

    add_item(chat_id, title, price_milli, stock)
    await update.message.reply_text(f"已添加：{title}，价格 {parts[1]}，库存 {'∞' if stock is None else stock}")

async def listrules_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat:
        return
    chat_id = update.effective_chat.id
    rows = list_rules(chat_id)
    if not rows:
        await update.message.reply_text("当前无规则")
        return
    lines = ["🎯 掉落规则："]
    for rid, name, p, mn, mx, en, pr in rows:
        lines.append(f"ID {rid} | {name} | p={p} | {milli_to_coin(mn)}~{milli_to_coin(mx)} | {'开' if en else '关'} | priority={pr}")
    await update.message.reply_text("\n".join(lines))

async def setrule_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    user_id = update.effective_user.id
    chat_id = update.effective_chat.id
    if not is_admin(user_id):
        await update.message.reply_text("你不是管理员")
        return

    if len(context.args) < 5:
        await update.message.reply_text("用法：/setrule 规则ID 概率 最小金币 最大金币 开关(1/0)")
        return

    try:
        rid = int(context.args[0])
        p = float(context.args[1])
        mn = coin_to_milli(context.args[2])
        mx = coin_to_milli(context.args[3])
        en = context.args[4] == "1"
        if p < 0 or p > 1:
            raise ValueError("概率范围错误")
        if mn <= 0 or mx < mn:
            raise ValueError("金额范围错误")
    except Exception:
        await update.message.reply_text("参数错误，请检查")
        return

    n = set_rule(chat_id, rid, p, mn, mx, en)
    await update.message.reply_text("规则已更新" if n > 0 else "规则ID不存在")

async def addadmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    operator = update.effective_user.id
    if not is_owner(operator):
        await update.message.reply_text("只有 owner 可添加管理员")
        return
    if not context.args:
        await update.message.reply_text("用法：/addadmin 用户ID")
        return
    try:
        uid = int(context.args[0])
    except Exception:
        await update.message.reply_text("用户ID必须是数字")
        return
    ok, msg = add_admin(operator, uid)
    await update.message.reply_text(msg if ok else f"失败：{msg}")

async def deladmin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    operator = update.effective_user.id
    if not is_owner(operator):
        await update.message.reply_text("只有 owner 可删除管理员")
        return
    if not context.args:
        await update.message.reply_text("用法：/deladmin 用户ID")
        return
    try:
        uid = int(context.args[0])
    except Exception:
        await update.message.reply_text("用户ID必须是数字")
        return
    ok, msg = del_admin(operator, uid)
    await update.message.reply_text(msg if ok else f"失败：{msg}")

async def listadmins_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_user:
        return
    operator = update.effective_user.id
    if not is_admin(operator):
        await update.message.reply_text("你不是管理员")
        return
    rows = list_admins()
    if not rows:
        await update.message.reply_text("暂无管理员")
        return
    lines = ["👮 管理员列表："]
    for uid, is_owner_flag, _created in rows:
        lines.append(f"{uid} | {'OWNER' if is_owner_flag else 'ADMIN'}")
    await update.message.reply_text("\n".join(lines))


async def on_group_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not update.message or not update.effective_chat or not update.effective_user:
        return
    if update.effective_chat.type not in ["group", "supergroup"]:
        return
    if update.effective_user.is_bot:
        return

    text = (update.message.text or "").strip()
    if len(text) < MIN_TEXT_LEN:
        return

    chat_id = update.effective_chat.id
    user_id = update.effective_user.id

    ensure_default_rules(chat_id)

    if not can_reward(chat_id, user_id, text):
        return

    rules = get_rules(chat_id)
    total = 0
    for _id, _name, p, mn, mx, _en, _pr in rules:
        if random.random() < p:
            total += random.randint(int(mn), int(mx))

    if total <= 0:
        return

    daily_key = f"daily:{chat_id}:{user_id}"
    got = int(rds.get(daily_key) or 0)
    allow = max(0, DAILY_MAX_MILLI - got)
    grant = min(total, allow)
    if grant <= 0:
        return

    wallet_add(chat_id, user_id, grant)
    add_daily_reward(chat_id, user_id, grant)

    await update.message.reply_text(f"🎉 {update.effective_user.first_name} 获得 {milli_to_coin(grant)} 金币")


def main():
    init_db()

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    # 用户命令
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("coins", coins))
    app.add_handler(CommandHandler("shop", shop))
    app.add_handler(CommandHandler("buy", buy))

    # 管理员命令
    app.add_handler(CommandHandler("additem", additem_cmd))
    app.add_handler(CommandHandler("listrules", listrules_cmd))
    app.add_handler(CommandHandler("setrule", setrule_cmd))

    # owner 命令
    app.add_handler(CommandHandler("addadmin", addadmin_cmd))
    app.add_handler(CommandHandler("deladmin", deladmin_cmd))
    app.add_handler(CommandHandler("listadmins", listadmins_cmd))

    # 群消息监听
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_group_text))

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
        logger.warning("WEBHOOK_URL 未设置，回退 polling（不推荐生产）")
        app.run_polling()


if __name__ == "__main__":
    main()
