```md
# Telegram 金币机器人（Zeabur 生产版）

## 功能
- 群聊发言概率掉落金币（多奖池）
- `/coins` 查看余额
- `/shop` 查看商店
- `/buy 商品ID` 购买商品
- 管理员：`/additem`、`/listrules`、`/setrule`
- Owner：`/addadmin`、`/deladmin`、`/listadmins`

## 必填环境变量
- BOT_TOKEN
- DATABASE_URL
- REDIS_URL
- WEBHOOK_URL

## 可选环境变量
- WEBHOOK_PATH=/telegram/webhook
- PORT=8080
- OWNER_IDS=631234269,6376186830
- MIN_TEXT_LEN=5
- COOLDOWN_SECONDS=20
- DAILY_MAX_MILLI=50000
- ENABLE_SAME_TEXT_BLOCK=1

## 命令用法

### 用户
- /coins
- /shop
- /buy 1

### 管理员
- /additem 商品名 | 价格 | 库存(可选)
  - 例如：`/additem 可乐 | 1.5 | 100`
- /listrules
- /setrule 规则ID 概率 最小金币 最大金币 开关(1/0)
  - 例如：`/setrule 3 0.001 1 2 1`

### Owner
- /addadmin 用户ID
- /deladmin 用户ID
- /listadmins
```
