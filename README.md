# 分布式配额决策 API（三级令牌桶）

Python + FastAPI + PostgreSQL 实现的分布式配额预占服务。配额按
**租户（tenant）→ 主体（subject）→ 操作（action）** 三级令牌桶组织，
一次申请必须同时通过三级检查；任一级不足即拒绝，响应给出**所有不足层级**
与其中**最长的 `retry_after`**。

核心保证：

- **原子检查 + 扣减**：三级桶在单个数据库事务中按固定全局顺序加行锁，
  并发申请不会超卖，也不会死锁。
- **预占 / 提交 / 取消三段式**：预占立即扣额；提交不再扣；取消归还且只归还一次。
- **超时自动归还**：后台 reaper 回收过期预占，与取消之间通过行锁仲裁，
  二者无论谁先谁后，token 恰好归还一次。
- **请求幂等**：以 `request_id` 为幂等键，原参重试返回与首次完全一致的结果
  （含授予时的 `expires_at`、拒绝时的 `retry_after_seconds`）；改参重试返回
  `409 Conflict`。
- **策略与预占解耦**：修改桶容量/补充速率只影响之后的新申请，
  不改写任何已存在的预占。

---

## 目录结构

```
app/
  main.py            FastAPI 入口、lifespan（连接池 + reaper）
  config.py          环境变量配置
  database.py        asyncpg 连接池、schema 初始化（含 SQL 切分器）
  db/schema.sql      表结构与全部存储函数（事务边界在函数内）
  service.py         指纹计算与 DB 调用封装
  schemas.py         Pydantic 模型
  reaper.py          过期预占后台回收循环
  routers/
    quota.py         /v1/reserve /v1/commit /v1/cancel
    admin.py         /admin 策略与状态管理、手动回收
tests/               pytest（使用 pgserver 内嵌 PostgreSQL，无需外部服务）
docker-compose.yml   db + api 两服务
Dockerfile
```

---

## 快速开始（Docker Compose）

要求 Docker 20.10+ 与 Compose v2。

```bash
docker compose up --build
```

- API 文档（Swagger UI）：<http://localhost:8000/docs>
- 健康检查：<http://localhost:8000/health>
- PostgreSQL：`localhost:5432`，库/用户/密码均为 `quota`

停止并清理数据卷：

```bash
docker compose down -v
```

### 配置（docker-compose.yml 中 `api.environment`，全部可覆盖）

| 环境变量 | 默认值 | 说明 |
|---|---|---|
| `QUOTA_DATABASE_URL` | `postgresql://quota:quota@db:5432/quota` | asyncpg DSN |
| `QUOTA_DEFAULT_CAPACITY` | `100` | 桶首次被访问时自动创建的容量 |
| `QUOTA_DEFAULT_REFILL_RATE` | `10` | 默认补充速率（token/秒） |
| `QUOTA_RESERVATION_TTL_SECONDS` | `60` | 预占未提交的存活时间，超时自动取消归还 |
| `QUOTA_REAPER_INTERVAL_SECONDS` | `1` | 后台回收轮询间隔（秒） |
| `QUOTA_REAPER_BATCH_SIZE` | `200` | 每轮最多回收行数（`FOR UPDATE SKIP LOCKED`） |
| `QUOTA_DB_MIN_SIZE` / `QUOTA_DB_MAX_SIZE` | `2` / `10` | 连接池大小 |
| `QUOTA_DB_CONNECT_TIMEOUT` | `60` | 启动时等待数据库的最长秒数 |

应用启动时自动创建表与存储函数（幂等），无需手工迁移。

### 不使用 Docker（本地运行）

```bash
pip install -r requirements.txt
export QUOTA_DATABASE_URL='postgresql://quota:quota@localhost:5432/quota'
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

---

## API

所有业务接口均为 `POST` JSON，字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `tenant` | string，非空 | 租户标识 |
| `subject` | string，非空 | 主体标识（用户/服务账号等） |
| `action` | string，非空 | 操作标识 |
| `cost` | number，> 0 | 本次申请的 token 数 |
| `request_id` | string，非空 | 客户端生成的幂等键（建议 UUID） |

### 1. 预占 `POST /v1/reserve`

**成功 `200`**：三级桶同时足额，立即扣减，预占在 TTL 内有效。

```json
{
  "outcome": "granted",
  "request_id": "req-1",
  "cost": 30,
  "expires_at": "2026-09-17T10:00:00+00:00",
  "available": [
    {"level": "tenant",  "tenant": "acme", "subject": null,   "action": null,
     "tokens_available": 100.0, "capacity": 100.0, "refill_rate": 5.0},
    {"level": "subject", "tenant": "acme", "subject": "bob", "action": null,
     "tokens_available": 100.0, "capacity": 100.0, "refill_rate": 5.0},
    {"level": "action",  "tenant": "acme", "subject": "bob", "action": "read",
     "tokens_available": 100.0, "capacity": 100.0, "refill_rate": 5.0}
  ],
  "replayed": false
}
```

**拒绝 `429`**：任一级不足。`levels` 列出**所有**不足层级；
`retry_after_seconds` 取其中最长等待秒数（向上取整）。
若某级 `cost > capacity`（永不可能满足）或该级补充速率为 0，
该级 `retryable=false`、`retry_after_seconds=null`，顶层 `retry_after_seconds`
也为 `null`。

```json
{
  "outcome": "denied",
  "request_id": "req-2",
  "cost": 60,
  "levels": [
    {"level": "tenant",  "tokens_available": 40, "required": 60,
     "retry_after_seconds": 4, "retryable": true},
    {"level": "subject", "tokens_available": 40, "required": 60,
     "retry_after_seconds": 4, "retryable": true},
    {"level": "action",  "tokens_available": 10, "required": 60,
     "retry_after_seconds": 10, "retryable": true}
  ],
  "retry_after_seconds": 10,
  "replayed": false
}
```

拒绝不扣减任何 token。

**幂等行为**：

- 相同 `request_id` + 相同参数 → 返回首次结果，`replayed=true`，不二次扣减。
- 相同 `request_id` + 任一参数变化 → `409`
  （`detail.error = request_id_conflict`）。

### 2. 提交 `POST /v1/commit`

请求体与 reserve 完全一致（用于指纹校验）。提交**不再扣减** token，
只是把预占标记为 `committed`。

- 首次提交：`200 {"outcome":"committed","replayed":false}`
- 重复提交：`200 {"outcome":"committed","replayed":true}`（幂等）
- `request_id` 不存在：`404`
- 参数与原预占不一致：`409`
- 预占已被取消或已超时：`409 reservation_cancelled / reservation_expired`
- 原申请本身被拒绝（denied）：`409 reservation_denied`

### 3. 取消 `POST /v1/cancel`

归还三级桶的预占 token，**保证只归还一次**。

- 首次取消（预占仍有效）：
  `200 {"outcome":"cancelled","refunded":true,"replayed":false}`
- 重复取消：`200 {"outcome":"cancelled","refunded":false,"replayed":true}`
- 取消时预占刚好已被超时回收：
  `200 {"outcome":"expired","refunded":false,"replayed":true}`
  （归还已由回收完成，取消仍按成功处理，调用方无需区分补偿）
- 已提交/被拒绝的预占取消：`409`，且**不归还**
- `request_id` 不存在：`404`；参数不一致：`409`

归还的 token 会先做惰性补充再叠加，并按容量上限截断，不会超过桶容量。

### 4. 管理接口

| 方法与路径 | 说明 |
|---|---|
| `PUT /admin/policies/tenant?tenant=t` | 设置租户级容量与补充速率 |
| `PUT /admin/policies/subject?tenant=t&subject=s` | 主体级 |
| `PUT /admin/policies/action?tenant=t&subject=s&action=a` | 操作级 |
| `GET /admin/policies/{level}?...` | 查看单级策略 |
| `GET /admin/buckets?tenant=t` | 列出该租户全部桶及实时 token |
| `GET /admin/reservations/{request_id}` | 查看预占状态机 |
| `POST /admin/recycle` | 立即触发一次过期回收，返回 `{"expired": N}` |

策略请求体：`{"capacity": 100, "refill_rate": 5}`
（`capacity > 0`，`refill_rate >= 0`，单位 token/秒）。

更新策略不触碰 `quota_reservations`，因此：

- 已有预占的到期时间、状态、金额完全不变；
- 降低容量时，桶当前 token 先惰性补充再按新容量截断；
- 新申请立即使用新容量与速率。

桶也可以不预先配置：首次 reserve 时按 `QUOTA_DEFAULT_*` 自动创建。

---

## 令牌桶与并发模型

### 三级桶

每个 `(tenant, subject, action)` 申请同时作用于三行：

```
(tenant=t, subject='', action='')              -- 租户级
(tenant=t, subject=s,  action='')              -- 主体级
(tenant=t, subject=s,  action=a)               -- 操作级
```

### 惰性补充

不依赖定时器。读取桶时按距上次更新的时长补充：

```
available = min(capacity, tokens + refill_rate × (now − updated_at))
```

授予时三级同时写回 `tokens = available − cost`。

### 原子性（`quota_reserve` 存储函数）

1. 先以 `INSERT ... ON CONFLICT DO NOTHING` 抢占 `request_id`
   （唯一主键即分布式幂等屏障）；败者读取胜者决策并回放。
2. 指纹不符直接返回 `conflict`。
3. 以 `SELECT ... FOR UPDATE` 按 **tenant → subject → action 固定顺序**
   锁三级桶（缺失行先 upsert 再锁），因此：
   - 并发申请被串行化，余额判断与扣减在同一事务中，**不可能超卖**；
   - 所有事务加锁顺序一致，**不可能死锁**。
4. 任一级不足：不扣减，持久化拒绝决策（供同 `request_id` 回放），
   返回全部不足层级。
5. 全部足额：三级同时扣减，预占置为 `granted` 并记录到期时间。

### 取消与超时回收的竞态

- 取消（`quota_cancel`）先锁预占行，`granted → cancelled` 后归还。
- 回收（`quota_recycle_expired`）用 `FOR UPDATE SKIP LOCKED` 批量选
  `granted 且 expires_at <= now()` 的行，**拿到锁后再次复查状态**，
  已被取消的行直接跳过。
- 两条路径只有一个能完成状态翻转，归还函数只被翻转者调用，
  因此取消与回收并发、多副本同时回收，都只会归还一次。
  多个 API 副本可同时运行 reaper。

### 状态机

```
                 reserve 足额
                 ┌────────► granted ──commit──► committed（终态）
 (request_id) ───┤            │  │
                 │      cancel│  └─TTL/reaper─► expired（终态，已归还）
                 │            ▼
                 │        cancelled（终态，已归还）
                 └─不足──► denied（终态，回放拒绝决策）
```

重复提交/取消均为幂等成功；非法流转（提交已取消、取消已提交等）返回 409。

### 典型时序建议

```
t0  POST /v1/reserve  → granted（拿到 expires_at）
t1  执行业务
t2a 业务成功 → POST /v1/commit
t2b 业务失败 → POST /v1/cancel
    进程崩溃/忘记收尾 → TTL 后由 reaper 自动 cancel 并归还
```

---

## 本地开发与测试

测试使用 [`pgserver`](https://pypi.org/project/pgserver/) 在内嵌目录启动
PostgreSQL，无需 Docker 或本机数据库：

```bash
pip install -r requirements.txt
pip install pytest pytest-asyncio httpx pgserver
pytest -q
```

测试覆盖：三级扣减、多级不足与最长 retry_after、不可重试场景、
授予/拒绝的幂等回放、改参冲突、提交幂等不扣费、取消只归还一次、
30 并发不超卖、同一 request_id 并发只有一次扣减、
取消与回收并发只归还一次、过期释放容量、策略更新不影响存量预占。

手工触发回收：`POST /admin/recycle`（生产中由后台循环自动执行）。
