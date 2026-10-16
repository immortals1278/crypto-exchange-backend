# 数字资产撮合交易系统 · 后端

> 一个用于学习与面试展示的加密货币撮合交易后端，实现「订单接入 → 内存撮合 → 结算清分」的完整交易链路。
> 重点打磨了分布式场景下的两个经典难题：**撮合引擎的高可用（Leader 选举 + Fencing Token 防脑裂）** 与 **消息的可靠投递（Outbox 模式 + at-least-once）**。

- 项目链接：https://github.com/immortals1278/crypto-exchange-backend
- 技术栈：**Go + Gin + PostgreSQL + Redis + Kafka**

---

## 一、项目简介

系统采用**微服务架构**，按职责拆分为三个可独立部署、可水平扩展的服务：

| 服务 | 端口 | 职责 |
| --- | --- | --- |
| **API Gateway（网关）** | `8100` | 统一入口：限流、幂等、CORS、反向代理 |
| **Order Service（订单服务）** | `8103` | 订单接入、资金锁定、订单落库、Outbox 投递、账户结算 |
| **Matching Engine（撮合引擎）** | `8101` | 内存订单簿、价格-时间优先撮合、Leader 选举、状态快照 |

服务间通过 **Kafka 事件驱动**通信，解耦下单与撮合、撮合与结算，天然支持削峰与异步处理。

---

## 二、技术选型

| 组件 | 选型 | 说明 |
| --- | --- | --- |
| 语言 / 框架 | Go 1.26 / Gin | 高性能、易并发，Gin 提供 HTTP 路由与中间件 |
| 数据库 | PostgreSQL（`pgx/v5`） | 事务、行级锁、`ON CONFLICT` 原子更新、`SKIP LOCKED` |
| 缓存 | Redis（`go-redis/v9`） | 订单簿快照、限流计数、幂等 Key 存储 |
| 消息队列 | Kafka（`franz-go`） | 事件驱动、消费组、手动提交 offset |
| 数值精度 | `shopspring/decimal` | 资金/价格精确十进制运算，避免浮点误差 |
| 日志 | `zap` | 结构化、高性能日志 |

---

## 三、系统架构

```
                          ┌──────────────┐
                          │    Client    │
                          └──────┬───────┘
                                 │ HTTP（携带 Idempotency-Key）
                          ┌──────▼───────┐
                          │ API Gateway  │  :8100
                          │ 限流/幂等/CORS│
                          │ 反向代理      │
                          └──────┬───────┘
                                 │ 反向代理
                          ┌──────▼────────┐
                          │ Order Service │  :8103
                          │ 下单+锁资金    │
                          │ 订单落库       │
                          │ Outbox 事务写入│
                          └──┬─────────┬──┘
                   Outbox Worker│        │消费结算事件
                              │        │
                    ┌─────────▼──┐  ┌──▼──────────────┐
                    │   Kafka    │◀─┤ Settlement 消费  │
                    │  (franz-go)│  │ (账户清算/解冻)  │
                    └─────────┬──┘  └─────────────────┘
                ┌─────────────┼───────────────┐
        orders  │      trades │      settlements│
          ┌─────▼─────┐       │                │
          │ Matching   │───────┘                │
          │ Engine     │────────────────────────┘
          │ (仅Leader) │  :8101
          │ 内存订单簿  │
          │ Leader选举  │
          └─────┬──────┘
                │ 订单簿快照
          ┌─────▼─────┐
          │   Redis    │  订单簿缓存 / 限流 / 幂等
          └───────────┘
```

**交易链路（一次下单的完整旅程）：**

1. 客户端携带 `Idempotency-Key` 调用 `POST /api/v1/orders`，网关先做**限流**与**幂等**校验，再反向代理到 Order Service。
2. Order Service 在**同一个数据库事务**内完成：校验账户 → 锁定资金 → 订单落库 → 写入 Outbox 表，保证「订单」与「待发消息」原子提交。
3. **Outbox Worker** 定时扫描 `outbox_messages` 中 `Pending` 记录，投递 `order.placed` 事件到 Kafka，成功后删除记录。
4. **Matching Engine（仅 Leader 实例）** 消费 `order.placed`，在内存订单簿中执行撮合，产出成交、结算与订单簿更新事件。
5. Order Service 消费 `settlement` 事件完成账户清算（解冻/扣减），并通过幂等检查防止重复结算。

---

## 四、核心亮点

### 1. 撮合引擎 Leader 选举 + Fencing Token 防脑裂

- 基于 **PostgreSQL 租约锁** 实现 Leader 选举：`partition_leader_locks` 表通过 `INSERT ... ON CONFLICT ... WHERE expires_at < now` 实现「只有租约过期才能抢占」的原子竞选。
- 每次抢占成功，`fencing_token` **单调递增 +1**，作为该 Leader 的**防脑裂令牌**。
- 成为 Leader 后：重置内存状态 → 从 DB 还原订单簿快照 → 启动 Kafka 消费；失去 Leader 时立即把 fencing token 清零并停止消费，**确保旧 Leader 在脑裂期间无法继续覆盖新 Leader 的快照/结算**。
- 续租每 5s 一次、租约 15s，留出 3 个心跳周期的容错窗口，兼顾故障切换速度与误判稳定性。

### 2. 内存撮合引擎（价格-时间优先 + 自成交保护）

- 每个交易对维护独立的**内存订单簿**（`bids` 降序 / `asks` 升序），撮合采用**价格-时间优先**：取对手方最优价成交，成交价使用挂单方价格（Maker 优先）。
- 支持限价单、市价单、部分成交、撤单，全程使用 `decimal` 精确计算。
- 引擎内用 `sync.Mutex` 串行化对单个订单簿的写入，保证并发安全；`EngineManager` 用 `sync.RWMutex` 管理多个交易对。

### 3. 引擎重启状态还原（快照重建）

- 撮合引擎本身**无状态**，所有「权威数据」沉淀在 PostgreSQL。
- 冷启动时从 DB 读取全部活跃订单（`GetActiveOrders`），按「原数量 − 已成交量」还原订单簿，实现引擎重启后的**秒级状态恢复**，无需依赖内存持久化。
- 恢复完成后将订单簿快照同步到 Redis，供行情/前端查询。

### 4. Outbox 模式保证落库与消息投递的最终一致性

- 「订单落库」与「发送 Kafka 事件」存在经典的**分布式事务难题**，系统用 **Transactional Outbox** 解决：
  - 业务事务内把事件先写入 `outbox_messages` 表（与业务数据同库、同事务）；
  - 后台 Worker 独立扫描并投递，成功后删除记录。
- 查询用 `FOR UPDATE SKIP LOCKED` 支持**多实例并发扫描互不阻塞**；`retry_count` 记录失败次数，配合 grace period 避免投递过早。

### 5. at-least-once 语义 + 指数退避重试

- 消费端关闭自动提交（`DisableAutoCommit`），**处理成功后才手动提交 offset**，避免「先提交后处理」导致的丢消息。
- 处理失败时按 **指数退避**（100ms → 200ms → … → 封顶 30s）原地重试，保证 at-least-once 投递；重试期间仍可响应取消信号，保证优雅退出。
- 配合下游**幂等处理**，把 at-least-once 的副作用收敛掉。

### 6. 网关幂等 + 限流（防重复下单）

- 网关基于 **`Idempotency-Key`**（Redis 存储，24h 过期）拦截重复下单请求，保证客户端重试/网络抖动不会产生重复订单。
- **令牌桶限流**（Redis 实现，默认 100 req/s），Redis 不可用时自动降级为内存限流与内存幂等，保证网关不因 Redis 故障而不可用。
- CORS 统一在网关处理，避免各服务重复配置、产生不一致。

---

## 五、目录结构

```
.
├── cmd/
│   ├── gateway/           # API 网关入口
│   ├── orderService/      # 订单服务入口
│   └── matching-engine/   # 撮合引擎入口
├── internal/
│   ├── api/               # HTTP handler / 参数校验 / 路由注册
│   ├── domain/            # 领域模型、事件定义、Kafka Topic 常量
│   ├── middleware/        # 限流、幂等中间件（Redis + 内存双实现）
│   ├── matching/          # 撮合订阅器、快照还原
│   │   └── engine/        # 撮合引擎、订单簿、引擎管理器
│   ├── order/             # 订单服务、结算订阅器
│   ├── repository/        # Postgres / Redis 数据访问层
│   └── infrastructure/
│       ├── db/            # pgx 连接池、事务上下文
│       ├── election/      # Leader 选举（租约锁 + Fencing Token）
│       ├── kafka/         # producer / consumer 封装（franz-go）
│       ├── outbox/        # Outbox 模型 / Repository / Worker
│       ├── redis/         # Redis 客户端
│       └── logger/        # zap 日志
└── sql/
    └── schema.sql         # 建表语句（订单/账户/成交/outbox/选举锁）
```

---

## 六、关键数据模型

| 表 | 作用 |
| --- | --- |
| `users` / `accounts` | 用户与账户（`balance` + `locked` 双列，`CHECK` 约束保证非负） |
| `orders` | 订单（`side/type/status` 用 SMALLINT 枚举，`filled_quantity` 记录部分成交） |
| `trades` | 成交历史（`maker_order_id` / `taker_order_id`） |
| `outbox_messages` | Outbox 事件（`status/retry_count`，部分索引加速 Pending 扫描） |
| `partition_leader_locks` | 选举锁（`leader_id` + 单调递增 `fencing_token` + `expires_at`） |

---

## 七、快速开始

### 环境变量

| 变量 | 服务 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | order-service / matching-engine | PostgreSQL 连接串 |
| `REDIS_URL` | gateway / matching-engine | Redis 地址 |
| `KAFKA_BROKERS` | 全部 | Kafka broker 列表（逗号分隔） |
| `INSTANCE_ID` | matching-engine | 引擎实例唯一标识（参与选举） |
| `KAFKA_RESET_OFFSET` | 消费端 | 无 offset 时从 `latest` / `earliest` 开始 |
| `ORDER_SERVICE_PORT` | gateway / order-service | 订单服务地址 / 端口 |
| `MATCHING_ENGINE_PORT` | matching-engine | 健康检查端口 |
| `GIN_MODE` | 全部 | `release` 时关闭 topic 自动创建 |

### 启动

```bash
# 1. 初始化数据库（PostgreSQL）
psql "$DATABASE_URL" -f sql/schema.sql

# 2. 启动订单服务
ORDER_SERVICE_PORT=8103 go run ./cmd/orderService

# 3. 启动撮合引擎（可多实例，自动选主）
INSTANCE_ID=engine-1 MATCHING_ENGINE_PORT=8101 go run ./cmd/matching-engine

# 4. 启动网关
go run ./cmd/gateway
```

### 下单示例

```bash
curl -X POST http://localhost:8100/api/v1/orders \
  -H "Content-Type: application/json" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"symbol":"BTC-USD","side":"BUY","type":"LIMIT","price":"50000","quantity":"0.1"}'
```

---

## 八、设计权衡与可改进方向

- **强一致 vs 最终一致**：撮合状态以 DB 为权威、内存为加速缓存，用最终一致换取高吞吐；对一致性要求更高的场景可考虑 Raft（如 etcd/Raft 库）替代 DB 租约锁，减少续租抖动。
- **单订单簿单锁**：当前每个 `Symbol` 一把互斥锁，简单正确；极致吞吐可改为按价格档位细粒度锁或无锁订单簿。
- **Outbox 删除策略**：当前发送成功即删除记录，便于省空间；如需审计/对账可改为标记 `Published` 并定期归档。
- **可观测性**：可补充 metrics（撮合延迟、订单簿深度、Outbox 积压量）与 tracing，便于线上排障。
