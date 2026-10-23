# 数据库迁移说明：PostgreSQL → MySQL

本文档记录本次将数据库从 **PostgreSQL（`pgx/v5`）** 迁移到 **MySQL（`go-sql-driver/mysql`）** 的全部改动、映射关系与注意事项。

## 一、变更概述

| 维度 | 迁移前 | 迁移后 |
| --- | --- | --- |
| 驱动 | `github.com/jackc/pgx/v5` | `github.com/go-sql-driver/mysql`（标准库 `database/sql`） |
| 连接池 | `pgxpool.Pool` | `*sql.DB`（`database/sql` 自带连接池） |
| 事务 | `pgx.Tx` + `pgx.BeginFunc` | `*sql.Tx` + 自实现 `BeginTx` 封装 |
| 占位符 | `$1, $2, ...` | `?` |
| 批量插入 | `CopyFrom`（COPY 协议） | 多值 `INSERT ... VALUES (...),(...)` |
| 无行错误 | `pgx.ErrNoRows` | `sql.ErrNoRows` |
| 建表脚本 | `sql/schema.sql` | `sql/schema_mysql.sql` |

## 二、依赖变更

`go.mod`：

- 移除：`github.com/jackc/pgx/v5`（及其间接依赖 `pgpassfile`、`pgservicefile`、`puddle/v2`）。
- 新增：`github.com/go-sql-driver/mysql v1.8.1`。

> 已执行 `go mod tidy` 清理 `go.sum`。

## 三、文件变更清单

### 新增

| 文件 | 说明 |
| --- | --- |
| `internal/infrastructure/db/mysql.go` | MySQL 连接池构造（替代原 `postgres.go`） |
| `internal/repository/mysql.go` | `MySQLRepository` 基类（替代原 `postgres.go`） |
| `sql/schema_mysql.sql` | MySQL 版建表脚本 |

### 删除

| 文件 | 说明 |
| --- | --- |
| `internal/infrastructure/db/postgres.go` | 被 `mysql.go` 替代 |
| `internal/repository/postgres.go` | 被 `mysql.go` 替代 |

### 修改

| 文件 | 改动 |
| --- | --- |
| `internal/infrastructure/db/context.go` | `GetTx` 返回 `*sql.Tx` |
| `internal/infrastructure/db/config.go` | `MinOpenConns` → `MaxIdleConns` |
| `internal/repository/order_repository.go` | SQL 占位符、`CopyFrom` → 多值 INSERT、修复列名/占位符 bug |
| `internal/repository/account_repository.go` | SQL 占位符、修复列名/尾逗号/约束 bug |
| `internal/repository/trade_repository.go` | 占位符 + 接收者类型重命名 |
| `internal/repository/user_repository.go` | 表名 `user`→`users`、占位符 + 接收者重命名 |
| `internal/infrastructure/outbox/repository.go` | 占位符、`CopyFrom` → 多值 INSERT、`= ANY(?)` → `IN (...)` |
| `internal/infrastructure/election/repository.go` | 选主逻辑重写（见下文） |
| `internal/order/event_subscribers.go` | `pgx.ErrNoRows` → `sql.ErrNoRows` |
| `cmd/orderService/main.go` | `NewPostgresPool`→`NewMySQLPool`、`NewPostgresRepository`→`NewMySQLRepository` |
| `cmd/matching-engine/main.go` | 同上 |
| `go.mod` / `go.sum` | 依赖替换 |
| `README.md` | 技术栈、DSN、建表命令等同步 |

## 四、接口与类型映射（pgx → database/sql）

| pgx | database/sql |
| --- | --- |
| `pgxpool.ParseConfig` + `NewWithConfig` | `sql.Open("mysql", dsn)` |
| `poolConfig.MaxConns` | `db.SetMaxOpenConns` |
| `poolConfig.MinConns` | `db.SetMaxIdleConns` |
| `poolConfig.MaxConnIdleTime` | `db.SetConnMaxIdleTime` |
| `poolConfig.MaxConnLifetime` | `db.SetConnMaxLifetime` |
| `pgx.BeginFunc(ctx, pool, fn)` | 自实现 `BeginTx` + `Commit/Rollback` |
| `pgx.Tx` | `*sql.Tx` |
| `Exec` / `Query` / `QueryRow` | `ExecContext` / `QueryContext` / `QueryRowContext` |
| `pgconn.CommandTag` | `sql.Result` |
| `pgx.Rows` / `pgx.Row` | `*sql.Rows` / `*sql.Row` |
| `pgx.ErrNoRows` | `sql.ErrNoRows` |

### 事务透传进 context 的方式不变

原「把 `tx` 存进 `context`，`GetExecutor` 优先取 tx」的模式是驱动无关的，迁移后保持一致：

```go
// 事务内
txCtx := context.WithValue(ctx, db.TxKey, tx) // tx 现在是 *sql.Tx
// 取值
func GetTx(ctx context.Context) *sql.Tx { tx, _ := ctx.Value(TxKey).(*sql.Tx); return tx }
```

`*sql.DB` 与 `*sql.Tx` 都实现了 `ExecContext/QueryContext/QueryRowContext`，因此 `DBExecutor` 接口可原样保留。

## 五、SQL 方言差异处理

| 原 PostgreSQL 写法 | MySQL 写法 |
| --- | --- |
| `$1, $2, ...` | `?`（所有位置统一为 `?`） |
| `tx.CopyFrom(...)` | 多值 `INSERT ... VALUES (...),(...),...` |
| `id = ANY($1)` | `id IN (?, ?, ...)`（动态拼占位符） |
| `ON CONFLICT ... DO UPDATE ... RETURNING` | `ON DUPLICATE KEY UPDATE` + 事务内回读（见下文） |
| `FOR SHARE` | `FOR SHARE`（MySQL 8.0 支持） |
| `FOR UPDATE SKIP LOCKED` | `FOR UPDATE SKIP LOCKED`（MySQL 8.0.1+ 支持） |
| `BYTEA` | `BLOB` |
| `UUID` 类型 + `uuid-ossp` 扩展 | `CHAR(36)`，Go 端 `google/uuid` 的 `Value()/Scan()` 直接读写字符串 |
| 部分索引 `CREATE INDEX ... WHERE status = 0` | 普通联合索引 `(status, created_at)`（MySQL 不支持部分索引） |

## 六、关键逻辑重写说明

### 1. Leader 选举 `AcquireLock`（重点）

原实现依赖 PostgreSQL 的 `INSERT ... ON CONFLICT ... DO UPDATE ... WHERE ... RETURNING fencing_token` 一次拿到「是否抢到」和「新 fencing_token」。

MySQL 没有 `RETURNING`，改为**事务 + `ON DUPLICATE KEY UPDATE` + `FOR UPDATE` 回读**：

```sql
INSERT INTO partition_leader_locks (partition, leader_id, fencing_token, expires_at)
VALUES (?, ?, 1, ?)
ON DUPLICATE KEY UPDATE
    leader_id     = IF(expires_at < ?, VALUES(leader_id), leader_id),
    fencing_token = IF(expires_at < ?, fencing_token + 1, fencing_token),
    expires_at    = IF(expires_at < ?, VALUES(expires_at), expires_at);
-- 随后在事务内 SELECT ... FOR UPDATE 回读 leader_id，判断是否抢到
```

语义等价：

- 无记录 → 插入，`fencing_token = 1`；
- 记录已过期 → `IF` 条件成立，换成新 leader、token+1；
- 记录未过期 → `IF` 保持原值，回读的 `leader_id != 本实例` → 竞选失败。

> 注意：`VALUES()` 在 MySQL 8.0.20+ 已标记为 deprecated（仍可用，仅告警）。如需消除告警，可改用 8.0.19+ 的行别名语法 `INSERT ... AS new ... new.leader_id`。为兼顾兼容性此处保留 `VALUES()`。

### 2. 批量插入 `CopyFrom` → 多值 INSERT

`BatchCreateOrders` / `BatchInsert` 原先用 `pgx` 的 COPY 协议批量写入，MySQL 无此能力，改为拼接多值 `INSERT`：

```go
INSERT INTO orders (id, user_id, ...) VALUES (?,?,...),(?,?,...),...
```

功能等价；批量吞吐从 COPY 协议降级为多值 INSERT，对当前撮合/outbox 热路径影响可接受。

### 3. `= ANY(?)` → `IN (?, ?, ...)`

`MarkPublishedWorker` 原先 `WHERE id = ANY($1)`，改为动态生成 `IN` 占位符列表。

## 七、Schema 变更

详见 `sql/schema_mysql.sql`。主要差异：

1. 去掉 `CREATE EXTENSION "uuid-ossp"`。
2. 所有 `UUID` 列改为 `CHAR(36)`。
3. `BYTEA` → `BLOB`。
4. `outbox_messages` 的部分索引改为普通联合索引。
5. 其余（`BIGINT`、`SMALLINT`、`DECIMAL(38,18)`、`CHECK`）MySQL 均支持。

> MySQL 版本要求：**8.0+**（`FOR SHARE`、`SKIP LOCKED` 均需 8.0；`CHECK` 约束 8.0.16+ 才真正生效）。

## 八、环境变量 / DSN 变更

`DATABASE_URL` 的格式需要从 PostgreSQL 连接串改为 MySQL DSN：

```text
# 迁移前（PostgreSQL）
postgres://user:pass@localhost:5432/atlas?sslmode=disable

# 迁移后（MySQL）
user:pass@tcp(localhost:3306)/atlas?parseTime=true&charset=utf8mb4
```

推荐在 DSN 中带上 `parseTime=true` 与 `charset=utf8mb4`（与建表字符集一致）。

## 九、顺带修复的既有 bug

迁移过程中发现并修复了以下与 SQL 相关的既有问题（原代码在 PostgreSQL 下同样会出错）：

| 位置 | 问题 | 修复 |
| --- | --- | --- |
| `order_repository.CreateOrder` | 列名 `userid` 与 schema 不符、8 列只传 7 参、字段顺序错 | 改为 `user_id`，8 列 8 参 |
| `order_repository.UpdateOrder` | `WHERE id = $4` 但只传 3 参（缺少 `$3`） | 改为 `WHERE id = ?` 且 3 参 |
| `order_repository.scanOrder` | SELECT 含 `user_id` 但未扫描，`GetOrderForUpdate` 返回的 `UserID` 为空 | 补扫 `user_id`，统一 8 列 |
| `account_repository.LockFunds/BatchLockFunds` | `SET ... locked = locked + $1,` 尾逗号导致语法错误、`userid` 列名 | 去尾逗号、改 `user_id` |
| `account_repository.UnlockFunds` | 解锁条件误写成 `balance >= $1`（应为 `locked >= $1`） | 改为 `locked >= ?` |
| `account_repository.UpdateBalance` | `time.Now().UnixMilli` 少了 `()`（传的是函数值） | 改为 `time.Now().UnixMilli()` |
| `user_repository.CreateUser` | 表名 `user`（实际为 `users`，且 `user` 是 MySQL 保留字） | 改为 `users` |
| `repository.ValidateFencingTokenTx` | 原实现任何错误都返回 `true`（吞掉真实 DB 错误） | 仅 `sql.ErrNoRows` 返回 `true`，其余返回错误 |

> 这些修复**不改变业务逻辑**，只修正了会触发运行时错误/语法错误的缺陷。

## 十、迁移步骤

```bash
# 1. 建库建表（MySQL 8.0+）
mysql -h <host> -P 3306 -u <user> -p <dbname> < sql/schema_mysql.sql

# 2. 设置环境变量（DSN 格式见上文）
export DATABASE_URL='user:pass@tcp(localhost:3306)/atlas?parseTime=true&charset=utf8mb4'

# 3. 启动各服务（与之前一致）
ORDER_SERVICE_PORT=8103 go run ./cmd/orderService
INSTANCE_ID=engine-1 MATCHING_ENGINE_PORT=8101 go run ./cmd/matching-engine
go run ./cmd/gateway
```

## 十一、验证情况

- `go build ./...` 通过。
- `go vet ./...` 无新增问题（`event_subscribers.go` 中 4 处 `fmt.Errorf` 缺少 `%w` 为迁移前既有问题，未在本此改动范围内处理）。
