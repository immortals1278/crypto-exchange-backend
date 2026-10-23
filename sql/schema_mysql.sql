-- MySQL 8.0+ 建表语句（本项目已从 PostgreSQL 迁移到 MySQL）
-- UUID 统一以 CHAR(36) 存储，Go 端 google/uuid 的 Value()/Scan() 直接以字符串读写

-- 用户表
CREATE TABLE IF NOT EXISTS users (
    id            CHAR(36)     NOT NULL PRIMARY KEY,   -- Go 端使用 UUID v7 生成
    email         VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at    BIGINT       NOT NULL,               -- Unix 毫秒，由 Go 生成
    updated_at    BIGINT       NOT NULL                -- Unix 毫秒，由 Go 生成
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 账户（钱包）表
CREATE TABLE IF NOT EXISTS accounts (
    id         CHAR(36)       NOT NULL PRIMARY KEY,    -- Go 端使用 UUID v7 生成
    user_id    CHAR(36)       NOT NULL,
    currency   VARCHAR(10)    NOT NULL,
    balance    DECIMAL(38,18) NOT NULL DEFAULT 0,
    locked     DECIMAL(38,18) NOT NULL DEFAULT 0,
    created_at BIGINT         NOT NULL,                -- Unix 毫秒，由 Go 生成
    updated_at BIGINT         NOT NULL,                -- Unix 毫秒，由 Go 生成
    UNIQUE KEY uk_accounts_user_currency (user_id, currency),
    KEY idx_accounts_user_id (user_id),
    CONSTRAINT fk_accounts_user FOREIGN KEY (user_id) REFERENCES users(id),
    CONSTRAINT chk_accounts_balance CHECK (balance >= 0),
    CONSTRAINT chk_accounts_locked CHECK (locked >= 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 订单表
-- side:   1=BUY, 2=SELL
-- status: 1=NEW, 2=PARTIALLY_FILLED, 3=FILLED, 4=CANCELED, 5=REJECTED
CREATE TABLE IF NOT EXISTS orders (
    id              CHAR(36)       NOT NULL PRIMARY KEY,
    user_id         CHAR(36)       NOT NULL,
    symbol          VARCHAR(20)    NOT NULL,           -- 例如 "BTC-USD"
    side            SMALLINT       NOT NULL,           -- 1=BUY, 2=SELL
    price           DECIMAL(38,18) NOT NULL,
    quantity        DECIMAL(38,18) NOT NULL,
    filled_quantity DECIMAL(38,18) NOT NULL DEFAULT 0,
    status          SMALLINT       NOT NULL,           -- 1=NEW, 2=PARTIALLY_FILLED, 3=FILLED, 4=CANCELED, 5=REJECTED
    created_at      BIGINT         NOT NULL,           -- Unix 毫秒，由 Go 生成
    updated_at      BIGINT         NOT NULL,           -- Unix 毫秒，由 Go 生成
    KEY idx_orders_user_id (user_id),
    KEY idx_orders_status (status),
    KEY idx_orders_symbol_side_price (symbol, side, price), -- 供撮合引擎使用
    CONSTRAINT fk_orders_user FOREIGN KEY (user_id) REFERENCES users(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 成交表（成交历史）
CREATE TABLE IF NOT EXISTS trades (
    id             CHAR(36)       NOT NULL PRIMARY KEY,
    maker_order_id CHAR(36)       NOT NULL,
    taker_order_id CHAR(36)       NOT NULL,
    symbol         VARCHAR(20)    NOT NULL,
    price          DECIMAL(38,18) NOT NULL,
    quantity       DECIMAL(38,18) NOT NULL,
    created_at     BIGINT         NOT NULL,           -- Unix 毫秒，由 Go 生成
    KEY idx_trades_symbol (symbol),
    CONSTRAINT fk_trades_maker FOREIGN KEY (maker_order_id) REFERENCES orders(id),
    CONSTRAINT fk_trades_taker FOREIGN KEY (taker_order_id) REFERENCES orders(id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------
-- Outbox 模式（消息可靠传递）
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_messages (
    id             CHAR(36)     NOT NULL PRIMARY KEY,   -- UUID v7
    aggregate_id   VARCHAR(64)  NOT NULL,               -- 业务 ID
    aggregate_type VARCHAR(32)  NOT NULL,               -- 事件分类
    topic          VARCHAR(128) NOT NULL,               -- 目标 Kafka topic
    partition_key  VARCHAR(64)  NOT NULL,               -- Kafka partition key
    payload        BLOB         NOT NULL,               -- 序列化的事件 payload
    status         SMALLINT     NOT NULL DEFAULT 0,     -- 0=Pending, 1=Published
    retry_count    INT          NOT NULL DEFAULT 0,     -- 已重试次数
    created_at     BIGINT       NOT NULL,               -- 创建时间
    published_at   BIGINT       NOT NULL DEFAULT 0,     -- 成功发送时间
    -- MySQL 不支持部分索引，退化为普通联合索引 (status, created_at)
    KEY idx_outbox_messages_status_created_at (status, created_at),
    KEY idx_outbox_messages_aggregate_id (aggregate_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- ----------------------------------------------------------
-- Leader 选举（Kafka 分区选主）
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS partition_leader_locks (
    partition     VARCHAR(128) NOT NULL PRIMARY KEY,    -- Partition 唯一标识
    leader_id     VARCHAR(255) NOT NULL,                -- Leader 实例 ID
    fencing_token BIGINT       NOT NULL DEFAULT 1,      -- 单调递增防脑裂令牌
    expires_at    BIGINT       NOT NULL,                -- 租约到期时间
    KEY idx_partition_leader_locks_expires_at (expires_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
