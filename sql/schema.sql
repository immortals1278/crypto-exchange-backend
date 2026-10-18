-- 启用 UUID 扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 用户表
CREATE TABLE IF NOT EXISTS users (
    id UUID NOT NULL PRIMARY KEY,          -- Go 端使用 UUID v7 生成
    email VARCHAR(255) NOT NULL UNIQUE,
    password_hash VARCHAR(255) NOT NULL,
    created_at BIGINT NOT NULL,            -- Unix 毫秒，由 Go 生成
    updated_at BIGINT NOT NULL             -- Unix 毫秒，由 Go 生成
);

-- 账户（钱包）表
CREATE TABLE IF NOT EXISTS accounts (
    id UUID NOT NULL PRIMARY KEY,          -- Go 端使用 UUID v7 生成
    user_id UUID NOT NULL REFERENCES users(id),
    currency VARCHAR(10) NOT NULL,
    balance DECIMAL(38, 18) NOT NULL DEFAULT 0,
    locked DECIMAL(38, 18) NOT NULL DEFAULT 0,
    created_at BIGINT NOT NULL,            -- Unix 毫秒，由 Go 生成
    updated_at BIGINT NOT NULL,            -- Unix 毫秒，由 Go 生成
    UNIQUE(user_id, currency),
    CHECK (balance >= 0),
    CHECK (locked >= 0)
);

-- 订单表
-- side:   1=BUY, 2=SELL
-- status: 1=NEW, 2=PARTIALLY_FILLED, 3=FILLED, 4=CANCELED, 5=REJECTED
CREATE TABLE IF NOT EXISTS orders (
    id UUID NOT NULL PRIMARY KEY,          -- Go 端使用 UUID v7 生成
    user_id UUID NOT NULL REFERENCES users(id),
    symbol VARCHAR(20) NOT NULL,           -- 例如 "BTC-USD"
    side SMALLINT NOT NULL,                -- 1=BUY, 2=SELL
    price DECIMAL(38, 18) NOT NULL,
    quantity DECIMAL(38, 18) NOT NULL,
    filled_quantity DECIMAL(38, 18) NOT NULL DEFAULT 0,
    status SMALLINT NOT NULL,              -- 1=NEW, 2=PARTIALLY_FILLED, 3=FILLED, 4=CANCELED, 5=REJECTED
    created_at BIGINT NOT NULL,            -- Unix 毫秒，由 Go 生成
    updated_at BIGINT NOT NULL             -- Unix 毫秒，由 Go 生成
);

-- 成交表（成交历史）
CREATE TABLE IF NOT EXISTS trades (
    id UUID NOT NULL PRIMARY KEY,          -- Go 端使用 UUID v7 生成
    maker_order_id UUID NOT NULL REFERENCES orders(id),
    taker_order_id UUID NOT NULL REFERENCES orders(id),
    symbol VARCHAR(20) NOT NULL,
    price DECIMAL(38, 18) NOT NULL,
    quantity DECIMAL(38, 18) NOT NULL,
    created_at BIGINT NOT NULL             -- Unix 毫秒，由 Go 生成
);

-- 性能索引
CREATE INDEX IF NOT EXISTS idx_orders_user_id ON orders(user_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_symbol_side_price ON orders(symbol, side, price); -- 供撮合引擎使用

-- ----------------------------------------------------------
-- Outbox 模式（消息可靠传递）
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS outbox_messages (
    id              UUID        NOT NULL PRIMARY KEY,    -- UUID v7
    aggregate_id    VARCHAR(64) NOT NULL,                -- 业务 ID
    aggregate_type  VARCHAR(32) NOT NULL,                -- 事件分类
    topic           VARCHAR(128) NOT NULL,                -- 目标 Kafka topic
    partition_key   VARCHAR(64) NOT NULL,                -- Kafka partition key
    payload         BYTEA       NOT NULL,                -- 序列化的事件 payload
    status          SMALLINT    NOT NULL DEFAULT 0,       -- 0=Pending, 1=Published
    retry_count     INT         NOT NULL DEFAULT 0,      -- 已重试次数
    created_at      BIGINT      NOT NULL,                -- 创建时间
    published_at    BIGINT      NOT NULL DEFAULT 0       -- 成功发送时间
);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_status_created_at
    ON outbox_messages (status, created_at)
    WHERE status = 0;

CREATE INDEX IF NOT EXISTS idx_outbox_messages_aggregate_id
    ON outbox_messages (aggregate_id);

-- ----------------------------------------------------------
-- Leader 选举（Kafka 分区选主）
-- ----------------------------------------------------------
CREATE TABLE IF NOT EXISTS partition_leader_locks (
    partition     VARCHAR(128) NOT NULL PRIMARY KEY,  -- Partition 唯一标识
    leader_id     VARCHAR(255) NOT NULL,              -- Leader 实例 ID
    fencing_token BIGINT       NOT NULL DEFAULT 1,    -- 单调递增防脑裂令牌
    expires_at    BIGINT       NOT NULL               -- 租约到期时间
);

CREATE INDEX IF NOT EXISTS idx_partition_leader_locks_expires_at
    ON partition_leader_locks (expires_at);