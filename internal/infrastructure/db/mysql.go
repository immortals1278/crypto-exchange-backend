package db

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// NewMySQLPool 根据传入的 DBConfig 初始化 MySQL 连接池（*sql.DB 本身即连接池）
func NewMySQLPool(ctx context.Context, cfg DBConfig) (*sql.DB, error) {
	if cfg.URL == "" {
		return nil, fmt.Errorf("数据库 URL 未设置")
	}

	// sql.Open 会解析 DSN 但不会真正建立连接
	pool, err := sql.Open("mysql", cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("解析数据库配置失败: %w", err)
	}

	// 覆盖为我们自定义的连接池参数
	if cfg.MaxOpenConns > 0 {
		pool.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		pool.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.MaxIdleTime > 0 {
		pool.SetConnMaxIdleTime(cfg.MaxIdleTime)
	}
	if cfg.MaxLifeTime > 0 {
		pool.SetConnMaxLifetime(cfg.MaxLifeTime)
	}

	// 主动探测一次，让配置错误（账号/密码/地址）尽早暴露
	if err := pool.PingContext(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("创建数据库连接池失败: %w", err)
	}

	return pool, nil
}
