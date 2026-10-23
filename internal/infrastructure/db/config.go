package db

import "time"

// DBConfig 数据库连接池配置（MySQL / database/sql）
type DBConfig struct {
	URL          string        // MySQL DSN，例如 user:pass@tcp(host:3306)/db?parseTime=true
	MaxOpenConns int           // sql.DB.SetMaxOpenConns
	MaxIdleConns int           // sql.DB.SetMaxIdleConns
	MaxIdleTime  time.Duration // sql.DB.SetConnMaxIdleTime
	MaxLifeTime  time.Duration // sql.DB.SetConnMaxLifetime
}

func DefaultDBConfig(url string) DBConfig {
	return DBConfig{
		URL:          url,
		MaxOpenConns: 50,
		MaxIdleConns: 5, // 保持 5 条空闲连接预热
		MaxIdleTime:  5 * time.Minute,
		MaxLifeTime:  1 * time.Hour,
	}
}
