package repository

import (
	"atlas-trading-infrastructure/internal/infrastructure/db"
	"atlas-trading-infrastructure/internal/order"
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type MySQLRepository struct {
	db *sql.DB
}

func NewMySQLRepository(db *sql.DB) *MySQLRepository {
	return &MySQLRepository{db: db}
}

// 承诺 MySQLRepository 实现了 OrderRepository 接口，如果没实现则通过不了编译
var _ order.OrderRepository = (*MySQLRepository)(nil)
var _ order.DBTransaction = (*MySQLRepository)(nil)

// ExecTx 通过事务实现「成功提交、失败回滚」
func (r *MySQLRepository) ExecTx(ctx context.Context, fn func(ctx context.Context) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}

	// panic 时也要回滚，避免连接泄漏
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	// 将 tx 存进 context，供内部 GetExecutor 取用
	txCtx := context.WithValue(ctx, db.TxKey, tx)
	if err := fn(txCtx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// *sql.DB 和 *sql.Tx 都实现了这三个方法
type DBExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// 有些操作需要在事务内执行，有些不用（单步操作直接执行）
func (r *MySQLRepository) GetExecutor(ctx context.Context) DBExecutor {
	if tx := db.GetTx(ctx); tx != nil {
		return tx
	}
	return r.db
}

func (r *MySQLRepository) ValidateFencingTokenTx(ctx context.Context, partition string, token int64) (bool, error) {
	var currentToken int64

	err := r.GetExecutor(ctx).QueryRowContext(ctx, `
		SELECT fencing_token
		FROM partition_leader_locks
		WHERE partition = ?
		FOR SHARE`,
		partition,
	).Scan(&currentToken)

	if err != nil {
		// 没有 leader 持有锁（系统刚启动）→ 允许通过
		if errors.Is(err, sql.ErrNoRows) {
			return true, nil
		}
		return false, fmt.Errorf("查询 fencing_token 失败: %w", err)
	}

	// 检查是否为最新的 fencingToken
	if token < currentToken {
		return false, nil
	}
	return true, nil
}
