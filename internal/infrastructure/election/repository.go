package election

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Repository struct {
	pool *sql.DB
}

func NewRepository(pool *sql.DB) *Repository {
	return &Repository{pool: pool}
}

// 租约有效时间
var leaseDuration = 15 * time.Second

// 竞选 leader
//
// MySQL 没有 ON CONFLICT ... RETURNING，这里用「事务 + ON DUPLICATE KEY UPDATE + 回读」实现等价语义：
//   - 无记录：插入，fencing_token = 1
//   - 记录已过期（expires_at < now）：抢占成功，leader_id 换成本实例，fencing_token + 1
//   - 记录未过期：IF(...) 保持原值，回读 leader_id != 本实例 → 竞选失败
func (r *Repository) AcquireLock(ctx context.Context, partition, instanceID string) (int64, bool, error) {
	now := time.Now().UnixMilli()
	expiresAt := time.Now().Add(leaseDuration).UnixMilli()

	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return 0, false, fmt.Errorf("开启事务失败: %w", err)
	}
	defer tx.Rollback() // 提交后此调用为 no-op

	_, err = tx.ExecContext(ctx, `
		INSERT INTO partition_leader_locks (partition, leader_id, fencing_token, expires_at)
		VALUES (?, ?, 1, ?)
		ON DUPLICATE KEY UPDATE
			leader_id     = IF(expires_at < ?, VALUES(leader_id), leader_id),
			fencing_token = IF(expires_at < ?, fencing_token + 1, fencing_token),
			expires_at    = IF(expires_at < ?, VALUES(expires_at), expires_at)`,
		partition, instanceID, expiresAt, now, now, now)
	if err != nil {
		return 0, false, fmt.Errorf("写入 leader 锁失败: %w", err)
	}

	// 回读当前持有者，判断是否抢到
	var fencingToken int64
	var leaderID string
	err = tx.QueryRowContext(ctx, `
		SELECT fencing_token, leader_id
		FROM partition_leader_locks
		WHERE partition = ?
		FOR UPDATE`, partition).Scan(&fencingToken, &leaderID)
	if err != nil {
		return 0, false, fmt.Errorf("读取 leader 锁失败: %w", err)
	}

	if leaderID != instanceID {
		// 锁仍被他人有效持有，竞选失败
		return 0, false, nil
	}

	if err := tx.Commit(); err != nil {
		return 0, false, fmt.Errorf("提交 leader 锁失败: %w", err)
	}
	return fencingToken, true, nil
}

// 延长租约
func (r *Repository) ExtendLease(ctx context.Context, partition, instanceID string, fencingToken int64) error {
	expiresAt := time.Now().Add(leaseDuration)

	cmdTag, err := r.pool.ExecContext(ctx, `
		UPDATE partition_leader_locks
		SET expires_at = ?
		WHERE partition = ? AND leader_id = ? AND fencing_token = ?`,
		expiresAt.UnixMilli(), partition, instanceID, fencingToken)

	if err != nil {
		return fmt.Errorf("延长租约失败: %w", err)
	}
	affected, err := cmdTag.RowsAffected()
	if err != nil {
		return fmt.Errorf("延长租约失败: %w", err)
	}
	if affected == 0 {
		return fmt.Errorf("租约已失效")
	}
	return nil
}

func (r *Repository) ReleaseLock(ctx context.Context, partition, instanceID string) error {
	_, err := r.pool.ExecContext(ctx, `
		UPDATE partition_leader_locks
		SET expires_at = 0
		WHERE partition = ? AND leader_id = ?`,
		partition, instanceID)
	return err
}
