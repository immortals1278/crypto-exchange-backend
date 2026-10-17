package election

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// 租约有效时间
var leaseDuration = 15 * time.Second

// 竞选leader
func (r *Repository) AcquireLock(ctx context.Context, partition, instanceID string) (fencingToken int64, acquired bool, err error) {
	now := time.Now().UnixMilli()
	expiresAt := time.Now().Add(leaseDuration).UnixMilli()

	row := r.pool.QueryRow(ctx, `INSERT INTO partition_leader_locks (partition, leader_id, fencing_token, expires_at)
		VALUES ($1, $2, 1, $3)
		ON CONFLICT (partition) DO UPDATE
		  SET leader_id     = EXCLUDED.leader_id,
		      fencing_token = partition_leader_locks.fencing_token + 1,
		      expires_at    = EXCLUDED.expires_at
		  WHERE partition_leader_locks.expires_at < $4
		RETURNING fencing_token`, partition, instanceID, expiresAt, now)

	err = row.Scan(&fencingToken)
	if err != nil {
		// 目前有人持有租约 竞选失败
		if errors.Is(err, pgx.ErrNoRows) { // pgx 找不到任何符合条件的行
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("取得leader失败 : %w", err)

	}

	return fencingToken, true, nil
}

// 延长租约
func (r *Repository) ExtendLease(ctx context.Context, partition, instanceID string, fencingToken int64) error {
	expiresAt := time.Now().Add(leaseDuration)

	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE partition_leader_locks
		SET expires_at = $1
		WHERE partition = $2 AND leader_id = $3 AND fencing_token = $4`,
		expiresAt, partition, instanceID, fencingToken)

	if err != nil {
		return fmt.Errorf("延长租约失败")
	}
	if cmdTag.RowsAffected() == 0 {
		return fmt.Errorf("租约已失效")
	}
	return nil
}

func (r *Repository) ReleaseLock(ctx context.Context, partition, instanceID string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE partition_leader_locks
		SET expires_at = 0
		WHERE partition = $1 AND leader_id = $2
		`, partition, instanceID)
	return err
}
