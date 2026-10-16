package outbox

import (
	"atlas-trading-infrastructure/internal/infrastructure/db"
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

type dbExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) WithTx(ctx context.Context, fn func(context.Context) error) error {
	return pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		txCtx := context.WithValue(ctx, db.TxKey, tx)
		return fn(txCtx)
	})
}

func (r *Repository) getExecutor(ctx context.Context) dbExecutor {
	if tx := db.GetTx(ctx); tx != nil {
		return tx
	}
	return r.pool
}

// 给热路径 gracePeriod 的时间将已发送的信息删掉
func (r *Repository) FetchingPending(ctx context.Context, batchSize int, gracePeriod time.Duration) ([]*Message, error) {

	threshold := time.Now().Add(-gracePeriod).UnixMilli()
	rows, err := r.getExecutor(ctx).Query(ctx, `
		SELECT id, aggregate_id, aggregate_type, topic, partition_key, payload, status, retry_count, created_at, published_at
		FROM outbox_messages
		WHERE status = $1 AND created_at <= $2
		ORDER BY created_at ASC
		LIMIT $3
		FOR UPDATE SKIP LOCKED`,
		StatusPending, threshold, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		m := &Message{}
		err := rows.Scan(
			&m.ID, &m.AggregateID, &m.AggregateType,
			&m.Topic, &m.PartitionKey, &m.Payload,
			&m.Status, &m.RetryCount, &m.CreatedAt, &m.PublishedAt,
		)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

// 更新重复次数
func (r *Repository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	_, err := r.getExecutor(ctx).Exec(ctx, `
		UPDATE outbox_messages
		SET retry_count = retry_count + 1
		WHERE id = $1`,
		id,
	)
	return err
}

func (r *Repository) MarkPublishedWorker(ctx context.Context, batch []uuid.UUID) error {
	if len(batch) == 0 {
		return nil
	}
	_, err := r.getExecutor(ctx).Exec(ctx, `
	DELETE FROM outbox_messages
	WHERE id = ANY($1)`,
		batch)

	return err
}

func (r *Repository) InsertMsg(ctx context.Context, msg Message) error {
	msg.ID, _ = uuid.NewV7()
	msg.CreatedAt = time.Now().UnixMilli()
	msg.Status = StatusPending

	_, err := r.getExecutor(ctx).Exec(ctx, `
		INSERT INTO outbox_messages
			(id, aggregate_id, aggregate_type, topic, partition_key, payload, status, retry_count, created_at, published_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		msg.ID,
		msg.AggregateID,
		msg.AggregateType,
		msg.Topic,
		msg.PartitionKey,
		msg.Payload,
		msg.Status,
		msg.RetryCount,
		msg.CreatedAt,
		msg.PublishedAt,
	)
	return err
}

func (r *Repository) BatchInsert(ctx context.Context, msgs []*Message) error {
	if len(msgs) == 0 {
		return nil
	}

	tx := db.GetTx(ctx)
	if tx == nil {
		return fmt.Errorf("BatchInsert 必须在事务内执行")
	}

	rows := make([][]any, 0, len(msgs))
	for _, msg := range msgs {
		msg.ID, _ = uuid.NewV7()
		msg.CreatedAt = time.Now().UnixMilli()
		msg.Status = StatusPending

		rows = append(rows, []any{
			msg.ID, msg.AggregateID, msg.AggregateType, msg.Topic, msg.PartitionKey, msg.Payload, msg.Status, msg.RetryCount, msg.CreatedAt, msg.PublishedAt,
		})
	}

	_, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"outbox_messages"},
		[]string{"id", "aggregate_id", "aggregate_type", "topic", "partition_key", "payload", "status", "retry_count", "created_at", "published_at"},
		pgx.CopyFromRows(rows),
	)

	return err

}
