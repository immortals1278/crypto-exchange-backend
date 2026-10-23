package outbox

import (
	"atlas-trading-infrastructure/internal/infrastructure/db"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Repository struct {
	pool *sql.DB
}

type dbExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func NewRepository(pool *sql.DB) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) WithTx(ctx context.Context, fn func(context.Context) error) error {
	tx, err := r.pool.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开启事务失败: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
	}()

	txCtx := context.WithValue(ctx, db.TxKey, tx)
	if err := fn(txCtx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
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
	rows, err := r.getExecutor(ctx).QueryContext(ctx, `
		SELECT id, aggregate_id, aggregate_type, topic, partition_key, payload, status, retry_count, created_at, published_at
		FROM outbox_messages
		WHERE status = ? AND created_at <= ?
		ORDER BY created_at ASC
		LIMIT ?
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

// 更新重试次数
func (r *Repository) IncrementRetry(ctx context.Context, id uuid.UUID) error {
	_, err := r.getExecutor(ctx).ExecContext(ctx, `
		UPDATE outbox_messages
		SET retry_count = retry_count + 1
		WHERE id = ?`,
		id,
	)
	return err
}

func (r *Repository) MarkPublishedWorker(ctx context.Context, batch []uuid.UUID) error {
	if len(batch) == 0 {
		return nil
	}

	// MySQL 没有 = ANY(?)，改为动态生成 IN (?, ?, ...)
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(batch)), ",")
	args := make([]any, len(batch))
	for i, id := range batch {
		args[i] = id
	}

	_, err := r.getExecutor(ctx).ExecContext(ctx,
		"DELETE FROM outbox_messages WHERE id IN ("+placeholders+")", args...)

	return err
}

func (r *Repository) InsertMsg(ctx context.Context, msg Message) error {
	msg.ID, _ = uuid.NewV7()
	msg.CreatedAt = time.Now().UnixMilli()
	msg.Status = StatusPending

	_, err := r.getExecutor(ctx).ExecContext(ctx, `
		INSERT INTO outbox_messages
			(id, aggregate_id, aggregate_type, topic, partition_key, payload, status, retry_count, created_at, published_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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

	// MySQL 没有 COPY 协议，改为多值 INSERT
	const columns = "(id, aggregate_id, aggregate_type, topic, partition_key, payload, status, retry_count, created_at, published_at)"
	const rowPlaceholders = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

	var sb strings.Builder
	sb.WriteString("INSERT INTO outbox_messages ")
	sb.WriteString(columns)
	sb.WriteString(" VALUES ")

	args := make([]any, 0, len(msgs)*10)
	for i, msg := range msgs {
		msg.ID, _ = uuid.NewV7()
		msg.CreatedAt = time.Now().UnixMilli()
		msg.Status = StatusPending

		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(rowPlaceholders)
		args = append(args,
			msg.ID, msg.AggregateID, msg.AggregateType, msg.Topic, msg.PartitionKey,
			msg.Payload, msg.Status, msg.RetryCount, msg.CreatedAt, msg.PublishedAt)
	}

	_, err := tx.ExecContext(ctx, sb.String(), args...)
	return err
}
