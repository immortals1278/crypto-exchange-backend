package outbox

import (
	"atlas-trading-infrastructure/internal/infrastructure/logger"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"go.uber.org/zap"
)

type WorkerRepository interface {
	WithTx(ctx context.Context, fn func(context.Context) error) error //把回调函数包进一个数据库事务里执行。
	FetchingPending(ctx context.Context, batchSize int, gracePeriod time.Duration) ([]*Message, error)
	MarkPublishedWorker(ctx context.Context, ids []uuid.UUID) error
	IncrementRetry(ctx context.Context, id uuid.UUID) error
}

var _ WorkerRepository = (*Repository)(nil)

// 解耦worker与kafka
type Publisher interface {
	PublishRaw(ctx context.Context, topic, partitionKey string, value []byte) error
}

// Worker：定期扫描outbox_message表，将pending的消息发送到kafka，并标记为published
type Worker struct {
	repo      WorkerRepository
	publisher Publisher
	interval  time.Duration //定时扫描间隔
	batchSize int           //每批处理量
}

func NewWorker(repo *Repository, publisher Publisher, interval time.Duration, batchSize int) *Worker {
	return &Worker{
		repo:      repo,
		publisher: publisher,
		interval:  interval,
		batchSize: batchSize,
	}
}

// 在orderService main.go 中开一个goroutine启动
func (w *Worker) Start(ctx context.Context) {
	logger.Log.Info("Outbox worker 已启动")
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("outbox worker收到关闭信号,正在停止")
			return
		case <-ticker.C: // 计时器每响一次
			w.process(ctx)
		}
	}
}

// 进行一次批量扫描并发送

func (w *Worker) process(ctx context.Context) {

	err := w.repo.WithTx(ctx, func(txCtx context.Context) error {
		msgs, err := w.repo.FetchingPending(txCtx, w.batchSize, 5*time.Second)
		if err != nil {
			return fmt.Errorf("获取pending信息失败: %w", err)
		}
		SuccessID := make([]uuid.UUID, 0, len(msgs))

		for _, msg := range msgs {
			publishErr := w.publisher.PublishRaw(ctx, msg.Topic, msg.PartitionKey, msg.Payload)
			if publishErr != nil {
				logger.Log.Warn("Outbox Worker 发送信息到 kafka 失败",
					zap.String("id", msg.ID.String()),
					zap.String("topic", msg.Topic),
					zap.Error(publishErr),
				)
				if err := w.repo.IncrementRetry(txCtx, msg.ID); err != nil {
					return fmt.Errorf("增加 retry_count 失败: %w", err)
				}
				continue
			}
			SuccessID = append(SuccessID, msg.ID)
		}

		if len(SuccessID) == 0 {
			return nil
		}

		if err := w.repo.MarkPublishedWorker(txCtx, SuccessID); err != nil {
			return fmt.Errorf("已发送的outboxMsg清除失败:%w", err)
		}
		return nil
	})

	if err != nil {
		logger.Log.Error("outbox worker 批量处理失败", zap.Error(err))
	}
}

// 将任意结构体转化成json []byte
func MarshalPayload(v any) ([]byte, error) {
	return json.Marshal(v)
}
