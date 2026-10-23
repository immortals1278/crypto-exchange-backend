package main

// 初始化db
// 初始化handlers+执行handlers注册路由
import (
	"atlas-trading-infrastructure/internal/api"
	"atlas-trading-infrastructure/internal/domain"
	"atlas-trading-infrastructure/internal/infrastructure/db"
	"atlas-trading-infrastructure/internal/infrastructure/kafka"
	"atlas-trading-infrastructure/internal/infrastructure/logger"
	"atlas-trading-infrastructure/internal/infrastructure/outbox"
	"atlas-trading-infrastructure/internal/order"
	"atlas-trading-infrastructure/internal/repository"
	"context"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func main() {
	defer logger.Sync()

	// 设置数据库链接
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		logger.Log.Fatal("DATABASE_URL未设置环境变量")
	}
	dbCfg := db.DefaultDBConfig(dbURL)
	dbCfg.MaxOpenConns = 150            // order-service 分配前台流量，分配更多连接
	dbCfg.MaxIdleTime = 5 * time.Minute // 连接在池中空闲超过五分钟被关闭
	pool, err := db.NewMySQLPool(context.Background(), dbCfg)
	if err != nil {
		logger.Log.Fatal("连接数据库失败", zap.Error(err))
	}
	defer pool.Close()

	repo := repository.NewMySQLRepository(pool)

	// kafka Producer（微服务模式kafka连接失败直接fatal）
	kafkaCfg := kafka.DefaultConfig()
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		kafkaCfg.Brokers = strings.Split(brokers, ",")
	}
	if resetOffset := os.Getenv("KAFKA_RESET_OFFSET"); resetOffset != "" {
		kafkaCfg.ResetOffset = strings.ToLower(resetOffset)
	}
	if os.Getenv("GIN_MODE") == "release" {
		kafkaCfg.AllowAutoTopicCreation = false
	}

	kafkaProducer, err := kafka.NewProducer(kafkaCfg)
	if err != nil {
		logger.Log.Error("kafka启动失败 微服务模式无法启动", zap.Error(err))
	}
	eventBus := domain.EventPublisher(kafkaProducer)
	logger.Log.Info("kafka producer 已连接")

	// outbox
	outboxCtx, cancelOutbox := context.WithCancel(context.Background())
	outboxRepo := outbox.NewRepository(pool)
	worker := outbox.NewWorker(outboxRepo, kafkaProducer, 10*time.Second, 100)
	go worker.Start(outboxCtx)

	svc := order.NewService(
		repo, repo, repo, repo, repo,
		eventBus,
		kafkaProducer,
		outboxRepo,
	)

	// 启动kafka consumer
	eventSubscriber := order.NewEventSubscriber(repo, repo, repo, repo, kafkaProducer)
	consumerCtx, cancelConsumers := context.WithCancel(context.Background())
	defer cancelConsumers()

	settleConsumer, err := kafka.NewConsumer(kafkaCfg, "order-service", []string{domain.TopicSettlements})
	if err != nil {
		logger.Log.Fatal("orderService 建立consumer失败", zap.Error(err))
	}
	settleConsumer.Start(consumerCtx, eventSubscriber.HandleEvents)
	logger.Log.Info("Kafka settlement consumer 已启动", zap.String("topic", domain.TopicSettlements))

	r := gin.New()
	r.Use(gin.Recovery())

	handler := api.NewHandler(svc)
	v1 := r.Group("/api/v1")
	handler.RegisterRoutes(v1)

	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok", "service": "order-service"})
	})

	// 启动http服务器
	port := os.Getenv("ORDER_SERVICE_PORT")
	if port == "" {
		port = "8103"
	}

	srv := &http.Server{Addr: ":" + port, Handler: r}

	go func() {
		logger.Log.Info("orderService 启动")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Fatal("order-service 启动失败", zap.Error(err))
		}
	}()

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Log.Info("orderService收到关闭信号")

	cancelConsumers()
	cancelOutbox()
	// 关闭kafka consumer
	shutDown := make(chan struct{})
	go func() {
		if settleConsumer != nil {
			settleConsumer.Wait()
		}
		close(shutDown)
	}()
	select {
	case <-shutDown:
		logger.Log.Info("kafka consumer关闭成功")
	case <-time.After(10 * time.Second):
		logger.Log.Warn("kafka consumer等待超时，强制关闭")
	}

	if kafkaProducer != nil {
		kafkaProducer.Close()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("order-service 强制关闭", zap.Error(err))
	}
	logger.Info("order-service 优雅关闭完成")

}
