package main

import (
	"atlas-trading-infrastructure/internal/domain"
	"atlas-trading-infrastructure/internal/infrastructure/db"
	"atlas-trading-infrastructure/internal/infrastructure/election"
	"atlas-trading-infrastructure/internal/infrastructure/kafka"
	"atlas-trading-infrastructure/internal/infrastructure/logger"
	"atlas-trading-infrastructure/internal/infrastructure/redis"
	"atlas-trading-infrastructure/internal/matching"
	"atlas-trading-infrastructure/internal/matching/engine"
	"atlas-trading-infrastructure/internal/repository"
	"context"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

func main() {
	defer logger.Sync()

	// 数据库
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		logger.Log.Fatal("engine DATABASE_URL 没设置环境变量")
	}
	dbCfg := db.DefaultDBConfig(dbURL)
	dbCfg.MaxOpenConns = 20 // 撮合引擎数据库操作较少

	pool, err := db.NewMySQLPool(context.Background(), dbCfg)
	if err != nil {
		logger.Log.Fatal("engine 数据库连接失败", zap.Error(err))
	}
	defer pool.Close()

	repo := repository.NewMySQLRepository(pool)

	// redis
	redisCfg := redis.DefaultConfig()
	if redisAddr := os.Getenv("REDIS_URL"); redisAddr != "" {
		redisCfg.Addr = redisAddr
	}
	redisClient, err := redis.NewClient(redisCfg)
	if err != nil {
		logger.Log.Warn("engine redis创建失败, 订单薄缓存不可用", zap.Error(err))
	}

	var cacheRepo domain.CacheRepository

	if redisClient != nil {
		cacheRepo = repository.NewRedisCacheRepository(redisClient)
		logger.Log.Info("redis已连接")
	}

	// kafka producer
	kafkaCfg := kafka.DefaultConfig()
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		kafkaCfg.Brokers = strings.Split(brokers, ",") // broker地址列表
	}
	if resetOffset := os.Getenv("KAFKA_RESET_OFFSET"); resetOffset != "" {
		kafkaCfg.ResetOffset = strings.ToLower(resetOffset) // 没有初始offset时从哪开始读
	}
	if os.Getenv("KAFKA_ALLOW_AUTO_CREATE") == "false" {
		kafkaCfg.AllowAutoTopicCreation = false
	} else if os.Getenv("KAFKA_ALLOW_AUTO_CREATE") == "true" {
		kafkaCfg.AllowAutoTopicCreation = true
	}

	producer, err := kafka.NewProducer(kafkaCfg)
	if err != nil {
		logger.Log.Fatal("kafka连接失败", zap.Error(err))
	}
	defer producer.Close()
	logger.Log.Info("kafka producer已连接")

	// 撮合引擎
	engineManager := engine.NewEngineManager()
	svc := matching.NewSubscriber(engineManager, producer, cacheRepo)

	// leader election设置 （election设置完后才启动consumer）
	// instance id使用通host name
	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		logger.Log.Fatal("获取实例ID失败", zap.Error(err))
	}
	electionRepo := election.NewRepository(pool)
	elector := election.NewElector(electionRepo, "matching-engine: global", instanceID)

	// consumer 生命周期管理
	var (
		consumerMu     sync.Mutex
		matchConsumer  *kafka.Consumer
		consumerCancel context.CancelFunc
	)

	// 执行冷启动 并执行kafka consumer
	onBecomeLeader := func() {
		consumerMu.Lock()
		defer consumerMu.Unlock()

		logger.Log.Info("已成为leader 开始冷启动", zap.String("instanceID", instanceID))

		// 清空数据，防止上一任leader 数据污染
		engineManager.Reset()

		// 设置fencing token
		svc.SetFencingToken(elector.GetFencingToken())

		// 还原撮合引擎快照
		logger.Log.Info("正在从db还原撮合引擎快照")
		if err := matching.RestoreEngineSnapshot(context.Background(), repo, engineManager); err != nil {
			logger.Log.Error("还原撮合引擎快照失败", zap.Error(err))
		}
		logger.Log.Info("撮合引擎快照还原完成")

		topicCtx, topicCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer topicCancel()
		if err := producer.CreateTopics(topicCtx, []string{
			domain.TopicOrders,
			domain.TopicSettlements,
			domain.TopicTrades,
			domain.TopicOrderBook,
			domain.TopicOrderUpdates,
		}); err != nil {
			logger.Log.Warn("CreateTopics 失败", zap.Error(err))
		} else {
			logger.Log.Info("Kafka topics 已初始化")
		}

		// 同步订单薄至redis
		restoredSymbols := svc.SyncRecoveredOrderBooks(20)
		if len(restoredSymbols) == 0 {
			logger.Log.Info("冷启动后没有需要同步的快照")
		} else {
			logger.Log.Info("快照已同步至redis",
				zap.Strings("symbols", restoredSymbols),
			)
		}

		// 建立consumer 单独的context 让onLoseLeader可以单独取消
		consumerCtx, cancel := context.WithCancel(context.Background())
		consumerCancel = cancel

		consumer, consumerErr := kafka.NewConsumer(kafkaCfg, "matching-engine", []string{domain.TopicOrders})
		if consumerErr != nil {
			logger.Log.Error("matching-engine: 建立 matching consumer 失败", zap.Error(consumerErr))
			cancel()
			return
		}

		matchConsumer = consumer
		matchConsumer.Start(consumerCtx, svc.HandleEvents)
		logger.Log.Info("Kafka matching consumer 已启动", zap.String("topic", domain.TopicOrders))
	}

	// 停止kafka consumer
	onLoseLeader := func() {
		consumerMu.Lock()
		defer consumerMu.Unlock()

		logger.Log.Warn(" 已失去 Leader 身份，正在停止 Kafka Consumer",
			zap.String("instanceID", instanceID),
		)
		// 先将fencingtoken清0 让任何还在执行的handleEvent 停止工作
		svc.SetFencingToken(0)
		if consumerCancel != nil {
			consumerCancel()
			consumerCancel = nil
		}
		if matchConsumer != nil {
			matchConsumer.Wait() // 外部阻塞等待consumer完全退出
			matchConsumer = nil
		}
		logger.Log.Info("Kafka Consumer 已停止")
	}

	globalCtx, globalCancel := context.WithCancel(context.Background())
	defer globalCancel()

	go elector.Run(globalCtx, onBecomeLeader, onLoseLeader)
	logger.Log.Info("elector leader 已启动")

	// health check
	r := gin.New()
	r.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"status":     "ok",
			"service":    "matching-engine",
			"is_leader":  elector.IsLeader(),
			"instanceID": instanceID,
		})
	}) // 靠这个url判断进程是否还活着

	port := os.Getenv("MATCHING_ENGINE_PORT")
	if port == "" {
		port = "8101"
	}

	srv := &http.Server{Addr: ":" + port, Handler: r}
	go func() {
		logger.Log.Info("Health check server 已启动", zap.String("port", ":"+port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Log.Error("Health check server 错误", zap.Error(err))
		}
	}()

	// 优雅关闭
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	logger.Log.Info("收到关闭信号", zap.String("signal", sig.String()))

	// 关闭context 同时触发 elector释放锁
	globalCancel()

	// 等待consumer把信息处理完
	consumerMu.Lock()
	if matchConsumer != nil {
		matchConsumer.Wait()
	}
	consumerMu.Unlock()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Log.Error("health check关闭失败", zap.Error(err))
	}
	logger.Log.Info("优雅关闭成功")

}
