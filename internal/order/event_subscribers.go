package order

import (
	"atlas-trading-infrastructure/internal/domain"
	"atlas-trading-infrastructure/internal/infrastructure/logger"
	"atlas-trading-infrastructure/internal/matching/engine"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

var ErrIdempotencySkip = errors.New("idempotency skip")
var ErrStaleSettlementEvent = errors.New("stale settlement event")

type AccountUpdate struct {
	UserID   uuid.UUID
	Currency string
	Amount   decimal.Decimal // 余额变动
	Unlock   decimal.Decimal // 解锁金额
}

type EventSubscriber struct {
	orderRepo   OrderRepository
	accountRepo AccountRepository
	tradeRepo   TradeRepository
	txManager   DBTransaction
	eventBus    domain.EventPublisher
}

func NewEventSubscriber(
	orderRepo OrderRepository,
	accountRepo AccountRepository,
	tradeRepo TradeRepository,
	txManager DBTransaction,
	eventBus domain.EventPublisher,
) *EventSubscriber {
	return &EventSubscriber{
		orderRepo:   orderRepo,
		accountRepo: accountRepo,
		tradeRepo:   tradeRepo,
		txManager:   txManager,
		eventBus:    eventBus,
	}
}

func (s *EventSubscriber) HandleEvents(ctx context.Context, key, value []byte) (err error) {
	var envelope struct {
		EventType domain.EventType `json:"event_type"`
	}

	if err := json.Unmarshal(value, &envelope); err != nil {
		return fmt.Errorf("解析事件失败:%w", err)
	}

	switch envelope.EventType {
	//撮合成功后在db内结算
	case domain.EventSettlementRequested:
		var event domain.SettlementRequestedEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return fmt.Errorf("解析SettlementRequestedEvent失败: %w", err)
		}
		return s.handleSettlementRequestedEvent(ctx, &event)
	case domain.EventOrderCanceled:
		var event domain.OrderCanceledEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return fmt.Errorf("解析orderCanceledEvent失败: %w", err)
		}
		return s.handleOrderCanceled(ctx, &event)
	default:
		logger.Warn("收到未知EventType", zap.String("event_type", string(envelope.EventType)))
		return nil
	}
}

func (s *EventSubscriber) handleSettlementRequestedEvent(ctx context.Context, event *domain.SettlementRequestedEvent) error {
	// 幂等性保护
	if len(event.Trades) > 0 {
		exists, err := s.tradeRepo.TradeExistsByID(ctx, event.Trades[0].ID)
		if err != nil {
			return fmt.Errorf("幂等检查失败: %w", err)
		}
		if exists {
			logger.Info("结算事件已处理",
				zap.String("trade_id", event.Trades[0].ID.String()),
			)
			return nil
		}
	} else {
		takerOrder, err := s.orderRepo.GetOrder(ctx, event.TakerOrderID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				logger.Warn("对应订单不存在 跳过",
					zap.String("taker_order_id", event.TakerOrderID.String()),
				)
				return nil
			}
			return fmt.Errorf("查询 taker 订单失败: %w", err)
		}
		if takerOrder.Status != domain.StatusNew {
			logger.Info("无成交结算事件已处理 跳过",
				zap.String("order_id", event.TakerOrderID.String()),
				zap.String("status", domain.StatusToString(takerOrder.Status)),
			)
			return nil
		}
	}

	err := s.executeSettlementTx(ctx, event)
	return err
}

func (s *EventSubscriber) executeSettlementTx(ctx context.Context, event *domain.SettlementRequestedEvent) error {
	var updateOrders []*domain.Order

	err := s.txManager.ExecTx(ctx, func(ctx context.Context) error {
		// 二次检验fencingToken是否合法
		valid, err := s.txManager.ValidateFencingTokenTx(ctx, "matching-engine:global", event.FencingToken)
		if err != nil {
			return fmt.Errorf("TX 内部检验fencingToken失败: %w", err)
		}
		if !valid {
			logger.Warn("fencingToken不符合",
				zap.Int64("event_fencing_token", event.FencingToken),
			)
			return ErrStaleSettlementEvent
		}

		makerOrderIDsMap := make(map[uuid.UUID]bool)
		for _, order := range event.Trades {
			makerOrderIDsMap[order.MakerOrderID] = true
		}
		//只有一个吃单
		makerOrderIDsMap[event.TakerOrderID] = true

		var allOrderIDs []uuid.UUID
		for id := range makerOrderIDsMap {
			allOrderIDs = append(allOrderIDs, id)
		}

		//排序防止死锁
		sort.Slice(allOrderIDs, func(i, j int) bool {
			return allOrderIDs[i].String() < allOrderIDs[j].String()
		})

		lockedOrders := make(map[uuid.UUID]*domain.Order)

		// 锁定对应订单
		for _, id := range allOrderIDs {
			lockedOrder, err := s.orderRepo.GetOrderForUpdate(ctx, id)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					logger.Warn("收到过期的结算事件 跳过避免无线重试",
						zap.String("taker_order_id", event.TakerOrderID.String()),
						zap.String("missing_order_id", id.String()),
						zap.Int("trade_count", len(event.Trades)),
					)
					return ErrStaleSettlementEvent
				}
				return fmt.Errorf("锁定订单失败,订单: %s : %w", id, err)
			}
			lockedOrders[id] = lockedOrder
		}

		takerOrder := lockedOrders[event.TakerOrderID]

		// tx内再查一次幂等
		if takerOrder.Status != domain.StatusNew {
			return ErrIdempotencySkip
		}

		if len(event.Trades) > 0 {
			exists, err := s.tradeRepo.TradeExistsByID(ctx, event.Trades[0].ID)
			if err != nil {
				return fmt.Errorf("tx内部幂等性检查失败: %w", err)
			}
			if exists {
				return ErrIdempotencySkip
			}
		}

		allAccountUpdates := make([]AccountUpdate, 0)

		for _, trade := range event.Trades {
			makerOrder := lockedOrders[trade.MakerOrderID]

			// 更新挂单成交量
			makerOrder.FilledQuantity = makerOrder.FilledQuantity.Add(trade.Quantity)
			if makerOrder.FilledQuantity.Equal(makerOrder.Quantity) {
				makerOrder.Status = domain.StatusFilled
			} else if makerOrder.FilledQuantity.GreaterThan(decimal.Zero) {
				makerOrder.Status = domain.StatusPartiallyFilled
			}
			makerOrder.UpdatedAt = time.Now().UnixMilli()

			// 计算结算资金
			updates, err := s.calculateTradesSettlement(trade, takerOrder, makerOrder)
			if err != nil {
				return fmt.Errorf("计算结算资金失败: %w", err)
			}
			allAccountUpdates = append(allAccountUpdates, updates...)

			takerOrder.FilledQuantity = takerOrder.FilledQuantity.Add(trade.Quantity)
		}

		var refundAmount decimal.Decimal // 自成交订单要退的款
		if takerOrder.FilledQuantity.Equal(takerOrder.Quantity) {
			takerOrder.Status = domain.StatusFilled
		} else if event.RemainingQty.IsZero() { // 自成交保护
			takerOrder.Status = domain.StatusCanceled
			canceledQty := takerOrder.Quantity.Sub(takerOrder.FilledQuantity)
			if takerOrder.Side == domain.SideBuy {
				refundAmount = canceledQty.Mul(takerOrder.Price)
			} else {
				refundAmount = canceledQty
			}
		} else if takerOrder.FilledQuantity.GreaterThan(decimal.Zero) {
			takerOrder.Status = domain.StatusPartiallyFilled
		}

		if refundAmount.GreaterThan(decimal.Zero) {
			allAccountUpdates = append(allAccountUpdates, AccountUpdate{
				UserID:   takerOrder.UserID,
				Currency: event.LockedCurrency,
				Unlock:   refundAmount.Round(8),
			})
		}

		for _, id := range allOrderIDs {
			updateOrder := lockedOrders[id]
			updateOrder.UpdatedAt = time.Now().UnixMilli()
			if err := s.orderRepo.UpdateOrder(ctx, updateOrder); err != nil {
				return fmt.Errorf("更新订单失败: %w", err)
			}
			updateOrders = append(updateOrders, cloneOrder(updateOrder))
		}

		for _, trade := range event.Trades {
			if err := s.tradeRepo.CreateTrade(ctx, trade); err != nil {
				return fmt.Errorf("建立成交记录失败: %w", err)
			}
		}

		// 结算用户数据
		aggregatedUpdates := aggregateAndSortAccountUpdate(allAccountUpdates)
		for _, update := range aggregatedUpdates {
			if update.Unlock.GreaterThan(decimal.Zero) {
				if err := s.accountRepo.UnlockFunds(ctx, update.UserID, update.Currency, update.Amount); err != nil {
					return fmt.Errorf("解锁资金失败: %w", err)
				}
			}
			if !update.Amount.IsZero() {
				if err := s.accountRepo.UpdateBalance(ctx, update.UserID, update.Currency, update.Amount); err != nil {
					return fmt.Errorf("更新用户余额失败: %w", err)
				}
			}
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, ErrIdempotencySkip) || errors.Is(err, ErrStaleSettlementEvent) {
			return nil
		}
		return err
	}

	// 将来实现前端推送服务订阅这个topic
	if s.eventBus != nil {
		for _, order := range updateOrders {
			event := &domain.OrderUpdatedEvent{
				EventType: domain.EventOrderUpdated,
				Symbol:    order.Symbol,
				Order:     order,
			}

			publishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if err := s.eventBus.Publish(publishCtx, domain.TopicOrderUpdates, order.Symbol, event); err != nil {
				logger.Error("发布orderUpdatedEvent失败", zap.Error(err))
			}
			cancel()
		}
	}

	return nil
}

func (s *EventSubscriber) calculateTradesSettlement(trade *engine.Trade, takerOrder *domain.Order, makerOrder *domain.Order) ([]AccountUpdate, error) {
	tradeValue := trade.Price.Mul(trade.Quantity)

	var Buyer, Seller *domain.Order
	if takerOrder.Side == domain.SideBuy {
		Buyer = takerOrder
		Seller = makerOrder
	} else {
		Buyer = makerOrder
		Seller = takerOrder
	}

	base, quote, err := splitSymbol(takerOrder.Symbol)
	if err != nil {
		return nil, err
	}

	// taker.Price ≥ maker.Price 如果taker是买单的话冻结的比实际花的多
	buyerUnlockAmount := tradeValue
	if takerOrder.ID == Buyer.ID && !takerOrder.Price.IsZero() {
		buyerUnlockAmount = takerOrder.Price.Mul(trade.Quantity)
	}

	buyerUnlockAmount = buyerUnlockAmount.Round(8)
	tradeValue = tradeValue.Round(8)
	tradeQty := trade.Quantity.Round(8)

	updates := []AccountUpdate{
		{UserID: Buyer.UserID, Currency: quote, Amount: tradeValue.Neg(), Unlock: buyerUnlockAmount},
		{UserID: Buyer.UserID, Currency: base, Amount: tradeQty, Unlock: decimal.Zero},
		{UserID: Seller.UserID, Currency: base, Amount: tradeQty.Neg(), Unlock: tradeQty},
		{UserID: Seller.UserID, Currency: quote, Amount: tradeValue, Unlock: decimal.Zero},
	}

	return updates, nil

}

func (s *EventSubscriber) handleOrderCanceled(ctx context.Context, event *domain.OrderCanceledEvent) error {

	var copyOrder *domain.Order
	var orderSymbol string // 用来发updated event

	err := s.txManager.ExecTx(ctx, func(ctx context.Context) error {
		// 二次检验fencingToken是否合法
		valid, err := s.txManager.ValidateFencingTokenTx(ctx, "matching-engine:global", event.FencingToken)
		if err != nil {
			return fmt.Errorf("TX 内部检验fencingToken失败: %w", err)
		}
		if !valid {
			logger.Warn("fencingToken不符合",
				zap.Int64("event_fencing_token", event.FencingToken),
			)
			// 避免consumer 无限循环
			return ErrStaleSettlementEvent
		}

		order, err := s.orderRepo.GetOrderForUpdate(ctx, event.OrderID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				logger.Warn("收到过期撤单结算事件 订单不存在 跳过",
					zap.String("order_id", event.OrderID.String()),
				)
				return ErrStaleSettlementEvent
			}
			return fmt.Errorf("锁定订单失败: %w", err)
		}

		if order.Status == domain.StatusFilled || order.Status == domain.StatusCanceled {
			logger.Info("订单已取消或完成", zap.String("order_id", order.ID.String()))
			return nil
		}

		remain := order.Quantity.Sub(order.FilledQuantity)

		currency, amount, err := calculateAmount(order, remain)
		if err != nil {
			return fmt.Errorf("计算解锁资金失败: %w", err)
		}

		if amount.GreaterThan(decimal.Zero) {
			if err := s.accountRepo.UnlockFunds(ctx, order.UserID, currency, amount); err != nil {
				return fmt.Errorf("解锁资金失败: %w", err)
			}
		}

		order.Status = domain.StatusCanceled

		if err := s.orderRepo.UpdateOrder(ctx, order); err != nil {
			return fmt.Errorf("更新订单状态失败: %w", err)
		}

		orderSymbol = order.Symbol
		snapshot := *order    // 拷贝当前订单状态
		copyOrder = &snapshot // 把副本的地址赋给指针变量

		return nil
	})

	if errors.Is(err, ErrStaleSettlementEvent) {
		return nil
	}
	// 取消成功发kafka
	if err == nil && copyOrder != nil && s.eventBus != nil {
		updateEvent := &domain.OrderUpdatedEvent{
			EventType: domain.EventOrderUpdated,
			Symbol:    orderSymbol,
			Order:     copyOrder,
		}

		publishCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		if publishErr := s.eventBus.Publish(publishCtx, domain.TopicOrderUpdates, orderSymbol, updateEvent); publishErr != nil {
			logger.Error("kafka发布OrderUpdatedEvent(取消)失败", zap.Error(publishErr))
		}
		cancel()
	}

	return err
}

// 整理成account -> currency -> amount并排序
func aggregateAndSortAccountUpdate(updates []AccountUpdate) []AccountUpdate {
	aggMap := make(map[string]AccountUpdate)

	for _, update := range updates {
		key := update.UserID.String() + "_" + update.Currency

		if up, ok := aggMap[key]; ok {
			up.Amount = up.Amount.Add(update.Amount)
			up.Unlock = up.Unlock.Add(update.Unlock)
		} else {
			copyUp := update
			aggMap[key] = copyUp
		}
	}

	var result []AccountUpdate

	for _, res := range aggMap {
		if !res.Amount.IsZero() || !res.Unlock.IsZero() {
			result = append(result, res)
		}
	}

	// 排序防死锁
	sort.Slice(result, func(i, j int) bool {
		// 先按userID排再按currency
		if result[i].UserID.String() != result[j].UserID.String() {
			return result[i].UserID.String() < result[j].UserID.String()
		}
		return result[i].Currency < result[j].Currency
	})

	return result
}

func calculateAmount(order *domain.Order, remain decimal.Decimal) (currency string, amount decimal.Decimal, err error) {
	base, quote, err := splitSymbol(order.Symbol)
	if err != nil {
		return "", decimal.Zero, err
	}
	if order.Side == domain.SideBuy {
		return quote, order.Price.Mul(remain), nil
	}
	return base, remain, nil
}

func cloneOrder(o *domain.Order) *domain.Order {
	if o == nil {
		return nil
	}
	copyOrder := *o
	return &copyOrder
}
