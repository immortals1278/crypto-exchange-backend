package domain

import (
	"atlas-trading-infrastructure/internal/matching/engine"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type EventType string

// 定义kafka常数
const (
	TopicOrders       = "exchange.orders"
	TopicSettlements  = "exchange.settlements"
	TopicTrades       = "exchange.trades"
	TopicOrderBook    = "exchange.orderbook"
	TopicOrderUpdates = "exchange.order_updates"
)

const (
	EventOrderPlaced          EventType = "order.placed"
	EventOrderCancelRequested EventType = "order.cancel_requested"
	EventOrderCanceled        EventType = "order.canceled"
	EventSettlementRequested  EventType = "settlement.requested"
	EventTradeExecuted        EventType = "trade.executed"
	EventOrderBookUpdated     EventType = "orderbook.updated"
	EventOrderUpdated         EventType = "order.updated"
)

type OrderPlacedEvent struct {
	EventType      EventType       `json:"event_type"`
	Symbol         string          `json:"symbol"`
	OrderID        uuid.UUID       `json:"order_id"`
	UserID         uuid.UUID       `json:"user_id"`
	Side           OrderSide       `json:"side"`
	Price          decimal.Decimal `json:"price"`
	Quantity       decimal.Decimal `json:"quantity"`
	AmountLocked   decimal.Decimal `json:"amount_locked"`
	LockedCurrency string          `json:"locked_currency"`
}

type OrderCanceledEvent struct {
	EventType    EventType `json:"event_type"`
	Symbol       string    `json:"symbol"`
	OrderID      uuid.UUID `json:"order_id"`
	UserID       uuid.UUID `json:"user_id"`
	FencingToken int64     `json:"fencing_token"` // 不知道有什么用
}

type OrderCancelRequestedEvent struct {
	EventType EventType `json:"event_type"`
	Symbol    string    `json:"symbol"`
	OrderID   uuid.UUID `json:"order_id"`
	Side      OrderSide `json:"side"`
	UserID    uuid.UUID `json:"user_id"`
}

type OrderUpdatedEvent struct {
	EventType EventType `json:"event_type"`
	Symbol    string    `json:"symbol"`
	Order     *Order    `json:"order"`
}

type SettlementRequestedEvent struct {
	EventType      EventType       `json:"event_type"`
	Symbol         string          `json:"symbol"`
	TakerOrderID   uuid.UUID       `json:"taker_order_id"`
	AmountLocked   decimal.Decimal `json:"amount_locked"`
	LockedCurrency string          `json:"locked_currency"`
	RemainingQty   decimal.Decimal `json:"remaining_qty"`
	Trades         []*engine.Trade `json:"trades"`
	FencingToken   int64           `json:"fencing_token"` // 判断是否来自合法的 Leader
}

type OnOrderBookUpdatedEvent struct {
	EventType EventType                 `json:"event_type"`
	Symbol    string                    `json:"symbol"`
	Snapshot  *engine.OrderBookSnapshot `json:"snapshot"`
}
