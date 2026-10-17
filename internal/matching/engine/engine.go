package engine

import (
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type Engine struct {
	orderbook *Orderbook
	mu        sync.Mutex
}

func NewEngine(Symbol string) *Engine {
	return &Engine{
		orderbook: NewOrderbook(Symbol),
	}
}

// 处理新订单，返回成交结果
func (e *Engine) Process(order *Order) []*Trade {
	e.mu.Lock() // 防止两个goroutine同时改orderbook
	defer e.mu.Unlock()

	var trades []*Trade

	if order.Side == SideBuy {
		trades = e.matchBuyOrder(order)
	}
	if order.Side == SideSell {
		trades = e.matchSellOrder(order)
	}

	if order.Quantity.IsPositive() {
		e.orderbook.AddOrder(order)
	}

	return trades
}

// 买单撮合逻辑
func (e *Engine) matchBuyOrder(buyOrder *Order) []*Trade {
	var trades []*Trade
	for {
		bestAsk := e.orderbook.BestAsk()
		if bestAsk == nil {
			break
		}

		// 避免自成交（同一用户买单撞到自己的卖单）
		if buyOrder.UserID == bestAsk.UserID {
			buyOrder.Quantity = decimal.Zero
			break
		}

		// 检查价格是否匹配
		if buyOrder.Price.LessThan(bestAsk.Price) {
			break
		}

		// 计算成交数量
		matchQty := buyOrder.Quantity
		if bestAsk.Quantity.LessThan(matchQty) {
			matchQty = bestAsk.Quantity
		}

		// 记录成交
		tradeID, _ := uuid.NewV7()
		trade := &Trade{
			ID:           tradeID,
			Symbol:       e.orderbook.Symbol,
			MakerOrderID: bestAsk.Id,
			TakerOrderID: buyOrder.Id,
			Price:        bestAsk.Price, // price用挂单的价格
			Quantity:     matchQty,
			CreatedAt:    time.Now().UnixMilli(),
		}
		trades = append(trades, trade)

		bestAsk.Quantity.Sub(matchQty)
		buyOrder.Quantity.Sub(matchQty)

		// 如果卖单完全成交，从orderbook移除
		if bestAsk.Quantity.IsZero() {
			e.orderbook.RemoveAskOrder()
		}

		// 如果买单完全成交，结束
		if buyOrder.Quantity.IsZero() {
			break
		}
	}

	return trades
}

// 卖单撮合逻辑
func (e *Engine) matchSellOrder(sellOrder *Order) []*Trade {
	var trades []*Trade
	for {
		bestBid := e.orderbook.BestBid()
		if bestBid == nil {
			break
		}

		// 避免自成交（同一用户卖单撞到自己的买单）
		if sellOrder.UserID == bestBid.UserID {
			sellOrder.Quantity = decimal.Zero
			break
		}

		// 检查价格是否匹配
		if sellOrder.Price.GreaterThan(bestBid.Price) {
			break
		}

		// 计算成交数量
		matchQty := sellOrder.Quantity
		if bestBid.Quantity.LessThan(matchQty) {
			matchQty = bestBid.Quantity
		}

		// 记录成交
		tradeID, _ := uuid.NewV7()
		trade := &Trade{
			ID:           tradeID,
			Symbol:       e.orderbook.Symbol,
			MakerOrderID: bestBid.Id,
			TakerOrderID: sellOrder.Id,
			Price:        bestBid.Price, // price用挂单的价格
			Quantity:     matchQty,
			CreatedAt:    time.Now().UnixMilli(),
		}
		trades = append(trades, trade)

		bestBid.Quantity.Sub(matchQty)
		sellOrder.Quantity.Sub(matchQty)

		// 如果挂单完全成交，从orderbook移除
		if bestBid.Quantity.IsZero() {
			e.orderbook.RemoveBidOrder()
		}

		// 如果吃单完全成交，结束
		if sellOrder.Quantity.IsZero() {
			break
		}
	}

	return trades
}

func (e *Engine) Cancel(orderID uuid.UUID, side OrderSide) bool {
	e.mu.Lock()
	defer e.mu.Unlock()

	return e.orderbook.RemoveOrder(orderID, side)
}

func (e *Engine) GetOrderBookSnapshort(depth int) *OrderBookSnapshot {
	snapShort := &OrderBookSnapshot{
		Symbol: e.orderbook.Symbol,
		Bids:   make([]OrderBookLevel, 0),
		Asks:   make([]OrderBookLevel, 0),
	}

	// 处理买单
	for i, order := range e.orderbook.bids {
		if depth > 0 && i >= depth {
			break
		}
		snapShort.Bids = append(snapShort.Bids, OrderBookLevel{
			Price:    order.Price,
			Quantity: order.Quantity,
		})
	}

	//处理卖单
	for i, order := range e.orderbook.asks {
		if depth > 0 && i >= depth {
			break
		}
		snapShort.Asks = append(snapShort.Asks, OrderBookLevel{
			Price:    order.Price,
			Quantity: order.Quantity,
		})
	}

	return snapShort
}

func (e *Engine) RestoreOrder(order *Order) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.orderbook.AddOrder(order)
}
