package engine

import (
	"sort"

	"github.com/google/uuid"
)

type Orderbook struct {
	Symbol string
	bids   []*Order
	asks   []*Order
}

func NewOrderbook(Symbol string) *Orderbook {
	return &Orderbook{
		Symbol: Symbol,
		bids:   make([]*Order, 0),
		asks:   make([]*Order, 0),
	}
}

func (ob *Orderbook) AddOrder(order *Order) {
	if order.Side == SideBuy {
		ob.bids = append(ob.bids, order)
		// 降序
		sort.SliceStable(ob.bids, func(i, j int) bool {
			return ob.bids[i].Price.GreaterThan(ob.bids[j].Price)
		})
	} else {
		ob.asks = append(ob.asks, order)
		// 升序
		sort.SliceStable(ob.asks, func(i, j int) bool {
			return ob.asks[i].Price.LessThan(ob.asks[j].Price)
		})

	}
}

func (ob *Orderbook) BestAsk() (order *Order) {
	if len(ob.asks) == 0 {
		return nil
	}
	return ob.asks[0]
}

func (ob *Orderbook) RemoveAskOrder() {
	if len(ob.asks) > 0 {
		ob.asks[0] = nil // 避免内存泄露
		ob.asks = ob.asks[1:]
	}
}

func (ob *Orderbook) RemoveBidOrder() {
	if len(ob.bids) > 0 {
		ob.bids[0] = nil // 避免内存泄露
		ob.bids = ob.bids[1:]
	}
}

func (ob *Orderbook) BestBid() (order *Order) {
	if len(ob.bids) == 0 {
		return nil
	}
	return ob.bids[0]
}

func (ob *Orderbook) RemoveOrder(orderID uuid.UUID, side OrderSide) bool {
	if side == SideBuy {
		for i, order := range ob.bids {
			if order.Id == orderID {
				copy(ob.bids[i:], ob.bids[i+1:]) // 防止内存泄露
				ob.bids[len(ob.bids)-1] = nil    // 置空尾部残留
				ob.bids = ob.bids[:len(ob.bids)-1]
				return true
			}
		}
	} else {
		for i, order := range ob.asks {
			if order.Id == orderID {
				copy(ob.asks[i:], ob.asks[i+1:])
				ob.asks[len(ob.asks)-1] = nil
				ob.asks = ob.asks[:len(ob.asks)-1]
				return true
			}
		}
	}
	return false
}
