package repository

import (
	"atlas-trading-infrastructure/internal/matching/engine"
	"context"

	"github.com/google/uuid"
)

func (r *MySQLRepository) CreateTrade(ctx context.Context, trade *engine.Trade) error {
	executor := r.GetExecutor(ctx)

	query := `
	INSERT INTO trades (id, symbol, maker_order_id, taker_order_id, price, quantity, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)`
	_, err := executor.ExecContext(ctx, query, trade.ID, trade.Symbol, trade.MakerOrderID, trade.TakerOrderID,
		trade.Price, trade.Quantity, trade.CreatedAt)
	return err
}

func (r *MySQLRepository) TradeExistsByID(ctx context.Context, id uuid.UUID) (bool, error) {
	executor := r.GetExecutor(ctx)
	var exists bool
	query := `SELECT EXISTS(SELECT 1 FROM trades WHERE id = ?)`
	err := executor.QueryRowContext(ctx, query, id).Scan(&exists)
	return exists, err
}
