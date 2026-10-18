package repository

import (
	"atlas-trading-infrastructure/internal/domain"
	"atlas-trading-infrastructure/internal/infrastructure/db"
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (r *PostgresRepository) CreateOrder(ctx context.Context, order *domain.Order) error {
	executor := r.GetExecutor(ctx)
	query := `
		INSERT INTO orders (id, userid, symbol, price, quantity, side, status, filled_quantity)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`

	_, err := executor.Exec(ctx, query, order.ID, order.Symbol, order.Side,
		order.Price, order.Quantity, order.FilledQuantity, order.Status)

	return err
}

func (r *PostgresRepository) BatchCreateOrders(ctx context.Context, orders []*domain.Order) error {
	if len(orders) == 0 {
		return nil
	}

	tx := db.GetTx(ctx)
	if tx == nil {
		return fmt.Errorf("BatchCreateOrders must be called within ExecTx")
	} // 确保 BatchCreateOrders 方法必须在事务内部调用

	rows := make([][]any, 0, len(orders))
	for _, order := range orders {
		rows = append(rows, []any{
			order.ID, order.UserID, order.Symbol, order.Side,
			order.Price, order.Quantity, order.FilledQuantity, order.Status,
		})
	}

	_, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"orders"},
		[]string{"id", "user_id", "symbol", "side", "price", "quantity", "filled_quantity", "status"},
		pgx.CopyFromRows(rows),
	)
	return err
}

func (r *PostgresRepository) GetOrder(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	executor := r.GetExecutor(ctx)
	query := `
		SELECT id, userid, symbol, side, price, quantity, filled_quantity, status
		FROM orders WHERE id = $1`

	row := executor.QueryRow(ctx, query, id)
	return scanOrder(row)
}

// FOR UPDATE 悲观锁在事务中锁定查询到的数据行，防止其他事务同时修改，保证数据一致性。
func (r *PostgresRepository) GetOrderForUpdate(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	executor := r.GetExecutor(ctx)
	query := `
		SELECT id, userid, symbol, side, price, quantity, filled_quantity, status
		FROM orders WHERE id = $1
		FOR UPDATE`
	row := executor.QueryRow(ctx, query, id)
	return scanOrder(row)
}

func (r *PostgresRepository) UpdateOrder(ctx context.Context, order *domain.Order) error {
	executor := r.GetExecutor(ctx)
	query := `
		UPDATE orders 
		SET filled_quantity = $1, status = $2
		WHERE id = $4`

	_, err := executor.Exec(ctx, query,
		order.FilledQuantity, order.Status, order.ID)
	return err
}

// 多行单行查询都实现了这个接口
type rowScanner interface {
	Scan(dest ...any) error
}

func scanOrder(row rowScanner) (*domain.Order, error) {
	var o domain.Order
	err := row.Scan(
		&o.ID, &o.Symbol, &o.Side,
		&o.Price, &o.Quantity, &o.FilledQuantity, &o.Status)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *PostgresRepository) GetActiveOrders(ctx context.Context) ([]*domain.Order, error) {
	executor := r.GetExecutor(ctx)
	// Status 1=NEW, 2=PARTIALLY_FILLED
	query := `
		SELECT id, user_id, symbol, side, price, quantity, filled_quantity, status, created_at, updated_at
		FROM orders 
		WHERE status IN (1, 2) 
		ORDER BY created_at ASC`

	rows, err := executor.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []*domain.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return orders, nil
}
