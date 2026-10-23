package repository

import (
	"atlas-trading-infrastructure/internal/domain"
	"atlas-trading-infrastructure/internal/infrastructure/db"
	"context"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

func (r *MySQLRepository) CreateOrder(ctx context.Context, order *domain.Order) error {
	executor := r.GetExecutor(ctx)
	query := `
		INSERT INTO orders (id, user_id, symbol, side, price, quantity, filled_quantity, status)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := executor.ExecContext(ctx, query,
		order.ID, order.UserID, order.Symbol, order.Side,
		order.Price, order.Quantity, order.FilledQuantity, order.Status)

	return err
}

func (r *MySQLRepository) BatchCreateOrders(ctx context.Context, orders []*domain.Order) error {
	if len(orders) == 0 {
		return nil
	}

	tx := db.GetTx(ctx)
	if tx == nil {
		return fmt.Errorf("BatchCreateOrders must be called within ExecTx")
	} // 确保 BatchCreateOrders 方法必须在事务内部调用

	// MySQL 没有 COPY 协议，改为多值 INSERT
	const columns = "(id, user_id, symbol, side, price, quantity, filled_quantity, status)"
	const rowPlaceholders = "(?, ?, ?, ?, ?, ?, ?, ?)"

	var sb strings.Builder
	sb.WriteString("INSERT INTO orders ")
	sb.WriteString(columns)
	sb.WriteString(" VALUES ")

	args := make([]any, 0, len(orders)*8)
	for i, order := range orders {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(rowPlaceholders)
		args = append(args,
			order.ID, order.UserID, order.Symbol, order.Side,
			order.Price, order.Quantity, order.FilledQuantity, order.Status)
	}

	_, err := tx.ExecContext(ctx, sb.String(), args...)
	return err
}

func (r *MySQLRepository) GetOrder(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	executor := r.GetExecutor(ctx)
	query := `
		SELECT id, user_id, symbol, side, price, quantity, filled_quantity, status
		FROM orders WHERE id = ?`

	row := executor.QueryRowContext(ctx, query, id)
	return scanOrder(row)
}

// FOR UPDATE 悲观锁在事务中锁定查询到的数据行，防止其他事务同时修改，保证数据一致性。
func (r *MySQLRepository) GetOrderForUpdate(ctx context.Context, id uuid.UUID) (*domain.Order, error) {
	executor := r.GetExecutor(ctx)
	query := `
		SELECT id, user_id, symbol, side, price, quantity, filled_quantity, status
		FROM orders WHERE id = ?
		FOR UPDATE`
	row := executor.QueryRowContext(ctx, query, id)
	return scanOrder(row)
}

func (r *MySQLRepository) UpdateOrder(ctx context.Context, order *domain.Order) error {
	executor := r.GetExecutor(ctx)
	query := `
		UPDATE orders
		SET filled_quantity = ?, status = ?
		WHERE id = ?`

	_, err := executor.ExecContext(ctx, query,
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
		&o.ID, &o.UserID, &o.Symbol, &o.Side,
		&o.Price, &o.Quantity, &o.FilledQuantity, &o.Status)
	if err != nil {
		return nil, err
	}
	return &o, nil
}

func (r *MySQLRepository) GetActiveOrders(ctx context.Context) ([]*domain.Order, error) {
	executor := r.GetExecutor(ctx)
	// Status 1=NEW, 2=PARTIALLY_FILLED
	query := `
		SELECT id, user_id, symbol, side, price, quantity, filled_quantity, status
		FROM orders
		WHERE status IN (1, 2)
		ORDER BY created_at ASC`

	rows, err := executor.QueryContext(ctx, query)
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
