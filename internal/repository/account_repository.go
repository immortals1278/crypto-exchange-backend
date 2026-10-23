package repository

import (
	"atlas-trading-infrastructure/internal/domain"
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func (r *MySQLRepository) UpdateBalance(ctx context.Context, userID uuid.UUID, currency string, amount decimal.Decimal) error {
	executor := r.GetExecutor(ctx)
	query := `
		UPDATE accounts
		SET balance = balance + ?, updated_at = ?
		WHERE user_id = ? AND currency = ?`
	_, err := executor.ExecContext(ctx, query, amount, time.Now().UnixMilli(), userID, currency)

	return err
}

func (r *MySQLRepository) LockFunds(ctx context.Context, userID uuid.UUID, currency string, amount decimal.Decimal) error {
	executor := r.GetExecutor(ctx)
	query := `
		UPDATE accounts
		SET balance = balance - ?, locked = locked + ?
		WHERE user_id = ? AND currency = ? AND balance >= ?`

	tag, err := executor.ExecContext(ctx, query, amount, amount, userID, currency, amount)
	if err != nil {
		return err
	}

	// 返回 query 操作了多少行数据
	affected, err := tag.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return domain.ErrInsufficientFunds
	}

	return nil
}

func (r *MySQLRepository) BatchLockFunds(ctx context.Context, lockFunds map[uuid.UUID]map[string]decimal.Decimal) error {
	executor := r.GetExecutor(ctx)

	UserID := make([]uuid.UUID, 0, len(lockFunds))

	for uid := range lockFunds {
		UserID = append(UserID, uid)
	}
	// 排序防死锁
	for i := 0; i < len(lockFunds); i++ {
		for j := i + 1; j < len(lockFunds); j++ {
			if UserID[i].String() > UserID[j].String() {
				UserID[i], UserID[j] = UserID[j], UserID[i]
			}
		}
	}

	for _, uid := range UserID {
		currency := lockFunds[uid] //cur -> amount
		curkey := make([]string, 0, len(currency))
		for cur := range currency {
			curkey = append(curkey, cur)
		}
		//给币种排序防死锁
		for i := 0; i < len(curkey); i++ {
			for j := i + 1; j < len(curkey); j++ {
				if curkey[i] > curkey[j] {
					curkey[i], curkey[j] = curkey[j], curkey[i]
				}
			}
		}

		for _, cur := range curkey {
			amount := currency[cur]
			query := `
				UPDATE accounts
				SET balance = balance - ?, locked = locked + ?
				WHERE user_id = ? AND currency = ? AND balance >= ?`
			tag, err := executor.ExecContext(ctx, query, amount, amount, uid, cur, amount)
			if err != nil {
				return err
			}
			affected, err := tag.RowsAffected()
			if err != nil {
				return err
			}
			if affected == 0 {
				return domain.ErrInsufficientFunds
			}
		}
	}

	return nil
}

func (r *MySQLRepository) UnlockFunds(ctx context.Context, userID uuid.UUID, currency string, amount decimal.Decimal) error {
	executor := r.GetExecutor(ctx)
	query := `
		UPDATE accounts
		SET balance = balance + ?, locked = locked - ?
		WHERE user_id = ? AND currency = ? AND locked >= ?`

	_, err := executor.ExecContext(ctx, query, amount, amount, userID, currency, amount)
	if err != nil {
		return err
	}

	return nil
}

func (r *MySQLRepository) GetAccountsByUser(ctx context.Context, userID uuid.UUID) ([]*domain.Account, error) {
	executor := r.GetExecutor(ctx)
	query := `SELECT id, user_id, currency, balance, locked, created_at, updated_at FROM accounts WHERE user_id = ?`

	rows, err := executor.QueryContext(ctx, query, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var accounts []*domain.Account
	for rows.Next() {
		var acc domain.Account
		if err := rows.Scan(&acc.ID, &acc.UserID, &acc.Currency, &acc.Balance, &acc.Locked, &acc.CreatedAt, &acc.UpdatedAt); err != nil {
			return nil, err
		}
		accounts = append(accounts, &acc)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return accounts, nil
}
