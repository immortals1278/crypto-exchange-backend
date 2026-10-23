package repository

import (
	"atlas-trading-infrastructure/internal/domain"
	"context"
)

func (r *MySQLRepository) CreateUser(ctx context.Context, user *domain.User) error {
	executor := r.GetExecutor(ctx)

	query := `INSERT INTO users (id, email, password_hash, created_at, updated_at) VALUES (?, ?, ?, ?, ?)`
	_, err := executor.ExecContext(ctx, query, user.ID, user.Email, user.PasswordHash, user.CreatedAt, user.UpdatedAt)
	return err
}

func (r *MySQLRepository) GetUserByEmail(ctx context.Context, email string) (*domain.User, error) {
	executor := r.GetExecutor(ctx)
	query := `SELECT id, email, password_hash, created_at, updated_at FROM users WHERE email = ?`

	row := executor.QueryRowContext(ctx, query, email)
	var u domain.User
	err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.CreatedAt, &u.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &u, nil
}
