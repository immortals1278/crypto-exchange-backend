package db

import (
	"context"
	"database/sql"
)

type txKeyType struct{}

var TxKey = txKeyType{}

// 从 context 中取得事务 *sql.Tx
func GetTx(ctx context.Context) *sql.Tx {
	tx, _ := ctx.Value(TxKey).(*sql.Tx)
	return tx
}
