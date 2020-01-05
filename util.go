package pg_util

import (
	"context"

	"github.com/jackc/pgx/v4"
)

// Ensure context is not nill
func ensureCtx(c *context.Context) {
	if *c == nil {
		*c = context.Background()
	}
}

// Interface required to start a transaction or subtransation via savepoints
type TxStarter interface {
	Begin(context.Context) (pgx.Tx, error)
}

// InTransaction runs a function inside a transaction and handles commiting
// and rollback on error.
//
// Can also be used for nested pseudotransactions via savepoints.
//
// ctx: Context to bind the query to. Defaults to context.Background().
// conn: Anything, that can start a new transaction or subtransaction.
// fn: Function to execute on the transaction.
func InTransaction(
	ctx context.Context,
	conn TxStarter,
	fn func(pgx.Tx) error,
) (err error) {
	ensureCtx(&ctx)

	tx, err := conn.Begin(ctx)
	if err != nil {
		return
	}

	err = fn(tx)
	if err != nil {
		tx.Rollback(ctx)
		return
	}
	return tx.Commit(ctx)
}

// Execute all SQL statement strings and return on first error, if any.
//
// ctx: Context to bind the query to. Defaults to context.Background().
func ExecAll(ctx context.Context, tx pgx.Tx, q ...string) error {
	ensureCtx(&ctx)

	for _, q := range q {
		if _, err := tx.Exec(context.Background(), q); err != nil {
			return err
		}
	}
	return nil
}
