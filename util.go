package pg_util

import "database/sql"

// InTransaction runs a function inside a transaction and handles commiting
// and rollback on error.
func InTransaction(db *sql.DB, fn func(*sql.Tx) error) (err error) {
	tx, err := db.Begin()
	if err != nil {
		return
	}

	err = fn(tx)
	if err != nil {
		tx.Rollback()
		return
	}
	return tx.Commit()
}

// Execute all SQL statement strings and return on first error, if any
func ExecAll(tx *sql.Tx, q ...string) error {
	for _, q := range q {
		if _, err := tx.Exec(q); err != nil {
			return err
		}
	}
	return nil
}
