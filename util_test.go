package pg_util

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

// Return database URL
func getURL(t *testing.T) string {

	t.Helper()

	f, err := os.Open("test_config.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var conf struct {
		DBURL string `json:"db_url"`
	}
	err = json.NewDecoder(f).Decode(&conf)
	if err != nil {
		t.Fatal(err)
	}
	return conf.DBURL
}

func TestInTransaction(t *testing.T) {
	t.Parallel()

	u := getURL(t)
	conn, err := pgx.Connect(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	pool, err := pgxpool.Connect(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())

	cases := [...]struct {
		name    string
		starter TxStarter
	}{
		{"connection", conn},
		{"pool", pool},
		{"transaction", tx},
	}

	for i := range cases {
		c := cases[i]
		t.Run(c.name, func(t *testing.T) {
			err := InTransaction(
				context.Background(),
				c.starter,
				func(tx pgx.Tx) (err error) {
					_, err = tx.Exec(context.Background(), "select 1")
					return
				},
			)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestInTransactionPanic(t *testing.T) {
	t.Parallel()

	u := getURL(t)
	conn, err := pgx.Connect(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())

	var _tx pgx.Tx
	defer func() {
		recover()
		err = _tx.Rollback(context.Background())
		if err != pgx.ErrTxClosed {
			t.Fatalf("unexpected error: %s", err)
		}
	}()
	err = InTransaction(
		context.Background(),
		conn,
		func(tx pgx.Tx) (err error) {
			_tx = tx
			_, err = tx.Exec(context.Background(), "select 1")
			if err != nil {
				return
			}
			panic("foo")
		},
	)
	if err != nil {
		t.Fatal(err)
	}
}
