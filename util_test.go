package pg_util

import (
	"context"
	"encoding/json"
	"os"
	"sync"
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

	var (
		u  = getURL(t)
		wg sync.WaitGroup
	)

	conn, err := pgx.Connect(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}

	pool, err := pgxpool.Connect(context.Background(), u)
	if err != nil {
		t.Fatal(err)
	}

	cases := [...]struct {
		name    string
		starter TxStarter
		ctx     context.Context
	}{
		{"connection", conn, context.Background()},
		{"pool", pool, context.Background()},
		{"nil context", pool, nil},
	}

	for i := range cases {
		c := cases[i]
		wg.Add(1)
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			defer wg.Done()

			err := InTransaction(c.ctx, c.starter, func(tx pgx.Tx) (err error) {
				_, err = tx.Exec(context.Background(), "select 1")
				return
			})
			if err != nil {
				t.Fatal(err)
			}
		})
	}

	go func() {
		wg.Wait()

		conn.Close(context.Background())
		pool.Close()
	}()
}
