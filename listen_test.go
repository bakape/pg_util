package pg_util

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
)

type testConfig struct {
	DBURL string `json:"db_url"`
}

func TestReconnect(t *testing.T) {
	f, err := os.Open("test_config.json")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	var conf testConfig
	err = json.NewDecoder(f).Decode(&conf)
	if err != nil {
		t.Fatal(err)
	}
	connOpts, err := pgx.ParseConfig(conf.DBURL)
	if err != nil {
		t.Fatal(err)
	}

	var (
		wg                                        sync.WaitGroup
		ctx, cancel                               = context.WithCancel(context.Background())
		msgI                                      = 0
		errorFired, connLossFired, reconnectFired uint64
	)
	wg.Add(2)
	defer cancel()
	err = Listen(ListenOpts{
		ConnectionURL: conf.DBURL,
		Channel:       "test",
		Context:       ctx,
		OnError: func(_ error) {
			atomic.StoreUint64(&errorFired, 1)
		},
		OnConnectionLoss: func() {
			atomic.StoreUint64(&connLossFired, 1)
		},
		OnReconnect: func() {
			atomic.StoreUint64(&reconnectFired, 1)
		},
		OnMsg: func(s string) error {
			defer wg.Done()

			std := fmt.Sprintf("message_%d", msgI)
			if s != std {
				t.Fatalf("invalid message: %s != %s", s, std)
			}
			msgI++

			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	conn, err := pgx.ConnectConfig(context.Background(), connOpts)
	if err != nil {
		t.Fatal(err)
	}

	// Send first message
	_, err = conn.Exec(context.Background(), `notify test, 'message_0'`)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate disconnect
	_, err = conn.Exec(
		context.Background(),
		fmt.Sprintf(
			`SELECT pg_terminate_backend(pg_stat_activity.pid)
			FROM pg_stat_activity
			WHERE pg_stat_activity.datname = '%s'
			  AND pid <> pg_backend_pid();`,
			connOpts.Database,
		),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Send second message after the client reconnected
	time.Sleep(time.Second * 2)
	_, err = conn.Exec(context.Background(), `notify test, 'message_1'`)
	if err != nil {
		t.Fatal(err)
	}

	// Assert functions fired
	if atomic.LoadUint64(&errorFired) == 0 {
		t.Fatal("error handler did not fire")
	}
	if atomic.LoadUint64(&connLossFired) == 0 {
		t.Fatal("connection loss handler did not fire")
	}
	if atomic.LoadUint64(&reconnectFired) == 0 {
		t.Fatal("reconnection handler did not fire")
	}

	wg.Wait()
}
