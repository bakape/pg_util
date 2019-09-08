package pg_util

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx"
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
	connOpts, err := pgx.ParseURI(conf.DBURL)
	if err != nil {
		t.Fatal(err)
	}

	var (
		wg                        sync.WaitGroup
		ctx, cancel               = context.WithCancel(context.Background())
		msgI                      = 0
		errorFired, connLossFired bool
	)
	wg.Add(2)
	defer cancel()
	err = Listen(ListenOpts{
		ConnectionURL: conf.DBURL,
		Channel:       "test",
		Context:       ctx,
		OnError: func(_ error) {
			errorFired = true
		},
		OnConnectionLoss: func() {
			connLossFired = true
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

	conn, err := pgx.Connect(connOpts)
	if err != nil {
		t.Fatal(err)
	}

	// Send first message
	_, err = conn.Exec(`notify test, 'message_0'`)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate disconnect
	_, err = conn.Exec(
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
	_, err = conn.Exec(`notify test, 'message_1'`)
	if err != nil {
		t.Fatal(err)
	}

	// Assert functions fired
	if !errorFired {
		t.Fatal("error handler did not fire")
	}
	if !connLossFired {
		t.Fatal("connection loss handler did not fire")
	}

	wg.Wait()
}
