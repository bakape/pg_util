package pg_util

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx"
)

// Options for calling Listen()
type ListenOpts struct {
	// Prevent identical messages from triggering the handler for up to
	// DebounceInterval. If 0, all messages trigger the handler.
	DebounceInterval time.Duration

	// URL to connect on to the database. Required.
	ConnectionURL string

	// Channel to listen on. Required.
	Channel string

	// Message handler. Required.
	OnMsg func(msg string) error

	// Optional error handler
	OnError func(err error)

	// Optional channel for cancelling listening
	Canceller <-chan struct{}
}

// Listen assigns a function to listen to Postgres notifications on a channel
func Listen(opts ListenOpts) (err error) {
	connOpts, err := pgx.ParseURI(opts.ConnectionURL)
	if err != nil {
		return
	}
	conn, err := pgx.Connect(connOpts)
	if err != nil {
		return
	}
	err = conn.Listen(opts.Channel)
	if err != nil {
		return
	}

	type message struct {
		string
		error
	}

	receive := make(chan message)
	go func() {
		for {
			n, err := conn.WaitForNotification(context.Background())
			if err != nil {
				return
			}
			receive <- message{n.Payload, err}
		}
	}()

	go func() {
		pending := make(map[string]struct{})
		runPending := make(chan string)

		handleError := func(format string, args ...interface{}) {
			if opts.OnError != nil {
				format = "pg_util: " + format
				opts.OnError(fmt.Errorf(format, args...))
			}
		}

		handle := func(msg string) {
			err := opts.OnMsg(msg)
			if err != nil {
				handleError(
					"listening on channel=`%s` msg=`%s` error=`%s",
					opts.Channel, msg, err,
				)
			}
		}

		for {
			select {
			case <-opts.Canceller:
				err := conn.Unlisten(opts.Channel)
				if err != nil {
					handleError(
						"unlistening channel=`%s` error=`%s`",
						opts.Channel, err,
					)
					return
				}
			case msg := <-receive:
				if msg.error != nil {
					handleError(
						"receiving message channel=%s error=%s",
						opts.Channel, err,
					)
				} else {
					if opts.DebounceInterval == 0 {
						handle(msg.string)
					} else {
						_, ok := pending[msg.string]
						if !ok {
							pending[msg.string] = struct{}{}
							time.AfterFunc(opts.DebounceInterval, func() {
								runPending <- msg.string
							})
						}
					}
				}
			case msg := <-runPending:
				delete(pending, msg)
				handle(msg)
			}
		}
	}()

	return
}
