package pg_util

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v4"
)

// Options for calling Listen()
type ListenOpts struct {
	// Prevent identical messages from triggering the handler for up to
	// DebounceInterval. If 0, all messages trigger the handler.
	DebounceInterval time.Duration

	// URL to connect to the database on. Required.
	ConnectionURL string

	// Channel to listen on. Required.
	Channel string

	// Message handler. Required.
	OnMsg func(msg string) error

	// Optional error handler
	OnError func(err error)

	// Optional handler for database connection loss. The connection will be
	// automatically reestablished regardless, but this can be used to hook
	// extra logic on the library user's side of the application.
	OnConnectionLoss func()

	// Optional handler for reconnection after database connection loss
	OnReconnect func()

	// Optional context for cancelling listening
	Context context.Context
}

// Listen assigns a function to listen to Postgres notifications on a channel
func Listen(opts ListenOpts) (err error) {
	if opts.Context == nil {
		opts.Context = context.Background()
	}

	connOpts, err := pgx.ParseConfig(opts.ConnectionURL)
	if err != nil {
		return
	}

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
				"listening on channel=%s msg=%s error=%s",
				opts.Channel, msg, err,
			)
		}
	}

	reconnect := make(chan struct{})

	// Reusable function for handling connection loss
	listen := func(conn *pgx.Conn, ctx context.Context) (err error) {
		_, err = conn.Exec(opts.Context, `listen `+strconv.Quote(opts.Channel))
		if err != nil {
			return
		}

		ctx, cancel := context.WithCancel(ctx)
		receive := make(chan string)
		go func() {
			defer cancel()                         // Don't leak child context
			defer conn.Close(context.Background()) // Or connection

			for {
				n, err := conn.WaitForNotification(ctx)
				if err != nil {
					cancel()
					if opts.OnConnectionLoss != nil {
						opts.OnConnectionLoss()
					}
					handleError(
						"wating for message channel=%s error=%s",
						opts.Channel, err,
					)
					select {
					case <-opts.Context.Done():
					case reconnect <- struct{}{}:
					}
					return
				}
				select {
				case <-ctx.Done():
					return
				case receive <- n.Payload:
				}
			}
		}()

		go func() {
			pending := make(map[string]struct{})
			runPending := make(chan string)

			for {
				select {
				case <-ctx.Done():
					return
				case msg := <-receive:
					if opts.DebounceInterval == 0 {
						handle(msg)
					} else {
						_, ok := pending[msg]
						if !ok {
							pending[msg] = struct{}{}
							time.AfterFunc(opts.DebounceInterval, func() {
								select {
								case <-ctx.Done():
								case runPending <- msg:
								}
							})
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

	conn, err := pgx.ConnectConfig(opts.Context, connOpts)
	if err != nil {
		return
	}
	err = listen(conn, opts.Context)
	if err != nil {
		return
	}

	go func() {
		for {
			select {
			case <-opts.Context.Done():
				return
			case <-reconnect:
			reconnect:
				for {
					conn, err := pgx.ConnectConfig(opts.Context, connOpts)
					switch err {
					case nil:
						err = listen(conn, opts.Context)
						if err == nil {
							if opts.OnReconnect != nil {
								opts.OnReconnect()
							}
							break reconnect
						} else {
							handleError(
								"reconnecting channel=%s error=%s",
								opts.Channel, err,
							)
						}
					default:
						handleError(
							"reconnecting channel=%s error=%s",
							opts.Channel, err,
						)
					}

					// Try to reconnect again after one second, if parent
					// context still open
					select {
					case <-opts.Context.Done():
						return
					case <-time.After(time.Second):
					}
				}
			}
		}
	}()

	return
}
