package pg_util

import (
	"fmt"
	"time"

	"github.com/lib/pq"
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

	// Optional connection loss handler
	OnConnectionLoss func()

	// Optional error handler
	OnError func(err error)

	// Optional channel for cancelling listening
	Canceller <-chan struct{}
}

// Listen assigns a function to listen to Postgres notifications on a channel
func Listen(opts ListenOpts) (err error) {
	l := pq.NewListener(opts.ConnectionURL, time.Second, time.Second*10, nil)
	err = l.Listen(opts.Channel)
	if err != nil {
		return
	}

	go func() {
		pending := make(map[string]struct{})
		runPending := make(chan string)

		handle := func(msg string) {
			err := opts.OnMsg(msg)
			if err != nil && opts.OnError != nil {
				opts.OnError(
					fmt.Errorf(
						"pg_util: listening on channel=`%s` msg=`%s` error=`%s`\n",
						opts.Channel, msg, err,
					),
				)
			}
		}

		for {
			select {
			case <-opts.Canceller:
				err := l.UnlistenAll()
				if err != nil && opts.OnError != nil {
					if opts.OnError != nil {
						opts.OnError(
							fmt.Errorf(
								"pg_util: unlistening channel=`%s` error=`%s`\n",
								opts.Channel, err,
							),
						)
					}
					return
				}
			case msg := <-l.Notify:
				if msg == nil {
					if opts.OnConnectionLoss != nil {
						opts.OnConnectionLoss()
					}
				} else {
					if opts.DebounceInterval == 0 {
						handle(msg.Extra)
					} else {
						_, ok := pending[msg.Extra]
						if !ok {
							pending[msg.Extra] = struct{}{}
							time.AfterFunc(opts.DebounceInterval, func() {
								runPending <- msg.Extra
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
