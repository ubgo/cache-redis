package rediscache

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
	"github.com/ubgo/cache"
)

// Invalidation implements cache.Invalidation over Redis Pub/Sub so every pod
// can drop its local L1 copy when any pod mutates a key.
//
//	inv := rediscache.NewInvalidation(rdb, "cache:invalidate")
//	tc  := tieredcache.New(
//	    tieredcache.WithL1(mem),
//	    tieredcache.WithL2(rediscache.New(rdb)),
//	    tieredcache.WithInvalidation(inv),
//	)
type Invalidation struct {
	rdb     redis.UniversalClient
	channel string
}

// NewInvalidation builds a Redis-backed invalidation bus on channel.
func NewInvalidation(rdb redis.UniversalClient, channel string) *Invalidation {
	return &Invalidation{rdb: rdb, channel: channel}
}

// Publish announces invalidated keys (one Pub/Sub message each). One message
// per key — not a batched payload — so a subscriber can act on each key as it
// arrives without parsing. The sentinel cache.InvalidateAll is published as an
// ordinary key by callers (e.g. tiered Flush) and interpreted by Subscribe's
// consumer. Returns on the first PUBLISH error; already-sent keys are not
// rolled back (invalidation is best-effort / at-most-once by design).
func (i *Invalidation) Publish(ctx context.Context, keys ...string) error {
	for _, k := range keys {
		if err := i.rdb.Publish(ctx, i.channel, k).Err(); err != nil {
			return err
		}
	}
	return nil
}

// Subscribe delivers invalidated keys until ctx is cancelled. Blocks; run in
// its own goroutine.
func (i *Invalidation) Subscribe(ctx context.Context, fn func(key string)) error {
	ps := i.rdb.Subscribe(ctx, i.channel)
	// Always close the subscription so the underlying connection is returned
	// to the pool even on context cancellation or channel close.
	defer func() { _ = ps.Close() }()
	ch := ps.Channel()
	// Blocks until ctx is cancelled (clean shutdown -> ctx.Err) or the
	// Pub/Sub channel closes unexpectedly (-> error). Pub/Sub is fire-and-
	// forget: messages published while no subscriber is connected are lost,
	// which is acceptable because a missed invalidation only costs one stale
	// L1 read until the L1 TTL elapses.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case msg, ok := <-ch:
			if !ok {
				return errors.New("rediscache: invalidation channel closed")
			}
			fn(msg.Payload)
		}
	}
}

var _ cache.Invalidation = (*Invalidation)(nil)
