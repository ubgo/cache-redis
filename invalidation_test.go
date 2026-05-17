// invalidation_test.go — tests for Redis Pub/Sub invalidation (publish/subscribe round-trip, interface conformance).

package rediscache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ubgo/cache"
	rediscache "github.com/ubgo/cache-redis"
)

func TestRedisInvalidationPubSub(t *testing.T) {
	rdb := newMini(t)
	inv := rediscache.NewInvalidation(rdb, "cache:invalidate")

	var mu sync.Mutex
	var got []string
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = inv.Subscribe(ctx, func(k string) {
			mu.Lock()
			got = append(got, k)
			mu.Unlock()
		})
	}()
	time.Sleep(80 * time.Millisecond) // let SUBSCRIBE register

	if err := inv.Publish(context.Background(), "k1", "k2"); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0] != "k1" || got[1] != "k2" {
		t.Fatalf("got %v, want [k1 k2]", got)
	}
}

func TestRedisInvalidationImplementsInterface(t *testing.T) {
	var _ cache.Invalidation = rediscache.NewInvalidation(newMini(t), "ch")
}
