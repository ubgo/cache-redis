// rediscache_test.go — tests for the rediscache adapter (conformance suite via miniredis, prefix isolation, TTL/Close edge cases).

package rediscache_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/ubgo/cache"
	rediscache "github.com/ubgo/cache-redis"
	"github.com/ubgo/cache/cachetest"
)

// newMini starts an in-process miniredis whose clock is advanced in real time
// by a background ticker, so the conformance suite's wall-clock TTL sleeps
// actually expire keys — no Docker, no real Redis.
func newMini(t testing.TB) *redis.Client {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
				mr.FastForward(20 * time.Millisecond)
			}
		}
	}()
	t.Cleanup(func() {
		close(stop)
		wg.Wait()
		mr.Close()
	})
	return redis.NewClient(&redis.Options{Addr: mr.Addr()})
}

func TestConformance(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) cache.Cache {
		return rediscache.New(newMini(t))
	})
}

func TestConformanceWithPrefix(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) cache.Cache {
		return rediscache.New(newMini(t), rediscache.WithPrefix("svc:test"))
	})
}

func TestPrefixIsolationAndScopedFlush(t *testing.T) {
	ctx := context.Background()
	rdb := newMini(t)
	a := rediscache.New(rdb, rediscache.WithPrefix("a"))
	b := rediscache.New(rdb, rediscache.WithPrefix("b"))

	if err := a.Set(ctx, "k", []byte("av"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := b.Set(ctx, "k", []byte("bv"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := a.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if ok, _ := a.Has(ctx, "k"); ok {
		t.Fatal("a flush did not clear a")
	}
	if ok, _ := b.Has(ctx, "k"); !ok {
		t.Fatal("a flush wrongly cleared b (prefix isolation broken)")
	}
}

func TestTTLReportsNoExpiry(t *testing.T) {
	ctx := context.Background()
	c := rediscache.New(newMini(t))
	if err := c.Set(ctx, "k", []byte("v"), 0); err != nil {
		t.Fatal(err)
	}
	d, err := c.TTL(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if d != 0 {
		t.Fatalf("want 0 for no-expiry key, got %v", d)
	}
	if _, err := c.TTL(ctx, "missing"); err != cache.ErrNotFound {
		t.Fatalf("want ErrNotFound for missing key, got %v", err)
	}
}

func TestClosedReturnsErrClosed(t *testing.T) {
	ctx := context.Background()
	c := rediscache.New(newMini(t))
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close must be idempotent, got %v", err)
	}
	if _, err := c.Get(ctx, "k"); err != cache.ErrClosed {
		t.Fatalf("want ErrClosed after Close, got %v", err)
	}
}
