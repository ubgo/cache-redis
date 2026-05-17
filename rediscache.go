// rediscache.go — the Redis-backed cache.Cache adapter (package rediscache, github.com/ubgo/cache-redis).
//
// Package role: rediscache is the Redis 6+ adapter in the ubgo/cache family;
// it makes a go-redis client satisfy the cache.Cache contract so callers stay
// backend-agnostic. See doc.go for the package overview and invariants.
//
// This file: defines Cache (the adapter), Option/WithPrefix, New, the key
// namespacing helper k, mapErr (redis.Nil -> cache.ErrNotFound), and every
// cache.Cache method. Invariants an AI must keep: Expire uses PEXPIRE
// (millisecond precision, never plain EXPIRE which rounds sub-second TTLs up
// to 1s); keyspace walks use cursor-based SCAN (batch 512) — never KEYS;
// Flush is prefix-scoped when a prefix is set (else FlushDB); the iter cursor
// silently skips a key that expires between SCAN and GET (a correct race);
// Close only marks the adapter closed, never closing the shared client.
//
// AI-context: this is an adapter-of-cache.Cache — it owns no concurrency
// primitive except the atomic `closed` flag; goroutine safety is delegated to
// go-redis and to Redis's atomic commands.

package rediscache

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ubgo/cache"
)

// Cache adapts a go-redis client to cache.Cache. Construct with New.
//
// The only mutable state is closed; prefix is set once at construction and
// read-only thereafter. All goroutine safety is delegated to go-redis (the
// client is concurrency-safe) and to Redis (individual commands are atomic),
// so Cache itself needs no locks.
type Cache struct {
	rdb    redis.UniversalClient
	prefix string
	// closed is checked first by every method so a closed adapter never
	// touches the network. atomic so Close can race safely with in-flight ops.
	closed atomic.Bool
}

// Option configures New.
type Option func(*Cache)

// WithPrefix isolates this cache from other users of the same Redis DB. A
// trailing ":" is added if absent. Flush is scoped to the prefix.
func WithPrefix(p string) Option {
	return func(c *Cache) {
		if p != "" && p[len(p)-1] != ':' {
			p += ":"
		}
		c.prefix = p
	}
}

// New wraps a go-redis client (redis.Client, ClusterClient, …).
func New(rdb redis.UniversalClient, opts ...Option) *Cache {
	c := &Cache{rdb: rdb}
	for _, o := range opts {
		o(c)
	}
	return c
}

// k namespaces a contract-level key into the Redis keyspace. With no prefix
// it is the identity; the inverse (de-prefixing) happens only in iter.Next.
func (c *Cache) k(key string) string { return c.prefix + key }

// mapErr normalises Redis's "no such key" sentinel to the contract's
// ErrNotFound so callers never have to import go-redis to detect a miss. All
// other errors pass through unwrapped so callers can type-assert net errors.
func mapErr(err error) error {
	if errors.Is(err, redis.Nil) {
		return cache.ErrNotFound
	}
	return err
}

// Get implements cache.Cache.
func (c *Cache) Get(ctx context.Context, key string) ([]byte, error) {
	if c.closed.Load() {
		return nil, cache.ErrClosed
	}
	b, err := c.rdb.Get(ctx, c.k(key)).Bytes()
	if err != nil {
		return nil, mapErr(err)
	}
	return b, nil
}

// GetMulti implements cache.Cache.
func (c *Cache) GetMulti(ctx context.Context, keys []string) (map[string][]byte, error) {
	if c.closed.Load() {
		return nil, cache.ErrClosed
	}
	if len(keys) == 0 {
		return map[string][]byte{}, nil
	}
	pk := make([]string, len(keys))
	for i, key := range keys {
		pk[i] = c.k(key)
	}
	vals, err := c.rdb.MGet(ctx, pk...).Result()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]byte, len(keys))
	for i, v := range vals {
		if v == nil {
			continue
		}
		switch t := v.(type) {
		case string:
			out[keys[i]] = []byte(t)
		case []byte:
			out[keys[i]] = t
		}
	}
	return out, nil
}

// Has implements cache.Cache.
func (c *Cache) Has(ctx context.Context, key string) (bool, error) {
	if c.closed.Load() {
		return false, cache.ErrClosed
	}
	n, err := c.rdb.Exists(ctx, c.k(key)).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// TTL implements cache.Cache.
func (c *Cache) TTL(ctx context.Context, key string) (time.Duration, error) {
	if c.closed.Load() {
		return 0, cache.ErrClosed
	}
	d, err := c.rdb.TTL(ctx, c.k(key)).Result()
	if err != nil {
		return 0, err
	}
	// go-redis maps Redis's -2 (no key) / -1 (no expiry) to those raw
	// time.Duration values.
	if d == -2*time.Nanosecond {
		return 0, cache.ErrNotFound
	}
	if d < 0 { // -1: exists, no expiry
		return 0, nil
	}
	return d, nil
}

// Set implements cache.Cache.
func (c *Cache) Set(ctx context.Context, key string, val []byte, ttl time.Duration) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	if ttl < 0 {
		ttl = 0
	}
	return c.rdb.Set(ctx, c.k(key), val, ttl).Err()
}

// SetMulti implements cache.Cache.
func (c *Cache) SetMulti(ctx context.Context, items map[string]cache.Item) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	pipe := c.rdb.Pipeline()
	for k, it := range items {
		ttl := it.TTL
		if ttl < 0 {
			ttl = 0
		}
		pipe.Set(ctx, c.k(k), it.Value, ttl)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// SetNX implements cache.Cache.
func (c *Cache) SetNX(ctx context.Context, key string, val []byte, ttl time.Duration) (bool, error) {
	if c.closed.Load() {
		return false, cache.ErrClosed
	}
	if ttl < 0 {
		ttl = 0
	}
	return c.rdb.SetNX(ctx, c.k(key), val, ttl).Result()
}

// Expire implements cache.Cache.
func (c *Cache) Expire(ctx context.Context, key string, ttl time.Duration) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	if ttl <= 0 {
		ok, err := c.rdb.Persist(ctx, c.k(key)).Result()
		if err != nil {
			return err
		}
		if !ok {
			// Persist returns false if key missing OR already had no TTL;
			// disambiguate with Exists.
			if has, _ := c.Has(ctx, key); !has {
				return cache.ErrNotFound
			}
		}
		return nil
	}
	// PExpire (millisecond precision) — plain EXPIRE truncates sub-second
	// TTLs up to 1s, which breaks short-lived entries.
	ok, err := c.rdb.PExpire(ctx, c.k(key), ttl).Result()
	if err != nil {
		return err
	}
	if !ok {
		return cache.ErrNotFound
	}
	return nil
}

// Touch implements cache.Cache.
func (c *Cache) Touch(ctx context.Context, key string) error {
	return c.Expire(ctx, key, time.Hour)
}

// Incr implements cache.Cache.
func (c *Cache) Incr(ctx context.Context, key string, delta int64) (int64, error) {
	if c.closed.Load() {
		return 0, cache.ErrClosed
	}
	return c.rdb.IncrBy(ctx, c.k(key), delta).Result()
}

// Decr implements cache.Cache.
func (c *Cache) Decr(ctx context.Context, key string, delta int64) (int64, error) {
	if c.closed.Load() {
		return 0, cache.ErrClosed
	}
	return c.rdb.DecrBy(ctx, c.k(key), delta).Result()
}

// Del implements cache.Cache.
func (c *Cache) Del(ctx context.Context, keys ...string) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	if len(keys) == 0 {
		return nil
	}
	pk := make([]string, len(keys))
	for i, key := range keys {
		pk[i] = c.k(key)
	}
	return c.rdb.Del(ctx, pk...).Err()
}

// scanKeys walks every key matching the glob `match` using a cursor-based
// SCAN (batch size 512) and applies fn to each non-empty batch. SCAN — never
// KEYS — keeps this O(1) memory and non-blocking even on multi-million-key
// databases. Shared by DeleteByPrefix and prefix-scoped Flush.
func (c *Cache) scanKeys(ctx context.Context, match string, fn func(batch []string) error) error {
	var cursor uint64
	for {
		batch, next, err := c.rdb.Scan(ctx, cursor, match, 512).Result()
		if err != nil {
			return err
		}
		if len(batch) > 0 {
			if err := fn(batch); err != nil {
				return err
			}
		}
		cursor = next
		if cursor == 0 {
			return nil
		}
	}
}

// DeleteByPrefix implements cache.Cache. SCAN-based (never KEYS).
func (c *Cache) DeleteByPrefix(ctx context.Context, prefix string) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	return c.scanKeys(ctx, c.k(prefix)+"*", func(batch []string) error {
		return c.rdb.Del(ctx, batch...).Err()
	})
}

// Flush implements cache.Cache. Scoped to the prefix when one is set.
func (c *Cache) Flush(ctx context.Context) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	if c.prefix != "" {
		return c.scanKeys(ctx, c.prefix+"*", func(batch []string) error {
			return c.rdb.Del(ctx, batch...).Err()
		})
	}
	return c.rdb.FlushDB(ctx).Err()
}

// Iterate implements cache.Cache. SCAN cursor-based.
func (c *Cache) Iterate(ctx context.Context, opts cache.IterateOpts) cache.Iterator {
	count := int64(opts.Count)
	if count <= 0 {
		count = 256
	}
	return &iter{
		c:     c,
		ctx:   ctx,
		match: c.k(opts.Prefix) + "*",
		count: count,
	}
}

// iter is a forward-only SCAN cursor. It cannot reuse scanKeys because it must
// yield key/value pairs lazily (one GET per key) rather than process whole
// batches. A key that expires between SCAN and GET is silently skipped — that
// is a correct, expected race, not an error.
type iter struct {
	c      *Cache
	ctx    context.Context
	match  string
	count  int64
	cursor uint64
	done   bool
	buf    []string
	k      string
	v      []byte
	err    error
}

func (it *iter) Next() bool {
	for {
		if len(it.buf) == 0 {
			if it.done {
				return false
			}
			keys, next, err := it.c.rdb.Scan(it.ctx, it.cursor, it.match, it.count).Result()
			if err != nil {
				it.err = err
				return false
			}
			it.buf = keys
			it.cursor = next
			if next == 0 {
				it.done = true
			}
			continue
		}
		k := it.buf[0]
		it.buf = it.buf[1:]
		v, err := it.c.rdb.Get(it.ctx, k).Bytes()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // expired between SCAN and GET
			}
			it.err = err
			return false
		}
		it.k = k[len(it.c.prefix):]
		it.v = v
		return true
	}
}

func (it *iter) Key() string   { return it.k }
func (it *iter) Value() []byte { return it.v }
func (it *iter) Err() error    { return it.err }
func (it *iter) Close() error  { return nil }

// Ping implements cache.Cache.
func (c *Cache) Ping(ctx context.Context) error {
	if c.closed.Load() {
		return cache.ErrClosed
	}
	return c.rdb.Ping(ctx).Err()
}

// Close implements cache.Cache. Idempotent; does not close a client the
// caller may still be using elsewhere — it only marks this adapter closed.
func (c *Cache) Close() error {
	c.closed.Store(true)
	return nil
}

// Stats implements cache.Cache. Entries is the DB key count (best-effort).
func (c *Cache) Stats() cache.Stats {
	var entries int64
	if !c.closed.Load() {
		if n, err := c.rdb.DBSize(context.Background()).Result(); err == nil {
			entries = n
		}
	}
	return cache.Stats{Entries: entries}
}

var _ cache.Cache = (*Cache)(nil)
