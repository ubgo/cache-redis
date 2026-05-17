# Cache methods (`cache.Cache` over Redis)

Every method maps to a native Redis command. `redis.Nil` is normalised to
`cache.ErrNotFound`; all other Redis errors pass through unwrapped. `KEYS` is
**never** used — every keyspace walk is cursor-based `SCAN`. Snippets assume:

```go
ctx := context.Background()
rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
c := rediscache.New(rdb)
defer c.Close()
```

## Read

### Get

`Get(ctx, key)` → Redis `GET`. Miss → `cache.ErrNotFound`.

```go
v, err := c.Get(ctx, "user:42")
if errors.Is(err, cache.ErrNotFound) { /* miss */ }
```

### GetMulti

`GetMulti(ctx, keys)` → single `MGET`. Absent keys omitted from the map. Empty
input returns an empty map (no round trip).

```go
m, _ := c.GetMulti(ctx, []string{"a", "b"})
```

### Has

`Has(ctx, key)` → `EXISTS` (no value transfer).

```go
ok, _ := c.Has(ctx, "session:42")
```

### TTL

`TTL(ctx, key)` → `TTL`. Redis `-2` (no key) → `cache.ErrNotFound`; `-1`
(exists, no expiry) → `(0, nil)`; otherwise the remaining duration.

```go
d, err := c.TTL(ctx, "session:42")
// err==ErrNotFound → absent; d==0,err==nil → no expiry; d>0 → remaining
```

## Write

### Set

`Set(ctx, key, val, ttl)` → `SET` (with `EX`/`PX` when `ttl > 0`). A negative
`ttl` is clamped to 0 (no expiry).

```go
_ = c.Set(ctx, "k", []byte("v"), 5*time.Minute)
```

### SetMulti

`SetMulti(ctx, items)` → one **pipeline** of `SET`s (one round trip).

```go
_ = c.SetMulti(ctx, map[string]cache.Item{
	"a": {Value: []byte("1"), TTL: time.Minute},
	"b": {Value: []byte("2")},
})
```

### SetNX

`SetNX(ctx, key, val, ttl)` → `SET NX`. Returns `(true, nil)` only if created.

Use cases: distributed locks, write-once idempotency keys.

```go
ok, _ := c.SetNX(ctx, "lock:job", []byte("1"), 30*time.Second)
if ok { /* acquired the lock */ }
```

### Expire

`Expire(ctx, key, ttl)`. `ttl > 0` → `PEXPIRE` (**millisecond** precision —
plain `EXPIRE` would round sub-second TTLs up to 1s and break rate
limiters/short locks). `ttl <= 0` → `PERSIST` (remove expiry). Missing key →
`cache.ErrNotFound` (disambiguated via `EXISTS` for the persist case).

```go
_ = c.Expire(ctx, "rl:ip", 250*time.Millisecond) // sub-second, preserved
_ = c.Expire(ctx, "k", 0)                        // make permanent
```

### Touch

`Touch(ctx, key)` → `Expire(ctx, key, time.Hour)`.

```go
_ = c.Touch(ctx, "session:42")
```

## Counters

### Incr / Decr

`Incr` → `INCRBY`, `Decr` → `DECRBY`. Atomic server-side; a missing key starts
at 0 per Redis semantics. Values may go negative.

Use cases: rate limiting, view counters, inventory.

```go
n, _ := c.Incr(ctx, "rl:ip:1.2.3.4", 1)
_, _ = c.Decr(ctx, "stock:sku9", 1)
```

## Delete

### Del

`Del(ctx, keys...)` → `DEL`. Empty list is a no-op.

```go
_ = c.Del(ctx, "a", "b", "c")
```

### DeleteByPrefix

`DeleteByPrefix(ctx, prefix)` → cursor `SCAN` (batch 512) + batched `DEL`.
Never `KEYS`, so it is safe on multi-million-key databases.

```go
_ = c.DeleteByPrefix(ctx, "user:42:") // safe on huge keyspaces
```

### Flush

`Flush(ctx)`. With a `WithPrefix` set → `SCAN` + `DEL` of `prefix*` only.
Without a prefix → `FLUSHDB` (wipes the whole DB).

```go
// With WithPrefix: only your keys. Without: the entire DB — be careful.
_ = c.Flush(ctx)
```

## Iterate

### Iterate

`Iterate(ctx, cache.IterateOpts)` → cursor `SCAN` (default count 256, override
via `IterateOpts.Count`) yielding key/value lazily (one `GET` per key). A key
that expires between `SCAN` and `GET` is silently skipped (a correct race, not
an error). Returned keys have the prefix stripped. Always `Close()`; check
`Err()` after the loop.

```go
it := c.Iterate(ctx, cache.IterateOpts{Prefix: "user:", Count: 512})
defer it.Close()
for it.Next() {
	fmt.Println(it.Key(), string(it.Value()))
}
if err := it.Err(); err != nil { log.Fatal(err) }
```

## Lifecycle

### Ping

`Ping(ctx)` → Redis `PING`. `cache.ErrClosed` after `Close`.

```go
if err := c.Ping(ctx); err != nil { log.Fatal("redis down:", err) }
```

### Close

`Close()` — idempotent; marks the adapter closed so further ops return
`cache.ErrClosed`. **Does not** close the go-redis client (the caller may
share it). Close the client yourself when you own it.

```go
defer c.Close()      // adapter
defer rdb.Close()    // the client you created
```

### Stats

`Stats()` → `cache.Stats{Entries: DBSIZE}` (best-effort; other fields zero —
Redis tracks hit/miss server-side, not per-adapter).

```go
fmt.Println("keys in db:", c.Stats().Entries)
```
