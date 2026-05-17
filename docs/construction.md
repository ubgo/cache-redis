# Construction & options

### New

`func New(rdb redis.UniversalClient, opts ...Option) *Cache`

What it is: wraps any go-redis client (`*redis.Client`, `*redis.ClusterClient`,
`*redis.Ring`, …) as a `cache.Cache`. `Close` only marks the adapter closed —
it never closes the client you passed in (you may share it elsewhere).

Use cases:

- Back any code written against `cache.Cache` with a shared Redis.
- Use a `ClusterClient` for a sharded Redis without code changes.

```go
package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	rediscache "github.com/ubgo/cache-redis"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	c := rediscache.New(rdb)
	defer c.Close()

	ctx := context.Background()
	_ = c.Set(ctx, "k", []byte("v"), 0)
	v, _ := c.Get(ctx, "k")
	fmt.Println(string(v)) // v
}
```

### Cache

`type Cache struct { ... }`

What it is: the adapter. Its only mutable state is a `closed` atomic; all
concurrency safety is delegated to go-redis (concurrency-safe client) and Redis
(atomic commands), so it needs no locks of its own.

Use cases:

- Hold as `cache.Cache` for backend-agnostic code.
- Share one `*Cache` across all goroutines safely.

```go
var generic cache.Cache = rediscache.New(rdb)
```

### Option

`type Option func(*Cache)`

What it is: the functional-option type used by `New`.

```go
opts := []rediscache.Option{rediscache.WithPrefix("svc:billing")}
c := rediscache.New(rdb, opts...)
```

### WithPrefix

`func WithPrefix(p string) Option`

What it is: prepends `p` (a trailing `:` is appended if absent) to every key,
isolating this cache from other users of the same Redis DB. `Flush` is scoped
to the prefix (it `SCAN`s + `DEL`s only `prefix*` instead of `FLUSHDB`).
`Iterate` strips the prefix back off returned keys.

Use cases:

- Multi-tenant: two services share one Redis DB without colliding.
- Safe `Flush` — wipe only your service's keys, never the whole DB.
- Environment separation (`stg:` vs `prod:`) on a shared instance.

```go
c := rediscache.New(rdb, rediscache.WithPrefix("svc:billing"))
_ = c.Set(ctx, "user:42", []byte("v"), 0) // stored as svc:billing:user:42
_ = c.Flush(ctx)                          // deletes only svc:billing:* keys
```
