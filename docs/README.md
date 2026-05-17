# cache-redis — feature cookbook

Exhaustive, example-driven reference for every exported identifier in
`github.com/ubgo/cache-redis` (package `rediscache`).

Import path:

```go
import rediscache "github.com/ubgo/cache-redis"
```

`rediscache.Cache` implements [`cache.Cache`](https://github.com/ubgo/cache)
and passes the shared `cachetest.Run` conformance suite. `rediscache.Invalidation`
implements `cache.Invalidation` over Redis Pub/Sub.

## Pages

- [Construction & options](construction.md) — `New`, `WithPrefix`, the `Cache` / `Option` types.
- [Cache methods](cache-methods.md) — every `cache.Cache` method and the exact Redis command it maps to.
- [Invalidation bus](invalidation.md) — `NewInvalidation`, `Invalidation`, `Publish`, `Subscribe`.

## Capability matrix

| Exported symbol | Kind | Redis behavior | Page |
|---|---|---|---|
| `New` | constructor | wraps a `redis.UniversalClient` | [Construction](construction.md#new) |
| `Cache` | type | adapter, lock-free (Redis is atomic) | [Construction](construction.md#cache) |
| `Option` | type | functional option | [Construction](construction.md#option) |
| `WithPrefix` | option | key namespacing; `Flush` scoped to prefix | [Construction](construction.md#withprefix) |
| `Get` | method | `GET` | [Cache methods](cache-methods.md#get) |
| `GetMulti` | method | `MGET` | [Cache methods](cache-methods.md#getmulti) |
| `Has` | method | `EXISTS` | [Cache methods](cache-methods.md#has) |
| `TTL` | method | `TTL` (-2 → ErrNotFound, -1 → 0) | [Cache methods](cache-methods.md#ttl) |
| `Set` | method | `SET [EX]` | [Cache methods](cache-methods.md#set) |
| `SetMulti` | method | pipelined `SET` | [Cache methods](cache-methods.md#setmulti) |
| `SetNX` | method | `SET NX` | [Cache methods](cache-methods.md#setnx) |
| `Expire` | method | `PEXPIRE` / `PERSIST` | [Cache methods](cache-methods.md#expire) |
| `Touch` | method | `PEXPIRE` 1h | [Cache methods](cache-methods.md#touch) |
| `Incr` / `Decr` | method | `INCRBY` / `DECRBY` | [Cache methods](cache-methods.md#counters) |
| `Del` | method | `DEL` | [Cache methods](cache-methods.md#del) |
| `DeleteByPrefix` | method | `SCAN` + batched `DEL` | [Cache methods](cache-methods.md#deletebyprefix) |
| `Flush` | method | prefix `SCAN`+`DEL`, else `FLUSHDB` | [Cache methods](cache-methods.md#flush) |
| `Iterate` | method | cursor `SCAN` + lazy `GET` | [Cache methods](cache-methods.md#iterate) |
| `Ping` | method | `PING` | [Cache methods](cache-methods.md#ping) |
| `Close` | method | marks adapter closed only | [Cache methods](cache-methods.md#close) |
| `Stats` | method | `DBSIZE` → `Entries` | [Cache methods](cache-methods.md#stats) |
| `Invalidation` | type | `cache.Invalidation` over Pub/Sub | [Invalidation](invalidation.md#invalidation) |
| `NewInvalidation` | constructor | builds the bus | [Invalidation](invalidation.md#newinvalidation) |
| `(*Invalidation).Publish` | method | `PUBLISH` per key | [Invalidation](invalidation.md#publish) |
| `(*Invalidation).Subscribe` | method | `SUBSCRIBE` loop | [Invalidation](invalidation.md#subscribe) |

There are **no `ErrUnsupported` cases** — Redis serves the entire contract.
