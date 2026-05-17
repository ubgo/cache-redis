// doc.go — canonical package documentation (package rediscache, github.com/ubgo/cache-redis).
//
// Package role: this file is the authoritative overview for the ubgo/cache
// Redis adapter; start here before reading rediscache.go (the adapter) or
// invalidation.go (the Pub/Sub bus).
//
// This file: holds ONLY the package doc comment below — no code. It
// enumerates the design invariants (PEXPIRE not EXPIRE, SCAN not KEYS,
// prefix-scoped Flush, Close marks-closed-only, errors verbatim except
// redis.Nil -> cache.ErrNotFound) that the other files implement.
//
// AI-context: the // Package … block below is the godoc package doc; do not
// duplicate it elsewhere (revive flags duplicate package comments). This
// header is separated from it by a blank line so it stays a file header.

// Package rediscache is the Redis 6+ adapter for github.com/ubgo/cache,
// backed by github.com/redis/go-redis/v9.
//
// It implements cache.Cache and passes the shared cachetest.Run conformance
// suite, so it is a drop-in for any code written against the cache contract.
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	c := rediscache.New(rdb)
//	defer c.Close()
//
// Native Redis features are used directly: TTL via SET EX/PX, SetNX via
// SET NX, Incr/Decr via INCRBY/DECRBY, GetMulti via MGET, SetMulti via a
// pipeline. DeleteByPrefix and Iterate use SCAN (never KEYS) so they are safe
// on large keyspaces. An optional key prefix isolates this cache from other
// users of the same Redis database, and Flush is scoped to that prefix.
//
// Design invariants worth knowing before editing this package:
//
//   - Expire uses PEXPIRE (millisecond precision). Plain EXPIRE only accepts
//     whole seconds and would silently round sub-second TTLs up to 1s, which
//     breaks rate-limiter / short-lived-lock use cases.
//   - KEYS is never used. It blocks the single-threaded Redis command loop on
//     large databases; every keyspace walk is cursor-based SCAN.
//   - Close only marks this adapter closed; it never closes the go-redis
//     client, because the caller may share that client with other code.
//   - Errors from Redis are returned verbatim (unwrapped) except redis.Nil,
//     which is normalised to cache.ErrNotFound so callers stay backend-agnostic.
//
// Invalidation (see invalidation.go) layers a Redis Pub/Sub bus on top so a
// tiered cache can keep per-pod L1 copies coherent across processes.
package rediscache
