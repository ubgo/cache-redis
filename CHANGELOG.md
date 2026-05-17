# Changelog

All notable changes to `github.com/ubgo/cache-redis` are documented here.
Format follows Keep a Changelog; the project follows SemVer (pre-GA in `v0.x`).

## [Unreleased]

### Added

- Redis 6+ adapter (go-redis/v9) implementing `cache.Cache`.
- Native ops: `SET EX/PX`, `SET NX`, `INCRBY`/`DECRBY`, `MGET`, pipelined
  `SetMulti`; `PEXPIRE` for millisecond-precision `Expire`.
- `SCAN`-based `DeleteByPrefix` and `Iterate` (never `KEYS`).
- `WithPrefix` key isolation with prefix-scoped `Flush`.
- Idempotent `Close` (marks adapter closed; does not close a shared client).
- Passes the shared `github.com/ubgo/cache/cachetest` suite under `-race`,
  in-process via miniredis (no Docker).
- `NewInvalidation`: Redis Pub/Sub implementation of `cache.Invalidation`
  for cross-pod L1 invalidation (miniredis-tested).

[Unreleased]: https://github.com/ubgo/cache-redis/commits/main
