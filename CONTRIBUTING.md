# Contributing to ubgo/cache-redis

Thanks for helping improve the Redis adapter for `github.com/ubgo/cache`.

## Local gate (must be green before every commit / PR)

```sh
gofmt -w .
go build ./...
go test -race -count=1 ./...
golangci-lint run ./...
```

Or via the Taskfile: `task check` (runs `fmt:check` + `vet` + `test:race`).

The CI gate is identical. A change is not done until all four commands report **0 failures and 0 lint issues**. `golangci-lint` runs `revive`, `staticcheck`, `govet`, `errcheck`, `gocritic`, `misspell`, `unconvert`, `ineffassign`, and `unused` (see `.golangci.yml`).

## Conformance contract

This adapter implements `cache.Cache` and **must keep passing the shared conformance suite** `github.com/ubgo/cache/cachetest`:

```go
func TestConformance(t *testing.T) {
	cachetest.Run(t, func(t *testing.T) cache.Cache {
		return rediscache.New(newMini(t))
	})
}
```

`cachetest.Run` is the executable definition of the contract. The non-negotiable invariants it enforces:

- `Get` returns `(nil, cache.ErrNotFound)` on miss or expiry — never `(nil, nil)`.
- `ttl <= 0` means "no expiry".
- `SetNX` returns `(true, nil)` **only** when it created the key.
- `Incr`/`Decr` are atomic; a missing key is treated as `0`.
- After `Close()`, every method returns `cache.ErrClosed`; `Close()` is idempotent.

If you change behaviour, the conformance suite (and the prefix-isolation / TTL / closed-state tests in `rediscache_test.go`) must still pass unchanged.

## Docker-free tests (`miniredis`)

There is no Redis service in CI. `rediscache_test.go` starts an in-process [`miniredis`](https://github.com/alicebob/miniredis) and runs a background ticker that calls `mr.FastForward(20ms)` every 20ms. This makes the conformance suite's real-time `time.Sleep` TTL assertions actually expire keys without a wall clock dependency. Do not remove the fast-forward goroutine — TTL tests will hang or flake without it.

Run the optional real-Redis smoke path locally with a running server only when validating protocol-level behaviour; it is never required for the gate.

## Local dependency (`replace`)

`github.com/ubgo/cache` is developed in a sibling repo and not yet tagged, so `go.mod` carries `replace github.com/ubgo/cache => ../cache`. **Do not edit `go.mod`, `go.sum`, `LICENSE`, `NOTICE`, or `.gitignore`** as part of a feature change. The `replace` directive is removed (and a real version pinned) only at release time.

Layout for local development:

```
ubgo/
  cache/          # the contract + cachetest suite
  cache-redis/    # this module (replace -> ../cache)
```

## Doc-comment style

- Every exported symbol has a doc comment that starts with its name (`revive` enforces this).
- Comments explain **why** and call out invariants / edge cases, not just what the code does (e.g. "PEXPIRE — plain EXPIRE truncates sub-second TTLs"). Keep the existing WHY-comments when refactoring.
- The `ctx` parameter is part of the `cache.Cache` contract; adapters that do not consult it still keep the named parameter. `.golangci.yml` already excludes the `unused-parameter: 'ctx'` revive warning — do not rename `ctx` to `_`.
- Keep `doc.go` accurate: it documents the package-level behaviour and is the godoc landing page.

## Pull requests

1. Keep the gate green (above).
2. Add or extend a test for any behaviour change (conformance first, then a targeted test).
3. Update `README.md` and `CHANGELOG.md` when public behaviour changes.
4. One logical change per PR.
