# Roadmap

This roadmap tracks datadi's development in phases. The current focus is on internal correctness and stability — hardening the engine before designing any public API or adding external-facing features.

## Phase 1 - Initial scaffolding

- [x] `Task` struct with status tracking and retry fields
- [x] Channel-based in-memory `Queue`
- [x] `Worker` with blocking `Start()` loop
- [x] Multiple concurrent workers via goroutines + `sync.WaitGroup`
- [x] Graceful shutdown via `context.Context` + OS signal handling
- [x] Exponential backoff (500ms -> 1s -> 2s -> ...) confirmed working
- [x] Panic recovery in `process()`
- [x] `DeadLetterQueue` — mutex-protected storage, `List()` returns a defensive copy, `Requeue()` for manual retry

## Phase 2 - Internal stability (current focus)

Priority order:

1. [x] **Finish registry refactor** - `HandlerFunc` signature decoupled to `func(ctx context.Context, payload []byte) error`, so handlers never depend on datadi's internal `Task` type. `Dispatch` takes `(name string, payload []byte)` directly.
2. [x] **Distinguish error types** - retryable vs. permanent vs. cancelled. Introduce a `PermanentError` type so handlers can signal "don't retry this" (e.g. invalid payload) instead of wasting retry attempts. Context cancellation during shutdown should not count as a failed attempt.
3. [x] **Make shutdown-time task loss visible** - a task mid-retry-sleep when `ctx.Done()` fires currently vanishes silently. At minimum this needs to be logged; ideally it's dead-lettered instead of dropped.
4. [x] **Tests** - backoff math, dispatch-to-unregistered-name, dead-letter transitions. Covers registry dispatch/registration, dead-letter queue add/list/requeue, and worker process/backoff/shutdown behavior via fakes.

## Phase 3 - Registry & task abstraction

- [x] Registry fully generic, zero awareness of task semantics
- [x] Example/demo handlers live only in `cmd/server/main.go`, never in `internal/`
- [x] Typed payload pattern documented (consumer defines and marshals/unmarshals their own payload structs) — see `HandlerFunc` doc comment in `internal/registry/registry.go` and `demoPayload` in `cmd/server/main.go`
- [x] Per-task timeout via `context.WithTimeout` at dispatch time — `task.Task.Timeout`, applied in `Worker.process`; expiry is treated as a normal retryable failure, distinct from shutdown cancellation

## Phase 4 - Persistence

- [x] Tasks and their retry/status state survive a process restart — `internal/store.Store` (SQLite via `modernc.org/sqlite`, pure Go/no cgo) upserts on every state transition (`Worker.save`/`saveDeadLetter`, decoupled from the shutdown context so writes still land during cancellation)
- [x] SQLite store, avoids standing up external infra for a learning project
- [x] Migration strategy for schema changes — embedded, numbered `.sql` files in `internal/store/migrations/`, tracked via a `schema_migrations` table, applied forward-only on `Open`
- [x] Requeue in-flight/pending tasks on startup — `Store.LoadPendingAndRunning` (running tasks come back as pending, since their in-flight attempt was lost) and `Store.LoadDeadLetters` restore state in `cmd/server/main.go` before workers start

Known gaps (found in review, low severity, not yet fixed):

- `main.go`'s two startup-recovery calls (`LoadPendingAndRunning`, `LoadDeadLetters`) use the cancelable shutdown `ctx` and `log.Fatalf` on error — a SIGINT landing in that narrow startup window causes an abrupt exit instead of the graceful shutdown path. Should use `context.Background()` like the rest of the persistence calls.
- `queue.DeadLetterQueue.Requeue` has no `Persister` hook, so a manually requeued task's on-disk row keeps its dead-lettered state; if the process restarts before the requeue finishes reprocessing, `Store.LoadDeadLetters` resurrects it a second time. Currently latent — `Requeue` has no production caller yet — but needs a fix before Phase 6 exposes it over an API. `Store.Save`'s upsert also never clears `dead_letter_reason`/`dead_letter_at` on a task that succeeds after being revived, for the same reason.

## Phase 5 - Distribution (not started)

- [ ] Multiple worker processes/nodes pulling from a shared queue
- [ ] Task visibility/leasing so two nodes don't process the same task
- [ ] Node health / heartbeat
- [ ] Leader election or equivalent coordination, if needed

## Phase 6 - Public API & external interface (not started)

Deliberately deferred until Phases 3–4 are solid. Once the internals are
stable:

- [ ] Public `datadi` package consumers import without touching `internal/`
- [ ] `datadi.Register(name, handler)`, `datadi.Enqueue(...)` style surface
- [ ] Possibly an HTTP or gRPC interface for cross-process enqueue/inspect (e.g. `GET /deadletters`, `POST /tasks`)
