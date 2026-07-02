# Roadmap

This roadmap tracks datadi's development in phases. The current focus is on internal correctness and stability — hardening the engine before designing any public API or adding external-facing features.

## Phase 1 - Initial scaffolding

- [x] `Task` struct with status tracking and retry fields
- [x] Channel-based in-memory `Queue`
- [x] `Worker` with blocking `Start()` loop
- [x] Multiple concurrent workers via goroutines + `sync.WaitGroup`
- [x] Graceful shutdown via `context.Context` + OS signal handling
- [x] Exponential backoff (500ms → 1s → 2s → ...) confirmed working
- [x] Panic recovery in `process()`
- [x] `DeadLetterQueue` — mutex-protected storage, `List()` returns a defensive copy, `Requeue()` for manual retry

## Phase 2 - Internal stability (current focus)

Priority order:

1. [ ] **Finish registry refactor** - `HandlerFunc` signature decoupled to `func(ctx context.Context, payload []byte) error`, so handlers never depend on datadi's internal `Task` type. `Dispatch` takes `(name string, payload []byte)` directly.
2. [ ] **Distinguish error types** - retryable vs. permanent vs. cancelled. Introduce a `PermanentError` type so handlers can signal "don't retry this" (e.g. invalid payload) instead of wasting retry attempts. Context cancellation during shutdown should not count as a failed attempt.
3. [ ] **Make shutdown-time task loss visible** - a task mid-retry-sleep when `ctx.Done()` fires currently vanishes silently. At minimum this needs to be logged; ideally it's dead-lettered instead of dropped.
4. [ ] **Tests** - backoff math, dispatch-to-unregistered-name, dead-letter transitions. Doesn't need to be exhaustive yet, just enough to catch regressions as the engine keeps changing.

## Phase 4 - Registry & task abstraction

- [ ] Registry fully generic, zero awareness of task semantics
- [ ] Example/demo handlers live only in `cmd/server/main.go`, never in `internal/`
- [ ] Typed payload pattern documented (consumer defines and marshals/unmarshals their own payload structs)
- [ ] Per-task timeout via `context.WithTimeout` at dispatch time

## Phase 5 - Persistence (not started)

- [ ] Tasks and their retry/status state survive a process restart
- [ ] Likely SQLite or file-backed store to start (avoids standing up external infra for a learning project)
- [ ] Migration strategy for schema changes
- [ ] Requeue in-flight/pending tasks on startup

## Phase 6 - Distribution (not started)

- [ ] Multiple worker processes/nodes pulling from a shared queue
- [ ] Task visibility/leasing so two nodes don't process the same task
- [ ] Node health / heartbeat
- [ ] Leader election or equivalent coordination, if needed

## Phase 7 - Public API & external interface (not started)

Deliberately deferred until Phases 3–4 are solid. Once the internals are
stable:

- [ ] Public `datadi` package consumers import without touching `internal/`
- [ ] `datadi.Register(name, handler)`, `datadi.Enqueue(...)` style surface
- [ ] Possibly an HTTP or gRPC interface for cross-process enqueue/inspect (e.g. `GET /deadletters`, `POST /tasks`)
