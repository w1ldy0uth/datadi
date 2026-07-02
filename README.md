# datadi

> From french: file d'attente de tâches distribuées - distributed task queue

datadi is a distributed task queue written in Go, built as a self-directed learning project. It's inspired by systems like RabbitMQ, Celery, and Redis task queues.

## Getting started

```bash
go run ./cmd/server
```

Runs a demo server with a few worker goroutines processing simulated tasks, including intermittent simulated failures to exercise the retry path. Ctrl+C triggers graceful shutdown.

Run the race detector regularly during development:

```bash
go run -race ./cmd/server
```

## Module

```go
github.com/w1ldy0uth/datadi
```

## Status

This is an actively developing project. See [ROADMAP.md](./ROADMAP.md) for what's done, what's in progress, and what's next.
