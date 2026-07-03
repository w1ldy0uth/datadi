package registry

import (
	"context"
	"fmt"
	"sync"
)

type HandlerFunc func(ctx context.Context, payload []byte) error

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]HandlerFunc
}

func New() *Registry {
	return &Registry{handlers: make(map[string]HandlerFunc)}
}

func (r *Registry) Register(name string, handler HandlerFunc) error {
	if name == "" {
		return fmt.Errorf("Registry: task name cannot be empty")
	}
	if handler == nil {
		return fmt.Errorf("Registry: handler for %q cannot be nil", name)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[name]; exists {
		return fmt.Errorf("Registry: handler already registered for %q", name)
	}
	r.handlers[name] = handler
	return nil
}

func (r *Registry) Dispatch(ctx context.Context, name string, payload []byte) error {
	r.mu.RLock()
	handler, ok := r.handlers[name]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("Registry: no handler registered for %q", name)
	}
	return handler(ctx, payload)
}
