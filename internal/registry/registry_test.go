package registry

import (
	"context"
	"testing"
)

func TestDispatch_UnregisteredName(t *testing.T) {
	r := New()

	err := r.Dispatch(context.Background(), "does-not-exist", nil)
	if err == nil {
		t.Fatal("expected an error dispatching to an unregistered name, got nil")
	}
}

func TestDispatch_CallsRegisteredHandler(t *testing.T) {
	r := New()
	var gotPayload []byte

	if err := r.Register("echo", func(_ context.Context, payload []byte) error {
		gotPayload = payload
		return nil
	}); err != nil {
		t.Fatalf("Register: %v", err)
	}

	want := []byte("hello")
	if err := r.Dispatch(context.Background(), "echo", want); err != nil {
		t.Fatalf("Dispatch: %v", err)
	}
	if string(gotPayload) != string(want) {
		t.Fatalf("handler received payload %q, want %q", gotPayload, want)
	}
}

func TestRegister_EmptyName(t *testing.T) {
	r := New()
	err := r.Register("", func(context.Context, []byte) error { return nil })
	if err == nil {
		t.Fatal("expected an error registering an empty name, got nil")
	}
}

func TestRegister_NilHandler(t *testing.T) {
	r := New()
	err := r.Register("task", nil)
	if err == nil {
		t.Fatal("expected an error registering a nil handler, got nil")
	}
}

func TestRegister_Duplicate(t *testing.T) {
	r := New()
	handler := func(context.Context, []byte) error { return nil }

	if err := r.Register("task", handler); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if err := r.Register("task", handler); err == nil {
		t.Fatal("expected an error re-registering the same name, got nil")
	}
}
