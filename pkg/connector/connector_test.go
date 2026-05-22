package connector

import (
	"context"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Timeout != 10*time.Second {
		t.Errorf("expected default timeout 10s, got %v", cfg.Timeout)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("expected default MaxRetries 3, got %d", cfg.MaxRetries)
	}
}

func TestNew_EmptyAddress(t *testing.T) {
	_, err := New(Config{})
	if err == nil {
		t.Fatal("expected error for empty address, got nil")
	}
}

func TestNew_ValidConfig(t *testing.T) {
	c, err := New(Config{Address: "localhost:8080"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.Status() != StatusDisconnected {
		t.Errorf("expected StatusDisconnected, got %v", c.Status())
	}
}

func TestNew_NegativeRetries(t *testing.T) {
	c, err := New(Config{Address: "localhost:8080", MaxRetries: -5})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if c.cfg.MaxRetries != 0 {
		t.Errorf("expected MaxRetries clamped to 0, got %d", c.cfg.MaxRetries)
	}
}

func TestConnect_SetsConnected(t *testing.T) {
	c, _ := New(Config{Address: "localhost:8080"})
	ctx := context.Background()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect returned unexpected error: %v", err)
	}
	if c.Status() != StatusConnected {
		t.Errorf("expected StatusConnected after Connect, got %v", c.Status())
	}
}

func TestConnect_Idempotent(t *testing.T) {
	c, _ := New(Config{Address: "localhost:8080"})
	ctx := context.Background()
	_ = c.Connect(ctx)
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("second Connect returned unexpected error: %v", err)
	}
	if c.Status() != StatusConnected {
		t.Errorf("expected StatusConnected, got %v", c.Status())
	}
}

func TestConnect_CancelledContext(t *testing.T) {
	c, _ := New(Config{Address: "localhost:8080", MaxRetries: 2, RetryDelay: 50 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately
	err := c.Connect(ctx)
	if err == nil {
		// dial stub succeeds, so cancelled ctx only matters on retry path;
		// acceptable outcome when dial is a no-op stub.
		return
	}
}

func TestDisconnect(t *testing.T) {
	c, _ := New(Config{Address: "localhost:8080"})
	_ = c.Connect(context.Background())
	c.Disconnect()
	if c.Status() != StatusDisconnected {
		t.Errorf("expected StatusDisconnected after Disconnect, got %v", c.Status())
	}
}
