package connector

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Status represents the connection status of a connector.
type Status int

const (
	StatusDisconnected Status = iota
	StatusConnecting
	StatusConnected
	StatusError
)

// Config holds configuration for a connector instance.
type Config struct {
	Address     string
	Timeout     time.Duration
	MaxRetries  int
	RetryDelay  time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Timeout:    10 * time.Second,
		MaxRetries: 3,
		RetryDelay: 2 * time.Second,
	}
}

// Connector manages a connection to a remote service.
type Connector struct {
	mu      sync.RWMutex
	cfg     Config
	status  Status
	cancel  context.CancelFunc
}

// New creates a new Connector with the given configuration.
func New(cfg Config) (*Connector, error) {
	if cfg.Address == "" {
		return nil, errors.New("connector: address must not be empty")
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultConfig().Timeout
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	return &Connector{
		cfg:    cfg,
		status: StatusDisconnected,
	}, nil
}

// Connect initiates the connection, retrying up to MaxRetries times.
func (c *Connector) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.status == StatusConnected {
		c.mu.Unlock()
		return nil
	}
	c.status = StatusConnecting
	c.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt <= c.cfg.MaxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			c.setStatus(StatusError)
			return err
		}
		lastErr = c.dial(ctx)
		if lastErr == nil {
			c.setStatus(StatusConnected)
			return nil
		}
		if attempt < c.cfg.MaxRetries {
			select {
			case <-time.After(c.cfg.RetryDelay):
			case <-ctx.Done():
				c.setStatus(StatusError)
				return ctx.Err()
			}
		}
	}
	c.setStatus(StatusError)
	return lastErr
}

// dial performs the actual connection attempt (stub for extension).
func (c *Connector) dial(_ context.Context) error {
	// Real implementation would dial c.cfg.Address here.
	return nil
}

// Disconnect closes the current connection.
func (c *Connector) Disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}
	c.status = StatusDisconnected
}

// Status returns the current connection status.
func (c *Connector) Status() Status {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.status
}

func (c *Connector) setStatus(s Status) {
	c.mu.Lock()
	c.status = s
	c.mu.Unlock()
}
