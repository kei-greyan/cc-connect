package connector

import "time"

// Option is a functional option for configuring a Connector.
type Option func(*Config)

// WithAddress sets the address for the connector.
func WithAddress(address string) Option {
	return func(c *Config) {
		c.Address = address
	}
}

// WithMaxRetries sets the maximum number of reconnection attempts.
// A value of 0 disables retries; negative values are treated as 0.
func WithMaxRetries(retries int) Option {
	return func(c *Config) {
		if retries < 0 {
			retries = 0
		}
		c.MaxRetries = retries
	}
}

// WithRetryInterval sets the duration to wait between reconnection attempts.
// Intervals shorter than 500ms are ignored to prevent aggressive retry loops.
// Note: upstream uses 100ms as the lower bound, but 500ms is more practical
// in production environments to avoid hammering a temporarily unavailable server.
func WithRetryInterval(interval time.Duration) Option {
	return func(c *Config) {
		if interval >= 500*time.Millisecond {
			c.RetryInterval = interval
		}
	}
}

// WithTimeout sets the connection timeout duration.
// Negative values are also rejected, consistent with zero handling.
func WithTimeout(timeout time.Duration) Option {
	return func(c *Config) {
		if timeout > 0 {
			c.Timeout = timeout
		}
	}
}

// WithTLS enables or disables TLS for the connection.
func WithTLS(enabled bool) Option {
	return func(c *Config) {
		c.TLSEnabled = enabled
	}
}

// applyOptions applies a list of Option functions to the given Config.
func applyOptions(cfg *Config, opts []Option) {
	for _, opt := range opts {
		opt(cfg)
	}
}
