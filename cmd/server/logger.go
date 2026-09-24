package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"
)

// LoggerAdapter implements the Info/Error logger interface used across the
// codebase on top of log/slog, so key/value pairs are kept as structured
// fields instead of being dropped.
type LoggerAdapter struct {
	l *slog.Logger
}

// newLogger builds a logger writing JSON (default) or text to w.
func newLogger(w io.Writer, level, format string) *LoggerAdapter {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if strings.ToLower(format) == "text" {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	return &LoggerAdapter{l: slog.New(h)}
}

// Info logs at info level.
func (l *LoggerAdapter) Info(msg string, keysAndValues ...interface{}) {
	l.l.Info(msg, keysAndValues...)
}

// Warn logs at warn level.
func (l *LoggerAdapter) Warn(msg string, keysAndValues ...interface{}) {
	l.l.Warn(msg, keysAndValues...)
}

// Error logs at error level.
func (l *LoggerAdapter) Error(msg string, keysAndValues ...interface{}) {
	l.l.Error(msg, keysAndValues...)
}

// Func adapts the logger to the func(msg, kv...) signature some handlers use.
func (l *LoggerAdapter) Func() func(msg string, kv ...interface{}) {
	return func(msg string, kv ...interface{}) { l.Info(msg, kv...) }
}

// redisLogger routes go-redis internal messages (dial retries, pool errors)
// into the structured log instead of raw stderr, and rate-limits them so an
// outage does not flood the logs.
type redisLogger struct {
	l    *LoggerAdapter
	last atomic.Int64 // unix nanos of the last emitted message
}

func (r *redisLogger) Printf(_ context.Context, format string, v ...interface{}) {
	now := time.Now().UnixNano()
	if prev := r.last.Load(); now-prev < int64(10*time.Second) || !r.last.CompareAndSwap(prev, now) {
		return
	}
	r.l.Warn("redis client", "detail", fmt.Sprintf(format, v...))
}
