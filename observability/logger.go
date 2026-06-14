// Package observability provides observability utilities for cachex.
package observability

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

// Level represents the log level.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// CacheLogger provides structured logging for cache operations.
type CacheLogger struct {
	mu       sync.RWMutex
	level    Level
	format   string
	output   *os.File
	fields   map[string]interface{}
	callback func(Level, string, map[string]interface{})
}

// LoggerOption is a functional option for CacheLogger.
type LoggerOption func(*CacheLogger)

// WithLevel sets the log level.
func WithLevel(level Level) LoggerOption {
	return func(l *CacheLogger) {
		l.level = level
	}
}

// WithFormat sets the log format (json, text).
func WithFormat(format string) LoggerOption {
	return func(l *CacheLogger) {
		l.format = format
	}
}

// WithOutput sets the log output.
func WithOutput(output *os.File) LoggerOption {
	return func(l *CacheLogger) {
		l.output = output
	}
}

// WithCallback sets a callback for log messages.
func WithCallback(cb func(Level, string, map[string]interface{})) LoggerOption {
	return func(l *CacheLogger) {
		l.callback = cb
	}
}

// WithFields sets default fields.
func WithFields(fields map[string]interface{}) LoggerOption {
	return func(l *CacheLogger) {
		l.fields = fields
	}
}

// NewLogger creates a new CacheLogger.
func NewLogger(opts ...LoggerOption) *CacheLogger {
	l := &CacheLogger{
		level:  LevelInfo,
		format: "json",
		output: os.Stdout,
		fields: make(map[string]interface{}),
	}

	for _, opt := range opts {
		opt(l)
	}

	return l
}

// SetLevel sets the log level.
func (l *CacheLogger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// SetFormat sets the log format.
func (l *CacheLogger) SetFormat(format string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.format = format
}

// Debug logs a debug message.
func (l *CacheLogger) Debug(ctx context.Context, msg string, fields ...map[string]interface{}) {
	l.log(ctx, LevelDebug, msg, fields...)
}

// Info logs an info message.
func (l *CacheLogger) Info(ctx context.Context, msg string, fields ...map[string]interface{}) {
	l.log(ctx, LevelInfo, msg, fields...)
}

// Warn logs a warning message.
func (l *CacheLogger) Warn(ctx context.Context, msg string, fields ...map[string]interface{}) {
	l.log(ctx, LevelWarn, msg, fields...)
}

// Error logs an error message.
func (l *CacheLogger) Error(ctx context.Context, msg string, fields ...map[string]interface{}) {
	l.log(ctx, LevelError, msg, fields...)
}

// log logs a message at the specified level.
func (l *CacheLogger) log(ctx context.Context, level Level, msg string, fields ...map[string]interface{}) {
	l.mu.RLock()
	if level < l.level {
		l.mu.RUnlock()
		return
	}
	format := l.format
	output := l.output
	callback := l.callback
	defaultFields := l.fields
	l.mu.RUnlock()

	mergedFields := make(map[string]interface{})
	for k, v := range defaultFields {
		mergedFields[k] = v
	}
	for _, f := range fields {
		for k, v := range f {
			mergedFields[k] = v
		}
	}

	mergedFields["timestamp"] = time.Now().UTC().Format(time.RFC3339)
	mergedFields["level"] = level.String()
	mergedFields["message"] = msg

	var logLine string
	if format == "json" {
		data, _ := json.Marshal(mergedFields)
		logLine = string(data)
	} else {
		logLine = formatTextLog(level, msg, mergedFields)
	}

	if callback != nil {
		callback(level, logLine, mergedFields)
	}

	fmt.Fprintln(output, logLine)
}

func formatTextLog(level Level, msg string, fields map[string]interface{}) string {
	var parts []string
	parts = append(parts, fmt.Sprintf("[%s]", level.String()))
	parts = append(parts, msg)

	for k, v := range fields {
		if k != "timestamp" && k != "level" && k != "message" {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
	}

	result := parts[0]
	for i := 1; i < len(parts); i++ {
		result += " " + parts[i]
	}
	return result
}

// NewCacheLogger creates a logger with cache-related fields.
func NewCacheLogger(backend string) *CacheLogger {
	return NewLogger(WithFields(map[string]interface{}{
		"backend": backend,
		"service": "cachex",
	}))
}

// LoggingObserver implements cachex.Observer with logging.
type LoggingObserver struct {
	logger *CacheLogger
}

// NewLoggingObserver creates a new logging observer.
func NewLoggingObserver(logger *CacheLogger) *LoggingObserver {
	return &LoggingObserver{logger: logger}
}

// OnOperation implements Observer interface.
func (o *LoggingObserver) OnOperation(ctx context.Context, operation string, backend string, err error, duration time.Duration) {
	fields := map[string]interface{}{
		"operation":   operation,
		"backend":     backend,
		"duration_ms": duration.Milliseconds(),
	}

	if err != nil {
		o.logger.Error(ctx, "cache operation failed", fields)
		return
	}

	o.logger.Debug(ctx, "cache operation completed", fields)
}

// OnError implements Observer interface.
func (o *LoggingObserver) OnError(ctx context.Context, operation string, backend string, err error) {
	o.logger.Error(ctx, "cache error", map[string]interface{}{
		"operation": operation,
		"backend":   backend,
		"error":     err.Error(),
	})
}
