// Package metrics provides observability metrics for cachex.
package metrics

import (
	"context"
	"time"
)

// MetricsCollector defines the interface for collecting cache metrics.
type MetricsCollector interface {
	// RecordGet records a GET operation.
	// hit indicates whether the key was found (true) or missing (false).
	// latency is the time taken for the operation.
	RecordGet(ctx context.Context, hit bool, latency time.Duration)

	// RecordSet records a SET operation.
	// latency is the time taken for the operation.
	RecordSet(ctx context.Context, latency time.Duration)

	// RecordDelete records a DELETE operation.
	// keysDeleted is the number of keys that were deleted.
	// latency is the time taken for the operation.
	RecordDelete(ctx context.Context, keysDeleted int64, latency time.Duration)

	// RecordError records an error that occurred during an operation.
	// operation is the name of the operation (get, set, delete, etc.).
	// err is the error that occurred.
	RecordError(ctx context.Context, operation string, err error)
}
