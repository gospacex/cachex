// Package scan provides iterator-based key scanning to prevent OOM on large datasets.
package scan

import "context"

// Scanner defines the interface for iterating over cache keys in batches.
// This prevents loading all keys into memory at once, which is critical
// for large datasets.
type Scanner interface {
	// Scan returns a channel that yields batches of keys matching the pattern.
	// The channel is closed when all keys have been scanned or when ctx is cancelled.
	// batchSize controls the number of keys returned per iteration (backend-dependent).
	//
	// Example:
	//	ch := scanner.Scan(ctx, "user:*", 100)
	//	for keys := range ch {
	//		for _, key := range keys {
	//			// process key
	//		}
	//	}
	Scan(ctx context.Context, pattern string, batchSize int) <-chan []string

	// ScanAll collects all keys matching the pattern into a slice.
	// This is a convenience method for cases where all keys are needed.
	// For large datasets, prefer Scan() to avoid memory issues.
	ScanAll(ctx context.Context, pattern string) ([]string, error)
}
