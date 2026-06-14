// Package pubsub provides distributed cache invalidation via Pub/Sub.
//
// It offers two main abstractions:
//   - Publisher: publishes cache invalidation events to channels
//   - Subscriber: subscribes to cache invalidation events and invalidates local cache
//
// This enables distributed cache invalidation across multiple nodes.
package pubsub

import (
	"context"
)

// Publisher defines the interface for publishing messages to a channel.
type Publisher interface {
	// Publish publishes a message to the specified channel.
	// The key parameter identifies the cache key being invalidated.
	Publish(ctx context.Context, channel, key string) error

	// Close closes the publisher and releases resources.
	Close() error
}

// Subscriber defines the interface for subscribing to messages on a channel.
type Subscriber interface {
	// Subscribe subscribes to the specified channel and handles messages
	// with the given handler function.
	// The handler is called with the key from each received message.
	Subscribe(ctx context.Context, channel string, handler func(key string)) error

	// Close closes the subscriber and releases resources.
	Close() error
}

// PubSub combines Publisher and Subscriber interfaces.
type PubSub interface {
	Publisher
	Subscriber
}

// InMemoryPubSub provides an in-process Pub/Sub implementation using Go channels.
type InMemoryPubSub struct {
	subscriptions map[string][]chan string
	mu            chan struct{} // Mutex for map access
	closeChan     chan struct{}
	closed        bool
}

// NewInMemoryPubSub creates a new in-memory Pub/Sub instance.
func NewInMemoryPubSub() *InMemoryPubSub {
	return &InMemoryPubSub{
		subscriptions: make(map[string][]chan string),
		mu:            make(chan struct{}, 1),
		closeChan:     make(chan struct{}),
	}
}

// Publish publishes a message to the specified channel.
func (p *InMemoryPubSub) Publish(ctx context.Context, channel, key string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closeChan:
		return ErrClosed
	default:
	}

	p.mu <- struct{}{}
	subs, ok := p.subscriptions[channel]
	<-p.mu

	if !ok || len(subs) == 0 {
		return nil // No subscribers, no-op
	}

	for _, ch := range subs {
		select {
		case ch <- key:
		default:
			// Non-blocking send to each subscriber
		}
	}

	return nil
}

// Subscribe subscribes to the specified channel.
func (p *InMemoryPubSub) Subscribe(ctx context.Context, channel string, handler func(key string)) error {
	p.mu <- struct{}{}
	defer func() { <-p.mu }()

	if p.closed {
		return ErrClosed
	}

	ch := make(chan string)
	p.subscriptions[channel] = append(p.subscriptions[channel], ch)

	go func() {
		for {
			select {
			case key, ok := <-ch:
				if !ok {
					return
				}
				handler(key)
			case <-ctx.Done():
				p.mu <- struct{}{}
				p.removeSubscription(channel, ch)
				<-p.mu
				return
			case <-p.closeChan:
				p.mu <- struct{}{}
				p.removeSubscription(channel, ch)
				<-p.mu
				return
			}
		}
	}()

	return nil
}

// removeSubscription removes a channel from the subscriptions list.
func (p *InMemoryPubSub) removeSubscription(channel string, ch chan string) {
	subs, ok := p.subscriptions[channel]
	if !ok {
		return
	}
	for i, c := range subs {
		if c == ch {
			p.subscriptions[channel] = append(subs[:i], subs[i+1:]...)
			return
		}
	}
}

// Close closes the Pub/Sub and all subscriptions.
func (p *InMemoryPubSub) Close() error {
	p.mu <- struct{}{}
	defer func() { <-p.mu }()

	if p.closed {
		return nil
	}
	p.closed = true
	close(p.closeChan)

	for _, subs := range p.subscriptions {
		for _, ch := range subs {
			close(ch)
		}
	}
	p.subscriptions = make(map[string][]chan string)

	return nil
}

// ErrClosed is returned when operations are attempted on a closed Pub/Sub.
var ErrClosed = &PubSubError{Message: "pub/sub closed"}

// PubSubError represents a Pub/Sub specific error.
type PubSubError struct {
	Message string
	Cause   error
}

func (e *PubSubError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *PubSubError) Unwrap() error {
	return e.Cause
}
