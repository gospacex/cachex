// Package pubsub provides distributed cache invalidation via Pub/Sub.
package pubsub

import (
	"context"
	"fmt"

	"github.com/gospacex/cachex"
	"github.com/redis/go-redis/v9"
)

// RedisPublisher implements Publisher using Redis Pub/Sub.
type RedisPublisher struct {
	client *redis.Client
}

// NewRedisPublisher creates a new Redis publisher.
func NewRedisPublisher(client *redis.Client) *RedisPublisher {
	return &RedisPublisher{client: client}
}

// Publish publishes a message to the specified Redis channel.
func (p *RedisPublisher) Publish(ctx context.Context, channel, key string) error {
	if p.client == nil {
		return fmt.Errorf("redis client is nil")
	}
	return p.client.Publish(ctx, channel, key).Err()
}

// Close closes the publisher (no-op for Redis).
func (p *RedisPublisher) Close() error {
	return nil
}

// RedisSubscriber implements Subscriber using Redis Pub/Sub.
type RedisSubscriber struct {
	client *redis.Client
	sub    *redis.PubSub
}

// NewRedisSubscriber creates a new Redis subscriber.
func NewRedisSubscriber(client *redis.Client) *RedisSubscriber {
	return &RedisSubscriber{client: client}
}

// Subscribe subscribes to the specified Redis channel.
func (s *RedisSubscriber) Subscribe(ctx context.Context, channel string, handler func(key string)) error {
	if s.client == nil {
		return fmt.Errorf("redis client is nil")
	}

	s.sub = s.client.Subscribe(ctx, channel)
	// Give time for subscription error to surface
	select {
	case <-s.sub.Channel():
		// Message received means subscription successful
	case <-ctx.Done():
		return ctx.Err()
	}

	go func() {
		ch := s.sub.Channel()
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				handler(msg.Payload)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Close closes the subscriber.
func (s *RedisSubscriber) Close() error {
	if s.sub != nil {
		return s.sub.Close()
	}
	return nil
}

// RedisPubSub implements PubSub using Redis.
type RedisPubSub struct {
	client *redis.Client
	sub    *redis.PubSub
}

// NewRedisPubSub creates a new Redis Pub/Sub instance.
func NewRedisPubSub(client *redis.Client) *RedisPubSub {
	return &RedisPubSub{client: client}
}

// Publish publishes a message to the specified Redis channel.
func (p *RedisPubSub) Publish(ctx context.Context, channel, key string) error {
	if p.client == nil {
		return fmt.Errorf("redis client is nil")
	}
	return p.client.Publish(ctx, channel, key).Err()
}

// Subscribe subscribes to the specified Redis channel.
func (p *RedisPubSub) Subscribe(ctx context.Context, channel string, handler func(key string)) error {
	if p.client == nil {
		return fmt.Errorf("redis client is nil")
	}

	p.sub = p.client.Subscribe(ctx, channel)
	// Give time for subscription error to surface
	select {
	case <-p.sub.Channel():
		// Message received means subscription successful
	case <-ctx.Done():
		return ctx.Err()
	}

	go func() {
		ch := p.sub.Channel()
		for {
			select {
			case msg, ok := <-ch:
				if !ok {
					return
				}
				handler(msg.Payload)
			case <-ctx.Done():
				return
			}
		}
	}()

	return nil
}

// Close closes the Pub/Sub.
func (p *RedisPubSub) Close() error {
	if p.sub != nil {
		return p.sub.Close()
	}
	return nil
}

// NewRedisPubSubFromCache creates a Redis Pub/Sub from a cachex.Cache instance.
// If the cache is a redisCache, it extracts the underlying client.
func NewRedisPubSubFromCache(cache cachex.Cache) (*RedisPubSub, error) {
	// Try to get the underlying redis client
	type redisClientGetter interface {
		Client() *redis.Client
	}

	if getter, ok := cache.(redisClientGetter); ok {
		client := getter.Client()
		if client != nil {
			return NewRedisPubSub(client), nil
		}
	}

	return nil, fmt.Errorf("cache does not support Redis Pub/Sub")
}
