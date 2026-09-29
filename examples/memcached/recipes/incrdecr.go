// cachex/memcached - Incr/Decr 原子计数器案例
package memcachedrecipes

import (
	"context"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
)

type CounterExamples struct{ mc *memcache.Client }

func NewCounterExamples(mc *memcache.Client) *CounterExamples {
	return &CounterExamples{mc: mc}
}

// ExampleIncrement 原子自增，适合 PV/UV 计数、限流
func (e *CounterExamples) ExampleIncrement(ctx context.Context) error {
	key := "counter:page:view:home"
	e.mc.Delete(key)
	// 首次 Increment（key 不存在时报 ErrCacheMiss）
	// Memcached 要求 key 已存在且值为数字，初始先 Set 为 "0"
	e.mc.Set(&memcache.Item{
		Key:        key,
		Value:      []byte("0"),
		Expiration: 0, // 永不过期
	})
	// 自增 1
	newVal, err := e.mc.Increment(key, 1)
	if err != nil {
		return fmt.Errorf("Increment: %w", err)
	}
	fmt.Printf("INCR %s → %d\n", key, newVal)
	// 批量自增
	for i := 0; i < 99; i++ {
		e.mc.Increment(key, 1)
	}
	final, _ := e.mc.Increment(key, 0)
	fmt.Printf("INCR 100 次后 = %d\n", final)
	// 自增 10
	newVal, _ = e.mc.Increment(key, 10)
	fmt.Printf("INCRBY 10 → %d\n", newVal)
	e.mc.Delete(key)
	return nil
}

// ExampleDecrement 原子自减，库存扣减、倒计时
func (e *CounterExamples) ExampleDecrement(ctx context.Context) error {
	key := "counter:stock:item:42"
	e.mc.Delete(key)
	e.mc.Set(&memcache.Item{
		Key:        key,
		Value:      []byte("100"),
		Expiration: 0,
	})
	// 卖出 5 件
	newVal, err := e.mc.Decrement(key, 5)
	if err != nil {
		return fmt.Errorf("Decrement: %w", err)
	}
	fmt.Printf("DECR 5 → 剩余库存 = %d\n", newVal)
	// 自减到 0 不会变负数
	for i := 0; i < 200; i++ {
		e.mc.Decrement(key, 1)
	}
	bottom, _ := e.mc.Get(key)
	fmt.Printf("大量 DECR 后 = %s（不低于 0）\n", bottom.Value)
	e.mc.Delete(key)
	return nil
}

// ExampleIncrementNotFound 不存在时 Increment 返回 ErrCacheMiss
func (e *CounterExamples) ExampleIncrementNotFound(ctx context.Context) error {
	key := "counter:nonexist"
	e.mc.Delete(key)
	_, err := e.mc.Increment(key, 1)
	if err == memcache.ErrCacheMiss {
		fmt.Println("Increment 不存在的 key → ErrCacheMiss（预期行为）")
	} else if err != nil {
		return fmt.Errorf("unexpected error: %w", err)
	}
	return nil
}
