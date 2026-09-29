// cachex/memcached - 基础 GET/SET/ADD/REPLACE 命令案例
package memcachedrecipes

import (
	"context"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
)

type BasicExamples struct{ mc *memcache.Client }

func NewBasicExamples(mc *memcache.Client) *BasicExamples {
	return &BasicExamples{mc: mc}
}

// ExampleGetSet Get/Set 基本读写
func (e *BasicExamples) ExampleGetSet(ctx context.Context) error {
	key := "memcached:user:1001"
	// Set 写入（过期时间：3600 秒）
	err := e.mc.Set(&memcache.Item{
		Key:        key,
		Value:      []byte(`{"name":"Alice","age":30}`),
		Flags:      0,
		Expiration: 3600,
	})
	if err != nil {
		return fmt.Errorf("Set: %w", err)
	}
	// Get 读取
	item, err := e.mc.Get(key)
	if err != nil {
		return fmt.Errorf("Get: %w", err)
	}
	fmt.Printf("GET %s = %s\n", key, item.Value)
	// 清理
	e.mc.Delete(key)
	return nil
}

// ExampleAddReplace Add（不存在才存）/ Replace（存在才替换）
func (e *BasicExamples) ExampleAddReplace(ctx context.Context) error {
	// ADD：key 不存在时写入，已存在返回 ErrNotStored
	err := e.mc.Add(&memcache.Item{
		Key:        "memcached:lock:init",
		Value:      []byte("initial-value"),
		Expiration: 60,
	})
	if err == memcache.ErrNotStored {
		fmt.Println("ADD: key 已存在，写入被拒绝")
	} else if err != nil {
		return fmt.Errorf("Add: %w", err)
	} else {
		fmt.Println("ADD: 成功写入新 key")
	}
	// REPLACE：key 必须已存在，否则返回 ErrNotStored
	err = e.mc.Replace(&memcache.Item{
		Key:        "memcached:lock:init",
		Value:      []byte("updated-value"),
		Expiration: 60,
	})
	if err == memcache.ErrNotStored {
		fmt.Println("REPLACE: key 不存在，替换被拒绝")
	} else if err != nil {
		return fmt.Errorf("Replace: %w", err)
	} else {
		fmt.Println("REPLACE: 成功替换已存在的 key")
	}
	// 读取验证
	item, _ := e.mc.Get("memcached:lock:init")
	fmt.Printf("GET after replace = %s\n", item.Value)
	e.mc.Delete("memcached:lock:init")
	return nil
}

// ExampleTouch 重置 TTL（续期）
func (e *BasicExamples) ExampleTouch(ctx context.Context) error {
	key := "memcached:session:abc123"
	e.mc.Set(&memcache.Item{
		Key:        key,
		Value:      []byte("session-data"),
		Expiration: 10, // 10 秒后过期
	})
	// Touch 将 TTL 重置为 300 秒
	err := e.mc.Touch(key, 300)
	if err != nil {
		return fmt.Errorf("Touch: %w", err)
	}
	fmt.Printf("Touch %s → TTL 重置为 300s\n", key)
	e.mc.Delete(key)
	return nil
}
