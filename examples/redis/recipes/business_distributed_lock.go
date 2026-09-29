// cachex/redis - 业务场景：分布式锁
// 实现方案：SET NX EX + Lua 原子释放（见 examples_advanced.go ExampleEvalLock）
package redisrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type DistributedLockExamples struct{ rdb redis.UniversalClient }

func NewDistributedLockExamples(rdb redis.UniversalClient) *DistributedLockExamples {
	return &DistributedLockExamples{rdb: rdb}
}

// ExampleBasicLock SET NX EX 加锁 + Lua 安全释放
func (e *DistributedLockExamples) ExampleBasicLock(ctx context.Context) error {
	lockKey := "lock:order:checkout:5001"
	lockVal := "token-worker-a-uuid"
	ttl := 10 * time.Second

	e.rdb.Del(ctx, lockKey)

	// 尝试加锁
	ok, err := e.rdb.SetNX(ctx, lockKey, lockVal, ttl).Result()
	if err != nil {
		return fmt.Errorf("SETNX: %w", err)
	}
	fmt.Printf("  SETNX 加锁 result = %v\n", ok)
	if !ok {
		fmt.Println("  锁已被其他节点持有，获取失败")
		return nil
	}
	fmt.Println("  ✅ 加锁成功，持有 10s")

	// 模拟业务处理
	time.Sleep(100 * time.Millisecond)

	// Lua 脚本原子释放（防误删他人锁）
	unlockScript := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	result, err := unlockScript.Run(ctx, e.rdb, []string{lockKey}, lockVal).Int()
	if err != nil {
		return fmt.Errorf("unlock: %w", err)
	}
	fmt.Printf("  Lua 解锁 result = %d（1=成功释放，0=值不匹配）\n", result)

	// 验证锁已释放
	exists, _ := e.rdb.Exists(ctx, lockKey).Result()
	fmt.Printf("  锁是否存在 = %v\n", exists > 0)
	return nil
}

// ExampleRenewalLock 锁续期：业务未完成前防止 TTL 过期被他人抢锁
func (e *DistributedLockExamples) ExampleRenewalLock(ctx context.Context) error {
	lockKey := "lock:longtask:report-gen"
	lockVal := "token-worker-b-uuid"
	initialTTL := 5 * time.Second

	e.rdb.Del(ctx, lockKey)

	// 加锁
	ok, _ := e.rdb.SetNX(ctx, lockKey, lockVal, initialTTL).Result()
	if !ok {
		fmt.Println("  加锁失败，锁已被持有")
		return nil
	}
	fmt.Println("  ✅ 加锁成功，TTL=5s")

	// Lua 续期脚本：仅当 value 匹配时才延长 TTL
	renewScript := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return -1
		end
	`)
	// 模拟长时间任务：每 2s 续期一次
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Second)
		res, err := renewScript.Run(ctx, e.rdb, []string{lockKey}, lockVal, "10").Int64()
		if err != nil {
			fmt.Printf("  续期失败: %v\n", err)
			break
		}
		fmt.Printf("  续期 #%d: result=%d (1=成功)\n", i+1, res)
	}

	// 释放
	unlockScript := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	unlockScript.Run(ctx, e.rdb, []string{lockKey}, lockVal).Int()
	fmt.Println("  任务完成，锁已释放")
	return nil
}
