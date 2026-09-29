// cachex/redis - 业务场景：分布式限流
// 实现方案：INCR + EXPIRE 原子计数器（滑动窗口简化版）
package redisrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type RateLimitExamples struct{ rdb redis.UniversalClient }

func NewRateLimitExamples(rdb redis.UniversalClient) *RateLimitExamples {
	return &RateLimitExamples{rdb: rdb}
}

// ExampleTokenBucket 令牌桶限流：窗口内最大 N 次请求
func (e *RateLimitExamples) ExampleTokenBucket(ctx context.Context) error {
	key := "ratelimit:api:user:1001"
	windowSec := int64(60) // 1 分钟窗口
	maxReq := int64(10)    // 最多 10 次

	e.rdb.Del(ctx, key)

	for i := int64(1); i <= maxReq+3; i++ {
		// INCR 原子自增，首次返回 1 时设 TTL
		cur, err := e.rdb.Incr(ctx, key).Result()
		if err != nil {
			return fmt.Errorf("INCR: %w", err)
		}
		if cur == 1 {
			e.rdb.Expire(ctx, key, time.Duration(windowSec)*time.Second)
		}
		remaining := maxReq - cur
		status := "✅ 允许"
		if remaining < 0 {
			status = "🚫 限流"
		}
		fmt.Printf("  请求 #%d: 当前=%d 剩余=%d %s\n", i, cur, remaining, status)
	}

	ttl, _ := e.rdb.TTL(ctx, key).Result()
	fmt.Printf("  窗口剩余 TTL = %ds\n", ttl)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleSlidingWindowLog 滑动窗口日志：每条请求记一个 zset member，score=time
// 比固定窗口更精准，无边界突刺问题
func (e *RateLimitExamples) ExampleSlidingWindowLog(ctx context.Context) error {
	key := "sliding:api:user:2001"
	windowMs := int64(1000) // 1 秒窗口（测试用短窗口）
	maxReq := int64(3)

	e.rdb.Del(ctx, key)

	now := time.Now().UnixMilli()
	for i := int64(1); i <= maxReq+2; i++ {
		member := fmt.Sprintf("%d:%d", now, i) // 用序号保证唯一
		// ZADD 写入（score=毫秒时间戳）
		e.rdb.ZAdd(ctx, key, &redis.Z{Score: float64(now), Member: member})
		// 删除窗口外的旧记录
		cutoff := now - windowMs
		e.rdb.ZRemRangeByScore(ctx, key, "-inf", fmt.Sprintf("%d", cutoff))
		// 当前窗口内请求数
		count, _ := e.rdb.ZCount(ctx, key, fmt.Sprintf("%d", cutoff), "+inf").Result()
		status := "✅ 允许"
		if count > maxReq {
			status = "🚫 限流"
		}
		fmt.Printf("  请求 #%d: 窗口内=%d %s\n", i, count, status)
	}
	e.rdb.Del(ctx, key)
	return nil
}
