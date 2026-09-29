// cachex/redis - 业务场景：延迟队列
// 实现方案：ZSet score=执行时间戳，定时轮询 score≤now 的任务
package redisrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type DelayedQueueExamples struct{ rdb redis.UniversalClient }

func NewDelayedQueueExamples(rdb redis.UniversalClient) *DelayedQueueExamples {
	return &DelayedQueueExamples{rdb: rdb}
}

// ExampleDelayQueue 延迟队列：ZADD(score=执行时间) + ZRANGEBYSCORE 轮询消费
func (e *DelayedQueueExamples) ExampleDelayQueue(ctx context.Context) error {
	key := "delayqueue:tasks"
	e.rdb.Del(ctx, key)

	now := float64(time.Now().Unix())

	// 入队：不同延迟时间
	tasks := []struct {
		name  string
		delay time.Duration
	}{
		{"task-timeout-refund", 500 * time.Millisecond},   // 500ms 后执行
		{"task-send-sms", 200 * time.Millisecond},         // 200ms 后执行
		{"task-generate-report", 2000 * time.Millisecond}, // 2s 后执行
	}
	for _, t := range tasks {
		score := now + t.delay.Seconds()
		e.rdb.ZAdd(ctx, key, &redis.Z{Score: score, Member: t.name})
		fmt.Printf("  ENQUEUE %s (score=%.0f, delay=%v)\n", t.name, score, t.delay)
	}

	// 模拟调度循环：每隔 200ms 检查一次到期任务
	fmt.Println("\n  调度循环（等待任务到期）:")
	processed := 0
	for processed < len(tasks) {
		time.Sleep(150 * time.Millisecond)
		// 取出所有 score <= now 的任务
		cutoff := float64(time.Now().Unix())
		items, _ := e.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
			Min: "-inf",
			Max: fmt.Sprintf("%.6f", float64(cutoff)),
		}).Result()
		if len(items) == 0 {
			continue
		}
		for _, z := range items {
			fmt.Printf("  EXECUTE %s (score=%.0f)\n", z.Member, z.Score)
			e.rdb.ZRem(ctx, key, z.Member)
			processed++
		}
	}

	remaining, _ := e.rdb.ZCard(ctx, key).Result()
	fmt.Printf("  队列剩余 = %d\n", remaining)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleDelayQueueCronLike 类 Cron 任务：score = 下次触发时间戳（周期任务）
func (e *DelayedQueueExamples) ExampleDelayQueueCronLike(ctx context.Context) error {
	key := "delayqueue:cron:cleanup"
	e.rdb.Del(ctx, key)

	// 初始触发时间：当前时间 + 1s
	now := float64(time.Now().Unix())
	triggerAt := now + 1.0
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: triggerAt, Member: "cleanup-job"})
	fmt.Printf("  计划任务入队，预计执行时间 = %.0f\n", triggerAt)

	// 等待并消费
	time.Sleep(1100 * time.Millisecond)
	cutoff := float64(time.Now().Unix())
	items, _ := e.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%.6f", cutoff),
	}).Result()
	for _, z := range items {
		fmt.Printf("  ✅ cron 任务执行: %s\n", z.Member)
		e.rdb.ZRem(ctx, key, z.Member)
		// 重新入队（下一周期：+1s）
		newScore := float64(time.Now().Unix()) + 1.0
		e.rdb.ZAdd(ctx, key, &redis.Z{Score: newScore, Member: z.Member})
		fmt.Printf("  🔄 重新排程，下次执行时间 = %.0f\n", newScore)
	}
	e.rdb.Del(ctx, key)
	return nil
}
