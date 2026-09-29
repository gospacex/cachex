// cachex/redis - Sorted Set 命令案例
package redisrecipes

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type ZSetExamples struct{ rdb redis.UniversalClient }

func NewZSetExamples(rdb redis.UniversalClient) *ZSetExamples {
	return &ZSetExamples{rdb: rdb}
}

// ExampleBasic ZADD/ZRANGE/ZREMRANGEBYRANK
func (e *ZSetExamples) ExampleBasic(ctx context.Context) error {
	key := "zset:leaderboard:game:001"
	e.rdb.Del(ctx, key)
	// ZADD 写入（member, score）
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: 1500, Member: "player-A"})
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: 2300, Member: "player-B"})
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: 1800, Member: "player-C"})
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: 2300, Member: "player-D"})
	// ZRANGE 升序
	items, _ := e.rdb.ZRangeWithScores(ctx, key, 0, -1).Result()
	fmt.Println("ZRANGE（升序）:")
	for _, z := range items {
		fmt.Printf("  %s = %.0f\n", z.Member, z.Score)
	}
	// ZREVRANGE 降序（排行榜 Top N）
	top, _ := e.rdb.ZRevRangeWithScores(ctx, key, 0, 2).Result()
	fmt.Println("ZREVRANGE Top 3:")
	for _, z := range top {
		fmt.Printf("  %s = %.0f\n", z.Member, z.Score)
	}
	// ZSCORE 查询分数
	score, _ := e.rdb.ZScore(ctx, key, "player-B").Result()
	fmt.Printf("ZSCORE player-B = %.0f\n", score)
	// ZRANK 查询排名（从 0 开始）
	rank, _ := e.rdb.ZRank(ctx, key, "player-A").Result()
	fmt.Printf("ZRANK player-A = %d\n", rank)
	// ZCARD 集合大小
	card, _ := e.rdb.ZCard(ctx, key).Result()
	fmt.Printf("ZCARD = %d\n", card)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleZIncrBy ZINCRBY 热度累加，适合热搜榜
func (e *ZSetExamples) ExampleZIncrBy(ctx context.Context) error {
	key := "zset:hot:topic"
	e.rdb.Del(ctx, key)
	// 初始热度
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: 100, Member: "golang"})
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: 80, Member: "redis"})
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: 60, Member: "cache"})
	// 热度累加（每次曝光 +1）
	e.rdb.ZIncrBy(ctx, key, 50, "golang")
	e.rdb.ZIncrBy(ctx, key, 30, "redis")
	e.rdb.ZIncrBy(ctx, key, 100, "cache")
	top, _ := e.rdb.ZRevRangeWithScores(ctx, key, 0, -1).Result()
	fmt.Println("ZREVRANGE after INCRBY:")
	for _, z := range top {
		fmt.Printf("  %s = %.0f\n", z.Member, z.Score)
	}
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleZCount ZCOUNT/ZREMRANGEBYSCORE 分数区间过滤
func (e *ZSetExamples) ExampleZCount(ctx context.Context) error {
	key := "zset:scores:exam"
	e.rdb.Del(ctx, key)
	scores := map[string]float64{
		"alice": 92, "bob": 78, "carol": 85, "dave": 60, "eve": 45,
	}
	for m, s := range scores {
		e.rdb.ZAdd(ctx, key, &redis.Z{Score: s, Member: m})
	}
	// ZCOUNT 分数区间人数
	n, _ := e.rdb.ZCount(ctx, key, "80", "+inf").Result()
	fmt.Printf("ZCOUNT [80, +inf] = %d（及格人数）\n", n)
	n, _ = e.rdb.ZCount(ctx, key, "60", "80").Result()
	fmt.Printf("ZCOUNT [60, 80] = %d（中等分数）\n", n)
	// ZREMZRANGEBYSCORE 删除不及格（<60）
	e.rdb.ZRemRangeByScore(ctx, key, "-inf", "59.99")
	remain, _ := e.rdb.ZCard(ctx, key).Result()
	fmt.Printf("删除后剩余 = %d\n", remain)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleZRANGEBYSCORE 延迟队列：score = 执行时间戳
func (e *ZSetExamples) ExampleZRANGEBYSCOREDeferQueue(ctx context.Context) error {
	key := "zset:delay:tasks"
	e.rdb.Del(ctx, key)
	now := float64(1000)
	// 写入延迟任务（score = 预期执行时间戳）
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: now + 100, Member: "task-a"}) // 100s 后执行
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: now + 50, Member: "task-b"})  // 50s 后执行
	e.rdb.ZAdd(ctx, key, &redis.Z{Score: now + 200, Member: "task-c"}) // 200s 后执行
	// 消费：取出所有已到期的任务（score <= 当前时间）
	now = now + 75 // 时间推进到 75s
	tasks, _ := e.rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprintf("%.0f", now)}).Result()
	fmt.Printf("到期任务（≤%d）: ", int(now))
	for _, z := range tasks {
		fmt.Printf("%s ", z.Member)
	}
	fmt.Println()
	// 从队列中移除已消费的任务
	for _, z := range tasks {
		e.rdb.ZRem(ctx, key, z.Member)
	}
	remain, _ := e.rdb.ZCard(ctx, key).Result()
	fmt.Printf("剩余任务 = %d\n", remain)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleZSCAN 大有序集合安全遍历
func (e *ZSetExamples) ExampleZSCAN(ctx context.Context) error {
	key := "zset:large:metrics"
	e.rdb.Del(ctx, key)
	for i := 0; i < 100; i++ {
		e.rdb.ZAdd(ctx, key, &redis.Z{Score: float64(i), Member: fmt.Sprintf("m:%d", i)})
	}
	var cursor uint64
	count := 0
	for {
		items, next, _ := e.rdb.ZScan(ctx, key, cursor, "*", 20).Result()
		count += len(items)
		cursor = uint64(next)
		if cursor == 0 {
			break
		}
	}
	fmt.Printf("ZSCAN 遍历完成，共 %d 个成员\n", count)
	e.rdb.Del(ctx, key)
	return nil
}
