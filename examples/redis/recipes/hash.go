// cachex/redis - Hash 命令案例
package redisrecipes

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type HashExamples struct{ rdb redis.UniversalClient }

func NewHashExamples(rdb redis.UniversalClient) *HashExamples {
	return &HashExamples{rdb: rdb}
}

// ExampleBasic 基础 HSET/HGET/HDEL/HGETALL
func (e *HashExamples) ExampleBasic(ctx context.Context) error {
	key := "hash:user:1001"
	// HSET 单字段（Redis <4.0）或逐字段
	e.rdb.HSet(ctx, key, "name", "Alice")
	e.rdb.HSet(ctx, key, "email", "alice@example.com")
	e.rdb.HSet(ctx, key, "age", 30)
	// HGET
	v, _ := e.rdb.HGet(ctx, key, "name").Result()
	fmt.Printf("HGET %s name = %s\n", key, v)
	// HGETALL
	all, _ := e.rdb.HGetAll(ctx, key).Result()
	fmt.Printf("HGETALL %s = %v\n", key, all)
	// HDEL
	e.rdb.HDel(ctx, key, "age")
	left, _ := e.rdb.HLen(ctx, key).Result()
	fmt.Printf("HLEN after delete = %d\n", left)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleHMSetHMGet 批量字段写入/读取
func (e *HashExamples) ExampleHMSetHMGet(ctx context.Context) error {
	key := "hash:product:sku:888"
	// HMSET（或 HSET 多字段，Redis ≥4.0）
	e.rdb.HSet(ctx, key,
		"color", "red",
		"size", "L",
		"stock", 100,
		"price", 99.9,
	)
	// HMGET 精确字段
	vals, _ := e.rdb.HMGet(ctx, key, "color", "size", "stock").Result()
	fmt.Printf("HMGET = %v\n", vals)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleHIncrBy 字段级原子计数，适合销量/积分累加
func (e *HashExamples) ExampleHIncrBy(ctx context.Context) error {
	key := "hash:stats:article:42"
	// 阅读量自增
	v, _ := e.rdb.HIncrBy(ctx, key, "views", 1).Result()
	fmt.Printf("HINCRBY views = %d\n", v)
	v, _ = e.rdb.HIncrBy(ctx, key, "views", 99).Result()
	fmt.Printf("HINCRBY views +99 = %d\n", v)
	// 浮点自增（点赞数）
	f, _ := e.rdb.HIncrByFloat(ctx, key, "likes", 2.5).Result()
	fmt.Printf("HINCRBYFLOAT likes +2.5 = %.1f\n", f)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleHEXISTS HKEYS/HVALS/HSCAN 大 Hash 安全遍历
func (e *HashExamples) ExampleHEXISTS_HKEYS_HSCAN(ctx context.Context) error {
	key := "hash:config:app"
	e.rdb.Del(ctx, key)
	e.rdb.HSet(ctx, key, "debug", "false")
	e.rdb.HSet(ctx, key, "cache_ttl", "3600")
	e.rdb.HSet(ctx, key, "max_conn", "100")

	// HEXISTS 字段存在性
	exists, _ := e.rdb.HExists(ctx, key, "debug").Result()
	fmt.Printf("HEXISTS debug = %v\n", exists)

	// HKEYS / HVALS
	keys, _ := e.rdb.HKeys(ctx, key).Result()
	vals, _ := e.rdb.HVals(ctx, key).Result()
	fmt.Printf("HKEYS = %v\nHVALS = %v\n", keys, vals)

	// HSCAN 游标遍历（生产大 Hash 替代 HGETALL）
	var cursor uint64
	for {
		items, next, _ := e.rdb.HScan(ctx, key, cursor, "*", 10).Result()
		fmt.Printf("HSCAN cursor=%d items=%v\n", cursor, items)
		cursor = uint64(next)
		if cursor == 0 {
			break
		}
	}
	e.rdb.Del(ctx, key)
	return nil
}
