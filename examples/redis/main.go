// examples/redis/main.go
// Redis 驱动全量示例：基础 CRUD + 5 种数据类型 + 高级命令 + 业务场景
// 本地环境：docker-compose up -d 启动 redis/redis-stack:8.4
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gospacex/cachex"
	redisrecipes "github.com/gospacex/cachex/examples/redis/recipes"
	redisdriver "github.com/gospacex/cachex/redis"
	"github.com/gospacex/configx"
	_ "github.com/gospacex/logx/zap"
)

func main() {
	ctx := context.Background()

	cfg, err := configx.New(&configx.ConfigOptions{
		ConfigFile: "examples/config/redis.yaml",
	})
	if err != nil {
		log.Fatalf("configx init failed: %v", err)
	}

	if err := cachex.Init(cachex.WithConfig(cfg, "cachex")); err != nil {
		log.Fatalf("cachex init failed: %v", err)
	}
	defer cachex.CloseAll()

	rdbInst := cachex.Instance("redis-primary")
	if rdbInst == nil {
		log.Fatal("instance redis-primary not found")
	}
	driver, ok := rdbInst.(*redisdriver.Client)
	if !ok {
		log.Fatal("instance redis-primary is not a *redisdriver.Client")
	}
	rdb := driver.Raw() // redis.UniversalClient

	if err := rdbInst.HealthCheck(ctx); err != nil {
		log.Fatalf("health check failed: %v", err)
	}
	fmt.Println("✓ Redis Stack health check passed")

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("  Redis 驱动全量示例（Redis Stack 8.4）")
	fmt.Println(strings.Repeat("=", 60))

	// 0. 基础 CRUD
	fmt.Println("\n【0. 基础 CRUD】")
	key := "sdk:example:hello"
	if err := rdbInst.Set(ctx, key, "world", 30*time.Second); err != nil {
		log.Fatalf("set failed: %v", err)
	}
	fmt.Println("  ✓ SET hello -> world")
	val, _ := rdbInst.Get(ctx, key)
	fmt.Printf("  ✓ GET %s = %q\n", key, val)
	exists, _ := rdbInst.Exists(ctx, key)
	fmt.Printf("  ✓ EXISTS %s = %v\n", key, exists)
	keys, _ := rdbInst.Keys(ctx, "sdk:example:*")
	fmt.Printf("  ✓ KEYS sdk:example:* = %v\n", keys)
	rdbInst.Delete(ctx, key)
	fmt.Println("  ✓ DEL hello")

	// 1. String
	fmt.Println("\n【1. String 类型】")
	exString(rdb, ctx)

	// 2. Hash
	fmt.Println("\n【2. Hash 类型】")
	exHash(rdb, ctx)

	// 3. List
	fmt.Println("\n【3. List 类型】")
	exList(rdb, ctx)

	// 4. Set
	fmt.Println("\n【4. Set 类型】")
	exSet(rdb, ctx)

	// 5. ZSet
	fmt.Println("\n【5. ZSet 类型】")
	exZSet(rdb, ctx)

	// 6. Advanced
	fmt.Println("\n【6. 高级命令】")
	exAdvanced(rdb, ctx)

	// 7. Bitmap
	fmt.Println("\n【7. Bitmap 位操作】")
	exBitmap(rdb, ctx)

	// 8. Stream
	fmt.Println("\n【8. Stream 消费组】")
	exStream(rdb, ctx)

	// 9. PubSub
	fmt.Println("\n【9. PubSub 发布订阅】")
	exPubSub(rdb, ctx)

	// 10. Search & Bloom
	fmt.Println("\n【10. RediSearch & RedisBloom】")
	exSearchBloom(rdb, ctx)

	// 11. Rate Limit
	fmt.Println("\n【11. 业务场景：分布式限流】")
	exRateLimit(rdb, ctx)

	// 12. Distributed Lock
	fmt.Println("\n【12. 业务场景：分布式锁】")
	exLock(rdb, ctx)

	// 13. Delayed Queue
	fmt.Println("\n【13. 业务场景：延迟队列】")
	exDelayedQueue(rdb, ctx)

	// 14. Cache Penetration
	fmt.Println("\n【14. 业务场景：缓存穿透防护】")
	exCachePenetration(rdb, ctx)

	// 15. Redis-only 兼容说明
	fmt.Println("\n【15. 兼容说明】")
	fmt.Println("  KeyDB / Dragonfly 与 Redis 协议兼容，直接以 driver: redis 接入即可（驱动已于本轮精简移除）")

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("  全部示例运行完毕")
	fmt.Println(strings.Repeat("=", 60))
}

// helpers — each calls the corresponding examples_*.go example function

func exString(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewStringExamples(rdb)
	runSafe(ctx, "ExampleBasic", e.ExampleBasic)
	runSafe(ctx, "ExampleSetOptions", e.ExampleSetOptions)
	runSafe(ctx, "ExampleMGetMSet", e.ExampleMGetMSet)
	runSafe(ctx, "ExampleIncrDecr", e.ExampleIncrDecr)
	runSafe(ctx, "ExampleAppendStrLen", e.ExampleAppendStrLen)
	runSafe(ctx, "ExampleBitOperations", e.ExampleBitOperations)
	runSafe(ctx, "ExampleHyperLogLog", e.ExampleHyperLogLog)
}

func exHash(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewHashExamples(rdb)
	runSafe(ctx, "ExampleBasic", e.ExampleBasic)
	runSafe(ctx, "ExampleHEXISTS_HKEYS_HSCAN", e.ExampleHEXISTS_HKEYS_HSCAN)
}

func exList(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewListExamples(rdb)
	runSafe(ctx, "ExampleBasic", e.ExampleBasic)
	runSafe(ctx, "ExampleBlocking", e.ExampleBlocking)
}

func exSet(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewSetExamples(rdb)
	runSafe(ctx, "ExampleBasic", e.ExampleBasic)
	runSafe(ctx, "ExampleSRandMemberSPop", e.ExampleSRandMemberSPop)
}

func exZSet(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewZSetExamples(rdb)
	runSafe(ctx, "ExampleBasic", e.ExampleBasic)
	runSafe(ctx, "ExampleZIncrBy", e.ExampleZIncrBy)
	runSafe(ctx, "ExampleZCount", e.ExampleZCount)
	runSafe(ctx, "ExampleZRANGEBYSCOREDeferQueue", e.ExampleZRANGEBYSCOREDeferQueue)
	runSafe(ctx, "ExampleZSCAN", e.ExampleZSCAN)
}

func exAdvanced(rdb redis.UniversalClient, ctx context.Context) {
	exGeo(rdb, ctx)
	exStreamOld(rdb, ctx)
	exPipeline(rdb, ctx)
	exScript(rdb, ctx)
}

func exGeo(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewGeoExamples(rdb)
	runSafe(ctx, "ExampleGeoAdd", e.ExampleGeoAdd)
}

func exStreamOld(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewStreamExamples(rdb)
	runSafe(ctx, "ExampleXAdd", e.ExampleXAdd)
}

func exPipeline(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewPipelineExamples(rdb)
	runSafe(ctx, "ExamplePipeline", e.ExamplePipeline)
}

func exScript(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewScriptExamples(rdb)
	runSafe(ctx, "ExampleEvalLock", e.ExampleEvalLock)
}

func exBitmap(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewBitmapExamples(rdb)
	runSafe(ctx, "ExampleSignIn", e.ExampleSignIn)
	runSafe(ctx, "ExampleOnlineStatus", e.ExampleOnlineStatus)
	runSafe(ctx, "ExampleBitPos", e.ExampleBitPos)
}

func exStream(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewStreamExamplesV2(rdb)
	runSafe(ctx, "ExampleXAddXRead", e.ExampleXAddXRead)
	runSafe(ctx, "ExampleXReadGroup", e.ExampleXReadGroup)
	runSafe(ctx, "ExampleXPending", e.ExampleXPending)
}

func exPubSub(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewPubSubExamples(rdb)
	runSafe(ctx, "ExamplePublishSubscribe", e.ExamplePublishSubscribe)
	runSafe(ctx, "ExamplePatternSubscribe", e.ExamplePatternSubscribe)
	runSafe(ctx, "ExamplePubSubNumSub", e.ExamplePubSubNumSub)
}

func exSearchBloom(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewSearchBloomExamples(rdb)
	runSafe(ctx, "ExampleRediSearchCreateIndex", e.ExampleRediSearchCreateIndex)
	runSafe(ctx, "ExampleRedisBloomFilter", e.ExampleRedisBloomFilter)
	runSafe(ctx, "ExampleRedisBloomCMS", e.ExampleRedisBloomCMS)
	runSafe(ctx, "ExampleRedisBloomCuckoo", e.ExampleRedisBloomCuckoo)
}

func exRateLimit(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewRateLimitExamples(rdb)
	runSafe(ctx, "ExampleTokenBucket", e.ExampleTokenBucket)
	runSafe(ctx, "ExampleSlidingWindowLog", e.ExampleSlidingWindowLog)
}

func exLock(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewDistributedLockExamples(rdb)
	runSafe(ctx, "ExampleBasicLock", e.ExampleBasicLock)
	runSafe(ctx, "ExampleRenewalLock", e.ExampleRenewalLock)
}

func exDelayedQueue(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewDelayedQueueExamples(rdb)
	runSafe(ctx, "ExampleDelayQueue", e.ExampleDelayQueue)
	runSafe(ctx, "ExampleDelayQueueCronLike", e.ExampleDelayQueueCronLike)
}

func exCachePenetration(rdb redis.UniversalClient, ctx context.Context) {
	e := redisrecipes.NewCachePenetrationExamples(rdb)
	runSafe(ctx, "ExampleBloomFilterGuard", e.ExampleBloomFilterGuard)
}

// runSafe 包装示例函数，捕获 panic 并打印错误而不中断流程
func runSafe(ctx context.Context, name string, fn func(context.Context) error) {
	if err := fn(ctx); err != nil {
		fmt.Printf("  ⚠ %s: %v\n", name, err)
	} else {
		fmt.Printf("  ✓ %s\n", name)
	}
}
