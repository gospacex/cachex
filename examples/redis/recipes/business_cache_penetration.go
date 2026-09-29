// cachex/redis - 业务场景：缓存穿透防护
// 方案一：布隆过滤器预判 key 是否存在
// 方案二：null 值短 TTL 缓存（防恶意请求打到 DB）
package redisrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type CachePenetrationExamples struct{ rdb redis.UniversalClient }

func NewCachePenetrationExamples(rdb redis.UniversalClient) *CachePenetrationExamples {
	return &CachePenetrationExamples{rdb: rdb}
}

// ExampleBloomFilterGuard 布隆过滤器：查询前先判断 key 是否可能存在
func (e *CachePenetrationExamples) ExampleBloomFilterGuard(ctx context.Context) error {
	bfKey := "bf:products"
	existingIDs := []string{"prod:1001", "prod:1002", "prod:1003", "prod:1004", "prod:1005"}

	// 初始化 BF（若环境支持 RedisBloom）
	reserveCmd := redis.NewStatusCmd(ctx, "BF.RESERVE", bfKey, "0.001", "10000")
	_ = e.rdb.Process(ctx, reserveCmd)
	if err := reserveCmd.Err(); err != nil {
		fmt.Printf("  BF.RESERVE 不可用（非 Redis Stack），切换至 null-cache 方案\n")
		return e.exampleNullCacheGuard(ctx)
	}
	fmt.Println("  BF.RESERVE 创建成功")

	// 预填充已知 product ID
	for _, id := range existingIDs {
		e.rdb.Do(ctx, "BF.ADD", bfKey, id)
	}

	// 查询：现有 ID → BF 说存在 → 再查缓存
	for _, id := range existingIDs {
		c := redis.NewIntCmd(ctx, "BF.EXISTS", bfKey, id)
		_ = e.rdb.Process(ctx, c)
		exists, _ := c.Result()
		if exists == 1 {
			val, _ := e.rdb.Get(ctx, id).Result()
			fmt.Printf("  %s: BF命中 → 缓存值=%q\n", id, val)
		}
	}

	// 查询：不存在的 ID → BF 说一定不存在 → 直接拦截
	fmt.Println("  恶意请求（不存在）:")
	for _, id := range []string{"prod:9999", "prod:8888", "prod:0000"} {
		c := redis.NewIntCmd(ctx, "BF.EXISTS", bfKey, id)
		_ = e.rdb.Process(ctx, c)
		exists, _ := c.Result()
		if exists == 0 {
			fmt.Printf("  %s: BF 判定不存在，直接拦截，不查 DB ✓\n", id)
		} else {
			fmt.Printf("  %s: BF 误报（概率极低）\n", id)
		}
	}

	e.rdb.Del(ctx, bfKey)
	return nil
}

// exampleNullCacheGuard null 值短 TTL 缓存（不依赖 RedisBloom）
func (e *CachePenetrationExamples) exampleNullCacheGuard(ctx context.Context) error {
	known := []string{"prod:1001", "prod:1002"}
	notFound := []string{"prod:9999", "prod:8888"}

	for _, id := range known {
		e.rdb.Set(ctx, id, `{"name":"real product"}`, 24*time.Hour)
	}

	fmt.Println("  正常查询:")
	for _, id := range known {
		val, err := e.rdb.Get(ctx, id).Result()
		if err == nil {
			fmt.Printf("  %s: cache HIT → %s\n", id, val)
		}
	}

	fmt.Println("  穿透防护（null cache）:")
	for _, id := range notFound {
		_, err := e.rdb.Get(ctx, id).Result()
		if err == redis.Nil {
			fmt.Printf("  %s: cache MISS，写入 null 占位（TTL=5s）\n", id)
			e.rdb.Set(ctx, id, "", 5*time.Second)
		}
		// 第二次查询命中 null cache
		_, err2 := e.rdb.Get(ctx, id).Result()
		if err2 == redis.Nil {
			fmt.Printf("  %s: 再次查询仍为 Nil → 走 null cache 分支 ✓（TTL内不回源）\n", id)
		}
	}
	e.rdb.Del(ctx, known...)
	e.rdb.Del(ctx, notFound...)
	return nil
}
