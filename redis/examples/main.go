// cachex/redis/examples: Redis 缓存示例
// 功能：Set / Get / Exists / Keys / Delete / HealthCheck
//
// 运行：
//
//	cd cachex/redis && ./scripts/start.sh     # 起 Redis Stack（宿主 6379，密码 redis123456）
//	cd examples && go run .
//	cd .. && ./scripts/stop.sh
//
// 若 6379 已被其它容器占用，用 HOST_PORT=<空闲端口> ./scripts/start.sh，
// 并同步修改 config.yaml 的 addr。
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gospacex/cachex"
	_ "github.com/gospacex/cachex/redis"
	"github.com/gospacex/configx"
	"github.com/gospacex/logx"
	_ "github.com/gospacex/logx/zap"
)

func main() {
	ctx := context.Background()

	if err := logx.ReadConfig(); err != nil {
		log.Printf("logx.ReadConfig fallback: %v", err)
	}
	logger := logx.GetGlobal()
	logger.InfoContext(ctx, "cachex redis example started", logx.String("driver", "redis"))

	instances, err := cachex.ReadAndCreate(configx.ConfigOptions{
		ConfigFile: "config.yaml",
		ConfigType: "yaml",
	})
	if err != nil {
		logger.Fatal("create cachex client failed", logx.Err(err))
	}
	defer func() {
		for name, c := range instances {
			if cerr := c.Close(); cerr != nil {
				logger.Error("close cache failed", logx.String("name", name), logx.Err(cerr))
			}
		}
	}()

	cache := instances["default"]
	if cache == nil {
		logger.Fatal("instance \"default\" not found; 检查 config.yaml 的 cachex.instances[].name")
	}

	// HealthCheck（cachex 的签名带 ctx，与 kvx 不同）
	if err := cache.HealthCheck(ctx); err != nil {
		logger.Fatal("health check failed", logx.Err(err))
	}
	logger.InfoContext(ctx, "health check passed")

	// Set / Get
	if err := cache.Set(ctx, "k1", "v1", 10*time.Second); err != nil {
		logger.Fatal("set failed", logx.Err(err))
	}
	logger.InfoContext(ctx, "set key", logx.String("key", "k1"))

	val, err := cache.Get(ctx, "k1")
	if err != nil {
		logger.Fatal("get failed", logx.Err(err))
	}
	logger.InfoContext(ctx, "get value", logx.String("value", val))

	// Exists
	ok, err := cache.Exists(ctx, "k1")
	if err != nil {
		logger.Fatal("exists failed", logx.Err(err))
	}
	logger.InfoContext(ctx, "key exists", logx.Any("exists", ok))

	// Keys（Redis 支持 SCAN，按 pattern 枚举）
	keys, err := cache.Keys(ctx, "k*")
	if err != nil {
		logger.Fatal("keys failed", logx.Err(err))
	}
	logger.InfoContext(ctx, "keys found", logx.Int("count", len(keys)))

	// Delete
	if err := cache.Delete(ctx, "k1"); err != nil {
		logger.Fatal("delete failed", logx.Err(err))
	}
	logger.InfoContext(ctx, "deleted key", logx.String("key", "k1"))

	// 删完确认真的没了。
	//
	// ⚠️ 这里必须用 Exists，不能用 Get 判空：cachex 的 Get 在 miss 时返回
	// ("", nil)（redis.Nil 被吞），与 kvx 不同——它无法区分"缓存了空字符串"
	// 和"压根不存在"。Exists 才是接口层提供的正统判定手段。
	// 该行为在 cachex/redis/FEATURE_MATRIX.md 与 memcached/FEATURE_MATRIX.md
	// 中有明确记录，属既有语义，不要靠 Get 的 err 来推断存在性。
	ok, err = cache.Exists(ctx, "k1")
	if err != nil {
		logger.Fatal("exists after delete failed", logx.Err(err))
	}
	if ok {
		logger.Fatal("key still exists after delete")
	}
	logger.InfoContext(ctx, "key gone after delete")

	fmt.Println("\n✅ All operations completed successfully.")
}
