// cachex/memcached/examples: Memcached 缓存示例
// 功能：Set / Get / Exists / Delete / HealthCheck，并演示 Keys 的能力降级
//
// 运行：
//
//	cd cachex/memcached && ./scripts/start.sh   # 宿主 21121 → 容器 11211
//	cd examples && go run .
//	cd .. && ./scripts/stop.sh
//
// 若 21121 被占用：HOST_PORT=<空闲端口> ./scripts/start.sh，并同步改 config.yaml。
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gospacex/cachex"
	_ "github.com/gospacex/cachex/memcached"
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
	logger.InfoContext(ctx, "cachex memcached example started", logx.String("driver", "memcached"))

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

	// Keys：memcached 协议没有枚举原语，驱动明确返回错误（不是静默空结果）。
	// 这里演示正确的降级姿势——判定后跳过，而不是把 err 当致命错误中断。
	if _, err := cache.Keys(ctx, "k*"); err != nil {
		logger.InfoContext(ctx, "Keys 不被 memcached 支持，按设计降级跳过", logx.Err(err))
	}

	// Delete
	if err := cache.Delete(ctx, "k1"); err != nil {
		logger.Fatal("delete failed", logx.Err(err))
	}
	logger.InfoContext(ctx, "deleted key", logx.String("key", "k1"))

	// 删完确认真的没了。
	//
	// ⚠️ 这里必须用 Exists，不能用 Get 判空：cachex 的 Get 在 miss 时返回
	// ("", nil)（gomemcache 的 ErrCacheMiss 被吞），无法区分"缓存了空字符串"
	// 和"压根不存在"。该行为在 memcached/FEATURE_MATRIX.md 中有明确记录。
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
