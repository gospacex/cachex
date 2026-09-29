// examples/memcached/main.go
// Memcached 驱动全量示例：基础 CRUD + 自增/自减 + CAS 乐观锁 + 批量操作
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/gospacex/cachex"
	mcrecipes "github.com/gospacex/cachex/examples/memcached/recipes"
	mcdriver "github.com/gospacex/cachex/memcached"
	"github.com/gospacex/configx"
	_ "github.com/gospacex/logx/zap"
)

func main() {
	ctx := context.Background()

	cfg, err := configx.New(&configx.ConfigOptions{
		ConfigFile: "examples/config/memcached.yaml",
	})
	if err != nil {
		log.Fatalf("configx init failed: %v", err)
	}

	if err := cachex.Init(cachex.WithConfig(cfg, "cachex")); err != nil {
		log.Fatalf("cachex init failed: %v", err)
	}
	defer cachex.CloseAll()

	mcInst := cachex.Instance("memcached-primary")
	if mcInst == nil {
		log.Fatal("instance memcached-primary not found")
	}
	mcDriver, ok := mcInst.(*mcdriver.Client)
	if !ok {
		log.Fatal("instance memcached-primary is not a *mcdriver.Client")
	}
	mcRaw := mcDriver.Raw() // *memcache.Client

	if err := mcInst.HealthCheck(ctx); err != nil {
		log.Fatalf("health check failed: %v", err)
	}
	fmt.Println("✓ Memcached health check passed")

	fmt.Println("\n" + "═══════════════════════════════════════════")
	fmt.Println("  Memcached 驱动全量示例")
	fmt.Println("═══════════════════════════════════════════")

	// 0. 基础 CRUD（cachex.Cache 接口层）
	fmt.Println("\n【0. 基础 CRUD】")
	key := "sdk:example:hello"
	if err := mcInst.Set(ctx, key, "world", 30*time.Second); err != nil {
		log.Fatalf("set failed: %v", err)
	}
	fmt.Println("  ✓ SET hello -> world")
	val, err := mcInst.Get(ctx, key)
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}
	fmt.Printf("  ✓ GET %s = %q\n", key, val)
	exists, _ := mcInst.Exists(ctx, key)
	fmt.Printf("  ✓ EXISTS %s = %v\n", key, exists)
	mcInst.Delete(ctx, key)
	fmt.Println("  ✓ DEL hello")
	_, err = mcInst.Get(ctx, key)
	if err != nil {
		fmt.Printf("  ✓ GET %s after delete = miss (%v)\n", key, err)
	}
	// Keys 不支持
	if _, err := mcInst.Keys(ctx, "*"); err != nil {
		fmt.Printf("  ✓ KEYS 按预期不支持: %v\n", err)
	}

	// 1. 基础操作（GET/SET/ADD/REPLACE/TOUCH）
	fmt.Println("\n【1. 基础操作】")
	exBasic(mcRaw, ctx)

	// 2. 自增/自减（INCR/DECR）
	fmt.Println("\n【2. 自增/自减计数器】")
	exIncrDecr(mcRaw, ctx)

	// 3. CAS 乐观锁
	fmt.Println("\n【3. CAS 乐观锁】")
	exCAS(mcRaw, ctx)

	// 4. 批量操作（GetMulti/FlushAll/Stats）
	fmt.Println("\n【4. 批量操作】")
	exBatch(mcRaw, ctx)

	fmt.Println("\n" + "═══════════════════════════════════════════")
	fmt.Println("  全部示例运行完毕")
	fmt.Println("═══════════════════════════════════════════")
}

func exBasic(client *memcache.Client, ctx context.Context) {
	e := mcrecipes.NewBasicExamples(client)
	runSafe(ctx, "ExampleGetSet", e.ExampleGetSet)
	runSafe(ctx, "ExampleAddReplace", e.ExampleAddReplace)
	runSafe(ctx, "ExampleTouch", e.ExampleTouch)
}

func exIncrDecr(client *memcache.Client, ctx context.Context) {
	e := mcrecipes.NewCounterExamples(client)
	runSafe(ctx, "ExampleIncrement", e.ExampleIncrement)
	runSafe(ctx, "ExampleDecrement", e.ExampleDecrement)
	runSafe(ctx, "ExampleIncrementNotFound", e.ExampleIncrementNotFound)
}

func exCAS(client *memcache.Client, ctx context.Context) {
	e := mcrecipes.NewCasExamples(client)
	runSafe(ctx, "ExampleCAS", e.ExampleCAS)
	runSafe(ctx, "ExampleCASRetry", e.ExampleCASRetry)
}

func exBatch(client *memcache.Client, ctx context.Context) {
	e := mcrecipes.NewBatchExamples(client)
	runSafe(ctx, "ExampleGetMulti", e.ExampleGetMulti)
	runSafe(ctx, "ExampleFlushAll", e.ExampleFlushAll)
	runSafe(ctx, "ExampleStats", e.ExampleStats)
}

func runSafe(ctx context.Context, name string, fn func(context.Context) error) {
	if err := fn(ctx); err != nil {
		fmt.Printf("  ⚠ %s: %v\n", name, err)
	} else {
		fmt.Printf("  ✓ %s\n", name)
	}
}
