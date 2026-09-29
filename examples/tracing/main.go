// examples/tracing/main.go — 调用链验证示例
// 功能：启用调用链开关，验证 GET / SET / DELETE 命令自动产生 OTel span
// 上报目标：本地 Jaeger（docker-compose 启动），UI 访问 http://localhost:16686
//
// 运行方式（需先 docker compose up -d）：
//
//	go run ./examples/tracing/main.go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/gospacex/cachex"
	redisdriver "github.com/gospacex/cachex/redis"
	_ "github.com/gospacex/logx/zap"
	"go.opentelemetry.io/otel"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

func main() {
	ctx := context.Background()

	// 1. 用 WithConfigFile 加载 cachex 配置；tracing.enabled=true 时 SDK 自动初始化 TracerProvider
	if err := cachex.Init(cachex.WithConfigFile("examples/config/redis.yaml")); err != nil {
		log.Fatalf("cachex init failed: %v", err)
	}
	defer cachex.CloseAll()

	rdbInst := cachex.Instance("redis-primary")
	if rdbInst == nil {
		log.Fatal("instance redis-primary not found")
	}
	if _, ok := rdbInst.(*redisdriver.Client); !ok {
		log.Fatal("instance redis-primary is not a *redisdriver.Client")
	}

	// 2. 验证调用链开关已开启
	if !cachex.IsTracingEnabled() {
		log.Fatal("tracing is NOT enabled — this is a test failure")
	}
	fmt.Println("✓ 调用链开关已开启 (IsTracingEnabled = true)")

	// 3. 执行操作：每次操作都会因 IsTracingEnabled=true 而产生 db.redis.* span
	fmt.Println("\n--- 执行 Redis 操作，span 将上报到 Jaeger ---")

	tracer := otel.Tracer("github.com/gospacex/cachex/tracing-example")

	// ① 带 parent span 的 SET
	appCtx, spanSet := tracer.Start(ctx, "app-SET-user-profile")
	spanSet.SetAttributes(semconv.DBSystemRedis, semconv.DBNameKey.String("0"))
	if err := rdbInst.Set(appCtx, "trace:user:1001", `{"name":"Alice"}`, 30*time.Second); err != nil {
		log.Fatalf("set failed: %v", err)
	}
	spanSet.End()
	fmt.Println("  ✓ SET trace:user:1001（带 parent span）完成")

	// ② GET — 独立 span，无 parent
	val, err := rdbInst.Get(ctx, "trace:user:1001")
	if err != nil {
		log.Fatalf("get failed: %v", err)
	}
	fmt.Printf("  ✓ GET trace:user:1001 = %q\n", val)

	// ③ EXISTS
	exists, _ := rdbInst.Exists(ctx, "trace:user:1001")
	fmt.Printf("  ✓ EXISTS trace:user:1001 = %v\n", exists)

	// ④ DELETE
	if err := rdbInst.Delete(ctx, "trace:user:1001"); err != nil {
		log.Fatalf("delete failed: %v", err)
	}
	fmt.Println("  ✓ DELETE trace:user:1001 完成")

	// ⑤ 完整父子嵌套流程
	fmt.Println("\n--- 带 parent span 的完整操作链 ---")
	orderCtx, spanParent := tracer.Start(ctx, "app-transact-order")
	spanParent.SetAttributes(semconv.DBSystemRedis, semconv.DBNameKey.String("0"))

	if err := rdbInst.Set(orderCtx, "trace:order:8888", `{"item":"widget","qty":3}`, 60*time.Second); err != nil {
		log.Fatalf("set order failed: %v", err)
	}
	fmt.Println("  ✓ SET trace:order:8888（子 span 嵌套在 app-transact-order 下）")

	if v, _ := rdbInst.Get(orderCtx, "trace:order:8888"); v != "" {
		fmt.Printf("  ✓ GET trace:order:8888 = %q（透传 parent）\n", v)
	}
	if err := rdbInst.Delete(orderCtx, "trace:order:8888"); err != nil {
		log.Fatalf("delete order failed: %v", err)
	}
	fmt.Println("  ✓ DELETE trace:order:8888")

	spanParent.End()

	fmt.Println("\n✓ 所有 span 已通过 OTLP HTTP 上报到 Jaeger")
	fmt.Println("  访问 http://localhost:16686 查看 traces")
	fmt.Println("  搜索 service=cachex-tracing-example 即可看到完整调用链")
}
