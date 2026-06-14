package assert

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
)

const (
	// BackendJaeger / BackendRedisStream / BackendKafkaTopic 是 3 种 trace 后端。
	BackendJaeger      = "jaeger"
	BackendRedisStream = "redis_stream"
	BackendKafkaTopic  = "kafka_topic"

	// TopologySingle / TopologyCluster 是 cachex 6 组合中的拓扑维度。
	TopologySingle  = "single"
	TopologyCluster = "cluster"

	defaultAssertTimeout = 15 * time.Second
	pollInterval         = 500 * time.Millisecond
)

// repoRoot 计算 example/<driver>_test 相对仓库根的路径。
// 约定：e2e_test.go 位于 example/<driver>_test/，compose 假设在 repo 根 test/docker。
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root, err := filepath.Abs(filepath.Join(wd, "..", ".."))
	if err != nil {
		t.Fatalf("abs repo root: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Skipf("repo root not found from %q (expected ../..): %v", wd, err)
	}
	return root
}

// StartStack 启动 driver 拓扑对应的 docker-compose + 对应 trace backend 容器。
//
// driver    = "redisx" | "kafkax" | "tracing"（无 _test 后缀）
// topology  = "single" | "cluster"
// backend   = "jaeger" | "redis_stream" | "kafka_topic"
//
// single 拓扑假定 driver broker 已由外部 docker run 提供（不二次启），
// cluster 拓扑走 test/docker/<driver>/cluster.yaml 拉起专属 broker 集群。
//
// 若 docker 不可用 / compose 文件缺失 → t.Skip，不 Fail。
// t.Cleanup 自动 down -v。
func StartStack(t *testing.T, driver, topology, backend string) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available, skipping e2e")
	}
	root := repoRoot(t)

	// 1. trace backend 容器（redis / kafka）—— 三个独立 compose
	if backend == BackendJaeger {
		t.Logf("[start-stack] backend=jaeger: 跳过 trace compose 启停 (依赖外部 jaeger 容器)")
	} else {
		traceCompose := filepath.Join(root, "test", "docker", "trace", backend+".yaml")
		runComposeOrSkip(t, traceCompose, "up", "-d", "--wait")
		t.Cleanup(func() { runCompose(t, traceCompose, "down", "-v") })
	}

	// 2. driver 拓扑容器 —— single 跳过（共享 compose 已在外端起好）
	if topology == TopologySingle {
		t.Logf("[start-stack] topology=single: 跳过 driver 容器启停 (driver=%s 由外部 docker run 提供)", driver)
		return
	}
	driverCompose := filepath.Join(root, "test", "docker", driver, topology+".yaml")
	runComposeOrSkip(t, driverCompose, "up", "-d", "--wait")
	t.Cleanup(func() { runCompose(t, driverCompose, "down", "-v") })
}

func runComposeOrSkip(t *testing.T, file string, args ...string) {
	t.Helper()
	if _, err := os.Stat(file); err != nil {
		t.Skipf("compose file %s not found: %v", file, err)
	}
	runCompose(t, file, args...)
}

func runCompose(t *testing.T, file string, args ...string) {
	t.Helper()
	cmd := exec.Command("docker", append([]string{"compose", "-f", file}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v failed: %v\n%s", args, err, out)
	}
}

// NewTraceID 返回 16 字节随机 trace.TraceID。
// e2e 测试用它在 cachex.Set 之前 StartSpan(ctx, "cache.set", trace.WithTraceID(want))
// 注入已知 ID，然后在 AssertSpanInBackend 里用同一 ID 去 backend 查找。
func NewTraceID(t *testing.T) trace.TraceID {
	t.Helper()
	var tid trace.TraceID
	if _, err := io.ReadFull(rand.Reader, tid[:]); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return tid
}

// TraceIDHex 返回 16 字节 trace.TraceID 的 32 字符 hex 表示。
func TraceIDHex(tid trace.TraceID) string {
	return tid.String()
}

// AssertSpanInBackend 轮询 backend 直到找到匹配 want 的 span 或超时。
//
// backend 决定查询端点：
//   - jaeger:        GET http://localhost:16686/api/traces/<hex>
//   - redis_stream:  XRange trace:<driver>:<topology>
//   - kafka_topic:   起临时 consumer 拉 trace-spans-<driver> topic
func AssertSpanInBackend(t *testing.T, ctx context.Context, backend, driver, topology string, want trace.TraceID) {
	AssertSpanInBackendWithTimeout(t, ctx, backend, driver, topology, want, defaultAssertTimeout)
}

// AssertSpanInBackendWithTimeout 同 AssertSpanInBackend，可自定义超时。
func AssertSpanInBackendWithTimeout(t *testing.T, ctx context.Context, backend, driver, topology string, want trace.TraceID, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		found, err := queryBackend(ctx, backend, driver, topology, want)
		if err != nil {
			lastErr = err
		} else if found {
			return
		}
		time.Sleep(pollInterval)
	}
	t.Fatalf("[assert] trace_id=%s not found in backend=%s driver=%s topology=%s within %s (last_err=%v)",
		want, backend, driver, topology, timeout, lastErr)
}

func queryBackend(ctx context.Context, backend, driver, topology string, want trace.TraceID) (bool, error) {
	switch backend {
	case BackendJaeger:
		return queryJaeger(ctx, want)
	case BackendRedisStream:
		return queryRedisStream(ctx, driver, topology, want)
	case BackendKafkaTopic:
		return queryKafkaTopic(ctx, driver, topology, want)
	}
	return false, fmt.Errorf("unknown backend: %s", backend)
}

func queryJaeger(ctx context.Context, want trace.TraceID) (bool, error) {
	url := fmt.Sprintf("http://localhost:16686/api/traces/%s", want.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false, fmt.Errorf("build request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// 连不上 jaeger 不算 hard error，让上层轮询
		return false, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, nil
	}
	var body struct {
		Data []struct {
			TraceID string `json:"traceID"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return false, fmt.Errorf("decode jaeger response: %w", err)
	}
	for _, d := range body.Data {
		if d.TraceID == want.String() {
			return true, nil
		}
	}
	return false, nil
}

func queryRedisStream(ctx context.Context, driver, topology string, want trace.TraceID) (bool, error) {
	stream := fmt.Sprintf("trace:%s:%s", driver, topology)
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = client.Close() }()
	res, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		return false, nil
	}
	for _, msg := range res {
		payload, ok := msg.Values["data"].(string)
		if !ok {
			continue
		}
		var rec struct {
			TraceID string `json:"trace_id"`
		}
		if err := json.Unmarshal([]byte(payload), &rec); err != nil {
			continue
		}
		if rec.TraceID == want.String() {
			return true, nil
		}
	}
	return false, nil
}

func queryKafkaTopic(ctx context.Context, driver, topology string, want trace.TraceID) (bool, error) {
	spans := fetchKafkaTopicSpans(ctx, driver, topology, want)
	return len(spans) > 0, nil
}

// kafkaTraceBrokers 暴露给 fetchKafkaTopicSpans（trace_helpers.go）共享。
const kafkaTraceBrokers = "localhost:19092"
