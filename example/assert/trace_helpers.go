package assert

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/trace"
)

// newJaegerRequest / httpDefaultDo 是 fetchJaegerSpans 的私有辅助，
// 让 fetchJaegerSpans 保持扁平不掺杂 http 构造细节。
func newJaegerRequest(ctx context.Context, url string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build jaeger request %q: %w", url, err)
	}
	req.Header.Set("Accept", "application/json")
	return req, nil
}

func httpDefaultDo(req *http.Request) (*http.Response, error) {
	return http.DefaultClient.Do(req)
}

var _ = io.Discard // 占位以防未来用于 trace body 调试

// SpanRecord 是 trace backend span 的统一抽象。
// jaeger backend 字段全填；redis_stream / kafka_topic 自定义 exporter
// 不写 Kind / ParentSpanID / Status，因此这两个 backend 下这三个字段
// 必为空字符串。调用方按 backend 决定是否校验这些字段。
type SpanRecord struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	Name         string
	Kind         string
	Attributes   map[string]string
	StartTime    time.Time
	Status       string
}

// SpanExpect 描述 AssertSpanFields 期望匹配的 span 字段。
// Kind 为空字符串 → 跳过 Kind 校验（redis/kafka backend 必传 ""）。
// Attributes 走子集匹配：期望 attrs 必须出现且值相等，多余 attrs 忽略。
type SpanExpect struct {
	Name       string
	Kind       string // "" = skip
	Attributes map[string]string
}

// AssertSpanFields 断言 spans 中至少有一个 SpanRecord 满足 want 的 name+attributes。
func AssertSpanFields(t *testing.T, spans []SpanRecord, want SpanExpect) {
	t.Helper()
	for _, s := range spans {
		if s.Name != want.Name {
			continue
		}
		if want.Kind != "" && s.Kind != want.Kind {
			continue
		}
		matched := true
		for k, v := range want.Attributes {
			if s.Attributes[k] != v {
				matched = false
				break
			}
		}
		if matched {
			return
		}
	}
	t.Fatalf("AssertSpanFields: no span matches name=%q attrs=%v in %d fetched spans",
		want.Name, want.Attributes, len(spans))
}

// AssertTraceContextLoose 降级断言：spans 中至少有一条 TraceID 等于 want。
// 用于 redis/kafka backend（自定义 exporter 不写 ParentSpanID）。
func AssertTraceContextLoose(t *testing.T, spans []SpanRecord, traceID string) {
	t.Helper()
	for _, s := range spans {
		if s.TraceID == traceID {
			return
		}
	}
	t.Fatalf("AssertTraceContextLoose: no span has TraceID=%q in %d fetched spans",
		traceID, len(spans))
}

// FetchSpansByTraceID 拉取指定 trace_id 的所有 SpanRecord。
// backend 决定查询端点（jaeger HTTP / redis XRange / kafka consumer pull）。
// 返回空切片（不报错）表示 backend 暂未 flush，可由调用方重试。
func FetchSpansByTraceID(t *testing.T, backend, driver, topology string, traceID trace.TraceID) []SpanRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	switch backend {
	case BackendJaeger:
		return fetchJaegerSpans(ctx, traceID)
	case BackendRedisStream:
		return fetchRedisStreamSpans(ctx, driver, topology, traceID)
	case BackendKafkaTopic:
		return fetchKafkaTopicSpans(ctx, driver, topology, traceID)
	default:
		t.Fatalf("FetchSpansByTraceID: unknown backend %q", backend)
		return nil
	}
}

func fetchJaegerSpans(ctx context.Context, want trace.TraceID) []SpanRecord {
	url := fmt.Sprintf("http://localhost:16686/api/traces/%s", want.String())
	req, err := newJaegerRequest(ctx, url)
	if err != nil {
		return nil
	}
	resp, err := httpDefaultDo(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}
	var body struct {
		Data []struct {
			Spans []struct {
				TraceID       string                        `json:"traceID"`
				SpanID        string                        `json:"spanID"`
				ParentSpanID  string                        `json:"parentSpanID"`
				OperationName string                        `json:"operationName"`
				Tags          []struct{ Key, Value string } `json:"tags"`
			} `json:"spans"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil
	}
	var out []SpanRecord
	for _, d := range body.Data {
		for _, s := range d.Spans {
			rec := SpanRecord{
				TraceID:      s.TraceID,
				SpanID:       s.SpanID,
				ParentSpanID: s.ParentSpanID,
				Name:         s.OperationName,
				Attributes:   map[string]string{},
			}
			for _, tag := range s.Tags {
				rec.Attributes[tag.Key] = tag.Value
			}
			out = append(out, rec)
		}
	}
	return out
}

func fetchRedisStreamSpans(ctx context.Context, driver, topology string, want trace.TraceID) []SpanRecord {
	stream := fmt.Sprintf("trace:%s:%s", driver, topology)
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer func() { _ = client.Close() }()
	res, err := client.XRange(ctx, stream, "-", "+").Result()
	if err != nil {
		return nil
	}
	var out []SpanRecord
	for _, msg := range res {
		payload, ok := msg.Values["data"].(string)
		if !ok {
			continue
		}
		var rec struct {
			TraceID string            `json:"trace_id"`
			SpanID  string            `json:"span_id"`
			Name    string            `json:"name"`
			Kind    string            `json:"kind"`
			Attrs   map[string]string `json:"attributes"`
		}
		if err := json.Unmarshal([]byte(payload), &rec); err != nil {
			continue
		}
		if rec.TraceID != want.String() {
			continue
		}
		out = append(out, SpanRecord{
			TraceID:    rec.TraceID,
			SpanID:     rec.SpanID,
			Name:       rec.Name,
			Kind:       rec.Kind,
			Attributes: rec.Attrs,
		})
	}
	return out
}

func fetchKafkaTopicSpans(ctx context.Context, driver, topology string, want trace.TraceID) []SpanRecord {
	topic := fmt.Sprintf("trace-spans-%s", driver)
	sc := sarama.NewConfig()
	sc.Version = sarama.V2_8_0_0
	sc.Consumer.Offsets.Initial = sarama.OffsetOldest
	sc.ClientID = fmt.Sprintf("assert-trace-fetch-%d", time.Now().UnixNano())

	consumer, err := sarama.NewConsumer([]string{kafkaTraceBrokers}, sc)
	if err != nil {
		return nil
	}
	defer func() { _ = consumer.Close() }()
	pc, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		return nil
	}
	defer func() { _ = pc.Close() }()

	readCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var out []SpanRecord
	for {
		select {
		case msg := <-pc.Messages():
			if msg == nil {
				continue
			}
			var rec struct {
				TraceID string            `json:"trace_id"`
				SpanID  string            `json:"span_id"`
				Name    string            `json:"name"`
				Attrs   map[string]string `json:"attributes"`
			}
			if err := json.Unmarshal(msg.Value, &rec); err != nil {
				continue
			}
			if rec.TraceID != want.String() {
				continue
			}
			out = append(out, SpanRecord{
				TraceID:    rec.TraceID,
				SpanID:     rec.SpanID,
				Name:       rec.Name,
				Attributes: rec.Attrs,
			})
		case <-readCtx.Done():
			return out
		}
	}
}
