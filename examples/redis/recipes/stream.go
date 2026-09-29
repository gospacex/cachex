// cachex/redis - Stream 消息流案例
// 适合：事件溯源、日志聚合、MQ 队列、削峰填谷
package redisrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type StreamExamplesV2 struct{ rdb redis.UniversalClient }

func NewStreamExamplesV2(rdb redis.UniversalClient) *StreamExamplesV2 {
	return &StreamExamplesV2{rdb: rdb}
}

// ExampleXAddXRead XADD 写入 + XREAD 读取最新消息
func (e *StreamExamplesV2) ExampleXAddXRead(ctx context.Context) error {
	stream := "stream:events:order:v2"
	e.rdb.Del(ctx, stream)

	id1, _ := e.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"user": "u1", "action": "create", "amount": "99"},
	}).Result()
	id2, _ := e.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"user": "u2", "action": "pay", "amount": "199"},
	}).Result()
	id3, _ := e.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"user": "u1", "action": "refund", "amount": "99"},
	}).Result()
	fmt.Printf("XADD 写入 3 条: %s, %s, %s\n", id1, id2, id3)

	msgs, _ := e.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{stream},
		Block:   0,
		Count:   10,
	}).Result()
	for _, g := range msgs {
		fmt.Printf("  Stream=%s entries=%d\n", g.Stream, len(g.Messages))
		for _, m := range g.Messages {
			fmt.Printf("    ID=%s data=%v\n", m.ID, m.Values)
		}
	}
	n, _ := e.rdb.XLen(ctx, stream).Result()
	fmt.Printf("  XLEN = %d\n", n)
	rangeMsgs, _ := e.rdb.XRange(ctx, stream, "-", "+").Result()
	fmt.Printf("  XRANGE 全部 %d 条\n", len(rangeMsgs))
	e.rdb.Del(ctx, stream)
	return nil
}

// ExampleXReadGroup XREADGROUP 消费组模式（生产者-消费者）
func (e *StreamExamplesV2) ExampleXReadGroup(ctx context.Context) error {
	stream := "stream:orders:queue"
	group := "grp:workers"
	e.rdb.Del(ctx, stream)
	e.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()

	for i := 1; i <= 5; i++ {
		e.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"order_id": fmt.Sprintf("ORD-%d", i), "amount": i * 100},
		})
	}
	fmt.Println("生产者写入 5 条订单")

	msgs, _ := e.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "c1",
		Streams:  []string{stream, "0"},
		Count:    3,
		Block:    500 * time.Millisecond,
	}).Result()
	for _, g := range msgs {
		for _, m := range g.Messages {
			fmt.Printf("  消费者 c1 收到: ID=%s data=%v\n", m.ID, m.Values)
		}
		ids := make([]string, len(g.Messages))
		for i, m := range g.Messages {
			ids[i] = m.ID
		}
		e.rdb.XAck(ctx, stream, group, ids...)
		fmt.Printf("  XACK 确认 %d 条\n", len(ids))
	}

	msgs2, _ := e.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "c2",
		Streams:  []string{stream, "0"},
		Count:    10,
		Block:    0,
	}).Result()
	for _, g := range msgs2 {
		for _, m := range g.Messages {
			fmt.Printf("  消费者 c2 收到: ID=%s data=%v\n", m.ID, m.Values)
		}
	}

	info, _ := e.rdb.XInfoGroups(ctx, stream).Result()
	for _, g := range info {
		fmt.Printf("  Group=%s pending=%d consumers=%d\n", g.Name, g.Pending, g.Consumers)
	}

	e.rdb.XGroupDestroy(ctx, stream, group)
	e.rdb.Del(ctx, stream)
	return nil
}

// ExampleXPending Pending 消息重投（处理失败的消息）
func (e *StreamExamplesV2) ExampleXPending(ctx context.Context) error {
	stream := "stream:jobs:retry"
	group := "grp:retry-workers"
	e.rdb.Del(ctx, stream)
	e.rdb.XGroupCreateMkStream(ctx, stream, group, "0").Err()

	for i := 1; i <= 3; i++ {
		e.rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: stream,
			Values: map[string]interface{}{"job": fmt.Sprintf("job-%d", i)},
		})
	}

	// 读取但不 ACK（模拟处理失败）
	msgs, _ := e.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    group,
		Consumer: "c1",
		Streams:  []string{stream, "0"},
		Count:    3,
		Block:    0,
	}).Result()
	var pendingIDs []string
	for _, g := range msgs {
		for _, m := range g.Messages {
			pendingIDs = append(pendingIDs, m.ID)
		}
	}
	fmt.Printf("  c1 读取但未 ACK: %d 条\n", len(pendingIDs))

	pending, _ := e.rdb.XPending(ctx, stream, group).Result()
	fmt.Printf("  XPENDING: 队列中有 %d 条待处理消息\n", pending.Count)

	// XCLAIM 转移给 c2 重处理
	if len(pendingIDs) > 0 {
		e.rdb.XClaim(ctx, &redis.XClaimArgs{
			Stream:   stream,
			Group:    group,
			Consumer: "c2",
			MinIdle:  time.Second,
			Messages: pendingIDs,
		})
		fmt.Printf("  XCLAIM 已转移 %d 条消息给 c2\n", len(pendingIDs))
	}

	// 清理
	e.rdb.XGroupDestroy(ctx, stream, group)
	e.rdb.Del(ctx, stream)
	return nil
}
