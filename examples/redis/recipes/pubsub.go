// cachex/redis - PubSub 发布订阅案例
// 适合：实时通知、配置热更新、事件广播
package redisrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type PubSubExamples struct{ rdb redis.UniversalClient }

func NewPubSubExamples(rdb redis.UniversalClient) *PubSubExamples {
	return &PubSubExamples{rdb: rdb}
}

// ExamplePublishSubscribe 简单发布/订阅
func (e *PubSubExamples) ExamplePublishSubscribe(ctx context.Context) error {
	ch := "channel:notifications"
	e.rdb.Del(ctx, ch)

	// 订阅者：开启 goroutine 监听
	sub := e.rdb.Subscribe(ctx, ch)
	defer sub.Close()

	received := make(chan *redis.Message, 5)
	go func() {
		for {
			msg, err := sub.ReceiveMessage(ctx)
			if err != nil {
				return
			}
			received <- msg
		}
	}()

	// 给订阅者一点时间建立连接
	time.Sleep(50 * time.Millisecond)

	// 发布者发送 3 条消息
	topics := []string{"order created", "payment done", "shipping started"}
	for _, t := range topics {
		e.rdb.Publish(ctx, ch, t)
	}
	fmt.Println("发布者发送 3 条消息")

	// 收集收到的消息
	for i := 0; i < 3; i++ {
		select {
		case msg := <-received:
			fmt.Printf("  订阅者收到 [%s]: %s\n", msg.Channel, msg.Payload)
		case <-time.After(2 * time.Second):
			fmt.Println("  超时等待消息")
		}
	}
	return nil
}

// ExamplePatternSubscribe 模式订阅（PSubscribe）匹配多频道
func (e *PubSubExamples) ExamplePatternSubscribe(ctx context.Context) error {
	// 发布者在多个 channel 发广播
	channels := []string{"evt:user:login", "evt:user:logout", "evt:order:create"}
	for _, ch := range channels {
		e.rdb.Publish(ctx, ch, "event fired")
	}

	// 订阅者用模式匹配所有 evt:* 频道
	sub := e.rdb.PSubscribe(ctx, "evt:*")
	defer sub.Close()

	received := make(chan *redis.Message, 5)
	go func() {
		for {
			msg, err := sub.ReceiveMessage(ctx)
			if err != nil {
				return
			}
			received <- msg
		}
	}()
	time.Sleep(50 * time.Millisecond)

	// 再次发送
	for _, ch := range channels {
		e.rdb.Publish(ctx, ch, "another event")
	}
	fmt.Println("模式订阅 evt:* 收到:")
	for i := 0; i < 3; i++ {
		select {
		case msg := <-received:
			fmt.Printf("  [%s] pattern=%s payload=%s\n", msg.Channel, msg.Pattern, msg.Payload)
		case <-time.After(2 * time.Second):
			break
		}
	}
	return nil
}

// ExamplePubSubNumSub 查询订阅者数量
func (e *PubSubExamples) ExamplePubSubNumSub(ctx context.Context) error {
	ch := "channel:metrics"
	// 订阅一次（产生订阅者计数）
	sub := e.rdb.Subscribe(ctx, ch)
	defer sub.Close()
	time.Sleep(50 * time.Millisecond)

	num, _ := e.rdb.PubSubNumSub(ctx, ch).Result()
	fmt.Printf("  channel %q 当前订阅者数 = %d\n", ch, num[ch])

	numPat, _ := e.rdb.PubSubNumPat(ctx).Result()
	fmt.Printf("  当前模式订阅总数 = %d\n", numPat)
	return nil
}
