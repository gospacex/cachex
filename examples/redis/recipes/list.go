// cachex/redis - List 命令案例
package redisrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type ListExamples struct{ rdb redis.UniversalClient }

func NewListExamples(rdb redis.UniversalClient) *ListExamples {
	return &ListExamples{rdb: rdb}
}

// ExampleBasic LPUSH/RPUSH/LPOP/RPOP/LRANGE
func (e *ListExamples) ExampleBasic(ctx context.Context) error {
	key := "list:tasks"
	e.rdb.Del(ctx, key)
	// RPUSH 入队
	e.rdb.RPush(ctx, key, "task-1", "task-2", "task-3")
	// LLEN
	n, _ := e.rdb.LLen(ctx, key).Result()
	fmt.Printf("LLEN = %d\n", n)
	// LRANGE 查看全部
	items, _ := e.rdb.LRange(ctx, key, 0, -1).Result()
	fmt.Printf("LRANGE = %v\n", items)
	// LINDEX 按索引
	item, _ := e.rdb.LIndex(ctx, key, 1).Result()
	fmt.Printf("LINDEX[1] = %s\n", item)
	// RPOP 出队
	val, _ := e.rdb.RPop(ctx, key).Result()
	fmt.Printf("RPOP = %s\n", val)
	// LTRIM 裁剪保留前2条
	e.rdb.LTrim(ctx, key, 0, 1)
	remain, _ := e.rdb.LRange(ctx, key, 0, -1).Result()
	fmt.Printf("after LTrim [0:1] = %v\n", remain)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleBlocking BLPOP/BRPOP 阻塞式消费队列
func (e *ListExamples) ExampleBlocking(ctx context.Context) error {
	key := "list:job:queue"
	e.rdb.Del(ctx, key)
	// 生产者：RPUSH
	go func() {
		time.Sleep(500 * time.Millisecond)
		e.rdb.RPush(ctx, key, "job-a", "job-b")
	}()
	// 消费者：BLPOP 阻塞最多 3 秒
	vals, err := e.rdb.BLPop(ctx, 3*time.Second, key).Result()
	if err != nil {
		fmt.Printf("BLPOP timeout: %v\n", err)
		return nil
	}
	fmt.Printf("BLPOP key=%s val=%s\n", vals[0], vals[1])
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleRPopLPush 可靠队列（消息 ACK 模型）
// 从 src 右弹出，推入 dst 左端；消费者确认后再从 dst 删除。
func (e *ListExamples) ExampleRPopLPush(ctx context.Context) error {
	src := "list:queue:pending"
	dst := "list:queue:processing"
	e.rdb.Del(ctx, src, dst)
	e.rdb.RPush(ctx, src, "msg-1", "msg-2")
	// 取出一条进入处理中队列
	val, _ := e.rdb.RPopLPush(ctx, src, dst).Result()
	fmt.Printf("RPOPLPUSH = %s（此时 msg 在 processing 队列）\n", val)
	// 消费者处理完成后 ACK
	e.rdb.LRem(ctx, dst, 1, val)
	fmt.Printf("ACK %s done\n", val)
	e.rdb.Del(ctx, src, dst)
	return nil
}
