// cachex/redis - String 高级命令案例
// 通过 Raw() 返回的 redis.UniversalClient 使用，不经过熔断器/追踪。
package redisrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// StringExamples 演示 go-redis/v8 String 类型的所有常见操作。
// 调用方先通过 cachex 拿到驱动 Client，再调用 client.Raw() 获得句柄。
type StringExamples struct{ rdb redis.UniversalClient }

func NewStringExamples(rdb redis.UniversalClient) *StringExamples {
	return &StringExamples{rdb: rdb}
}

// ExampleBasic 基础 GET/SET/DEL/EXISTS
func (e *StringExamples) ExampleBasic(ctx context.Context) error {
	key := "string:basic:user:1001"
	// Set 带 TTL（24h）
	if err := e.rdb.Set(ctx, key, `{"name":"Alice","age":30}`, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("Set: %w", err)
	}
	// Get
	val, err := e.rdb.Get(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("Get: %w", err)
	}
	fmt.Printf("GET %s = %s\n", key, val)
	// Exists
	ok, _ := e.rdb.Exists(ctx, key).Result()
	fmt.Printf("EXISTS %s = %v\n", key, ok > 0)
	// Delete
	if err := e.rdb.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("Del: %w", err)
	}
	fmt.Printf("DEL %s done\n", key)
	return nil
}

// ExampleSetOptions SET 条件写入：NX（不存在才设）/ XX（存在才设）/ EX/PX
func (e *StringExamples) ExampleSetOptions(ctx context.Context) error {
	// NX + EX：仅当 key 不存在时才写入，并设过期时间（常用分布式锁模式）
	ok, err := e.rdb.SetNX(ctx, "lock:task:1", "worker-a", 10*time.Second).Result()
	if err != nil {
		return fmt.Errorf("SetNX: %w", err)
	}
	fmt.Printf("SETNX lock:task:1 = %v\n", ok)
	// XX：仅当 key 已存在时才覆盖
	ok2, err := e.rdb.SetXX(ctx, "lock:task:1", "worker-b", 5*time.Second).Result()
	fmt.Printf("SETXX lock:task:1 = %v（已持有者才会被覆盖）\n", ok2)
	return nil
}

// ExampleMGetMSet 批量读写，减少网络 RTT
func (e *StringExamples) ExampleMGetMSet(ctx context.Context) error {
	keys := []string{"k:a", "k:b", "k:c"}
	// MSET 原子批量写入
	if err := e.rdb.MSet(ctx, "k:a", "va", "k:b", "vb", "k:c", "vc").Err(); err != nil {
		return fmt.Errorf("MSet: %w", err)
	}
	// MGET 批量读取
	vals, err := e.rdb.MGet(ctx, keys...).Result()
	if err != nil {
		return fmt.Errorf("MGet: %w", err)
	}
	for i, v := range vals {
		fmt.Printf("MGet[%d] = %v\n", i, v)
	}
	// 清理
	e.rdb.Del(ctx, keys...)
	return nil
}

// ExampleIncrDecr 原子计数，适合限流、秒杀库存、计数器
func (e *StringExamples) ExampleIncrDecr(ctx context.Context) error {
	key := "counter:page:view"
	// 自增
	n, _ := e.rdb.Incr(ctx, key).Result()
	fmt.Printf("INCR %s = %d\n", key, n)
	// 自增指定步长
	n2, _ := e.rdb.IncrBy(ctx, key, 10).Result()
	fmt.Printf("INCRBY %s 10 = %d\n", key, n2)
	// 自减
	n3, _ := e.rdb.Decr(ctx, key).Result()
	fmt.Printf("DECR %s = %d\n", key, n3)
	// 浮点自增（用于金额类场景）
	f, _ := e.rdb.IncrByFloat(ctx, key, -0.5).Result()
	fmt.Printf("INCRBYFLOAT %s -0.5 = %.1f\n", key, f)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleAppendStrLen 字符串拼接与长度
func (e *StringExamples) ExampleAppendStrLen(ctx context.Context) error {
	key := "string:log"
	e.rdb.Del(ctx, key)
	// Append 追加
	newLen, _ := e.rdb.Append(ctx, key, "hello").Result()
	fmt.Printf("APPEND %s 'hello' → len=%d\n", key, newLen)
	newLen, _ = e.rdb.Append(ctx, key, " world").Result()
	fmt.Printf("APPEND %s ' world' → len=%d\n", key, newLen)
	// StrLen
	l, _ := e.rdb.StrLen(ctx, key).Result()
	fmt.Printf("STRLEN %s = %d\n", key, l)
	// GetRange 获取子串
	sub, _ := e.rdb.GetRange(ctx, key, 0, 4).Result()
	fmt.Printf("GETRANGE %s [0:5] = %s\n", key, sub)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleBitOperations Bitmap：签到、在线状态、特征向量
func (e *StringExamples) ExampleBitOperations(ctx context.Context) error {
	key := "bitmap:signin:202609" // 2026年9月签到
	// 清空
	e.rdb.Del(ctx, key)
	// 9月1日签到（offset=0）
	e.rdb.SetBit(ctx, key, 0, 1)
	// 9月3日签到（offset=2）
	e.rdb.SetBit(ctx, key, 2, 1)
	// 查询某日是否签到
	v, _ := e.rdb.GetBit(ctx, key, 0).Result()
	fmt.Printf("9月1日签到 = %d\n", v)
	v, _ = e.rdb.GetBit(ctx, key, 1).Result()
	fmt.Printf("9月2日签到 = %d\n", v)
	// 统计总签到天数
	cnt, _ := e.rdb.BitCount(ctx, key, nil).Result()
	fmt.Printf("本月签到天数 = %d\n", cnt)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleHyperLogLog HyperLogLog 基数统计，适合 UV 去重计数
func (e *StringExamples) ExampleHyperLogLog(ctx context.Context) error {
	key := "hll:uv:page:index"
	// 添加访问记录（每个用户 ID 一条）
	for i := 0; i < 100; i++ {
		e.rdb.PFAdd(ctx, key, fmt.Sprintf("user:%d", i))
	}
	// 添加重复数据（不影响结果，概率误差 ~0.81%）
	for i := 0; i < 50; i++ {
		e.rdb.PFAdd(ctx, key, fmt.Sprintf("user:%d", i))
	}
	cnt, _ := e.rdb.PFCount(ctx, key).Result()
	fmt.Printf("UV 估算值（含50条重复）= %d\n", cnt)
	e.rdb.Del(ctx, key)
	return nil
}
