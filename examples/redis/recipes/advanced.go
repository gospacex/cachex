// cachex/redis - Geo / Stream / PubSub / Pipeline / Script 命令案例
package redisrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// ===== Geo 地理空间 =====

type GeoExamples struct{ rdb redis.UniversalClient }

func NewGeoExamples(rdb redis.UniversalClient) *GeoExamples {
	return &GeoExamples{rdb: rdb}
}

// ExampleGeoAdd GeoSearch 附近门店/用户查询
func (e *GeoExamples) ExampleGeoAdd(ctx context.Context) error {
	key := "geo:store:beijing"
	e.rdb.Del(ctx, key)
	// GEOADD 存储位置（经度, 纬度, member）——v8 接收指针切片
	e.rdb.GeoAdd(ctx, key,
		&redis.GeoLocation{Longitude: 116.40, Latitude: 39.90, Name: "store-A"},
		&redis.GeoLocation{Longitude: 116.45, Latitude: 39.92, Name: "store-B"},
		&redis.GeoLocation{Longitude: 116.50, Latitude: 39.88, Name: "store-C"},
	)
	// GEODIST 计算距离（单位：m）
	dist, _ := e.rdb.GeoDist(ctx, key, "store-A", "store-B", "m").Result()
	fmt.Printf("store-A ↔ store-B 距离 = %.0fm\n", dist)
	// GEOPOS 获取坐标
	pos, _ := e.rdb.GeoPos(ctx, key, "store-A").Result()
	fmt.Printf("store-A 坐标 = %v\n", pos)
	// GEORADIUS 查找 1km 内的门店（v8 用 GeoRadiusQuery）
	members, _ := e.rdb.GeoRadius(ctx, key, 116.42, 39.91, &redis.GeoRadiusQuery{
		Radius:    1,
		Unit:      "km",
		WithCoord: true,
		WithDist:  true,
	}).Result()
	fmt.Println("GEORADIUS 1km 内门店:")
	for _, m := range members {
		fmt.Printf("  %s dist=%.0fm loc=%.4f,%.4f\n", m.Name, m.Dist, m.Longitude, m.Latitude)
	}
	e.rdb.Del(ctx, key)
	return nil
}

// ===== Stream 消息流 =====

type StreamExamples struct{ rdb redis.UniversalClient }

func NewStreamExamples(rdb redis.UniversalClient) *StreamExamples {
	return &StreamExamples{rdb: rdb}
}

// ExampleXAdd 事件追加与 XREAD 消费
func (e *StreamExamples) ExampleXAdd(ctx context.Context) error {
	stream := "stream:events:order"
	e.rdb.Del(ctx, stream)
	// XADD 追加事件（* 让 Redis 自动生成 ID）
	id1, _ := e.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"user": "u1", "action": "create", "amount": "99"},
	}).Result()
	id2, _ := e.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{"user": "u2", "action": "pay", "amount": "199"},
	}).Result()
	fmt.Printf("XADD → %s, %s\n", id1, id2)
	// XREAD 读取最新消息（$ 表示从当前位置开始，只收新消息）
	msgs, _ := e.rdb.XRead(ctx, &redis.XReadArgs{
		Streams: []string{stream},
		Block:   0, // 不阻塞
		Count:   10,
	}).Result()
	for _, g := range msgs {
		fmt.Printf("Stream=%s entries=%d\n", g.Stream, len(g.Messages))
		for _, m := range g.Messages {
			fmt.Printf("  ID=%s data=%v\n", m.ID, m.Values)
		}
	}
	// XLEN 获取流长度
	n, _ := e.rdb.XLen(ctx, stream).Result()
	fmt.Printf("XLEN = %d\n", n)
	e.rdb.Del(ctx, stream)
	return nil
}

// ===== Pipeline 管道批量 =====

type PipelineExamples struct{ rdb redis.UniversalClient }

func NewPipelineExamples(rdb redis.UniversalClient) *PipelineExamples {
	return &PipelineExamples{rdb: rdb}
}

// ExamplePipeline 管道批量操作，减少网络 RTT
func (e *PipelineExamples) ExamplePipeline(ctx context.Context) error {
	keyPrefix := "pipe:"
	e.rdb.Del(ctx, keyPrefix+"a", keyPrefix+"b", keyPrefix+"c")
	// 构建管道（命令不立即执行，批量发送）
	pipe := e.rdb.Pipeline()
	pipe.Set(ctx, keyPrefix+"a", "va", 0)
	pipe.Set(ctx, keyPrefix+"b", "vb", 0)
	pipe.Set(ctx, keyPrefix+"c", "vc", 0)
	pipe.Incr(ctx, keyPrefix+"counter")
	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("pipeline exec: %w", err)
	}
	// 验证
	v, _ := e.rdb.Get(ctx, keyPrefix+"a").Result()
	fmt.Printf("PIPE Get %sa = %s\n", keyPrefix, v)
	c, _ := e.rdb.Get(ctx, keyPrefix+"counter").Result()
	fmt.Printf("PIPE INCR counter = %s\n", c)
	e.rdb.Del(ctx, keyPrefix+"a", keyPrefix+"b", keyPrefix+"c", keyPrefix+"counter")
	return nil
}

// ===== Scripting Lua 脚本 =====

type ScriptExamples struct{ rdb redis.UniversalClient }

func NewScriptExamples(rdb redis.UniversalClient) *ScriptExamples {
	return &ScriptExamples{rdb: rdb}
}

// ExampleEval 分布式锁：SET NX EX + Lua 续期/释放
func (e *ScriptExamples) ExampleEvalLock(ctx context.Context) error {
	lockKey := "lock:order:checkout:5001"
	lockVal := "token-abc123"
	// 加锁：SET NX EX
	acquired, err := e.rdb.SetNX(ctx, lockKey, lockVal, 10*time.Second).Result()
	if err != nil {
		return fmt.Errorf("SETNX: %w", err)
	}
	fmt.Printf("加锁 result = %v\n", acquired)
	if !acquired {
		fmt.Println("锁已被其他节点持有")
		return nil
	}
	// Lua 脚本原子释放：仅当 value 匹配时才删除（防误删他人锁）
	unlockScript := redis.NewScript(`
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`)
	ok, err := unlockScript.Run(ctx, e.rdb, []string{lockKey}, lockVal).Int()
	if err != nil {
		return fmt.Errorf("unlock: %w", err)
	}
	fmt.Printf("Lua 解锁 result = %d（1=成功释放，0=值不匹配）\n", ok)
	return nil
}
