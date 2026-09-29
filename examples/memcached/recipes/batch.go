// cachex/memcached - GetMulti / FlushAll / Stats 案例
package memcachedrecipes

import (
	"context"
	"fmt"

	"github.com/bradfitz/gomemcache/memcache"
)

type BatchExamples struct{ mc *memcache.Client }

func NewBatchExamples(mc *memcache.Client) *BatchExamples {
	return &BatchExamples{mc: mc}
}

// ExampleGetMulti 批量读取多个 key，一次请求减少网络 RTT
func (e *BatchExamples) ExampleGetMulti(ctx context.Context) error {
	// 先写入若干 key
	keys := []string{"batch:a", "batch:b", "batch:c"}
	for i, k := range keys {
		e.mc.Set(&memcache.Item{
			Key:   k,
			Value: []byte(fmt.Sprintf("value-%d", i)),
		})
	}
	// 批量读取
	items, err := e.mc.GetMulti(keys)
	if err != nil {
		return fmt.Errorf("GetMulti: %w", err)
	}
	fmt.Println("GetMulti 结果:")
	for _, k := range keys {
		if item, ok := items[k]; ok {
			fmt.Printf("  %s = %s\n", k, item.Value)
		} else {
			fmt.Printf("  %s = MISS\n", k)
		}
	}
	// 清理
	for _, k := range keys {
		e.mc.Delete(k)
	}
	return nil
}

// ExampleFlushAll 清空所有数据（仅测试/调试环境使用）
func (e *BatchExamples) ExampleFlushAll(ctx context.Context) error {
	// 注意：FlushAll 是危险操作，会清除整台 memcached 服务器的所有 key
	// 生产环境严禁调用，此处仅做演示
	// e.mc.FlushAll()
	fmt.Println("ExampleFlushAll: 注释掉的演示代码，生产环境请勿调用 FlushAll")
	return nil
}

// ExampleStats 查询服务器统计信息
func (e *BatchExamples) ExampleStats(ctx context.Context) error {
	// 方式一：获取各服务器的连接状态（ping 检查）
	err := e.mc.Ping()
	if err != nil {
		fmt.Printf("Ping 失败: %v\n", err)
	} else {
		fmt.Println("Ping: memcached 连接正常")
	}
	// 方式二：通过 GetStats 获取详细统计（返回 map[string]map[string]string）
	// 注意：此版本 gomemcache 提供的是 ServerStats 方法
	// 以下为常见用法示意：
	// stats, err := e.mc.GetStats("slabs", "size", "get", "cmd", "bytes", "curr_items")
	// for addr, s := range stats {
	//     fmt.Printf("%s: %+v\n", addr, s)
	// }
	fmt.Println("Stats: 查看实际可用字段请参考 memcache.ServerStats")
	return nil
}
