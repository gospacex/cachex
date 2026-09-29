// cachex/memcached - Compare-And-Swap 并发安全更新案例
package memcachedrecipes

import (
	"context"
	"fmt"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
)

type CasExamples struct{ mc *memcache.Client }

func NewCasExamples(mc *memcache.Client) *CasExamples {
	return &CasExamples{mc: mc}
}

// ExampleCAS 标准 CAS 流程：Get → 取 CasID → CompareAndSwap
func (e *CasExamples) ExampleCAS(ctx context.Context) error {
	key := "cas:account:btc:001"
	e.mc.Delete(key)
	// 初始化账户余额
	e.mc.Set(&memcache.Item{
		Key:        key,
		Value:      []byte("1000"),
		Expiration: 0,
	})
	// 第一步：读取当前值和 CasID
	item, err := e.mc.Get(key)
	if err != nil {
		return fmt.Errorf("Get: %w", err)
	}
	oldVal := string(item.Value)
	fmt.Printf("当前余额 = %s（casID=%d）\n", oldVal, item.CasID)
	// 模拟业务：从余额中支出 200
	newVal := fmt.Sprintf("%d", 1000-200)
	// CAS 更新（携带 CasID，只有 value 未变时才写入成功）
	err = e.mc.CompareAndSwap(&memcache.Item{
		Key:        key,
		Value:      []byte(newVal),
		CasID:      item.CasID, // 必须与 Get 时拿到的 CasID 一致
		Expiration: 0,
	})
	if err == memcache.ErrCASConflict {
		fmt.Println("CAS 冲突：期间有其他请求修改了该 key，需要重试")
	} else if err != nil {
		return fmt.Errorf("CAS: %w", err)
	} else {
		fmt.Printf("CAS 成功：余额从 %s → %s\n", oldVal, newVal)
	}
	// 验证最终值
	final, _ := e.mc.Get(key)
	fmt.Printf("最终余额 = %s\n", final.Value)
	e.mc.Delete(key)
	return nil
}

// ExampleCASRetry CAS 冲突重试循环
func (e *CasExamples) ExampleCASRetry(ctx context.Context) error {
	key := "cas:inventory:sku:999"
	e.mc.Delete(key)
	e.mc.Set(&memcache.Item{
		Key:        key,
		Value:      []byte("50"),
		Expiration: 0,
	})
	// 模拟并发竞争：两次同时读取、修改、写入
	go func() {
		item, _ := e.mc.Get(key)
		time.Sleep(50 * time.Millisecond)
		e.mc.CompareAndSwap(&memcache.Item{
			Key:   key,
			Value: []byte(fmt.Sprintf("%d", 50-1)),
			CasID: item.CasID,
		})
	}()
	go func() {
		item, _ := e.mc.Get(key)
		time.Sleep(30 * time.Millisecond)
		e.mc.CompareAndSwap(&memcache.Item{
			Key:   key,
			Value: []byte(fmt.Sprintf("%d", 50-2)),
			CasID: item.CasID,
		})
	}()
	// 主线程等待
	time.Sleep(200 * time.Millisecond)
	final, _ := e.mc.Get(key)
	fmt.Printf("CAS 竞争后库存 = %s（可能被覆盖，视时序而定）\n", final.Value)
	e.mc.Delete(key)
	return nil
}
