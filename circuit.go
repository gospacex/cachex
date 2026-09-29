// cachex/circuit.go — 熔断器封装
package cachex

import (
	"context"
	"time"

	"github.com/sony/gobreaker"
)

// CircuitBreaker 封装 gobreaker，提供类型安全的熔断器操作
type CircuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

// NewCircuitBreaker 创建默认配置的熔断器。
//
// 参数显式指定而非依赖 gobreaker 默认值（其默认 Interval=0 即不清零统计、
// Timeout=60s），语义如下：
//
//	MaxRequests = 1     半开状态只允许 1 个探测请求，成功即闭合
//	Interval    = 30s   统计按 30 秒滚动窗口重置，避免陈旧失败影响当前判定
//	Timeout     = 30s   跳闸后等待 30 秒转入半开，探测下游是否恢复
//	ReadyToTrip         连续失败达到 5 次即跳闸
func NewCircuitBreaker(name string) *CircuitBreaker {
	settings := gobreaker.Settings{
		Name:        name,
		MaxRequests: 1,
		Interval:    30 * time.Second,
		Timeout:     30 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 5
		},
	}
	return &CircuitBreaker{
		cb: gobreaker.NewCircuitBreaker(settings),
	}
}

// Execute 在熔断器保护下执行 fn。
// 若 ctx 已取消，直接返回 ctx.Err()，不再占用熔断器的一次执行配额；
// ctx 为 nil 时按"不校验取消"处理（兼容历史调用）。
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	_, err := cb.cb.Execute(func() (interface{}, error) {
		if err := fn(); err != nil {
			return nil, err
		}
		return nil, nil
	})
	return err
}

// State 返回当前熔断器状态
// 0=Closed, 1=Open, 2=Half-Open
func (cb *CircuitBreaker) State() int {
	switch cb.cb.State() {
	case gobreaker.StateClosed:
		return 0
	case gobreaker.StateOpen:
		return 1
	case gobreaker.StateHalfOpen:
		return 2
	default:
		return -1
	}
}
