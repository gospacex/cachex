// cachex/redis - Bitmap 位操作案例
// 适合：签到系统、在线状态、布隆过滤器底层、特征标记
package redisrecipes

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type BitmapExamples struct{ rdb redis.UniversalClient }

func NewBitmapExamples(rdb redis.UniversalClient) *BitmapExamples {
	return &BitmapExamples{rdb: rdb}
}

// ExampleSignIn 签到系统：一天一个 bit，offset=day-1
func (e *BitmapExamples) ExampleSignIn(ctx context.Context) error {
	key := "bitmap:signin:202609:user:1001"
	e.rdb.Del(ctx, key)

	days := []int{1, 3, 5, 7, 8, 10} // 签到的日期
	for _, d := range days {
		e.rdb.SetBit(ctx, key, int64(d-1), 1)
	}

	// 检查某日是否签到
	for _, d := range []int{1, 2, 5, 9} {
		v, _ := e.rdb.GetBit(ctx, key, int64(d-1)).Result()
		fmt.Printf("  9月%d日签到 = %d\n", d, v)
	}
	// 统计本月签到天数
	cnt, _ := e.rdb.BitCount(ctx, key, nil).Result()
	fmt.Printf("  本月签到天数 = %d\n", cnt)

	// BitOp AND：两个用户共同签到日（交集）
	key2 := "bitmap:signin:202609:user:1002"
	e.rdb.Del(ctx, key2)
	e.rdb.SetBit(ctx, key2, 0, 1) // 1日
	e.rdb.SetBit(ctx, key2, 2, 1) // 3日
	e.rdb.SetBit(ctx, key2, 9, 1) // 10日
	e.rdb.BitOpAnd(ctx, "bitmap:signin:common", key, key2)
	common, _ := e.rdb.BitCount(ctx, "bitmap:signin:common", nil).Result()
	fmt.Printf("  共同签到天数 = %d\n", common)

	e.rdb.Del(ctx, key, key2, "bitmap:signin:common")
	return nil
}

// ExampleOnlineStatus 在线状态：offset=userId，1=在线，0=离线
func (e *BitmapExamples) ExampleOnlineStatus(ctx context.Context) error {
	key := "bitmap:online:room:chat01"
	e.rdb.Del(ctx, key)

	// 模拟 100 人上线
	for i := 0; i < 100; i++ {
		if i == 7 || i == 23 || i == 55 {
			continue // 这 3 人未上线
		}
		e.rdb.SetBit(ctx, key, int64(i), 1)
	}
	// 统计在线人数
	online, _ := e.rdb.BitCount(ctx, key, nil).Result()
	fmt.Printf("  在线人数 = %d / 100\n", online)
	// 检查特定用户
	for _, uid := range []int{7, 23, 50} {
		v, _ := e.rdb.GetBit(ctx, key, int64(uid)).Result()
		fmt.Printf("  用户 %d 在线 = %v\n", uid, v == 1)
	}
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleBitPos 查找连续位：找到第一个指定值的 bit 位置
func (e *BitmapExamples) ExampleBitPos(ctx context.Context) error {
	key := "bitmap:slots"
	e.rdb.Del(ctx, key)
	// 设置第 0、1、2、5、6 位为 1
	for _, i := range []int{0, 1, 2, 5, 6} {
		e.rdb.SetBit(ctx, key, int64(i), 1)
	}
	// 找第一个 0 位（从 offset=0 开始）
	pos, _ := e.rdb.BitPos(ctx, key, 0, 0, -1).Result()
	fmt.Printf("  第一个空闲槽位 = offset %d\n", pos)
	// 找第一个 1 位
	pos1, _ := e.rdb.BitPos(ctx, key, 1, 0, -1).Result()
	fmt.Printf("  第一个占用槽位 = offset %d\n", pos1)
	e.rdb.Del(ctx, key)
	return nil
}
