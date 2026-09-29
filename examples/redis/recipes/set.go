// cachex/redis - Set 命令案例
package redisrecipes

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type SetExamples struct{ rdb redis.UniversalClient }

func NewSetExamples(rdb redis.UniversalClient) *SetExamples {
	return &SetExamples{rdb: rdb}
}

// ExampleBasic SADD/SMEMBERS/SISMEMBER/SREM/SCARD
func (e *SetExamples) ExampleBasic(ctx context.Context) error {
	key := "set:article:tags:42"
	e.rdb.Del(ctx, key)
	// SADD 添加成员
	e.rdb.SAdd(ctx, key, "go", "redis", "cache", "go")
	// SMEMBERS 获取全部
	members, _ := e.rdb.SMembers(ctx, key).Result()
	fmt.Printf("SMEMBERS = %v\n", members)
	// SISMEMBER 判断成员
	isMember, _ := e.rdb.SIsMember(ctx, key, "redis").Result()
	fmt.Printf("SISMEMBER redis = %v\n", isMember)
	// SCARD 集合大小
	card, _ := e.rdb.SCard(ctx, key).Result()
	fmt.Printf("SCARD = %d\n", card)
	// SREM 移除
	e.rdb.SRem(ctx, key, "go")
	left, _ := e.rdb.SCard(ctx, key).Result()
	fmt.Printf("after SREM go → SCARD = %d\n", left)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleSetOps 交集/并集/差集 — 社交推荐、标签聚合
func (e *SetExamples) ExampleSetOps(ctx context.Context) error {
	a := "set:follow:1001" // 用户 1001 关注的人
	b := "set:follow:1002" // 用户 1002 关注的人
	e.rdb.Del(ctx, a, b)
	e.rdb.SAdd(ctx, a, "user-A", "user-B", "user-C")
	e.rdb.SAdd(ctx, b, "user-B", "user-C", "user-D")
	// 共同关注（交集）
	inter, _ := e.rdb.SInter(ctx, a, b).Result()
	fmt.Printf("SINTER = %v（共同关注）\n", inter)
	// 合并关注（并集）
	union, _ := e.rdb.SUnion(ctx, a, b).Result()
	fmt.Printf("SUNION = %v\n", union)
	// 1001 独有的关注（差集）
	diff, _ := e.rdb.SDiff(ctx, a, b).Result()
	fmt.Printf("SDIFF = %v（可能感兴趣的人）\n", diff)
	e.rdb.Del(ctx, a, b)
	return nil
}

// ExampleSRandMemberSPop 随机抽取 — 抽奖、随机推荐
func (e *SetExamples) ExampleSRandMemberSPop(ctx context.Context) error {
	key := "set:prize:lottery"
	e.rdb.Del(ctx, key)
	prizes := []string{"iPhone", "AirPods", "优惠券", "谢谢参与"}
	// SAdd 需要 []interface{}，逐元素展开
	members := make([]interface{}, len(prizes))
	for i, v := range prizes {
		members[i] = v
	}
	e.rdb.SAdd(ctx, key, members...)
	// SRANDMEMBER 随机抽取 N 个（不弹出）
	srand, _ := e.rdb.SRandMemberN(ctx, key, 2).Result()
	fmt.Printf("SRANDMEMBER N=2 = %v\n", srand)
	// SPOP 随机弹出（不放回）
	val, _ := e.rdb.SPop(ctx, key).Result()
	fmt.Printf("SPOP = %s（已取出，奖品池减少）\n", val)
	left, _ := e.rdb.SMembers(ctx, key).Result()
	fmt.Printf("剩余奖品 = %v\n", left)
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleSSCAN 大集合游标遍历
func (e *SetExamples) ExampleSSCAN(ctx context.Context) error {
	key := "set:large:users"
	e.rdb.Del(ctx, key)
	for i := 0; i < 100; i++ {
		e.rdb.SAdd(ctx, key, fmt.Sprintf("user:%d", i))
	}
	var cursor uint64
	count := 0
	for {
		members, next, _ := e.rdb.SScan(ctx, key, cursor, "*", 20).Result()
		count += len(members)
		cursor = uint64(next)
		if cursor == 0 {
			break
		}
	}
	fmt.Printf("SSCAN 遍历完成，共 %d 个成员\n", count)
	e.rdb.Del(ctx, key)
	return nil
}
