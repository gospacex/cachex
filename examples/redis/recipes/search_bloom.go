// cachex/redis - RediSearch & RedisBloom 原始命令示例
//
// 注意：go-redis/v8 不封装 FT.* 和 BF.* 命令，需通过 Execute/RawCommand 发送原始命令。
// 生产环境如需类型安全封装，建议引入 redis/go-redis/extra/redisbloom/v9（本仓库网络受限，此处用 raw）。
// 本地环境使用 redis/redis-stack:8.4 镜像，内置 RediSearch + RedisBloom。
package redisrecipes

import (
	"context"
	"fmt"

	"github.com/go-redis/redis/v8"
)

type SearchBloomExamples struct{ rdb redis.UniversalClient }

func NewSearchBloomExamples(rdb redis.UniversalClient) *SearchBloomExamples {
	return &SearchBloomExamples{rdb: rdb}
}

// ExampleRediSearchCreateIndex 创建 FT.SEARCH 索引
// REDISEARCH_NOTES:
//
//	CREATE INDEX 仅在 Redis Stack 上可用，普通 Redis 会返回 ERR unknown command.
func (e *SearchBloomExamples) ExampleRediSearchCreateIndex(ctx context.Context) error {
	// 先写入几个文档（RedisHash 格式）
	docs := map[string]map[string]string{
		"doc:1": {"title": "Go Redis Client", "body": "go-redis is a Go client for Redis", "tags": "go,redis,client"},
		"doc:2": {"title": "Redis Streams", "body": "Redis Streams for message queuing", "tags": "redis,stream,mq"},
		"doc:3": {"title": "Memcached Basics", "body": "Memcached is a distributed memory caching system", "tags": "memcached,cache"},
	}
	for k, fields := range docs {
		m := make([]interface{}, 0, len(fields)*2+1)
		m = append(m, k)
		for f, v := range fields {
			m = append(m, f, v)
		}
		e.rdb.HMSet(ctx, k, m...).Err()
	}

	// FT.CREATE 创建全文索引（NUMERIC score 字段暂不支持，仅 TEXT）
	// 原始命令格式：FT.CREATE idx ON HASH PREFIX 1 doc: SCHEMA title BODY tags
	cmd := redis.NewStringCmd(ctx,
		"FT.CREATE", "idx:docs",
		"ON", "HASH",
		"PREFIX", "1", "doc:",
		"SCHEMA", "title", "TEXT", "body", "TEXT", "tags", "TEXT",
	)
	_ = e.rdb.Process(ctx, cmd)
	res, err := cmd.Result()
	if err != nil {
		fmt.Printf("  FT.CREATE 警告（非 Redis Stack 环境）: %v\n", err)
		return nil
	}
	fmt.Printf("  FT.CREATE 成功: %s\n", res)

	// FT.SEARCH 全文搜索
	searchCmd := redis.NewStringSliceCmd(ctx, "FT.SEARCH", "idx:docs", "redis*", "-LIMIT", "0", "3")
	_ = e.rdb.Process(ctx, searchCmd)
	results, _ := searchCmd.Result()
	fmt.Printf("  FT.SEARCH 'redis*' → %d 结果\n", len(results)/2) // 每两条一行（id + field）

	// 清理索引和文档
	e.rdb.Do(ctx, "FT.DROPINDEX", "idx:docs").Err()
	for k := range docs {
		e.rdb.Del(ctx, k)
	}
	return nil
}

// ExampleRedisBloomFilter BF.ADD / BF.EXISTS / BF.RESERVE
func (e *SearchBloomExamples) ExampleRedisBloomFilter(ctx context.Context) error {
	key := "bf:urls"

	// BF.RESERVE 创建布隆过滤器（容量 1000，错误率 0.01）
	reserveCmd := redis.NewStatusCmd(ctx,
		"BF.RESERVE", key, "0.01", "1000")
	_ = e.rdb.Process(ctx, reserveCmd)
	if err := reserveCmd.Err(); err != nil {
		fmt.Printf("  BF.RESERVE 警告（非 Redis Stack 环境）: %v\n", err)
		return nil
	}
	fmt.Println("  BF.RESERVE 创建成功")

	// BF.ADD 添加元素
	urls := []string{
		"https://example.com/a",
		"https://example.com/b",
		"https://example.com/c",
	}
	for _, url := range urls {
		cmd := redis.NewIntCmd(ctx, "BF.ADD", key, url)
		_ = e.rdb.Process(ctx, cmd)
		ok, _ := cmd.Result()
		fmt.Printf("  BF.ADD %s → %d\n", url, ok)
	}

	// BF.EXISTS 查询
	for _, url := range urls {
		cmd := redis.NewIntCmd(ctx, "BF.EXISTS", key, url)
		_ = e.rdb.Process(ctx, cmd)
		exists, _ := cmd.Result()
		fmt.Printf("  BF.EXISTS %s → %v\n", url, exists == 1)
	}

	// BF.EXISTS 一个未添加的 URL（可能误判，概率极低）
	cmd := redis.NewIntCmd(ctx, "BF.EXISTS", key, "https://unknown.com/z")
	_ = e.rdb.Process(ctx, cmd)
	exists, _ := cmd.Result()
	fmt.Printf("  BF.EXISTS unknown → %v（误报概率极低）\n", exists == 1)

	// 清理
	e.rdb.Del(ctx, key)
	return nil
}

// ExampleRedisBloomFilterCMS CMS 冷启统计（Count-Min Sketch）
func (e *SearchBloomExamples) ExampleRedisBloomCMS(ctx context.Context) error {
	key := "cms:pageviews"

	// CMS.INITBYDIM 初始化（宽度 500，深度 3）
	initCmd := redis.NewStringCmd(ctx, "CMS.INITBYDIM", key, "500", "3")
	_ = e.rdb.Process(ctx, initCmd)
	if err := initCmd.Err(); err != nil {
		fmt.Printf("  CMS.INITBYDIM 警告: %v\n", err)
		return nil
	}
	fmt.Println("  CMS.INITBYDIM 创建成功")

	// CMS.INSERT 插入计数
	pageViews := []string{"/home", "/home", "/api/users", "/home", "/api/orders"}
	for _, page := range pageViews {
		cmd := redis.NewStringCmd(ctx, "CMS.INSERT", key, page, "1")
		_ = e.rdb.Process(ctx, cmd)
	}

	// CMS.QUERY 查询
	for _, page := range []string{"/home", "/api/users", "/nonexist"} {
		cmd := redis.NewIntCmd(ctx, "CMS.QUERY", key, page)
		_ = e.rdb.Process(ctx, cmd)
		cnt, _ := cmd.Result()
		fmt.Printf("  CMS.QUERY %s → %d\n", page, cnt)
	}

	e.rdb.Del(ctx, key)
	return nil
}

// ExampleRedisBloomFilterCuckoo CF.ADD / CF.EXISTS
func (e *SearchBloomExamples) ExampleRedisBloomCuckoo(ctx context.Context) error {
	key := "cf:blacklist"

	// CF.ADD 添加黑名单 IP
	cmds := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	for _, ip := range cmds {
		c := redis.NewIntCmd(ctx, "CF.ADD", key, ip)
		_ = e.rdb.Process(ctx, c)
		added, _ := c.Result()
		fmt.Printf("  CF.ADD %s → added=%d\n", ip, added)
	}

	// CF.EXISTS 查询
	for _, ip := range cmds {
		c := redis.NewIntCmd(ctx, "CF.EXISTS", key, ip)
		_ = e.rdb.Process(ctx, c)
		exists, _ := c.Result()
		fmt.Printf("  CF.EXISTS %s → %v\n", ip, exists == 1)
	}
	// 未添加的 IP
	c := redis.NewIntCmd(ctx, "CF.EXISTS", key, "10.0.0.99")
	_ = e.rdb.Process(ctx, c)
	exists, _ := c.Result()
	fmt.Printf("  CF.EXISTS 10.0.0.99 → %v\n", exists == 1)

	e.rdb.Del(ctx, key)
	return nil
}
