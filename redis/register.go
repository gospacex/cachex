// cachex/redis - 驱动注册
package redis

import "github.com/gospacex/cachex"

func init() {
	cachex.RegisterCreator("redis", func(cfg *cachex.Config) (cachex.Cache, error) {
		return New(cfg)
	})
}
