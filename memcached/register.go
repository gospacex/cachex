// cachex/memcached - 驱动注册
package memcached

import "github.com/gospacex/cachex"

func init() {
	cachex.RegisterCreator("memcached", func(cfg *cachex.Config) (cachex.Cache, error) {
		return New(cfg)
	})
}
