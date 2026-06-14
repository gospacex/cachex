package redisx

import "github.com/gospacex/cachex"

func init() {
	cachex.RegisterRedisPool(PPS)
	cachex.RegisterRedisClusterPool(PPC)
}
