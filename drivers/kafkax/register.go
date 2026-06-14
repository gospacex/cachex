package kafkax

import "github.com/gospacex/cachex"

func init() {
	cachex.RegisterKafkaPool(PPS)
}
