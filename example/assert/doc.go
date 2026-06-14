// Package assert 提供 cachex 6 backend × 2 topology 组合 e2e 测试共享的辅助函数。
//
// 设计原则：
//   - 不抽象 cachex 自身的 Set/Get 签名（cachex.Cache 是统一的，直接 import cachex + drivers）
//   - 抽象 trace_id 生成、backend span 轮询、docker 容器生命周期
//   - docker / 业务 broker 不可达 → t.Skip，不 Fail
//   - redis 端跨进程 trace 测试由 initx.WithRedisClient 注入 *redis.Client，
//     kafka 端由 initx.WithKafkaProducer 注入 sarama.SyncProducer
//
// # SOP：跑 e2e 前的手工准备
//
// 跑 e2e 前用户须按 backend 启对应容器：
//
//	# jaeger backend：依赖外部 jaeger 容器
//	docker run -d -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one:latest
//
//	# redis_stream backend：起一个 redis
//	docker run -d -p 6379:6379 redis:7-alpine
//
//	# kafka_topic backend：起一个 kafka（暴露 19092 host port 避免与 9092 业务端口冲突）
//	docker run -d -p 19092:9092 apache/kafka:latest
//
//	# 业务 broker（单测 redisx / kafkax）
//	# 单拓扑：复用上方 jaeger / redis / kafka 容器（host port 6379 / 19092）
//	# 集群拓扑：起 redis cluster (7000-7005) 或 kafka cluster (9092-9094)
//
//	# 跑测试
//	go test -v -count=1 ./example/redisx_test/      # 6 组合
//	go test -v -count=1 ./example/kafkax_test/      # 6 组合
//	go test -v -count=1 ./example/tracing_test/     # 5 文件
//
// # 3 backend 验证强度差异矩阵
//
// jaeger backend 走严格 HTTP API（查得到全部 SpanRecord 字段）；
// redis_stream / kafka_topic 走自定义 SpanExporter（不写 Kind / ParentSpanID / Status
// 字段），调用方按 backend 走两条路径。
package assert
