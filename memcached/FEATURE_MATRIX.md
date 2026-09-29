# Memcached 驱动功能矩阵

> 代码依据：`memcached.go`、`register.go`（cachex/memcached）
> 更新日期：2026-09-28 | 驱动注册名：`memcached` | 客户端库：`github.com/bradfitz/gomemcache/memcache`

## 1. 基础命令矩阵（cachex.Cache 接口）

| 接口方法 | 底层命令 | 状态 | 行为说明 |
|---|---|---|---|
| `Get(ctx, key)` | `get` | ✅ 支持 | Miss 返回 `("", nil)`（`ErrCacheMiss` 被吞） |
| `Set(ctx, key, val, ttl)` | `set` | ✅ 支持 | `ttl <= 0` 表示**永不过期**（与 Redis 系语义一致）；`0 < ttl < 1s` 向上取整为 1 秒；TTL 最大 30 天（协议限制，超出按 unix 时间戳解释） |
| `Delete(ctx, key)` | `delete` | ✅ 支持 | Miss 静默成功（`ErrCacheMiss` 吞掉） |
| `Exists(ctx, key)` | `get`（模拟） | ✅ 支持 | 用 GET 模拟，读值后丢弃——**多一次反序列化开销** |
| `Keys(ctx, pattern)` | — | ❌ 不支持 | 直接返回错误 `"Keys is not supported by memcached protocol"`；Memcached 无键遍历协议 |
| `Close()` | — | ✅ 支持 | 空实现（gomemcache 无显式关闭），总是返回 nil |
| `HealthCheck(ctx)` | `get _health_check_` | ✅ 支持 | 用 GET Miss 模拟心跳（Memcached 协议无 PING 命令） |

### OTel 追踪属性

| 属性 | 值 |
|---|---|
| span name | `db.memcached.{COMMAND}` |
| `db.system` | `memcached` |
| `db.memcached.command` | `GET` / `SET` / `DELETE` / `EXISTS` / `PING` |
| `db.memcached.key` | 请求 key（非空时） |

> 注意：`Exists` 复用 `GET` span、`Delete` 使用 `DELETE`、健康检查使用 `PING`（虚拟命令名）。

## 2. 数据类型与命令矩阵

Memcached 仅支持 **String（字符串）** 类型，无 Hash/List/Set/Sorted Set 等复合数据结构。

### 2.1 String（字符串）

> 当前接口封装了 Memcached 唯一的数据类型。Memcached 是纯 KV 存储，所有 value 均为字节数组。

| 命令 | 说明 | 接口状态 | 应用场景 |
|---|---|---|---|
| `get` | 获取值 | ✅ `Get()` | 缓存读取、Session 读取 |
| `set` | 设置值（支持 TTL） | ✅ `Set()` | 缓存写入、Session 存储、验证码存储 |
| `delete` | 删除值 | ✅ `Delete()` | 缓存失效、Session 注销 |
| `gets` | 获取值+CAS token | ❌ 未封装 | 乐观锁读取（需配合 `cas` 使用） |
| `cas` | Compare-And-Swap 写入 | ❌ 未封装 | 乐观锁更新（避免并发覆盖） |
| `incr/decr` | 原子自增/自减 | ❌ 未封装 | 秒杀库存扣减、限流计数器、阅读量统计 |
| `append` | 追加值 | ❌ 未封装 | 日志追加、审计轨迹 |
| `prepend` | 前置插入 | ❌ 未封装 | 优先级队列头插 |
| `flush_all` | 清空所有 key | ❌ 未封装 | 缓存全量失效、测试环境重置 |
| `stats` | 获取服务端统计 | ❌ 未封装 | 运维监控、命中率统计 |

> **与 Redis 系驱动的关键差异**：Memcached 无 Hash/List/Set/Sorted Set/Bitmap/HyperLogLog/Geospatial/Stream/Pub/Sub 等复合数据类型。

### 2.2 Memcached 不支持的能力

| Redis 能力 | Memcached 状态 | 替代方案 |
|---|---|---|
| Hash（对象字段存储） | ❌ 协议不支持 | 序列化为 JSON string 存储 |
| List（列表/队列） | ❌ 协议不支持 | 序列化为 JSON string；或使用外部消息队列 |
| Set（集合运算） | ❌ 协议不支持 | 应用层 Set 实现 |
| Sorted Set（排行榜） | ❌ 协议不支持 | 应用层排序或使用 Redis |
| Bitmap（位操作） | ❌ 协议不支持 | 应用层 byte 数组模拟 |
| HyperLogLog（基数统计） | ❌ 协议不支持 | 应用层估算或使用 Redis |
| Geospatial（地理查询） | ❌ 协议不支持 | 使用 Redis 或数据库 |
| Stream（消息流） | ❌ 协议不支持 | 使用 Kafka/RabbitMQ |
| Pub/Sub（发布订阅） | ❌ 协议不支持 | 使用 NATS/RabbitMQ |
| Lua 脚本 | ❌ 协议不支持 | 应用层实现 |
| 事务（MULTI/EXEC） | ❌ 协议不支持 | CAS 乐观锁替代 |
| SCAN 游标遍历 | ❌ 协议不支持 | 业务自持键清单 |
| TTL 精细控制 | ⚠️ 最大 30 天 | 超长 TTL 需转 unix 时间戳 |

## 3. 高级命令与扩展能力矩阵

| 能力 | 底层命令 | 接口状态 | 应用场景 | 备注 |
|---|---|---|---|---|
| 原子计数 | `incr`/`decr` | ❌ 未封装 | 秒杀库存扣减、API 限流计数器、页面浏览量统计 | 协议原生支持，值必须为数字字符串 |
| 乐观锁（CAS） | `gets` + `cas` | ❌ 未封装 | 并发安全更新（避免覆盖其他客户端修改） | 先 `gets` 获取 CAS token，再 `cas` 带 token 写入 |
| 批量读写 | `get` 多 key | ⚠️ 部分 | 一次 get 可传多 key（`\r\n` 分隔） | gomemcache 的 `GetMulti` 支持，但 `cachex.Cache` 接口仅单 key |
| 批量删除 | `delete` 多 key | ⚠️ 部分 | gomemcache 支持 `Delete` 多 key | 同上，接口未暴露 |
| 前置/追加插入 | `prepend`/`append` | ❌ 未封装 | 优先级队列头插；日志追加 | 协议原生支持 |
| 服务端统计 | `stats` | ❌ 未封装 | 运维监控（命中率、eviction、连接数） | 需直接操作 gomemcache Client |
| 缓存全量清空 | `flush_all` | ❌ 未封装 | 测试环境重置、紧急缓存失效 | ⚠️ 生产慎用 |
| 连接池管理 | `MaxIdleConns` | ✅ `PoolSize` 映射 | 控制空闲连接数 | — |
| 节点故障转移 | 一致性哈希自动 | ✅ 服务端能力 | 多节点自动路由 | gomemcache 内置一致性哈希 |

> Memcached 协议简洁（14 条命令），扩展能力有限。上述"未封装"命令均属协议级能力，封装难度低但需评估业务需求。

## 4. 部署拓扑矩阵

| 模式 | 状态 | 说明 |
|---|---|---|
| 单节点 | ✅ 支持 | `Addr` 单地址直连 |
| 多节点客户端分片 | ✅ 支持 | `Addr` 逗号分隔多地址，gomemcache 内置一致性哈希选择节点 |
| 一致性哈希开关 | ⚠️ 字段摆设 | `ClusterConfig.ConsistentHash=true` 仅设置 `cfg.Cluster="true"`，但驱动 `New()` **不消费**该字段（gomemcache 多地址本身就是一致性哈希，字段冗余但无害） |
| SASL 认证 | ❌ 未接线 | `UseSASL`/`Password` 字段存在，但驱动不传给 gomemcache（`mc.Authenticator` 未设置）——带认证的 Memcached（如云托管版）当前连不上 |
| TLS | ❌ 不支持 | `TLSEnabled` 字段被忽略；gomemcache 本身不支持 TLS，需外挂 proxy（如 mcrouter/stunnel） |
| 连接池 | ✅ 支持 | `PoolSize` 映射 `MaxIdleConns` |
| 连接重试 | ❌ 无 | 初始化健康检查一次，无重试退避 |

## 5. 配置字段生效状态（cachex.Config）

| 字段 | 状态 | 说明 |
|---|---|---|
| `Addr` | ✅ 生效 | 逗号分隔多节点（docker-compose 单机为 `127.0.0.1:21121`） |
| `Password` | ⚠️ 不生效 | 存入 cfg 但驱动未使用（SASL 未接线）；支持 `${ENV_VAR}` 展开但不影响连接 |
| `PoolSize` | ✅ 生效 | 映射 `mc.MaxIdleConns` |
| `Timeout` | ✅ 生效 | 映射 `mc.Timeout`，默认 5s |
| `ReadTimeout` / `WriteTimeout` | ✅ 生效 | gomemcache 仅单 Timeout，两者合并使用 `Timeout` |
| `RetryCount` | ✅ 生效 | 初始化健康检查重试，默认 3（指数退避 100ms→200ms→400ms） |
| `DB` | ❌ 不适用 | Memcached 无 DB 概念，字段忽略 |
| `MinIdleConn` | ⚠️ 语义偏差 | `MaxIdleConn`（config）→ `MinIdleConn`（cachex.Config）→ 实际只用 `PoolSize` 映射的 `MaxIdleConns`——命名链路有误导性 |
| `TLSEnabled` | ❌ 不生效 | gomemcache 本身不支持 TLS，需外挂 proxy（如 mcrouter/stunnel） |
| `Cluster` | ❌ 不生效 | 字段被忽略（多节点通过 Addr 逗号分隔实现） |
| `Name` / `Driver` | ✅ 生效 | — |

### 新增企业级能力（v2.0）

| 能力 | 状态 | 说明 |
|---|---|---|
| 全局调用链开关 | ✅ 支持 | `TracingConfig.Enabled=true` 时所有命令自动开 span |
| 密码环境变量展开 | ✅ 支持 | `Password` 支持 `${MY_PASS}` 语法，SDK Init 时解析 |
| Prometheus 指标 | ✅ 支持 | `cachex.RecordCommand` / `UpdateCircuitBreakerState` 自动记录（无 pool 指标，gomemcache 不暴露池统计） |
| 熔断器 | ✅ 支持 | 每个实例独立 `gobreaker` 实例 |
| 独立读写超时 | ⚠️ 部分 | gomemcache 仅单 Timeout；`ReadTimeout`/`WriteTimeout` 均回退到 `Timeout` |

### 驱动自有配置（memcached/config.go）未接线字段

`MaxConn`、`KeyPrefix`、`UseSASL`（Standalone）；
`ConsistentHash`（Cluster，半摆设）。

## 6. 企业级业务场景矩阵

| 业务场景 | 支持状态 | 实现方式与约束 |
|---|---|---|
| 热点数据缓存（Cache-Aside） | ✅ 完整支持 | 纯 KV 缓存是 Memcached 的设计本位；多线程架构适合中 value 高吞吐 |
| 会话存储（Session） | ✅ 支持 | Set TTL + Get；注意 TTL ≤ 0 变 300s，永不过期会话需每 300s 内刷新 |
| 验证码 / OTP | ✅ 完整支持 | 短 TTL 场景语义正确 |
| 临时数据/短时效缓存 | ✅ 优势场景 | 默认 300s TTL 适合"忘了传 TTL 也不过期泄漏"的安全默认 |
| 高并发读（多线程模型） | ✅ 优势场景 | 无 Redis 单线程瓶颈，CPU 核数即扩展上限 |
| 分布式计数（goroutine 结果缓存） | ✅ 支持 | Get-Set 模式（非原子） |
| 缓存键批量管理/清理 | ❌ 不支持 | 无 Keys/SCAN；需业务自持键清单（sidecar 表）或 `flush_all` |
| 分布式锁 | ❌ 不支持 | Memcached 的 add NX 语义未封装，且无过期续期 |
| 限流计数 | ⚠️ 需扩展 | 无 INCR 封装（协议有 `incr`，未接线） |
| 排行榜/集合 | ❌ 不支持 | 协议无结构类型 |
| 发布订阅 | ❌ 不支持 | — |
| 延迟队列 | ❌ 不支持 | — |
| 持久化需求 | ❌ 不支持 | Memcached 纯内存（docker-compose 挂载目录仅存 socket/元数据），重启即丢 |
| 多实例隔离（多租户） | ⚠️ 部分 | 仅靠命名实例区分；无 DB/前缀隔离（`KeyPrefix` 未接线） |
| 健康探针 | ✅ 支持 | GET 模拟心跳 |
| 链路追踪 | ✅ 支持 | `db.memcached.*` span |
| 云托管/带认证实例 | ❌ 当前不支持 | SASL 未接线 |
| 闪崩保护（缓存击穿） | ❌ 不支持 | — |

## 7. 已知限制与风险

1. **TTL 语义跨驱动不一致（最关键）**：`ttl <= 0` 时 Memcached=300s、Redis/Dragonfly/KeyDB=永不过期。跨驱动切换或通用封装层必须显式传 TTL，禁止零值穿透。
2. **Keys 不可用**：接口契约在此驱动上"合法破坏"——调用方若做通用 Cache 抽象，必须在文档层标注该驱动不支持遍历。
3. **TTL 30 天上限**：`Set` 传 `ttl > 30 天` 时 gomemcache 传秒数会溢出协议语义，需转 unix 绝对时间戳（当前实现未做该转换，**长期缓存会行为异常**）。
4. **SASL/TLS 缺失**：公网或云托管 Memcached 无法接入。
5. **健康检查产生 Miss 垃圾键**：`_health_check_` 每次探测真实发 GET，虽 Miss 不落盘，但高频探针会产生无效流量；可接受但需知悉。
6. **`MinIdleConn` 映射语义混乱**：`MaxIdleConn`（config）→ `MinIdleConn`（cachex.Config）→ 实际只用 `PoolSize` 映射的 `MaxIdleConns`——命名链路有误导性。
7. **Close 空实现**：重复调用无副作用，但也不会主动断连（进程退出才释放）。
8. **无池指标**：gomemcache 不暴露活跃/空闲连接数，`UpdatePoolMetrics` 调用时驱动模块返回 0。

## 8. 快速接入示例

```go
import (
    "github.com/gospacex/cachex"
    _ "github.com/gospacex/cachex/memcached" // 注册驱动
)

instances, _ := cachex.New([]*cachex.Config{{
    Name: "default", Driver: "memcached",
    Addr: "127.0.0.1:11211", // 多节点: "10.0.0.1:11211,10.0.0.2:11211"
    PoolSize: 10, Timeout: 5 * time.Second,
}})
client := instances["default"] // cachex.Cache
// 注意：Set 必须显式传 TTL，ttl=0 → 300s 默认
```
