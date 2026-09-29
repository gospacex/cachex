# Redis 驱动功能矩阵（兼容 KeyDB / DragonflyDB / Valkey）

> 代码依据：`redis.go`、`register.go`（cachex/redis）
> 更新日期：2026-09-28 | 驱动注册名：`redis` | 客户端库：`github.com/go-redis/redis/v8`

**SDK 兼容性声明**：本 SDK 基于 RESP（Redis 协议）实现，**兼容 Redis、KeyDB、DragonflyDB、Valkey 等 Redis 协议中间件**——上述中间件均可通过 `Driver: "redis"` 直接接入本 SDK，无需单独驱动（详见第 8 节）。

## 1. 基础命令矩阵（cachex.Cache 接口）

| 接口方法 | 底层命令 | 状态 | 行为说明 |
|---|---|---|---|
| `Get(ctx, key)` | `GET` | ✅ 支持 | Miss 时返回 `("", nil)`，不返回错误（`redis.Nil` 被吞） |
| `Set(ctx, key, val, ttl)` | `SET` | ✅ 支持 | `ttl <= 0` 时归零为**永不过期**（非 Memcached 的 300s） |
| `Delete(ctx, key)` | `DEL` | ✅ 支持 | 删除不存在的 key 不报错 |
| `Exists(ctx, key)` | `EXISTS` | ✅ 支持 | 返回 `count > 0` |
| `Keys(ctx, pattern)` | `KEYS` | ✅ 支持 | ⚠️ 生产环境慎用——O(N) 阻塞命令 |
| `Close()` | 连接池关闭 | ✅ 支持 | Close 后继续操作返回错误 |
| `HealthCheck(ctx)` | `PING` | ✅ 支持 | 带独立 span 与错误日志 |

### 基础命令 OTel 追踪属性

| 属性 | 值 |
|---|---|
| span name | `db.redis.{COMMAND}` |
| `db.system` | `redis` |
| `db.redis.command` | `GET` / `SET` / `DEL` / `EXISTS` / `KEYS` / `PING` |
| `db.redis.key` | 请求 key（非空时） |
| 附加 | `db.redis.found`、`db.redis.ttl_ms`、`db.redis.keys_count` |

## 2. 数据类型与命令矩阵

Redis 支持丰富的数据类型，以下按类型列出 `cachex.Cache` 接口已封装和未封装的命令。

### 2.1 String（字符串）

> 当前接口**仅封装 String 类型**，是唯一可通过 `cachex.Cache` 直接操作的数据类型。

| 命令 | 说明 | 接口状态 | 应用场景 |
|---|---|---|---|
| `GET` | 获取值 | ✅ `Get()` | 缓存读取、Session 读取、配置读取 |
| `SET` | 设置值（支持 TTL） | ✅ `Set()` | 缓存写入、Session 存储、验证码存储 |
| `SET NX` | 仅 key 不存在时设置 | ❌ 未封装 | 分布式锁、并发锁、幂等性保证 |
| `SET XX` | 仅 key 存在时设置 | ❌ 未封装 | 条件更新、CAS 语义 |
| `SET EX/PX` | 设置值+过期时间 | ✅ `Set(ttl)` | 带 TTL 缓存、Session 过期 |
| `MGET` | 批量获取 | ❌ 未封装 | 批量缓存加载、缓存预热 |
| `MSET` | 批量设置 | ❌ 未封装 | 批量写入、初始化缓存 |
| `INCR/INCRBY` | 原子自增 | ❌ 未封装 | 秒杀库存扣减、限流计数器、阅读量统计 |
| `DECR/DECRBY` | 原子自减 | ❌ 未封装 | 库存回退、配额扣减 |
| `APPEND` | 追加值 | ❌ 未封装 | 日志追加、审计轨迹 |
| `STRLEN` | 获取值长度 | ❌ 未封装 | 内容校验、大 value 检测 |
| `GETRANGE` | 获取子串 | ❌ 未封装 | 分页读取、大文本分段 |
| `SETRANGE` | 覆盖写入 | ❌ 未封装 | 固定长度记录更新 |
| `SETBIT/GETBIT/BITCOUNT` | 位操作 | ❌ 未封装 | 签到系统、用户在线状态、特征标记 |
| `SETEX/PSETEX` | 设置值+过期（旧语法） | ✅ `Set(ttl)` | 同 SET EX |

### 2.2 Hash（哈希表）

| 命令 | 说明 | 接口状态 | 应用场景 |
|---|---|---|---|
| `HGET` | 获取字段值 | ❌ 未封装 | 用户资料读取、商品属性读取 |
| `HSET` | 设置字段值 | ❌ 未封装 | 用户资料更新、商品属性存储 |
| `HDEL` | 删除字段 | ❌ 未封装 | 字段级删除、属性清理 |
| `HGETALL` | 获取所有字段 | ❌ 未封装 | 对象完整加载、详情页渲染 |
| `HMGET` | 批量获取字段 | ❌ 未封装 | 精选字段加载、减少网络开销 |
| `HMSET` | 批量设置字段 | ❌ 未封装 | 对象批量写入、配置批量更新 |
| `HINCRBY` | 字段原子自增 | ❌ 未封装 | 商品销量统计、用户积分累加 |
| `HLEN` | 获取字段数量 | ❌ 未封装 | 属性计数、空对象检测 |
| `HEXISTS` | 判断字段存在 | ❌ 未封装 | 属性存在性检查、条件渲染 |
| `HKEYS/HVALS` | 获取所有 key/value | ❌ 未封装 | 表单选项加载、枚举值提取 |
| `HSCAN` | 游标遍历字段 | ❌ 未封装 | 大 Hash 安全遍历 |
| **典型场景** | | | 用户资料（user:1001 → {name, email, avatar}）；商品 SKU 属性；Session 内部字段 |

### 2.3 List（列表）

| 命令 | 说明 | 接口状态 | 应用场景 |
|---|---|---|---|
| `LPUSH/RPUSH` | 左/右插入 | ❌ 未封装 | 消息队列生产、最新动态流、任务入队 |
| `LPOP/RPOP` | 左/右弹出 | ❌ 未封装 | 消息消费、任务出队 |
| `LRANGE` | 范围获取 | ❌ 未封装 | 最新 N 条查询、时间线读取 |
| `LLEN` | 列表长度 | ❌ 未封装 | 队列深度监控、待处理任务数 |
| `LINDEX` | 按索引获取 | ❌ 未封装 | 随机访问、分页 |
| `LSET` | 按索引设置 | ❌ 未封装 | 指定位置更新 |
| `LREM` | 删除指定值 | ❌ 未封装 | 消息确认删除、任务完成移除 |
| `LTRIM` | 保留指定范围 | ❌ 未封装 | 保留最近 100 条、列表裁剪 |
| `BLPOP/BRPOP` | 阻塞弹出 | ❌ 未封装 | 阻塞式消息队列、任务调度 |
| `RPOPLPUSH` | 右弹左推 | ❌ 未封装 | 可靠消息队列（ACK 机制） |
| **典型场景** | | | 简易消息队列（LPUSH+BRPOP）；最新评论/动态（LPUSH+LTRIM+LRANGE）；任务队列（RPOPLPUSH 实现可靠消费） |

### 2.4 Set（集合）

| 命令 | 说明 | 接口状态 | 应用场景 |
|---|---|---|---|
| `SADD` | 添加成员 | ❌ 未封装 | 标签添加、好友关注、用户分组 |
| `SREM` | 移除成员 | ❌ 未封装 | 标签移除、取消关注 |
| `SISMEMBER` | 判断成员存在 | ❌ 未封装 | 是否已点赞、是否已签到、权限检查 |
| `SMEMBERS` | 获取所有成员 | ❌ 未封装 | 标签列表、好友列表 |
| `SCARD` | 集合大小 | ❌ 未封装 | 粉丝数、标签计数 |
| `SINTER` | 交集 | ❌ 未封装 | 共同好友、共同标签、交集推荐 |
| `SUNION` | 并集 | ❌ 未封装 | 合并标签、关注合并 |
| `SDIFF` | 差集 | ❌ 未封装 | 推荐系统（你可能认识的人）、未读消息 |
| `SRANDMEMBER` | 随机获取 | ❌ 未封装 | 随机推荐、抽奖系统 |
| `SPOP` | 随机弹出 | ❌ 未封装 | 抽奖（不放回）、随机任务分配 |
| `SSCAN` | 游标遍历 | ❌ 未封装 | 大集合安全遍历 |
| **典型场景** | | | 标签系统（article:tags → {go, redis, cache}）；共同好友（SINTER user:1:friends user:2:friends）；抽奖（SRANDMEMBER/POP）；权限白名单（SISMEMBER 快速判断） |

### 2.5 Sorted Set（有序集合）

| 命令 | 说明 | 接口状态 | 应用场景 |
|---|---|---|---|
| `ZADD` | 添加成员+分数 | ❌ 未封装 | 排行榜写入、延迟队列入队（score=执行时间戳） |
| `ZREM` | 移除成员 | ❌ 未封装 | 排行榜清除、任务取消 |
| `ZRANGE` | 按分数升序获取 | ❌ 未封装 | 排行榜 Top N、时间范围查询 |
| `ZREVRANGE` | 按分数降序获取 | ❌ 未封装 | 热门排行、销量排行 |
| `ZRANGEBYSCORE` | 按分数范围获取 | ❌ 未封装 | 延迟队列消费（score≤当前时间）；价格区间筛选 |
| `ZSCORE` | 获取成员分数 | ❌ 未封装 | 查询排名分数、判断优先级 |
| `ZRANK/ZREVRANK` | 获取排名 | ❌ 未封装 | 用户排名查询、Top N 展示 |
| `ZINCRBY` | 分数自增 | ❌ 未封装 | 热度累加、投票计数 |
| `ZCARD` | 集合大小 | ❌ 未封装 | 排行榜总人数 |
| `ZCOUNT` | 分数范围计数 | ❌ 未封装 | 分数区间统计、活跃用户数 |
| `ZREMZRANGEBYSCORE` | 删除分数范围 | ❌ 未封装 | 清理过期延迟任务 |
| `ZSCAN` | 游标遍历 | ❌ 未封装 | 大排行榜安全遍历 |
| **典型场景** | | | 游戏排行榜（ZADD player:score + ZREVRANGE Top 10）；延迟队列（ZADD task:delay score=执行时间 + ZRANGEBYSCORE 消费）；热搜榜（ZINCRBY 累加热度 + ZREVRANGE 取 Top）；时间线（score=时间戳 + ZRANGE 按时间排序） |

### 2.6 其他数据类型（go-redis/v8 直接支持）

> 以下类型无需 Redis Stack 插件，标准 Redis 即可运行。通过 `driver.Raw()` 获取 `redis.UniversalClient` 后使用。

| 类型 | 命令 | 接口状态 | 示例文件 | 应用场景 |
|---|---|---|---|---|
| **Bitmap** | `SETBIT`/`GETBIT`/`BITCOUNT`/`BITOP`/`BITPOS` | ✅ 示例可跑 | `examples_bitmap.go` | 签到系统（每天一个 offset）、用户在线状态（userId→bit）、特征标记（用户画像 bit 向量） |
| **HyperLogLog** | `PFADD`/`PFCOUNT`/`PFMERGE` | ✅ 示例可跑 | `examples_string.go`（ExampleHyperLogLog） | UV 统计（12KB 存 2^64 个元素的基数估算）；页面访问去重计数 |
| **Geospatial** | `GEOADD`/`GEODIST`/`GEOSEARCH`/`GEORADIUS` | ✅ 示例可跑 | `examples_advanced.go`（GeoExamples） | 附近的人/门店（GEOSEARCH）；地理围栏（GEORADIUS + WITHCOORD）；距离计算（GEODIST） |
| **Stream** | `XADD`/`XREAD`/`XREADGROUP`/`XACK`/`XPENDING`/`XCLAIM` | ✅ 示例可跑 | `examples_advanced.go` / `examples_stream.go` | 事件溯源（追加日志）；消费者组（可靠消息队列）；CQRS 读模型同步 |
| **Pub/Sub** | `PUBLISH`/`SUBSCRIBE`/`PSUBSCRIBE`/`PubSubNumSub` | ✅ 示例可跑 | `examples_pubsub.go` | 实时通知推送；配置变更广播；多节点事件同步 |
| **Scripting** | `EVAL`/`EVALSHA` | ✅ 示例可跑 | `examples_advanced.go`（ScriptExamples） | 复杂原子操作（分布式锁续期、限流窗口滑动）；服务端原子逻辑 |
| **Transaction** | `MULTI`/`EXEC`/`WATCH` | ✅ 示例可跑 | `examples_advanced.go`（PipelineExamples） | 原子批量操作；乐观锁 CAS 更新 |
| **Pipeline** | `Pipeline` 批量发送 | ✅ 示例可跑 | `examples_advanced.go`（PipelineExamples） | 批量命令网络优化（减少 RTT）；缓存预热加速 |

## 3. 高级命令与扩展能力矩阵

| 能力 | 底层命令 | 接口状态 | 应用场景 | 备注 |
|---|---|---|---|---|
| 原子计数 | `INCR`/`DECR`/`INCRBY` | ✅ 示例可跑 | 秒杀库存扣减、API 限流计数器、页面浏览量统计 | Redis 单线程保证原子性 |
| 分布式锁 | `SET key val NX EX ttl` | ✅ 示例可跑 | 分布式互斥（定时任务、配置刷新）；Lua 续期 | 详见 `examples_business_distributed_lock.go` |
| 可重入锁 | `SET NX` + Hash 记录重入 | ❌ 未封装 | 同一 goroutine 多次加锁场景 | 需 Lua 脚本实现 |
| 批量读写 | `MGET`/`MSET` | ✅ 示例可跑 | 缓存预热（启动时批量加载）；批量查询优化 | 减少网络 RTT |
| TTL 管理 | `EXPIRE`/`TTL`/`PEXPIRE`/`PTTL` | ❌ 未封装 | Session 续期；动态过期策略 | 当前仅 Set 时可传 TTL |
| 滑动窗口限流 | `ZRANGEBYSCORE` + Lua | ✅ 示例可跑 | API 限流（每分钟 N 次）；精确流量控制 | 详见 `examples_business_rate_limit.go` |
| 固定窗口限流 | `INCR` + `EXPIRE` | ✅ 示例可跑 | 简单限流（每秒 N 次） | 详见 `examples_business_rate_limit.go` |
| 令牌桶限流 | Lua 脚本 | ✅ 示例可跑 | 平滑限流、突发流量控制 | 详见 `examples_business_rate_limit.go` |
| 乐观锁（CAS） | `WATCH` + `MULTI/EXEC` | ✅ 示例可跑 | 条件更新（余额扣减前校验） | 失败需重试 |
| 发布订阅 | `PUBLISH`/`SUBSCRIBE` | ✅ 示例可跑 | 实时通知（WebSocket 推送源）；配置变更广播 | 非持久化，消息可能丢失 |
| Stream 消息队列 | `XADD`/`XREADGROUP`/`XACK` | ✅ 示例可跑 | 可靠消息队列（替代 Kafka 轻量场景）；事件溯源 | Redis 5.0+ 原生支持 |
| SCAN 安全遍历 | `SCAN`/`HSCAN`/`SSCAN`/`ZSCAN` | ✅ 示例可跑 | 替代 KEYS 的生产级键遍历；游标分批扫描 | O(1) 每次调用，不阻塞 |
| Lua 脚本 | `EVAL`/`EVALSHA` | ✅ 示例可跑 | 复杂原子逻辑（分布式锁续期、库存扣减+校验） | 减少网络往返，原子执行 |
| 管道批量 | `Pipeline` | ✅ 示例可跑 | 批量命令加速（缓存预热、批量删除） | 减少网络 RTT，非原子 |
| 地理围栏 | `GEOSEARCH`/`GEORADIUS` | ✅ 示例可跑 | 附近门店/用户查询；进出围栏事件 | 需 Geo 类型支持 |
| 基数统计 | `PFADD`/`PFCOUNT` | ✅ 示例可跑 | 日活/月活用户统计（UV）；去重计数 | 误差约 0.81%，内存极低 |
| 延迟队列 | `ZADD` score=时间戳 + `ZRANGEBYSCORE` | ✅ 示例可跑 | 延迟任务执行、类 Cron 调度 | 详见 `examples_business_delayed_queue.go` |
| 缓存穿透防护（BF） | `BF.ADD`/`BF.EXISTS` | ✅ Redis Stack | 布隆过滤器预判 key 是否存在 | 需 `redis/redis-stack:8.4` |
| 缓存穿透防护（null cache） | `SET key "" TTL` | ✅ 示例可跑 | null 值短 TTL 兜底方案 | 无需额外插件，见 `examples_business_cache_penetration.go` |

### 3.1 Redis Stack 扩展（RediSearch + RedisBloom）

> 需使用 `redis/redis-stack:8.4` Docker 镜像（已配置在 `docker-compose.yml`），内置 RediSearch 和 RedisBloom 插件。
> go-redis/v8 不封装以上插件命令，使用**原始命令模式**调用：
>
> ```go
> rdb := driver.Raw() // redis.UniversalClient
> cmd := redis.NewStringCmd(ctx, "FT.SEARCH", "idx:products", "title:(phone)", "LIMIT", "0", "10")
> _ = rdb.Process(ctx, cmd)
> result, _ := cmd.Result()
> ```

| 插件 | 命令前缀 | 接口状态 | 示例文件 | 应用场景 |
|---|---|---|---|---|
| **RediSearch** | `FT.*` | ✅ 原始命令可跑 | `examples_search_bloom.go`（FT.SEARCH 示例） | 全文检索、模糊搜索、聚合统计；替代 ES 的轻量场景 |
| **RedisBloom** | `BF.*` / `CF.*` / `CMS.*` / `TDIGEST.*` | ✅ 原始命令可跑 | `examples_search_bloom.go` | 布隆过滤器（防穿透）、计数布隆（CMS）、cuckoo filter |

**接入方式**：`docker-compose up -d` 启动 `redis/redis-stack:8.4`，`Addr` 指向 `localhost:6379`（默认），无需额外密码配置。

## 4. 部署拓扑矩阵

| 模式 | 状态 | 接线方式 |
|---|---|---|
| 单机 Standalone | ✅ 支持 | `cfg.Addr` 直连（默认分支） |
| Sentinel 哨兵 | ✅ 支持 | `cfg.SentinelAddrs != ""` → `NewFailoverClient`，`SentinelMaster` 指定主节点 |
| Cluster 集群 | ✅ 支持 | `cfg.Cluster != ""` → `NewClusterClient`，逗号分隔地址 |
| TLS | ✅ 支持 | `cfg.TLSEnabled` → `MinVersion: TLS1.2` |
| 连接重试 | ✅ 支持 | `New()` 内 Ping 重试 3 次（指数退避 100ms→200ms→400ms） |

> Redis 是四个驱动中**唯一**支持 Sentinel/Cluster 三模式的驱动。

## 5. 配置字段生效状态（cachex.Config）

| 字段 | 状态 | 说明 |
|---|---|---|
| `Addr` | ✅ 生效 | 单机地址；Sentinel/Cluster 模式下未使用 |
| `Password` | ✅ 生效 | 三种模式均传递；支持 `${ENV_VAR}` 语法，Init 时自动展开 |
| `DB` | ✅ 生效 | 仅单机/Sentinel 传递（Cluster 协议无 DB） |
| `PoolSize` | ✅ 生效 | 三模式均传递，默认 10 |
| `MinIdleConn` | ✅ 生效 | 默认 0 |
| `Timeout` | ✅ 生效 | Dial 超时，同时作为 Read/Write 默认值 |
| `ReadTimeout` | ✅ 生效 | 驱动读超时；若为 0 则回退到 `Timeout` |
| `WriteTimeout` | ✅ 生效 | 驱动写超时；若为 0 则回退到 `Timeout` |
| `RetryCount` | ✅ 生效 | 初始化 Ping 重试次数，默认 3（指数退避 100ms→200ms→400ms） |
| `SentinelMaster` / `SentinelAddrs` | ✅ 生效 | 逗号分隔多地址 |
| `Cluster` | ✅ 生效 | 非空字符串即启用集群模式 |
| `TLSEnabled` | ✅ 生效 | 启用 TLS 1.2+ |
| `Name` / `Driver` | ✅ 生效 | 注册表分发用 |

### 新增企业级能力（v2.0）

| 能力 | 状态 | 说明 |
|---|---|---|
| 全局调用链开关 | ✅ 支持 | `TracingConfig.Enabled=true` 时所有命令自动开 span；全局原子切换，无 per-instance 配置项 |
| 密码环境变量展开 | ✅ 支持 | `Password` 支持 `${MY_PASS}` 语法，SDK Init 时通过 `os.LookupEnv` 解析；未找到时报错 |
| Prometheus 指标 | ✅ 支持 | `cachex.RecordCommand` / `UpdatePoolMetrics` / `UpdateCircuitBreakerState` 驱动内每命令自动记录 |
| 熔断器 | ✅ 支持 | 每个实例独立 `gobreaker` 实例，`Execute(ctx, fn)` 自动包装；`State()` 0/1/2 分别对应 Closed/Open/Half-Open |
| 独立读写超时 | ✅ 支持 | `ReadTimeout`/`WriteTimeout` 覆盖 `Timeout`，默认 5s |

### 驱动自有配置（redis/config.go）已接线字段

`StandaloneConfig` / `ClusterConfig` 中以下字段现已全部传递到 `cachex.Config`：
`ReadTimeout`、`WriteTimeout`、`RetryCount`（映射 `MaxRetries`）、`Timeout`。
旧有未接线字段 `RetryTimes`、`RetryDelay`、`RouteByLatency`、`RouteRandomly`、`ContextTimeout`、`MaxRedirects`、`DisableIndice` 保留在结构体中但未传入 `cachex.Config`，未来按需接线。

## 6. 企业级业务场景矩阵

| 业务场景 | 支持状态 | 实现方式与约束 |
|---|---|---|
| 会话存储（Session） | ✅ 完整支持 | Set 带 TTL + Get；`Get` Miss 返回空串，需业务侧区分"空值"与"未命中" |
| 分布式锁 | ✅ 完整支持 | `SET NX EX` + Lua 原子释放/续期；详见 `examples_business_distributed_lock.go` |
| 限流计数（Rate Limit） | ✅ 完整支持 | Token Bucket（INCR+EXPIRE）、Sliding Window Log（ZSet score=时间戳）；详见 `examples_business_rate_limit.go` |
| 验证码 / OTP | ✅ 完整支持 | Set TTL + Get + Delete 三件套全覆盖 |
| 热点数据缓存（Cache-Aside） | ✅ 完整支持 | Get/Set/Delete + Exists 完整，配合 Keys 可做批量失效（生产慎用 Keys） |
| 分布式会话共享（多副本） | ✅ 支持 | Sentinel/Cluster 模式提供高可用 |
| 秒杀/计数器扣减 | ✅ 支持 | INCR/DECR/INCRBY 通过 `driver.Raw()` 获取 `redis.UniversalClient` 调用；详见 `examples_string.go` |
| 发布订阅 | ✅ 支持 | `PUBLISH`/`SUBSCRIBE`/`PSUBSCRIBE`；详见 `examples_pubsub.go` |
| 排行榜（ZSet） | ✅ 完整支持 | ZADD/ZREVRANGE/ZINCRBY；详见 `examples_zset.go` |
| 延迟队列 | ✅ 完整支持 | ZADD(score=执行时间戳) + ZRANGEBYSCORE 轮询消费；详见 `examples_business_delayed_queue.go` |
| 批量读写（MGET/MSET） | ✅ 支持 | `driver.Raw()` 获取 `redis.UniversalClient` 调用 MGet/MSet；详见 `examples_string.go` |
| 缓存预热 | ✅ 支持 | 应用启动后逐 key Set（无批量，效率受限） |
| 多实例隔离（多租户） | ✅ 支持 | `cachex.Instances` 命名实例 + `DB` 编号隔离 |
| 健康探针（K8s liveness） | ✅ 支持 | `HealthCheck` PING 直连验证 |
| 链路追踪（全链路 APM） | ✅ 支持 | 每命令自动 span，属性符合 OTel 语义约定 `db.redis.*` |
| 灰度/金丝雀读旧数据 | ⚠️ 部分 | 无版本 key/双写封装，需业务层实现 |
| 缓存穿透防护（布隆过滤器） | ✅ Redis Stack | `BF.RESERVE`/`BF.ADD`/`BF.EXISTS` 原始命令；详见 `examples_business_cache_penetration.go` |
| 缓存穿透防护（null cache） | ✅ 基础方案 | null 值短 TTL 兜底；不依赖 Redis Stack |
| 签到/在线状态（Bitmap） | ✅ 支持 | `SETBIT`/`GETBIT`/`BITCOUNT`；详见 `examples_bitmap.go` |
| 附近的人/地理围栏（Geo） | ✅ 支持 | `GEOADD`/`GEOSEARCH`/`GEODIST`；详见 `examples_advanced.go` |
| 全文检索（RediSearch） | ✅ Redis Stack | `FT.CREATE`/`FT.SEARCH` 原始命令；详见 `examples_search_bloom.go` |
| 流式事件队列（Stream） | ✅ 支持 | `XADD`/`XREADGROUP`/`XACK`/`XPENDING`；详见 `examples_stream.go` |

## 7. 已知限制与风险

1. **`Keys` 阻塞风险**：KEYS 是 O(N) 全库扫描，大库会阻塞 Redis 主线程；建议未来扩展 `SCAN` 游标接口。
2. **TTL=0 语义陷阱**：`Set` 传入 `ttl <= 0` 表示永久有效，与 Memcached 驱动（默认 300s）语义不同，跨驱动迁移需注意。
3. **`Get` 空值歧义**：Miss 与存储空串不可区分，业务需用 `Exists` 辅助或自定编码。
4. **熔断器默认参数偏宽松**：gobreaker 默认允许 5 次连续失败后跳闸；若需要更敏感的故障检测，需通过封装层定制 `Settings`。
5. **`UniversalClient` 池统计**：`poolActive`/`poolIdle` 通过读取驱动构建时的 `cfg.PoolSize`/`cfg.MinIdleConn` 静态估算（go-redis/v8 未暴露实时池计数器），指标值为近似值。
6. **密码环境变量仅在 Init 时展开**：热更新配置时若新密码含 `${VAR}` 语法，需在 `SDK.Init()` 阶段重新触发展开；configx Watch 回调中已有显式调用。

## 8. 协议兼容中间件（KeyDB / DragonflyDB / Valkey）

本驱动基于 RESP（Redis Serialization Protocol）协议通信，**任何兼容 Redis 协议的中间件均可直接复用本驱动接入**，无需单独的驱动实现——只需把 `Addr` 指向对应中间件实例。

### 8.1 兼容矩阵

| 中间件 | 协议兼容性 | 接入方式 | 差异说明 |
|---|---|---|---|
| **Redis** | 原生 | `Driver: "redis"` + `Addr: host:6379` | 基准实现，全功能支持 |
| **KeyDB** | ✅ 完全兼容 RESP | `Driver: "redis"` + `Addr: host:6379` | 多线程架构，协议 100% 兼容；单机/Sentinel/Cluster 全支持 |
| **DragonflyDB** | ✅ 兼容 RESP2/RESP3 | `Driver: "redis"` + `Addr: host:6379` | 多线程架构；单机模式为主，集群为分片方案（非 Redis Cluster 协议），Sentinel 不适用 |
| **Valkey** | ✅ 完全兼容 RESP | `Driver: "redis"` + `Addr: host:6379` | Redis 7.2 分支延续，协议 100% 兼容，Sentinel/Cluster 均支持 |
| **Memcached** | ❌ 协议不同 | `Driver: "memcached"`（独立驱动） | 二进制/文本协议，非 RESP，必须用 cachex/memcached 驱动 |

> 判断标准：中间件是否说 RESP 协议。KeyDB、DragonflyDB、Valkey 均以 Redis 协议兼容为卖点，`go-redis` 客户端对它们透明无感。

### 8.2 KeyDB 接入示例

```go
import (
    "github.com/gospacex/cachex"
    _ "github.com/gospacex/cachex/redis" // 同一个 redis 驱动
)

// KeyDB 兼容 Redis 协议，直接复用 redis 驱动，仅 Addr 不同
instances, _ := cachex.New([]*cachex.Config{{
    Name: "keydb", Driver: "redis",
    Addr: "127.0.0.1:6379", // KeyDB 默认端口与 Redis 相同
    Password: "keydb123456",
    PoolSize: 10, Timeout: 5 * time.Second,
}})
client := instances["keydb"] // cachex.Cache
```

### 8.3 DragonflyDB 接入示例

```go
// DragonflyDB 同样兼容 Redis 协议，复用 redis 驱动
instances, _ := cachex.New([]*cachex.Config{{
    Name: "dragonfly", Driver: "redis",
    Addr: "127.0.0.1:6379", // Dragonfly 默认端口 6379
    Password: "",           // Dragonfly 默认无密码（可通过 --requirepass 设置）
    PoolSize: 10, Timeout: 5 * time.Second,
}})
client := instances["dragonfly"] // cachex.Cache
```

### 8.4 兼容性注意事项

1. **命令集差异**：本驱动仅使用 `GET/SET/DEL/EXISTS/KEYS/PING` 基础命令，这些命令在 KeyDB、DragonflyDB、Valkey 中均 100% 支持，基础功能无差异。
2. **多实例隔离**：`Name` 字段区分实例（如上例 `keydb`、`dragonfly`），可同时接入多种中间件，互不干扰。
3. **拓扑差异**：
   - KeyDB/Valkey 完整支持 Sentinel/Cluster（`SentinelAddrs`/`Cluster` 配置可用）；
   - DragonflyDB 的集群是自带分片方案，**不走 Redis Cluster 协议**，`Cluster` 配置对其不适用，直接 `Addr` 单点接入即可。
4. **TTL 语义一致**：上述 RESP 中间件的 `SET key val EX ttl` 语义一致，`Set(ttl <= 0)` 永不过期的行为跨中间件统一。
5. **性能特征不同**：KeyDB/DragonflyDB 为多线程架构，`KEYS` 等 O(N) 命令的阻塞影响相对较小，但生产环境仍建议避免（见第 7 节）。

## 9. 快速接入示例

```go
import (
    "github.com/gospacex/cachex"
    _ "github.com/gospacex/cachex/redis" // 注册驱动
)

instances, _ := cachex.New([]*cachex.Config{{
    Name: "default", Driver: "redis",
    Addr: "127.0.0.1:6379", Password: "redis123456",
    PoolSize: 10, Timeout: 5 * time.Second,
}})
client := instances["default"] // cachex.Cache
```

> 接入 KeyDB / DragonflyDB / Valkey 时，`Driver` 仍为 `"redis"`，仅改 `Addr` 与凭据，详见第 8 节。
