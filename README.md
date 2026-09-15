# OrderRaft

基于 [hashicorp/raft](https://github.com/hashicorp/raft) 与 gRPC 的订单服务示例。它把订单的写操作建模为 Raft 日志命令:由 Leader 复制并提交后,在有限状态机(FSM)中按序执行,从而使集群中的每个节点都拥有完全一致的订单数据视图。

> 当前为单节点演示实现:使用内存日志存储与内存传输,服务重启后数据会丢失。后续可替换为持久化存储与真实网络传输,扩展成多节点集群。

## 功能特性

- gRPC API:订单创建、查询、状态流转(proto3 定义)
- Raft 共识:所有写操作提交到 Raft 日志,保证节点间状态一致
- 确定性 FSM:订单数据与幂等记录统一在 FSM 内应用
- 幂等去重:以 `request_id` 为键记录首次成功的决策;重放请求返回首次执行结果且标记 `replayed=true`
- 参数校验分层:纯参数校验在 gRPC 层完成,非法请求不进入 Raft 日志、也不占用 `request_id`
- 写请求转发:非 Leader 节点会把写请求转发给 Leader,客户端无需感知集群拓扑;转发标记必须匹配集群共享密钥,无法被客户端伪造
- 优雅关闭:收到 SIGINT/SIGTERM 后先停止接收新请求,等待在途请求结束,再关闭 Raft 节点
- 可配置:节点 ID、监听地址、转发目标、各类超时、快照阈值与日志级别均可通过命令行参数设置
- 状态流转校验:非法迁移(如已取消后再次支付)会被拒绝
- 快照与恢复:FSM 支持确定性序列化的快照,可完整恢复内存状态;快照阈值/间隔/尾部日志可通过命令行参数配置
- 单元测试:状态流转、FSM、幂等、快照、Raft 节点、gRPC 服务层(含 Leader 转发)与命令行参数均有覆盖,并包含"同一日志状态一致/可交换命令顺序无关"的性质测试;CI 另外运行竞态检测、静态检查与生成代码校验

## 技术栈

| 组件 | 说明 |
| --- | --- |
| Go | 1.26+(`go.mod` 声明 1.26.0) |
| [hashicorp/raft](https://github.com/hashicorp/raft) | Raft 共识库 v1.7.3 |
| gRPC / Protocol Buffers | RPC 接口与消息定义 |
| Makefile | proto 生成、测试、构建 |

## 架构

```text
                ┌─────────────────────────────────┐
                │        gRPC OrderService        │
                │  CreateOrder / GetOrder /       │
                │  ChangeOrderStatus              │
                └────────────────┬────────────────┘
                                 │ 写操作(RaftCommand)
                ┌────────────────▼────────────────┐
                │      Raft Node(Leader)          │
                │   Apply → Raft 日志复制/提交      │
                └────────────────┬────────────────┘
                                 │ 日志条目
                ┌────────────────▼────────────────┐
                │      FSM(确定性、带锁)           │
                │   订单存储 + 幂等记录 + 快照      │
                └─────────────────────────────────┘
```

读操作(`GetOrder`)直接读取节点本地的 FSM,不经过 Raft 日志。

## 快速开始

### 环境要求

- Go 1.26+(或使用 `GOTOOLCHAIN=auto`,让 Go 自动下载匹配的工具链)
- 可选:`make`
- 仅在需要重新生成 proto 代码时需要 `protoc`、`protoc-gen-go`、`protoc-gen-go-grpc`(生成代码已提交在 `gen/`,版本须与生成文件头部一致)

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.4.0
```

### 运行测试

```bash
make test       # go test ./...
make test-race  # go test -race -count=1 ./...
make ci         # 格式检查 + 静态检查 + 竞态测试 + 构建,与 CI 保持一致
```

### 启动服务端

```bash
go run ./cmd
```

服务端会启动一个单节点 Raft,并在 `:50052` 上提供 gRPC 服务。所有参数都有默认值,可以直接启动:

```bash
go run ./cmd -node-id node-1 -grpc-addr :50052 -log-level info
```

按 `Ctrl+C` 触发优雅关闭:先停止接收新请求,等待在途请求处理完成,再关闭 Raft 节点。

### 运行客户端示例

```bash
go run ./cmd/client
```

客户端演示完整链路:创建订单(每次运行使用唯一 `order_id`)→ 用同一 `request_id` 重放验证幂等 → 支付 → 查询订单,因此可以反复运行。

### 构建

```bash
make build  # go build ./...
```

### 重新生成 proto

```bash
make proto  # 需要 protoc 工具链
```

### 命令行参数

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-node-id` | `node-1` | 本节点的 Raft 节点 ID |
| `-raft-addr` | 空(取 `-node-id`) | 本节点的 Raft 传输地址;当前为内存传输,该地址只在进程内用于标识节点 |
| `-grpc-addr` | `:50052` | gRPC 监听地址 |
| `-peers` | 空 | 节点 ID 到 gRPC 地址的映射,如 `node-1=10.0.0.1:50052,node-2=10.0.0.2:50052` |
| `-forward-token` | 空 | 集群共享的转发密钥;配置 `-peers` 时必填,用于识别内部转发请求 |
| `-apply-timeout` | `3s` | 提交 Raft 日志的超时 |
| `-forward-timeout` | `3s` | 把写请求转发给 Leader 的超时 |
| `-leader-wait-timeout` | `5s` | 启动时等待 Raft Leader 的超时 |
| `-shutdown-timeout` | `10s` | 优雅关闭时等待在途请求的超时 |
| `-snapshot-interval` | `120s` | 自动触发快照的最小间隔;`0` 表示禁用自动快照(需手动调用 `raft.Snapshot()`) |
| `-snapshot-threshold` | `8192` | 自上次快照以来触发快照的日志条目数;调小可在演示中真正触发快照 |
| `-trailing-logs` | `10240` | 快照完成后保留的尾部日志条目数 |
| `-log-level` | `info` | Raft 日志级别:`trace`/`debug`/`info`/`warn`/`error` |

非 Leader 节点收到写请求时,会按 `-peers` 查到 Leader 的 gRPC 地址并转发;查不到时返回 `UNAVAILABLE` 并提示补全 `-peers`。转发请求会携带 `x-orderraft-forwarded`,取值必须与 `-forward-token` 常量时间相等才会被信任,其余取值一律不可信,因此客户端伪造该 metadata 无法短路转发。

### 升级注意(破坏性变更)

- **`-forward-token` 是新增的必填项**:此前配置了 `-peers` 的部署在升级后若未提供 `-forward-token` 会直接启动失败。这是修复"客户端可伪造 `x-orderraft-forwarded` 短路转发"所必需的——入站标记不再被无条件信任。
- **集群内所有节点的 `-forward-token` 必须完全一致**:token 不一致时,入站转发标记在接收方会被判定为不可信,防环机制随之失效——两个互相指向对方的节点可能来回转发直到 `-forward-timeout`/调用方 deadline 超时,而不是快速返回 `already forwarded`。
- 旧版本对"同一 `request_id` + 不同命令"返回冲突错误,新版本改为返回首次决策(见 [API](#api) 的幂等契约)。

## API

服务定义见 `proto/order/v1/order.proto`,gRPC 明文监听 `50052`。

| RPC | 请求关键字段 | 说明 |
| --- | --- | --- |
| `CreateOrder` | `request_id`、`order_id`、`user_id`、`amount_cents`、`currency` | 创建订单;`order_id` 已存在时返回 `ALREADY_EXISTS`;重放时 `replayed=true` |
| `GetOrder` | `order_id` | 读取本地 FSM 中的订单;不存在返回 `NOT_FOUND` |
| `ChangeOrderStatus` | `request_id`、`order_id`、`target_status` | 状态流转;非法迁移返回 `FAILED_PRECONDITION` |

写接口都要求提供全局唯一的 `request_id`,服务端据此做幂等处理:

- 相同 `request_id` + 相同命令:返回首次执行结果,`replayed=true`
- 相同 `request_id` + 不同命令:以日志中先出现的决策为准,返回已记录结果并标记 `replayed=true`(**不会报错**);Raft 保证所有节点以相同顺序 apply 同一份日志,因此各节点结果一致
  - 这是明确契约:客户端**必须自行保证 `request_id` 与业务意图一一对应**。若误复用 `request_id`,服务端不会返回冲突错误,而是返回首次决策,业务意图不会被执行
  - 为便于发现误用,FSM 会累加 `IdempotencyConflicts` 计数,并在检测到"同一 `request_id` 携带不同命令"时输出告警日志(日志级别跟随 `-log-level`)
- 纯参数校验失败(`request_id`/`order_id` 为空、`amount_cents <= 0` 等)在 gRPC 层直接拒绝,不进入 Raft 日志,也不占用 `request_id`;修正参数后可用同一 `request_id` 重试
- 非 Leader 节点收到写请求时会先转发给 Leader;若在提交过程中发生 Leader 变更,会重新解析 Leader 并转发一次(见下),`GetOrder` 始终读取本节点 FSM

### 订单状态机

| 当前状态 | 允许流转到 |
| --- | --- |
| `CREATED` | `PAID`、`CANCELLED` |
| `PAID` | `SHIPPED`、`REFUNDING` |
| `SHIPPED` | `COMPLETED` |
| `REFUNDING` | `REFUNDED` |

其他任何迁移都会返回 `INVALID_STATUS_TRANSITION`。

## 项目结构

```text
OrderRaft/
├── cmd/
│   ├── main.go          # 服务端入口:参数解析、信号处理、优雅关闭
│   ├── flags.go         # 命令行参数与 -peers 解析
│   └── client/
│       └── main.go      # 客户端调用示例(含幂等重放演示)
├── proto/order/v1/      # proto3 定义(订单接口 + Raft 命令/快照消息)
├── gen/order/v1/        # 由 protoc 生成的 Go 代码
├── internal/
│   ├── domain/          # 订单状态流转校验
│   ├── fsm/             # Raft FSM:命令应用、幂等、快照、恢复
│   ├── grpcapi/         # gRPC 服务实现:参数校验、错误映射、Leader 转发
│   └── raftnode/        # Raft 节点封装(Apply / Leader / 关闭)
├── .github/workflows/   # CI:格式、静态检查、竞态测试、构建、生成代码校验
├── Makefile
└── go.mod / go.sum
```

## 实现细节

- **命令编码**:写操作编码为 `RaftCommand`(`request_id`、`schema_version`、操作与 `applied_at_unix_ms`),经确定性 protobuf 序列化后提交给 Raft。
- **参数校验分层**:纯参数校验在 `grpcapi` 完成;FSM 只保留与状态相关的校验(订单是否存在、状态迁移是否合法),并在入口保留一份纯校验作为防御。非法命令不会写入幂等表。
- **幂等**:FSM 为每个成功决策的 `request_id` 保存命令指纹与结果;重放时返回缓存结果并将 `replayed` 置为 `true`。同一 `request_id` 携带不同命令时同样返回首次决策,保证确定性收敛;此时指纹比对会命中差异,FSM 累加 `IdempotencyConflicts` 并在配置了日志时输出告警,使误用不至于完全不可见。
- **Leader 转发**:非 Leader 节点收到写请求时,按 Raft 报告的 Leader ID 在 `-peers` 中查到其 gRPC 地址,用生成的 client stub 原样转发并回传响应。转发请求携带 `x-orderraft-forwarded: <forward-token>`,只有取值与集群共享密钥常量时间相等的标记才会被信任,其余取值一律不可信;入站 metadata 不会被就地修改。收到可信标记的节点不会再次转发,避免节点之间来回弹跳。若 `IsLeader` 检查与 `Apply` 之间发生 Leader 变更,会重新解析 Leader 并转发一次。
- **Leader 变更重试的可观测行为**:`ErrLeadershipLost` 意味着原条目可能已经提交并 apply,此时转发重试会让同一命令第二次进入日志(多一条冗余条目);FSM 幂等表会拦截并返回 `replayed=true`。结果正确,但客户端可能对"首次提交却收到 `replayed=true`"感到意外,应以 `request_id` 为准判断。
- **优雅关闭**:收到 SIGINT/SIGTERM 后调用 `GracefulStop` 停止接收新请求并等待在途请求,超过 `-shutdown-timeout` 则强制停止并仅告警,保证 Raft 节点与连接仍被正常关闭。
- **快照**:`Snapshot()` 只短暂持有读锁抓取指针快照,克隆、排序与序列化都在锁外完成;按 `order_id`/`request_id` 排序后确定性序列化为 `FSMStateSnapshot`;`Restore()` 可完整恢复节点内存状态。`-snapshot-interval`/`-snapshot-threshold`/`-trailing-logs` 控制快照触发与日志保留,其中 `-snapshot-interval=0` 会禁用自动快照定时器(hashicorp/raft v1.7.3 的 `ValidateConfig` 拒绝 `SnapshotInterval == 0`,因此这里用一个不会触发的超大间隔等价实现)。
- **确定性**:排序的快照、确定性序列化、显式的时间戳与版本号,保证各节点状态可复现;测试覆盖"同一日志在不同节点状态一致"与"可交换命令顺序无关"。

## 已知限制与后续规划

- 当前使用内存存储(InmemStore / InmemSnapshotStore)与内存传输,进程重启后数据丢失
- 写请求虽然可以转发给 Leader,但集群仍是单节点:`-raft-addr` 目前只在进程内标识节点,节点发现与多节点集群要接入真实网络传输(`raft.NetworkTransport`)后才有意义
- 读请求(`GetOrder`)直接读本节点 FSM,多节点下可能读到旧数据,尚未提供线性一致读
- `-apply-timeout`(`raft.Apply` 的 timeout)只覆盖把命令**入队到 apply 通道**的阶段,不约束多数派复制与提交;Leader 失去多数派时写请求可能长时间阻塞,需要由调用方自己设置 gRPC deadline 兜底
- 幂等记录只增不减,长时间运行会持续占用内存并放大快照体积,尚未提供淘汰策略
- `InmemStore` 不截断日志,即使触发快照日志也会持续增长;真实部署需换成可截断的持久化 LogStore
- 转发使用明文连接与静态共享密钥,尚未支持 TLS 与 mTLS 鉴权
- Go module 名为 `example.com/OrderRaft`,如计划作为库被外部引用,建议调整 module path
- 可扩展方向:多节点集群、持久化日志与快照、gRPC TLS/鉴权、线性一致读校验、结构化日志与指标
