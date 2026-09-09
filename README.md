# OrderRaft

基于 [hashicorp/raft](https://github.com/hashicorp/raft) 与 gRPC 的订单服务示例。它把订单的写操作建模为 Raft 日志命令:由 Leader 复制并提交后,在有限状态机(FSM)中按序执行,从而使集群中的每个节点都拥有完全一致的订单数据视图。

> 当前为单节点演示实现:使用内存日志存储与内存传输,服务重启后数据会丢失。后续可替换为持久化存储与真实网络传输,扩展成多节点集群。

## 功能特性

- gRPC API:订单创建、查询、状态流转(proto3 定义)
- Raft 共识:所有写操作提交到 Raft 日志,保证节点间状态一致
- 确定性 FSM:订单数据与幂等记录统一在 FSM 内应用
- 幂等去重:以 `request_id` 为键并校验命令指纹;重放请求返回首次执行结果且标记 `replayed=true`
- 状态流转校验:非法迁移(如已取消后再次支付)会被拒绝
- 快照与恢复:FSM 支持确定性序列化的快照,可完整恢复内存状态
- 单元测试:状态流转、FSM、幂等、快照、Raft 节点均有覆盖

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
- 仅在需要重新生成 proto 代码时需要 `protoc`、`protoc-gen-go`、`protoc-gen-go-grpc`(生成代码已提交在 `gen/`)

### 运行测试

```bash
make test   # 等价于 go test ./...
```

### 启动服务端

```bash
go run ./cmd
```

服务端会启动一个单节点 Raft,并在 `:50052` 上提供 gRPC 服务。

### 运行客户端示例

```bash
go run ./cmd/client
```

客户端演示完整链路:创建订单(`order-001`)→ 支付 → 查询订单。

### 构建

```bash
make build  # go build ./...
```

### 重新生成 proto

```bash
make proto  # 需要 protoc 工具链
```

## API

服务定义见 `proto/order/v1/order.proto`,gRPC 明文监听 `50052`。

| RPC | 请求关键字段 | 说明 |
| --- | --- | --- |
| `CreateOrder` | `request_id`、`order_id`、`user_id`、`amount_cents`、`currency` | 创建订单;`order_id` 已存在时返回 `ALREADY_EXISTS` |
| `GetOrder` | `order_id` | 读取本地 FSM 中的订单;不存在返回 `NOT_FOUND` |
| `ChangeOrderStatus` | `request_id`、`order_id`、`target_status` | 状态流转;非法迁移返回 `FAILED_PRECONDITION` |

写接口都要求提供全局唯一的 `request_id`,服务端据此做幂等处理:

- 相同 `request_id` + 相同命令(指纹一致):返回首次执行结果,`replayed=true`
- 相同 `request_id` + 不同命令(指纹不一致):返回请求冲突错误

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
│   ├── main.go          # gRPC 服务端入口(单节点 Raft)
│   └── client/
│       └── main.go      # 客户端调用示例
├── proto/order/v1/      # proto3 定义(订单接口 + Raft 命令/快照消息)
├── gen/order/v1/        # 由 protoc 生成的 Go 代码
├── internal/
│   ├── domain/          # 订单状态流转校验
│   ├── fsm/             # Raft FSM:命令应用、幂等、快照、恢复
│   └── raftnode/        # Raft 节点封装(Apply / Leader / 关闭)
├── Makefile
└── go.mod / go.sum
```

## 实现细节

- **命令编码**:写操作编码为 `RaftCommand`(`request_id`、`schema_version`、操作与 `applied_at_unix_ms`),经确定性 protobuf 序列化后提交给 Raft。
- **幂等**:FSM 为每个 `request_id` 保存命令指纹与结果;重放时直接返回缓存结果,并将 `replayed` 置为 `true`。
- **快照**:`Snapshot()` 按 `order_id`/`request_id` 排序后确定性序列化为 `FSMStateSnapshot`;`Restore()` 可完整恢复节点内存状态。
- **确定性**:排序的快照、确定性序列化、显式的时间戳与版本号,保证各节点状态可复现。

## 已知限制与后续规划

- 当前使用内存存储(InmemStore / InmemSnapshotStore)与内存传输,进程重启后数据丢失
- 目前只支持单节点启动,尚未提供多节点集群配置与节点发现
- Go module 名为 `example.com/OrderRaft`,如计划作为库被外部引用,建议调整 module path
- 可扩展方向:多节点集群、持久化日志与快照、gRPC TLS/鉴权、线性一致读校验
