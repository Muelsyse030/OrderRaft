# OrderRaft 代码审查报告

> **状态:已过时(历史报告)**。本报告描述的是修复前的 commit `1f696ca`,其 P0/P1/P2 问题均已在后续修复中处理完毕,验收结果与当前遗留问题请见 `docs/code-review-round2.md`。请勿据本报告判断当前代码。

| 项目 | 内容 |
| --- | --- |
| 审查对象 | `example.com/OrderRaft`(commit `1f696ca`) |
| 审查范围 | 全部手写源码、proto 定义、单元测试、Makefile、CI 工作流、README |
| 审查方式 | 静态通读 + `go vet` + `go test` + `go test -race` + 缺陷复现实验 |
| 结论 | 单节点语义下无致命缺陷;多节点语义下的实现隐藏一致性风险 |

## 1. 验证结果

| 检查项 | 命令 | 结果 |
| --- | --- | --- |
| 静态检查 | `go vet ./...` | 通过 |
| 单元测试 | `go test ./...` | 通过(7 个包) |
| 竞态检测 | `go test -race -count=1 ./...` | 通过 |
| 缺陷复现 | 临时测试(运行后已删除) | 复现 2 个缺陷,见 §3.1、§4.2 |

## 2. 总体评价

工程素养明显高于一般示例项目:

- **分层清晰**:`domain`(状态迁移)/`fsm`(确定性状态机)/`raftnode`(共识封装)/`grpcapi`(接口与转发)职责边界明确。
- **错误处理规范**:统一 `%w` 包装,`errors.Is` 精确映射到 gRPC 状态码,对外错误可判别。
- **防御性编码**:nil receiver、超时参数、空 `request_id`、快照中的 nil/重复 ID 均有校验。
- **工程化完备**:proto 生成代码入 CI 校验、`gofmt`/`vet`/`-race`/`go mod tidy` 全部纳入流水线。
- **测试覆盖**:状态迁移、FSM、幂等、快照、Raft 节点、gRPC 服务层(含 Leader 转发)、CLI 参数。

核心矛盾在于:**README 把"多节点集群"列为后续规划,但代码已实现写请求转发、Leader 选主、快照恢复等分布式语义**。这些代码在单节点模式下无法被真正检验,而其中恰好包含会破坏 Raft 一致性不变量的缺陷。

## 3. P0 — 破坏一致性/正确性

### 3.1 幂等记录存于 FSM 而非日志,且会记录"未提交"条目的结果 → 多节点状态分歧

**位置**:`internal/fsm/fsm.go:65-92`(`ApplyCommand` 写入 `f.idempotencyRecords`)、`internal/fsm/raft_apply.go:17-35`

**问题**:幂等缓存是节点本地派生状态,来源于"被 apply 过的日志条目",而非"被提交的日志条目"。Raft 允许未提交条目被新 Leader 覆盖;此时 FSM 中已缓存 `request_id → 结果`,当该 `request_id` 以不同命令重新提交时,该节点会判定为指纹冲突并**拒绝执行**,而从未见过旧命令的节点则正常执行。两节点订单集合不同,且没有任何机制会收敛。

**复现证据**(临时测试,单进程内):

```text
nodeA order-A=order-A order-B见到? false
nodeB order-B=order-B order-A见到? false
=> 两个节点在同一 request_id 下得到不同订单,FSM 状态分歧
```

**影响**:分布式系统中不可恢复的一类错误;快照会把分歧永久固化,`Restore` 会将其复制到新节点。

**建议**:

1. 将请求去重下沉到提交路径(Leader 在提交前维护去重表,或把 `request_id → 决策` 作为日志内容的一部分);
2. 或至少让冲突处理**确定性且可交换**(以日志中先出现的条目为准,后续同 ID 条目直接返回已记录结果而不报错);
3. 补充"节点 apply 顺序不同应得到相同状态"的性质测试。

### 3.2 客户端可伪造 `x-orderraft-forwarded` 元数据,转发被短路

**位置**:`internal/grpcapi/server.go:27-28`(常量定义)、`internal/grpcapi/server.go:252-257`(检查)

**问题**:循环防护完全依赖入站 metadata,而该 metadata 由客户端控制。外部客户端只要携带 `x-orderraft-forwarded: true` 访问任意 Follower,就会在真正转发之前被判定为"已转发过一次",直接返回 `UNAVAILABLE`:写请求被拒绝,而 Leader 实际正常。

**影响**:转发链路的可用性可被客户端(或误配置的代理)轻易破坏。

**建议**:入口处先 `metadata.FromIncomingContext` 剔除该 key(边缘剥离);或改用仅存在于内部连接的标记(连接级状态、独立内部端口)。

### 3.3 `IsLeader()` 检查与 `Apply` 之间的 TOCTOU:失去 Leader 后既不转发也不重试

**位置**:`internal/grpcapi/server.go:131-137` + `:152-155`、`:206-212` + `:225-228`;错误映射见 `:305-308`

**问题**:检查通过后发生 Leader 变更,`Apply` 返回 `raft.ErrNotLeader` / `ErrLeadershipLost`,代码直接映射为 `UNAVAILABLE` 返回客户端。而这段代码的明确设计意图是"客户端无需感知集群拓扑",此处违背该意图。

**建议**:

- `ErrNotLeader` 时重新解析 Leader 并走 `forward`(需防止无限循环,已有 forwarded 标记可复用);
- `ErrLeadershipLost` 可安全重试一次;
- 或在错误详情中带回新 Leader 地址供客户端重定向。

## 4. P1 — 设计与健壮性

### 4.1 状态机校验在日志内部执行,失败结果写入幂等表 → `request_id` 被永久污染

**位置**:`internal/fsm/fsm.go:46-64`(进 Raft 前无法拦截)、`:68-76`(冲突检测先于一切)、`:88-91`(失败结果也缓存)

**问题**:

- `schema_version != 1`、`applied_at_unix_ms <= 0`、`request_id == ""` 等校验只在日志提交并复制之后才失败,浪费复制带宽并膨胀日志;
- 更严重的是,命令内容非法(如 `amount_cents <= 0`)同样被写入幂等表。客户端**换正确内容重试同一 `request_id`** 会命中指纹冲突,永久返回 `ALREADY_EXISTS`,与 README 承诺的"复用 `request_id` 即可安全重试"相矛盾。

**建议**:纯参数校验前移到 `grpcapi`;FSM 只保留与状态相关的校验;不缓存"确定性可重算"的失败,或显式区分"已执行决策"与"未执行拒绝"。

### 4.2 `ApplyCreate` / `ApplyChangeStatus` 是绕过 Raft 与幂等表的旁路

**位置**:`internal/fsm/fsm.go:167-242`

**问题**:这两个导出方法可直接修改内存状态、不写幂等表、不进日志;`grep` 确认生产代码零调用,仅 `internal/fsm/fsm_test.go` 引用。复现:

```text
second ApplyCreate err=<nil> order=order-002
=> 同一 request_id 生成第二个订单,幂等表未被写入
```

**影响**:对 `internal/` 内任意包可见,一旦误用即静默破坏一致性;同时与 FSM 的校验逻辑重复维护。

**建议**:删除,或降为非导出并加 `// 仅供测试` 注释;测试改用 `ApplyCommand`。

### 4.3 声明了快照能力,但快照永远不会被触发

**位置**:`internal/raftnode/node.go:75-77`(仅透出 `SnapshotInterval`)、`:84-85`(InmemStore / InmemSnapshotStore)

**事实**:hashicorp/raft `DefaultConfig` 为 `SnapshotInterval=120s`、`SnapshotThreshold=8192`。`shouldSnapshot()` 要求 `lastIdx - lastSnap >= SnapshotThreshold`,而阈值既不可配置,项目也不调用 `raft.Snapshot()`。因此:

- 快照/恢复代码在真实运行中**从未执行**;
- InmemStore 永不截断日志,日志无限增长。

**建议**:将 `SnapshotThreshold` / `TrailingLogs` 暴露到 `Config` 与 CLI,或启动后周期性调用 `raft.Snapshot()`;同步校正文档。

### 4.4 快照持读锁完成全部序列化,手动 `RUnlock` 分散在多个分支

**位置**:`internal/fsm/snapshot.go:29`(加锁)至 `:99`(释放),`:101-109` 在锁外 marshal

**问题**:`Snapshot()` 在 `RLock` 内完成所有订单克隆与两轮 ID 排序,长时间阻塞全部写操作(`Apply` 需写锁)。手动解锁散布在 `:41`、`:70`、`:79` 三处错误分支,后续修改极易漏解锁或双重解锁。

**建议**:锁内只做"指针/切片快照 + 排序键收集",克隆与排序移到锁外;或统一用 `defer` 收敛解锁路径。

### 4.5 `Close()` 之后连接表可被重新填充

**位置**:`internal/grpcapi/server.go:102-121`(`Close`)、`:286-299`(`connection`)

**问题**:`Close` 清空 `s.conns` 后,若仍有并发 `forward` 进入,`connection()` 会新建连接并重新写入 map,随后无人关闭。

**建议**:增加 `closed bool` 标志,`connection` 在已关闭时返回错误。

### 4.6 优雅关闭超时被视为致命错误,`defer` 清理链被跳过

**位置**:`cmd/main.go:106` 返回错误 → `:25` `log.Fatalf`(内部 `os.Exit(1)`)

**问题**:`shutdown()` 超时返回 error 是**设计内的**正常路径,但 `main` 用 `log.Fatalf` 处理,`os.Exit` 会跳过 `run` 中所有 `defer`(`api.Close()`、`node.Close()`、`listener.Close()`)。Raft 节点无法优雅关闭,与 README 的"优雅关闭"承诺相悖。

**建议**:超时仅 `log.Printf` 告警并正常返回,让 `defer` 完成收尾。

### 4.7 启动期信号竞态

**位置**:`cmd/main.go:88-93`(`Serve` 启动后才注册 `signal.NotifyContext`)

**问题**:在 `Serve` 与信号注册之间到达的 SIGTERM 走默认行为直接杀进程,跳过优雅关闭。

**建议**:把 `signal.NotifyContext` 提到 `Serve` 之前。

## 5. P2 — 代码质量与文档一致性

| # | 位置 | 问题 |
| --- | --- | --- |
| 1 | `internal/fsm/fsm.go:70` | `"%w , %s"` 逗号前多空格 |
| 2 | `internal/domain/order_status.go:30` | 错误信息末尾多余空格 `"%s -> %s "` |
| 3 | `internal/fsm/fsm.go:226-227` | `"" + "%w: %s"` 无意义字符串拼接残留 |
| 4 | `cmd/flags.go:45-47`、`:70-72` | `peerGRPCAddrs()` 被解析两次(校验 + 取用);可在 `parseOptions` 解析一次并回填 |
| 5 | `internal/grpcapi/server.go:301-318` | `mapRaftApplyError` 默认落到 `Internal`;除 `ErrEnqueueTimeout` 外的上游错误多为瞬态,更适合 `Unavailable`,以免客户端放弃重试 |
| 6 | `internal/grpcapi/server.go:282` | 使用 `conn.Invoke` 直接调用,绕过拦截器与重试策略;宜改用生成的 client stub |
| 7 | `internal/raftnode/node.go:22-23,79-82` | 注释称"接入真实网络传输后填 host:port",但 `-raft-addr` 当前完全未生效,易误导 |
| 8 | `cmd/client/main.go:26,52` | 硬编码 `request-001/002` + `order-001`,重复运行必然返回 `ALREADY_EXISTS`/`FailedPrecondition`,示例开箱即失败 |
| 9 | `proto/order/v1/command.proto:25` | `oneof operation{` 缺空格(无功能影响) |
| 10 | `README.md:17,176,184` | "快照与恢复""幂等记录会放大快照"与实际(快照从不触发)不符;`-raft-addr` 未标注未生效 |
| 11 | `internal/fsm/*_test.go` | 全部为 `package fsm` 白盒测试;缺少"不同 apply 顺序得到相同状态"的性质测试 —— 这正是 §3.1 逃逸的原因 |
| 12 | 全局 | 写路径无结构化日志与指标;已依赖 `go-metrics` 但未接入 |

## 6. 优点(建议保持)

- proto 使用 `paths=source_relative`,CI 以 `git diff -I'^//'` 忽略生成文件头部版本差异(`.github/workflows/ci.yml:63-67`),生成代码校验细致。
- 命令指纹只覆盖 `operation` 内容,刻意排除 `applied_at_unix_ms`,使"同请求不同时间"正确识别为重放(`internal/fsm/fsm.go:258-301`,测试 `internal/fsm/command_test.go:37-62` 验证)。
- 快照按 `order_id`/`request_id` 排序 + 确定性 marshal(`internal/fsm/snapshot.go:31-97`),思路正确(仅未被触发执行)。
- `Restore` 先构建新 map 再原子替换,并完整校验重复 ID、nil、`request_id` 不匹配(`internal/fsm/snapshot.go:199-284`)。
- 对外返回一律 `cloneOrder`/`proto.Clone`,有效防止别名泄漏(`internal/fsm/fsm_test.go:131` 有对应测试)。
- 转发链路有"只转发一次"的循环防护设计(缺陷仅在信任边界,见 §3.2)。

## 7. 建议修复顺序

1. **§3.1 幂等语义**:决定项目定位。若短期只做单节点,应在代码中明确断言并隔离/删除转发与快照的半成品实现,而非保留看似可用的代码。
2. **§3.2 转发元数据伪造**、**§3.3 Leader 切换重试**:改动小、收益直接。
3. **§4.1 校验分层与 `request_id` 污染**、**§4.2 删除 Raft 旁路方法**。
4. **§4.3 快照阈值**、**§4.6/§4.7 关闭与信号竞态**。
5. P2 清理与文档校正,并补充"顺序无关"的 FSM 性质测试作为回归防线。

## 8. 审查副作用声明

本次审查为只读操作。除两个仅用于验证、运行后已删除的临时测试文件外,未修改仓库任何文件(`git status` 保持干净)。本报告为唯一新增产物。
