# OrderRaft 代码审查报告(第二轮)

| 项目 | 内容 |
| --- | --- |
| 审查对象 | 当前工作树(基于 `1f696ca` 的未提交修复,16 文件改动,+860/−362) |
| 审查范围 | 全部改动 diff、新增测试、CLI 契约、README 一致性、并发与快照路径 |
| 验证手段 | `make fmt-check` / `make vet` / `make test-race` / `make build` + 3 组临时探测(已删除) |
| 结论 | 上轮 P0/P1 全部修复且经验证有效;剩余 7 项为可清除的遗留成本与边界问题 |

> 说明:本次审查开始前工作树已被改动(修复上轮问题),因此本轮针对**当前工作树**重新审查。
> 上轮报告 `docs/code-review.md` 描述的是修复前状态,已过时,见 §4 的对照表。

## 1. 验证结果(全部通过)

| 检查项 | 命令 | 结果 |
| --- | --- | --- |
| 格式 | `make fmt-check` | 通过 |
| 静态检查 | `make vet` | 通过 |
| 竞态测试 | `make test-race` | 通过(7 包) |
| 构建 | `make build` | 通过 |

额外临时验证(写入临时测试 → 运行 → 立即删除,仓库未受影响):

| 验证项 | 方法 | 结果 |
| --- | --- | --- |
| 快照往返 | 快照 → `Persist` → `Restore` → 重放 | 恢复后 `PAID`/version=2,重放 `replayed=true`,且快照逐字节一致 |
| 新并发优化 | 4 写 goroutine × 4 Snapshot goroutine,`-race -count=2` | 无告警 |
| 非法参数不再污染 `request_id` | 黑盒:非法 `amount` 首失败 → 同 ID 修正后重试 | 第二次成功创建(旧版本会返回冲突) |

## 2. 修复验收

| 上轮问题 | 修复方式 | 验收 |
| --- | --- | --- |
| P0#1 幂等导致状态分歧 | 改为"日志中先出现者优先":同 `request_id` 命中即返回已记录决策,不再因指纹不同报错。所有节点按相同顺序 apply 同一份已提交日志,因此结果一致 | **成立**。`hashicorp/raft` 只在条目被多数派提交后才回调 `Apply`,故 FSM 记录的每个决策都来自已提交日志;`ReplaySuccessfulLogIsIdempotent` 与新增的 `TestApplySameLogProducesIdenticalState` 覆盖 |
| P0#2 伪造转发元数据 | 引入集群共享密钥 `ForwardToken`,入口无条件剥离该 key,仅常量时间比对上密钥的取值才可信 | **成立**。`takeForwardedFlag` 先 `delete` 再校验;`TestForgedForwardedMetadataIsIgnored` 覆盖 |
| P0#3 Leader 变更 TOCTOU | 捕获 `ErrNotLeader`/`ErrLeadershipLost` 后重新解析 Leader 并转发一次(已转发过的请求不再转发,防环) | **成立**。`TestLeaderChangeFallsBackToForwarding` 覆盖 |
| P1#4 `request_id` 被污染 | 纯参数校验前移到 `grpcapi`,FSM 仅保留状态相关校验;只有 `ResultCodeOK` 才写入幂等表 | **成立**。`TestInvalidRequestDoesNotPoisonRequestID` + 黑盒验证 |
| P1#5 Raft 旁路方法 | 删除 `ApplyCreate`/`ApplyChangeStatus` 及 `ErrOrderExists`/`ErrRequestConflict` | **成立**,无残留引用(`grep` 确认) |
| P1#6 快照从不触发 | 新增 `-snapshot-interval`/`-snapshot-threshold`/`-trailing-logs`,`Config` 同步透出,README 说明调小阈值可真正触发 | **成立**;`InmemStore` 不截断日志已在 README 已知限制中说明 |
| P1#7 快照持锁克隆 | `view()` 在读锁内只做 map 浅拷贝,克隆/排序/序列化移到锁外 | **成立**。安全性依赖"插入后不再就地修改"的不变量,当前代码满足;并发 `-race` 压测无告警 |
| P1#8 `Close` 后可重填连接 | 新增 `closed` 标志,`connection()` 在关闭后拒绝 | **成立**。`TestClosePreventsNewConnections` 覆盖 |
| P1#9 关闭超时被当致命错误 | `shutdown` 超时仅 `log.Printf` 告警并正常返回,`defer` 链得以执行 | **成立** |
| P1#10 启动期信号竞态 | `signal.NotifyContext` 提前到创建 Raft 节点之前 | **成立** |
| P2 项 | 多余空格、无意义拼接、`peerGRPCAddrs` 重复解析、`conn.Invoke` → 生成 stub、`-raft-addr` 注释、client 硬编码 ID、proto 空格、README 一致性 | 全部处理;`gofmt`/`vet` 干净 |

**新增的质量保障**:`internal/fsm/determinism_test.go` 补齐了上轮指出的关键测试缺口——同一日志状态一致、可交换命令顺序无关、重放不改状态。这正是原先让分歧缺陷逃逸的那类测试。

## 3. 剩余问题

### P1(建议在合并前处理)

#### 3.1 命令指纹已成为纯粹的遗留成本

**位置**:`internal/fsm/fsm.go`(`ApplyCommand` 每条命令都调用 `commandFingerprint`)、`internal/fsm/snapshot.go:107,262,290`

**问题**:旧语义下指纹用于判定"同一 `request_id` 被用于不同命令"。新语义改为"先出现者优先"后,指纹**不再参与任何判定**:

- `ApplyCommand` 对每条写入命令都执行一次确定性 proto marshal(纯 CPU 开销);
- `idempotencyRecord.fingerprint` 只增不减地占用内存,并随快照持久化、参与逐字节比较;
- `Restore` 中唯一的用途是校验"指纹非空"——这是对自身写入数据的恒真校验。

代码注释称其用于"诊断客户端误用",但没有任何路径读取它做诊断,属未兑现的设计意图。

**后果**:每条写请求白付一次 marshal + 每请求一条常驻内存 + 快照体积放大;真正发生 `request_id` 误用时,服务端既无错误也无日志,客户端静默拿到别的内容(见 3.2)。

**建议**:二选一并保持负载与意图一致。若不再需要误用检测,删除 `fingerprint` 字段、计算与快照字段(需同步 `proto` 与快照版本);若确需可观测性,则把它变成真实信号(命中不同指纹时计数/打日志),而不是"算了不用"。

#### 3.2 同 `request_id` 不同内容时静默返回首次结果

**位置**:`internal/fsm/fsm.go`(`ApplyCommand` 命中分支)、`README.md` 的幂等说明

**问题**:新语义对客户端误复用 `request_id` 不再报错,而是返回首次决策并标记 `replayed=true`。例如客户端用 `X` 支付订单 A,误复用 `X` 支付订单 B,会收到"支付成功 + 订单 A + replayed=true"——**HTTP/gRPC 层面成功,但业务意图完全未被执行**。旧语义会返回冲突错误。

**评估**:在集群一致性维度上,新行为是正确的(确定性、可收敛);但把"误用检测"完全移除后,错误只从"客户端立刻可见"变成"客户端永远不可见"。对于一个示例项目可接受,对真实支付语义不是。

**建议**:至少保留可观测性(3.1 的方案二);README 中把这一行为作为**明确契约**写清(当前已写"以先出现的决策为准",建议补一句"不会报错,客户端需自行保证 `request_id` 与业务意图一一对应")。

#### 3.3 `takeForwardedFlag` 就地修改 gRPC 入站 metadata map

**位置**:`internal/grpcapi/server.go` — `delete(incoming, ForwardedMetadataKey)`

**问题**:`metadata.FromIncomingContext` 返回的 `MD` 在 gRPC 内部由 map 持有,`MD` 无并发保护,文档要求"不得被并发读写"。就地 `delete` 会修改调用方仍可观察到的共享结构;一旦后续在同一次调用中新增拦截器/并发读取同一 `MD`,即成为数据竞争。这是为省一次 map 分配引入了隐式契约依赖。

**建议**:改用 `metadata.MD.Remove`(返回副本,不修改原 map):

```go
incoming, ok := metadata.FromIncomingContext(ctx)
if !ok {
    return false
}
values := incoming.Get(ForwardedMetadataKey)
if len(values) == 0 {
    return false
}
_, _ = incoming.Remove(ForwardedMetadataKey) // 返回剥离后的副本,不改动原 map
```

#### 3.4 未知错误全部映射为 `Unavailable`,故障定性失真

**位置**:`internal/grpcapi/server.go` — `mapRaftApplyError` 的 `default` 分支

**问题**:原先默认 `Internal`,现改为 `Unavailable`。动机合理(上游错误多为瞬态,避免客户端放弃重试),但副作用是**真正不可重试的错误**也被标成可重试:命令解码失败、FSM 应用失败、`future.Response()` 类型断言失败等都会表现为 `UNAVAILABLE`,客户端按重试语义持续重试,而服务端没有任何日志记录原始错误。

**建议**:保留显式的内部错误分支(如 `fsm.ApplyResponse.Err` 包装出的解码错误)映射为 `Internal`,并在映射前对未知错误 `log.Printf` 记录一次原始 error;或至少区分"提交阶段失败"(Unavailable)与"应用阶段失败"(Internal)。

### P2

#### 3.5 `-snapshot-interval` 拒绝 0,与 raft 语义不一致

**位置**:`cmd/flags.go` — `if o.snapshotInterval <= 0 { return errors.New("snapshot-interval 必须大于 0") }`

**问题**:`hashicorp/raft` 把 `SnapshotInterval == 0` 定义为"禁用自动快照定时器"(用户手动调 `raft.Snapshot()`),这是合法配置;当前 CLI 无法表达。

**建议**:允许 `0`(禁用),仅拒绝负值;或在帮助文本中说明"不支持禁用"。

#### 3.6 `-forward-token` 是未在早期版本出现的新必填项,属破坏性 CLI 变更

**位置**:`cmd/flags.go` — `len(o.peerAddrs) > 0 && forwardToken == ""` 时启动失败

**问题**:此前配置了 `-peers` 的部署升级后会直接启动失败。虽然"强制设置密钥"正是修复伪造漏洞所需,且错误信息清晰,但 README 未把它标为破坏性变更,运维容易踩坑。

**建议**:在 README 的升级说明中单列一条(需要新增"升级注意"小节),并强调**集群内所有节点的 token 必须一致**,否则转发会退化为 `already forwarded` 错误而不是完成转发。

#### 3.7 Leader 变更重试会向日志追加重复条目

**位置**:`internal/grpcapi/server.go` 两个写方法中的 `isLeaderChanged` 重试分支

**问题**:`ErrLeadershipLost` 意味着条目可能**已经被提交并 apply**,此时转发重试会让同一条命令第二次进入日志;FSM 以幂等表拦截,客户端会收到 `replayed=true`。结果正确,但客户端可能对"首次提交却返回 replayed"感到意外,且日志会多一条冗余条目。

**建议**:可接受,无需改动;建议在 README 或代码注释中记录这一可观测行为(目前注释只说明"重新解析 Leader 并转发一次")。

#### 3.8 `docs/code-review.md` 已过时

上一轮报告写于修复前,现在与代码矛盾(其中 P0/P1 多数已修复)。建议按 §4 更新为"已修复"状态或直接归档,避免后来者据过时报告做判断。

## 4. 上轮报告对照(便于更新 `docs/code-review.md`)

| 上轮条目 | 当前状态 |
| --- | --- |
| P0#1 幂等分歧 | 已修复并新增性质测试 |
| P0#2 伪造转发标记 | 已修复(`ForwardToken` + 入口剥离) |
| P0#3 Leader 变更 TOCTOU | 已修复(重试转发一次) |
| P1#4 `request_id` 污染 | 已修复(校验分层 + 仅缓存成功决策) |
| P1#5 Raft 旁路方法 | 已删除 |
| P1#6 快照从不触发 | 已修复(阈值/间隔/尾部日志可配) |
| P1#7 快照持锁克隆 | 已修复(锁内浅拷贝,锁外克隆) |
| P1#8 `Close` 后可重填连接 | 已修复 |
| P1#9 关闭超时退出码 | 已修复(仅告警) |
| P1#10 信号竞态 | 已修复 |
| P2 全部条目 | 已处理 |
| — | **新增**:§3.1 指纹遗留成本、§3.2 静默返回首次结果、§3.3 metadata 就地修改、§3.4 错误定性、§3.5 `snapshot-interval` 边界、§3.6 破坏性 CLI、§3.7 重复日志条目 |

## 5. 优点(本轮新增)

- **语义一致性论证清晰**:`raft_apply.go` 与 `ApplyCommand` 的注释明确解释了"仅 apply 已提交条目 + 先出现者优先 ⇒ 各节点结果一致"的推理链,可维护性好。
- **确定性测试到位**:`determinism_test.go` 的 `buildLog()` 覆盖成功、状态相关失败、`request_id` 复用、纯参数非法四类命令,并用快照字节比较断言状态等价——比逐字段断言更能捕获顺序敏感缺陷。
- **校验分层彻底且不重复失效**:`grpcapi` 做用户可见校验,FSM 入口保留一份纯校验作为防御,`ErrInvalidCommand` 统一包装。
- **`ForwardToken` 空值语义安全**:未配置时任何入站标记都不可信(而非放行),默认安全。
- **修复纪律好**:每项修复都补了对应回归测试(`TestForgedForwardedMetadataIsIgnored`、`TestLeaderChangeFallsBackToForwarding`、`TestInvalidRequestDoesNotPoisonRequestID`、`TestClosePreventsNewConnections`、`TestMapRaftApplyError`),README 与 CLI 帮助文本同步更新。

## 6. 建议处理顺序

1. §3.3 metadata 就地修改(一行改动,消除共享状态隐患)
2. §3.4 未知错误定性 + 原始错误日志(可观测性)
3. §3.1 + §3.2 指纹的最终决策:删除负载,或兑现为可观测信号(涉及 proto 与快照版本,建议明确取舍后一次做完)
4. §3.5 `snapshot-interval` 允许 0;§3.6 README 增补升级注意
5. §3.8 更新或归档 `docs/code-review.md`

## 7. 审查副作用声明

本轮审查为只读操作。三组临时探测测试(`internal/fsm/zz_*.go`、`cmd/client/zz_probe_*.go`)均在运行后立即删除;后台探测服务进程已终止;`git status` 除本次工作树既有改动外无新增文件。
