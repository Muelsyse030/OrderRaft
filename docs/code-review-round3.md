# OrderRaft 代码审查报告(第三轮)

| 项目 | 内容 |
| --- | --- |
| 审查对象 | 当前工作树(head 仍为 `1f696ca`,但工作树已含后续多轮修复) |
| 审查重点 | 前三轮未覆盖区域 + 对当前工作树的独立复核 |
| 验证手段 | `gofmt` / `go vet` / `go test -race` / `staticcheck` / `go mod tidy` / 真实 `protoc` 重生成 / 端到端运行 |
| 结论 | 三轮累计问题已基本清空;剩 4 项遗留,其中 3 项为可安全清除的死代码与轻微资源问题 |

> **重要前提**:本仓库的工作树在审查期间持续变化。第二轮报告(`docs/code-review-round2.md`)写作时的 7 项遗留问题**现已全部修复**,该报告已过时;`docs/code-review.md` 已自行标注为历史报告。本报告针对的是**当前工作树**。

## 1. 验证结果(全部通过)

| 检查项 | 命令 | 结果 |
| --- | --- | --- |
| 格式 | `gofmt -l .` | 无输出 |
| 静态检查 | `go vet ./...` | 通过 |
| 竞态测试 | `go test -race -count=1 ./...` | 通过(7 包) |
| 依赖整洁 | `go mod tidy` + `git diff --exit-code go.mod go.sum` | 无差异 |
| **静态分析(新增)** | `staticcheck ./...` | **手写代码零告警**;仅生成代码报 SA1019(见 §1.1) |
| **生成代码一致性(新增)** | 真实 `protoc` 3.21.12 + 声明版本插件重新生成 | **与提交内容逐字节一致** |
| 端到端:快照 | `-snapshot-threshold 5 -snapshot-interval 1s` + 客户端压测 | 日志出现 `starting snapshot up to: index=8` / `snapshot complete` |
| 端到端:优雅关闭 | `kill -TERM` | `收到退出信号…` → `gRPC 服务已优雅关闭`,进程干净退出 |
| 端到端:客户端幂等演示 | 连续运行两次 `go run ./cmd/client` | 两次均成功,`replayed=true`,订单 ID 各自唯一 |

### 1.1 生成代码的 staticcheck 告警(非缺陷)

```
gen/order/v1/*.pb.go: (protoimpl.MessageInfo).Exporter is deprecated (SA1019)
```

由 `protoc-gen-go v1.34.2` 相对当前 `google.golang.org/protobuf v1.36.12` 偏旧引起,属生成代码固有现象。若希望在 CI 中启用 staticcheck,需排除 `gen/`;或升级插件版本重新生成(升级会改变文件头版本号,CI 的 `git diff -I'^//'` 规则仍能容忍)。

## 2. 本轮新发现

### 2.1 快照序列化结果被多余地复制一次

**位置**:`internal/fsm/snapshot.go:39`

```go
data, err := proto.MarshalOptions{Deterministic: true}.Marshal(state)
...
return &raftSnapshot{
    data: append([]byte(nil), data...),   // ← data 已是独占的新缓冲区
}, nil
```

**问题**:`proto.Marshal` 返回的切片由本次调用独占,调用方持有所有权;`Snapshot()` 返回后没有任何别名共享它。这次 `append` 会再分配一整份快照大小的内存并复制,随后 `data` 立即变成垃圾。快照大小与订单数+幂等记录数成正比,是该项目内存占用最大的对象,等于让**每次快照的峰值内存翻倍**且多一次全量拷贝。

**建议**:直接 `data: data`。若担心后续有人在 marshal 之外复用该切片,应改为注释说明所有权,而不是留一次无意义的深拷贝。

### 2.2 关闭超时路径丢弃 `GracefulStop` 协程

**位置**:`cmd/main.go:121-139`

**问题**:`shutdown` 在 `timer.C` 分支调用 `server.Stop()` 后直接返回,而承载 `server.GracefulStop()` 的协程无人等待——它依赖 `Stop()` 才能返回,期间一直挂起。该协程最多在进程退出前存活,属轻微资源泄漏;更关键的是**关闭超时路径没有测试覆盖**,而这是 README 明确承诺的行为。

**建议**:返回前等待该协程收尾(`Stop()` 后 `<-done`),并补一个"用不会退出的在途 RPC 触发超时分支"的单元测试;或把 `GracefulStop` 放进可取消的封装里。

### 2.3 `FSM.Apply` 未按 raft 契约检查日志类型

**位置**:`internal/fsm/raft_apply.go:27`

**事实**:`hashicorp/raft` 的 `FSM` 接口文档明确要求:

> These log entries … could be of a few log types. **Clients should check the log type prior to attempting to decode the data attached.** Presently the `LogCommand` and `LogConfiguration` types will be sent.

`OrderRaft` 的 `Apply` 无类型判断,把任意 `log.Data` 当 `RaftCommand` 解码。实测:

```text
LogConfiguration -> err=apply raft log entry failed: decode raft command: proto: cannot parse invalid wire-format data
LogNoop          -> err=apply raft log entry failed: decode raft command: ...
LogBarrier       -> err=apply raft log entry failed: decode raft command: ...
```

**当前为何没出问题**:框架只在 `fsm.Apply` 前分派 `LogCommand`,而 `LogConfiguration` 仅当 FSM 实现 `ConfigurationStore` 时才回落到 `Apply`——本项目未实现该接口(`grep` 确认无 `StoreConfiguration`)。也就是说,**当前正确性依赖一个未被任何注释或测试固定的隐式事实**。

**风险**:一旦为快照/配置感知而实现 `ConfigurationStore`(raft 示例中的常见做法),成员变更条目会立即进入 `Apply` 并被误判为解码失败;错误被静默吞掉(见 §2.4),只有在用户 `Apply` 的 future 上才可能外显。

**建议**:在 `Apply` 开头按文档显式判断类型,非 `LogCommand` 直接返回 `nil`/忽略:

```go
if logEntry.Type != raft.LogCommand {
    // 与 raft 文档一致:非命令条目(LogConfiguration/LogNoop/LogBarrier)不由 FSM 处理
    return &ApplyResponse{}
}
```

注意 `runFSM` 会对 `Apply` 的返回值做 `*ApplyResponse` 类型断言,因此**任何分支都必须返回 `*ApplyResponse`**(当前实现满足,新增分支需保持)。

### 2.4 FSM 应用阶段错误在底层被静默吞掉

**位置**:`internal/fsm/raft_apply.go:36-44`(产生错误);框架侧:`hashicorp/raft` 的 `runFSM` 只把 `response` 交给 future,**不检查也不记录**;快照恢复回放路径更直接 `_ = fsm.Apply(&entry)`。

**事实**:若崩溃恢复时某条已提交日志解码失败,该条目被静默跳过——该节点会**永久缺失一条状态变更**,而 Raft 与调用方都不知道。`ErrApplyFailed` 目前只在两条路径可见:用户触发的 `Node.Apply`(经 `mapRaftApplyError` → `Internal`),以及 `fsm.NewWithLogger` 场景下 `ApplyCommand` 的日志。

**建议**:`Apply` 的解码失败分支用 `f.logger`(已具备)记录一次 `Error`,让恢复期回放失败在任何情况下都留痕;这是**低成本、高收益**的一行防御。

## 3. 第二轮遗留问题的最终状态

| 二轮条目 | 当前状态 | 依据 |
| --- | --- | --- |
| 3.1 指纹成为纯粹遗留成本 | **已修复**:指纹改为真实信号 | `fsm.go:116-118` 命中不同指纹时 `recordIdempotencyConflict`;`:151-162` 累加计数并 `slog.Warn`;`command_test.go:188` 断言计数为 1 |
| 3.2 同 `request_id` 不同内容静默返回 | **已修复**:行为可观测 | 计数 `IdempotencyConflicts()` + 告警日志 |
| 3.3 就地修改入站 metadata | **已修复** | `server.go:383-408` 改为只读判断,注释说明为何不就地删除 |
| 3.4 未知错误全部 `Unavailable` | **已修复** | `server.go:426-428` 新增 `ErrApplyFailed` → `Internal`;`:439-443` 未归类错误记录原始日志后再返回 `Unavailable` |
| 3.5 `-snapshot-interval` 拒绝 0 | **已修复** | `flags.go:89-93` 允许 0(禁用)并拒绝负值/过小值;`node.go:56,96-97` 以 `disabledSnapshotInterval` 关闭定时器 |
| 3.6 破坏性 CLI 变更无文档 | **已修复** | README 新增「升级注意(破坏性变更)」小节,含 token 必须一致的说明 |
| 3.7 `ErrLeadershipLost` 重复入日志条目 | **已修复(文档层面)** | `server.go:169-172,252-253` 明确注释该冗余条目与 `replayed=true` 的可观测后果 |
| 3.8 `docs/code-review.md` 过时 | **已修复** | 文件头已加「状态:已过时(历史报告)」提示 |

**新增的正面变化**:`FSM` 引入 `slog`(`NewWithLogger`)、`cmd/main.go:141-153` 把 `-log-level` 映射为 slog 级别、`cmd/flags.go` 新增 `DisableSnapshots` 语义,README 同步更新。日志与指标方向的改进方向正确。

## 4. 测试缺口

| 缺口 | 说明 |
| --- | --- |
| 关闭超时分支 | `shutdown()` 的 `timer.C` 分支(强制 `Stop`)无测试,而这是 README 承诺的行为,也是 §2.2 的所在地 |
| 非 `LogCommand` 条目 | 无测试固定"FSM 只处理命令条目"的契约(§2.3) |
| `FSM.Apply` 解码失败 | `ErrApplyFailed` 的生成路径有映射测试(`TestMapRaftApplyError`),但没有"日志损坏时 FSM 行为"的测试 |
| 多节点一致性 | 目前全部为单节点/存根测试。`-peers` + `ForwardToken` 的真实双节点互通(含 token 不一致时的行为)无端到端验证,README 已说明属规划中,建议至少在测试中固定"token 不匹配 ⇒ 不转发"的语义 |

## 5. 优点(第三轮独立复核确认)

- **接口契约正确性**:`Apply` 的返回值始终保持 `*ApplyResponse`,满足 `runFSM` 的类型断言(`resp = r.fsm.Apply(...)` 后直接断言),不会 panic——这一点作者显然是有意为之。
- **指纹的最终用法**比"删除"更好:保留确定性 marshal + `bytes.Equal`,把误用变成**可观测信号**而非错误,既维持了"先出现者优先"的确定性,又恢复了可见性。
- **常量时间比较**用于 token 校验,且未配置 token 时默认不可信(默认安全)。
- **`disabledSnapshotInterval = 1 << 62`** 的写法正确落在 `randomTimeout` 的 `int64` 范围内,不会溢出为负值导致定时器风暴。
- **`Snapshot()` 的锁外克隆**:实测 4 写 × 4 快照并发在 `-race` 下无告警,且"插入后不再就地修改"的不变量在 `applyCreateLocked`/`applyChangeStatusLocked`/幂等表写入三处都成立。

## 6. 建议处理顺序

1. **§2.4 恢复期回放失败留痕**(一行日志,收益最大)
2. **§2.3 显式检查日志类型**(按 raft 文档补齐契约,防未来回归)
3. **§2.1 删除多余快照拷贝**(纯收益,零风险)
4. **§2.2 关闭超时路径收尾 + 补测试**
5. 文档:在 README「已知限制」中补一条"apply 超时只覆盖入队阶段",避免运维误判(`raft.Apply` 的 timeout 仅对 `applyCh` 入队生效,提交阶段可能无限等待)

## 7. 审查副作用声明

本轮为只读审查。临时探测测试(`internal/fsm/zz_*.go`)与后台服务进程均已清理,`/tmp` 下构建产物已删除;仓库仅新增本报告,`git status` 中除既有改动外无新增文件。
