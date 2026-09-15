package fsm

import (
	"errors"
	"fmt"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

var (
	// ErrApplyFailed 表示日志条目已经提交,但在 FSM 应用阶段失败(日志无法解码、
	// FSM 返回了非法响应等)。这类错误与请求参数无关,重试不会改变结果,
	// 上层应映射为 Internal 而不是可重试的 Unavailable。
	ErrApplyFailed = errors.New("apply raft log entry failed")
)

type ApplyResponse struct {
	Result *orderv1.RaftCommandResult
	Err    error
}

// Apply 实现 raft.FSM。hashicorp/raft 只在日志条目被多数派提交后才调用 Apply,
// 因此这里记录的每个 request_id 决策都来自已提交的日志;配合 ApplyCommand 的
// "先出现者优先"规则,各节点以相同顺序 apply 同一份日志会得到完全一致的状态。
//
// 按 raft 文档要求,Apply 只处理 LogCommand 条目;LogConfiguration/LogNoop/
// LogBarrier 等非命令条目直接忽略。返回值始终是 *ApplyResponse,以满足 runFSM
// 的类型断言。
func (f *FSM) Apply(logEntry *raft.Log) interface{} {
	if logEntry == nil {
		f.logError("raft log entry is nil")

		return &ApplyResponse{
			Err: fmt.Errorf("%w: raft log is required", ErrApplyFailed),
		}
	}

	if logEntry.Type != raft.LogCommand {
		// 与 raft 文档一致:非命令条目不由 FSM 处理。
		// 目前框架只在实现了 ConfigurationStore 时才会把 LogConfiguration 送进 Apply,
		// 这里显式忽略,避免未来实现该接口后把配置条目误判为解码失败。
		return &ApplyResponse{}
	}

	command := &orderv1.RaftCommand{}

	if err := proto.Unmarshal(logEntry.Data, command); err != nil {
		decodeErr := fmt.Errorf(
			"%w: decode raft command: %w",
			ErrApplyFailed,
			err,
		)

		// 快照恢复回放路径不会检查 Apply 的返回值,这里必须自己留痕:
		// 否则该节点会静默缺失一条已提交的状态变更。
		f.logError(
			"failed to decode committed raft log entry",
			"index", logEntry.Index,
			"term", logEntry.Term,
			"error", decodeErr,
		)

		return &ApplyResponse{Err: decodeErr}
	}
	result, err := f.ApplyCommand(command)
	return &ApplyResponse{Result: result, Err: err}
}
