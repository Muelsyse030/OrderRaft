package fsm

import (
	"bytes"
	"errors"
	"testing"

	orderv1 "example.com/OrderRaft/gen/order/v1"
)

// buildLog 构造一段包含成功、状态相关失败、request_id 冲突与非法命令的"日志"。
// 非法命令不会进入真实 Raft 日志(grpcapi 已拦截),这里用于验证 FSM 入口的确定性。
func buildLog() []*orderv1.RaftCommand {
	invalidCreate := newCreateCommand("request-007", "order-007", 1700000007000)
	invalidCreate.GetCreateOrder().AmountCents = 0

	return []*orderv1.RaftCommand{
		newCreateCommand("request-001", "order-001", 1700000000000),
		newChangeStatusCommand(
			"request-002",
			"order-001",
			orderv1.OrderStatus_ORDER_STATUS_PAID,
			1700000001000,
		),
		// 同一 request_id + 相同命令：重放。
		newCreateCommand("request-001", "order-001", 1700000009999),
		// 同一 request_id + 不同命令：以先出现的决策为准。
		newCreateCommand("request-001", "order-002", 1700000002000),
		newCreateCommand("request-003", "order-002", 1700000003000),
		// request-002 已决策(order-001 -> PAID)，该条不得改变 order-002。
		newChangeStatusCommand(
			"request-002",
			"order-002",
			orderv1.OrderStatus_ORDER_STATUS_PAID,
			1700000004000,
		),
		// 状态相关失败：不写入幂等表，稍后修正条件可成功。
		newChangeStatusCommand(
			"request-004",
			"order-404",
			orderv1.OrderStatus_ORDER_STATUS_PAID,
			1700000005000,
		),
		newCreateCommand("request-006", "order-404", 1700000006000),
		newChangeStatusCommand(
			"request-004",
			"order-404",
			orderv1.OrderStatus_ORDER_STATUS_PAID,
			1700000006500,
		),
		// 订单已存在：状态相关失败。
		newCreateCommand("request-005", "order-001", 1700000008000),
		// 纯参数非法：返回 error，且不占用 request_id。
		invalidCreate,
		newCreateCommand("request-007", "order-007", 1700000007500),
	}
}

// 同一份已提交日志在不同节点上 apply 后必须得到完全一致的状态。
func TestApplySameLogProducesIdenticalState(t *testing.T) {
	log := buildLog()

	nodeA := New()
	nodeB := New()

	applyLog(t, nodeA, log)
	applyLog(t, nodeB, log)

	stateA := snapshotData(t, nodeA)
	stateB := snapshotData(t, nodeB)

	if !bytes.Equal(stateA, stateB) {
		t.Fatal("两个节点 apply 同一份日志后状态不一致")
	}
}

// 不相交(可交换)的命令即使 apply 顺序不同，最终状态也必须一致。
func TestApplyDisjointCommandsIsOrderIndependent(t *testing.T) {
	bootstrap := []*orderv1.RaftCommand{
		newCreateCommand("request-a", "order-a", 1700000000000),
		newCreateCommand("request-b", "order-b", 1700000001000),
		newCreateCommand("request-c", "order-c", 1700000002000),
	}

	changes := []*orderv1.RaftCommand{
		newChangeStatusCommand(
			"request-d",
			"order-a",
			orderv1.OrderStatus_ORDER_STATUS_PAID,
			1700000003000,
		),
		newChangeStatusCommand(
			"request-e",
			"order-b",
			orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
			1700000004000,
		),
		newCreateCommand("request-f", "order-f", 1700000005000),
	}

	forward := New()
	applyLog(t, forward, bootstrap)
	applyLog(t, forward, changes)

	reversed := New()
	applyLog(t, reversed, bootstrap)
	for i := len(changes) - 1; i >= 0; i-- {
		applyLog(t, reversed, []*orderv1.RaftCommand{changes[i]})
	}

	if !bytes.Equal(snapshotData(t, forward), snapshotData(t, reversed)) {
		t.Fatal("可交换命令在不同 apply 顺序下得到了不同状态")
	}
}

// 重放已经成功决策的日志不会改变状态，也不会产生新订单。
func TestReplaySuccessfulLogIsIdempotent(t *testing.T) {
	commands := []*orderv1.RaftCommand{
		newCreateCommand("request-001", "order-001", 1700000000000),
		newCreateCommand("request-002", "order-002", 1700000001000),
		newChangeStatusCommand(
			"request-003",
			"order-001",
			orderv1.OrderStatus_ORDER_STATUS_PAID,
			1700000002000,
		),
	}

	state := New()
	applyLog(t, state, commands)

	before := snapshotData(t, state)

	for _, command := range commands {
		result, err := state.ApplyCommand(command)
		if err != nil {
			t.Fatalf("replay command %s: %v", command.GetRequestId(), err)
		}
		if !result.GetReplayed() {
			t.Fatalf("replay of %s should be marked replayed", command.GetRequestId())
		}
	}

	if !bytes.Equal(before, snapshotData(t, state)) {
		t.Fatal("重放成功日志后状态发生了变化")
	}
}

func applyLog(t *testing.T, state *FSM, commands []*orderv1.RaftCommand) {
	t.Helper()

	for _, command := range commands {
		if _, err := state.ApplyCommand(command); err != nil &&
			!errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("apply command %s: %v", command.GetRequestId(), err)
		}
	}
}

func snapshotData(t *testing.T, state *FSM) []byte {
	t.Helper()

	snapshot, err := state.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}
	defer snapshot.Release()

	raftSnapshot, ok := snapshot.(*raftSnapshot)
	if !ok {
		t.Fatalf("unexpected snapshot type: %T", snapshot)
	}

	return append([]byte(nil), raftSnapshot.data...)
}
