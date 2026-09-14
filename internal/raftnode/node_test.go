package raftnode

import (
	"testing"
	"time"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"example.com/OrderRaft/internal/fsm"

	"github.com/hashicorp/raft"
)

func TestNormalizeLogLevel(t *testing.T) {
	cases := map[string]string{
		"":        "info",
		"   ":     "info",
		"INFO":    "info",
		" Debug ": "debug",
		"warn":    "warn",
		"error":   "error",
	}

	for input, expected := range cases {
		actual, err := normalizeLogLevel(input)
		if err != nil {
			t.Fatalf("normalizeLogLevel(%q) error = %v", input, err)
		}
		if actual != expected {
			t.Fatalf(
				"normalizeLogLevel(%q) = %q, 期望 %q",
				input,
				actual,
				expected,
			)
		}
	}

	if _, err := normalizeLogLevel("verbose"); err == nil {
		t.Fatal("未知日志级别应返回错误")
	}
}

func TestNewSingleNodeValidatesConfig(t *testing.T) {
	if _, err := NewSingleNode(Config{LocalID: " "}, fsm.New()); err == nil {
		t.Fatal("节点 ID 为空时应返回错误")
	}

	if _, err := NewSingleNode(Config{LocalID: "node-1"}, nil); err == nil {
		t.Fatal("FSM 为空时应返回错误")
	}

	if _, err := NewSingleNode(
		Config{LocalID: "node-1", LogLevel: "verbose"},
		fsm.New(),
	); err == nil {
		t.Fatal("日志级别非法时应返回错误")
	}
}

func TestNodeApplyRejectsInvalidInput(t *testing.T) {
	var uninitialized *Node

	if _, err := uninitialized.Apply(nil, time.Second); err == nil {
		t.Fatal("未初始化的节点应返回错误")
	}

	node := newTestNode(t)

	if _, err := node.Apply(nil, time.Second); err == nil {
		t.Fatal("命令为空时应返回错误")
	}

	if _, err := node.Apply(newCreateCommand(), 0); err == nil {
		t.Fatal("超时为 0 时应返回错误")
	}
}

func TestNodeLeaderAddressFallsBackToNodeID(t *testing.T) {
	defaultNode := newTestNode(t)
	address, id := defaultNode.Leader()
	if string(id) != "node-1" {
		t.Fatalf("leader id = %q, 期望 node-1", id)
	}
	if string(address) != "node-1" {
		t.Fatalf("leader address = %q, 期望回退为 node-1", address)
	}

	explicit := newTestNodeWithConfig(t, Config{
		LocalID:            "node-9",
		RaftAddress:        "127.0.0.1:50062",
		LogLevel:           "error",
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}, fsm.New())
	address, id = explicit.Leader()
	if string(address) != "127.0.0.1:50062" || string(id) != "node-9" {
		t.Fatalf("leader = (%q, %q), 期望 (127.0.0.1:50062, node-9)", address, id)
	}
}

func TestSingleNodeApply(t *testing.T) {
	state := fsm.New()
	node := newTestNodeWithState(t, state)

	command := newCreateCommand()

	result, err := node.Apply(
		command,
		3*time.Second,
	)
	if err != nil {
		t.Fatalf(
			"first Apply() error = %v",
			err,
		)
	}

	if result.GetCode() != fsm.ResultCodeOK {
		t.Fatalf(
			"unexpected result code: %s",
			result.GetCode(),
		)
	}

	if result.GetReplayed() {
		t.Fatal(
			"first Raft Apply must not be replayed",
		)
	}

	if result.GetOrder().GetStatus() !=
		orderv1.OrderStatus_ORDER_STATUS_CREATED {
		t.Fatalf(
			"unexpected order status: %s",
			result.GetOrder().GetStatus(),
		)
	}

	if result.GetOrder().GetVersion() != 1 {
		t.Fatalf(
			"unexpected order version: %d",
			result.GetOrder().GetVersion(),
		)
	}

	// 再次通过 Raft 提交相同命令，验证 FSM 幂等记录。
	replayed, err := node.Apply(
		command,
		3*time.Second,
	)
	if err != nil {
		t.Fatalf(
			"second Apply() error = %v",
			err,
		)
	}

	if !replayed.GetReplayed() {
		t.Fatal(
			"second Raft Apply should be replayed",
		)
	}

	order, err := state.GetOrder("raft-order-001")
	if err != nil {
		t.Fatalf(
			"GetOrder() error = %v",
			err,
		)
	}

	if order.GetVersion() != 1 {
		t.Fatalf(
			"replay changed order version: %d",
			order.GetVersion(),
		)
	}
}

func TestNodeHandlesNilReceiver(t *testing.T) {
	var uninitialized *Node

	if err := uninitialized.Close(); err != nil {
		t.Fatalf("nil 节点 Close() error = %v", err)
	}
	if state := uninitialized.State(); state != raft.Shutdown {
		t.Fatalf("nil 节点 State() = %s", state)
	}
	if uninitialized.IsLeader() {
		t.Fatal("nil 节点不应是 Leader")
	}
	if uninitialized.FSM() != nil {
		t.Fatal("nil 节点的 FSM() 应为 nil")
	}
}

func newTestNode(t *testing.T) *Node {
	t.Helper()

	return newTestNodeWithState(t, fsm.New())
}

func newTestNodeWithState(t *testing.T, state *fsm.FSM) *Node {
	t.Helper()

	return newTestNodeWithConfig(t, Config{
		LocalID:            "node-1",
		LogLevel:           "error",
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}, state)
}

func newTestNodeWithConfig(t *testing.T, config Config, state *fsm.FSM) *Node {
	t.Helper()

	node, err := NewSingleNode(config, state)
	if err != nil {
		t.Fatalf(
			"NewSingleNode() error = %v",
			err,
		)
	}

	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Errorf(
				"close raft node: %v",
				err,
			)
		}
	})

	if err := node.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf(
			"WaitForLeader() error = %v",
			err,
		)
	}

	if node.State() != raft.Leader {
		t.Fatalf(
			"expected Leader, got %s",
			node.State(),
		)
	}

	return node
}

func newCreateCommand() *orderv1.RaftCommand {
	return &orderv1.RaftCommand{
		RequestId:       "raft-request-001",
		AppliedAtUnixMs: 1700000000000,
		SchemaVersion:   1,
		Operation: &orderv1.RaftCommand_CreateOrder{
			CreateOrder: &orderv1.CreateOrderCommand{
				OrderId:     "raft-order-001",
				UserId:      "user-001",
				AmountCents: 1999,
				Currency:    "CNY",
			},
		},
	}
}
