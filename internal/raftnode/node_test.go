package raftnode

import (
	"testing"
	"time"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"example.com/OrderRaft/internal/fsm"

	"github.com/hashicorp/raft"
)

func TestSingleNodeApply(t *testing.T) {
	state := fsm.New()

	node, err := NewSingleNode(
		"node-1",
		state,
	)
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

	if err := node.WaitForLeader(5 * time.Second); err != nil {
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

	command := &orderv1.RaftCommand{
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
