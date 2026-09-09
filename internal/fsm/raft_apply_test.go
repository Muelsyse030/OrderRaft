package fsm

import (
	"testing"

	orderv1 "example.com/OrderRaft/gen/order/v1"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

func TestRaftApply(t *testing.T) {
	state := New()

	command := &orderv1.RaftCommand{
		RequestId:       "request-raft-001",
		AppliedAtUnixMs: 1700000000000,
		SchemaVersion:   1,
		Operation: &orderv1.RaftCommand_CreateOrder{
			CreateOrder: &orderv1.CreateOrderCommand{
				OrderId:     "order-raft-001",
				UserId:      "user-001",
				AmountCents: 1999,
				Currency:    "CNY",
			},
		},
	}

	data, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(command)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}

	logEntry := &raft.Log{
		Type: raft.LogCommand,
		Data: data,
	}

	firstValue := state.Apply(logEntry)

	firstResponse, ok := firstValue.(*ApplyResponse)
	if !ok {
		t.Fatalf(
			"unexpected response type: %T",
			firstValue,
		)
	}

	if firstResponse.Err != nil {
		t.Fatalf(
			"first Apply() error = %v",
			firstResponse.Err,
		)
	}

	if firstResponse.Result == nil {
		t.Fatal("first Apply() result is nil")
	}

	if firstResponse.Result.GetCode() != ResultCodeOK {
		t.Fatalf(
			"unexpected result code: %s",
			firstResponse.Result.GetCode(),
		)
	}

	if firstResponse.Result.GetReplayed() {
		t.Fatal("first Apply() must not be replayed")
	}

	if firstResponse.Result.GetOrder().GetVersion() != 1 {
		t.Fatalf(
			"unexpected first version: %d",
			firstResponse.Result.GetOrder().GetVersion(),
		)
	}

	// 模拟同一条日志或同一请求再次应用。
	secondValue := state.Apply(logEntry)

	secondResponse, ok := secondValue.(*ApplyResponse)
	if !ok {
		t.Fatalf(
			"unexpected second response type: %T",
			secondValue,
		)
	}

	if secondResponse.Err != nil {
		t.Fatalf(
			"second Apply() error = %v",
			secondResponse.Err,
		)
	}

	if !secondResponse.Result.GetReplayed() {
		t.Fatal("second Apply() should be replayed")
	}

	order, err := state.GetOrder("order-raft-001")
	if err != nil {
		t.Fatalf("GetOrder() error = %v", err)
	}

	if order.GetVersion() != 1 {
		t.Fatalf(
			"replaying log changed version: %d",
			order.GetVersion(),
		)
	}

	if order.GetCreatedAtUnixMs() != 1700000000000 {
		t.Fatalf(
			"unexpected created_at: %d",
			order.GetCreatedAtUnixMs(),
		)
	}
}
