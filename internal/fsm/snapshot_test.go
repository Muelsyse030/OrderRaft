package fsm

import (
	"bytes"
	"io"
	"testing"

	orderv1 "example.com/OrderRaft/gen/order/v1"
)

// memorySnapshotSink 是测试使用的内存快照目标。
type memorySnapshotSink struct {
	bytes.Buffer

	closed    bool
	cancelled bool
}

func (s *memorySnapshotSink) ID() string {
	return "memory-snapshot"
}

func (s *memorySnapshotSink) Close() error {
	s.closed = true
	return nil
}

func (s *memorySnapshotSink) Cancel() error {
	s.cancelled = true
	return nil
}

func TestSnapshotAndRestore(t *testing.T) {
	original := New()

	createCommand := &orderv1.RaftCommand{
		RequestId:       "snapshot-create-001",
		AppliedAtUnixMs: 1700000000000,
		SchemaVersion:   1,
		Operation: &orderv1.RaftCommand_CreateOrder{
			CreateOrder: &orderv1.CreateOrderCommand{
				OrderId:     "snapshot-order-001",
				UserId:      "user-001",
				AmountCents: 1999,
				Currency:    "CNY",
			},
		},
	}

	createResult, err := original.ApplyCommand(createCommand)
	if err != nil {
		t.Fatalf(
			"apply create command: %v",
			err,
		)
	}

	if createResult.GetCode() != ResultCodeOK {
		t.Fatalf(
			"unexpected create result: %s",
			createResult.GetCode(),
		)
	}

	changeCommand := &orderv1.RaftCommand{
		RequestId:       "snapshot-change-001",
		AppliedAtUnixMs: 1700000001000,
		SchemaVersion:   1,
		Operation: &orderv1.RaftCommand_ChangeOrderStatus{
			ChangeOrderStatus: &orderv1.ChangeOrderStatusCommand{
				OrderId: "snapshot-order-001",
				TargetStatus: orderv1.
					OrderStatus_ORDER_STATUS_PAID,
			},
		},
	}

	changeResult, err := original.ApplyCommand(changeCommand)
	if err != nil {
		t.Fatalf(
			"apply change command: %v",
			err,
		)
	}

	if changeResult.GetCode() != ResultCodeOK {
		t.Fatalf(
			"unexpected change result: %s",
			changeResult.GetCode(),
		)
	}

	snapshot, err := original.Snapshot()
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	defer snapshot.Release()

	sink := &memorySnapshotSink{}

	if err := snapshot.Persist(sink); err != nil {
		t.Fatalf("persist snapshot: %v", err)
	}

	if !sink.closed {
		t.Fatal("snapshot sink was not closed")
	}

	if sink.cancelled {
		t.Fatal("snapshot sink was unexpectedly cancelled")
	}

	restored := New()

	source := io.NopCloser(
		bytes.NewReader(sink.Bytes()),
	)

	if err := restored.Restore(source); err != nil {
		t.Fatalf("restore snapshot: %v", err)
	}

	order, err := restored.GetOrder("snapshot-order-001")
	if err != nil {
		t.Fatalf(
			"get restored order: %v",
			err,
		)
	}

	if order.GetStatus() !=
		orderv1.OrderStatus_ORDER_STATUS_PAID {
		t.Fatalf(
			"unexpected restored status: %s",
			order.GetStatus(),
		)
	}

	if order.GetVersion() != 2 {
		t.Fatalf(
			"unexpected restored version: %d",
			order.GetVersion(),
		)
	}

	if order.GetCreatedAtUnixMs() != 1700000000000 {
		t.Fatalf(
			"unexpected restored created_at: %d",
			order.GetCreatedAtUnixMs(),
		)
	}

	if order.GetUpdatedAtUnixMs() != 1700000001000 {
		t.Fatalf(
			"unexpected restored updated_at: %d",
			order.GetUpdatedAtUnixMs(),
		)
	}

	// 验证幂等记录也已经恢复。
	replayedResult, err := restored.ApplyCommand(
		changeCommand,
	)
	if err != nil {
		t.Fatalf(
			"replay restored command: %v",
			err,
		)
	}

	if !replayedResult.GetReplayed() {
		t.Fatal(
			"restored idempotency result should be replayed",
		)
	}

	if replayedResult.GetOrder().GetVersion() != 2 {
		t.Fatalf(
			"replay changed restored order version: %d",
			replayedResult.GetOrder().GetVersion(),
		)
	}
}
