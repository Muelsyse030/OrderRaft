package fsm

import (
	"errors"
	"testing"

	orderv1 "example.com/OrderRaft/gen/order/v1"
)

func TestApplyCommandIdempotency(t *testing.T) {
	state := New()

	firstCommand := newCreateCommand(
		"request-001",
		"order-001",
		1700000000000,
	)

	firstResult, err := state.ApplyCommand(firstCommand)
	if err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}

	if firstResult.GetCode() != ResultCodeOK {
		t.Fatalf(
			"unexpected first result code: %s",
			firstResult.GetCode(),
		)
	}

	if firstResult.GetReplayed() {
		t.Fatal("first result must not be replayed")
	}

	// 模拟客户端重试。
	// 重试时 applied_at 可以不同，但业务内容必须相同。
	retryCommand := newCreateCommand(
		"request-001",
		"order-001",
		1700000009999,
	)

	retryResult, err := state.ApplyCommand(retryCommand)
	if err != nil {
		t.Fatalf("retry ApplyCommand() error = %v", err)
	}

	if !retryResult.GetReplayed() {
		t.Fatal("retry result should be replayed")
	}

	if retryResult.GetOrder().GetVersion() != 1 {
		t.Fatalf(
			"retry must not increase version: %d",
			retryResult.GetOrder().GetVersion(),
		)
	}

	if retryResult.GetOrder().GetCreatedAtUnixMs() !=
		firstCommand.GetAppliedAtUnixMs() {
		t.Fatal("retry must return the original created_at")
	}
}

func TestApplyCommandRejectsRequestIDConflict(t *testing.T) {
	state := New()

	_, err := state.ApplyCommand(
		newCreateCommand(
			"request-001",
			"order-001",
			1700000000000,
		),
	)
	if err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}

	// 使用相同 request_id 创建不同订单，应当拒绝。
	_, err = state.ApplyCommand(
		newCreateCommand(
			"request-001",
			"order-002",
			1700000001000,
		),
	)

	if !errors.Is(err, ErrRequestConflict) {
		t.Fatalf(
			"expected ErrRequestConflict, got %v",
			err,
		)
	}
}

func newCreateCommand(
	requestID string,
	orderID string,
	appliedAtUnixMs int64,
) *orderv1.RaftCommand {
	return &orderv1.RaftCommand{
		RequestId:       requestID,
		AppliedAtUnixMs: appliedAtUnixMs,
		SchemaVersion:   1,
		Operation: &orderv1.RaftCommand_CreateOrder{
			CreateOrder: &orderv1.CreateOrderCommand{
				OrderId:     orderID,
				UserId:      "user-001",
				AmountCents: 1999,
				Currency:    "CNY",
			},
		},
	}
}
