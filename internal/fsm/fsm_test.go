package fsm

import (
	"testing"

	orderv1 "example.com/OrderRaft/gen/order/v1"
)

func TestFSMApplyCreateAndChangeStatus(t *testing.T) {
	state := New()

	createdAt := int64(1700000000000)
	updatedAt := int64(1700000001000)

	createResult, err := state.ApplyCommand(
		newCreateCommand("request-001", "order-001", createdAt),
	)
	if err != nil {
		t.Fatalf("ApplyCommand(create) error = %v", err)
	}
	if createResult.GetCode() != ResultCodeOK {
		t.Fatalf("unexpected create code: %s", createResult.GetCode())
	}

	order := createResult.GetOrder()
	if order.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATED {
		t.Fatalf("unexpected initial status: %v", order.GetStatus())
	}
	if order.GetVersion() != 1 {
		t.Fatalf("unexpected initial version: %d", order.GetVersion())
	}
	if order.GetCreatedAtUnixMs() != createdAt {
		t.Fatalf("unexpected created_at: %d", order.GetCreatedAtUnixMs())
	}

	changeResult, err := state.ApplyCommand(
		newChangeStatusCommand(
			"request-002",
			"order-001",
			orderv1.OrderStatus_ORDER_STATUS_PAID,
			updatedAt,
		),
	)
	if err != nil {
		t.Fatalf("ApplyCommand(change) error = %v", err)
	}
	if changeResult.GetCode() != ResultCodeOK {
		t.Fatalf("unexpected change code: %s", changeResult.GetCode())
	}

	updated := changeResult.GetOrder()
	if updated.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PAID {
		t.Fatalf("unexpected status: %v", updated.GetStatus())
	}
	if updated.GetVersion() != 2 {
		t.Fatalf("unexpected version: %d", updated.GetVersion())
	}
	if updated.GetCreatedAtUnixMs() != createdAt {
		t.Fatal("created_at should not change")
	}
	if updated.GetUpdatedAtUnixMs() != updatedAt {
		t.Fatalf("unexpected updated_at: %d", updated.GetUpdatedAtUnixMs())
	}
}

func TestFSMRejectsIllegalTransition(t *testing.T) {
	state := New()

	if _, err := state.ApplyCommand(
		newCreateCommand("request-001", "order-001", 1700000000000),
	); err != nil {
		t.Fatalf("ApplyCommand(create) error = %v", err)
	}

	result, err := state.ApplyCommand(
		newChangeStatusCommand(
			"request-002",
			"order-001",
			orderv1.OrderStatus_ORDER_STATUS_COMPLETED,
			1700000001000,
		),
	)
	if err != nil {
		t.Fatalf("ApplyCommand(change) error = %v", err)
	}
	if result.GetCode() != ResultCodeInvalidStatusTransition {
		t.Fatalf("code = %s, 期望 %s", result.GetCode(), ResultCodeInvalidStatusTransition)
	}

	// 状态相关失败不占用 request_id:修正目标状态后可用同一 request_id 重试。
	retry, err := state.ApplyCommand(
		newChangeStatusCommand(
			"request-002",
			"order-001",
			orderv1.OrderStatus_ORDER_STATUS_PAID,
			1700000002000,
		),
	)
	if err != nil {
		t.Fatalf("ApplyCommand(retry) error = %v", err)
	}
	if retry.GetCode() != ResultCodeOK {
		t.Fatalf("retry code = %s, 期望 %s", retry.GetCode(), ResultCodeOK)
	}
	if retry.GetReplayed() {
		t.Fatal("修正后的重试不应被判定为重放")
	}
}

func TestFSMRejectsDuplicateOrder(t *testing.T) {
	state := New()

	if _, err := state.ApplyCommand(
		newCreateCommand("request-001", "order-001", 1700000000000),
	); err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}

	result, err := state.ApplyCommand(
		newCreateCommand("request-002", "order-001", 1700000000000),
	)
	if err != nil {
		t.Fatalf("second ApplyCommand() error = %v", err)
	}
	if result.GetCode() != ResultCodeOrderAlreadyExists {
		t.Fatalf("code = %s, 期望 %s", result.GetCode(), ResultCodeOrderAlreadyExists)
	}
}

func TestFSMReturnsCopy(t *testing.T) {
	state := New()

	result, err := state.ApplyCommand(
		newCreateCommand("request-001", "order-001", 1700000000000),
	)
	if err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}

	result.GetOrder().Status = orderv1.OrderStatus_ORDER_STATUS_CANCELLED

	actual, err := state.GetOrder("order-001")
	if err != nil {
		t.Fatalf("GetOrder() error = %v", err)
	}

	if actual.GetStatus() != orderv1.OrderStatus_ORDER_STATUS_CREATED {
		t.Fatal("FSM state was modified through returned pointer")
	}
}
