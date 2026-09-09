package fsm

import (
	"errors"
	"testing"

	orderv1 "example.com/OrderRaft/gen/order/v1"
)

func TestFSMApplyCreateAndChangeStatus(t *testing.T) {
	state := New()

	createdAt := int64(1700000000000)
	updatedAt := int64(1700000001000)

	order, err := state.ApplyCreate(
		&orderv1.CreateOrderRequest{
			RequestId:   "request-001",
			OrderId:     "order-001",
			UserId:      "user-001",
			AmountCents: 1999,
			Currency:    "CNY",
		},
		createdAt,
	)
	if err != nil {
		t.Fatalf("ApplyCreate() error = %v", err)
	}

	if order.GetStatus() !=
		orderv1.OrderStatus_ORDER_STATUS_CREATED {
		t.Fatalf("unexpected initial status: %v", order.GetStatus())
	}

	if order.GetVersion() != 1 {
		t.Fatalf("unexpected initial version: %d", order.GetVersion())
	}

	if order.GetCreatedAtUnixMs() != createdAt {
		t.Fatalf(
			"unexpected created_at: %d",
			order.GetCreatedAtUnixMs(),
		)
	}

	updated, err := state.ApplyChangeStatus(
		&orderv1.ChangeOrderStatusRequest{
			RequestId:    "request-002",
			OrderId:      "order-001",
			TargetStatus: orderv1.OrderStatus_ORDER_STATUS_PAID,
		},
		updatedAt,
	)
	if err != nil {
		t.Fatalf("ApplyChangeStatus() error = %v", err)
	}

	if updated.GetStatus() !=
		orderv1.OrderStatus_ORDER_STATUS_PAID {
		t.Fatalf("unexpected status: %v", updated.GetStatus())
	}

	if updated.GetVersion() != 2 {
		t.Fatalf("unexpected version: %d", updated.GetVersion())
	}

	if updated.GetCreatedAtUnixMs() != createdAt {
		t.Fatalf("created_at should not change")
	}

	if updated.GetUpdatedAtUnixMs() != updatedAt {
		t.Fatalf(
			"unexpected updated_at: %d",
			updated.GetUpdatedAtUnixMs(),
		)
	}
}

func TestFSMRejectsIllegalTransition(t *testing.T) {
	state := New()

	_, err := state.ApplyCreate(
		&orderv1.CreateOrderRequest{
			RequestId:   "request-001",
			OrderId:     "order-001",
			UserId:      "user-001",
			AmountCents: 1999,
			Currency:    "CNY",
		},
		1700000000000,
	)
	if err != nil {
		t.Fatalf("ApplyCreate() error = %v", err)
	}

	_, err = state.ApplyChangeStatus(
		&orderv1.ChangeOrderStatusRequest{
			RequestId:    "request-002",
			OrderId:      "order-001",
			TargetStatus: orderv1.OrderStatus_ORDER_STATUS_COMPLETED,
		},
		1700000001000,
	)
	if err == nil {
		t.Fatal("expected illegal transition error")
	}
}

func TestFSMRejectsDuplicateOrder(t *testing.T) {
	state := New()

	req := &orderv1.CreateOrderRequest{
		RequestId:   "request-001",
		OrderId:     "order-001",
		UserId:      "user-001",
		AmountCents: 1999,
		Currency:    "CNY",
	}

	_, err := state.ApplyCreate(req, 1700000000000)
	if err != nil {
		t.Fatalf("first ApplyCreate() error = %v", err)
	}

	_, err = state.ApplyCreate(req, 1700000000000)
	if !errors.Is(err, ErrOrderExists) {
		t.Fatalf("expected ErrOrderExists, got %v", err)
	}
}

func TestFSMReturnsCopy(t *testing.T) {
	state := New()

	order, err := state.ApplyCreate(
		&orderv1.CreateOrderRequest{
			RequestId:   "request-001",
			OrderId:     "order-001",
			UserId:      "user-001",
			AmountCents: 1999,
			Currency:    "CNY",
		},
		1700000000000,
	)
	if err != nil {
		t.Fatalf("ApplyCreate() error = %v", err)
	}

	order.Status = orderv1.OrderStatus_ORDER_STATUS_CANCELLED

	actual, err := state.GetOrder("order-001")
	if err != nil {
		t.Fatalf("GetOrder() error = %v", err)
	}

	if actual.GetStatus() !=
		orderv1.OrderStatus_ORDER_STATUS_CREATED {
		t.Fatalf("FSM state was modified through returned pointer")
	}
}
