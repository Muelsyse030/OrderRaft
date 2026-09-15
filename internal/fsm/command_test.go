package fsm

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
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

// 同一个 request_id 携带不同命令时，以日志中先出现的决策为准，
// 后续条目直接返回已记录结果而不报错，保证各节点确定性一致。
func TestApplyCommandResolvesRequestIDConflictDeterministically(t *testing.T) {
	state := New()

	firstResult, err := state.ApplyCommand(
		newCreateCommand(
			"request-001",
			"order-001",
			1700000000000,
		),
	)
	if err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}

	conflictingResult, err := state.ApplyCommand(
		newCreateCommand(
			"request-001",
			"order-002",
			1700000001000,
		),
	)
	if err != nil {
		t.Fatalf("conflicting ApplyCommand() error = %v", err)
	}

	if conflictingResult.GetCode() != ResultCodeOK {
		t.Fatalf(
			"conflicting result code = %s, 期望 %s",
			conflictingResult.GetCode(),
			ResultCodeOK,
		)
	}

	if !conflictingResult.GetReplayed() {
		t.Fatal("conflicting result should be marked replayed")
	}

	if conflictingResult.GetOrder().GetId() != firstResult.GetOrder().GetId() {
		t.Fatalf(
			"conflicting result order = %s, 期望首次决策的 %s",
			conflictingResult.GetOrder().GetId(),
			firstResult.GetOrder().GetId(),
		)
	}

	if _, err := state.GetOrder("order-002"); !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("冲突命令不应改变状态, order-002 err = %v", err)
	}
}

// 纯参数校验失败不占用 request_id：修正参数后可用同一 request_id 重试。
func TestApplyCommandInvalidRequestDoesNotPoisonRequestID(t *testing.T) {
	state := New()

	invalid := newCreateCommand(
		"request-001",
		"order-001",
		1700000000000,
	)
	invalid.GetCreateOrder().AmountCents = 0

	if _, err := state.ApplyCommand(invalid); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("expected ErrInvalidCommand, got %v", err)
	}

	result, err := state.ApplyCommand(
		newCreateCommand(
			"request-001",
			"order-001",
			1700000001000,
		),
	)
	if err != nil {
		t.Fatalf("retry after invalid command error = %v", err)
	}
	if result.GetCode() != ResultCodeOK {
		t.Fatalf("retry code = %s, 期望 %s", result.GetCode(), ResultCodeOK)
	}
	if result.GetReplayed() {
		t.Fatal("修正后的重试不应被判定为重放")
	}
}

// request_id 被复用于不同命令时必须留下可观测信号(计数 + 告警日志),
// 但返回值仍以先出现的决策为准,保持各节点确定性一致。
func TestApplyCommandSignalsRequestIDMisuse(t *testing.T) {
	var logs bytes.Buffer
	state := NewWithLogger(slog.New(slog.NewTextHandler(&logs, nil)))

	if _, err := state.ApplyCommand(
		newCreateCommand("request-001", "order-001", 1700000000000),
	); err != nil {
		t.Fatalf("first ApplyCommand() error = %v", err)
	}
	if state.IdempotencyConflicts() != 0 {
		t.Fatal("首次命令不应计为误用")
	}

	// 相同命令重放不是误用。
	if _, err := state.ApplyCommand(
		newCreateCommand("request-001", "order-001", 1700000005000),
	); err != nil {
		t.Fatalf("replay ApplyCommand() error = %v", err)
	}
	if state.IdempotencyConflicts() != 0 {
		t.Fatal("相同命令重放不应计为误用")
	}

	// 同一 request_id + 不同命令 => 计数并告警。
	result, err := state.ApplyCommand(
		newCreateCommand("request-001", "order-002", 1700000006000),
	)
	if err != nil {
		t.Fatalf("conflicting ApplyCommand() error = %v", err)
	}
	if result.GetOrder().GetId() != "order-001" {
		t.Fatalf("误用时应返回首次决策, 实际 order = %s", result.GetOrder().GetId())
	}
	if state.IdempotencyConflicts() != 1 {
		t.Fatalf("IdempotencyConflicts() = %d, 期望 1", state.IdempotencyConflicts())
	}
	if !strings.Contains(logs.String(), "request-001") {
		t.Fatalf("应输出包含 request_id 的告警日志, got %q", logs.String())
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

func newChangeStatusCommand(
	requestID string,
	orderID string,
	target orderv1.OrderStatus,
	appliedAtUnixMs int64,
) *orderv1.RaftCommand {
	return &orderv1.RaftCommand{
		RequestId:       requestID,
		AppliedAtUnixMs: appliedAtUnixMs,
		SchemaVersion:   1,
		Operation: &orderv1.RaftCommand_ChangeOrderStatus{
			ChangeOrderStatus: &orderv1.ChangeOrderStatusCommand{
				OrderId:      orderID,
				TargetStatus: target,
			},
		},
	}
}
