package fsm

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"example.com/OrderRaft/internal/domain"
	"google.golang.org/protobuf/proto"
)

const (
	ResultCodeOK                      = "OK"
	ResultCodeInvalidArgument         = "INVALID_ARGUMENT"
	ResultCodeOrderAlreadyExists      = "ORDER_ALREADY_EXISTS"
	ResultCodeOrderNotFound           = "ORDER_NOT_FOUND"
	ResultCodeInvalidStatusTransition = "INVALID_STATUS_TRANSITION"

	// commandSchemaVersion 是当前 FSM 能识别的 RaftCommand 版本。
	commandSchemaVersion = 1

	// snapshotSchemaVersion 是当前 FSM 能识别的快照版本。
	snapshotSchemaVersion = 1
)

var (
	ErrRequestRequired = errors.New("request is required")
	ErrOrderNotFound   = errors.New("order not found")

	// ErrInvalidCommand 表示命令本身不合法(纯参数校验失败),与 FSM 当前状态无关。
	// 这类失败不会占用 request_id,客户端修正参数后可用同一 request_id 重试。
	ErrInvalidCommand = errors.New("invalid raft command")
)

// idempotencyRecord 记录某个 request_id 首次执行成功的决策。
//
// 只有真正改变状态的决策才会写入这里:纯参数校验失败依赖命令自身即可确定性重算,
// 状态相关失败(订单不存在、非法流转等)依赖 apply 时的状态,也会随日志重放
// 在相同状态上重新求值,因此都不需要、也不应该占用 request_id。
//
// fingerprint 用于识别"同一 request_id 被复用于不同命令"的客户端误用:
// 命中不同指纹时会累加 IdempotencyConflicts 并输出告警日志(见 ApplyCommand),
// 而不是静默吞掉这个信号。
type idempotencyRecord struct {
	fingerprint []byte
	result      *orderv1.RaftCommandResult
}

type FSM struct {
	mu                 sync.RWMutex
	orders             map[string]*orderv1.Order
	idempotencyRecords map[string]*idempotencyRecord

	// idempotencyConflicts 统计本节点观察到的 request_id 误用次数。
	idempotencyConflicts uint64

	// logger 为 nil 时不输出日志。
	logger *slog.Logger
}

func New() *FSM {
	return NewWithLogger(nil)
}

// NewWithLogger 创建带结构化日志的 FSM。logger 为 nil 时关闭日志。
// 目前用于在检测到 request_id 被复用于不同命令时输出告警。
func NewWithLogger(logger *slog.Logger) *FSM {
	return &FSM{
		orders:             make(map[string]*orderv1.Order),
		idempotencyRecords: make(map[string]*idempotencyRecord),
		logger:             logger,
	}
}

// IdempotencyConflicts 返回本节点观察到的"同一 request_id 携带不同命令"次数。
// 该计数是节点本地可观测指标,不参与共识,也不写入快照。
func (f *FSM) IdempotencyConflicts() uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()

	return f.idempotencyConflicts
}

// ApplyCommand 应用一条已提交的 Raft 命令。
//
// 幂等语义:
//   - 纯参数校验失败返回 error,不写入幂等表,避免 request_id 被永久污染。
//   - 只有成功(改变状态)的决策才写入幂等表。
//   - 同一个 request_id 再次出现时,一律返回首次记录的决策(result.Replayed=true),
//     不再因为命令指纹不同而报错。hashicorp/raft 保证所有节点以相同顺序 apply
//     同一份已提交日志,因此"日志中先出现的条目优先"的规则在所有节点上结果一致,
//     FSM 状态不会因重放或重复提交而分歧。
//   - 指纹不同意味着客户端误用了 request_id:返回值保持不变,但会累加
//     IdempotencyConflicts 并在配置了 logger 时输出告警,避免误用完全不可见。
func (f *FSM) ApplyCommand(command *orderv1.RaftCommand) (*orderv1.RaftCommandResult, error) {
	if command == nil {
		return nil, ErrRequestRequired
	}

	if err := validateCommand(command); err != nil {
		return nil, err
	}

	fingerprint, err := commandFingerprint(command)
	if err != nil {
		return nil, err
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if record, exists := f.idempotencyRecords[command.GetRequestId()]; exists {
		if !bytes.Equal(record.fingerprint, fingerprint) {
			f.recordIdempotencyConflict(command.GetRequestId())
		}

		replayedResult := cloneCommandResult(record.result)
		replayedResult.Replayed = true

		return replayedResult, nil
	}

	var result *orderv1.RaftCommandResult

	switch operation := command.GetOperation().(type) {
	case *orderv1.RaftCommand_CreateOrder:
		result = f.applyCreateLocked(command, operation.CreateOrder)

	case *orderv1.RaftCommand_ChangeOrderStatus:
		result = f.applyChangeStatusLocked(command, operation.ChangeOrderStatus)

	default:
		// validateCommand 已经排除了该分支,保留作为防御。
		return nil, fmt.Errorf("%w: operation is required", ErrInvalidCommand)
	}

	if result.GetCode() == ResultCodeOK {
		f.idempotencyRecords[command.GetRequestId()] = &idempotencyRecord{
			fingerprint: append([]byte(nil), fingerprint...),
			result:      cloneCommandResult(result),
		}
	}

	return cloneCommandResult(result), nil
}

// recordIdempotencyConflict 记录一次 request_id 误用。调用方必须持有写锁。
func (f *FSM) recordIdempotencyConflict(requestID string) {
	f.idempotencyConflicts++

	f.logWarn(
		"request_id reused with a different command; returning the first decision",
		"request_id", requestID,
	)
}

// logWarn / logError 在未配置 logger 时静默,避免调用方到处判空。
func (f *FSM) logWarn(message string, args ...any) {
	if f.logger == nil {
		return
	}
	f.logger.Warn(message, args...)
}

func (f *FSM) logError(message string, args ...any) {
	if f.logger == nil {
		return
	}
	f.logger.Error(message, args...)
}

// validateCommand 只做与 FSM 状态无关的纯参数校验。
// 相关的请求参数校验在 grpcapi 层完成,这里保留一份以确保 FSM 入口自身也是安全的。
func validateCommand(command *orderv1.RaftCommand) error {
	switch {
	case command.GetRequestId() == "":
		return fmt.Errorf("%w: request_id is required", ErrInvalidCommand)

	case command.GetSchemaVersion() != commandSchemaVersion:
		return fmt.Errorf(
			"%w: unsupported command schema version: %d",
			ErrInvalidCommand,
			command.GetSchemaVersion(),
		)

	case command.GetAppliedAtUnixMs() <= 0:
		return fmt.Errorf(
			"%w: applied_at_unix_ms must be greater than zero",
			ErrInvalidCommand,
		)
	}

	switch operation := command.GetOperation().(type) {
	case *orderv1.RaftCommand_CreateOrder:
		return validateCreateOrderCommand(operation.CreateOrder)

	case *orderv1.RaftCommand_ChangeOrderStatus:
		return validateChangeOrderStatusCommand(operation.ChangeOrderStatus)

	default:
		return fmt.Errorf("%w: operation is required", ErrInvalidCommand)
	}
}

func validateCreateOrderCommand(create *orderv1.CreateOrderCommand) error {
	switch {
	case create == nil:
		return fmt.Errorf("%w: create_order command is required", ErrInvalidCommand)

	case create.GetOrderId() == "":
		return fmt.Errorf("%w: order_id is required", ErrInvalidCommand)

	case create.GetUserId() == "":
		return fmt.Errorf("%w: user_id is required", ErrInvalidCommand)

	case create.GetAmountCents() <= 0:
		return fmt.Errorf("%w: amount_cents must be greater than zero", ErrInvalidCommand)

	case create.GetCurrency() == "":
		return fmt.Errorf("%w: currency is required", ErrInvalidCommand)
	}

	return nil
}

func validateChangeOrderStatusCommand(change *orderv1.ChangeOrderStatusCommand) error {
	switch {
	case change == nil:
		return fmt.Errorf("%w: change_order_status command is required", ErrInvalidCommand)

	case change.GetOrderId() == "":
		return fmt.Errorf("%w: order_id is required", ErrInvalidCommand)

	case change.GetTargetStatus() ==
		orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED:
		return fmt.Errorf("%w: target_status is required", ErrInvalidCommand)
	}

	return nil
}

func (f *FSM) applyCreateLocked(command *orderv1.RaftCommand, create *orderv1.CreateOrderCommand) *orderv1.RaftCommandResult {
	if _, exists := f.orders[create.GetOrderId()]; exists {
		return newCommandResult(
			command.GetRequestId(),
			ResultCodeOrderAlreadyExists,
			fmt.Sprintf("order already exists: %s", create.GetOrderId()),
			nil,
		)
	}

	order := &orderv1.Order{
		Id:              create.GetOrderId(),
		UserId:          create.GetUserId(),
		AmountCents:     create.GetAmountCents(),
		Currency:        create.GetCurrency(),
		Status:          orderv1.OrderStatus_ORDER_STATUS_CREATED,
		Version:         1,
		CreatedAtUnixMs: command.GetAppliedAtUnixMs(),
		UpdatedAtUnixMs: command.GetAppliedAtUnixMs(),
	}

	f.orders[order.GetId()] = order

	return newCommandResult(command.GetRequestId(), ResultCodeOK, "order created", order)
}

func (f *FSM) applyChangeStatusLocked(command *orderv1.RaftCommand, change *orderv1.ChangeOrderStatusCommand) *orderv1.RaftCommandResult {
	order, exists := f.orders[change.GetOrderId()]
	if !exists {
		return newCommandResult(
			command.GetRequestId(),
			ResultCodeOrderNotFound,
			fmt.Sprintf("order not found: %s", change.GetOrderId()),
			nil,
		)
	}

	if err := domain.ValidateStatusTransition(order.GetStatus(), change.GetTargetStatus()); err != nil {
		return newCommandResult(
			command.GetRequestId(),
			ResultCodeInvalidStatusTransition,
			err.Error(),
			order,
		)
	}

	updatedOrder := cloneOrder(order)
	updatedOrder.Status = change.GetTargetStatus()
	updatedOrder.Version++
	updatedOrder.UpdatedAtUnixMs = command.GetAppliedAtUnixMs()

	f.orders[updatedOrder.GetId()] = updatedOrder

	return newCommandResult(command.GetRequestId(), ResultCodeOK, "order status changed", updatedOrder)
}

func (f *FSM) GetOrder(orderID string) (*orderv1.Order, error) {
	if orderID == "" {
		return nil, errors.New("order_id is required")
	}
	f.mu.RLock()
	defer f.mu.RUnlock()

	order, exists := f.orders[orderID]
	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrOrderNotFound, orderID)
	}
	return cloneOrder(order), nil
}

func commandFingerprint(command *orderv1.RaftCommand) ([]byte, error) {
	var (
		operationName string
		message       proto.Message
	)

	switch operation := command.GetOperation().(type) {
	case *orderv1.RaftCommand_CreateOrder:
		if operation.CreateOrder == nil {
			return nil, fmt.Errorf(
				"%w: create_order command is required",
				ErrInvalidCommand,
			)
		}

		operationName = "create_order"
		message = operation.CreateOrder

	case *orderv1.RaftCommand_ChangeOrderStatus:
		if operation.ChangeOrderStatus == nil {
			return nil, fmt.Errorf(
				"%w: change_order_status command is required",
				ErrInvalidCommand,
			)
		}

		operationName = "change_order_status"
		message = operation.ChangeOrderStatus

	default:
		return nil, fmt.Errorf("%w: operation is required", ErrInvalidCommand)
	}

	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("marshal command fingerprint: %w", err)
	}

	fingerprint := make([]byte, 0, len(operationName)+1+len(payload))

	fingerprint = append(fingerprint, operationName...)
	fingerprint = append(fingerprint, 0)
	fingerprint = append(fingerprint, payload...)

	return fingerprint, nil
}

func cloneOrder(order *orderv1.Order) *orderv1.Order {
	if order == nil {
		return nil
	}
	return proto.Clone(order).(*orderv1.Order)
}

func newCommandResult(requestID string, code string, message string, order *orderv1.Order) *orderv1.RaftCommandResult {
	return &orderv1.RaftCommandResult{
		RequestId: requestID,
		Code:      code,
		Message:   message,
		Order:     cloneOrder(order),
		Replayed:  false,
	}
}

func cloneCommandResult(result *orderv1.RaftCommandResult) *orderv1.RaftCommandResult {
	if result == nil {
		return nil
	}
	return proto.Clone(result).(*orderv1.RaftCommandResult)
}
