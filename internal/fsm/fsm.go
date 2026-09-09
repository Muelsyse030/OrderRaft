package fsm

import (
	"bytes"
	"errors"
	"fmt"
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
)

var (
	ErrRequestRequired = errors.New("request is required")
	ErrOrderExists     = errors.New("order already exists")
	ErrOrderNotFound   = errors.New("order not found")
	ErrRequestConflict = errors.New("request_id was used with another command")
)

type idempotencyRecord struct {
	fingerprint []byte
	result      *orderv1.RaftCommandResult
}
type FSM struct {
	mu                 sync.RWMutex
	orders             map[string]*orderv1.Order
	idempotencyRecords map[string]*idempotencyRecord
}

func New() *FSM {
	return &FSM{
		orders:             make(map[string]*orderv1.Order),
		idempotencyRecords: make(map[string]*idempotencyRecord),
	}
}
func (f *FSM) ApplyCommand(command *orderv1.RaftCommand) (*orderv1.RaftCommandResult, error) {
	if command == nil {
		return nil, ErrRequestRequired
	}

	if command.GetRequestId() == "" {
		return nil, errors.New("request_id is required")
	}

	if command.GetSchemaVersion() != 1 {
		return nil, fmt.Errorf("unsupported command schema version: %d", command.GetSchemaVersion())
	}

	if command.GetAppliedAtUnixMs() <= 0 {
		return nil, errors.New("applied_at_unix_ms must be greater than zero")
	}
	fingerprint, err := commandFingerprint(command)
	if err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if record, exists := f.idempotencyRecords[command.GetRequestId()]; exists {
		if !bytes.Equal(record.fingerprint, fingerprint) {
			return nil, fmt.Errorf("%w , %s", ErrRequestConflict, command.GetRequestId())
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
		return nil, errors.New("command operation is required")
	}

	f.idempotencyRecords[command.GetRequestId()] = &idempotencyRecord{
		fingerprint: append([]byte(nil), fingerprint...),
		result:      cloneCommandResult(result),
	}
	return cloneCommandResult(result), nil
}
func (f *FSM) applyCreateLocked(command *orderv1.RaftCommand, create *orderv1.CreateOrderCommand) *orderv1.RaftCommandResult {
	if create == nil {
		return newCommandResult(command.GetRequestId(), ResultCodeInvalidArgument, "create_order command is required", nil)
	}

	if create.GetOrderId() == "" {
		return newCommandResult(command.GetRequestId(), ResultCodeInvalidArgument, "order_id is required", nil)
	}

	if create.GetUserId() == "" {
		return newCommandResult(command.GetRequestId(), ResultCodeInvalidArgument, "user_id is required", nil)
	}

	if create.GetAmountCents() <= 0 {
		return newCommandResult(command.GetRequestId(), ResultCodeInvalidArgument, "amount_cents must be greater than zero", nil)
	}

	if create.GetCurrency() == "" {
		return newCommandResult(command.GetRequestId(), ResultCodeInvalidArgument, "currency is required", nil)
	}

	if _, exists := f.orders[create.GetOrderId()]; exists {
		return newCommandResult(command.GetRequestId(), ResultCodeOrderAlreadyExists, fmt.Sprintf("order already exists: %s", create.GetOrderId()), nil)
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
	if change == nil {
		return newCommandResult(command.GetRequestId(), ResultCodeInvalidArgument, "change_order_status command is required", nil)
	}

	if change.GetOrderId() == "" {
		return newCommandResult(command.GetRequestId(), ResultCodeInvalidArgument, "order_id is required", nil)
	}

	if change.GetTargetStatus() ==
		orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED {
		return newCommandResult(command.GetRequestId(), ResultCodeInvalidArgument, "target_status is required", nil)
	}

	order, exists := f.orders[change.GetOrderId()]
	if !exists {
		return newCommandResult(command.GetRequestId(), ResultCodeOrderNotFound, fmt.Sprintf("order not found: %s", change.GetOrderId()), nil)
	}

	if err := domain.ValidateStatusTransition(order.GetStatus(), change.GetTargetStatus()); err != nil {
		return newCommandResult(command.GetRequestId(), ResultCodeInvalidStatusTransition, err.Error(), order)
	}

	updatedOrder := cloneOrder(order)
	updatedOrder.Status = change.GetTargetStatus()
	updatedOrder.Version++
	updatedOrder.UpdatedAtUnixMs = command.GetAppliedAtUnixMs()

	f.orders[updatedOrder.GetId()] = updatedOrder

	return newCommandResult(command.GetRequestId(), ResultCodeOK, "order status changed", updatedOrder)
}

func (f *FSM) ApplyCreate(req *orderv1.CreateOrderRequest, createdAtUnixMs int64) (*orderv1.Order, error) {
	if req == nil {
		return nil, ErrRequestRequired
	}

	if req.GetOrderId() == "" {
		return nil, errors.New("order_id is required")
	}

	if req.GetUserId() == "" {
		return nil, errors.New("user_id is required")
	}

	if req.GetAmountCents() <= 0 {
		return nil, errors.New("amount_cents must be greater than zero")
	}

	if req.GetCurrency() == "" {
		return nil, errors.New("currency is required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, exists := f.orders[req.GetOrderId()]; exists {
		return nil, fmt.Errorf("%w: %s", ErrOrderExists, req.GetOrderId())
	}
	order := &orderv1.Order{
		Id:              req.GetOrderId(),
		UserId:          req.GetUserId(),
		AmountCents:     req.GetAmountCents(),
		Currency:        req.GetCurrency(),
		Status:          orderv1.OrderStatus_ORDER_STATUS_CREATED,
		Version:         1,
		CreatedAtUnixMs: createdAtUnixMs,
		UpdatedAtUnixMs: createdAtUnixMs,
	}
	f.orders[order.GetId()] = order

	return cloneOrder(order), nil
}

func (f *FSM) ApplyChangeStatus(req *orderv1.ChangeOrderStatusRequest, updatedAtUnixMs int64) (*orderv1.Order, error) {
	if req == nil {
		return nil, ErrRequestRequired
	}

	if req.GetOrderId() == "" {
		return nil, errors.New("order_id is required")
	}

	if req.GetTargetStatus() ==
		orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED {
		return nil, errors.New("target_status is required")
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	order, exists := f.orders[req.GetOrderId()]
	if !exists {
		return nil, fmt.Errorf(""+
			"%w: %s", ErrOrderNotFound, req.GetOrderId())
	}

	if err := domain.ValidateStatusTransition(order.GetStatus(), req.GetTargetStatus()); err != nil {
		return nil, err
	}

	updatedOrder := cloneOrder(order)
	updatedOrder.Status = req.GetTargetStatus()
	updatedOrder.Version++
	updatedOrder.UpdatedAtUnixMs = updatedAtUnixMs

	f.orders[updatedOrder.GetId()] = updatedOrder

	return cloneOrder(updatedOrder), nil
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
			return nil, errors.New(
				"create_order command is required",
			)
		}

		operationName = "create_order"
		message = operation.CreateOrder

	case *orderv1.RaftCommand_ChangeOrderStatus:
		if operation.ChangeOrderStatus == nil {
			return nil, errors.New(
				"change_order_status command is required",
			)
		}

		operationName = "change_order_status"
		message = operation.ChangeOrderStatus

	default:
		return nil, errors.New("command operation is required")
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
