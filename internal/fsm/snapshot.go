package fsm

import (
	"errors"
	"fmt"
	"io"
	"sort"

	orderv1 "example.com/OrderRaft/gen/order/v1"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

var _ raft.FSM = (*FSM)(nil)
var _ raft.FSMSnapshot = (*raftSnapshot)(nil)

type raftSnapshot struct {
	data []byte
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	state, err := f.snapshotState()
	if err != nil {
		return nil, err
	}

	data, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal FSM snapshot: %w",
			err,
		)
	}

	// data 由本次 Marshal 独占分配,返回后没有别名共享,直接转移所有权即可,
	// 不需要再整份复制一遍(快照体积与订单/幂等记录数成正比)。
	return &raftSnapshot{
		data: data,
	}, nil
}

// snapshotState 在读锁内只抓取指针快照与排序键,克隆和排序都在锁外完成,
// 避免长时间的深拷贝阻塞写路径(Apply 需要写锁)。
//
// 已存入 orders / idempotencyRecords 的值在插入后不再被就地修改:
// Apply 只会整体替换 map 中的条目,因此锁外读取这些指针是安全的。
func (f *FSM) snapshotState() (*orderv1.FSMStateSnapshot, error) {
	orders, records := f.view()

	orderIDs := make([]string, 0, len(orders))
	for orderID := range orders {
		orderIDs = append(orderIDs, orderID)
	}
	sort.Strings(orderIDs)

	requestIDs := make([]string, 0, len(records))
	for requestID := range records {
		requestIDs = append(requestIDs, requestID)
	}
	sort.Strings(requestIDs)

	state := &orderv1.FSMStateSnapshot{
		SchemaVersion:      snapshotSchemaVersion,
		Orders:             make([]*orderv1.Order, 0, len(orderIDs)),
		IdempotencyRecords: make([]*orderv1.IdempotencyRecordSnapshot, 0, len(requestIDs)),
	}

	for _, orderID := range orderIDs {
		order := orders[orderID]
		if order == nil {
			return nil, fmt.Errorf(
				"snapshot order %q is nil",
				orderID,
			)
		}

		state.Orders = append(
			state.Orders,
			cloneOrder(order),
		)
	}

	for _, requestID := range requestIDs {
		record := records[requestID]

		if record == nil {
			return nil, fmt.Errorf(
				"snapshot idempotency record %q is nil",
				requestID,
			)
		}

		if record.result == nil {
			return nil, fmt.Errorf(
				"snapshot idempotency result %q is nil",
				requestID,
			)
		}

		state.IdempotencyRecords = append(
			state.IdempotencyRecords,
			&orderv1.IdempotencyRecordSnapshot{
				RequestId: requestID,
				Fingerprint: append(
					[]byte(nil),
					record.fingerprint...,
				),
				Result: cloneCommandResult(record.result),
			},
		)
	}

	return state, nil
}

// view 在锁内抓取 map 的浅拷贝,解锁路径由 defer 统一收敛。
func (f *FSM) view() (map[string]*orderv1.Order, map[string]*idempotencyRecord) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	orders := make(map[string]*orderv1.Order, len(f.orders))
	for orderID, order := range f.orders {
		orders[orderID] = order
	}

	records := make(map[string]*idempotencyRecord, len(f.idempotencyRecords))
	for requestID, record := range f.idempotencyRecords {
		records[requestID] = record
	}

	return orders, records
}

func (s *raftSnapshot) Persist(
	sink raft.SnapshotSink,
) error {
	if sink == nil {
		return errors.New("snapshot sink is required")
	}

	written, err := sink.Write(s.data)
	if err != nil {
		_ = sink.Cancel()

		return fmt.Errorf(
			"write FSM snapshot: %w",
			err,
		)
	}

	if written != len(s.data) {
		_ = sink.Cancel()

		return io.ErrShortWrite
	}

	if err := sink.Close(); err != nil {
		_ = sink.Cancel()

		return fmt.Errorf(
			"close FSM snapshot sink: %w",
			err,
		)
	}

	return nil
}

func (s *raftSnapshot) Release() {
	s.data = nil
}

func (f *FSM) Restore(
	source io.ReadCloser,
) error {
	if source == nil {
		return errors.New("snapshot source is required")
	}

	data, readErr := io.ReadAll(source)
	closeErr := source.Close()

	if readErr != nil {
		return fmt.Errorf(
			"read FSM snapshot: %w",
			readErr,
		)
	}

	if closeErr != nil {
		return fmt.Errorf(
			"close FSM snapshot source: %w",
			closeErr,
		)
	}

	state := &orderv1.FSMStateSnapshot{}
	if err := proto.Unmarshal(data, state); err != nil {
		return fmt.Errorf(
			"unmarshal FSM snapshot: %w",
			err,
		)
	}

	if state.GetSchemaVersion() != snapshotSchemaVersion {
		return fmt.Errorf(
			"unsupported FSM snapshot schema version: %d",
			state.GetSchemaVersion(),
		)
	}

	restoredOrders := make(
		map[string]*orderv1.Order,
		len(state.GetOrders()),
	)

	for _, order := range state.GetOrders() {
		if order == nil {
			return errors.New(
				"snapshot contains nil order",
			)
		}

		if order.GetId() == "" {
			return errors.New(
				"snapshot contains order without id",
			)
		}

		if _, exists := restoredOrders[order.GetId()]; exists {
			return fmt.Errorf(
				"snapshot contains duplicate order: %s",
				order.GetId(),
			)
		}

		restoredOrders[order.GetId()] = cloneOrder(order)
	}

	restoredRecords := make(
		map[string]*idempotencyRecord,
		len(state.GetIdempotencyRecords()),
	)

	for _, record := range state.GetIdempotencyRecords() {
		if record == nil {
			return errors.New(
				"snapshot contains nil idempotency record",
			)
		}

		requestID := record.GetRequestId()
		if requestID == "" {
			return errors.New(
				"snapshot contains idempotency record without request_id",
			)
		}

		if len(record.GetFingerprint()) == 0 {
			return fmt.Errorf(
				"snapshot idempotency record %q has empty fingerprint",
				requestID,
			)
		}

		if record.GetResult() == nil {
			return fmt.Errorf(
				"snapshot idempotency record %q has nil result",
				requestID,
			)
		}

		if record.GetResult().GetRequestId() != requestID {
			return fmt.Errorf(
				"snapshot idempotency record %q has mismatched result request_id %q",
				requestID,
				record.GetResult().GetRequestId(),
			)
		}

		if _, exists := restoredRecords[requestID]; exists {
			return fmt.Errorf(
				"snapshot contains duplicate request_id: %s",
				requestID,
			)
		}

		restoredRecords[requestID] = &idempotencyRecord{
			fingerprint: append(
				[]byte(nil),
				record.GetFingerprint()...,
			),
			result: cloneCommandResult(
				record.GetResult(),
			),
		}
	}

	f.mu.Lock()
	f.orders = restoredOrders
	f.idempotencyRecords = restoredRecords
	f.mu.Unlock()

	return nil
}
