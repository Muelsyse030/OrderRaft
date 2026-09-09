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
	state := &orderv1.FSMStateSnapshot{
		SchemaVersion:      1,
		Orders:             make([]*orderv1.Order, 0),
		IdempotencyRecords: make([]*orderv1.IdempotencyRecordSnapshot, 0),
	}

	f.mu.RLock()

	orderIDs := make([]string, 0, len(f.orders))
	for orderID := range f.orders {
		orderIDs = append(orderIDs, orderID)
	}
	sort.Strings(orderIDs)

	for _, orderID := range orderIDs {
		order := f.orders[orderID]
		if order == nil {
			f.mu.RUnlock()

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

	requestIDs := make(
		[]string,
		0,
		len(f.idempotencyRecords),
	)

	for requestID := range f.idempotencyRecords {
		requestIDs = append(requestIDs, requestID)
	}
	sort.Strings(requestIDs)

	for _, requestID := range requestIDs {
		record := f.idempotencyRecords[requestID]

		if record == nil {
			f.mu.RUnlock()

			return nil, fmt.Errorf(
				"snapshot idempotency record %q is nil",
				requestID,
			)
		}

		if record.result == nil {
			f.mu.RUnlock()

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

	f.mu.RUnlock()

	data, err := proto.MarshalOptions{
		Deterministic: true,
	}.Marshal(state)
	if err != nil {
		return nil, fmt.Errorf(
			"marshal FSM snapshot: %w",
			err,
		)
	}

	return &raftSnapshot{
		data: append([]byte(nil), data...),
	}, nil
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

	if state.GetSchemaVersion() != 1 {
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