package fsm

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
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

// raft 会把非命令条目也交给 FSM,FSM 必须按文档忽略它们而不是尝试解码。
func TestRaftApplyIgnoresNonCommandEntries(t *testing.T) {
	state := New()

	nonCommandTypes := []raft.LogType{
		raft.LogConfiguration,
		raft.LogNoop,
		raft.LogBarrier,
	}

	for _, logType := range nonCommandTypes {
		value := state.Apply(&raft.Log{
			Type: logType,
			Data: []byte("not-a-raft-command"),
		})

		response, ok := value.(*ApplyResponse)
		if !ok {
			t.Fatalf("log type %d: unexpected response type: %T", logType, value)
		}
		if response.Err != nil {
			t.Fatalf("log type %d: 非命令条目不应报错, got %v", logType, response.Err)
		}
		if response.Result != nil {
			t.Fatalf("log type %d: 非命令条目不应产生结果", logType)
		}
	}
}

// 已提交日志解码失败时必须留下 Error 日志:恢复期回放不会检查 Apply 的返回值。
func TestRaftApplyLogsDecodeFailure(t *testing.T) {
	var logs bytes.Buffer
	state := NewWithLogger(slog.New(slog.NewTextHandler(&logs, nil)))

	value := state.Apply(&raft.Log{
		Type:  raft.LogCommand,
		Index: 7,
		Term:  2,
		Data:  []byte{0xff},
	})

	response, ok := value.(*ApplyResponse)
	if !ok {
		t.Fatalf("unexpected response type: %T", value)
	}
	if !errors.Is(response.Err, ErrApplyFailed) {
		t.Fatalf("expected ErrApplyFailed, got %v", response.Err)
	}
	if !strings.Contains(logs.String(), "decode") {
		t.Fatalf("解码失败应输出日志, got %q", logs.String())
	}
}
