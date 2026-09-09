package fsm

import (
	"errors"
	"fmt"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type ApplyResponse struct {
	Result *orderv1.RaftCommandResult
	Err    error
}

func (f *FSM) Apply(logEntry *raft.Log) interface{} {
	if logEntry == nil {
		return &ApplyResponse{
			Err: errors.New("raft log is required"),
		}
	}

	command := &orderv1.RaftCommand{}

	if err := proto.Unmarshal(logEntry.Data, command); err != nil {
		return &ApplyResponse{
			Err: fmt.Errorf(
				"decode raft command: %w",
				err,
			),
		}
	}
	result, err := f.ApplyCommand(command)
	return &ApplyResponse{Result: result, Err: err}
}
