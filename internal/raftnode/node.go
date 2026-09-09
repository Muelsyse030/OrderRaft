package raftnode

import (
	"errors"
	"fmt"
	"time"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"example.com/OrderRaft/internal/fsm"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

type Node struct {
	raft      *raft.Raft
	state     *fsm.FSM
	transport *raft.InmemTransport
}

func NewSingleNode(localID string, state *fsm.FSM) (*Node, error) {
	if localID == "" {
		return nil, errors.New("local raft node ID is required")
	}

	if state == nil {
		return nil, errors.New("FSM is required")
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(localID)
	store := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()
	address, transport := raft.NewInmemTransport(
		raft.ServerAddress(localID),
	)

	raftInstance, err := raft.NewRaft(config, state, store, store, snapshotStore, transport)
	if err != nil {
		_ = transport.Close()
		return nil, fmt.Errorf("create raft instance: %w", err)
	}
	node := &Node{
		raft:      raftInstance,
		state:     state,
		transport: transport,
	}
	configuration := raft.Configuration{
		Servers: []raft.Server{
			{
				ID:       config.LocalID,
				Address:  address,
				Suffrage: raft.Voter,
			},
		},
	}

	if err := raftInstance.
		BootstrapCluster(configuration).
		Error(); err != nil {
		_ = raftInstance.Shutdown().Error()
		_ = transport.Close()
		return nil, fmt.Errorf("bootstrap single-node raft cluster: %w", err)
	}
	return node, nil
}

func (n *Node) WaitForLeader(timeout time.Duration) error {
	if n == nil || n.raft == nil {
		return errors.New("raft node is not initialized")
	}
	if timeout <= 0 {
		return errors.New("leader wait timeout must be positive")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		if n.raft.State() == raft.Leader {
			return nil
		}

		select {
		case <-timer.C:
			return fmt.Errorf(
				"wait for raft leader timed out: current state=%s",
				n.raft.State(),
			)

		case <-ticker.C:
		}
	}
}
func (n *Node) Apply(command *orderv1.RaftCommand, timeout time.Duration) (*orderv1.RaftCommandResult, error) {
	if n == nil || n.raft == nil {
		return nil, errors.New("raft node is not initialized")
	}
	if command == nil {
		return nil, errors.New("raft command is required")
	}
	if timeout <= 0 {
		return nil, errors.New("raft apply timeout must be positive")
	}
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("marshal raft command: %w", err)
	}
	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, fmt.Errorf(
			"apply raft command: %w",
			err,
		)
	}
	rawResponse := future.Response()
	response, ok := rawResponse.(*fsm.ApplyResponse)
	if !ok {
		return nil, fmt.Errorf("unexpected FSM response type: %T", rawResponse)
	}
	if response.Err != nil {
		return nil, fmt.Errorf("FSM apply command: %w", response.Err)
	}
	if response.Result == nil {
		return nil, errors.New("FSM returned nil command result")
	}
	return proto.Clone(response.Result).(*orderv1.RaftCommandResult), nil
}

func (n *Node) State() raft.RaftState {
	if n == nil || n.raft == nil {
		return raft.Shutdown
	}
	return n.raft.State()
}

func (n *Node) IsLeader() bool {
	return n.State() == raft.Leader
}

func (n *Node) Leader() (raft.ServerAddress, raft.ServerID) {
	if n == nil || n.raft == nil {
		return "", ""
	}
	return n.raft.LeaderWithID()
}

func (n *Node) FSM() *fsm.FSM {
	if n == nil {
		return nil
	}
	return n.state
}

func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	var shutdownErr error
	var transportErr error
	if n.raft != nil {
		shutdownErr = n.raft.Shutdown().Error()
	}
	if n.transport != nil {
		transportErr = n.transport.Close()
	}
	return errors.Join(
		shutdownErr,
		transportErr,
	)
}
