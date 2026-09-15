package raftnode

import (
	"errors"
	"fmt"
	"strings"
	"time"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"example.com/OrderRaft/internal/fsm"

	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/proto"
)

// Config 描述单个 Raft 节点的启动参数，零值字段使用 hashicorp/raft 的默认值。
type Config struct {
	// LocalID 是本节点的 Raft 节点 ID，必填。
	LocalID string

	// RaftAddress 是本节点通告给集群的 Raft 传输地址。
	// 当前使用内存传输(InmemTransport)，该地址只在进程内用于标识节点，
	// 不会建立真实网络连接；要组成多节点集群必须替换为 raft.NetworkTransport。
	RaftAddress string

	// LogLevel 是 raft 内部日志级别：trace/debug/info/warn/error。
	// 留空时使用 info，避免默认的 debug 日志淹没输出。
	LogLevel string

	// 以下超时留空时使用 hashicorp/raft 默认值。
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration

	// SnapshotInterval 是自动触发快照的最小间隔，为零时使用 raft 默认值(120s)。
	SnapshotInterval time.Duration

	// DisableSnapshots 为 true 时关闭自动快照定时器(忽略 SnapshotInterval)。
	// hashicorp/raft v1.7.3 的 ValidateConfig 拒绝 SnapshotInterval == 0,
	// 因此这里用一个实际不会触发的超大间隔来等价实现"禁用"。
	DisableSnapshots bool

	// SnapshotThreshold 是自上次快照以来累积的日志条目数，达到后触发快照，
	// 为零时使用 raft 默认值(8192)。调小它可以让单节点演示真正执行快照/恢复路径。
	SnapshotThreshold uint64

	// TrailingLogs 是快照完成后保留的尾部日志条目数，便于慢节点追赶，
	// 为零时使用 raft 默认值(10240)。
	TrailingLogs uint64
}

// disabledSnapshotInterval 用于实现"禁用自动快照"。
// raft 的 SnapshotInterval 上限受 randomTimeout 的 minVal+extra 计算约束，
// 取 2^62 纳秒(约 146 年)既不会溢出，也不会在进程生命周期内触发。
const disabledSnapshotInterval = time.Duration(1) << 62

type Node struct {
	raft      *raft.Raft
	state     *fsm.FSM
	transport *raft.InmemTransport
}

// 支持通过 -log-level 调整 raft 内部日志级别。
var supportedLogLevels = []string{"trace", "debug", "info", "warn", "error"}

func NewSingleNode(cfg Config, state *fsm.FSM) (*Node, error) {
	if strings.TrimSpace(cfg.LocalID) == "" {
		return nil, errors.New("local raft node ID is required")
	}

	if state == nil {
		return nil, errors.New("FSM is required")
	}

	logLevel, err := normalizeLogLevel(cfg.LogLevel)
	if err != nil {
		return nil, err
	}

	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(cfg.LocalID)
	config.LogLevel = logLevel
	if cfg.HeartbeatTimeout > 0 {
		config.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	if cfg.ElectionTimeout > 0 {
		config.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.LeaderLeaseTimeout > 0 {
		config.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	}
	if cfg.CommitTimeout > 0 {
		config.CommitTimeout = cfg.CommitTimeout
	}
	if cfg.DisableSnapshots {
		config.SnapshotInterval = disabledSnapshotInterval
	} else if cfg.SnapshotInterval > 0 {
		config.SnapshotInterval = cfg.SnapshotInterval
	}
	if cfg.SnapshotThreshold > 0 {
		config.SnapshotThreshold = cfg.SnapshotThreshold
	}
	if cfg.TrailingLogs > 0 {
		config.TrailingLogs = cfg.TrailingLogs
	}

	address := raft.ServerAddress(strings.TrimSpace(cfg.RaftAddress))
	if address == "" {
		address = raft.ServerAddress(cfg.LocalID)
	}

	store := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()
	transportAddress, transport := raft.NewInmemTransport(address)

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
				Address:  transportAddress,
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

func normalizeLogLevel(level string) (string, error) {
	level = strings.ToLower(strings.TrimSpace(level))
	if level == "" {
		return "info", nil
	}
	for _, supported := range supportedLogLevels {
		if level == supported {
			return level, nil
		}
	}
	return "", fmt.Errorf(
		"unsupported raft log level %q, expected one of %s",
		level,
		strings.Join(supportedLogLevels, "/"),
	)
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
		return nil, fmt.Errorf(
			"%w: unexpected FSM response type: %T",
			fsm.ErrApplyFailed,
			rawResponse,
		)
	}
	if response.Err != nil {
		// response.Err 可能已经包装了 ErrApplyFailed(解码失败)或 ErrInvalidCommand
		// (纯参数校验失败),这里只补上"应用阶段"上下文,保留 errors.Is 链。
		return nil, fmt.Errorf("FSM apply command: %w", response.Err)
	}
	if response.Result == nil {
		return nil, fmt.Errorf(
			"%w: FSM returned nil command result",
			fsm.ErrApplyFailed,
		)
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
