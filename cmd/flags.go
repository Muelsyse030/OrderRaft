package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// options 是服务端的命令行配置。
type options struct {
	nodeID            string
	raftAddr          string
	grpcAddr          string
	peers             string
	peerAddrs         map[raft.ServerID]string
	forwardToken      string
	applyTimeout      time.Duration
	forwardTimeout    time.Duration
	leaderWaitTimeout time.Duration
	shutdownTimeout   time.Duration
	snapshotInterval  time.Duration
	snapshotThreshold uint64
	trailingLogs      uint64
	logLevel          string
}

func parseOptions(args []string, output io.Writer) (options, error) {
	var opts options

	flags := flag.NewFlagSet("orderraft", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&opts.nodeID, "node-id", "node-1", "本节点的 Raft 节点 ID")
	flags.StringVar(&opts.raftAddr, "raft-addr", "", "本节点的 Raft 传输地址,留空时取 node-id(当前为内存传输,仅进程内有效)")
	flags.StringVar(&opts.grpcAddr, "grpc-addr", ":50052", "gRPC 服务监听地址")
	flags.StringVar(&opts.peers, "peers", "", "集群节点 ID 到 gRPC 地址的映射,格式 node-1=host:port,node-2=host:port")
	flags.StringVar(&opts.forwardToken, "forward-token", "", "集群共享的转发密钥,配置 -peers 时必填;用于识别内部转发请求")
	flags.DurationVar(&opts.applyTimeout, "apply-timeout", 3*time.Second, "提交 Raft 日志的超时")
	flags.DurationVar(&opts.forwardTimeout, "forward-timeout", 3*time.Second, "把写请求转发给 Leader 的超时")
	flags.DurationVar(&opts.leaderWaitTimeout, "leader-wait-timeout", 5*time.Second, "启动时等待 Raft Leader 的超时")
	flags.DurationVar(&opts.shutdownTimeout, "shutdown-timeout", 10*time.Second, "优雅关闭时等待在途请求的超时")
	flags.DurationVar(&opts.snapshotInterval, "snapshot-interval", 120*time.Second, "自动触发快照的最小间隔,0 表示禁用自动快照")
	flags.Uint64Var(&opts.snapshotThreshold, "snapshot-threshold", 8192, "自上次快照以来触发快照的日志条目数")
	flags.Uint64Var(&opts.trailingLogs, "trailing-logs", 10240, "快照完成后保留的尾部日志条目数")
	flags.StringVar(&opts.logLevel, "log-level", "info", "Raft 日志级别:trace/debug/info/warn/error")

	if err := flags.Parse(args); err != nil {
		return options{}, err
	}

	// -peers 只解析一次,校验与使用都复用同一份结果。
	peerAddrs, err := parsePeerGRPCAddrs(opts.peers)
	if err != nil {
		return options{}, err
	}
	opts.peerAddrs = peerAddrs

	if err := opts.validate(); err != nil {
		return options{}, err
	}
	return opts, nil
}

func (o options) validate() error {
	if strings.TrimSpace(o.nodeID) == "" {
		return errors.New("node-id 不能为空")
	}
	if strings.TrimSpace(o.grpcAddr) == "" {
		return errors.New("grpc-addr 不能为空")
	}
	if o.applyTimeout <= 0 {
		return errors.New("apply-timeout 必须大于 0")
	}
	if o.forwardTimeout <= 0 {
		return errors.New("forward-timeout 必须大于 0")
	}
	if o.leaderWaitTimeout <= 0 {
		return errors.New("leader-wait-timeout 必须大于 0")
	}
	if o.shutdownTimeout <= 0 {
		return errors.New("shutdown-timeout 必须大于 0")
	}
	// 0 表示禁用自动快照(由 raftnode 翻译为不会触发的超大间隔);
	// hashicorp/raft v1.7.3 自身拒绝 SnapshotInterval == 0,且要求非零值 >= 5ms。
	if o.snapshotInterval < 0 {
		return errors.New("snapshot-interval 不能为负数,0 表示禁用自动快照")
	}
	if o.snapshotInterval > 0 && o.snapshotInterval < 5*time.Millisecond {
		return errors.New("snapshot-interval 必须为 0(禁用)或不小于 5ms")
	}
	if o.snapshotThreshold == 0 {
		return errors.New("snapshot-threshold 必须大于 0")
	}
	if len(o.peerAddrs) > 0 && strings.TrimSpace(o.forwardToken) == "" {
		return errors.New("配置 -peers 时必须同时配置 -forward-token,用于识别内部转发请求")
	}
	return nil
}

// parsePeerGRPCAddrs 解析 -peers 参数，例如 "node-1=10.0.0.1:50052,node-2=10.0.0.2:50052"。
func parsePeerGRPCAddrs(raw string) (map[raft.ServerID]string, error) {
	peers := make(map[raft.ServerID]string)

	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		id, address, found := strings.Cut(entry, "=")
		id = strings.TrimSpace(id)
		address = strings.TrimSpace(address)
		if !found || id == "" || address == "" {
			return nil, fmt.Errorf("peers 参数格式错误: %q, 期望 node-1=host:port", entry)
		}
		if _, exists := peers[raft.ServerID(id)]; exists {
			return nil, fmt.Errorf("peers 参数中节点 ID 重复: %q", id)
		}
		peers[raft.ServerID(id)] = address
	}
	return peers, nil
}
