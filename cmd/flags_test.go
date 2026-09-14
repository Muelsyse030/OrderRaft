package main

import (
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestParseOptionsDefaults(t *testing.T) {
	opts, err := parseOptions(nil, io.Discard)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}

	if opts.nodeID != "node-1" {
		t.Fatalf("nodeID = %q, 期望 node-1", opts.nodeID)
	}
	if opts.grpcAddr != ":50052" {
		t.Fatalf("grpcAddr = %q, 期望 :50052", opts.grpcAddr)
	}
	if opts.raftAddr != "" {
		t.Fatalf("raftAddr = %q, 期望留空", opts.raftAddr)
	}
	if opts.logLevel != "info" {
		t.Fatalf("logLevel = %q, 期望 info", opts.logLevel)
	}
	if opts.applyTimeout != 3*time.Second {
		t.Fatalf("applyTimeout = %s, 期望 3s", opts.applyTimeout)
	}
	if opts.forwardTimeout != 3*time.Second {
		t.Fatalf("forwardTimeout = %s, 期望 3s", opts.forwardTimeout)
	}
	if opts.leaderWaitTimeout != 5*time.Second {
		t.Fatalf("leaderWaitTimeout = %s, 期望 5s", opts.leaderWaitTimeout)
	}
	if opts.shutdownTimeout != 10*time.Second {
		t.Fatalf("shutdownTimeout = %s, 期望 10s", opts.shutdownTimeout)
	}

	peers, err := opts.peerGRPCAddrs()
	if err != nil {
		t.Fatalf("peerGRPCAddrs() error = %v", err)
	}
	if len(peers) != 0 {
		t.Fatalf("peers = %v, 期望为空", peers)
	}
}

func TestParseOptionsOverrides(t *testing.T) {
	opts, err := parseOptions([]string{
		"-node-id", "node-2",
		"-raft-addr", "127.0.0.1:50062",
		"-grpc-addr", "127.0.0.1:50053",
		"-peers", "node-1=127.0.0.1:50052,node-2=127.0.0.1:50053",
		"-apply-timeout", "1500ms",
		"-forward-timeout", "2s",
		"-leader-wait-timeout", "1s",
		"-shutdown-timeout", "4s",
		"-log-level", "warn",
	}, io.Discard)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}

	if opts.nodeID != "node-2" || opts.raftAddr != "127.0.0.1:50062" {
		t.Fatalf("节点配置解析错误: %+v", opts)
	}
	if opts.logLevel != "warn" {
		t.Fatalf("logLevel = %q", opts.logLevel)
	}
	if opts.applyTimeout != 1500*time.Millisecond || opts.forwardTimeout != 2*time.Second {
		t.Fatalf("超时配置解析错误: %+v", opts)
	}

	peers, err := opts.peerGRPCAddrs()
	if err != nil {
		t.Fatalf("peerGRPCAddrs() error = %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("peers = %v, 期望 2 个节点", peers)
	}
	if peers[raft.ServerID("node-1")] != "127.0.0.1:50052" {
		t.Fatalf("node-1 的 gRPC 地址 = %q", peers[raft.ServerID("node-1")])
	}
}

func TestParseOptionsRejectsInvalidPeers(t *testing.T) {
	cases := map[string]string{
		"缺少等号":     "node-1",
		"缺少地址":     "node-1=",
		"缺少节点 ID":  "=127.0.0.1:50052",
		"节点 ID 重复": "node-1=127.0.0.1:50052,node-1=127.0.0.1:50053",
	}

	for name, peers := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseOptions([]string{"-peers", peers}, io.Discard); err == nil {
				t.Fatalf("peers = %q 应返回错误", peers)
			}
		})
	}
}

func TestParseOptionsIgnoresEmptyPeers(t *testing.T) {
	opts, err := parseOptions([]string{"-peers", "node-1=127.0.0.1:50052, ,"}, io.Discard)
	if err != nil {
		t.Fatalf("parseOptions() error = %v", err)
	}

	peers, err := opts.peerGRPCAddrs()
	if err != nil {
		t.Fatalf("peerGRPCAddrs() error = %v", err)
	}
	if len(peers) != 1 {
		t.Fatalf("peers = %v, 期望忽略空白项后只剩 1 个节点", peers)
	}
}

func TestParseOptionsRejectsBadInput(t *testing.T) {
	cases := map[string][]string{
		"未知参数":            {"-unknown"},
		"node-id 为空":      {"-node-id", " "},
		"grpc-addr 为空":    {"-grpc-addr", " "},
		"提交超时为 0":         {"-apply-timeout", "0s"},
		"转发超时为负":          {"-forward-timeout", "-1s"},
		"等待 Leader 超时为 0": {"-leader-wait-timeout", "0s"},
		"关闭超时为 0":         {"-shutdown-timeout", "0s"},
	}

	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := parseOptions(args, io.Discard); err == nil {
				t.Fatalf("args = %v 应返回错误", args)
			}
		})
	}
}
