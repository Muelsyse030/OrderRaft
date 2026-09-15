package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"example.com/OrderRaft/internal/fsm"
	"example.com/OrderRaft/internal/grpcapi"
	"example.com/OrderRaft/internal/raftnode"

	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	if err := run(os.Args[1:], os.Stderr); err != nil {
		log.Fatalf("order server 退出: %v", err)
	}
}

func run(args []string, output io.Writer) error {
	opts, err := parseOptions(args, output)
	if err != nil {
		return err
	}

	// 在启动 gRPC 服务之前注册信号处理，避免服务已就绪但信号尚未被捕获的竞态。
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger := slog.New(slog.NewTextHandler(output, &slog.HandlerOptions{
		Level: slogLevel(opts.logLevel),
	}))

	state := fsm.NewWithLogger(logger)
	node, err := raftnode.NewSingleNode(raftnode.Config{
		LocalID:           opts.nodeID,
		RaftAddress:       opts.raftAddr,
		LogLevel:          opts.logLevel,
		SnapshotInterval:  opts.snapshotInterval,
		DisableSnapshots:  opts.snapshotInterval == 0,
		SnapshotThreshold: opts.snapshotThreshold,
		TrailingLogs:      opts.trailingLogs,
	}, state)
	if err != nil {
		return fmt.Errorf("创建 Raft 节点失败: %w", err)
	}
	defer func() {
		if err := node.Close(); err != nil {
			log.Printf("关闭 Raft 节点失败: %v", err)
		}
	}()

	if err := node.WaitForLeader(opts.leaderWaitTimeout); err != nil {
		return fmt.Errorf("等待 Raft Leader 失败: %w", err)
	}
	leaderAddress, leaderID := node.Leader()
	log.Printf("Raft Leader 已就绪: id=%s address=%s", leaderID, leaderAddress)

	api, err := grpcapi.New(grpcapi.Config{
		Node:           node,
		PeerGRPCAddrs:  opts.peerAddrs,
		ForwardToken:   opts.forwardToken,
		ApplyTimeout:   opts.applyTimeout,
		ForwardTimeout: opts.forwardTimeout,
	})
	if err != nil {
		return fmt.Errorf("创建 gRPC 服务失败: %w", err)
	}
	defer func() {
		if err := api.Close(); err != nil {
			log.Printf("关闭转发连接失败: %v", err)
		}
	}()

	listener, err := net.Listen("tcp", opts.grpcAddr)
	if err != nil {
		return fmt.Errorf("监听 gRPC 端口失败: %w", err)
	}
	defer func() {
		_ = listener.Close()
	}()

	grpcServer := grpc.NewServer()
	api.Register(grpcServer)

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- grpcServer.Serve(listener)
	}()
	log.Printf("order gRPC server 正在监听 %s", opts.grpcAddr)

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("gRPC 服务异常退出: %w", err)
		}
		return nil
	case <-signalCtx.Done():
		log.Printf("收到退出信号,开始优雅关闭(最多等待 %s)", opts.shutdownTimeout)
	}

	// 优雅关闭超时是设计内的正常路径：仅告警并正常返回，让 defer 完成收尾。
	if err := shutdown(grpcServer, opts.shutdownTimeout); err != nil {
		log.Printf("警告: %v", err)
	}
	return nil
}

// forceStopGrace 是触发强制停止后,等待 GracefulStop 协程收尾的上限。
// 在途 handler 若忽略 context 可能永不返回,此时 GracefulStop 会一直等待,
// 所以关闭流程必须有界,不能被拖死。
const forceStopGrace = 200 * time.Millisecond

// shutdown 先停止接收新请求，等待在途请求结束后再返回。
func shutdown(server *grpc.Server, timeout time.Duration) error {
	done := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(done)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-done:
		log.Print("gRPC 服务已优雅关闭")
		return nil
	case <-timer.C:
		// server.Stop() 会等到在途 RPC 真正结束;若与仍在等待 handler 的
		// GracefulStop 并发调用,可能长时间阻塞。放到独立协程执行,保证关闭
		// 流程本身不再被拖住;进程随后正常退出会回收相关协程。
		go server.Stop()

		grace := time.NewTimer(forceStopGrace)
		defer grace.Stop()
		select {
		case <-done:
		case <-grace.C:
			log.Print("警告: 强制停止后仍存在未结束的在途请求")
		}
		return errors.New("优雅关闭超时,已强制停止 gRPC 服务")
	}
}

// slogLevel 把 -log-level 映射到 slog 级别,供 FSM 的结构化日志使用。
func slogLevel(level string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "trace", "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
