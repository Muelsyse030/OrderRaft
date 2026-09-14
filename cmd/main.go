package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
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

	peers, err := opts.peerGRPCAddrs()
	if err != nil {
		return err
	}

	state := fsm.New()
	node, err := raftnode.NewSingleNode(raftnode.Config{
		LocalID:     opts.nodeID,
		RaftAddress: opts.raftAddr,
		LogLevel:    opts.logLevel,
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
		PeerGRPCAddrs:  peers,
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

	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("gRPC 服务异常退出: %w", err)
		}
		return nil
	case <-signalCtx.Done():
		log.Printf("收到退出信号,开始优雅关闭(最多等待 %s)", opts.shutdownTimeout)
	}

	return shutdown(grpcServer, opts.shutdownTimeout)
}

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
		server.Stop()
		return errors.New("优雅关闭超时,已强制停止 gRPC 服务")
	}
}
