package main

import (
	"context"
	"net"
	"testing"
	"time"

	orderv1 "example.com/OrderRaft/gen/order/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// blockingOrderService 的 CreateOrder 会一直阻塞,用于制造"在途请求不结束"的场景。
type blockingOrderService struct {
	orderv1.UnimplementedOrderServiceServer

	started chan struct{}
	release chan struct{}
}

func (s *blockingOrderService) CreateOrder(
	context.Context,
	*orderv1.CreateOrderRequest,
) (*orderv1.CreateOrderResponse, error) {
	close(s.started)
	<-s.release

	return &orderv1.CreateOrderResponse{}, nil
}

// 没有在途请求时,优雅关闭应立即成功返回。
func TestShutdownGraceful(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	server := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(server, &blockingOrderService{
		started: make(chan struct{}),
		release: make(chan struct{}),
	})

	go func() {
		_ = server.Serve(listener)
	}()

	if err := shutdown(server, 2*time.Second); err != nil {
		t.Fatalf("shutdown() error = %v", err)
	}
}

// 关闭超时分支必须强制停止并返回错误,且不能因为 GracefulStop 协程而挂起。
func TestShutdownTimeoutForceStops(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	server := grpc.NewServer()
	service := &blockingOrderService{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	orderv1.RegisterOrderServiceServer(server, service)

	go func() {
		_ = server.Serve(listener)
	}()

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	client := orderv1.NewOrderServiceClient(conn)

	go func() {
		_, _ = client.CreateOrder(context.Background(), &orderv1.CreateOrderRequest{
			RequestId: "request-001",
			OrderId:   "order-001",
			UserId:    "user-001",
			Currency:  "CNY",
		})
	}()

	select {
	case <-service.started:
	case <-time.After(5 * time.Second):
		t.Fatal("在途请求未能进入 handler")
	}

	done := make(chan error, 1)
	go func() {
		done <- shutdown(server, 200*time.Millisecond)
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("关闭超时分支应返回错误")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown() 在超时分支挂起")
	}

	close(service.release)
}
