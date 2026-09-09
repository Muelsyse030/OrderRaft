package main

import (
	"context"
	"errors"
	"log"
	"net"
	"time"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"example.com/OrderRaft/internal/fsm"
	"example.com/OrderRaft/internal/raftnode"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type orderServer struct {
	orderv1.UnimplementedOrderServiceServer

	raftNode *raftnode.Node
}

func newOrderServer(node *raftnode.Node) *orderServer {
	return &orderServer{raftNode: node}
}

func (s *orderServer) CreateOrder(_ context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	command := &orderv1.RaftCommand{
		RequestId:       req.GetRequestId(),
		AppliedAtUnixMs: time.Now().UnixMilli(),
		SchemaVersion:   1,
		Operation: &orderv1.RaftCommand_CreateOrder{
			CreateOrder: &orderv1.CreateOrderCommand{
				OrderId:     req.GetOrderId(),
				UserId:      req.GetUserId(),
				AmountCents: req.GetAmountCents(),
				Currency:    req.GetCurrency(),
			},
		},
	}
	result, err := s.raftNode.Apply(
		command,
		3*time.Second,
	)
	if err != nil {
		return nil, mapRaftApplyError(err)
	}

	switch result.GetCode() {
	case fsm.ResultCodeOK:
		return &orderv1.CreateOrderResponse{
			RequestId: result.GetRequestId(),
			Code:      "ok",
			Message:   result.GetMessage(),
			Order:     result.GetOrder(),
		}, nil

	case fsm.ResultCodeInvalidArgument:
		return nil, status.Error(codes.InvalidArgument, result.GetMessage())
	case fsm.ResultCodeOrderAlreadyExists:
		return nil, status.Error(codes.AlreadyExists, result.GetMessage())
	default:
		return nil, status.Errorf(codes.Internal, "unexpected FSM result: code=%s message=%s", result.GetCode(), result.GetMessage())
	}
}

func (s *orderServer) GetOrder(_ context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "GetOrderRequest is nil")
	}
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "GetOrderRequest is empty")
	}
	order, err := s.raftNode.FSM().GetOrder(req.GetOrderId())
	if err != nil {
		if errors.Is(err, fsm.ErrOrderNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &orderv1.GetOrderResponse{Code: "ok", Message: "order found", Order: order}, nil
}

func (s *orderServer) ChangeOrderStatus(_ context.Context, req *orderv1.ChangeOrderStatusRequest) (*orderv1.ChangeOrderStatusResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}
	command := &orderv1.RaftCommand{
		RequestId:       req.GetRequestId(),
		AppliedAtUnixMs: time.Now().UnixMilli(),
		SchemaVersion:   1,
		Operation: &orderv1.RaftCommand_ChangeOrderStatus{
			ChangeOrderStatus: &orderv1.ChangeOrderStatusCommand{
				OrderId:      req.GetOrderId(),
				TargetStatus: req.GetTargetStatus(),
			},
		},
	}

	result, err := s.raftNode.Apply(
		command,
		3*time.Second,
	)
	if err != nil {
		return nil, mapRaftApplyError(err)
	}

	switch result.GetCode() {
	case fsm.ResultCodeOK:
		return &orderv1.ChangeOrderStatusResponse{
			RequestId: result.GetRequestId(),
			Code:      "ok",
			Message:   result.GetMessage(),
			Order:     result.GetOrder(),
			Replayed:  result.GetReplayed(),
		}, nil

	case fsm.ResultCodeInvalidArgument:
		return nil, status.Error(codes.InvalidArgument, result.GetMessage())
	case fsm.ResultCodeOrderNotFound:
		return nil, status.Error(codes.NotFound, result.GetMessage())
	case fsm.ResultCodeInvalidStatusTransition:
		return nil, status.Error(codes.FailedPrecondition, result.GetMessage())
	default:
		return nil, status.Errorf(codes.Internal, "unexpected FSM result: code=%s message=%s", result.GetCode(), result.GetMessage())
	}
}

func mapRaftApplyError(err error) error {
	switch {
	case errors.Is(err, fsm.ErrRequestConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, raft.ErrNotLeader):
		return status.Error(codes.Unavailable, "current node is not the Raft leader")
	case errors.Is(err, raft.ErrLeadershipLost):
		return status.Error(codes.Unavailable, "Raft leadership was lost while committing command")
	case errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return status.Error(codes.Unavailable, "Raft leadership transfer is in progress")
	case errors.Is(err, raft.ErrEnqueueTimeout):
		return status.Error(codes.DeadlineExceeded, "timed out submitting command to Raft")
	case errors.Is(err, raft.ErrRaftShutdown):
		return status.Error(codes.Unavailable, "Raft node is shut down")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func main() {
	state := fsm.New()
	node, err := raftnode.NewSingleNode("node-1", state)
	if err != nil {
		log.Fatalf("创建 Raft 节点失败: %v", err)
	}
	defer func() {
		if err := node.Close(); err != nil {
			log.Printf("关闭 Raft 节点失败: %v", err)
		}
	}()
	if err := node.WaitForLeader(5 * time.Second); err != nil {
		log.Fatalf("等待 Raft Leader 失败: %v", err)
	}
	leaderAddress, leaderID := node.Leader()
	log.Printf("Raft Leader 已就绪: id=%s address=%s", leaderID, leaderAddress)
	listener, err := net.Listen("tcp", ":50052")
	if err != nil {
		log.Fatalf("监听 gRPC 端口失败: %v", err)
	}
	grpcServer := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(grpcServer, newOrderServer(node))
	log.Println("order gRPC server is listening on :50052")

	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("启动 gRPC 服务失败: %v", err)
	}
}
