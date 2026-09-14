// Package grpcapi 实现 OrderService 的 gRPC 接口。
//
// 写请求必须由 Raft Leader 处理：本节点不是 Leader 时，会把请求原样转发给
// Leader 的 gRPC 地址，客户端因此不必感知集群拓扑。读请求直接读取本地 FSM。
package grpcapi

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"example.com/OrderRaft/internal/fsm"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	// ForwardedMetadataKey 标记请求已经由其他节点转发过一次，避免节点之间反复转发。
	ForwardedMetadataKey = "x-orderraft-forwarded"

	createOrderMethod       = "/order.v1.OrderService/CreateOrder"
	changeOrderStatusMethod = "/order.v1.OrderService/ChangeOrderStatus"

	defaultApplyTimeout   = 3 * time.Second
	defaultForwardTimeout = 3 * time.Second
)

// Node 是本包依赖的 Raft 能力，抽象成接口以便测试替换。
type Node interface {
	IsLeader() bool
	Leader() (raft.ServerAddress, raft.ServerID)
	Apply(command *orderv1.RaftCommand, timeout time.Duration) (*orderv1.RaftCommandResult, error)
	FSM() *fsm.FSM
}

type Config struct {
	Node Node

	// PeerGRPCAddrs 把 Raft 节点 ID 映射到对应节点的 gRPC 地址，
	// 非 Leader 节点据此把写请求转发给 Leader。
	PeerGRPCAddrs map[raft.ServerID]string

	// ApplyTimeout 是本节点提交 Raft 日志的超时。
	ApplyTimeout time.Duration

	// ForwardTimeout 是转发给 Leader 的超时；调用方上下文已有更早的
	// deadline 时以调用方为准。
	ForwardTimeout time.Duration

	// DialOptions 用于建立到 Leader 的连接，默认使用明文连接。
	DialOptions []grpc.DialOption
}

// Server 实现 orderv1.OrderServiceServer。
type Server struct {
	orderv1.UnimplementedOrderServiceServer

	cfg Config

	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
}

var _ orderv1.OrderServiceServer = (*Server)(nil)

func New(cfg Config) (*Server, error) {
	if cfg.Node == nil {
		return nil, errors.New("raft node is required")
	}
	if cfg.ApplyTimeout <= 0 {
		cfg.ApplyTimeout = defaultApplyTimeout
	}
	if cfg.ForwardTimeout <= 0 {
		cfg.ForwardTimeout = defaultForwardTimeout
	}
	if len(cfg.DialOptions) == 0 {
		cfg.DialOptions = []grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}
	}
	return &Server{
		cfg:   cfg,
		conns: make(map[string]*grpc.ClientConn),
	}, nil
}

// Register 把 OrderService 注册到 gRPC 服务器。
func (s *Server) Register(registrar grpc.ServiceRegistrar) {
	orderv1.RegisterOrderServiceServer(registrar, s)
}

// Close 释放转发用的客户端连接。
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	conns := make([]*grpc.ClientConn, 0, len(s.conns))
	for address, conn := range s.conns {
		conns = append(conns, conn)
		delete(s.conns, address)
	}
	s.mu.Unlock()

	var errs []error
	for _, conn := range conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *Server) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}

	if !s.cfg.Node.IsLeader() {
		response := &orderv1.CreateOrderResponse{}
		if err := s.forward(ctx, createOrderMethod, req, response); err != nil {
			return nil, err
		}
		return response, nil
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
	result, err := s.cfg.Node.Apply(command, s.cfg.ApplyTimeout)
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
			Replayed:  result.GetReplayed(),
		}, nil
	case fsm.ResultCodeInvalidArgument:
		return nil, status.Error(codes.InvalidArgument, result.GetMessage())
	case fsm.ResultCodeOrderAlreadyExists:
		return nil, status.Error(codes.AlreadyExists, result.GetMessage())
	default:
		return nil, unexpectedResult(result)
	}
}

func (s *Server) GetOrder(_ context.Context, req *orderv1.GetOrderRequest) (*orderv1.GetOrderResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetOrderId() == "" {
		return nil, status.Error(codes.InvalidArgument, "order_id is required")
	}

	state := s.cfg.Node.FSM()
	if state == nil {
		return nil, status.Error(codes.Internal, "local FSM is not available")
	}

	order, err := state.GetOrder(req.GetOrderId())
	if err != nil {
		if errors.Is(err, fsm.ErrOrderNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &orderv1.GetOrderResponse{Code: "ok", Message: "order found", Order: order}, nil
}

func (s *Server) ChangeOrderStatus(ctx context.Context, req *orderv1.ChangeOrderStatusRequest) (*orderv1.ChangeOrderStatusResponse, error) {
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "request is required")
	}
	if req.GetRequestId() == "" {
		return nil, status.Error(codes.InvalidArgument, "request_id is required")
	}

	if !s.cfg.Node.IsLeader() {
		response := &orderv1.ChangeOrderStatusResponse{}
		if err := s.forward(ctx, changeOrderStatusMethod, req, response); err != nil {
			return nil, err
		}
		return response, nil
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
	result, err := s.cfg.Node.Apply(command, s.cfg.ApplyTimeout)
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
		return nil, unexpectedResult(result)
	}
}

// forward 把写请求原样转发给当前 Leader，响应直接写回调用方。
func (s *Server) forward(ctx context.Context, method string, request, response proto.Message) error {
	if forwarded, ok := metadata.FromIncomingContext(ctx); ok && len(forwarded.Get(ForwardedMetadataKey)) > 0 {
		return status.Error(
			codes.Unavailable,
			"this node is not the raft leader and the request was already forwarded once",
		)
	}

	_, leaderID := s.cfg.Node.Leader()
	if leaderID == "" {
		return status.Error(codes.Unavailable, "raft leader is currently unknown; please retry")
	}

	address := strings.TrimSpace(s.cfg.PeerGRPCAddrs[leaderID])
	if address == "" {
		return status.Errorf(
			codes.Unavailable,
			"gRPC address of raft leader %q is not configured; start this node with -peers",
			leaderID,
		)
	}

	conn, err := s.connection(address)
	if err != nil {
		return status.Errorf(codes.Unavailable, "connect to raft leader %q at %s: %v", leaderID, address, err)
	}

	forwardCtx, cancel := context.WithTimeout(ctx, s.cfg.ForwardTimeout)
	defer cancel()
	forwardCtx = metadata.AppendToOutgoingContext(forwardCtx, ForwardedMetadataKey, "true")

	return conn.Invoke(forwardCtx, method, request, response)
}

// connection 按地址复用到其他节点的连接。
func (s *Server) connection(address string) (*grpc.ClientConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if conn, ok := s.conns[address]; ok {
		return conn, nil
	}
	conn, err := grpc.NewClient(address, s.cfg.DialOptions...)
	if err != nil {
		return nil, err
	}
	s.conns[address] = conn
	return conn, nil
}

func mapRaftApplyError(err error) error {
	switch {
	case errors.Is(err, fsm.ErrRequestConflict):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, raft.ErrNotLeader):
		return status.Error(codes.Unavailable, "current node is not the raft leader")
	case errors.Is(err, raft.ErrLeadershipLost):
		return status.Error(codes.Unavailable, "raft leadership was lost while committing command")
	case errors.Is(err, raft.ErrLeadershipTransferInProgress):
		return status.Error(codes.Unavailable, "raft leadership transfer is in progress")
	case errors.Is(err, raft.ErrEnqueueTimeout):
		return status.Error(codes.DeadlineExceeded, "timed out submitting command to raft")
	case errors.Is(err, raft.ErrRaftShutdown):
		return status.Error(codes.Unavailable, "raft node is shut down")
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func unexpectedResult(result *orderv1.RaftCommandResult) error {
	return status.Errorf(
		codes.Internal,
		"unexpected FSM result: code=%s message=%s",
		result.GetCode(),
		result.GetMessage(),
	)
}
