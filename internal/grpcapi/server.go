// Package grpcapi 实现 OrderService 的 gRPC 接口。
//
// 写请求必须由 Raft Leader 处理：本节点不是 Leader 时，会把请求原样转发给
// Leader 的 gRPC 地址，客户端因此不必感知集群拓扑。读请求直接读取本地 FSM。
package grpcapi

import (
	"context"
	"crypto/subtle"
	"errors"
	"log"
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
)

const (
	// ForwardedMetadataKey 标记请求已经由集群内其他节点转发过一次，避免节点之间反复转发。
	// 它的值必须等于 Config.ForwardToken：该 key 完全由客户端控制，只有匹配集群共享
	// 密钥的取值才会被信任，其余一律在入口处剥离。
	ForwardedMetadataKey = "x-orderraft-forwarded"

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

	// ForwardToken 是集群内节点共享的转发密钥。转发请求会携带
	// x-orderraft-forwarded: <token>，接收方只信任取值匹配的标记。
	// 未配置时任何入站标记都视为不可信并在入口剥离，因此客户端无法
	// 用伪造的 metadata 短路转发链路。
	ForwardToken string

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

	mu     sync.Mutex
	conns  map[string]*grpc.ClientConn
	closed bool
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

// Close 释放转发用的客户端连接，并阻止后续再建立新连接。
func (s *Server) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
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
	if err := validateCreateOrderRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	forwarded := s.takeForwardedFlag(ctx)

	if !s.cfg.Node.IsLeader() {
		if forwarded {
			return nil, alreadyForwardedError()
		}
		return s.forwardCreateOrder(ctx, req)
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
		// IsLeader 检查与 Apply 之间存在 TOCTOU：拿到 leadership 变更错误时
		// 重新解析 Leader 并转发一次，而不是把 UNAVAILABLE 直接抛给客户端。
		//
		// 注意 ErrLeadershipLost 时原条目可能已经提交并 apply，转发重试会让同一
		// 命令第二次进入日志；FSM 幂等表会拦截并返回 replayed=true（客户端可能
		// 对"首次提交却收到 replayed"感到意外，这属于可接受的冗余条目）。
		if isLeaderChanged(err) && !forwarded {
			if response, forwardErr := s.forwardCreateOrder(ctx, req); forwardErr == nil {
				return response, nil
			}
		}
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
	if err := validateChangeOrderStatusRequest(req); err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	forwarded := s.takeForwardedFlag(ctx)

	if !s.cfg.Node.IsLeader() {
		if forwarded {
			return nil, alreadyForwardedError()
		}
		return s.forwardChangeOrderStatus(ctx, req)
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
		// 同 CreateOrder：leadership 变更时改为转发给新 Leader；ErrLeadershipLost
		// 可能让同一命令第二次进入日志，由 FSM 幂等表拦截并返回 replayed=true。
		if isLeaderChanged(err) && !forwarded {
			if response, forwardErr := s.forwardChangeOrderStatus(ctx, req); forwardErr == nil {
				return response, nil
			}
		}
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

// forwardToLeader 用生成的 client stub 把写请求原样转发给当前 Leader。
//
// 使用 stub 而不是 conn.Invoke，可以复用 gRPC 的拦截器、重试与统计策略。
func forwardToLeader[Request, Response any](
	ctx context.Context,
	leaderConn func() (*grpc.ClientConn, error),
	timeout time.Duration,
	token string,
	call func(context.Context, orderv1.OrderServiceClient, Request) (Response, error),
	request Request,
) (Response, error) {
	var zero Response

	conn, err := leaderConn()
	if err != nil {
		return zero, err
	}

	forwardCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if token != "" {
		forwardCtx = metadata.AppendToOutgoingContext(forwardCtx, ForwardedMetadataKey, token)
	}

	return call(forwardCtx, orderv1.NewOrderServiceClient(conn), request)
}

func (s *Server) forwardCreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
	return forwardToLeader(
		ctx,
		s.leaderConn,
		s.cfg.ForwardTimeout,
		s.cfg.ForwardToken,
		func(ctx context.Context, client orderv1.OrderServiceClient, request *orderv1.CreateOrderRequest) (*orderv1.CreateOrderResponse, error) {
			return client.CreateOrder(ctx, request)
		},
		req,
	)
}

func (s *Server) forwardChangeOrderStatus(ctx context.Context, req *orderv1.ChangeOrderStatusRequest) (*orderv1.ChangeOrderStatusResponse, error) {
	return forwardToLeader(
		ctx,
		s.leaderConn,
		s.cfg.ForwardTimeout,
		s.cfg.ForwardToken,
		func(ctx context.Context, client orderv1.OrderServiceClient, request *orderv1.ChangeOrderStatusRequest) (*orderv1.ChangeOrderStatusResponse, error) {
			return client.ChangeOrderStatus(ctx, request)
		},
		req,
	)
}

// leaderConn 解析当前 Leader 并返回复用的连接。
func (s *Server) leaderConn() (*grpc.ClientConn, error) {
	_, leaderID := s.cfg.Node.Leader()
	if leaderID == "" {
		return nil, status.Error(codes.Unavailable, "raft leader is currently unknown; please retry")
	}

	address := strings.TrimSpace(s.cfg.PeerGRPCAddrs[leaderID])
	if address == "" {
		return nil, status.Errorf(
			codes.Unavailable,
			"gRPC address of raft leader %q is not configured; start this node with -peers",
			leaderID,
		)
	}

	conn, err := s.connection(address)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "connect to raft leader %q at %s: %v", leaderID, address, err)
	}
	return conn, nil
}

// connection 按地址复用到其他节点的连接；服务器关闭后拒绝再建立新连接。
func (s *Server) connection(address string) (*grpc.ClientConn, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil, errors.New("gRPC server is closed")
	}
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

// takeForwardedFlag 判断入站请求是否来自集群内其他节点的转发。
//
// 该 key 完全由客户端控制,是否可信只取决于取值是否与集群共享密钥
// ForwardToken 常量时间相等,未配置密钥时一律不可信。
//
// 这里刻意不修改 gRPC 持有的入站 metadata.MD(它不保证并发安全,就地删除会影响
// 调用方仍可观察到的共享结构);入站标记也不会被转发到下游——转发请求的
// outgoing metadata 是单独构造的,因此不需要就地"剥离"。
func (s *Server) takeForwardedFlag(ctx context.Context) bool {
	incoming, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}

	values := incoming.Get(ForwardedMetadataKey)
	if len(values) == 0 {
		return false
	}

	token := s.cfg.ForwardToken
	if token == "" {
		return false
	}

	for _, value := range values {
		if subtle.ConstantTimeCompare([]byte(value), []byte(token)) == 1 {
			return true
		}
	}
	return false
}

func alreadyForwardedError() error {
	return status.Error(
		codes.Unavailable,
		"this node is not the raft leader and the request was already forwarded once",
	)
}

func isLeaderChanged(err error) bool {
	return errors.Is(err, raft.ErrNotLeader) ||
		errors.Is(err, raft.ErrLeadershipLost)
}

func mapRaftApplyError(err error) error {
	switch {
	case errors.Is(err, fsm.ErrInvalidCommand):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, fsm.ErrApplyFailed):
		// 条目已提交但应用阶段失败(解码失败、FSM 返回非法响应等),重试不会改变结果。
		return status.Error(codes.Internal, err.Error())
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
		// 未归类的错误多来自 raft 提交阶段(超时、连接等瞬态问题),映射为
		// Unavailable 让客户端保留重试机会,同时记录一次原始错误便于定位。
		log.Printf("orderraft: 未归类的 raft apply 错误,按 Unavailable 返回: %v", err)
		return status.Error(codes.Unavailable, err.Error())
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

func validateCreateOrderRequest(req *orderv1.CreateOrderRequest) error {
	switch {
	case req.GetRequestId() == "":
		return errors.New("request_id is required")
	case req.GetOrderId() == "":
		return errors.New("order_id is required")
	case req.GetUserId() == "":
		return errors.New("user_id is required")
	case req.GetAmountCents() <= 0:
		return errors.New("amount_cents must be greater than zero")
	case req.GetCurrency() == "":
		return errors.New("currency is required")
	}
	return nil
}

func validateChangeOrderStatusRequest(req *orderv1.ChangeOrderStatusRequest) error {
	switch {
	case req.GetRequestId() == "":
		return errors.New("request_id is required")
	case req.GetOrderId() == "":
		return errors.New("order_id is required")
	case req.GetTargetStatus() == orderv1.OrderStatus_ORDER_STATUS_UNSPECIFIED:
		return errors.New("target_status is required")
	}
	return nil
}
