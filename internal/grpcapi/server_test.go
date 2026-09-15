package grpcapi

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	orderv1 "example.com/OrderRaft/gen/order/v1"
	"example.com/OrderRaft/internal/fsm"
	"example.com/OrderRaft/internal/raftnode"

	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// stubNode 模拟一个非 Leader 节点：它不持有 Raft，只报告 Leader 的位置。
type stubNode struct {
	leaderID   raft.ServerID
	leaderAddr raft.ServerAddress
	state      *fsm.FSM
}

func (s *stubNode) IsLeader() bool { return false }

func (s *stubNode) Leader() (raft.ServerAddress, raft.ServerID) {
	return s.leaderAddr, s.leaderID
}

func (s *stubNode) Apply(*orderv1.RaftCommand, time.Duration) (*orderv1.RaftCommandResult, error) {
	return nil, errors.New("非 Leader 节点不应在本地应用命令")
}

func (s *stubNode) FSM() *fsm.FSM { return s.state }

// leaderChangeNode 模拟"检查时自认为是 Leader，但提交时 leadership 已经丢失"的节点。
type leaderChangeNode struct {
	leaderID   raft.ServerID
	leaderAddr raft.ServerAddress
	state      *fsm.FSM
	applyErr   error
}

func (n *leaderChangeNode) IsLeader() bool { return true }

func (n *leaderChangeNode) Leader() (raft.ServerAddress, raft.ServerID) {
	return n.leaderAddr, n.leaderID
}

func (n *leaderChangeNode) Apply(*orderv1.RaftCommand, time.Duration) (*orderv1.RaftCommandResult, error) {
	return nil, n.applyErr
}

func (n *leaderChangeNode) FSM() *fsm.FSM { return n.state }

func TestNewRequiresRaftNode(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("缺少 Raft 节点时应返回错误")
	}
}

func TestNewAppliesDefaults(t *testing.T) {
	api, err := New(Config{Node: &stubNode{state: fsm.New()}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := api.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})

	if api.cfg.ApplyTimeout != defaultApplyTimeout {
		t.Fatalf("ApplyTimeout = %s, 期望默认值 %s", api.cfg.ApplyTimeout, defaultApplyTimeout)
	}
	if api.cfg.ForwardTimeout != defaultForwardTimeout {
		t.Fatalf("ForwardTimeout = %s, 期望默认值 %s", api.cfg.ForwardTimeout, defaultForwardTimeout)
	}
	if len(api.cfg.DialOptions) == 0 {
		t.Fatal("DialOptions 应填充默认的明文连接配置")
	}
}

func TestCreateOrderIsIdempotent(t *testing.T) {
	node := newLeaderNode(t)
	_, client := startAPI(t, Config{Node: node})

	ctx := context.Background()
	first, err := client.CreateOrder(ctx, createOrderRequest("request-001", "order-001"))
	if err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}
	if first.GetReplayed() {
		t.Fatal("首次创建不应标记 replayed")
	}
	if first.GetOrder().GetVersion() != 1 {
		t.Fatalf("首次创建 version = %d, 期望 1", first.GetOrder().GetVersion())
	}

	// 同一 request_id 重试：返回首次执行结果。
	retry, err := client.CreateOrder(ctx, createOrderRequest("request-001", "order-001"))
	if err != nil {
		t.Fatalf("重试 CreateOrder() error = %v", err)
	}
	if !retry.GetReplayed() {
		t.Fatal("重试应标记 replayed")
	}
	if retry.GetOrder().GetVersion() != 1 {
		t.Fatalf("重试不应改变 version: %d", retry.GetOrder().GetVersion())
	}
	if retry.GetOrder().GetCreatedAtUnixMs() != first.GetOrder().GetCreatedAtUnixMs() {
		t.Fatal("重试应返回首次创建时间")
	}

	// 换 request_id 但复用 order_id：订单已存在。
	_, err = client.CreateOrder(ctx, createOrderRequest("request-002", "order-001"))
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("重复 order_id 的 code = %v, 期望 AlreadyExists", status.Code(err))
	}

	// 缺少 request_id。
	_, err = client.CreateOrder(ctx, &orderv1.CreateOrderRequest{OrderId: "order-002"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("缺少 request_id 的 code = %v, 期望 InvalidArgument", status.Code(err))
	}
}

func TestChangeOrderStatusFollowsStateMachine(t *testing.T) {
	node := newLeaderNode(t)
	_, client := startAPI(t, Config{Node: node})

	ctx := context.Background()
	if _, err := client.CreateOrder(ctx, createOrderRequest("request-001", "order-001")); err != nil {
		t.Fatalf("CreateOrder() error = %v", err)
	}

	paid, err := client.ChangeOrderStatus(ctx, &orderv1.ChangeOrderStatusRequest{
		RequestId:    "request-002",
		OrderId:      "order-001",
		TargetStatus: orderv1.OrderStatus_ORDER_STATUS_PAID,
	})
	if err != nil {
		t.Fatalf("ChangeOrderStatus() error = %v", err)
	}
	if paid.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PAID {
		t.Fatalf("status = %s, 期望 PAID", paid.GetOrder().GetStatus())
	}
	if paid.GetOrder().GetVersion() != 2 {
		t.Fatalf("version = %d, 期望 2", paid.GetOrder().GetVersion())
	}

	replayed, err := client.ChangeOrderStatus(ctx, &orderv1.ChangeOrderStatusRequest{
		RequestId:    "request-002",
		OrderId:      "order-001",
		TargetStatus: orderv1.OrderStatus_ORDER_STATUS_PAID,
	})
	if err != nil {
		t.Fatalf("重试 ChangeOrderStatus() error = %v", err)
	}
	if !replayed.GetReplayed() {
		t.Fatal("重试应标记 replayed")
	}
	if replayed.GetOrder().GetVersion() != 2 {
		t.Fatalf("重试不应改变 version: %d", replayed.GetOrder().GetVersion())
	}

	// PAID -> COMPLETED 不是合法流转。
	_, err = client.ChangeOrderStatus(ctx, &orderv1.ChangeOrderStatusRequest{
		RequestId:    "request-003",
		OrderId:      "order-001",
		TargetStatus: orderv1.OrderStatus_ORDER_STATUS_COMPLETED,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("非法流转的 code = %v, 期望 FailedPrecondition", status.Code(err))
	}

	_, err = client.ChangeOrderStatus(ctx, &orderv1.ChangeOrderStatusRequest{
		RequestId:    "request-004",
		OrderId:      "order-404",
		TargetStatus: orderv1.OrderStatus_ORDER_STATUS_PAID,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("订单不存在的 code = %v, 期望 NotFound", status.Code(err))
	}
}

func TestWriteRequestsAreForwardedToLeader(t *testing.T) {
	leader := newLeaderNode(t)
	leaderAddr, leaderClient := startAPI(t, Config{Node: leader})

	followerState := fsm.New()
	follower := &stubNode{
		leaderID:   "node-1",
		leaderAddr: raft.ServerAddress(leaderAddr),
		state:      followerState,
	}
	_, followerClient := startAPI(t, Config{
		Node:          follower,
		PeerGRPCAddrs: map[raft.ServerID]string{"node-1": leaderAddr},
	})

	ctx := context.Background()
	created, err := followerClient.CreateOrder(ctx, createOrderRequest("request-001", "order-001"))
	if err != nil {
		t.Fatalf("转发 CreateOrder() error = %v", err)
	}
	if created.GetOrder().GetId() != "order-001" {
		t.Fatalf("转发的响应 order_id = %s", created.GetOrder().GetId())
	}

	if _, err := leaderClient.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: "order-001"}); err != nil {
		t.Fatalf("Leader 上应存在转发的订单: %v", err)
	}
	if _, err := followerState.GetOrder("order-001"); !errors.Is(err, fsm.ErrOrderNotFound) {
		t.Fatalf("非 Leader 不应在本地应用命令, err = %v", err)
	}

	// 幂等记录同样由 Leader 维护：从非 Leader 重试也能拿到 replayed。
	retry, err := followerClient.CreateOrder(ctx, createOrderRequest("request-001", "order-001"))
	if err != nil {
		t.Fatalf("转发重试 CreateOrder() error = %v", err)
	}
	if !retry.GetReplayed() {
		t.Fatal("转发重试应标记 replayed")
	}

	updated, err := followerClient.ChangeOrderStatus(ctx, &orderv1.ChangeOrderStatusRequest{
		RequestId:    "request-002",
		OrderId:      "order-001",
		TargetStatus: orderv1.OrderStatus_ORDER_STATUS_PAID,
	})
	if err != nil {
		t.Fatalf("转发 ChangeOrderStatus() error = %v", err)
	}
	if updated.GetOrder().GetStatus() != orderv1.OrderStatus_ORDER_STATUS_PAID {
		t.Fatalf("转发后的 status = %s, 期望 PAID", updated.GetOrder().GetStatus())
	}
}

func TestForwardedRequestIsNotForwardedAgain(t *testing.T) {
	const token = "test-forward-token"

	// 两个都认为自己是跟随者的节点互相指向对方，第二次转发必须被拒绝。
	second := &stubNode{leaderID: "node-1", leaderAddr: "node-1", state: fsm.New()}
	secondAddr, _ := startAPI(t, Config{Node: second, ForwardToken: token})

	first := &stubNode{leaderID: "node-1", leaderAddr: "node-1", state: fsm.New()}
	_, firstClient := startAPI(t, Config{
		Node:          first,
		PeerGRPCAddrs: map[raft.ServerID]string{"node-1": secondAddr},
		ForwardToken:  token,
	})

	_, err := firstClient.CreateOrder(context.Background(), createOrderRequest("request-001", "order-001"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("二次转发的 code = %v, 期望 Unavailable", status.Code(err))
	}
	if !strings.Contains(err.Error(), "already forwarded") {
		t.Fatalf("二次转发应报告转发标记, err = %v", err)
	}
}

// 客户端伪造 x-orderraft-forwarded 不应短路转发：取值不匹配集群密钥时必须在入口剥离。
func TestForgedForwardedMetadataIsIgnored(t *testing.T) {
	const token = "cluster-shared-token"

	leader := newLeaderNode(t)
	leaderAddr, leaderClient := startAPI(t, Config{Node: leader, ForwardToken: token})

	follower := &stubNode{
		leaderID:   "node-1",
		leaderAddr: raft.ServerAddress(leaderAddr),
		state:      fsm.New(),
	}
	_, followerClient := startAPI(t, Config{
		Node:          follower,
		PeerGRPCAddrs: map[raft.ServerID]string{"node-1": leaderAddr},
		ForwardToken:  token,
	})

	forged := metadata.AppendToOutgoingContext(
		context.Background(),
		ForwardedMetadataKey,
		"wrong-token",
	)

	created, err := followerClient.CreateOrder(forged, createOrderRequest("request-001", "order-001"))
	if err != nil {
		t.Fatalf("伪造转发标记后 CreateOrder() error = %v", err)
	}
	if created.GetOrder().GetId() != "order-001" {
		t.Fatalf("转发后的 order_id = %s", created.GetOrder().GetId())
	}

	if _, err := leaderClient.GetOrder(context.Background(), &orderv1.GetOrderRequest{OrderId: "order-001"}); err != nil {
		t.Fatalf("Leader 上应存在转发的订单: %v", err)
	}
}

// IsLeader 检查通过后 leadership 丢失时，应重新解析 Leader 并转发，而不是直接返回错误。
func TestLeaderChangeFallsBackToForwarding(t *testing.T) {
	leader := newLeaderNode(t)
	leaderAddr, leaderClient := startAPI(t, Config{Node: leader})

	node := &leaderChangeNode{
		leaderID:   "node-1",
		leaderAddr: raft.ServerAddress(leaderAddr),
		state:      fsm.New(),
		applyErr:   raft.ErrNotLeader,
	}
	_, client := startAPI(t, Config{
		Node:          node,
		PeerGRPCAddrs: map[raft.ServerID]string{"node-1": leaderAddr},
		ForwardToken:  "test-forward-token",
	})

	created, err := client.CreateOrder(context.Background(), createOrderRequest("request-001", "order-001"))
	if err != nil {
		t.Fatalf("leadership 丢失后转发 CreateOrder() error = %v", err)
	}
	if created.GetOrder().GetId() != "order-001" {
		t.Fatalf("转发后的 order_id = %s", created.GetOrder().GetId())
	}

	if _, err := leaderClient.GetOrder(context.Background(), &orderv1.GetOrderRequest{OrderId: "order-001"}); err != nil {
		t.Fatalf("Leader 上应存在转发的订单: %v", err)
	}
}

// 纯参数校验失败不会占用 request_id：修正参数后可用同一 request_id 重试。
func TestInvalidRequestDoesNotPoisonRequestID(t *testing.T) {
	node := newLeaderNode(t)
	_, client := startAPI(t, Config{Node: node})

	ctx := context.Background()

	invalid := createOrderRequest("request-001", "order-001")
	invalid.AmountCents = 0
	if _, err := client.CreateOrder(ctx, invalid); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("非法参数的 code = %v, 期望 InvalidArgument", status.Code(err))
	}

	created, err := client.CreateOrder(ctx, createOrderRequest("request-001", "order-001"))
	if err != nil {
		t.Fatalf("修正参数后 CreateOrder() error = %v", err)
	}
	if created.GetReplayed() {
		t.Fatal("修正参数后的请求不应被判定为重放")
	}
	if created.GetOrder().GetVersion() != 1 {
		t.Fatalf("version = %d, 期望 1", created.GetOrder().GetVersion())
	}
}

// Close 之后不得再建立新的转发连接。
func TestClosePreventsNewConnections(t *testing.T) {
	api, err := New(Config{Node: &stubNode{state: fsm.New()}})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if err := api.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, err := api.connection("127.0.0.1:1"); err == nil {
		t.Fatal("关闭后 connection() 应返回错误")
	}
	if err := api.Close(); err != nil {
		t.Fatalf("重复 Close() error = %v", err)
	}
}

func TestMapRaftApplyError(t *testing.T) {
	if code := status.Code(mapRaftApplyError(raft.ErrNotLeader)); code != codes.Unavailable {
		t.Fatalf("ErrNotLeader code = %v, 期望 Unavailable", code)
	}
	if code := status.Code(mapRaftApplyError(fsm.ErrInvalidCommand)); code != codes.InvalidArgument {
		t.Fatalf("ErrInvalidCommand code = %v, 期望 InvalidArgument", code)
	}
	// 应用阶段失败不可重试,应映射为 Internal 而不是 Unavailable。
	if code := status.Code(mapRaftApplyError(fsm.ErrApplyFailed)); code != codes.Internal {
		t.Fatalf("ErrApplyFailed code = %v, 期望 Internal", code)
	}
	// 其他瞬态错误默认映射为 Unavailable，保留客户端重试机会。
	if code := status.Code(mapRaftApplyError(errors.New("transient"))); code != codes.Unavailable {
		t.Fatalf("默认 code = %v, 期望 Unavailable", code)
	}
}

// takeForwardedFlag 不得就地修改 gRPC 持有的入站 metadata。
func TestTakeForwardedFlagDoesNotMutateIncomingMetadata(t *testing.T) {
	api, err := New(Config{
		Node:         &stubNode{state: fsm.New()},
		ForwardToken: "cluster-secret",
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = api.Close() })

	incoming := metadata.MD{
		ForwardedMetadataKey: []string{"cluster-secret"},
	}
	ctx := metadata.NewIncomingContext(context.Background(), incoming)

	if !api.takeForwardedFlag(ctx) {
		t.Fatal("与集群密钥一致的标记应被信任")
	}
	if len(incoming.Get(ForwardedMetadataKey)) == 0 {
		t.Fatal("入站 metadata 不应被就地修改")
	}

	// 取值不匹配时不可信。
	forged := metadata.NewIncomingContext(context.Background(), metadata.MD{
		ForwardedMetadataKey: []string{"true"},
	})
	if api.takeForwardedFlag(forged) {
		t.Fatal("伪造的转发标记不应被信任")
	}
}

func TestForwardingReportsMissingLeaderConfiguration(t *testing.T) {
	ctx := context.Background()

	unknown := &stubNode{state: fsm.New()}
	_, unknownClient := startAPI(t, Config{Node: unknown})
	_, err := unknownClient.CreateOrder(ctx, createOrderRequest("request-001", "order-001"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Leader 未知的 code = %v, 期望 Unavailable", status.Code(err))
	}
	if !strings.Contains(err.Error(), "leader is currently unknown") {
		t.Fatalf("应提示 Leader 未知, err = %v", err)
	}

	noAddress := &stubNode{leaderID: "node-2", leaderAddr: "node-2", state: fsm.New()}
	_, noAddressClient := startAPI(t, Config{
		Node:          noAddress,
		PeerGRPCAddrs: map[raft.ServerID]string{"node-2": ""},
	})
	_, err = noAddressClient.CreateOrder(ctx, createOrderRequest("request-001", "order-001"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Leader 地址未配置的 code = %v, 期望 Unavailable", status.Code(err))
	}
	if !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("应提示缺少 -peers 配置, err = %v", err)
	}
}

func TestGetOrderReadsLocalFSM(t *testing.T) {
	state := fsm.New()
	if _, err := state.ApplyCommand(&orderv1.RaftCommand{
		RequestId:       "request-001",
		AppliedAtUnixMs: 1700000000000,
		SchemaVersion:   1,
		Operation: &orderv1.RaftCommand_CreateOrder{
			CreateOrder: &orderv1.CreateOrderCommand{
				OrderId:     "order-001",
				UserId:      "user-001",
				AmountCents: 1999,
				Currency:    "CNY",
			},
		},
	}); err != nil {
		t.Fatalf("ApplyCommand() error = %v", err)
	}

	node := &stubNode{leaderID: "node-2", leaderAddr: "node-2", state: state}
	_, client := startAPI(t, Config{Node: node})

	ctx := context.Background()
	found, err := client.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: "order-001"})
	if err != nil {
		t.Fatalf("非 Leader 本地读 GetOrder() error = %v", err)
	}
	if found.GetOrder().GetUserId() != "user-001" {
		t.Fatalf("user_id = %s", found.GetOrder().GetUserId())
	}

	_, err = client.GetOrder(ctx, &orderv1.GetOrderRequest{OrderId: "order-404"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("订单不存在的 code = %v, 期望 NotFound", status.Code(err))
	}
}

func createOrderRequest(requestID, orderID string) *orderv1.CreateOrderRequest {
	return &orderv1.CreateOrderRequest{
		RequestId:   requestID,
		OrderId:     orderID,
		UserId:      "user-001",
		AmountCents: 1999,
		Currency:    "CNY",
	}
}

// newLeaderNode 启动一个单节点 Raft，并把选举超时调小以加快测试。
func newLeaderNode(t *testing.T) *raftnode.Node {
	t.Helper()

	node, err := raftnode.NewSingleNode(raftnode.Config{
		LocalID:            "node-1",
		LogLevel:           "error",
		HeartbeatTimeout:   100 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}, fsm.New())
	if err != nil {
		t.Fatalf("NewSingleNode() error = %v", err)
	}
	t.Cleanup(func() {
		if err := node.Close(); err != nil {
			t.Errorf("node.Close() error = %v", err)
		}
	})

	if err := node.WaitForLeader(10 * time.Second); err != nil {
		t.Fatalf("WaitForLeader() error = %v", err)
	}
	return node
}

// startAPI 启动一个只服务单节点的 gRPC 服务，返回监听地址和客户端。
func startAPI(t *testing.T, cfg Config) (string, orderv1.OrderServiceClient) {
	t.Helper()

	api, err := New(cfg)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen() error = %v", err)
	}

	server := grpc.NewServer()
	api.Register(server)

	go func() {
		_ = server.Serve(listener)
	}()

	t.Cleanup(func() {
		server.Stop()
		if err := api.Close(); err != nil {
			t.Errorf("api.Close() error = %v", err)
		}
	})

	address := listener.Addr().String()
	return address, newClient(t, address)
}

func newClient(t *testing.T, address string) orderv1.OrderServiceClient {
	t.Helper()

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
	})
	return orderv1.NewOrderServiceClient(conn)
}
