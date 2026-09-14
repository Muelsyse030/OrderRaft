package main

import (
	"context"
	"log"
	"time"

	orderv1 "example.com/OrderRaft/gen/order/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	conn, err := grpc.NewClient(
		"127.0.0.1:50052",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("连接服务端失败: %v", err)
	}
	defer conn.Close()

	client := orderv1.NewOrderServiceClient(conn)

	createResp, err := createOrder(client, "request-001")
	if err != nil {
		log.Fatalf("创建订单失败: %v", err)
	}

	log.Printf(
		"创建订单成功: code=%s message=%s replayed=%t order=%+v",
		createResp.GetCode(),
		createResp.GetMessage(),
		createResp.GetReplayed(),
		createResp.GetOrder(),
	)

	// 使用同一个 request_id 重试，验证幂等：服务端返回首次执行结果。
	replayedResp, err := createOrder(client, "request-001")
	if err != nil {
		log.Fatalf("重试创建订单失败: %v", err)
	}

	log.Printf(
		"重试创建订单: code=%s replayed=%t version=%d",
		replayedResp.GetCode(),
		replayedResp.GetReplayed(),
		replayedResp.GetOrder().GetVersion(),
	)

	changeResp, err := changeOrderStatus(client, "request-002", orderv1.OrderStatus_ORDER_STATUS_PAID)
	if err != nil {
		log.Fatalf("修改订单状态失败: %v", err)
	}

	log.Printf(
		"修改订单状态成功: code=%s message=%s order=%+v",
		changeResp.GetCode(),
		changeResp.GetMessage(),
		changeResp.GetOrder(),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	getResp, err := client.GetOrder(ctx, &orderv1.GetOrderRequest{
		OrderId: "order-001",
	})
	if err != nil {
		log.Fatalf("查询订单失败: %v", err)
	}

	log.Printf(
		"查询订单成功: code=%s message=%s order=%+v",
		getResp.GetCode(),
		getResp.GetMessage(),
		getResp.GetOrder(),
	)
}

func createOrder(client orderv1.OrderServiceClient, requestID string) (*orderv1.CreateOrderResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		RequestId:   requestID,
		OrderId:     "order-001",
		UserId:      "user-001",
		AmountCents: 1999,
		Currency:    "CNY",
	})
}

func changeOrderStatus(
	client orderv1.OrderServiceClient,
	requestID string,
	target orderv1.OrderStatus,
) (*orderv1.ChangeOrderStatusResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return client.ChangeOrderStatus(ctx, &orderv1.ChangeOrderStatusRequest{
		RequestId:    requestID,
		OrderId:      "order-001",
		TargetStatus: target,
	})
}
