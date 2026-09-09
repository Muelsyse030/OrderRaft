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

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	createResp, err := client.CreateOrder(ctx, &orderv1.CreateOrderRequest{
		RequestId:   "request-001",
		OrderId:     "order-001",
		UserId:      "user-001",
		AmountCents: 1999,
		Currency:    "CNY",
	})
	if err != nil {
		log.Fatalf("创建订单失败: %v", err)
	}

	log.Printf(
		"创建订单成功: code=%s message=%s order=%+v",
		createResp.GetCode(),
		createResp.GetMessage(),
		createResp.GetOrder(),
	)

	changeResp, err := client.ChangeOrderStatus(
		ctx,
		&orderv1.ChangeOrderStatusRequest{
			RequestId:    "request-002",
			OrderId:      "order-001",
			TargetStatus: orderv1.OrderStatus_ORDER_STATUS_PAID,
		},
	)
	if err != nil {
		log.Fatalf("修改订单状态失败: %v", err)
	}

	log.Printf(
		"修改订单状态成功: code=%s message=%s order=%+v",
		changeResp.GetCode(),
		changeResp.GetMessage(),
		changeResp.GetOrder(),
	)

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
