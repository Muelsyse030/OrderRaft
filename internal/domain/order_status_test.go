package domain

import (
	"testing"

	orderv1 "example.com/OrderRaft/gen/order/v1"
)

func TestValidateStatusTransition_Legal(t *testing.T) {
	tests := []struct {
		name string
		from orderv1.OrderStatus
		to   orderv1.OrderStatus
	}{
		{
			name: "created to paid",
			from: orderv1.OrderStatus_ORDER_STATUS_CREATED,
			to:   orderv1.OrderStatus_ORDER_STATUS_PAID,
		},
		{
			name: "created to cancelled",
			from: orderv1.OrderStatus_ORDER_STATUS_CREATED,
			to:   orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
		},
		{
			name: "paid to shipped",
			from: orderv1.OrderStatus_ORDER_STATUS_PAID,
			to:   orderv1.OrderStatus_ORDER_STATUS_SHIPPED,
		},
		{
			name: "paid to refunding",
			from: orderv1.OrderStatus_ORDER_STATUS_PAID,
			to:   orderv1.OrderStatus_ORDER_STATUS_REFUNDING,
		},
		{
			name: "shipped to completed",
			from: orderv1.OrderStatus_ORDER_STATUS_SHIPPED,
			to:   orderv1.OrderStatus_ORDER_STATUS_COMPLETED,
		},
		{
			name: "refunding to refunded",
			from: orderv1.OrderStatus_ORDER_STATUS_REFUNDING,
			to:   orderv1.OrderStatus_ORDER_STATUS_REFUNDED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateStatusTransition(tt.from, tt.to); err != nil {
				t.Fatalf("expected legal transition, got error: %v", err)
			}
		})
	}
}

func TestValidateStatusTransition_Illegal(t *testing.T) {
	tests := []struct {
		name string
		from orderv1.OrderStatus
		to   orderv1.OrderStatus
	}{
		{
			name: "created to shipped",
			from: orderv1.OrderStatus_ORDER_STATUS_CREATED,
			to:   orderv1.OrderStatus_ORDER_STATUS_SHIPPED,
		},
		{
			name: "created to completed",
			from: orderv1.OrderStatus_ORDER_STATUS_CREATED,
			to:   orderv1.OrderStatus_ORDER_STATUS_COMPLETED,
		},
		{
			name: "paid to completed",
			from: orderv1.OrderStatus_ORDER_STATUS_PAID,
			to:   orderv1.OrderStatus_ORDER_STATUS_COMPLETED,
		},
		{
			name: "completed to cancelled",
			from: orderv1.OrderStatus_ORDER_STATUS_COMPLETED,
			to:   orderv1.OrderStatus_ORDER_STATUS_CANCELLED,
		},
		{
			name: "refunded to paid",
			from: orderv1.OrderStatus_ORDER_STATUS_REFUNDED,
			to:   orderv1.OrderStatus_ORDER_STATUS_PAID,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateStatusTransition(tt.from, tt.to); err == nil {
				t.Fatalf(
					"expected illegal transition error: %s -> %s",
					tt.from,
					tt.to,
				)
			}
		})
	}
}
