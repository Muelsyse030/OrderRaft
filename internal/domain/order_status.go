package domain

import (
	"fmt"

	orderv1 "example.com/OrderRaft/gen/order/v1"
)

func ValidateStatusTransition(from orderv1.OrderStatus, to orderv1.OrderStatus) error {
	switch from {
	case orderv1.OrderStatus_ORDER_STATUS_CREATED:
		if to == orderv1.OrderStatus_ORDER_STATUS_PAID ||
			to == orderv1.OrderStatus_ORDER_STATUS_CANCELLED {
			return nil
		}
	case orderv1.OrderStatus_ORDER_STATUS_PAID:
		if to == orderv1.OrderStatus_ORDER_STATUS_SHIPPED ||
			to == orderv1.OrderStatus_ORDER_STATUS_REFUNDING {
			return nil
		}
	case orderv1.OrderStatus_ORDER_STATUS_SHIPPED:
		if to == orderv1.OrderStatus_ORDER_STATUS_COMPLETED {
			return nil
		}
	case orderv1.OrderStatus_ORDER_STATUS_REFUNDING:
		if to == orderv1.OrderStatus_ORDER_STATUS_REFUNDED {
			return nil
		}
	}
	return fmt.Errorf("invalid order status transition %s -> %s ", from.String(), to.String())
}
