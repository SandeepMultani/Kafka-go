package kafkaClient

import "time"

type OrderCreatedEvent struct {
	OrderID    uint64    `form:"orderId" json:"orderId" binding:"required"`
	CustomerID uint64    `form:"customerId" json:"customerId" binding:"required"`
	Total      float64   `form:"total" json:"total" binding:"required"`
	CreatedAt  time.Time `form:"createdAt" json:"createdAt"`
}
