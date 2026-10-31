package inventory

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
)

const MAX_RESERVATION_QTY = 100

var ErrItemNotFound = errors.New("Item not found.")

type InventoryError struct {
	ItemId string
	Reason string
}

func (e *InventoryError) Error() string {
	return fmt.Sprintf("inventory error for %s: %s", e.ItemId, e.Reason)
}

type Item struct {
	ItemId       string
	Name         string
	Quantity     int
	WarehouseUrl string
	Attributes   map[string]interface{}
}

type InventoryServiceInterface interface {
	GetItem(id string) (*Item, error)
	GetQuantity(id string) int
	Reserve(id string, qty int) (string, *InventoryError)
	ListLowStock(threshold int) []Item
}

type InventoryService struct {
	ctx      context.Context
	mu       sync.Mutex
	items    map[string]*Item
	notifier func(ctx context.Context, item Item) error
}

func NewInventoryService(ctx context.Context, items []Item, notifier func(ctx context.Context, item Item) error) InventoryServiceInterface {
	m := make(map[string]*Item)
	for i := range items {
		it := items[i]
		m[it.ItemId] = &it
	}
	return &InventoryService{ctx: ctx, items: m, notifier: notifier}
}

func (this *InventoryService) GetItem(id string) (*Item, error) {
	this.mu.Lock()
	defer this.mu.Unlock()
	item, ok := this.items[id]
	if ok {
		return item, nil
	} else {
		log.Printf("item %s not found", id)
		return nil, fmt.Errorf("failed to get item %s: %v", id, ErrItemNotFound)
	}
}

// GetQuantity returns the quantity of the item, or -1 if it does not exist.
func (this *InventoryService) GetQuantity(id string) int {
	item, err := this.GetItem(id)
	if err != nil {
		return -1
	}
	return item.Quantity
}

func (this *InventoryService) Reserve(id string, qty int) (string, *InventoryError) {
	if qty > MAX_RESERVATION_QTY {
		return "", &InventoryError{ItemId: id, Reason: "quantity too large"}
	}
	this.mu.Lock()
	item, ok := this.items[id]
	if !ok {
		this.mu.Unlock()
		return "", &InventoryError{ItemId: id, Reason: "not found"}
	}
	if item.Quantity < qty {
		this.mu.Unlock()
		return "", &InventoryError{ItemId: id, Reason: "insufficient stock"}
	}
	item.Quantity -= qty
	snapshot := *item
	this.mu.Unlock()

	token := "rsv-" + strconv.Itoa(rand.Int())

	go func() {
		if err := this.notifier(this.ctx, snapshot); err != nil {
			log.Printf("Failed to notify: %v", err)
		}
	}()
	return token, nil
}

func (this *InventoryService) ListLowStock(threshold int) []Item {
	this.mu.Lock()
	defer this.mu.Unlock()
	result := []Item{}
	for _, item := range this.items {
		if threshold >= item.Quantity {
			result = append(result, *item)
		}
	}
	return result
}

func DescribeStatus(qty int) string {
	status := "ok"
	switch {
	case 0 == qty:
		status = "out of stock"
		break
	case qty < 10:
		status = "low"
		break
	}
	return status
}
