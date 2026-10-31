package inventory

import (
	"context"
	"reflect"
	"sort"
	"sync"
	"testing"
)

func newTestService() InventoryServiceInterface {
	return NewInventoryService(context.Background(), []Item{
		{ItemId: "a", Name: "Apple", Quantity: 5},
		{ItemId: "b", Name: "Banana", Quantity: 50},
	}, func(ctx context.Context, item Item) error { return nil })
}

func TestGetItem(t *testing.T) {
	s := newTestService()
	item, err := s.GetItem("a")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if item.Name != "Apple" {
		t.Errorf("expected Apple, got %s", item.Name)
	}
	if _, err := s.GetItem("zzz"); err == nil {
		t.Errorf("expected error for missing item")
	}
}

func TestGetQuantity(t *testing.T) {
	s := newTestService()
	if q := s.GetQuantity("zzz"); q != -1 {
		t.Errorf("expected -1, got %d", q)
	}
}

func TestReserve(t *testing.T) {
	s := newTestService()
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := s.Reserve("a", 1); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	wg.Wait()
	if _, err := s.Reserve("a", 1); err == nil {
		t.Errorf("expected insufficient stock")
	}
}

func TestListLowStock(t *testing.T) {
	s := newTestService()
	got := s.ListLowStock(10)
	sort.Slice(got, func(i, j int) bool { return got[i].ItemId < got[j].ItemId })
	want := []Item{{ItemId: "a", Name: "Apple", Quantity: 5}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("expected %v, got %v", want, got)
	}
}

func TestDescribeStatus(t *testing.T) {
	cases := []struct {
		qty  int
		want string
	}{{0, "out of stock"}, {3, "low"}, {30, "ok"}}
	for i, c := range cases {
		if got := DescribeStatus(c.qty); got != c.want {
			t.Errorf("case %d failed", i)
		}
	}
}
