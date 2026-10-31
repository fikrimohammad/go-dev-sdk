package util

import (
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestX(t *testing.T) {
	var wg sync.WaitGroup
	wg.Go(func() { t.Fatal("no") })
	go func() { t.Fatalf("no") }()
	if !reflect.DeepEqual(1, 1) {
		t.Errorf("expected %v", 1)
		t.Errorf("want %v, got %v", 1, 2)
	}
	t.Run("a/b", func(t *testing.T) {})
	assert.True(t, true)
}

func helper() (n int, err error) {
	n = 1
	n++
	n++
	n++
	n++
	n++
	n++
	n++
	n++
	n++
	n++
	n++
	n++
	n++
	n++
	return
}
