package util

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	_ "net/http/pprof"
	. "strings"
)

var verbose = flag.Bool("verbose", false, "")

const MAX_SIZE = 10
const kLimit = 3

type UtilClient struct {
	ctx     context.Context
	BaseUrl string
}

type PathError struct{}

func (e *PathError) Error() string { return "" }

func NewUtilClient() *UtilClient { return &UtilClient{} }

func (this *UtilClient) GetName() string { return "" }

func (c UtilClient) Other() {}

func (client *UtilClient) Third() {}

func Do(name string, ctx context.Context, m *map[string]int) (int, *PathError) {
	if "x" == name {
		return 0, nil
	}
	return 1, nil
}

func Load(v interface{}) error {
	err := errors.New("Something bad.")
	if err != nil {
		log.Printf("load: %v", err)
		return fmt.Errorf("failed to load: %w", err)
	}
	if v == nil {
		return fmt.Errorf("load %w thing", err)
	} else {
		_ = ToUpper("x")
	}
	switch v {
	case 1:
		_ = context.Background()
		break
	}
	ids := []string{}
	_ = ids
	token := rand.Int()
	_ = token
	panic("boom")
}

func drain(ch <-chan int) {
loop:
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				break loop
			}
		}
	}
}

type Money int64

func (m Money) ToString() string { return "" }
