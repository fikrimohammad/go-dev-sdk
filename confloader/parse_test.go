package confloader

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseValue(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		typ  reflect.Type
		want any
	}{
		{"string", "hello", reflect.TypeFor[string](), "hello"},
		{"bool true", "true", reflect.TypeFor[bool](), true},
		{"bool false", "false", reflect.TypeFor[bool](), false},
		{"int", "42", reflect.TypeFor[int](), 42},
		{"int64", "42", reflect.TypeFor[int64](), int64(42)},
		{"uint", "7", reflect.TypeFor[uint](), uint(7)},
		{"float", "3.14", reflect.TypeFor[float64](), 3.14},
		{"json struct", `{"host":"h","port":9}`, reflect.TypeOf(struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		}{}), struct {
			Host string `json:"host"`
			Port int    `json:"port"`
		}{Host: "h", Port: 9}},
		{"yaml struct", "host: h\nport: 9", reflect.TypeOf(struct {
			Host string `yaml:"host"`
			Port int    `yaml:"port"`
		}{}), struct {
			Host string `yaml:"host"`
			Port int    `yaml:"port"`
		}{Host: "h", Port: 9}},
		{"pointer", "true", reflect.TypeFor[*bool](), func() any { b := true; return &b }()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseValue(tc.raw, tc.typ)
			if err != nil {
				t.Fatalf("parseValue: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestParseValueErrors(t *testing.T) {
	if _, err := parseValue("notint", reflect.TypeFor[int]()); !errors.Is(err, ErrParseFailed) {
		t.Fatalf("expected ErrParseFailed for bad int, got %v", err)
	}
	if _, err := parseValue("notbool", reflect.TypeFor[bool]()); !errors.Is(err, ErrParseFailed) {
		t.Fatalf("expected ErrParseFailed for bad bool, got %v", err)
	}
	if _, err := parseValue("garbage", reflect.TypeOf(struct{ A int }{})); !errors.Is(err, ErrParseFailed) {
		t.Fatalf("expected ErrParseFailed for bad struct, got %v", err)
	}
}
