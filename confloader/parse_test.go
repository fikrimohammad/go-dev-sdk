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
		{"string", "hello", reflect.TypeOf(""), "hello"},
		{"bool true", "true", reflect.TypeOf(false), true},
		{"bool false", "false", reflect.TypeOf(false), false},
		{"int", "42", reflect.TypeOf(0), 42},
		{"int64", "42", reflect.TypeOf(int64(0)), int64(42)},
		{"uint", "7", reflect.TypeOf(uint(0)), uint(7)},
		{"float", "3.14", reflect.TypeOf(float64(0)), 3.14},
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
		{"pointer", "true", reflect.TypeOf((*bool)(nil)), func() any { b := true; return &b }()},
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
	if _, err := parseValue("notint", reflect.TypeOf(0)); !errors.Is(err, ErrParseFailed) {
		t.Fatalf("expected ErrParseFailed for bad int, got %v", err)
	}
	if _, err := parseValue("notbool", reflect.TypeOf(false)); !errors.Is(err, ErrParseFailed) {
		t.Fatalf("expected ErrParseFailed for bad bool, got %v", err)
	}
	if _, err := parseValue("garbage", reflect.TypeOf(struct{ A int }{})); !errors.Is(err, ErrParseFailed) {
		t.Fatalf("expected ErrParseFailed for bad struct, got %v", err)
	}
}
