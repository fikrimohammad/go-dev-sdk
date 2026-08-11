package confloader

import (
	"context"
	"fmt"
	"reflect"
	"strings"
)

// getterMeta is the resolved metadata for a single Getter[T] field, derived
// from its `conf` struct tag.
type getterMeta struct {
	name   string // struct field name (used as the cache key)
	typ    reflect.Type
	folder string
	key    string
}

// parseConfTag parses a `conf:"folder=...,key=..."` tag into its components.
func parseConfTag(tag string) (folder, key string, err error) {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return "", "", ErrInvalidTag
	}
	for _, pair := range strings.Split(tag, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		k, v, found := strings.Cut(pair, "=")
		if !found {
			return "", "", fmt.Errorf("%w: %q", ErrInvalidTag, pair)
		}
		k = strings.TrimSpace(k)
		v = strings.TrimSpace(v)
		switch k {
		case "folder":
			folder = v
		case "key":
			key = v
		default:
			return "", "", fmt.Errorf("%w: unknown option %q", ErrInvalidTag, k)
		}
	}
	if key == "" {
		return "", "", ErrTagMissingKey
	}
	if folder == "" {
		return "", "", ErrTagMissingFolder
	}
	return folder, key, nil
}

// reflectGetters walks struct type T and returns the metadata for every
// Getter[T_i] field, validating the presence and shape of the conf tag.
//
// A Getter[T_i] is recognised by its Get method (func(context.Context) (T_i, error)),
// not by an interface, because Getter[T] is a generic struct type.
func reflectGetters(t reflect.Type) ([]getterMeta, error) {
	var metas []getterMeta
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		ft := field.Type

		get, ok := ft.MethodByName("Get")
		if !ok || !isGetterSignature(get) {
			continue
		}
		// T_i is the first return type of Get.
		out0 := get.Type.Out(0)

		tag, ok := field.Tag.Lookup("conf")
		if !ok {
			return nil, fmt.Errorf("%w: field %q", ErrMissingTag, field.Name)
		}
		folder, key, perr := parseConfTag(tag)
		if perr != nil {
			return nil, fmt.Errorf("confloader: field %q: %w", field.Name, perr)
		}

		metas = append(metas, getterMeta{
			name:   field.Name,
			typ:    out0, // the value type T_i
			folder: folder,
			key:    key,
		})
	}
	return metas, nil
}

// isGetterSignature reports whether m is `Get(context.Context) (T, error)`.
func isGetterSignature(m reflect.Method) bool {
	if m.Type.NumIn() != 2 || m.Type.NumOut() != 2 { // 2 == receiver + ctx
		return false
	}
	if m.Type.In(1) != ctxType {
		return false
	}
	out1 := m.Type.Out(1)
	return out1 == errType
}

// errType is the reflect type of the built-in error interface, used to
// validate Getter method signatures.
var errType = reflect.TypeOf((*error)(nil)).Elem()

// ctxType is the reflect type of context.Context, used to validate the Get
// method's first parameter.
var ctxType = reflect.TypeOf((*context.Context)(nil)).Elem()
