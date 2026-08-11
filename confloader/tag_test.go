package confloader

import (
	"errors"
	"reflect"
	"testing"
)

func TestParseConfTag(t *testing.T) {
	cases := []struct {
		name    string
		tag     string
		wantF   string
		wantK   string
		wantErr error
	}{
		{"ok", "folder=settings,key=debug", "settings", "debug", nil},
		{"reordered", "key=debug,folder=settings", "settings", "debug", nil},
		{"empty", "", "", "", ErrInvalidTag},
		{"no key", "folder=settings", "", "", ErrTagMissingKey},
		{"no folder", "key=debug", "", "", ErrTagMissingFolder},
		{"unknown opt", "foo=bar", "", "", ErrInvalidTag},
		{"malformed", "folder", "", "", ErrInvalidTag},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, k, err := parseConfTag(tc.tag)
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("expected %v, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if f != tc.wantF || k != tc.wantK {
				t.Fatalf("got (%q,%q), want (%q,%q)", f, k, tc.wantF, tc.wantK)
			}
		})
	}
}

func TestReflectGetters(t *testing.T) {
	type DBConfig struct {
		Host string
	}

	type good struct {
		DB  Getter[DBConfig] `conf:"folder=default,key=db"`
		Flg Getter[bool]     `conf:"folder=settings,key=flag"`
	}

	metas, err := reflectGetters(reflect.TypeOf(good{}))
	if err != nil {
		t.Fatalf("reflectGetters: %v", err)
	}
	if len(metas) != 2 {
		t.Fatalf("expected 2 getters, got %d", len(metas))
	}
	byName := map[string]getterMeta{}
	for _, m := range metas {
		byName[m.name] = m
	}
	if byName["DB"].folder != "default" || byName["DB"].key != "db" {
		t.Fatalf("DB meta wrong: %+v", byName["DB"])
	}
	if byName["Flg"].folder != "settings" || byName["Flg"].key != "flag" {
		t.Fatalf("Flg meta wrong: %+v", byName["Flg"])
	}

	// Non-Getter func field must be ignored.
	type withFunc struct {
		DB Getter[DBConfig] `conf:"folder=default,key=db"`
		F  func() string
	}
	metas, err = reflectGetters(reflect.TypeOf(withFunc{}))
	if err != nil {
		t.Fatalf("reflectGetters withFunc: %v", err)
	}
	if len(metas) != 1 {
		t.Fatalf("expected 1 getter (func ignored), got %d", len(metas))
	}

	// Getter without conf tag -> error.
	type noTag struct {
		DB Getter[DBConfig]
	}
	if _, err := reflectGetters(reflect.TypeOf(noTag{})); !errors.Is(err, ErrMissingTag) {
		t.Fatalf("expected ErrMissingTag, got %v", err)
	}

	// A field whose type is func() (T, string, error) is not a Getter[T], so
	// reflectGetters ignores it rather than erroring.
	type wrongSig struct {
		DB func() (DBConfig, string, error) `conf:"folder=default,key=db"`
	}
	metas, err = reflectGetters(reflect.TypeOf(wrongSig{}))
	if err != nil {
		t.Fatalf("reflectGetters wrongSig: %v", err)
	}
	if len(metas) != 0 {
		t.Fatalf("expected 0 getters for non-Getter signature, got %d", len(metas))
	}
}
