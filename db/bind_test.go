package db

import (
	"reflect"
	"testing"
)

func TestBindQuery_SkipsLiteralsAndComments(t *testing.T) {
	cases := []struct {
		name  string
		spec  driverSpec
		query string
		arg   any
		want  string
		args  []any
	}{
		{
			name:  "postgres literals, identifiers, dollar quotes and comments",
			spec:  postgresDriver,
			query: `SELECT :v, 'ratio: 1', "weird:col", $tag$a:b?c$tag$ -- note: x?`,
			arg:   map[string]any{"v": 1},
			want:  `SELECT $1, 'ratio: 1', "weird:col", $tag$a:b?c$tag$ -- note: x?`,
			args:  []any{1},
		},
		{
			name:  "mysql literals and comments",
			spec:  mysqlDriver,
			query: `SELECT :v, '12:30:00' -- note: x?`,
			arg:   map[string]any{"v": 1},
			want:  `SELECT ?, '12:30:00' -- note: x?`,
			args:  []any{1},
		},
		{
			name:  "postgres cast",
			spec:  postgresDriver,
			query: `SELECT :ts::timestamptz, (:id)::bigint`,
			arg:   map[string]any{"ts": "2024-01-01", "id": 7},
			want:  `SELECT $1::timestamptz, ($2)::bigint`,
			args:  []any{"2024-01-01", 7},
		},
		{
			name:  "colons that look like placeholders inside literals and comments",
			spec:  postgresDriver,
			query: `SELECT :v, ':name', "x:y", $tag$:z$tag$ -- :w`,
			arg:   map[string]any{"v": 1},
			want:  `SELECT $1, ':name', "x:y", $tag$:z$tag$ -- :w`,
			args:  []any{1},
		},
		{
			name:  "postgres array slices and assignment",
			spec:  postgresDriver,
			query: `SELECT arr[1:2], arr[:2], arr[1:], f(a := 1) FROM t WHERE id = :id`,
			arg:   map[string]any{"id": 7},
			want:  `SELECT arr[1:2], arr[:2], arr[1:], f(a := 1) FROM t WHERE id = $1`,
			args:  []any{7},
		},
		{
			name:  "postgres nested block comment",
			spec:  postgresDriver,
			query: `SELECT :v /* outer ? /* inner : */ still ? */`,
			arg:   map[string]any{"v": 1},
			want:  `SELECT $1 /* outer ? /* inner : */ still ? */`,
			args:  []any{1},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, args, err := bindQuery(c.spec, c.query, c.arg)
			if err != nil {
				t.Fatalf("bindQuery: %v", err)
			}
			if got != c.want {
				t.Fatalf("query = %q, want %q", got, c.want)
			}
			if !reflect.DeepEqual(args, c.args) {
				t.Fatalf("args = %#v, want %#v", args, c.args)
			}
		})
	}
}

func TestBindQuery_NilArg(t *testing.T) {
	// No binding is performed: the statement is passed through untouched,
	// including native placeholders and the PostgreSQL JSONB ? operator.
	query := `SELECT $1, 'why?', data ? 'key'`
	got, args, err := bindQuery(postgresDriver, query, nil)
	if err != nil {
		t.Fatalf("bindQuery: %v", err)
	}
	if got != query {
		t.Fatalf("query = %q, want %q", got, query)
	}
	if args != nil {
		t.Fatalf("args = %v, want nil", args)
	}
}

func TestBindQuery_BindError(t *testing.T) {
	_, _, err := bindQuery(postgresDriver, `SELECT :missing`, map[string]any{"other": 1})
	if err == nil {
		t.Fatal("expected bind error")
	}
}

func TestRebind(t *testing.T) {
	cases := []struct {
		name string
		spec driverSpec
		in   string
		want string
	}{
		{"question", mysqlDriver, "a = ? AND b = ?", "a = ? AND b = ?"},
		{"dollar", postgresDriver, "a = ? AND b = ?", "a = $1 AND b = $2"},
		{"at", genericDriver("sqlserver"), "a = ? AND b = ?", "a = @p1 AND b = @p2"},
		{"named", genericDriver("oci8"), "a = ? AND b = ?", "a = :arg1 AND b = :arg2"},
	}
	for _, c := range cases {
		if got := rebind(c.in, c.spec); got != c.want {
			t.Errorf("%s: rebind = %q, want %q", c.name, got, c.want)
		}
	}
}
