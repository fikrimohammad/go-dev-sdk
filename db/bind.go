package db

import (
	"database/sql/driver"
	"errors"
	"reflect"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

// Sentinel bytes temporarily stand in for punctuation that sqlx.Named would
// otherwise misinterpret. They are control characters that are not part of
// SQL syntax, so collisions with query text are practically impossible.
const (
	// castSentinel stands in for PostgreSQL's "::" cast operator. sqlx's named
	// parser drops the first colon of a "::" that does not directly follow a
	// :name (and rejects one that does), so protecting casts keeps
	// :param::type and (:param)::type working.
	castSentinel = "\x00"
	// colonSentinel stands in for ':' inside literals, quoted identifiers,
	// comments, and dollar-quoted strings so text like '12:30:00' or
	// 'ratio: 1' is not parsed as a named placeholder.
	colonSentinel = "\x01"
)

// bindQuery binds the named query end-to-end and returns the query with the
// driver's placeholder syntax plus the positional arguments.
//
// sqlx-sensitive punctuation (casts, literal colons, and stray colons) is
// protected first. Binding then happens in two steps:
//
//  1. sqlx.Named maps :name placeholders to struct/map values, producing a
//     query with '?' placeholders.
//  2. expandSlices expands IN (?) slices, and rebind rewrites the remaining
//     '?' placeholders to the driver syntax ($1 for PostgreSQL, @p1 for SQL
//     Server, '?' for MySQL).
//
// Keeping the intermediate query in '?' form matters for PostgreSQL: sqlx's
// BindNamed emits $1 directly for pgx, which sqlx.In cannot expand. A nil arg
// skips binding entirely and leaves the query unchanged, which is valid for
// queries without placeholders.
func bindQuery(d driverSpec, query string, arg any) (string, []any, error) {
	if arg == nil {
		// No binding is requested: leave the statement exactly as written so
		// driver-native syntax (including PostgreSQL's JSONB ? operator) is
		// never rewritten.
		return query, nil, nil
	}

	bound, args, err := sqlx.Named(protectStrayColons(protectColons(protectCasts(query), d.dialect)), arg)
	if err != nil {
		return "", nil, err
	}
	bound = strings.ReplaceAll(bound, castSentinel, "::")
	bound = strings.ReplaceAll(bound, colonSentinel, ":")

	bound, args, err = expandSlices(bound, args, d.dialect)
	if err != nil {
		return "", nil, err
	}

	return rebind(bound, d), args, nil
}

// protectCasts replaces PostgreSQL "::" casts with castSentinel so sqlx's
// named parser cannot mangle them. bindQuery restores the original text right
// after binding.
func protectCasts(query string) string {
	return strings.ReplaceAll(query, "::", castSentinel)
}

// protectStrayColons replaces ':' with colonSentinel wherever it cannot start
// a named placeholder, i.e. when the next byte is not an identifier-starting
// character. This keeps PostgreSQL constructs such as array slices (arr[1:2],
// arr[:2], arr[1:]) and ':=' out of sqlx's named binder. bindQuery restores the
// original text after binding.
func protectStrayColons(query string) string {
	var b strings.Builder
	b.Grow(len(query))
	for i := 0; i < len(query); i++ {
		if query[i] == ':' && (i+1 == len(query) || !isNameStart(query[i+1])) {
			b.WriteString(colonSentinel)
			continue
		}
		b.WriteByte(query[i])
	}
	return b.String()
}

func isNameStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// protectColons replaces ':' inside literals, quoted identifiers, comments,
// and dollar-quoted strings with colonSentinel so strings such as '12:30:00'
// or 'ratio: 1' are not mistaken for :name placeholders. bindQuery restores
// the original text right after binding.
func protectColons(query string, d dialect) string {
	var b strings.Builder
	b.Grow(len(query))
	for i := 0; i < len(query); {
		end := literalEnd(query, i, d)
		if end > i {
			b.WriteString(strings.ReplaceAll(query[i:end], ":", colonSentinel))
			i = end
			continue
		}
		b.WriteByte(query[i])
		i++
	}
	return b.String()
}

// literalEnd returns the index just past the literal, quoted identifier,
// comment, or dollar-quoted string starting at i, or i when no such construct
// starts there.
func literalEnd(query string, i int, d dialect) int {
	switch c := query[i]; {
	case c == '\'':
		return skipQuoted(query, i, '\'', d == dialectMySQL || isEscapeString(query, i, d))
	case c == '"':
		return skipQuoted(query, i, '"', d == dialectMySQL)
	case c == '`' && d == dialectMySQL:
		return skipQuoted(query, i, '`', false)
	case c == '#' && d == dialectMySQL:
		return skipLineComment(query, i+1)
	case c == '-' && i+1 < len(query) && query[i+1] == '-' && isLineComment(query, i, d):
		return skipLineComment(query, i+2)
	case c == '/' && i+1 < len(query) && query[i+1] == '*':
		return skipBlockComment(query, i+2, d == dialectPostgres)
	case c == '$' && d == dialectPostgres:
		if end, ok := skipDollarQuoted(query, i); ok {
			return end
		}
	}
	return i
}

// expandSlices expands slice arguments used with IN (?) into one placeholder
// per element, returning the rewritten query and flattened args. Placeholders
// are located with a literal-aware scanner instead of a plain [strings.Index], so
// '?' characters inside literals, quoted identifiers, and comments are left
// alone. When no argument is a slice the query is returned unchanged.
func expandSlices(query string, args []any, d dialect) (string, []any, error) {
	for i, arg := range args {
		if v, ok := arg.(driver.Valuer); ok {
			value, err := v.Value()
			if err != nil {
				return "", nil, err
			}
			args[i] = value
		}
	}

	expanded := false
	for _, arg := range args {
		if v, ok := sliceValue(arg); ok {
			if v.Len() == 0 {
				return "", nil, errors.New("db: empty slice passed to IN expansion")
			}
			expanded = true
		}
	}
	if !expanded {
		return query, args, nil
	}

	placeholders := placeholderIndexes(query, d)
	if len(placeholders) < len(args) {
		return "", nil, errors.New("db: number of bind vars less than arguments")
	}
	if len(placeholders) > len(args) {
		return "", nil, errors.New("db: number of bind vars exceeds arguments")
	}

	var b strings.Builder
	b.Grow(len(query) + len(args)*2)
	flat := make([]any, 0, len(args))

	prev := 0
	for i, idx := range placeholders {
		b.WriteString(query[prev:idx])
		if v, ok := sliceValue(args[i]); ok {
			for j := 0; j < v.Len(); j++ {
				if j > 0 {
					b.WriteString(", ")
				}
				b.WriteByte('?')
				flat = append(flat, v.Index(j).Interface())
			}
		} else {
			b.WriteByte('?')
			flat = append(flat, args[i])
		}
		prev = idx + 1
	}
	b.WriteString(query[prev:])

	return b.String(), flat, nil
}

// sliceValue reports whether arg is a slice eligible for IN expansion. []byte
// is a [driver.Value] and is never expanded.
func sliceValue(arg any) (reflect.Value, bool) {
	if arg == nil {
		return reflect.Value{}, false
	}
	v := reflect.ValueOf(arg)
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}, false
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Slice {
		return reflect.Value{}, false
	}
	if v.Type() == reflect.TypeFor[[]byte]() {
		return reflect.Value{}, false
	}
	return v, true
}

// rebind rewrites every positional '?' placeholder to the syntax required by
// d. Question-mark drivers are returned untouched.
func rebind(query string, d driverSpec) string {
	if d.bindType == bindQuestion {
		return query
	}

	placeholders := placeholderIndexes(query, d.dialect)
	if len(placeholders) == 0 {
		return query
	}

	var b strings.Builder
	b.Grow(len(query) + len(placeholders)*3)
	prev := 0
	for i, idx := range placeholders {
		b.WriteString(query[prev:idx])
		switch d.bindType {
		case bindDollar:
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(i + 1))
		case bindAt:
			b.WriteString("@p")
			b.WriteString(strconv.Itoa(i + 1))
		case bindNamed:
			b.WriteString(":arg")
			b.WriteString(strconv.Itoa(i + 1))
		default:
			b.WriteByte('?')
		}
		prev = idx + 1
	}
	b.WriteString(query[prev:])
	return b.String()
}

// placeholderIndexes returns the byte offsets of every '?' that is a
// placeholder rather than part of a string literal, quoted identifier,
// comment, or PostgreSQL dollar-quoted string.
//
// The scanner understands the quoting rules of both built-in dialects:
// doubled single and double quotes are escapes, MySQL string literals also
// honor backslash escapes, PostgreSQL E-prefixed string literals honor
// backslash escapes, and PostgreSQL dollar-quoted bodies ($$...$$,
// $tag$...$tag$) and nested block comments are skipped wholesale.
func placeholderIndexes(query string, d dialect) []int {
	indexes := make([]int, 0, strings.Count(query, "?"))
	for i := 0; i < len(query); {
		c := query[i]
		switch {
		case c == '\'':
			i = skipQuoted(query, i, '\'', d == dialectMySQL || isEscapeString(query, i, d))
		case c == '"':
			i = skipQuoted(query, i, '"', d == dialectMySQL)
		case c == '`' && d == dialectMySQL:
			i = skipQuoted(query, i, '`', false)
		case c == '#' && d == dialectMySQL:
			i = skipLineComment(query, i+1)
		case c == '-' && i+1 < len(query) && query[i+1] == '-' && isLineComment(query, i, d):
			i = skipLineComment(query, i+2)
		case c == '/' && i+1 < len(query) && query[i+1] == '*':
			i = skipBlockComment(query, i+2, d == dialectPostgres)
		case c == '$' && d == dialectPostgres:
			if end, ok := skipDollarQuoted(query, i); ok {
				i = end
				continue
			}
			i++
		case c == '?':
			indexes = append(indexes, i)
			i++
		default:
			i++
		}
	}
	return indexes
}

// skipQuoted returns the index just past the quoted run starting at start,
// where query[start] is the opening quote. A doubled quote is an escaped quote;
// backslash escapes are honored when backslash is true.
func skipQuoted(query string, start int, quote byte, backslash bool) int {
	for i := start + 1; i < len(query); {
		switch {
		case backslash && query[i] == '\\':
			i += 2
		case query[i] == quote:
			if i+1 < len(query) && query[i+1] == quote {
				i += 2
				continue
			}
			return i + 1
		default:
			i++
		}
	}
	return len(query)
}

// skipLineComment returns the index just past the next newline (or the end of
// the query) starting at start.
func skipLineComment(query string, start int) int {
	if i := strings.IndexByte(query[start:], '\n'); i >= 0 {
		return start + i + 1
	}
	return len(query)
}

// skipBlockComment returns the index just past the closing */ starting at
// start. PostgreSQL block comments nest; MySQL's do not.
func skipBlockComment(query string, start int, nested bool) int {
	depth := 1
	for i := start; i < len(query); {
		switch {
		case i+1 < len(query) && query[i] == '*' && query[i+1] == '/':
			depth--
			i += 2
			if depth == 0 {
				return i
			}
		case nested && i+1 < len(query) && query[i] == '/' && query[i+1] == '*':
			depth++
			i += 2
		default:
			i++
		}
	}
	return len(query)
}

// skipDollarQuoted returns the index just past a PostgreSQL dollar-quoted
// string starting at start, where query[start] is '$'. It reports false when
// the '$' does not open a dollar-quoted string (for example, a $1 parameter or
// a lone dollar sign).
func skipDollarQuoted(query string, start int) (int, bool) {
	// '$' is allowed inside PostgreSQL identifiers, so only treat it as a
	// dollar-quote opener at a token boundary.
	if start > 0 && isIdentChar(query[start-1]) {
		return 0, false
	}
	j := start + 1
	for j < len(query) && isDollarTagChar(query[j]) {
		j++
	}
	if j >= len(query) || query[j] != '$' {
		return 0, false
	}
	tag := query[start+1 : j]
	// A dollar-quote tag follows identifier rules and cannot start with a
	// digit, which keeps $1 bind parameters out of this branch.
	if tag != "" && tag[0] >= '0' && tag[0] <= '9' {
		return 0, false
	}

	delim := query[start : j+1]
	if k := strings.Index(query[j+1:], delim); k >= 0 {
		return j + 1 + k + len(delim), true
	}
	return len(query), true
}

func isDollarTagChar(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// isEscapeString reports whether the single quote at i opens a PostgreSQL
// E-prefixed string literal, in which a backslash escapes a quote.
func isEscapeString(query string, i int, d dialect) bool {
	if d != dialectPostgres || i == 0 {
		return false
	}
	if query[i-1] != 'E' && query[i-1] != 'e' {
		return false
	}
	if i == 1 {
		return true
	}
	return !isIdentChar(query[i-2])
}

func isIdentChar(c byte) bool {
	return c == '_' || c == '$' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// isLineComment reports whether "--" at i starts a comment. PostgreSQL always
// treats it as a comment; MySQL requires whitespace or end of input after it.
func isLineComment(query string, i int, d dialect) bool {
	if d != dialectMySQL {
		return true
	}
	return i+2 >= len(query) || isSpaceByte(query[i+2])
}

func isSpaceByte(c byte) bool {
	switch c {
	case ' ', '\t', '\n', '\r', '\f', '\v':
		return true
	default:
		return false
	}
}
