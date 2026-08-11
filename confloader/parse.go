package confloader

import (
	"fmt"
	"reflect"

	"gopkg.in/yaml.v3"
)

// parseValue decodes a raw string fetched from a provider into a freshly
// allocated value of targetType. Supported inputs:
//   - any type implementing encoding.TextUnmarshaler (time.Duration, net.IP,
//     url.URL, ...) — decoded from the raw text directly.
//   - any type implementing json.Unmarshaler / yaml.Unmarshaler — same.
//   - strings / bools / numbers / structs / maps / slices — decoded as YAML,
//     which also accepts JSON, so JSON-tagged structs work out of the box.
//
// Returns (nil, err) on failure; the decoded value is never returned together
// with an error (Go "if err != nil, ignore other returns" contract).
func parseValue(raw string, targetType reflect.Type) (any, error) {
	ptr := reflect.New(targetType)
	if err := yaml.Unmarshal([]byte(raw), ptr.Interface()); err != nil {
		return nil, fmt.Errorf("%w: cannot decode %q into %s: %v", ErrParseFailed, truncate(raw, 64), targetType, err)
	}
	return ptr.Elem().Interface(), nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
