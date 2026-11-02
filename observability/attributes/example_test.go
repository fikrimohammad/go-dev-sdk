package attributes_test

import (
	"fmt"

	"github.com/fikrimohammad/go-dev-sdk/observability/attributes"
)

func ExampleNormalizeKey() {
	fmt.Println(attributes.NormalizeKey("order_id"))
	fmt.Println(attributes.NormalizeKey("HTTP_METHOD"))
	fmt.Println(attributes.NormalizeKey("trace_id"))
	fmt.Println(attributes.NormalizeKey("http.response.status_code"))
	// Output:
	// order.id
	// http.method
	// trace_id
	// http.response.status_code
}

func ExampleConvertMapsToKVs() {
	kvs := attributes.ConvertMapsToKVs(
		map[string]any{"order_id": 42, "HTTP_METHOD": "GET"},
		map[string]any{"retry_count": 3, "ok": true},
	)
	for _, kv := range kvs {
		fmt.Printf("%s=%v\n", kv.Key, kv.Value.AsInterface())
	}
	// Output:
	// http.method=GET
	// ok=true
	// order.id=42
	// retry.count=3
}
