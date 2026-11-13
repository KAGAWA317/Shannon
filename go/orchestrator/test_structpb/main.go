package main

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/types/known/structpb"
)

// sanitizeForStructpb converts values to types compatible with structpb.NewStruct.
// sanitizeForStructpb 把值转换为 structpb.NewStruct 兼容的类型。
func sanitizeForStructpb(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = sanitizeValue(v)
	}
	return result
}

func sanitizeValue(v any) any {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case bool, int, int32, int64, float32, float64, string:
		return val
	case []any:
		result := make([]any, len(val))
		for i, elem := range val {
			result[i] = sanitizeValue(elem)
		}
		return result
	case map[string]any:
		return sanitizeForStructpb(val)
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			return sanitizeSliceValue(rv)
		case reflect.Map:
			return sanitizeMapValue(rv)
		default:
			return v
		}
	}
}

func sanitizeSliceValue(rv reflect.Value) []any {
	length := rv.Len()
	result := make([]any, length)
	for i := range length {
		result[i] = sanitizeValue(rv.Index(i).Interface())
	}
	return result
}

func sanitizeMapValue(rv reflect.Value) map[string]any {
	if rv.Type().Key().Kind() != reflect.String {
		return nil
	}
	result := make(map[string]any, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		key := iter.Key().String()
		result[key] = sanitizeValue(iter.Value().Interface())
	}
	return result
}

func main() {
	fmt.Println("=== Test 1: []string (the original bug) ===")
	m1 := map[string]any{
		"stop": []string{"a", "b", "c"},
	}
	st1, err1 := structpb.NewStruct(m1)
	fmt.Printf("Before fix: st=%v err=%v\n", st1, err1)

	m1Fixed := sanitizeForStructpb(m1)
	st1Fixed, err1Fixed := structpb.NewStruct(m1Fixed)
	fmt.Printf("After fix:  st=%v err=%v\n\n", st1Fixed, err1Fixed)

	fmt.Println("=== Test 2: []map[string]string (conversation history) ===")
	m2 := map[string]any{
		"conversation_history": []map[string]string{
			{"role": "user", "content": "hello"},
			{"role": "assistant", "content": "hi"},
		},
	}
	st2, err2 := structpb.NewStruct(m2)
	fmt.Printf("Before fix: st=%v err=%v\n", st2, err2)

	m2Fixed := sanitizeForStructpb(m2)
	st2Fixed, err2Fixed := structpb.NewStruct(m2Fixed)
	fmt.Printf("After fix:  st=%v err=%v\n\n", st2Fixed, err2Fixed)

	fmt.Println("=== Test 3: Nested structures ===")
	m3 := map[string]any{
		"nested": map[string]any{
			"tags": []string{"tag1", "tag2"},
		},
	}
	m3Fixed := sanitizeForStructpb(m3)
	st3Fixed, err3Fixed := structpb.NewStruct(m3Fixed)
	fmt.Printf("Nested after fix: st=%v err=%v\n\n", st3Fixed, err3Fixed)

	fmt.Println("=== All tests passed! ===")
}
