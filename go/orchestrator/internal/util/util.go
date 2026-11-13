package util

import (
	"slices"
	"strconv"
	"strings"
)

// ContainsString reports whether slice contains item.
func ContainsString(slice []string, item string) bool {
	return slices.Contains(slice, item)
}

// ParseNumericValue attempts to extract a numeric value from a free‑form string.
// Preference order: direct parse, "equals|is N" pattern, then last numeric token.
// ParseNumericValue 尝试从自由格式字符串中提取数值。
// 优先顺序：直接解析、"equals|is N" 模式，最后取末尾的数字 token。
func ParseNumericValue(response string) (float64, bool) {
	response = strings.TrimSpace(response)
	if val, err := strconv.ParseFloat(response, 64); err == nil {
		return val, true
	}
	fields := strings.Fields(response)
	var numbers []float64
	for i := range fields {
		token := strings.Trim(fields[i], ".,!?:;")
		if v, err := strconv.ParseFloat(token, 64); err == nil {
			numbers = append(numbers, v)
		}
		if (strings.EqualFold(token, "equals") || strings.EqualFold(token, "is")) && i+1 < len(fields) {
			next := strings.Trim(fields[i+1], ".,!?:;")
			if v, err := strconv.ParseFloat(next, 64); err == nil {
				return v, true
			}
		}
	}
	if len(numbers) > 0 {
		return numbers[len(numbers)-1], true
	}
	return 0, false
}

// TruncateString truncates s to maxLen and appends "..." if truncated (UTF-8 safe).
// If preserveWords is true, truncates at the last space before maxLen when possible.
// TruncateString 把 s 截断到 maxLen，截断时追加 "..."（UTF-8 安全）。
// preserveWords 为 true 时尽可能在 maxLen 之前的最后一个空格处截断。
func TruncateString(s string, maxLen int, preserveWords bool) string {
	if maxLen <= 0 {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= maxLen {
		return s
	}
	// Reserve space for ellipsis
	if maxLen <= 3 {
		return "..."[:maxLen]
	}
	cut := maxLen - 3
	if preserveWords {
		// Find last space before cut (in rune positions)
		// 在截断点之前查找最后一个空格（按 rune 位置）
		if idx := lastSpaceBeforeRune(s, cut); idx > 0 {
			cut = idx
		}
	}
	return string(runes[:cut]) + "..."
}

// lastSpaceBeforeRune finds the last space before pos (in rune count, UTF-8 safe)
// lastSpaceBeforeRune 查找 pos 之前的最后一个空格（按 rune 计数，UTF-8 安全）
func lastSpaceBeforeRune(s string, pos int) int {
	runes := []rune(s)
	if pos > len(runes) {
		pos = len(runes)
	}
	for i := pos - 1; i >= 0; i-- {
		if runes[i] == ' ' || runes[i] == '\t' || runes[i] == '\n' {
			return i
		}
	}
	return -1
}

// GetContextBool extracts a boolean value from a context map, handling both bool and string "true"/"false".
// This is needed because proto map<string, string> converts booleans to strings.
// GetContextBool 从 context map 中提取布尔值，同时处理 bool 与字符串 "true"/"false"。
// 这是因为 proto 的 map<string, string> 会把布尔值转成字符串。
func GetContextBool(ctx map[string]any, key string) bool {
	if ctx == nil {
		return false
	}
	v, ok := ctx[key]
	if !ok {
		return false
	}
	// Handle bool type
	if b, ok := v.(bool); ok {
		return b
	}
	// Handle string type ("true", "false", "1", "0")
	// 处理字符串类型（"true"、"false"、"1"、"0"）
	if s, ok := v.(string); ok {
		s = strings.ToLower(strings.TrimSpace(s))
		return s == "true" || s == "1"
	}
	return false
}

// GetContextInt extracts an integer value from a context map with a default fallback.
// Handles int, float64 (JSON numbers), and string representations.
// GetContextInt 从 context map 中提取整数，带默认值兜底。
// 处理 int、float64（JSON 数字）与字符串表示。
func GetContextInt(ctx map[string]any, key string, defaultVal int) int {
	if ctx == nil {
		return defaultVal
	}
	v, ok := ctx[key]
	if !ok {
		return defaultVal
	}
	switch val := v.(type) {
	case int:
		return val
	case int64:
		return int(val)
	case float64:
		return int(val)
	case string:
		if i, err := strconv.Atoi(strings.TrimSpace(val)); err == nil {
			return i
		}
	}
	return defaultVal
}
