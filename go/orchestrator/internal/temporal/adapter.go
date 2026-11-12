package temporal

import (
	"fmt"
	"reflect"

	"go.temporal.io/sdk/log"
	"go.uber.org/zap"
)

// ZapAdapter adapts zap logger to Temporal's logger interface
// ZapAdapter 把 zap logger 适配为 Temporal 的 logger 接口
type ZapAdapter struct {
	logger *zap.Logger
}

func NewZapAdapter(logger *zap.Logger) log.Logger {
	return &ZapAdapter{logger: logger}
}

func (z *ZapAdapter) Debug(msg string, keyvals ...any) {
	z.logger.Debug(msg, z.fieldsFromKeyvals(keyvals)...)
}

func (z *ZapAdapter) Info(msg string, keyvals ...any) {
	z.logger.Info(msg, z.fieldsFromKeyvals(keyvals)...)
}

func (z *ZapAdapter) Warn(msg string, keyvals ...any) {
	z.logger.Warn(msg, z.fieldsFromKeyvals(keyvals)...)
}

func (z *ZapAdapter) Error(msg string, keyvals ...any) {
	z.logger.Error(msg, z.fieldsFromKeyvals(keyvals)...)
}

func (z *ZapAdapter) fieldsFromKeyvals(keyvals []any) []zap.Field {
	fields := make([]zap.Field, 0, len(keyvals)/2)
	for i := 0; i < len(keyvals); i += 2 {
		if i+1 < len(keyvals) {
			key, ok := keyvals[i].(string)
			if ok {
				fields = append(fields, safeZapField(key, keyvals[i+1]))
			}
		}
	}
	return fields
}

// safeZapField creates a zap field, handling types that zap.Any() can't serialize
// safeZapField 创建 zap 字段，处理 zap.Any() 无法序列化的类型
func safeZapField(key string, val any) (field zap.Field) {
	// Recover from any panic during field creation
	// 从字段创建期间可能出现的 panic 中恢复
	defer func() {
		if r := recover(); r != nil {
			field = zap.String(key, fmt.Sprintf("<unserializable: %v>", r))
		}
	}()

	if val == nil {
		return zap.String(key, "<nil>")
	}

	// Check for types that can cause zap.Any() to panic
	// 检查可能导致 zap.Any() panic 的类型
	rv := reflect.ValueOf(val)
	switch rv.Kind() {
	case reflect.Func:
		return zap.String(key, "<func>")
	case reflect.Chan:
		return zap.String(key, "<chan>")
	case reflect.UnsafePointer:
		return zap.String(key, "<unsafe.Pointer>")
	case reflect.Invalid:
		return zap.String(key, "<invalid>")
	default:
		return zap.Any(key, val)
	}
}

// With returns a new logger with additional fields - required for Temporal SDK compatibility
// With 返回带附加字段的新 logger——Temporal SDK 兼容性所需
func (z *ZapAdapter) With(keyvals ...any) log.Logger {
	return &ZapAdapter{logger: z.logger.With(z.fieldsFromKeyvals(keyvals)...)}
}
