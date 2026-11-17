package circuitbreaker

import (
	"os"
	"strconv"
	"time"
)

// CircuitBreakerConfig represents configuration for a circuit breaker
// CircuitBreakerConfig 表示 circuit breaker 的配置
type CircuitBreakerConfig struct {
	MaxRequests      uint32
	Interval         time.Duration
	Timeout          time.Duration
	FailureThreshold uint32
	SuccessThreshold uint32
}

// GetRedisConfig returns Redis circuit breaker configuration from environment variables
// GetRedisConfig 从环境变量读取 Redis circuit breaker 配置
func GetRedisConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxRequests:      getEnvUint32("CB_REDIS_MAX_REQUESTS", 5),
		Interval:         getEnvDuration("CB_REDIS_INTERVAL", 30*time.Second),
		Timeout:          getEnvDuration("CB_REDIS_TIMEOUT", 15*time.Second),
		FailureThreshold: getEnvUint32("CB_REDIS_FAILURE_THRESHOLD", 3),
		SuccessThreshold: getEnvUint32("CB_REDIS_SUCCESS_THRESHOLD", 2),
	}
}

// GetDatabaseConfig returns PostgreSQL circuit breaker configuration from environment variables
// GetDatabaseConfig 从环境变量读取 PostgreSQL circuit breaker 配置
func GetDatabaseConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxRequests:      getEnvUint32("CB_DB_MAX_REQUESTS", 3),
		Interval:         getEnvDuration("CB_DB_INTERVAL", 60*time.Second),
		Timeout:          getEnvDuration("CB_DB_TIMEOUT", 30*time.Second),
		FailureThreshold: getEnvUint32("CB_DB_FAILURE_THRESHOLD", 5),
		SuccessThreshold: getEnvUint32("CB_DB_SUCCESS_THRESHOLD", 2),
	}
}

// GetGRPCConfig returns gRPC circuit breaker configuration from environment variables
// GetGRPCConfig 从环境变量读取 gRPC circuit breaker 配置
func GetGRPCConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxRequests:      getEnvUint32("CB_GRPC_MAX_REQUESTS", 5),
		Interval:         getEnvDuration("CB_GRPC_INTERVAL", 45*time.Second),
		Timeout:          getEnvDuration("CB_GRPC_TIMEOUT", 20*time.Second),
		FailureThreshold: getEnvUint32("CB_GRPC_FAILURE_THRESHOLD", 3),
		SuccessThreshold: getEnvUint32("CB_GRPC_SUCCESS_THRESHOLD", 2),
	}
}

// GetGRPCConnectionConfig returns gRPC connection circuit breaker configuration from environment variables
// GetGRPCConnectionConfig 从环境变量读取 gRPC 连接级 circuit breaker 配置
func GetGRPCConnectionConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxRequests:      getEnvUint32("CB_GRPC_CONN_MAX_REQUESTS", 3),
		Interval:         getEnvDuration("CB_GRPC_CONN_INTERVAL", 60*time.Second),
		Timeout:          getEnvDuration("CB_GRPC_CONN_TIMEOUT", 30*time.Second),
		FailureThreshold: getEnvUint32("CB_GRPC_CONN_FAILURE_THRESHOLD", 3),
		SuccessThreshold: getEnvUint32("CB_GRPC_CONN_SUCCESS_THRESHOLD", 1),
	}
}

// GetHTTPConfig returns HTTP circuit breaker configuration from environment variables
// GetHTTPConfig 从环境变量读取 HTTP circuit breaker 配置
func GetHTTPConfig() CircuitBreakerConfig {
	return CircuitBreakerConfig{
		MaxRequests:      getEnvUint32("CB_HTTP_MAX_REQUESTS", 5),
		Interval:         getEnvDuration("CB_HTTP_INTERVAL", 30*time.Second),
		Timeout:          getEnvDuration("CB_HTTP_TIMEOUT", 15*time.Second),
		FailureThreshold: getEnvUint32("CB_HTTP_FAILURE_THRESHOLD", 3),
		SuccessThreshold: getEnvUint32("CB_HTTP_SUCCESS_THRESHOLD", 2),
	}
}

// ToConfig converts CircuitBreakerConfig to circuit breaker Config
// ToConfig 将 CircuitBreakerConfig 转换为 circuit breaker 的 Config
func (cbc CircuitBreakerConfig) ToConfig() Config {
	return Config{
		MaxRequests:      cbc.MaxRequests,
		Interval:         cbc.Interval,
		Timeout:          cbc.Timeout,
		FailureThreshold: cbc.FailureThreshold,
		SuccessThreshold: cbc.SuccessThreshold,
		OnStateChange:    nil, // Will be set by wrapper 注：将由 wrapper 设置
	}
}

// Helper functions for environment variable parsing
// 解析环境变量的辅助函数

func getEnvUint32(key string, defaultValue uint32) uint32 {
	if val := os.Getenv(key); val != "" {
		if parsed, err := strconv.ParseUint(val, 10, 32); err == nil {
			return uint32(parsed)
		}
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if val := os.Getenv(key); val != "" {
		if parsed, err := time.ParseDuration(val); err == nil {
			return parsed
		}
	}
	return defaultValue
}
