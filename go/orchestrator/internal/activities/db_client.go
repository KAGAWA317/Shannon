package activities

import (
	"sync"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/db"
)

var (
	globalDBClient *db.Client
	dbClientMutex  sync.RWMutex
)

// SetGlobalDBClient sets the global database client for use by activities
// This should be called once during application initialization
// SetGlobalDBClient 设置供 activities 使用的全局数据库 client；
// 应在应用初始化阶段调用一次。
func SetGlobalDBClient(client *db.Client) {
	dbClientMutex.Lock()
	defer dbClientMutex.Unlock()
	globalDBClient = client
}

// GetGlobalDBClient returns the global database client
// Returns nil if not initialized
// GetGlobalDBClient 返回全局数据库 client；
// 未初始化时返回 nil。
func GetGlobalDBClient() *db.Client {
	dbClientMutex.RLock()
	defer dbClientMutex.RUnlock()
	return globalDBClient
}
