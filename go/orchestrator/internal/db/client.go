package db

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"go.uber.org/zap"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/circuitbreaker"
)

// Config holds database configuration
// Config 保存数据库配置
type Config struct {
	Host            string
	Port            int
	User            string
	Password        string
	Database        string
	MaxConnections  int
	IdleConnections int
	MaxLifetime     time.Duration
	SSLMode         string
}

// Client manages database connections and operations
// Client 管理数据库连接与操作
type Client struct {
	db     *circuitbreaker.DatabaseWrapper
	logger *zap.Logger
	config *Config

	// Write queue for async operations
	// 异步写入队列
	writeQueue chan WriteRequest
	workers    int
	stopCh     chan struct{}
	workerWg   sync.WaitGroup // Track worker goroutines for graceful shutdown 注：跟踪 worker goroutine 以支持优雅关闭
}

// WriteRequest represents an async write operation
// WriteRequest 表示一次异步写操作
type WriteRequest struct {
	Type     WriteType
	Data     any
	Callback func(error)
}

type WriteType int

const (
	WriteTypeTaskExecution WriteType = iota
	WriteTypeAgentExecution
	WriteTypeToolExecution
	WriteTypeSessionArchive
	WriteTypeAuditLog
	WriteTypeBatch
)

// String returns the string representation of WriteType
// String 返回 WriteType 的字符串表示
func (wt WriteType) String() string {
	switch wt {
	case WriteTypeTaskExecution:
		return "TaskExecution"
	case WriteTypeAgentExecution:
		return "AgentExecution"
	case WriteTypeToolExecution:
		return "ToolExecution"
	case WriteTypeSessionArchive:
		return "SessionArchive"
	case WriteTypeAuditLog:
		return "AuditLog"
	case WriteTypeBatch:
		return "Batch"
	default:
		return "Unknown"
	}
}

// NewClient creates a new database client with connection pool
// NewClient 创建带连接池的数据库客户端
func NewClient(config *Config, logger *zap.Logger) (*Client, error) {
	if config.MaxConnections == 0 {
		config.MaxConnections = 25
	}
	if config.IdleConnections == 0 {
		config.IdleConnections = 5
	}
	if config.MaxLifetime == 0 {
		config.MaxLifetime = 5 * time.Minute
	}
	if config.SSLMode == "" {
		config.SSLMode = "require"
	}

	// Build connection string
	// 拼接连接字符串
	dsn := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.Database, config.SSLMode,
	)

	// Open database connection
	// 打开数据库连接
	rawDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Configure connection pool
	// 配置连接池
	rawDB.SetMaxOpenConns(config.MaxConnections)
	rawDB.SetMaxIdleConns(config.IdleConnections)
	rawDB.SetConnMaxLifetime(config.MaxLifetime)

	// Create circuit breaker wrapped database
	// 创建带熔断器的数据库封装
	db := circuitbreaker.NewDatabaseWrapper(rawDB, logger)

	// Test connection
	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		rawDB.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	client := &Client{
		db:         db,
		logger:     logger,
		config:     config,
		writeQueue: make(chan WriteRequest, 1000), // Buffer size of 1000 注：缓冲区大小 1000
		workers:    10,                            // Default 10 workers 注：默认 10 个 worker
		stopCh:     make(chan struct{}),
	}

	// Start async workers
	// 启动异步 worker
	client.startWorkers()

	// Start health check routine
	// 启动健康检查例程
	go client.healthCheck()

	logger.Info("Database client initialized",
		zap.String("host", config.Host),
		zap.Int("max_connections", config.MaxConnections),
		zap.Int("workers", client.workers),
	)

	return client, nil
}

// startWorkers initializes the worker pool for async writes
// startWorkers 初始化异步写入的 worker 池
func (c *Client) startWorkers() {
	for i := 0; i < c.workers; i++ {
		c.workerWg.Add(1)
		go c.writeWorker(i)
	}
}

// writeWorker processes write requests from the queue
// writeWorker 处理队列中的写 request
func (c *Client) writeWorker(id int) {
	c.logger.Debug("Write worker started", zap.Int("worker_id", id))

	// Batch buffer for batch writes
	// 批量写入的缓冲
	batchBuffer := make([]WriteRequest, 0, 100)
	batchTicker := time.NewTicker(1 * time.Second)
	defer batchTicker.Stop()

	for {
		select {
		case <-c.stopCh:
			// Drain remaining requests
			// 排空剩余 request
			c.drainQueue(batchBuffer)
			c.logger.Info("Write worker stopped", zap.Int("worker_id", id))
			c.workerWg.Done()
			return

		case req := <-c.writeQueue:
			// Handle different write types
			// 处理不同的写入类型
			switch req.Type {
			case WriteTypeBatch:
				batchBuffer = append(batchBuffer, req)
				if len(batchBuffer) >= 100 {
					c.processBatch(batchBuffer)
					batchBuffer = batchBuffer[:0]
				}
			default:
				// Process immediately
				// 立即处理
				c.processWrite(req)
			}

		case <-batchTicker.C:
			// Flush batch buffer periodically
			// 定期刷出批量缓冲
			if len(batchBuffer) > 0 {
				c.processBatch(batchBuffer)
				batchBuffer = batchBuffer[:0]
			}
		}
	}
}

// processWrite handles a single write request
// processWrite 处理单个写 request
func (c *Client) processWrite(req WriteRequest) {
	var err error

	switch req.Type {
	case WriteTypeTaskExecution:
		if task, ok := req.Data.(*TaskExecution); ok {
			err = c.SaveTaskExecution(context.Background(), task)
		}
	case WriteTypeAgentExecution:
		if agent, ok := req.Data.(*AgentExecution); ok {
			err = c.SaveAgentExecution(context.Background(), agent)
		}
	case WriteTypeToolExecution:
		if tool, ok := req.Data.(*ToolExecution); ok {
			err = c.SaveToolExecution(context.Background(), tool)
		}
	case WriteTypeSessionArchive:
		if session, ok := req.Data.(*SessionArchive); ok {
			err = c.SaveSessionArchive(context.Background(), session)
		}
	case WriteTypeAuditLog:
		if audit, ok := req.Data.(*AuditLog); ok {
			err = c.SaveAuditLog(context.Background(), audit)
		}
	}

	// Call callback if provided
	// 如提供了回调则调用
	if req.Callback != nil {
		req.Callback(err)
	}

	if err != nil {
		c.logger.Error("Failed to process write request",
			zap.Int("type", int(req.Type)),
			zap.Error(err),
		)
	}
}

// processBatch handles batch writes
// processBatch 处理批量写入
func (c *Client) processBatch(batch []WriteRequest) {
	if len(batch) == 0 {
		return
	}

	c.logger.Debug("Processing batch writes", zap.Int("count", len(batch)))

	// Group by type for efficient batch inserts
	// 按类型分组以便高效批量插入
	taskExecutions := make([]*TaskExecution, 0)
	agentExecutions := make([]*AgentExecution, 0)
	toolExecutions := make([]*ToolExecution, 0)

	for _, req := range batch {
		switch req.Type {
		case WriteTypeTaskExecution:
			if task, ok := req.Data.(*TaskExecution); ok {
				taskExecutions = append(taskExecutions, task)
			}
		case WriteTypeAgentExecution:
			if agent, ok := req.Data.(*AgentExecution); ok {
				agentExecutions = append(agentExecutions, agent)
			}
		case WriteTypeToolExecution:
			if tool, ok := req.Data.(*ToolExecution); ok {
				toolExecutions = append(toolExecutions, tool)
			}
		case WriteTypeBatch:
			// WriteTypeBatch should contain a slice of inner requests
			// WriteTypeBatch 应包含一组内层 request
			if innerReqs, ok := req.Data.([]WriteRequest); ok {
				// Recursively process the inner requests
				// 递归处理内层 request
				for _, innerReq := range innerReqs {
					switch innerReq.Type {
					case WriteTypeTaskExecution:
						if task, ok := innerReq.Data.(*TaskExecution); ok {
							taskExecutions = append(taskExecutions, task)
						}
					case WriteTypeAgentExecution:
						if agent, ok := innerReq.Data.(*AgentExecution); ok {
							agentExecutions = append(agentExecutions, agent)
						}
					case WriteTypeToolExecution:
						if tool, ok := innerReq.Data.(*ToolExecution); ok {
							toolExecutions = append(toolExecutions, tool)
						}
					}
				}
			}
		}
	}

	// Batch insert each type
	// 按类型批量插入
	ctx := context.Background()

	if len(taskExecutions) > 0 {
		if err := c.BatchSaveTaskExecutions(ctx, taskExecutions); err != nil {
			c.logger.Error("Failed to batch save task executions", zap.Error(err))
		}
	}

	if len(agentExecutions) > 0 {
		if err := c.BatchSaveAgentExecutions(ctx, agentExecutions); err != nil {
			c.logger.Error("Failed to batch save agent executions", zap.Error(err))
		}
	}

	if len(toolExecutions) > 0 {
		if err := c.BatchSaveToolExecutions(ctx, toolExecutions); err != nil {
			c.logger.Error("Failed to batch save tool executions", zap.Error(err))
		}
	}
}

// drainQueue processes remaining requests during shutdown
// drainQueue 在关闭期间处理剩余 request
func (c *Client) drainQueue(batchBuffer []WriteRequest) {
	timeout := time.After(10 * time.Second)

	for {
		select {
		case req := <-c.writeQueue:
			c.processWrite(req)
		case <-timeout:
			c.logger.Warn("Timeout draining write queue")
			return
		default:
			// Queue is empty
			// 队列为空
			if len(batchBuffer) > 0 {
				c.processBatch(batchBuffer)
			}
			return
		}
	}
}

// QueueWrite adds a write request to the async queue
// QueueWrite 把写 request 加入异步队列
func (c *Client) QueueWrite(writeType WriteType, data any, callback func(error)) error {
	select {
	case c.writeQueue <- WriteRequest{
		Type:     writeType,
		Data:     data,
		Callback: callback,
	}:
		return nil
	default:
		// Queue is full - use synchronous fallback to avoid dropping writes
		// 队列已满——改用同步写入，避免丢失
		c.logger.Warn("Write queue is full, falling back to synchronous write",
			zap.String("type", writeType.String()))

		req := WriteRequest{
			Type:     writeType,
			Data:     data,
			Callback: callback,
		}

		// Execute synchronously
		// 同步执行
		c.processWrite(req)
		return nil
	}
}

// QueueWriteWithRetry attempts to queue a write with limited retries before fallback
// QueueWriteWithRetry 在回退前有限次 retry 入队
func (c *Client) QueueWriteWithRetry(writeType WriteType, data any, callback func(error)) error {
	const maxRetries = 3
	const retryDelay = 10 * time.Millisecond

	for attempt := range maxRetries {
		select {
		case c.writeQueue <- WriteRequest{
			Type:     writeType,
			Data:     data,
			Callback: callback,
		}:
			return nil
		default:
			if attempt < maxRetries-1 {
				time.Sleep(retryDelay)
				continue
			}
			// Final attempt failed, fall back to sync
			// 最后一次尝试失败，回退为同步写入
			c.logger.Warn("Write queue full after retries, using synchronous fallback",
				zap.String("type", writeType.String()),
				zap.Int("attempts", maxRetries))

			req := WriteRequest{
				Type:     writeType,
				Data:     data,
				Callback: callback,
			}
			c.processWrite(req)
			return nil
		}
	}
	return nil
}

// healthCheck periodically checks database connectivity
// healthCheck 定期检查数据库连通性
func (c *Client) healthCheck() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := c.db.PingContext(ctx); err != nil {
				c.logger.Error("Database health check failed", zap.Error(err))
			}
			cancel()
		}
	}
}

// Close gracefully shuts down the database client
// Close 优雅关闭数据库客户端
func (c *Client) Close() error {
	c.logger.Info("Shutting down database client")

	// Signal workers to stop
	// 通知 worker 停止
	close(c.stopCh)

	// Wait for all workers to finish draining
	// 等待所有 worker 排空
	c.logger.Info("Waiting for write workers to finish")
	c.workerWg.Wait()

	// Close database connection
	// 关闭数据库连接
	if err := c.db.Close(); err != nil {
		return fmt.Errorf("failed to close database: %w", err)
	}

	c.logger.Info("Database client closed")
	return nil
}

// GetDB returns the underlying database connection for direct queries
// GetDB 返回底层数据库连接，供直接查询使用
func (c *Client) GetDB() *sql.DB {
	return c.db.GetDB()
}

// Transaction helper for transactional operations using circuit breaker protected transaction
// 事务辅助函数，使用受 circuit breaker 保护的事务
func (c *Client) WithTransactionCB(ctx context.Context, fn func(*circuitbreaker.TxWrapper) error) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p)
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed: %v, original error: %w", rbErr, err)
		}
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	return nil
}

// Transaction helper for transactional operations (legacy API, bypasses circuit breaker)
// Deprecated: Use WithTransactionCB for circuit breaker protection
// 事务辅助函数（旧接口，绕过 circuit breaker）
// Deprecated: 使用 WithTransactionCB 以获得 circuit breaker 保护
func (c *Client) WithTransaction(ctx context.Context, fn func(*sql.Tx) error) error {
	rawTx, err := c.db.GetDB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			rawTx.Rollback()
			panic(p)
		}
	}()

	if err := fn(rawTx); err != nil {
		if rbErr := rawTx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed: %v, original error: %w", rbErr, err)
		}
		return err
	}

	if err := rawTx.Commit(); err != nil {
		return fmt.Errorf("commit failed: %w", err)
	}

	return nil
}

// Wrapper returns the underlying DatabaseWrapper for health checks and monitoring
// Wrapper 返回底层 DatabaseWrapper，用于健康检查与监控
func (c *Client) Wrapper() *circuitbreaker.DatabaseWrapper {
	return c.db
}
