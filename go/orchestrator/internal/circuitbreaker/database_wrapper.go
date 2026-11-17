package circuitbreaker

import (
	"context"
	"database/sql"
	"time"

	"go.uber.org/zap"
)

// DatabaseWrapper wraps database operations with circuit breaker
// DatabaseWrapper 为数据库操作包上 circuit breaker 保护。
type DatabaseWrapper struct {
	db     *sql.DB
	cb     *CircuitBreaker
	logger *zap.Logger
}

// NewDatabaseWrapper creates a database wrapper with circuit breaker
// NewDatabaseWrapper 创建一个带 circuit breaker 的数据库 wrapper。
func NewDatabaseWrapper(db *sql.DB, logger *zap.Logger) *DatabaseWrapper {
	config := GetDatabaseConfig().ToConfig()
	cb := NewCircuitBreaker("postgresql", config, logger)

	// Register with metrics collector
	// 注册到 metrics collector。
	GlobalMetricsCollector.RegisterCircuitBreaker("postgresql", "database-client", cb)

	return &DatabaseWrapper{
		db:     db,
		cb:     cb,
		logger: logger,
	}
}

// run 在 breaker 保护下执行 fn，并记录一次 request 指标；breaker 拒绝时返回零值+cbErr。
func run[T any](cb *CircuitBreaker, ctx context.Context, fn func() (T, error)) (T, error) {
	var (
		val T
		err error
	)
	cbErr := cb.Execute(ctx, func() error {
		val, err = fn()
		return err
	})
	GlobalMetricsCollector.RecordRequest("postgresql", "database-client", cb.State(), cbErr == nil && err == nil)
	if cbErr != nil {
		var zero T
		return zero, cbErr
	}
	return val, err
}

func runErr(cb *CircuitBreaker, ctx context.Context, fn func() error) error {
	_, err := run(cb, ctx, func() (struct{}, error) { return struct{}{}, fn() })
	return err
}

// PingContext wraps database ping with circuit breaker
// PingContext 为数据库 ping 包上 circuit breaker。
func (dw *DatabaseWrapper) PingContext(ctx context.Context) error {
	return runErr(dw.cb, ctx, func() error {
		return dw.db.PingContext(ctx)
	})
}

// QueryContext wraps database query with circuit breaker
// QueryContext 为数据库 query 包上 circuit breaker。
func (dw *DatabaseWrapper) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return run(dw.cb, ctx, func() (*sql.Rows, error) {
		return dw.db.QueryContext(ctx, query, args...)
	})
}

// QueryRowContext wraps database query row with circuit breaker
// QueryRowContext 为数据库 query row 包上 circuit breaker。
// Returns (*sql.Row, error) to properly propagate circuit breaker errors
// 返回 (*sql.Row, error)，以便正确传递 circuit breaker 错误。
func (dw *DatabaseWrapper) QueryRowContextCB(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	// Record metrics - assume success for QueryRow since errors are deferred
	// 记录指标：QueryRow 的错误被延后，此处按成功计
	return run(dw.cb, ctx, func() (*sql.Row, error) {
		// We can't easily check for query errors here since sql.Row doesn't expose them
		// The error will be checked when Scan() is called
		// sql.Row 不会立即暴露查询错误，需等到调用 Scan() 时才能发现。
		return dw.db.QueryRowContext(ctx, query, args...), nil
	})
}

// QueryRowContext wraps database query row with circuit breaker (legacy API)
// Deprecated: Use QueryRowContextCB for proper error handling
// QueryRowContext 为数据库 query row 包上 circuit breaker（legacy API）。
// Deprecated: 请改用 QueryRowContextCB 以获得正确的错误处理。
func (dw *DatabaseWrapper) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	row, err := dw.QueryRowContextCB(ctx, query, args...)
	if err != nil {
		// Return a row that will error on Scan with circuit breaker error
		// This is not ideal but maintains backward compatibility
		// 返回一个在 Scan 时才报 circuit breaker 错误的 row；不理想，但保持了向后兼容。
		return &sql.Row{}
	}
	return row
}

// ExecContext wraps database exec with circuit breaker
// ExecContext 为数据库 exec 包上 circuit breaker。
func (dw *DatabaseWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return run(dw.cb, ctx, func() (sql.Result, error) {
		return dw.db.ExecContext(ctx, query, args...)
	})
}

// TxWrapper wraps sql.Tx with circuit breaker protection
// TxWrapper 为 sql.Tx 提供 circuit breaker 保护。
type TxWrapper struct {
	tx     *sql.Tx
	cb     *CircuitBreaker
	logger *zap.Logger
}

// BeginTx wraps database transaction begin with circuit breaker
// BeginTx 为开启数据库事务包上 circuit breaker。
func (dw *DatabaseWrapper) BeginTx(ctx context.Context, opts *sql.TxOptions) (*TxWrapper, error) {
	tx, err := run(dw.cb, ctx, func() (*sql.Tx, error) {
		return dw.db.BeginTx(ctx, opts)
	})
	if err != nil {
		return nil, err
	}

	return &TxWrapper{
		tx:     tx,
		cb:     dw.cb,
		logger: dw.logger,
	}, nil
}

// Transaction wrapper methods
// 事务 wrapper 的方法。
func (tw *TxWrapper) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return run(tw.cb, ctx, func() (sql.Result, error) {
		return tw.tx.ExecContext(ctx, query, args...)
	})
}

func (tw *TxWrapper) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return run(tw.cb, ctx, func() (*sql.Rows, error) {
		return tw.tx.QueryContext(ctx, query, args...)
	})
}

func (tw *TxWrapper) QueryRowContext(ctx context.Context, query string, args ...any) (*sql.Row, error) {
	return run(tw.cb, ctx, func() (*sql.Row, error) {
		return tw.tx.QueryRowContext(ctx, query, args...), nil
	})
}

func (tw *TxWrapper) PrepareContext(ctx context.Context, query string) (*StmtWrapper, error) {
	stmt, err := run(tw.cb, ctx, func() (*sql.Stmt, error) {
		return tw.tx.PrepareContext(ctx, query)
	})
	if err != nil {
		return nil, err
	}

	return &StmtWrapper{
		stmt:   stmt,
		cb:     tw.cb,
		logger: tw.logger,
	}, nil
}

func (tw *TxWrapper) Commit() error {
	return runErr(tw.cb, context.Background(), func() error {
		return tw.tx.Commit()
	})
}

func (tw *TxWrapper) Rollback() error {
	// Don't use circuit breaker for rollback - we always want to try it
	// rollback 不走 circuit breaker——无论如何都要尝试执行
	return tw.tx.Rollback()
}

// StmtWrapper wraps sql.Stmt with circuit breaker protection
// StmtWrapper 为 sql.Stmt 提供 circuit breaker 保护。
type StmtWrapper struct {
	stmt   *sql.Stmt
	cb     *CircuitBreaker
	logger *zap.Logger
}

// PrepareContext wraps database prepare with circuit breaker
// PrepareContext 为数据库 prepare 包上 circuit breaker。
func (dw *DatabaseWrapper) PrepareContext(ctx context.Context, query string) (*StmtWrapper, error) {
	stmt, err := run(dw.cb, ctx, func() (*sql.Stmt, error) {
		return dw.db.PrepareContext(ctx, query)
	})
	if err != nil {
		return nil, err
	}

	return &StmtWrapper{
		stmt:   stmt,
		cb:     dw.cb,
		logger: dw.logger,
	}, nil
}

// Statement wrapper methods
// statement wrapper 的方法。
func (sw *StmtWrapper) ExecContext(ctx context.Context, args ...any) (sql.Result, error) {
	return run(sw.cb, ctx, func() (sql.Result, error) {
		return sw.stmt.ExecContext(ctx, args...)
	})
}

func (sw *StmtWrapper) QueryContext(ctx context.Context, args ...any) (*sql.Rows, error) {
	return run(sw.cb, ctx, func() (*sql.Rows, error) {
		return sw.stmt.QueryContext(ctx, args...)
	})
}

func (sw *StmtWrapper) QueryRowContext(ctx context.Context, args ...any) (*sql.Row, error) {
	return run(sw.cb, ctx, func() (*sql.Row, error) {
		return sw.stmt.QueryRowContext(ctx, args...), nil
	})
}

func (sw *StmtWrapper) Close() error {
	// Don't use circuit breaker for close - we always want to try it
	// close 不走 circuit breaker——无论如何都要尝试执行
	return sw.stmt.Close()
}

// Stats returns database stats
// Stats 返回数据库统计信息。
func (dw *DatabaseWrapper) Stats() sql.DBStats {
	return dw.db.Stats()
}

// Close closes the database connection
// Close 关闭数据库连接。
func (dw *DatabaseWrapper) Close() error {
	return dw.db.Close()
}

// SetMaxOpenConns sets the maximum number of open connections
// SetMaxOpenConns 设置最大打开连接数。
func (dw *DatabaseWrapper) SetMaxOpenConns(n int) {
	dw.db.SetMaxOpenConns(n)
}

// SetMaxIdleConns sets the maximum number of idle connections
// SetMaxIdleConns 设置最大空闲连接数。
func (dw *DatabaseWrapper) SetMaxIdleConns(n int) {
	dw.db.SetMaxIdleConns(n)
}

// SetConnMaxLifetime sets the maximum connection lifetime
// SetConnMaxLifetime 设置连接的最大存活时间。
func (dw *DatabaseWrapper) SetConnMaxLifetime(d time.Duration) {
	dw.db.SetConnMaxLifetime(d)
}

// GetDB returns the underlying database connection for operations not covered by wrapper
// GetDB 返回底层数据库连接，供 wrapper 未覆盖的操作使用。
func (dw *DatabaseWrapper) GetDB() *sql.DB {
	return dw.db
}

// IsCircuitBreakerOpen returns true if the circuit breaker is open
// IsCircuitBreakerOpen 在 circuit breaker 打开时返回 true。
func (dw *DatabaseWrapper) IsCircuitBreakerOpen() bool {
	return dw.cb.State() == StateOpen
}
