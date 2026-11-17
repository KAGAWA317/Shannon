package config

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// ConfigFormat represents supported configuration file formats
// ConfigFormat 表示支持的配置文件格式
type ConfigFormat string

const (
	FormatJSON ConfigFormat = "json"
	FormatYAML ConfigFormat = "yaml"
)

// ChangeEvent represents a configuration change event
// ChangeEvent 表示一次配置变更事件
type ChangeEvent struct {
	File      string         `json:"file"`
	Action    string         `json:"action"` // create, modify, delete 注：create、modify、delete
	Config    map[string]any `json:"config"`
	Timestamp time.Time      `json:"timestamp"`
}

// ChangeHandler is called when configuration changes
// ChangeHandler 在配置变更时被调用
type ChangeHandler func(event ChangeEvent) error

// ConfigManager manages file-based configuration with hot-reload
// ConfigManager 管理基于文件的配置并支持热重载
type ConfigManager struct {
	configDir      string
	configs        map[string]map[string]any
	handlers       map[string][]ChangeHandler
	policyHandlers []func() error // Policy reload handlers for .rego files 注：.rego 文件的 policy 重载 handler
	watcher        *fsnotify.Watcher
	started        bool
	stopCh         chan struct{}
	logger         *zap.Logger
	mu             sync.RWMutex
	watcherMu      sync.Mutex

	// Configuration validation
	// 配置校验
	validators map[string]func(map[string]any) error

	// Polling fallback for when fsnotify isn't reliable
	// fsnotify 不可靠时的轮询兜底
	pollInterval  time.Duration
	enablePolling bool
}

// NewConfigManager creates a new configuration manager
// NewConfigManager 新建一个配置管理器
func NewConfigManager(configDir string, logger *zap.Logger) (*ConfigManager, error) {
	if configDir == "" {
		return nil, fmt.Errorf("config directory cannot be empty")
	}

	// Ensure config directory exists
	// 确保配置目录存在
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create config directory: %w", err)
	}

	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create file watcher: %w", err)
	}

	return &ConfigManager{
		configDir:     configDir,
		configs:       make(map[string]map[string]any),
		handlers:      make(map[string][]ChangeHandler),
		validators:    make(map[string]func(map[string]any) error),
		watcher:       watcher,
		stopCh:        make(chan struct{}),
		logger:        logger,
		pollInterval:  10 * time.Second, // Fallback polling interval 注：兜底轮询间隔
		enablePolling: false,            // Disabled by default 注：默认关闭
	}, nil
}

// Start begins watching for configuration changes
// Start 开始监听配置变更
func (cm *ConfigManager) Start(ctx context.Context) error {
	// Fast path: avoid holding cm.mu while doing I/O (watcher add, file loads)
	// 快路径：避免在 I/O（注册 watcher、加载文件）期间持有 cm.mu
	cm.mu.Lock()
	if cm.started {
		cm.mu.Unlock()
		return nil
	}
	cm.mu.Unlock()

	// Add config directory to watcher (no cm.mu needed)
	// 把配置目录加入 watcher（无需 cm.mu）
	if err := cm.watcher.Add(cm.configDir); err != nil {
		return fmt.Errorf("failed to watch config directory: %w", err)
	}

	// Load initial configurations outside of cm.mu to avoid deadlocks
	// 在 cm.mu 之外加载初始配置，避免死锁
	if err := cm.loadAllConfigs(); err != nil {
		return fmt.Errorf("failed to load initial configs: %w", err)
	}

	// Mark as started
	// 标记为已启动
	cm.mu.Lock()
	cm.started = true
	// Snapshot values for logging while holding the lock
	// 持锁时快照各值用于日志
	loaded := len(cm.configs)
	polling := cm.enablePolling
	cm.mu.Unlock()

	// Start file watcher goroutine
	// 启动文件 watcher goroutine
	go cm.watchLoop()

	// Start polling fallback if enabled
	// 若已启用则启动轮询兜底
	if polling {
		go cm.pollLoop()
	}

	cm.logger.Info("Configuration manager started",
		zap.String("config_dir", cm.configDir),
		zap.Int("loaded_configs", loaded),
		zap.Bool("polling_enabled", polling),
	)

	return nil
}

// Stop stops watching for configuration changes
// Stop 停止监听配置变更
func (cm *ConfigManager) Stop() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.started {
		return nil
	}

	close(cm.stopCh)
	if err := cm.watcher.Close(); err != nil {
		cm.logger.Error("Error closing file watcher", zap.Error(err))
	}

	cm.started = false
	cm.logger.Info("Configuration manager stopped")

	return nil
}

// RegisterHandler registers a change handler for a specific config file
// RegisterHandler 为指定配置文件注册变更处理函数
func (cm *ConfigManager) RegisterHandler(filename string, handler ChangeHandler) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.handlers[filename] == nil {
		cm.handlers[filename] = make([]ChangeHandler, 0)
	}
	cm.handlers[filename] = append(cm.handlers[filename], handler)

	cm.logger.Info("Configuration handler registered",
		zap.String("filename", filename),
		zap.Int("total_handlers", len(cm.handlers[filename])),
	)
}

// RegisterValidator registers a configuration validator for a specific file
// RegisterValidator 为指定文件注册配置校验器
// RegisterValidator 为指定文件注册配置校验器
func (cm *ConfigManager) RegisterValidator(filename string, validator func(map[string]any) error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.validators[filename] = validator
	cm.logger.Info("Configuration validator registered", zap.String("filename", filename))
}

// RegisterPolicyHandler registers a handler for policy file changes
// RegisterPolicyHandler 注册 policy 文件变更的处理函数
// RegisterPolicyHandler 注册 policy 文件变更的处理函数
func (cm *ConfigManager) RegisterPolicyHandler(handler func() error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.policyHandlers = append(cm.policyHandlers, handler)
	cm.logger.Info("Policy reload handler registered")
}

// GetConfig returns the current configuration for a file
// GetConfig 返回某个文件的当前配置
func (cm *ConfigManager) GetConfig(filename string) (map[string]any, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	config, exists := cm.configs[filename]
	if !exists {
		return nil, false
	}

	// Return a copy to prevent concurrent modification
	// 返回副本，防止并发修改
	result := make(map[string]any)
	maps.Copy(result, config)

	return result, true
}

// GetAllConfigs returns all loaded configurations
// GetAllConfigs 返回所有已加载的配置
func (cm *ConfigManager) GetAllConfigs() map[string]map[string]any {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	result := make(map[string]map[string]any)
	for filename, config := range cm.configs {
		configCopy := make(map[string]any)
		maps.Copy(configCopy, config)
		result[filename] = configCopy
	}

	return result
}

// ReloadConfig manually reloads a specific configuration file
// ReloadConfig 手动重载指定的配置文件
func (cm *ConfigManager) ReloadConfig(filename string) error {
	filePath := filepath.Join(cm.configDir, filename)
	return cm.loadConfigFile(filePath, "manual_reload")
}

// ReloadAllConfigs manually reloads all configuration files
// ReloadAllConfigs 手动重载所有配置文件
func (cm *ConfigManager) ReloadAllConfigs() error {
	return cm.loadAllConfigs()
}

// SetConfig programmatically sets a configuration (useful for testing)
// SetConfig 以编程方式设置配置（便于测试）
func (cm *ConfigManager) SetConfig(filename string, config map[string]any) error {
	// Get validator with minimal lock time
	// 以最短持锁时间取出 validator
	cm.mu.RLock()
	validator, hasValidator := cm.validators[filename]
	cm.mu.RUnlock()

	// Validate outside of lock
	// 在锁外做校验
	if hasValidator {
		if err := validator(config); err != nil {
			return fmt.Errorf("configuration validation failed for %s: %w", filename, err)
		}
	}

	// Create a deep copy of the config for handlers
	// 为 handler 深拷贝配置
	configCopy := make(map[string]any)
	maps.Copy(configCopy, config)

	// Update config and copy handlers under lock
	// 在锁内更新配置并复制 handler 列表
	cm.mu.Lock()
	cm.configs[filename] = config
	handlers := make([]ChangeHandler, len(cm.handlers[filename]))
	copy(handlers, cm.handlers[filename])
	cm.mu.Unlock()

	// Notify handlers asynchronously without holding any locks
	// 不持锁异步通知 handler
	if len(handlers) > 0 {
		event := ChangeEvent{
			File:      filename,
			Action:    "programmatic_set",
			Config:    configCopy, // Use the copy to prevent concurrent modification 注：传副本，避免并发修改
			Timestamp: time.Now(),
		}

		// Execute handlers asynchronously to prevent blocking
		// 异步执行 handler，避免阻塞
		for _, handler := range handlers {
			h := handler
			go func() {
				if err := h(event); err != nil {
					cm.logger.Error("Configuration handler error",
						zap.String("filename", filename),
						zap.String("action", "programmatic_set"),
						zap.Error(err),
					)
				}
			}()
		}
	}

	cm.logger.Info("Configuration set programmatically",
		zap.String("filename", filename),
		zap.Int("keys", len(config)),
	)

	return nil
}

// EnablePolling enables polling fallback for unreliable filesystems
// EnablePolling 为不可靠的文件系统启用轮询兜底
func (cm *ConfigManager) EnablePolling(interval time.Duration) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cm.enablePolling = true
	cm.pollInterval = interval

	cm.logger.Info("Configuration polling enabled", zap.Duration("interval", interval))
}

// watchLoop handles file system events
// watchLoop 处理文件系统事件
func (cm *ConfigManager) watchLoop() {
	defer func() {
		if r := recover(); r != nil {
			cm.logger.Error("Watch loop panicked", zap.Any("panic", r))
		}
	}()

	for {
		select {
		case <-cm.stopCh:
			return
		case event, ok := <-cm.watcher.Events:
			if !ok {
				return
			}
			cm.handleWatchEvent(event)
		case err, ok := <-cm.watcher.Errors:
			if !ok {
				return
			}
			cm.logger.Error("File watcher error", zap.Error(err))
		}
	}
}

// pollLoop provides polling fallback for file changes
// pollLoop 提供文件变更的轮询兜底
func (cm *ConfigManager) pollLoop() {
	ticker := time.NewTicker(cm.pollInterval)
	defer ticker.Stop()

	lastModTimes := make(map[string]time.Time)

	for {
		select {
		case <-cm.stopCh:
			return
		case <-ticker.C:
			cm.checkForChanges(lastModTimes)
		}
	}
}

// checkForChanges checks for file modifications via polling
// checkForChanges 通过轮询检查文件是否被修改
func (cm *ConfigManager) checkForChanges(lastModTimes map[string]time.Time) {
	err := filepath.WalkDir(cm.configDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !cm.isConfigFile(path) {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return err
		}

		filename := filepath.Base(path)
		lastMod := lastModTimes[filename]
		currentMod := info.ModTime()

		if currentMod.After(lastMod) {
			lastModTimes[filename] = currentMod
			cm.logger.Debug("Detected file change via polling",
				zap.String("file", filename),
				zap.Time("mod_time", currentMod),
			)
			return cm.loadConfigFile(path, "polling_detected")
		}

		return nil
	})

	if err != nil {
		cm.logger.Error("Error during polling check", zap.Error(err))
	}
}

// handleWatchEvent processes file system watch events
// handleWatchEvent 处理文件系统 watch 事件
func (cm *ConfigManager) handleWatchEvent(event fsnotify.Event) {
	cm.watcherMu.Lock()
	defer cm.watcherMu.Unlock()

	filename := filepath.Base(event.Name)

	// Process config files OR policy files
	// 处理配置文件或 policy 文件
	isConfig := cm.isConfigFile(event.Name)
	isPolicy := cm.isPolicyFile(event.Name)

	if !isConfig && !isPolicy {
		return
	}

	cm.logger.Debug("File system event",
		zap.String("file", filename),
		zap.String("op", event.Op.String()),
	)

	var action string
	switch {
	case event.Op&fsnotify.Create == fsnotify.Create:
		action = "create"
	case event.Op&fsnotify.Write == fsnotify.Write:
		action = "modify"
	case event.Op&fsnotify.Remove == fsnotify.Remove:
		action = "delete"
	case event.Op&fsnotify.Rename == fsnotify.Rename:
		action = "rename"
	case event.Op&fsnotify.Chmod == fsnotify.Chmod:
		// Usually ignore chmod events
		// 通常忽略 chmod 事件
		return
	default:
		action = event.Op.String()
	}

	if action == "delete" || action == "rename" {
		if isConfig {
			cm.handleFileRemoval(filename)
		}
		// For policy files, we still trigger a reload since policies may reference the deleted file
		// policy 文件仍需触发重载，因为策略可能引用了被删除的文件
		if isPolicy {
			cm.handlePolicyReload(filename, action)
		}
	} else {
		// Small delay to handle rapid successive writes
		// 轻微延迟，以应对密集的连续写入
		time.Sleep(50 * time.Millisecond)

		if isConfig {
			if err := cm.loadConfigFile(event.Name, action); err != nil {
				cm.logger.Error("Failed to load config file",
					zap.String("file", filename),
					zap.String("action", action),
					zap.Error(err),
				)
			}
		}

		if isPolicy {
			cm.handlePolicyReload(filename, action)
		}
	}
}

// loadAllConfigs loads all configuration files in the directory
// loadAllConfigs 加载目录下所有配置文件
func (cm *ConfigManager) loadAllConfigs() error {
	return filepath.WalkDir(cm.configDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !cm.isConfigFile(path) {
			return nil
		}

		return cm.loadConfigFile(path, "initial_load")
	})
}

// loadConfigFile loads a single configuration file
// loadConfigFile 加载单个配置文件
func (cm *ConfigManager) loadConfigFile(filePath, action string) error {
	// Perform all I/O and parsing operations before acquiring any locks
	// 在获取任何锁之前完成全部 I/O 与解析操作
	data, err := os.ReadFile(filePath)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	filename := filepath.Base(filePath)
	config := make(map[string]any)

	// Parse based on file extension (no lock needed for these pure functions)
	// 按文件扩展名解析（这些纯函数无需加锁）
	format := cm.detectFormat(filename)
	switch format {
	case FormatJSON:
		if err := json.Unmarshal(data, &config); err != nil {
			return fmt.Errorf("failed to parse JSON config %s: %w", filename, err)
		}
	case FormatYAML:
		if err := yaml.Unmarshal(data, &config); err != nil {
			return fmt.Errorf("failed to parse YAML config %s: %w", filename, err)
		}
	default:
		return fmt.Errorf("unsupported config format for %s", filename)
	}

	// Get validator with minimal lock time
	// 以最短持锁时间取出 validator
	cm.mu.RLock()
	validator := cm.validators[filename]
	cm.mu.RUnlock()

	// Validate outside of lock
	// 在锁外做校验
	if validator != nil {
		if err := validator(config); err != nil {
			return fmt.Errorf("configuration validation failed for %s: %w", filename, err)
		}
	}

	// Create a deep copy of the config for handlers to avoid sharing mutable data
	// 为 handler 深拷贝配置，避免共享可变数据
	configCopy := make(map[string]any)
	maps.Copy(configCopy, config)

	// Update configuration and get handlers in one lock operation
	// 一次加锁完成配置更新与 handler 获取
	cm.mu.Lock()
	cm.configs[filename] = config
	// Make a copy of handlers slice to avoid holding lock during handler execution
	// 复制 handler 切片，避免执行 handler 期间持锁
	handlers := make([]ChangeHandler, len(cm.handlers[filename]))
	copy(handlers, cm.handlers[filename])
	cm.mu.Unlock()

	// Notify handlers without holding any locks (prevents deadlock if handler calls back)
	// 不持锁通知 handler（防止 handler 回调时死锁）
	if len(handlers) > 0 {
		event := ChangeEvent{
			File:      filename,
			Action:    action,
			Config:    configCopy, // Use the copy to prevent concurrent modification 注：传副本，避免并发修改
			Timestamp: time.Now(),
		}

		// Execute handlers asynchronously to prevent blocking
		// 异步执行 handler，避免阻塞
		for _, handler := range handlers {
			// Copy handler to avoid closure issues
			// 复制 handler，避免闭包捕获问题
			h := handler
			go func() {
				if err := h(event); err != nil {
					cm.logger.Error("Configuration handler error",
						zap.String("filename", filename),
						zap.String("action", action),
						zap.Error(err),
					)
				}
			}()
		}
	}

	cm.logger.Info("Configuration loaded",
		zap.String("filename", filename),
		zap.String("action", action),
		zap.String("format", string(format)),
		zap.Int("keys", len(config)),
	)

	return nil
}

// handleFileRemoval handles when a config file is removed
// handleFileRemoval 处理配置文件被删除的情况
func (cm *ConfigManager) handleFileRemoval(filename string) {
	// Get config and handlers, then remove from map
	// 取出配置与 handler，然后从 map 中删除
	cm.mu.Lock()
	config := cm.configs[filename]
	delete(cm.configs, filename)
	handlers := make([]ChangeHandler, len(cm.handlers[filename]))
	copy(handlers, cm.handlers[filename])
	cm.mu.Unlock()

	// Create a copy of the last known config for handlers
	// 为 handler 复制最后已知的配置
	var configCopy map[string]any
	if config != nil {
		configCopy = make(map[string]any)
		maps.Copy(configCopy, config)
	}

	// Notify handlers asynchronously without holding locks
	// 不持锁异步通知 handler
	if len(handlers) > 0 {
		event := ChangeEvent{
			File:      filename,
			Action:    "delete",
			Config:    configCopy, // Last known config (copy) 注：最后一次已知配置（副本）
			Timestamp: time.Now(),
		}

		// Execute handlers asynchronously to prevent blocking
		// 异步执行 handler，避免阻塞
		for _, handler := range handlers {
			h := handler
			go func() {
				if err := h(event); err != nil {
					cm.logger.Error("Configuration handler error on deletion",
						zap.String("filename", filename),
						zap.Error(err),
					)
				}
			}()
		}
	}

	cm.logger.Info("Configuration file removed", zap.String("filename", filename))
}

// handlePolicyReload triggers policy engine reloads when .rego files change
// handlePolicyReload 在 .rego 文件变更时触发 policy 引擎重载
func (cm *ConfigManager) handlePolicyReload(filename, action string) {
	cm.mu.RLock()
	handlers := make([]func() error, len(cm.policyHandlers))
	copy(handlers, cm.policyHandlers)
	cm.mu.RUnlock()

	cm.logger.Info("Policy file changed, triggering reload",
		zap.String("file", filename),
		zap.String("action", action),
		zap.Int("handlers", len(handlers)),
	)

	for _, handler := range handlers {
		if err := handler(); err != nil {
			cm.logger.Error("Policy reload handler failed",
				zap.String("file", filename),
				zap.String("action", action),
				zap.Error(err),
			)
		}
	}
}

// isConfigFile checks if a file is a supported configuration file
// isConfigFile 判断文件是否为支持的配置文件
func (cm *ConfigManager) isConfigFile(filename string) bool {
	ext := filepath.Ext(filename)
	return ext == ".json" || ext == ".yaml" || ext == ".yml"
}

// isPolicyFile checks if a file is a policy file that should trigger policy reloads
// isPolicyFile 判断文件是否为应触发 policy 重载的策略文件
func (cm *ConfigManager) isPolicyFile(filename string) bool {
	ext := filepath.Ext(filename)
	return ext == ".rego"
}

// detectFormat detects the configuration file format
// detectFormat 判断配置文件格式
func (cm *ConfigManager) detectFormat(filename string) ConfigFormat {
	ext := filepath.Ext(filename)
	switch ext {
	case ".json":
		return FormatJSON
	case ".yaml", ".yml":
		return FormatYAML
	default:
		return FormatJSON // Default fallback 注：默认兜底
	}
}
