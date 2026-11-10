use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tracing::{debug, info, warn};
use wasmtime::*;
use wasmtime_wasi::pipe::{MemoryInputPipe, MemoryOutputPipe};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtxBuilder};

use crate::config::Config;
use crate::metrics::{TOOL_DURATION, TOOL_EXECUTIONS};

/// WASI-enabled sandbox with proper isolation
/// 具备完整隔离能力的 WASI sandbox
#[derive(Clone)]
pub struct WasiSandbox {
    engine: Arc<Engine>,
    allowed_paths: Vec<PathBuf>,
    allow_env_access: bool,
    env_vars: HashMap<String, String>,
    memory_limit: usize,
    fuel_limit: u64,
    execution_timeout: Duration,
    table_elements_limit: usize,
    instances_limit: usize,
    tables_limit: usize,
    memories_limit: usize,
    /// Session workspace path for read-write access (mounted at /workspace)
    /// 用于读写访问的 session 工作区路径（挂载到 /workspace）
    session_workspace: Option<PathBuf>,
    /// User memory workspace path for persistent read-write access (mounted at /memory)
    /// 用于持久化读写访问的用户记忆工作区路径（挂载到 /memory）
    memory_workspace: Option<PathBuf>,
}

impl WasiSandbox {
    pub fn new() -> Result<Self> {
        let app_config = Config::global().unwrap_or_default();
        Self::with_config(&app_config)
    }

    pub fn with_config(app_config: &Config) -> Result<Self> {
        // Create wasmtime engine with security-focused configuration
        // 创建面向安全的 wasmtime 引擎配置
        let mut wasm_config = wasmtime::Config::new();

        // Enable necessary features for WASI
        // 启用 WASI 所需特性
        wasm_config.wasm_reference_types(true);
        wasm_config.wasm_bulk_memory(true);
        wasm_config.consume_fuel(true);

        // Security settings - enable epoch interruption for timeouts
        // 安全设置：启用 epoch 中断以实现 timeout 控制
        wasm_config.epoch_interruption(true);
        wasm_config.memory_guard_size(64 * 1024 * 1024); // 64MB guard
        wasm_config.parallel_compilation(false); // Reduce resource usage

        let engine = Arc::new(Engine::new(&wasm_config)?);

        Ok(Self {
            engine,
            allowed_paths: app_config
                .wasi
                .allowed_paths
                .iter()
                .map(PathBuf::from)
                .collect(),
            allow_env_access: false,
            env_vars: HashMap::new(),
            memory_limit: app_config.wasi.memory_limit_bytes,
            fuel_limit: app_config.wasi.max_fuel,
            execution_timeout: app_config.wasi_timeout(),
            // Python WASM requires larger table limits (5413+ elements)
            // Python WASM 需要更大的 table 上限（5413+ 个元素）
            table_elements_limit: 10000, // Increased for Python WASM
            instances_limit: 10,
            tables_limit: 10,
            memories_limit: 4,
            session_workspace: None,
            memory_workspace: None,
        })
    }

    #[allow(dead_code)]
    pub fn allow_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.allowed_paths.push(path.into());
        self
    }

    pub fn set_execution_timeout(mut self, timeout: Duration) -> Self {
        self.execution_timeout = timeout;
        self
    }

    pub fn allow_env_access(mut self, allow: bool) -> Self {
        self.allow_env_access = allow;
        self
    }

    pub fn set_env(mut self, key: String, value: String) -> Self {
        self.env_vars.insert(key, value);
        self
    }

    pub fn set_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = bytes;
        self
    }

    /// Set the session workspace path for read-write file access.
    /// 设置用于读写文件访问的 session 工作区路径。
    /// The workspace will be mounted at `/workspace` inside the WASI sandbox.
    /// 该工作区会挂载到 WASI sandbox 内的 `/workspace`。
    pub fn with_session_workspace(mut self, workspace_path: PathBuf) -> Self {
        self.session_workspace = Some(workspace_path);
        self
    }

    /// Get the session workspace path if set.
    /// 返回已设置的 session 工作区路径。
    pub fn session_workspace(&self) -> Option<&PathBuf> {
        self.session_workspace.as_ref()
    }

    /// Set the user memory workspace path for persistent read-write file access.
    /// 设置用于持久化读写文件访问的用户记忆工作区路径。
    /// The workspace will be mounted at `/memory` inside the WASI sandbox.
    /// 该工作区会挂载到 WASI sandbox 内的 `/memory`。
    pub fn with_memory_workspace(mut self, memory_path: PathBuf) -> Self {
        self.memory_workspace = Some(memory_path);
        self
    }

    #[allow(dead_code)]
    pub fn set_table_elements_limit(mut self, elems: usize) -> Self {
        self.table_elements_limit = elems;
        self
    }

    #[allow(dead_code)]
    pub fn set_instances_limit(mut self, n: usize) -> Self {
        self.instances_limit = n;
        self
    }

    #[allow(dead_code)]
    pub fn set_tables_limit(mut self, n: usize) -> Self {
        self.tables_limit = n;
        self
    }

    #[allow(dead_code)]
    pub fn set_memories_limit(mut self, n: usize) -> Self {
        self.memories_limit = n;
        self
    }

    pub async fn execute_wasm(&self, wasm_bytes: &[u8], input: &str) -> Result<String> {
        self.execute_wasm_with_args(wasm_bytes, input, None).await
    }

    pub async fn execute_wasm_with_args(
        &self,
        wasm_bytes: &[u8],
        input: &str,
        argv: Option<Vec<String>>,
    ) -> Result<String> {
        info!("Executing WASM with WASI isolation (argv: {:?})", argv);
        let start = Instant::now();

        // Validate permissions first
        // 先校验权限
        self.validate_permissions()
            .context("Permission validation failed")?;

        // Validate WASM module
        // 校验 WASM 模块
        if wasm_bytes.len() > 50 * 1024 * 1024 {
            return Err(anyhow::anyhow!(
                "WASM module too large: {} bytes",
                wasm_bytes.len()
            ));
        }

        if wasm_bytes.len() < 4 || &wasm_bytes[0..4] != b"\0asm" {
            return Err(anyhow::anyhow!("Invalid WASM module format"));
        }

        // Pre-validate module memory maximums against configured limit (best-effort)
        // 预校验模块声明的内存上限是否超过配置限制（尽力而为）
        // If the module declares a memory maximum larger than allowed, reject early.
        // 若模块声明的内存上限超过允许值，则提前拒绝。
        // Note: This does not replace runtime enforcement but prevents obviously unsafe modules.
        // 注意：这不能替代运行时强制限制，但可拦截明显不安全的模块。
        {
            let tmp_module = Module::new(&self.engine, wasm_bytes)
                .context("Failed to compile WASM module for inspection")?;
            let mut exceeds = false;
            for export in tmp_module.exports() {
                if let ExternType::Memory(mem_ty) = export.ty() {
                    if let Some(max_pages) = mem_ty.maximum() {
                        let max_bytes = (max_pages as usize) * (64 * 1024);
                        if max_bytes > self.memory_limit {
                            exceeds = true;
                            break;
                        }
                    }
                }
            }
            if exceeds {
                return Err(anyhow::anyhow!(
                    "WASM module declares memory larger than allowed (limit={} bytes)",
                    self.memory_limit
                ));
            }
        }

        // Clone data needed for the blocking task
        // 克隆阻塞 task 所需的数据
        let wasm_bytes = wasm_bytes.to_vec();
        let input = input.to_string();
        let argv = argv.clone();
        let engine = self.engine.clone();
        let memory_limit = self.memory_limit;
        let table_elements_limit = self.table_elements_limit;
        let instances_limit = self.instances_limit;
        let memories_limit = self.memories_limit;
        let tables_limit = self.tables_limit;
        let fuel_limit = self.fuel_limit;
        let execution_timeout = self.execution_timeout;
        let allowed_paths = self.allowed_paths.clone();
        let env_vars = self.env_vars.clone();
        let allow_env_access = self.allow_env_access;
        let session_workspace = self.session_workspace.clone();
        let memory_workspace = self.memory_workspace.clone();

        // Start epoch ticker for timeout enforcement with cancellation support
        // 启动 epoch 计时器以执行 timeout 控制，并支持取消
        let engine_weak = Arc::downgrade(&self.engine);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let ticker_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Some(engine) = engine_weak.upgrade() {
                            engine.increment_epoch();
                        } else {
                            break; // Engine dropped, stop ticking
                        }
                    }
                    _ = &mut stop_rx => {
                        break; // Stop requested after execution completes
                    }
                }
            }
        });

        // Run the WASM execution in a blocking thread to avoid async runtime conflicts
        // 在阻塞线程中执行 WASM，避免与异步运行时冲突
        let result = tokio::task::spawn_blocking(move || -> Result<String> {
            // Create WASI context with security-focused configuration
            // 创建面向安全的 WASI context
            let mut wasi_builder = WasiCtxBuilder::new();

            // Configure argv if provided (needed for Python WASM and other interpreters)
            // 若提供 argv 则进行配置（Python WASM 等解释器需要）
            if let Some(args) = argv {
                debug!("WASI: Setting argv: {:?}", args);
                wasi_builder.args(&args);
            }

            // Explicitly disable network access for security
            // 出于安全考虑，显式禁用网络访问
            // WASI preview1 does not provide network capabilities by default.
            // WASI preview1 默认不提供网络能力。
            // Network socket operations will fail with ENOSYS (function not implemented).
            // 网络 socket 操作会以 ENOSYS（功能未实现）失败。
            // This is enforced at the WASI capability level - no TCP/UDP/HTTP operations are possible.
            // 该限制在 WASI 能力层强制执行——无法进行任何 TCP/UDP/HTTP 操作。
            // Future WASI preview2 network proposals will require explicit opt-in.
            // 未来的 WASI preview2 网络提案需要显式开启。

            // Additional security: Don't inherit environment and stdio by default
            // 额外安全措施：默认不继承环境变量和标准输入输出
            // Note: WasiCtxBuilder doesn't inherit by default, we only add what we explicitly allow
            // 注意：WasiCtxBuilder 默认不继承，仅添加显式允许的内容

            // Configure environment variables (disabled by default for security)
            // 配置环境变量（出于安全默认禁用）
            if allow_env_access {
                for (key, value) in &env_vars {
                    wasi_builder.env(key, value);
                }
                debug!("WASI: Allowed {} environment variables", env_vars.len());
            }

            // Configure filesystem access with read-only permissions by default
            // 配置文件系统访问，默认只读权限
            for allowed_path in &allowed_paths {
                // Canonicalize path to prevent symlink escapes
                // 规范化路径，防止符号链接逃逸
                let canonical_path = match allowed_path.canonicalize() {
                    Ok(path) => path,
                    Err(e) => {
                        warn!(
                            "WASI: Failed to canonicalize path {:?}: {}",
                            allowed_path, e
                        );
                        continue;
                    }
                };

                // Verify the canonical path matches expected canonical form
                // 校验规范化路径与预期形式一致
                // This prevents symlink attacks that could escape the sandbox
                // 防止可能逃出 sandbox 的符号链接攻击
                // On macOS, /tmp is a symlink to /private/tmp, so we handle that case
                // macOS 上 /tmp 是指向 /private/tmp 的符号链接，需单独处理
                let expected_canonical = {
                    #[cfg(target_os = "macos")]
                    {
                        // macOS: /tmp is a symlink to /private/tmp
                        // macOS：/tmp 是指向 /private/tmp 的符号链接
                        // Rewrite /tmp/foo → /private/tmp/foo for comparison
                        // 比较时将 /tmp/foo 改写为 /private/tmp/foo
                        if let Ok(suffix) = allowed_path.strip_prefix("/tmp") {
                            std::path::PathBuf::from("/private/tmp").join(suffix)
                        } else {
                            allowed_path.clone()
                        }
                    }
                    #[cfg(not(target_os = "macos"))]
                    {
                        allowed_path.clone()
                    }
                };

                if canonical_path != expected_canonical {
                    warn!(
                        "WASI: Path {:?} resolves to {:?}, expected {:?}. Rejecting non-canonical allowed_path.",
                        allowed_path, canonical_path, expected_canonical
                    );
                    continue;
                }

                if canonical_path.exists() && canonical_path.is_dir() {
                    wasi_builder.preopened_dir(
                        canonical_path.clone(),
                        canonical_path.to_string_lossy(),
                        DirPerms::READ,  // Read-only directory access
                        FilePerms::READ, // Read-only file access
                    )?;
                    debug!(
                        "WASI: Allowed read-only directory access to {:?} (canonical: {:?})",
                        allowed_path, canonical_path
                    );
                }
            }

            // Mount session workspace with read-write permissions
            // 以读写权限挂载 session 工作区
            if let Some(workspace) = &session_workspace {
                if workspace.exists() && workspace.is_dir() {
                    // Canonicalize to prevent symlink escapes
                    // 规范化路径，防止符号链接逃逸
                    let canonical_workspace = workspace.canonicalize().map_err(|e| {
                        anyhow::anyhow!("Failed to canonicalize workspace {:?}: {}", workspace, e)
                    })?;

                    wasi_builder.preopened_dir(
                        canonical_workspace.clone(),
                        "/workspace",        // Guest path - always /workspace
                        DirPerms::all(),     // Read + write + create directories
                        FilePerms::all(),    // Read + write files
                    )?;
                    // Also mount as "." so relative paths in python_executor resolve
                    // 同时挂载为 "."，使 python_executor 中的相对路径
                    // to the workspace directory (WASI has no chdir syscall; CWD is
                    // 解析到工作区目录（WASI 没有 chdir 系统调用，CWD 由
                    // determined by the preopened "." mapping).
                    // 预打开的 "." 映射决定）。
                    wasi_builder.preopened_dir(
                        canonical_workspace.clone(),
                        ".",
                        DirPerms::all(),
                        FilePerms::all(),
                    )?;
                    info!(
                        "WASI: Mounted session workspace {:?} at /workspace and . with read-write access",
                        canonical_workspace
                    );
                } else {
                    warn!(
                        "WASI: Session workspace {:?} does not exist or is not a directory",
                        workspace
                    );
                }
            }

            // Mount user memory workspace with read-write permissions
            // 以读写权限挂载用户记忆工作区
            if let Some(ref memory_path) = memory_workspace {
                if memory_path.exists() && memory_path.is_dir() {
                    let canonical_memory = memory_path.canonicalize().map_err(|e| {
                        anyhow::anyhow!("Failed to canonicalize memory workspace {:?}: {}", memory_path, e)
                    })?;

                    wasi_builder.preopened_dir(
                        canonical_memory.clone(),
                        "/memory",           // Guest path - always /memory
                        DirPerms::all(),     // Read + write + create directories
                        FilePerms::all(),    // Read + write files
                    )?;
                    info!(
                        "WASI: Mounted user memory {:?} at /memory with read-write access",
                        canonical_memory
                    );
                } else {
                    warn!(
                        "WASI: User memory workspace {:?} does not exist or is not a directory",
                        memory_path
                    );
                }
            }

            // Configure stdin/stdout/stderr using in-memory pipes for isolation and capture
            // 用内存管道配置 stdin/stdout/stderr，实现隔离并捕获输出
            let stdin_pipe = MemoryInputPipe::new(input.as_bytes().to_vec());
            let stdout_pipe = MemoryOutputPipe::new(1024 * 1024); // 1MB buffer
            let stderr_pipe = MemoryOutputPipe::new(1024 * 1024);

            // Keep clones to read after execution
            // 保留克隆，便于执行后读取
            let stdout_reader = stdout_pipe.clone();
            let stderr_reader = stderr_pipe.clone();

            wasi_builder
                .stdin(stdin_pipe)
                .stdout(stdout_pipe)
                .stderr(stderr_pipe);

            // Build a Preview1 context to use with the core wasm Linker
            // 构建 Preview1 context，供核心 wasm Linker 使用
            let wasi_ctx = wasi_builder.build_p1();

            // Build store limits to enforce memory/table/instance caps
            // 构建 store 限制，强制执行内存/表/实例上限
            let store_limits = wasmtime::StoreLimitsBuilder::new()
                .memory_size(memory_limit)
                .table_elements(table_elements_limit)
                .instances(instances_limit)
                .memories(memories_limit)
                .tables(tables_limit)
                .trap_on_grow_failure(false)
                .build();

            // Host context containing WASI and limits for the store
            // 宿主 context，包含 WASI 与 store 限制
            struct HostCtx {
                wasi: wasmtime_wasi::preview1::WasiP1Ctx,
                limits: wasmtime::StoreLimits,
            }

            // Compile module
            // 编译模块
            let module = Module::new(&engine, &wasm_bytes).map_err(|e| {
                warn!("WASI: Failed to compile WASM module: {}", e);
                anyhow::anyhow!("Failed to compile WASM module: {}", e)
            })?;

            // Create store with WASI context and resource limits
            // 创建带 WASI context 和资源限制的 store
            let mut store = Store::new(
                &engine,
                HostCtx {
                    wasi: wasi_ctx,
                    limits: store_limits,
                },
            );
            // Attach limiter for memory/table growth
            // 挂载限制器，控制内存/表增长
            store.limiter(|host| &mut host.limits);

            // Set fuel limit for CPU control
            // 设置 fuel 上限以控制 CPU 消耗
            store
                .set_fuel(fuel_limit)
                .context("Failed to set fuel limit")?;

            // TODO: Add memory limits via StoreLimits once lifetime issues are resolved
            // TODO：待生命周期问题解决后，通过 StoreLimits 增加内存限制
            // For now, we rely on the memory guard size set in engine config (64MB)
            // 目前依赖引擎配置中的内存保护大小（64MB）
            debug!(
                "WASI: Memory limit configured via engine guard size: {}MB",
                64
            );

            // Set epoch deadline for timeout control
            // 设置 epoch 截止点以实现 timeout 控制
            let deadline_ticks = (execution_timeout.as_millis() / 100) as u64;
            store.set_epoch_deadline(deadline_ticks);

            // Create linker and add WASI so modules with WASI imports run correctly
            // 创建 linker 并加入 WASI，使带 WASI 导入的模块可正常运行
            let mut linker: Linker<HostCtx> = Linker::new(&engine);
            wasmtime_wasi::preview1::add_to_linker_sync(&mut linker, |t: &mut HostCtx| &mut t.wasi)
                .context("Failed to add WASI to linker")?;

            // Execute the module
            // 执行模块
            let instance = linker.instantiate(&mut store, &module).map_err(|e| {
                warn!("WASI: Failed to instantiate WASM module: {}", e);
                anyhow::anyhow!("Failed to instantiate WASM module: {}", e)
            })?;

            // Try to call _start (WASI main entry point)
            // 尝试调用 _start（WASI 主入口）
            let execution_result = if let Some(start_func) = instance.get_func(&mut store, "_start")
            {
                debug!("WASI: Found _start function, executing module with security limits");
                start_func.call(&mut store, &[], &mut [])
            } else {
                warn!("WASI: No _start function found in module");
                return Err(anyhow::anyhow!("WASM module has no _start entry point"));
            };

            // Handle execution result
            // 处理执行结果
            match execution_result {
                Ok(_) => {
                    debug!("WASI: Module executed successfully within security constraints");
                    // Read captured stdout (best-effort)
                    // 读取捕获的 stdout（尽力而为）
                    let out_bytes = stdout_reader.contents();
                    let out = String::from_utf8_lossy(out_bytes.as_ref()).to_string();
                    if let Some(tool_executions) = TOOL_EXECUTIONS.get() {
                        tool_executions
                            .with_label_values(&["wasi", "success"])
                            .inc();
                    }
                    if let Some(tool_duration) = TOOL_DURATION.get() {
                        tool_duration
                            .with_label_values(&["wasi"])
                            .observe(start.elapsed().as_secs_f64());
                    }
                    Ok(out)
                }
                Err(e) => {
                    warn!("WASI: Module execution failed: {}", e);
                    let err_bytes = stderr_reader.contents();
                    let err_out = String::from_utf8_lossy(err_bytes.as_ref()).to_string();
                    let error_msg = format!("[WASI] Execution failed: {}\n{}", e, err_out);
                    if let Some(tool_executions) = TOOL_EXECUTIONS.get() {
                        tool_executions.with_label_values(&["wasi", "error"]).inc();
                    }
                    if let Some(tool_duration) = TOOL_DURATION.get() {
                        tool_duration
                            .with_label_values(&["wasi"])
                            .observe(start.elapsed().as_secs_f64());
                    }
                    Err(anyhow::anyhow!(error_msg))
                }
            }
        })
        .await
        .context("WASM execution task panicked")?;

        // Stop epoch ticker
        // 停止 epoch 计时器
        let _ = stop_tx.send(());

        // Wait for ticker to stop
        // 等待计时器退出
        let _ = ticker_handle.await;

        result
    }

    pub fn validate_permissions(&self) -> Result<()> {
        for path in &self.allowed_paths {
            if !path.exists() {
                warn!("WASI: Allowed path does not exist: {:?}", path);
            } else if !path.is_dir() {
                return Err(anyhow::anyhow!(
                    "WASI: Allowed path is not a directory: {:?}",
                    path
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Minimal wasm binary exporting an empty `_start` function.
    // 导出空 `_start` 函数的最小 wasm 二进制。
    // (module (type (func)) (func (type 0)) (export "_start" (func 0)) (code (func)))
    const MINIMAL_WASM: &[u8] = &[
        0x00, 0x61, 0x73, 0x6d, // \0asm
        0x01, 0x00, 0x00, 0x00, // version 1
        // Type section: id=1, size=4, count=1, functype 0x60, 0 params, 0 results
        0x01, 0x04, 0x01, 0x60, 0x00, 0x00,
        // Function section: id=3, size=2, count=1, type index=0
        0x03, 0x02, 0x01, 0x00,
        // Export section: id=7, size=10, count=1, name len=6, name="_start", kind=func(0), index=0
        0x07, 0x0a, 0x01, 0x06, 0x5f, 0x73, 0x74, 0x61, 0x72, 0x74, 0x00, 0x00,
        // Code section: id=10, size=4, count=1, body size=2, locals=0, end=0x0b
        0x0a, 0x04, 0x01, 0x02, 0x00, 0x0b,
    ];

    #[tokio::test]
    async fn test_wasi_sandbox_creation() {
        let sandbox = WasiSandbox::new().unwrap();
        assert!(sandbox.validate_permissions().is_ok());
    }

    #[tokio::test]
    async fn test_wasi_sandbox_configuration() {
        let sandbox = WasiSandbox::new()
            .unwrap()
            .allow_env_access(true)
            .set_env("TEST_VAR".to_string(), "test_value".to_string())
            .set_execution_timeout(Duration::from_secs(60));

        assert!(sandbox.allow_env_access);
        assert_eq!(sandbox.env_vars.get("TEST_VAR").unwrap(), "test_value");
        assert_eq!(sandbox.execution_timeout, Duration::from_secs(60));
    }

    #[tokio::test]
    async fn test_wasi_sandbox_with_session_workspace() {
        let temp_dir = std::env::temp_dir().join("wasi-test-workspace");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let sandbox = WasiSandbox::new()
            .unwrap()
            .with_session_workspace(temp_dir.clone());

        assert!(sandbox.session_workspace().is_some());
        assert_eq!(sandbox.session_workspace().unwrap(), &temp_dir);

        // Cleanup
        // 清理
        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[tokio::test]
    async fn test_wasi_executes_minimal_wasm() {
        let sandbox = WasiSandbox::new().unwrap();
        let out = sandbox
            .execute_wasm(MINIMAL_WASM, "")
            .await
            .expect("minimal wasm should execute successfully");
        assert!(out.is_empty(), "expected empty stdout, got: {}", out);
    }
}
