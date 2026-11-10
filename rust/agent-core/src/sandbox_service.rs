//! gRPC service for WASI-isolated file operations.
//! 提供 WASI 隔离文件操作的 gRPC 服务。

use crate::memory_manager::MemoryManager;
use crate::safe_commands::SafeCommand;
use crate::workspace::WorkspaceManager;
use regex::Regex;
use std::path::PathBuf;
use std::time::Duration;
use std::time::Instant;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

// Include generated proto code
// 引入生成的 proto 代码
pub mod proto {
    tonic::include_proto!("shannon.sandbox");
}

use proto::sandbox_service_server::{SandboxService, SandboxServiceServer};
use proto::*;

/// Configuration for the sandbox service.
/// sandbox 服务配置。
pub struct SandboxConfig {
    /// Maximum file size for reads (default 10MB)
    /// 单次读取的最大文件大小（默认 10MB）
    pub max_read_bytes: usize,
    /// Maximum workspace size per session (default 100MB)
    /// 每个 session 的最大工作区大小（默认 100MB）
    pub max_workspace_bytes: u64,
    /// Maximum memory store size per user (default 10MB)
    /// 每个用户的最大记忆库大小（默认 10MB）
    pub max_memory_bytes: u64,
    /// Command timeout in seconds (default 30)
    /// 命令 timeout 时间，单位秒（默认 30）
    pub command_timeout_seconds: u32,
}

impl Default for SandboxConfig {
    fn default() -> Self {
        Self {
            max_read_bytes: 10 * 1024 * 1024,       // 10MB
            max_workspace_bytes: 100 * 1024 * 1024, // 100MB
            max_memory_bytes: 10 * 1024 * 1024,     // 10MB
            command_timeout_seconds: 30,
        }
    }
}

/// Implementation of the SandboxService.
/// SandboxService 的实现。
pub struct SandboxServiceImpl {
    workspace_mgr: WorkspaceManager,
    memory_mgr: MemoryManager,
    config: SandboxConfig,
}

impl SandboxServiceImpl {
    /// Create a new sandbox service with the given workspace base directory.
    /// 使用给定的工作区根目录创建 sandbox 服务。
    pub fn new(workspaces_dir: PathBuf) -> Self {
        Self {
            workspace_mgr: WorkspaceManager::new(workspaces_dir),
            memory_mgr: MemoryManager::from_env(),
            config: SandboxConfig::default(),
        }
    }

    /// Create from environment.
    /// 从环境变量创建。
    pub fn from_env() -> Self {
        Self {
            workspace_mgr: WorkspaceManager::from_env(),
            memory_mgr: MemoryManager::from_env(),
            config: SandboxConfig::default(),
        }
    }

    /// Create with custom config.
    /// 使用自定义配置创建。
    pub fn with_config(workspaces_dir: PathBuf, config: SandboxConfig) -> Self {
        Self {
            workspace_mgr: WorkspaceManager::new(workspaces_dir),
            memory_mgr: MemoryManager::from_env(),
            config,
        }
    }

    /// Create a tonic service.
    /// 创建 tonic 服务。
    pub fn into_service(self) -> SandboxServiceServer<Self> {
        SandboxServiceServer::new(self)
    }

    /// Resolve a path within a session's workspace or user's memory directory.
    /// 将 session 工作区或用户记忆目录内的路径解析为绝对路径。
    ///
    /// Paths prefixed with `/memory/` resolve to the user's persistent memory directory.
    /// 以 `/memory/` 为前缀的路径解析到用户的持久记忆目录。
    /// All other paths resolve to the session workspace (with `/workspace/` prefix stripped).
    /// 其余路径解析到 session 工作区（会去掉 `/workspace/` 前缀）。
    fn resolve_path(&self, session_id: &str, user_id: &str, path: &str) -> Result<PathBuf, Status> {
        // Check if path targets /memory/ (user persistent memory)
        // 检查路径是否指向 /memory/（用户持久记忆）
        if path.starts_with("/memory/") || path == "/memory" {
            let memory_subpath = path
                .strip_prefix("/memory/")
                .or_else(|| path.strip_prefix("/memory"))
                .unwrap_or("");

            if user_id.is_empty() {
                return Err(Status::invalid_argument(
                    "Cannot resolve /memory/ path without user_id",
                ));
            }

            let memory_dir = self
                .memory_mgr
                .get_memory_dir(user_id)
                .map_err(|e| Status::invalid_argument(format!("Invalid user for memory: {}", e)))?;

            if memory_subpath.is_empty() {
                return Ok(memory_dir);
            }

            // Security: reject path traversal in subpath
            // 安全：拒绝子路径中的路径穿越
            if memory_subpath.contains("..") {
                warn!(
                    user_id = %user_id,
                    path = %path,
                    violation = "memory_path_traversal",
                    "Security violation: path traversal in memory path"
                );
                return Err(Status::permission_denied(
                    "Path traversal not allowed in /memory",
                ));
            }

            let target = memory_dir.join(memory_subpath);

            // For existing paths, verify they're within memory dir
            // 对已存在的路径，校验其仍位于记忆目录内
            if target.exists() {
                let canonical = target
                    .canonicalize()
                    .map_err(|e| Status::not_found(format!("Path error: {}", e)))?;

                if !canonical.starts_with(&memory_dir) {
                    warn!(
                        user_id = %user_id,
                        path = %path,
                        resolved = %canonical.display(),
                        violation = "memory_path_escape",
                        "Security violation: path escapes memory directory"
                    );
                    return Err(Status::permission_denied("Path escapes memory directory"));
                }
                return Ok(canonical);
            }

            // For non-existing paths, validate parent
            // 对不存在的路径，校验其父目录
            if let Some(parent) = target.parent() {
                if parent.exists() {
                    let canonical_parent = parent
                        .canonicalize()
                        .map_err(|e| Status::internal(format!("Parent path error: {}", e)))?;

                    if !canonical_parent.starts_with(&memory_dir) {
                        warn!(
                            user_id = %user_id,
                            path = %path,
                            resolved_parent = %canonical_parent.display(),
                            violation = "memory_parent_escape",
                            "Security violation: parent path escapes memory directory"
                        );
                        return Err(Status::permission_denied("Path escapes memory directory"));
                    }
                }
            }

            return Ok(target);
        }

        // Normalize /workspace/ prefix (used by Firecracker VMs) to relative path
        // 去掉 Firecracker VM 使用的 /workspace/ 前缀，转为相对路径
        // This allows consistent path handling between python_executor and file_read/file_write
        // 使 python_executor 与 file_read/file_write 的路径处理保持一致
        let normalized_path = path
            .strip_prefix("/workspace/")
            .or_else(|| path.strip_prefix("/workspace"))
            .unwrap_or(path);

        // Security: Reject absolute paths upfront (after normalization)
        // 安全：归一化后直接拒绝绝对路径
        let req_path = std::path::Path::new(normalized_path);
        if req_path.is_absolute() {
            warn!(
                session_id = %session_id,
                path = %path,
                normalized = %normalized_path,
                violation = "absolute_path",
                "Security violation: absolute path not allowed"
            );
            return Err(Status::permission_denied(
                "Absolute paths not allowed; use relative paths within session workspace",
            ));
        }

        let workspace = self
            .workspace_mgr
            .get_workspace(session_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid session: {}", e)))?;

        // Handle empty path as workspace root
        // 空路径按项目根目录处理
        let relative = if normalized_path.is_empty() {
            "."
        } else {
            normalized_path
        };
        let target = workspace.join(relative);

        // For existing paths, verify they're within workspace
        // 对已存在的路径，校验其仍位于工作区内
        if target.exists() {
            let canonical = target
                .canonicalize()
                .map_err(|e| Status::not_found(format!("Path error: {}", e)))?;

            if !canonical.starts_with(&workspace) {
                warn!(
                    session_id = %session_id,
                    path = %path,
                    resolved = %canonical.display(),
                    violation = "path_escape",
                    "Security violation: path escapes workspace"
                );
                return Err(Status::permission_denied("Path escapes workspace"));
            }
            return Ok(canonical);
        }

        // For non-existing paths (writes), validate parent
        // 对不存在的路径（写入场景），校验其父目录
        if let Some(parent) = target.parent() {
            if parent.exists() {
                let canonical_parent = parent
                    .canonicalize()
                    .map_err(|e| Status::internal(format!("Parent path error: {}", e)))?;

                if !canonical_parent.starts_with(&workspace) {
                    warn!(
                        session_id = %session_id,
                        path = %path,
                        resolved_parent = %canonical_parent.display(),
                        violation = "parent_path_escape",
                        "Security violation: parent path escapes workspace"
                    );
                    return Err(Status::permission_denied("Path escapes workspace"));
                }
            }
        }

        Ok(target)
    }

    /// Check workspace quota before write operations.
    /// 写入前检查工作区配额。
    fn check_workspace_quota(&self, session_id: &str, additional_bytes: u64) -> Result<(), Status> {
        let current_size = self
            .workspace_mgr
            .get_workspace_size(session_id)
            .map_err(|e| Status::internal(format!("Quota check failed: {}", e)))?;

        if current_size + additional_bytes > self.config.max_workspace_bytes {
            return Err(Status::resource_exhausted(format!(
                "Workspace quota exceeded: {} + {} > {} bytes",
                current_size, additional_bytes, self.config.max_workspace_bytes
            )));
        }
        Ok(())
    }

    /// Check memory quota before /memory write operations.
    /// 写入 /memory 前检查记忆库配额。
    fn check_memory_quota(&self, user_id: &str, additional_bytes: u64) -> Result<(), Status> {
        let current_size = self
            .memory_mgr
            .get_memory_size(user_id)
            .map_err(|e| Status::invalid_argument(format!("Cannot check memory quota: {}", e)))?;

        if current_size + additional_bytes > self.config.max_memory_bytes {
            return Err(Status::resource_exhausted(format!(
                "Memory quota exceeded: {} + {} > {} bytes",
                current_size, additional_bytes, self.config.max_memory_bytes
            )));
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl SandboxService for SandboxServiceImpl {
    async fn file_read(
        &self,
        request: Request<FileReadRequest>,
    ) -> Result<Response<FileReadResponse>, Status> {
        let req = request.into_inner();
        info!(
            session_id = %req.session_id,
            path = %req.path,
            operation = "file_read",
            "Sandbox file read operation"
        );

        let target = self.resolve_path(&req.session_id, &req.user_id, &req.path)?;

        if !target.exists() {
            return Ok(Response::new(FileReadResponse {
                success: false,
                error: format!("File not found: {}. Use file_list(\".\") to discover correct file paths.", req.path),
                ..Default::default()
            }));
        }

        if !target.is_file() {
            return Ok(Response::new(FileReadResponse {
                success: false,
                error: format!("Path is a directory, not a file: {}. Use file_list(\"{}\") to see its contents.", req.path, req.path),
                ..Default::default()
            }));
        }

        // Check file size
        // 检查文件大小
        let metadata = std::fs::metadata(&target)
            .map_err(|e| Status::not_found(format!("File not found: {}", e)))?;

        // Cap max_bytes at configured limit to prevent bypass
        // 将 max_bytes 限制在配置上限内，防止绕过
        let max_bytes = if req.max_bytes > 0 {
            std::cmp::min(req.max_bytes as usize, self.config.max_read_bytes)
        } else {
            self.config.max_read_bytes
        };

        if metadata.len() as usize > max_bytes {
            return Ok(Response::new(FileReadResponse {
                success: false,
                error: format!(
                    "File too large: {} bytes (max {})",
                    metadata.len(),
                    max_bytes
                ),
                ..Default::default()
            }));
        }

        // Read file
        // 读取文件
        let content = std::fs::read_to_string(&target)
            .map_err(|e| Status::internal(format!("Read error: {}", e)))?;

        let file_type = target
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_string();

        Ok(Response::new(FileReadResponse {
            success: true,
            content,
            error: String::new(),
            size_bytes: metadata.len() as i64,
            file_type,
        }))
    }

    async fn file_write(
        &self,
        request: Request<FileWriteRequest>,
    ) -> Result<Response<FileWriteResponse>, Status> {
        let req = request.into_inner();
        let bytes_to_write = req.content.len();
        info!(
            session_id = %req.session_id,
            path = %req.path,
            bytes = bytes_to_write,
            append = req.append,
            operation = "file_write",
            "Sandbox file write operation"
        );

        let is_memory_path = req.path.starts_with("/memory/") || req.path == "/memory";
        if is_memory_path {
            self.check_memory_quota(&req.user_id, req.content.len() as u64)?;
        } else {
            self.check_workspace_quota(&req.session_id, req.content.len() as u64)?;
        }

        // Handle /memory/ prefix — resolve via user memory directory
        // 处理 /memory/ 前缀，通过用户记忆目录解析
        if is_memory_path {
            let target = self.resolve_path(&req.session_id, &req.user_id, &req.path)?;

            // Create parent directories if requested
            // 按需创建父目录
            if req.create_dirs {
                if let Some(parent) = target.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        Status::internal(format!("Failed to create parent dirs: {}", e))
                    })?;
                }
            }

            let bytes_written = if req.append {
                use std::io::Write;
                let mut f = std::fs::OpenOptions::new()
                    .append(true)
                    .create(true)
                    .open(&target)
                    .map_err(|e| Status::internal(format!("Failed to open for append: {}", e)))?;
                let content = req.content.as_bytes();
                f.write_all(content)
                    .map_err(|e| Status::internal(format!("Write failed: {}", e)))?;
                content.len() as i64
            } else {
                let content = req.content.as_bytes();
                std::fs::write(&target, content)
                    .map_err(|e| Status::internal(format!("Write failed: {}", e)))?;
                content.len() as i64
            };

            info!(
                user_id = %req.user_id,
                path = %req.path,
                bytes = bytes_written,
                "Memory file write completed"
            );

            return Ok(Response::new(FileWriteResponse {
                success: true,
                bytes_written,
                absolute_path: target.to_string_lossy().to_string(),
                ..Default::default()
            }));
        }

        // Normalize /workspace/ prefix (used by Firecracker VMs) to relative path
        // 去掉 Firecracker VM 使用的 /workspace/ 前缀，转为相对路径
        let normalized_path = req
            .path
            .strip_prefix("/workspace/")
            .or_else(|| req.path.strip_prefix("/workspace"))
            .unwrap_or(&req.path);

        // Security: Reject absolute paths upfront (after normalization)
        // 安全：归一化后直接拒绝绝对路径
        let req_path = std::path::Path::new(normalized_path);
        if req_path.is_absolute() {
            warn!(
                session_id = %req.session_id,
                path = %req.path,
                normalized = %normalized_path,
                violation = "absolute_path",
                operation = "file_write",
                "Security violation: absolute path not allowed"
            );
            return Ok(Response::new(FileWriteResponse {
                success: false,
                error: "Absolute paths not allowed; use relative paths within session workspace"
                    .to_string(),
                ..Default::default()
            }));
        }

        let workspace = self
            .workspace_mgr
            .get_workspace(&req.session_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid session: {}", e)))?;

        let target = workspace.join(normalized_path);

        // Security: Validate ALL path components BEFORE any directory creation
        // 安全：在创建任何目录之前先校验全部路径分量，
        // to prevent symlink attacks via parent directory manipulation
        // 防止通过操纵父目录实施符号链接攻击
        fn validate_path_components(
            workspace: &std::path::Path,
            target: &std::path::Path,
            session_id: &str,
        ) -> Result<(), Status> {
            // Check that target path string doesn't contain suspicious patterns
            // 检查目标路径字符串中是否含有可疑模式
            let target_str = target.to_string_lossy();
            if target_str.contains("..") {
                warn!(
                    session_id = %session_id,
                    path = %target_str,
                    violation = "path_traversal",
                    operation = "file_write",
                    "Security violation: path traversal attempt detected"
                );
                return Err(Status::permission_denied("Path traversal not allowed"));
            }

            // Validate each existing component is within workspace
            // 校验每个已存在的路径分量都位于工作区内
            let mut current = workspace.to_path_buf();
            for component in target
                .strip_prefix(workspace)
                .unwrap_or(target)
                .components()
            {
                use std::path::Component;
                match component {
                    Component::Normal(name) => {
                        current = current.join(name);
                        if current.exists() {
                            // Check for symlinks pointing outside workspace
                            // 检查是否存在指向工作区外的符号链接
                            let metadata = std::fs::symlink_metadata(&current).map_err(|e| {
                                Status::internal(format!("Path check error: {}", e))
                            })?;
                            if metadata.file_type().is_symlink() {
                                let link_target = std::fs::read_link(&current).map_err(|e| {
                                    Status::internal(format!("Symlink read error: {}", e))
                                })?;
                                // Resolve symlink and verify it's within workspace
                                // 解析符号链接并确认其目标位于工作区内
                                let resolved = if link_target.is_absolute() {
                                    link_target
                                } else {
                                    current.parent().unwrap_or(workspace).join(&link_target)
                                };
                                // Symlink must be resolvable and within workspace
                                // 符号链接必须可解析且位于工作区内
                                let canonical = resolved.canonicalize().map_err(|_| {
                                    warn!(
                                        session_id = %session_id,
                                        path = %current.display(),
                                        violation = "unresolvable_symlink",
                                        operation = "file_write",
                                        "Security violation: symlink target cannot be resolved"
                                    );
                                    Status::permission_denied("Symlink target cannot be resolved")
                                })?;
                                if !canonical.starts_with(workspace) {
                                    warn!(
                                        session_id = %session_id,
                                        path = %current.display(),
                                        symlink_target = %canonical.display(),
                                        violation = "symlink_escape",
                                        operation = "file_write",
                                        "Security violation: symlink escapes workspace"
                                    );
                                    return Err(Status::permission_denied(
                                        "Symlink escapes workspace",
                                    ));
                                }
                            }
                        }
                    }
                    Component::ParentDir => {
                        warn!(
                            session_id = %session_id,
                            path = %target.display(),
                            violation = "parent_traversal",
                            operation = "file_write",
                            "Security violation: parent directory traversal attempt"
                        );
                        return Err(Status::permission_denied(
                            "Parent directory traversal not allowed",
                        ));
                    }
                    _ => {}
                }
            }
            Ok(())
        }

        validate_path_components(&workspace, &target, &req.session_id)?;

        // Create parent directories if requested (now safe after validation)
        // 按需创建父目录（此时已完成校验，可安全创建）
        if req.create_dirs {
            if let Some(parent) = target.parent() {
                std::fs::create_dir_all(parent).map_err(|e| {
                    Status::internal(format!("Failed to create directories: {}", e))
                })?;
            }
        } else if let Some(parent) = target.parent() {
            if !parent.exists() {
                return Ok(Response::new(FileWriteResponse {
                    success: false,
                    error: "Parent directory does not exist".to_string(),
                    ..Default::default()
                }));
            }
        }

        // Post-creation verification (defense in depth)
        // 创建后的复核（纵深防御）
        if target.exists() {
            let canonical = target
                .canonicalize()
                .map_err(|e| Status::internal(format!("Path resolution error: {}", e)))?;
            if !canonical.starts_with(&workspace) {
                warn!(
                    session_id = %req.session_id,
                    path = %req.path,
                    "Path escape detected after creation"
                );
                return Err(Status::permission_denied("Path escapes workspace"));
            }
        }

        // Write file
        // 写入文件
        let bytes_written = if req.append {
            use std::io::Write;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&target)
                .map_err(|e| Status::internal(format!("Open error: {}", e)))?;
            file.write_all(req.content.as_bytes())
                .map_err(|e| Status::internal(format!("Write error: {}", e)))?;
            req.content.len() as i64
        } else {
            std::fs::write(&target, &req.content)
                .map_err(|e| Status::internal(format!("Write error: {}", e)))?;
            req.content.len() as i64
        };

        // Return relative path within workspace
        // 返回工作区内的相对路径
        let resolved = target.canonicalize().unwrap_or(target);
        let relative = resolved
            .strip_prefix(&workspace)
            .unwrap_or(&resolved)
            .to_string_lossy()
            .to_string();

        Ok(Response::new(FileWriteResponse {
            success: true,
            bytes_written,
            error: String::new(),
            absolute_path: relative,
        }))
    }

    async fn file_list(
        &self,
        request: Request<FileListRequest>,
    ) -> Result<Response<FileListResponse>, Status> {
        let req = request.into_inner();
        info!(
            session_id = %req.session_id,
            path = %req.path,
            pattern = %req.pattern,
            recursive = req.recursive,
            operation = "file_list",
            "Sandbox file list operation"
        );

        let target = self.resolve_path(&req.session_id, &req.user_id, &req.path)?;
        let workspace = if req.path.starts_with("/memory/") || req.path == "/memory" {
            self.memory_mgr
                .get_memory_dir(&req.user_id)
                .map_err(|e| Status::invalid_argument(format!("Invalid user for memory: {}", e)))?
        } else {
            self.workspace_mgr
                .get_workspace(&req.session_id)
                .map_err(|e| Status::invalid_argument(format!("Invalid session: {}", e)))?
        };

        if !target.is_dir() {
            return Ok(Response::new(FileListResponse {
                success: false,
                error: "Path is not a directory".to_string(),
                ..Default::default()
            }));
        }

        let mut entries = Vec::new();
        let mut file_count = 0i32;
        let mut dir_count = 0i32;

        fn collect_entries(
            dir: &std::path::Path,
            workspace: &std::path::Path,
            pattern: &str,
            recursive: bool,
            include_hidden: bool,
            entries: &mut Vec<FileEntry>,
            file_count: &mut i32,
            dir_count: &mut i32,
        ) -> Result<(), std::io::Error> {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();

                // Skip hidden files unless requested
                // 除非显式要求，否则跳过隐藏文件
                if !include_hidden && name.starts_with('.') {
                    continue;
                }

                let metadata = entry.metadata()?;
                let path = entry.path();
                let is_file = metadata.is_file();

                // Apply pattern filter to FILES only — always traverse directories
                // 仅对文件应用 pattern 过滤——递归时始终遍历目录，
                // when recursive. This prevents pattern="*.md" from skipping
                // 避免 pattern="*.md" 跳过 "findings/" 这类目录，
                // directories like "findings/" and never seeing their contents.
                // 导致其内容永远不被检索到。
                let matches_pattern = pattern.is_empty() || glob_match(pattern, &name);
                if !matches_pattern && is_file {
                    continue;
                }

                // For non-recursive mode, skip non-matching directories too
                // 非递归模式下，不匹配的目录同样跳过
                if !matches_pattern && !recursive {
                    continue;
                }

                let relative = path
                    .strip_prefix(workspace)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .to_string();

                // Only add matching entries to results
                // 仅将匹配的条目加入结果
                if matches_pattern {
                    if is_file {
                        *file_count += 1;
                    } else {
                        *dir_count += 1;
                    }

                    entries.push(FileEntry {
                        name,
                        path: relative,
                        is_file,
                        size_bytes: if is_file { metadata.len() as i64 } else { 0 },
                        modified_time: metadata
                            .modified()
                            .ok()
                            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0),
                    });
                }

                // Always recurse into directories when recursive=true,
                // recursive=true 时始终递归进入目录，
                // regardless of pattern match
                // 不受 pattern 匹配结果影响
                if recursive && !is_file {
                    collect_entries(
                        &path,
                        workspace,
                        pattern,
                        recursive,
                        include_hidden,
                        entries,
                        file_count,
                        dir_count,
                    )?;
                }
            }
            Ok(())
        }

        collect_entries(
            &target,
            &workspace,
            &req.pattern,
            req.recursive,
            req.include_hidden,
            &mut entries,
            &mut file_count,
            &mut dir_count,
        )
        .map_err(|e| Status::internal(format!("List error: {}", e)))?;

        // Sort by name
        // 按名称排序
        entries.sort_by(|a, b| a.name.cmp(&b.name));

        Ok(Response::new(FileListResponse {
            success: true,
            entries,
            error: String::new(),
            file_count,
            dir_count,
        }))
    }

    async fn execute_command(
        &self,
        request: Request<CommandRequest>,
    ) -> Result<Response<CommandResponse>, Status> {
        let req = request.into_inner();
        info!(
            session_id = %req.session_id,
            command = %req.command,
            user_id = %req.user_id,
            operation = "execute_command",
            "Sandbox command execution"
        );

        let workspace = self
            .workspace_mgr
            .get_workspace(&req.session_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid session: {}", e)))?;

        // Parse command
        // 解析命令
        let cmd = match SafeCommand::parse(&req.command) {
            Ok(c) => c,
            Err(e) => {
                return Ok(Response::new(CommandResponse {
                    success: false,
                    error: format!("Command not allowed: {}", e),
                    exit_code: 1,
                    ..Default::default()
                }));
            }
        };
        // Resolve memory access only when the command references /memory.
        // 仅在命令引用 /memory 时才解析记忆目录访问权限。
        let memory_workspace = if cmd.uses_memory() {
            if req.user_id.is_empty() {
                return Ok(Response::new(CommandResponse {
                    success: false,
                    error: "Cannot access /memory without authenticated user_id".to_string(),
                    exit_code: 1,
                    ..Default::default()
                }));
            } else {
                match self.memory_mgr.get_memory_dir(&req.user_id) {
                    Ok(dir) => Some(dir),
                    Err(e) => {
                        return Err(Status::invalid_argument(format!(
                            "Invalid user for memory: {}",
                            e
                        )))
                    }
                }
            }
        } else {
            None
        };

        let start = Instant::now();

        // Enforce timeout (use request timeout or config default, max 30s)
        // 强制 timeout（取 request timeout 或配置默认值，上限 30 秒）
        let timeout_secs = if req.timeout_seconds > 0 {
            req.timeout_seconds
                .min(self.config.command_timeout_seconds as i32) as u64
        } else {
            self.config.command_timeout_seconds as u64
        };
        let timeout = Duration::from_secs(timeout_secs);

        // Execute command with timeout using spawn_blocking for sync operations
        // 带 timeout 执行命令；同步操作通过 spawn_blocking 运行
        let result = tokio::time::timeout(timeout, async {
            let memory_workspace = memory_workspace;
            tokio::task::spawn_blocking(move || match memory_workspace {
                Some(dir) => cmd.execute_with_memory(&workspace, Some(&dir)),
                None => cmd.execute(&workspace),
            })
            .await
            .map_err(|e| anyhow::anyhow!("Task panicked: {}", e))?
        })
        .await;

        let execution_time_ms = start.elapsed().as_millis() as i64;

        match result {
            Ok(Ok(output)) => Ok(Response::new(CommandResponse {
                success: output.exit_code == 0,
                stdout: output.stdout,
                stderr: output.stderr,
                exit_code: output.exit_code,
                error: String::new(),
                execution_time_ms,
            })),
            Ok(Err(e)) => Ok(Response::new(CommandResponse {
                success: false,
                stdout: String::new(),
                stderr: e.to_string(),
                exit_code: 1,
                error: format!("Execution error: {}", e),
                execution_time_ms,
            })),
            Err(_) => {
                warn!(
                    session_id = %req.session_id,
                    command = %req.command,
                    timeout_secs = timeout_secs,
                    "Command timed out"
                );
                Ok(Response::new(CommandResponse {
                    success: false,
                    stdout: String::new(),
                    stderr: format!("Command timed out after {}s", timeout_secs),
                    exit_code: 124, // Standard timeout exit code
                    error: format!("Command timed out after {}s", timeout_secs),
                    execution_time_ms,
                }))
            }
        }
    }

    async fn file_search(
        &self,
        request: Request<FileSearchRequest>,
    ) -> Result<Response<FileSearchResponse>, Status> {
        let req = request.into_inner();
        info!(
            session_id = %req.session_id,
            query = %req.query,
            path = %req.path,
            regex = req.regex,
            include = %req.include,
            context_lines = req.context_lines,
            operation = "file_search",
            "Sandbox file search operation"
        );

        // Validate query
        // 校验查询
        if req.query.is_empty() {
            return Ok(Response::new(FileSearchResponse {
                success: false,
                error: "Search query cannot be empty".to_string(),
                ..Default::default()
            }));
        }
        if req.query.len() > 200 {
            return Ok(Response::new(FileSearchResponse {
                success: false,
                error: "Search query too long (max 200 chars)".to_string(),
                ..Default::default()
            }));
        }

        let context_lines = req.context_lines.clamp(0, 5) as usize;
        let max_results = if req.max_results > 0 { req.max_results as usize } else { 100 };

        let target = self.resolve_path(&req.session_id, "", &req.path)?;
        let workspace = self
            .workspace_mgr
            .get_workspace(&req.session_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid session: {}", e)))?;

        if !target.is_dir() {
            return Ok(Response::new(FileSearchResponse {
                success: false,
                error: "Path is not a directory".to_string(),
                ..Default::default()
            }));
        }

        // Build matcher
        // 构建匹配器
        let compiled_regex: Option<Regex> = if req.regex {
            match Regex::new(&format!("(?i){}", &req.query)) {
                Ok(r) => Some(r),
                Err(e) => {
                    return Ok(Response::new(FileSearchResponse {
                        success: false,
                        error: format!("Invalid regex: {}", e),
                        ..Default::default()
                    }));
                }
            }
        } else {
            None
        };
        let query_lower = req.query.to_lowercase();

        let mut matches = Vec::new();
        let mut files_scanned: i32 = 0;
        let mut truncated = false;

        const MAX_FILES: usize = 1000;
        const MAX_FILE_SIZE: u64 = 500 * 1024; // 500KB

        // Known binary extensions to skip
        // 需要跳过的已知二进制扩展名
        fn is_binary_ext(ext: &str) -> bool {
            matches!(
                ext,
                "png" | "jpg" | "jpeg" | "gif" | "bmp" | "ico" | "svg"
                    | "pdf" | "zip" | "tar" | "gz" | "bz2" | "xz" | "7z"
                    | "exe" | "dll" | "so" | "dylib" | "o" | "a"
                    | "wasm" | "pyc" | "class" | "jar"
                    | "mp3" | "mp4" | "avi" | "mov" | "wav"
                    | "ttf" | "otf" | "woff" | "woff2" | "eot"
                    | "sqlite" | "db"
            )
        }

        fn walk_and_search(
            dir: &std::path::Path,
            workspace: &std::path::Path,
            include_pattern: &str,
            compiled_regex: &Option<Regex>,
            query_lower: &str,
            context_lines: usize,
            max_results: usize,
            matches: &mut Vec<SearchMatch>,
            files_scanned: &mut i32,
            truncated: &mut bool,
        ) -> Result<(), std::io::Error> {
            if *files_scanned as usize >= MAX_FILES || matches.len() >= max_results {
                *truncated = true;
                return Ok(());
            }

            let entries = match std::fs::read_dir(dir) {
                Ok(e) => e,
                Err(_) => return Ok(()),
            };

            for entry in entries {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();

                // Skip hidden
                // 跳过隐藏文件
                if name.starts_with('.') {
                    continue;
                }

                let path = entry.path();
                let metadata = match entry.metadata() {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                if metadata.is_dir() {
                    walk_and_search(
                        &path,
                        workspace,
                        include_pattern,
                        compiled_regex,
                        query_lower,
                        context_lines,
                        max_results,
                        matches,
                        files_scanned,
                        truncated,
                    )?;
                    continue;
                }

                if !metadata.is_file() {
                    continue;
                }

                // Apply include glob filter on filename
                // 按文件名应用 include glob 过滤
                if !include_pattern.is_empty() && !glob_match(include_pattern, &name) {
                    continue;
                }

                // Skip binary extensions
                // 跳过二进制扩展名
                let ext = path.extension().and_then(|e| e.to_str()).unwrap_or("").to_lowercase();
                if is_binary_ext(&ext) {
                    continue;
                }

                // Skip large files
                // 跳过大文件
                if metadata.len() > MAX_FILE_SIZE {
                    continue;
                }

                *files_scanned += 1;
                if *files_scanned as usize > MAX_FILES {
                    *truncated = true;
                    return Ok(());
                }

                // Read file content
                // 读取文件内容
                let content = match std::fs::read_to_string(&path) {
                    Ok(c) => c,
                    Err(_) => continue, // skip non-UTF8 / unreadable
                };

                let lines: Vec<&str> = content.lines().collect();
                let relative = path
                    .strip_prefix(workspace)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .to_string();

                for (idx, line) in lines.iter().enumerate() {
                    let is_match = if let Some(ref re) = compiled_regex {
                        re.is_match(line)
                    } else {
                        line.to_lowercase().contains(query_lower)
                    };

                    if is_match {
                        // Collect context lines
                        // 收集 context 行
                        let mut context = Vec::new();
                        if context_lines > 0 {
                            let start = idx.saturating_sub(context_lines);
                            let end = std::cmp::min(idx + context_lines + 1, lines.len());
                            for ci in start..end {
                                if ci == idx {
                                    continue;
                                }
                                context.push(ContextLine {
                                    line: (ci + 1) as i32,
                                    content: lines[ci].to_string(),
                                });
                            }
                        }

                        matches.push(SearchMatch {
                            file: relative.clone(),
                            line: (idx + 1) as i32,
                            content: line.to_string(),
                            context,
                        });

                        if matches.len() >= max_results {
                            *truncated = true;
                            return Ok(());
                        }
                    }
                }
            }
            Ok(())
        }

        walk_and_search(
            &target,
            &workspace,
            &req.include,
            &compiled_regex,
            &query_lower,
            context_lines,
            max_results,
            &mut matches,
            &mut files_scanned,
            &mut truncated,
        )
        .map_err(|e| Status::internal(format!("Search error: {}", e)))?;

        let matches_found = matches.len() as i32;

        Ok(Response::new(FileSearchResponse {
            success: true,
            matches,
            error: String::new(),
            files_scanned,
            matches_found,
            truncated,
        }))
    }

    async fn file_edit(
        &self,
        request: Request<FileEditRequest>,
    ) -> Result<Response<FileEditResponse>, Status> {
        let req = request.into_inner();
        info!(
            session_id = %req.session_id,
            path = %req.path,
            replace_all = req.replace_all,
            operation = "file_edit",
            "Sandbox file edit operation"
        );

        let target = self.resolve_path(&req.session_id, "", &req.path)?;

        if !target.exists() {
            return Ok(Response::new(FileEditResponse {
                success: false,
                error: format!("File not found: {}. Use file_list(\".\") to discover correct file paths.", req.path),
                ..Default::default()
            }));
        }

        if !target.is_file() {
            return Ok(Response::new(FileEditResponse {
                success: false,
                error: format!("Path is a directory, not a file: {}", req.path),
                ..Default::default()
            }));
        }

        if req.old_text.is_empty() {
            return Ok(Response::new(FileEditResponse {
                success: false,
                error: "old_text cannot be empty".to_string(),
                ..Default::default()
            }));
        }

        let content = std::fs::read_to_string(&target)
            .map_err(|e| Status::internal(format!("Read error: {}", e)))?;

        let (new_content, replacements) = if req.replace_all {
            let count = content.matches(&req.old_text).count() as i32;
            (content.replace(&req.old_text, &req.new_text), count)
        } else {
            if let Some(pos) = content.find(&req.old_text) {
                let mut result = String::with_capacity(content.len());
                result.push_str(&content[..pos]);
                result.push_str(&req.new_text);
                result.push_str(&content[pos + req.old_text.len()..]);
                (result, 1)
            } else {
                return Ok(Response::new(FileEditResponse {
                    success: false,
                    error: "old_text not found in file".to_string(),
                    ..Default::default()
                }));
            }
        };

        if replacements == 0 {
            return Ok(Response::new(FileEditResponse {
                success: false,
                error: "old_text not found in file".to_string(),
                ..Default::default()
            }));
        }

        std::fs::write(&target, &new_content)
            .map_err(|e| Status::internal(format!("Write error: {}", e)))?;

        // Build a short snippet around the first replacement (char-boundary safe)
        // 在首个替换位置附近生成简短片段（字符边界安全）
        let snippet = if let Some(pos) = new_content.find(&req.new_text) {
            let raw_start = pos.saturating_sub(40);
            let raw_end = std::cmp::min(pos + req.new_text.len() + 40, new_content.len());
            // Find safe char boundaries to avoid panic on multi-byte UTF-8
            // 查找安全的字符边界，避免多字节 UTF-8 场景 panic
            let start = (0..=raw_start).rev()
                .find(|&i| new_content.is_char_boundary(i))
                .unwrap_or(0);
            let end = (raw_end..=new_content.len())
                .find(|&i| new_content.is_char_boundary(i))
                .unwrap_or(new_content.len());
            new_content[start..end].to_string()
        } else {
            String::new()
        };

        Ok(Response::new(FileEditResponse {
            success: true,
            error: String::new(),
            replacements,
            snippet,
            file_size_after: new_content.len() as i64,
        }))
    }

    async fn file_delete(
        &self,
        request: Request<FileDeleteRequest>,
    ) -> Result<Response<FileDeleteResponse>, Status> {
        let req = request.into_inner();
        info!(
            session_id = %req.session_id,
            path = %req.path,
            pattern = %req.pattern,
            recursive = req.recursive,
            operation = "file_delete",
            "Sandbox file delete operation"
        );

        // Hard-reject /memory/ paths
        // 硬性拒绝 /memory/ 路径
        if req.path.starts_with("/memory/") || req.path.starts_with("/memory")
            || req.path.starts_with("memory/") || req.path == "memory"
        {
            return Ok(Response::new(FileDeleteResponse {
                success: false,
                error: "Deleting from /memory/ is not allowed. file_delete only works in workspace.".to_string(),
                deleted_count: 0,
                deleted_paths: Vec::new(),
            }));
        }

        // Reject path traversal attempts
        // 拒绝路径穿越尝试
        if req.path.contains("..") {
            warn!(
                session_id = %req.session_id,
                path = %req.path,
                violation = "path_traversal",
                operation = "file_delete",
                "Security violation: path traversal attempt detected"
            );
            return Err(Status::permission_denied("Path traversal not allowed"));
        }

        let target = self.resolve_path(&req.session_id, "", &req.path)?;
        let workspace = self
            .workspace_mgr
            .get_workspace(&req.session_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid session: {}", e)))?;

        let pattern = req.pattern.as_str();

        // --- Mode 1: Single target (no pattern) ---
        // --- 模式 1：单个目标（无 pattern）---
        if pattern.is_empty() {
            if !target.exists() {
                return Ok(Response::new(FileDeleteResponse {
                    success: true,
                    error: String::new(),
                    deleted_count: 0,
                    deleted_paths: Vec::new(),
                }));
            }

            let relative = target
                .strip_prefix(&workspace)
                .unwrap_or(&target)
                .to_string_lossy()
                .to_string();

            if target.is_file() || target.is_symlink() {
                std::fs::remove_file(&target)
                    .map_err(|e| Status::internal(format!("Delete error: {}", e)))?;
                return Ok(Response::new(FileDeleteResponse {
                    success: true,
                    error: String::new(),
                    deleted_count: 1,
                    deleted_paths: vec![relative],
                }));
            }

            if target.is_dir() {
                match std::fs::remove_dir(&target) {
                    Ok(()) => {
                        return Ok(Response::new(FileDeleteResponse {
                            success: true,
                            error: String::new(),
                            deleted_count: 1,
                            deleted_paths: vec![relative],
                        }));
                    }
                    Err(e) => {
                        return Ok(Response::new(FileDeleteResponse {
                            success: false,
                            error: format!("Directory not empty or cannot delete: {}", e),
                            deleted_count: 0,
                            deleted_paths: Vec::new(),
                        }));
                    }
                }
            }

            return Ok(Response::new(FileDeleteResponse {
                success: false,
                error: format!("Unknown file type at path: {}", req.path),
                deleted_count: 0,
                deleted_paths: Vec::new(),
            }));
        }

        // --- Mode 2: Glob pattern ---
        // --- 模式 2：glob 模式 ---
        if !target.is_dir() {
            return Ok(Response::new(FileDeleteResponse {
                success: false,
                error: format!("Path must be a directory when using pattern, got: {}", req.path),
                deleted_count: 0,
                deleted_paths: Vec::new(),
            }));
        }

        let mut deleted_paths = Vec::new();
        let mut errors = Vec::new();

        fn collect_and_delete(
            dir: &std::path::Path,
            canonical_workspace: &std::path::Path,
            pattern: &str,
            recursive: bool,
            deleted_paths: &mut Vec<String>,
            errors: &mut Vec<String>,
        ) -> Result<(), std::io::Error> {
            for entry in std::fs::read_dir(dir)? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().to_string();
                let path = entry.path();
                let sym_metadata = match path.symlink_metadata() {
                    Ok(m) => m,
                    Err(_) => continue,
                };

                // Use path.metadata() (follows symlinks) to detect symlink-to-directory.
                // 用 path.metadata()（会跟随符号链接）识别指向目录的符号链接。
                // symlink_metadata().is_dir() is always false for symlinks themselves.
                // 对符号链接本身，symlink_metadata().is_dir() 恒为 false。
                let is_dir = if sym_metadata.is_symlink() {
                    path.metadata().map(|m| m.is_dir()).unwrap_or(false)
                } else {
                    sym_metadata.is_dir()
                };

                if is_dir && recursive {
                    // Symlink escape prevention: canonicalize before recursing
                    // 防止符号链接逃逸：递归前先 canonicalize，
                    // and verify the resolved path is still within the workspace.
                    // 并校验解析后的路径仍位于工作区内。
                    if sym_metadata.is_symlink() {
                        match path.canonicalize() {
                            Ok(canonical) if canonical.starts_with(canonical_workspace) => {
                                collect_and_delete(&canonical, canonical_workspace, pattern, recursive, deleted_paths, errors)?;
                            }
                            Ok(canonical) => {
                                errors.push(format!("{}: symlink escapes workspace (-> {})", path.display(), canonical.display()));
                                continue;
                            }
                            Err(e) => {
                                errors.push(format!("{}: cannot resolve symlink: {}", path.display(), e));
                                continue;
                            }
                        }
                    } else {
                        collect_and_delete(&path, canonical_workspace, pattern, recursive, deleted_paths, errors)?;
                    }
                    if glob_match(pattern, &name) {
                        if let Ok(()) = std::fs::remove_dir(&path) {
                            let relative = path
                                .strip_prefix(canonical_workspace)
                                .unwrap_or(&path)
                                .to_string_lossy()
                                .to_string();
                            deleted_paths.push(relative);
                        }
                    }
                    continue;
                }

                if !glob_match(pattern, &name) {
                    continue;
                }

                let relative = path
                    .strip_prefix(canonical_workspace)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .to_string();

                if sym_metadata.is_file() || sym_metadata.is_symlink() {
                    match std::fs::remove_file(&path) {
                        Ok(()) => deleted_paths.push(relative),
                        Err(e) => errors.push(format!("{}: {}", relative, e)),
                    }
                } else if is_dir {
                    match std::fs::remove_dir(&path) {
                        Ok(()) => deleted_paths.push(relative),
                        Err(e) => errors.push(format!("{}: {}", relative, e)),
                    }
                }
            }
            Ok(())
        }

        // Canonicalize workspace for symlink containment checks
        // 规范化工作区路径，用于符号链接包含性校验
        let canonical_workspace = workspace.canonicalize()
            .map_err(|e| Status::internal(format!("Workspace canonicalize error: {}", e)))?;

        collect_and_delete(&target, &canonical_workspace, pattern, req.recursive, &mut deleted_paths, &mut errors)
            .map_err(|e| Status::internal(format!("Delete walk error: {}", e)))?;

        let deleted_count = deleted_paths.len() as i32;
        let error_msg = if errors.is_empty() {
            String::new()
        } else {
            format!("Some deletions failed: {}", errors.join("; "))
        };

        Ok(Response::new(FileDeleteResponse {
            success: errors.is_empty(),
            error: error_msg,
            deleted_count,
            deleted_paths,
        }))
    }
}

/// Simple glob pattern matching (no regex to avoid DoS).
/// 简单的 glob 模式匹配（不用正则，避免 DoS）。
fn glob_match(pattern: &str, name: &str) -> bool {
    if pattern.is_empty() {
        return true;
    }
    glob_match_recursive(pattern.as_bytes(), name.as_bytes())
}

/// Recursive glob matcher without regex (prevents ReDoS attacks).
/// 不使用正则的递归 glob 匹配器（防止 ReDoS 攻击）。
fn glob_match_recursive(pattern: &[u8], name: &[u8]) -> bool {
    match (pattern.first(), name.first()) {
        (None, None) => true,
        (Some(b'*'), _) => {
            // '*' matches zero or more characters
            // '*' 匹配零个或多个字符
            glob_match_recursive(&pattern[1..], name)
                || (!name.is_empty() && glob_match_recursive(pattern, &name[1..]))
        }
        (Some(b'?'), Some(_)) => {
            // '?' matches exactly one character
            // '?' 精确匹配一个字符
            glob_match_recursive(&pattern[1..], &name[1..])
        }
        (Some(p), Some(n)) if *p == *n => glob_match_recursive(&pattern[1..], &name[1..]),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_file_read_write_roundtrip() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());

        // Write
        // 写入
        let write_req = FileWriteRequest {
            session_id: "test-session".to_string(),
            path: "hello.txt".to_string(),
            content: "Hello, World!".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let write_resp = service
            .file_write(tonic::Request::new(write_req))
            .await
            .unwrap();
        assert!(write_resp.into_inner().success);

        // Read
        // 读取
        let read_req = FileReadRequest {
            session_id: "test-session".to_string(),
            path: "hello.txt".to_string(),
            max_bytes: 0,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let read_resp = service
            .file_read(tonic::Request::new(read_req))
            .await
            .unwrap();
        let inner = read_resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.content, "Hello, World!");
    }

    #[tokio::test]
    async fn test_file_list() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());

        // Create files by getting workspace first
        // 先获取工作区再创建文件
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("file1.txt"), "content1").unwrap();
        std::fs::write(workspace.join("file2.txt"), "content2").unwrap();
        std::fs::create_dir(workspace.join("subdir")).unwrap();

        let req = FileListRequest {
            session_id: "test-session".to_string(),
            path: "".to_string(),
            pattern: "".to_string(),
            recursive: false,
            include_hidden: false,
            user_id: String::new(),
        };
        let resp = service.file_list(tonic::Request::new(req)).await.unwrap();
        let inner = resp.into_inner();

        assert!(inner.success);
        assert_eq!(inner.file_count, 2);
        assert_eq!(inner.dir_count, 1);
    }

    #[tokio::test]
    async fn test_execute_command() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());

        // Create workspace with file
        // 创建工作区并放入文件
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("test.txt"), "hello world").unwrap();

        let req = CommandRequest {
            session_id: "test-session".to_string(),
            command: "cat test.txt".to_string(),
            timeout_seconds: 5,
            user_id: String::new(),
        };
        let resp = service
            .execute_command(tonic::Request::new(req))
            .await
            .unwrap();
        let inner = resp.into_inner();

        assert!(inner.success);
        assert_eq!(inner.stdout, "hello world");
    }

    #[tokio::test]
    async fn test_session_isolation() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());

        // Write to session A
        // 写入 session A
        let write_req = FileWriteRequest {
            session_id: "session-a".to_string(),
            path: "secret.txt".to_string(),
            content: "Session A secret".to_string(),
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        service
            .file_write(tonic::Request::new(write_req))
            .await
            .unwrap();

        // Try to read from session B with path traversal
        // 尝试用路径穿越从 session B 读取
        let read_req = FileReadRequest {
            session_id: "session-b".to_string(),
            path: "../session-a/secret.txt".to_string(),
            max_bytes: 0,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp = service.file_read(tonic::Request::new(read_req)).await;

        // Should fail - path escapes workspace
        // 应当失败——路径逃出了工作区
        match resp {
            Ok(r) => {
                let inner = r.into_inner();
                assert!(!inner.success || inner.content.is_empty());
            }
            Err(e) => assert!(
                e.code() == tonic::Code::PermissionDenied || e.code() == tonic::Code::NotFound
            ),
        }
    }

    #[tokio::test]
    async fn test_quota_enforcement() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = SandboxConfig {
            max_workspace_bytes: 100, // Very small quota for testing
            ..Default::default()
        };
        let service = SandboxServiceImpl::with_config(temp.path().to_path_buf(), config);

        // Write that exceeds quota
        // 写入超过配额的数据
        let write_req = FileWriteRequest {
            session_id: "test-session".to_string(),
            path: "large.txt".to_string(),
            content: "x".repeat(200), // 200 bytes, exceeds 100 byte quota
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: String::new(),
        };
        let resp = service.file_write(tonic::Request::new(write_req)).await;

        // Should fail due to quota
        // 应当因超出配额而失败
        assert!(resp.is_err());
        let err = resp.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn test_memory_quota_enforcement() {
        let temp = tempfile::TempDir::new().unwrap();
        let config = SandboxConfig {
            max_memory_bytes: 100, // Very small memory quota for testing
            ..Default::default()
        };
        let service = SandboxServiceImpl::with_config(temp.path().to_path_buf(), config);

        // Write to /memory/ path that exceeds memory quota
        // 写入超过记忆配额的 /memory/ 路径
        let write_req = FileWriteRequest {
            session_id: "test-session".to_string(),
            path: "/memory/notes.txt".to_string(),
            content: "x".repeat(200), // 200 bytes, exceeds 100 byte memory quota
            append: false,
            create_dirs: false,
            encoding: "utf-8".to_string(),
            user_id: "test-memory-quota-user".to_string(),
        };
        let resp = service.file_write(tonic::Request::new(write_req)).await;

        // Should fail due to memory quota (not workspace quota)
        // 应当因记忆配额（而非工作区配额）失败
        assert!(resp.is_err());
        let err = resp.unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert!(
            err.message().contains("Memory quota"),
            "Expected memory quota error, got: {}",
            err.message()
        );
    }

    #[tokio::test]
    async fn test_dangerous_command_rejected() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());

        let req = CommandRequest {
            session_id: "test-session".to_string(),
            command: "curl http://evil.com".to_string(),
            timeout_seconds: 5,
            user_id: String::new(),
        };
        let resp = service
            .execute_command(tonic::Request::new(req))
            .await
            .unwrap();
        let inner = resp.into_inner();

        assert!(!inner.success);
        assert!(inner.error.contains("not allowed"));
    }

    #[tokio::test]
    async fn test_file_delete_single_file() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("to_delete.txt"), "delete me").unwrap();

        let req = FileDeleteRequest {
            session_id: "test-session".to_string(),
            path: "to_delete.txt".to_string(),
            pattern: String::new(),
            recursive: false,
        };
        let resp = service.file_delete(tonic::Request::new(req)).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.deleted_count, 1);
        assert_eq!(inner.deleted_paths, vec!["to_delete.txt"]);
        assert!(!workspace.join("to_delete.txt").exists());
    }

    #[tokio::test]
    async fn test_file_delete_empty_dir() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(workspace.join("empty_dir")).unwrap();

        let req = FileDeleteRequest {
            session_id: "test-session".to_string(),
            path: "empty_dir".to_string(),
            pattern: String::new(),
            recursive: false,
        };
        let resp = service.file_delete(tonic::Request::new(req)).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.deleted_count, 1);
        assert!(!workspace.join("empty_dir").exists());
    }

    #[tokio::test]
    async fn test_file_delete_non_empty_dir_rejected() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(workspace.join("non_empty")).unwrap();
        std::fs::write(workspace.join("non_empty/file.txt"), "content").unwrap();

        let req = FileDeleteRequest {
            session_id: "test-session".to_string(),
            path: "non_empty".to_string(),
            pattern: String::new(),
            recursive: false,
        };
        let resp = service.file_delete(tonic::Request::new(req)).await.unwrap();
        let inner = resp.into_inner();
        assert!(!inner.success);
        assert!(inner.error.contains("not empty") || inner.error.contains("Directory"));
        assert!(workspace.join("non_empty").exists());
    }

    #[tokio::test]
    async fn test_file_delete_not_found_idempotent() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(&workspace).unwrap();

        let req = FileDeleteRequest {
            session_id: "test-session".to_string(),
            path: "nonexistent.txt".to_string(),
            pattern: String::new(),
            recursive: false,
        };
        let resp = service.file_delete(tonic::Request::new(req)).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.deleted_count, 0);
    }

    #[tokio::test]
    async fn test_file_delete_memory_path_rejected() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());

        let req = FileDeleteRequest {
            session_id: "test-session".to_string(),
            path: "/memory/notes.md".to_string(),
            pattern: String::new(),
            recursive: false,
        };
        let resp = service.file_delete(tonic::Request::new(req)).await.unwrap();
        let inner = resp.into_inner();
        assert!(!inner.success);
        assert!(inner.error.to_lowercase().contains("memory"));
    }

    #[tokio::test]
    async fn test_file_delete_glob_pattern() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(&workspace).unwrap();
        std::fs::write(workspace.join("a.tmp"), "temp1").unwrap();
        std::fs::write(workspace.join("b.tmp"), "temp2").unwrap();
        std::fs::write(workspace.join("keep.txt"), "keep").unwrap();

        let req = FileDeleteRequest {
            session_id: "test-session".to_string(),
            path: ".".to_string(),
            pattern: "*.tmp".to_string(),
            recursive: false,
        };
        let resp = service.file_delete(tonic::Request::new(req)).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.deleted_count, 2);
        assert!(!workspace.join("a.tmp").exists());
        assert!(!workspace.join("b.tmp").exists());
        assert!(workspace.join("keep.txt").exists());
    }

    #[tokio::test]
    async fn test_file_delete_glob_recursive() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(workspace.join("sub")).unwrap();
        std::fs::write(workspace.join("a.tmp"), "temp1").unwrap();
        std::fs::write(workspace.join("sub/b.tmp"), "temp2").unwrap();
        std::fs::write(workspace.join("sub/keep.txt"), "keep").unwrap();

        let req = FileDeleteRequest {
            session_id: "test-session".to_string(),
            path: ".".to_string(),
            pattern: "*.tmp".to_string(),
            recursive: true,
        };
        let resp = service.file_delete(tonic::Request::new(req)).await.unwrap();
        let inner = resp.into_inner();
        assert!(inner.success);
        assert_eq!(inner.deleted_count, 2);
        assert!(!workspace.join("a.tmp").exists());
        assert!(!workspace.join("sub/b.tmp").exists());
        assert!(workspace.join("sub/keep.txt").exists());
    }

    #[tokio::test]
    async fn test_file_delete_path_traversal_rejected() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(&workspace).unwrap();

        let req = FileDeleteRequest {
            session_id: "test-session".to_string(),
            path: "../../../etc/passwd".to_string(),
            pattern: String::new(),
            recursive: false,
        };
        let resp = service.file_delete(tonic::Request::new(req)).await;
        assert!(resp.is_err());
    }

    /// Verify that collect_and_delete detects symlinks pointing to directories
    /// 校验 collect_and_delete 能检测出指向工作区外目录的符号链接
    /// outside the workspace (symlink escape via symlink-to-directory).
    /// （即通过符号链接指向目录实现的工作区逃逸）。
    ///
    /// Before the fix, symlink_metadata().is_dir() was always false for symlinks,
    /// 修复前，symlink_metadata().is_dir() 对符号链接恒为 false，
    /// so the escape-check branch was dead code and the symlink was silently skipped.
    /// 导致逃逸检查分支成为死代码，符号链接被静默跳过。
    #[cfg(unix)]
    #[tokio::test]
    async fn test_file_delete_symlink_to_external_dir_detected() {
        let temp = tempfile::TempDir::new().unwrap();
        let service = SandboxServiceImpl::new(temp.path().to_path_buf());
        let workspace = temp.path().join("test-session");
        std::fs::create_dir_all(&workspace).unwrap();

        // Create an external directory with a sensitive file (outside workspace).
        // 在工作区外创建包含敏感文件的外部目录。
        let external_dir = temp.path().join("external");
        std::fs::create_dir_all(&external_dir).unwrap();
        std::fs::write(external_dir.join("secret.txt"), "secret").unwrap();

        // Create a symlink inside the workspace pointing to the external directory.
        // 在工作区内创建指向该外部目录的符号链接。
        let symlink_path = workspace.join("escape_link");
        std::os::unix::fs::symlink(&external_dir, &symlink_path).unwrap();

        // Attempt a recursive glob delete starting from the workspace root.
        // 从项目根目录尝试递归 glob 删除。
        // With the bug, the symlink-to-dir escape check was dead code and the
        // 存在该 bug 时，符号链接指向目录的逃逸检查是死代码，
        // symlink would be silently skipped (no error, no deletion).
        // 符号链接会被静默跳过（既不报错也不删除）。
        // With the fix, collect_and_delete must detect the escape and report an error.
        // 修复后，collect_and_delete 必须检测到逃逸并返回错误。
        let req = FileDeleteRequest {
            session_id: "test-session".to_string(),
            path: ".".to_string(),
            pattern: "*".to_string(),
            recursive: true,
        };
        let resp = service.file_delete(tonic::Request::new(req)).await.unwrap();
        let inner = resp.into_inner();

        // The response should report the symlink escape in its error field.
        // response 应在 error 字段中报告符号链接逃逸。
        assert!(
            !inner.error.is_empty(),
            "Expected symlink escape error, but error field was empty. \
             success={}, deleted_paths={:?}",
            inner.success,
            inner.deleted_paths,
        );
        assert!(
            inner.error.contains("symlink escapes workspace"),
            "Error message should mention symlink escape, got: {}",
            inner.error,
        );

        // The external directory must not have been modified.
        // 外部目录必须未被改动。
        assert!(
            external_dir.join("secret.txt").exists(),
            "External file should not have been deleted",
        );
    }
}
