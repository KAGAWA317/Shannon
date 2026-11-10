//! Session workspace management for WASI sandbox isolation.
//! 面向 WASI sandbox 隔离的 session 工作区管理。
//!
//! Each session gets an isolated directory that can be mounted
//! 每个 session 拥有独立目录，可挂载到
//! into WASI sandboxes with read-write permissions.
//! WASI sandbox 并赋予读写权限。

use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// Manages per-session workspace directories.
/// 管理每个 session 的工作区目录。
pub struct WorkspaceManager {
    base_dir: PathBuf,
}

impl WorkspaceManager {
    /// Create a new workspace manager with the given base directory.
    /// 使用给定根目录创建工作区管理器。
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Create from environment variable or default.
    /// 从环境变量或默认值创建。
    pub fn from_env() -> Self {
        let base = std::env::var("SHANNON_SESSION_WORKSPACES_DIR")
            .unwrap_or_else(|_| "/tmp/shannon-sessions".to_string());
        Self::new(PathBuf::from(base))
    }

    /// Get the base directory.
    /// 返回根目录。
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Validate session ID format (alphanumeric + hyphen only).
    /// 校验 session ID 格式（仅允许字母数字与连字符）。
    fn validate_session_id(session_id: &str) -> Result<()> {
        if session_id.is_empty() {
            return Err(anyhow!("Session ID cannot be empty"));
        }
        if session_id.len() > 128 {
            return Err(anyhow!("Session ID too long (max 128 chars)"));
        }
        if !session_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.')
        {
            return Err(anyhow!(
                "Session ID must contain only alphanumeric, hyphen, or underscore"
            ));
        }
        // Reject path traversal attempts
        // 拒绝路径穿越尝试
        if session_id.contains("..") || session_id.starts_with('.') {
            return Err(anyhow!("Session ID cannot contain path traversal"));
        }
        Ok(())
    }

    /// Get or create workspace directory for a session.
    /// 获取或创建 session 的工作区目录。
    ///
    /// Security: Validates that the workspace path doesn't escape the base directory
    /// 安全：校验工作区路径不会逃出根目录，
    /// by checking the canonical path BEFORE and AFTER creation to prevent TOCTOU attacks.
    /// 在创建前后都检查规范化路径，以防止 TOCTOU 攻击。
    pub fn get_workspace(&self, session_id: &str) -> Result<PathBuf> {
        Self::validate_session_id(session_id)?;

        // Canonicalize base_dir first to establish trusted root
        // 先规范化根目录，建立可信基准
        let canonical_base = if self.base_dir.exists() {
            self.base_dir.canonicalize()?
        } else {
            std::fs::create_dir_all(&self.base_dir)?;
            self.base_dir.canonicalize()?
        };

        // Construct workspace path within canonical base (prevents symlink injection)
        // 在规范化根目录下拼接工作区路径（防止符号链接注入）
        let workspace = canonical_base.join(session_id);

        // Pre-creation check: Verify the path we're about to create is within base
        // 创建前检查：确认即将创建的路径位于根目录内
        // This catches cases where session_id might be manipulated
        // 用于捕获 session_id 被篡改的情况
        if !workspace.starts_with(&canonical_base) {
            return Err(anyhow!("Workspace path escapes base directory"));
        }

        // Check if workspace already exists
        // 检查工作区是否已存在
        if workspace.exists() {
            // Verify it's a directory, not a symlink to outside
            // 确认它是目录，而非指向外部的符号链接
            let metadata = std::fs::symlink_metadata(&workspace)?;
            if metadata.file_type().is_symlink() {
                return Err(anyhow!("Workspace is a symlink (potential attack)"));
            }
            if !metadata.is_dir() {
                return Err(anyhow!("Workspace path exists but is not a directory"));
            }
        } else {
            // Create workspace directory (handle race: another request may create it first)
            // 创建工作区目录（处理竞态：其他 request 可能先创建）
            match std::fs::create_dir(&workspace) {
                Ok(_) => info!("Created workspace for session: {}", session_id),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Race condition: another request created it between exists() and create_dir()
                    // 竞态：其他 request 在 exists() 与 create_dir() 之间创建了该目录
                    let metadata = std::fs::symlink_metadata(&workspace)?;
                    if metadata.file_type().is_symlink() {
                        return Err(anyhow!("Workspace is a symlink (potential attack)"));
                    }
                    if !metadata.is_dir() {
                        return Err(anyhow!("Workspace path exists but is not a directory"));
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Post-creation verification (defense in depth)
        // 创建后的复核（纵深防御）
        let canonical = workspace.canonicalize()?;
        if !canonical.starts_with(&canonical_base) {
            // This should never happen if pre-creation checks passed
            // 若创建前检查已通过，理论上不会走到这里
            // but we check anyway for defense in depth
            // 仍做检查以实现纵深防御
            std::fs::remove_dir(&workspace).ok(); // Cleanup
            return Err(anyhow!(
                "Workspace path escapes base directory after creation"
            ));
        }

        debug!("Workspace path for {}: {:?}", session_id, canonical);
        Ok(canonical)
    }

    /// Check if a path is within a session's workspace.
    /// 判断路径是否位于 session 工作区内。
    pub fn is_within_workspace(&self, session_id: &str, path: &Path) -> Result<bool> {
        let workspace = self.get_workspace(session_id)?;

        // Handle both existing and non-existing paths
        // 同时处理已存在和不存在的路径
        let check_path = if path.exists() {
            path.canonicalize()?
        } else {
            // For non-existing paths, canonicalize the parent and join
            // 对不存在的路径，规范化其父目录后再拼接
            let parent = path.parent().ok_or_else(|| anyhow!("Invalid path"))?;
            if parent.exists() {
                parent
                    .canonicalize()?
                    .join(path.file_name().unwrap_or_default())
            } else {
                return Ok(false);
            }
        };

        Ok(check_path.starts_with(&workspace))
    }

    /// Get workspace size in bytes.
    /// 返回工作区大小，单位字节。
    pub fn get_workspace_size(&self, session_id: &str) -> Result<u64> {
        let workspace = self.get_workspace(session_id)?;
        dir_size(&workspace)
    }

    /// Delete a session's workspace.
    /// 删除 session 的工作区。
    pub fn delete_workspace(&self, session_id: &str) -> Result<()> {
        Self::validate_session_id(session_id)?;
        let workspace = self.base_dir.join(session_id);

        if workspace.exists() {
            std::fs::remove_dir_all(&workspace)?;
            info!("Deleted workspace for session: {}", session_id);
        }
        Ok(())
    }

    /// List all session workspaces.
    /// 列出所有 session 工作区。
    pub fn list_workspaces(&self) -> Result<Vec<String>> {
        let mut sessions = Vec::new();

        if !self.base_dir.exists() {
            return Ok(sessions);
        }

        for entry in std::fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                if let Some(name) = entry.file_name().to_str() {
                    sessions.push(name.to_string());
                }
            }
        }
        Ok(sessions)
    }
}

/// Maximum number of entries to visit during a size scan to prevent DoS
/// 大小扫描时访问条目数的上限，防止
/// via deeply nested or symlink-looped directory trees.
/// 深层嵌套或符号链接成环的目录树造成 DoS。
const MAX_DIR_WALK_ENTRIES: usize = 50_000;

/// Calculate total size of a directory recursively.
/// 递归计算目录总大小。
///
/// Security: Skips symlinks entirely to prevent escape outside the workspace
/// 安全：完全跳过符号链接，防止逃出工作区，
/// and caps the total number of visited entries to avoid DoS from deeply nested trees.
/// 并限制访问条目总数，避免深层嵌套目录树导致 DoS。
fn dir_size(path: &Path) -> Result<u64> {
    let mut size = 0u64;
    let mut remaining = MAX_DIR_WALK_ENTRIES;
    dir_size_inner(path, &mut size, &mut remaining)?;
    Ok(size)
}

fn dir_size_inner(path: &Path, size: &mut u64, remaining: &mut usize) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }

    for entry in std::fs::read_dir(path)? {
        if *remaining == 0 {
            return Err(anyhow!(
                "Workspace walk exceeded {} entries",
                MAX_DIR_WALK_ENTRIES
            ));
        }
        *remaining = remaining.saturating_sub(1);

        let entry = entry?;
        // Use symlink_metadata to avoid following symlinks
        // 使用 symlink_metadata，避免跟随符号链接
        let metadata = entry.metadata().ok();
        let sym_meta = std::fs::symlink_metadata(entry.path()).ok();

        // Skip symlinks entirely (prevents escape + infinite loops)
        // 完全跳过符号链接（防止逃逸和死循环）
        if sym_meta.is_some_and(|m| m.file_type().is_symlink()) {
            continue;
        }

        if let Some(meta) = metadata {
            if meta.is_file() {
                *size += meta.len();
            } else if meta.is_dir() {
                dir_size_inner(&entry.path(), size, remaining)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_workspace_creation() {
        let temp = TempDir::new().unwrap();
        let mgr = WorkspaceManager::new(temp.path().to_path_buf());

        let ws = mgr.get_workspace("test-session-123").unwrap();
        assert!(ws.exists());
        assert!(ws.is_dir());
        assert!(ws.ends_with("test-session-123"));
    }

    #[test]
    fn test_invalid_session_id_rejected() {
        let temp = TempDir::new().unwrap();
        let mgr = WorkspaceManager::new(temp.path().to_path_buf());

        assert!(mgr.get_workspace("../escape").is_err());
        assert!(mgr.get_workspace("session;rm -rf").is_err());
        assert!(mgr.get_workspace("").is_err());
    }

    #[test]
    fn test_session_isolation() {
        let temp = TempDir::new().unwrap();
        let mgr = WorkspaceManager::new(temp.path().to_path_buf());

        let ws_a = mgr.get_workspace("session-a").unwrap();
        let ws_b = mgr.get_workspace("session-b").unwrap();

        assert_ne!(ws_a, ws_b);
        assert!(ws_a.ends_with("session-a"));
        assert!(ws_b.ends_with("session-b"));
    }

    #[test]
    fn test_path_within_workspace() {
        let temp = TempDir::new().unwrap();
        let mgr = WorkspaceManager::new(temp.path().to_path_buf());

        // Create workspace and a file
        // 创建工作区及一个文件
        let ws = mgr.get_workspace("test-session").unwrap();
        let test_file = ws.join("test.txt");
        std::fs::write(&test_file, "hello").unwrap();

        assert!(mgr.is_within_workspace("test-session", &test_file).unwrap());
        assert!(!mgr
            .is_within_workspace("test-session", Path::new("/etc/passwd"))
            .unwrap());
    }

    #[test]
    fn test_workspace_size() {
        let temp = TempDir::new().unwrap();
        let mgr = WorkspaceManager::new(temp.path().to_path_buf());

        let ws = mgr.get_workspace("test-session").unwrap();
        std::fs::write(ws.join("file1.txt"), "hello").unwrap();
        std::fs::write(ws.join("file2.txt"), "world").unwrap();

        let size = mgr.get_workspace_size("test-session").unwrap();
        assert_eq!(size, 10); // "hello" + "world"
    }

    #[test]
    fn test_delete_workspace() {
        let temp = TempDir::new().unwrap();
        let mgr = WorkspaceManager::new(temp.path().to_path_buf());

        let ws = mgr.get_workspace("to-delete").unwrap();
        std::fs::write(ws.join("file.txt"), "data").unwrap();
        assert!(ws.exists());

        mgr.delete_workspace("to-delete").unwrap();
        assert!(!ws.exists());
    }

    #[test]
    fn test_list_workspaces() {
        let temp = TempDir::new().unwrap();
        let mgr = WorkspaceManager::new(temp.path().to_path_buf());

        mgr.get_workspace("session-a").unwrap();
        mgr.get_workspace("session-b").unwrap();
        mgr.get_workspace("session-c").unwrap();

        let mut workspaces = mgr.list_workspaces().unwrap();
        workspaces.sort();

        assert_eq!(workspaces, vec!["session-a", "session-b", "session-c"]);
    }
}
