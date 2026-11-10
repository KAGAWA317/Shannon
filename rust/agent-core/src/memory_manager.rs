//! User memory directory management for persistent cross-session storage.
//! 用户记忆目录管理，用于跨 session 持久化存储。
//!
//! Each user gets an isolated memory directory that persists across sessions.
//! 每个用户拥有独立的记忆目录，可跨 session 保留。
//! This can be mounted into WASI sandboxes at `/memory` with read-write permissions.
//! 该目录可挂载到 WASI sandbox 的 `/memory` 并赋予读写权限。

use anyhow::{anyhow, Result};
use std::path::{Path, PathBuf};
use tracing::{debug, info};

/// Manages per-user memory directories.
/// 管理每个用户的记忆目录。
pub struct MemoryManager {
    base_dir: PathBuf,
}

impl MemoryManager {
    /// Create a new memory manager with the given base directory.
    /// 使用给定根目录创建记忆管理器。
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Create from environment variable or default.
    /// 从环境变量或默认值创建。
    pub fn from_env() -> Self {
        let base = std::env::var("SHANNON_USER_MEMORY_DIR")
            .unwrap_or_else(|_| "/tmp/shannon-users".to_string());
        Self::new(PathBuf::from(base))
    }

    /// Get the base directory.
    /// 返回根目录。
    pub fn base_dir(&self) -> &Path {
        &self.base_dir
    }

    /// Validate user ID format (alphanumeric + hyphen + underscore only).
    /// 校验用户 ID 格式（仅允许字母数字、连字符和下划线）。
    fn validate_user_id(user_id: &str) -> Result<()> {
        if user_id.is_empty() {
            return Err(anyhow!("User ID cannot be empty"));
        }
        if user_id.len() > 128 {
            return Err(anyhow!("User ID too long (max 128 chars)"));
        }
        if !user_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(anyhow!(
                "User ID must contain only alphanumeric, hyphen, or underscore"
            ));
        }
        // Reject path traversal attempts
        // 拒绝路径穿越尝试
        if user_id.contains("..") || user_id.starts_with('.') {
            return Err(anyhow!("User ID cannot contain path traversal"));
        }
        Ok(())
    }

    /// Get or create memory directory for a user.
    /// 获取或创建用户的记忆目录。
    ///
    /// Returns `{base_dir}/{user_id}/memory/`.
    /// 返回 `{base_dir}/{user_id}/memory/`。
    ///
    /// Security: Validates that the memory path doesn't escape the base directory
    /// 安全：校验记忆路径不会逃出根目录，
    /// by checking the canonical path BEFORE and AFTER creation to prevent TOCTOU attacks.
    /// 在创建前后都检查规范化路径，以防止 TOCTOU 攻击。
    pub fn get_memory_dir(&self, user_id: &str) -> Result<PathBuf> {
        Self::validate_user_id(user_id)?;

        // Canonicalize base_dir first to establish trusted root
        // 先规范化根目录，建立可信基准
        let canonical_base = if self.base_dir.exists() {
            self.base_dir.canonicalize()?
        } else {
            std::fs::create_dir_all(&self.base_dir)?;
            self.base_dir.canonicalize()?
        };

        // Construct memory path: {base}/{user_id}/memory/
        // 拼接记忆路径：{base}/{user_id}/memory/
        let user_dir = canonical_base.join(user_id);
        let memory_dir = user_dir.join("memory");

        // Pre-creation check: Verify the path we're about to create is within base
        // 创建前检查：确认即将创建的路径位于根目录内
        if !memory_dir.starts_with(&canonical_base) {
            return Err(anyhow!("Memory path escapes base directory"));
        }

        // Create the full directory hierarchy if needed
        // 按需创建完整目录层级
        if memory_dir.exists() {
            // Verify it's a directory, not a symlink to outside
            // 确认它是目录，而非指向外部的符号链接
            let metadata = std::fs::symlink_metadata(&memory_dir)?;
            if metadata.file_type().is_symlink() {
                return Err(anyhow!("Memory directory is a symlink (potential attack)"));
            }
            if !metadata.is_dir() {
                return Err(anyhow!("Memory path exists but is not a directory"));
            }
        } else {
            // Create user_dir and memory subdirectory
            // 创建用户目录及其 memory 子目录
            match std::fs::create_dir_all(&memory_dir) {
                Ok(_) => info!("Created memory directory for user: {}", user_id),
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // Race condition: another request created it
                    // 竞态：其他 request 已创建该目录
                    let metadata = std::fs::symlink_metadata(&memory_dir)?;
                    if metadata.file_type().is_symlink() {
                        return Err(anyhow!("Memory directory is a symlink (potential attack)"));
                    }
                    if !metadata.is_dir() {
                        return Err(anyhow!("Memory path exists but is not a directory"));
                    }
                }
                Err(e) => return Err(e.into()),
            }
        }

        // Post-creation verification (defense in depth)
        // 创建后的复核（纵深防御）
        let canonical = memory_dir.canonicalize()?;
        if !canonical.starts_with(&canonical_base) {
            std::fs::remove_dir_all(&memory_dir).ok(); // Cleanup
            return Err(anyhow!("Memory path escapes base directory after creation"));
        }

        debug!("Memory path for {}: {:?}", user_id, canonical);
        Ok(canonical)
    }

    /// Get size of a user's memory directory in bytes.
    /// 返回用户记忆目录大小，单位字节。
    pub fn get_memory_size(&self, user_id: &str) -> Result<u64> {
        let memory_dir = self.get_memory_dir(user_id)?;
        dir_size(&memory_dir)
    }

    /// Check if a path is within a user's memory directory.
    /// 判断路径是否位于用户记忆目录内。
    pub fn is_within_memory(&self, user_id: &str, path: &Path) -> Result<bool> {
        let memory_dir = self.get_memory_dir(user_id)?;

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

        Ok(check_path.starts_with(&memory_dir))
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
/// Skips symlinks entirely to avoid escape/infinite-loop risks.
/// 完全跳过符号链接，避免逃逸和死循环风险。
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
                "Memory scan exceeded {} entries",
                MAX_DIR_WALK_ENTRIES
            ));
        }
        *remaining = remaining.saturating_sub(1);

        let entry = entry?;
        let symlink_meta = std::fs::symlink_metadata(entry.path()).ok();

        // Skip symlinks entirely (prevents escape + infinite loops)
        // 完全跳过符号链接（防止逃逸和死循环）
        if symlink_meta.is_some_and(|m| m.file_type().is_symlink()) {
            continue;
        }

        let metadata = entry.metadata().ok();
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
    fn test_memory_dir_creation() {
        let temp = TempDir::new().unwrap();
        let mgr = MemoryManager::new(temp.path().to_path_buf());

        let dir = mgr.get_memory_dir("user-123").unwrap();
        assert!(dir.exists());
        assert!(dir.is_dir());
        assert!(dir.ends_with("memory"));
        // Verify the full path structure: {base}/user-123/memory
        // 校验完整路径结构：{base}/user-123/memory
        assert!(dir.parent().unwrap().ends_with("user-123"));
    }

    #[test]
    fn test_invalid_user_id_rejected() {
        let temp = TempDir::new().unwrap();
        let mgr = MemoryManager::new(temp.path().to_path_buf());

        assert!(mgr.get_memory_dir("../escape").is_err());
        assert!(mgr.get_memory_dir("user;rm -rf").is_err());
        assert!(mgr.get_memory_dir("").is_err());
        assert!(mgr.get_memory_dir(".hidden").is_err());
    }

    #[test]
    fn test_user_isolation() {
        let temp = TempDir::new().unwrap();
        let mgr = MemoryManager::new(temp.path().to_path_buf());

        let dir_a = mgr.get_memory_dir("user-a").unwrap();
        let dir_b = mgr.get_memory_dir("user-b").unwrap();

        assert_ne!(dir_a, dir_b);
        assert!(dir_a.parent().unwrap().ends_with("user-a"));
        assert!(dir_b.parent().unwrap().ends_with("user-b"));
    }

    #[test]
    fn test_path_within_memory() {
        let temp = TempDir::new().unwrap();
        let mgr = MemoryManager::new(temp.path().to_path_buf());

        // Create memory dir and a file
        // 创建记忆目录及一个文件
        let dir = mgr.get_memory_dir("test-user").unwrap();
        let test_file = dir.join("notes.md");
        std::fs::write(&test_file, "hello").unwrap();

        assert!(mgr.is_within_memory("test-user", &test_file).unwrap());
        assert!(!mgr
            .is_within_memory("test-user", Path::new("/etc/passwd"))
            .unwrap());
    }

    #[test]
    fn test_idempotent_creation() {
        let temp = TempDir::new().unwrap();
        let mgr = MemoryManager::new(temp.path().to_path_buf());

        let dir1 = mgr.get_memory_dir("user-x").unwrap();
        let dir2 = mgr.get_memory_dir("user-x").unwrap();
        assert_eq!(dir1, dir2);
    }

    #[test]
    fn test_from_env_default() {
        // Without env var set, should use default
        // 未设置环境变量时应使用默认值
        std::env::remove_var("SHANNON_USER_MEMORY_DIR");
        let mgr = MemoryManager::from_env();
        assert_eq!(mgr.base_dir(), Path::new("/tmp/shannon-users"));
    }
}
