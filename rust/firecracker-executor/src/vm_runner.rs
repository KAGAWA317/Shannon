use crate::config::Settings;
use crate::models::{ExecuteRequest, ExecuteResponse, GuestRequest};
use crate::vm_pool::{VmInstance, VmPool};
use crate::vsock_client::execute_guest_via_uds;
use crate::workspace_sync;
use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, warn};

/// Known EKS mount point prefix - when paths start with this, we need to translate
/// 已知的 EKS 挂载点前缀——路径以此开头时需要转换，
/// them to use the local FIRECRACKER_EFS_MOUNT which includes the PVC subdirectory.
/// 改用包含 PVC 子目录的本地 FIRECRACKER_EFS_MOUNT。
const EKS_MOUNT_PREFIX: &str = "/mnt/shannon-sessions/";

/// Error types to distinguish between transient and permanent failures
/// 用于区分瞬时错误与永久错误的错误类型
#[derive(Debug)]
pub enum ExecuteError {
    /// Pool exhausted - client should retry later or fall back
    /// 池已耗尽——客户端应稍后 retry 或回退
    PoolExhausted(String),
    /// VM boot failed - transient, can retry
    /// VM 启动失败——瞬时错误，可 retry
    VmBootFailed(String),
    /// Execution failed - not transient
    /// 执行失败——非瞬时错误
    ExecutionFailed(String),
}

pub struct VmRunner {
    settings: Settings,
    pool: Arc<VmPool>,
}

impl VmRunner {
    pub fn new(settings: Settings, pool: Arc<VmPool>) -> Self {
        Self { settings, pool }
    }

    /// Translate workspace path from EKS format to EC2 format.
    /// 将 EKS 格式的工作区路径转换为 EC2 格式。
    /// EKS pods mount EFS via Access Point at /mnt/shannon-sessions which maps to
    /// EKS Pod 通过 Access Point 将 EFS 挂载到 /mnt/shannon-sessions，
    /// a PVC subdirectory. EC2 mounts the EFS root at /mnt/shannon-sessions.
    /// 映射到 PVC 子目录；EC2 则把 EFS 根挂载到 /mnt/shannon-sessions。
    /// This translates /mnt/shannon-sessions/{session} → {efs_mount_point}/{session}
    /// 此处将 /mnt/shannon-sessions/{session} → {efs_mount_point}/{session}
    fn translate_workspace_path(&self, path: &str) -> String {
        let efs_mount = self.settings.efs_mount_point.to_string_lossy();
        let efs_mount_str = efs_mount.trim_end_matches('/');

        // Guard: if path already starts with our EFS mount point, don't double-translate
        // 保护：若路径已以本地 EFS 挂载点开头，避免重复转换
        if path.starts_with(efs_mount_str) {
            return path.to_string();
        }

        // Translate EKS Access Point path to EC2 full path
        // 将 EKS Access Point 路径转换为 EC2 完整路径
        if let Some(session_part) = path.strip_prefix(EKS_MOUNT_PREFIX) {
            let translated = format!("{}/{}", efs_mount_str, session_part.trim_start_matches('/'));
            debug!(
                original = %path,
                translated = %translated,
                "Translated EKS workspace path to EC2 path"
            );
            translated
        } else {
            // Path doesn't start with known EKS prefix, use as-is
            // 路径不以已知 EKS 前缀开头，原样使用
            path.to_string()
        }
    }

    /// Execute code in a Firecracker VM
    /// 在 Firecracker VM 中执行代码
    /// Returns (response, is_transient_error)
    /// 返回 (response, 是否为瞬时错误)
    /// is_transient_error=true means the caller should return 503
    /// is_transient_error=true 表示调用方应返回 503
    pub async fn execute(&self, req: ExecuteRequest) -> (ExecuteResponse, bool) {
        let start = Instant::now();
        let session_id = req.session_id.clone();
        let has_workspace = req.workspace_path.is_some();

        // Acquire workspace lock BEFORE any VM operations if workspace involved
        // 涉及工作区时，在任何 VM 操作之前先获取工作区锁
        // This prevents concurrent python_executor calls for the same session from corrupting ext4
        // 防止同一 session 的并发 python_executor 调用破坏 ext4
        let _workspace_guard = if has_workspace {
            if let Some(ref sid) = session_id {
                let lock = self.pool.get_workspace_lock(sid).await;
                Some(lock.lock_owned().await)
            } else {
                None
            }
        } else {
            None
        };

        // Lock held across: execute_inner (acquire + boot + execute) + release + post-sync
        // 锁的覆盖范围：execute_inner（获取 + 启动 + 执行）+ 释放 + 后置同步
        match self.execute_inner(req).await {
            Ok((resp, vm, tainted, workspace_path)) => {
                // For workspace sessions, mark as tainted to force VM termination.
                // 对涉及工作区的 session，标记为 tainted 以强制终止 VM。
                // This releases the ext4 block device so we can sync files.
                // 这样可释放 ext4 块设备，便于同步文件。
                // Session affinity is sacrificed for workspace file consistency.
                // 为保证工作区文件一致性，此处牺牲 session 亲和。
                let force_terminate = workspace_path.is_some();
                self.pool.release(vm, session_id.clone(), tainted || force_terminate).await;

                // CRITICAL: Post-sync regardless of resp.success
                // 关键：无论 resp.success 与否都要做后置同步。
                // Files must persist even if Python execution failed.
                // 即使 Python 执行失败，文件也必须持久化。
                // mark_clean depends on "post-sync succeeded", NOT resp.success.
                // mark_clean 取决于“后置同步成功”，而非 resp.success。
                if let Some(wp) = workspace_path.as_ref() {
                    // Retry sync with increasing delays - block device may take time to release
                    // 带递增延迟 retry 同步——块设备释放可能需要时间
                    let mut synced = false;
                    for delay_ms in [100, 200, 500, 1000] {
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        match workspace_sync::sync_ext4_to_directory(wp) {
                            Ok(()) => {
                                synced = true;
                                break;
                            }
                            Err(e) if e.contains("cannot mount") => {
                                debug!(workspace = %wp, delay_ms, "Sync retry - block device not yet released");
                                continue;
                            }
                            Err(e) => {
                                warn!(workspace = %wp, error = %e, "Failed to post-sync workspace");
                                break;
                            }
                        }
                    }
                    if synced {
                        let _ = workspace_sync::mark_clean(&format!("{}.ext4", wp.trim_end_matches('/')));
                    } else {
                        warn!(workspace = %wp, "Post-sync failed after retries - files may not be visible to file_read");
                    }
                }
                // _workspace_guard drops here, releasing lock
                // 此处 _workspace_guard 被丢弃，锁随之释放

                (
                    ExecuteResponse {
                        duration_ms: Some(start.elapsed().as_millis() as u64),
                        ..resp
                    },
                    false,
                )
            }
            Err(ExecuteError::PoolExhausted(msg)) => {
                // Return 503-worthy response (P1 fix: fallback trigger)
                // 返回应触发 503 的 response（P1 修复：触发回退）
                (
                    ExecuteResponse {
                        success: false,
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code: 1,
                        error: Some(msg),
                        duration_ms: Some(start.elapsed().as_millis() as u64),
                    },
                    true, // Signal caller to return 503
                )
            }
            Err(ExecuteError::VmBootFailed(msg)) => {
                // Also transient
                // 同样是瞬时错误
                (
                    ExecuteResponse {
                        success: false,
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code: 1,
                        error: Some(msg),
                        duration_ms: Some(start.elapsed().as_millis() as u64),
                    },
                    true,
                )
            }
            Err(ExecuteError::ExecutionFailed(msg)) => {
                (
                    ExecuteResponse {
                        success: false,
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code: 1,
                        error: Some(msg),
                        duration_ms: Some(start.elapsed().as_millis() as u64),
                    },
                    false,
                )
            }
        }
    }

    async fn execute_inner(
        &self,
        req: ExecuteRequest,
    ) -> Result<(ExecuteResponse, VmInstance, bool, Option<String>), ExecuteError> {
        let session_id = req.session_id.clone();

        // Translate workspace path from EKS format to EC2 format if needed
        // 按需将工作区路径从 EKS 格式转换为 EC2 格式
        let translated_path = req
            .workspace_path
            .as_ref()
            .map(|p| self.translate_workspace_path(p));
        let workspace_path = translated_path.as_deref();

        // Acquire VM with workspace path (P0 fix: EFS wiring)
        // 带工作区路径获取 VM（P0 修复：EFS 接线）
        let vm = self
            .pool
            .acquire(session_id, workspace_path)
            .await
            .map_err(|e| {
                let msg = e.to_string();
                if msg.contains("pool exhausted") {
                    ExecuteError::PoolExhausted(msg)
                } else {
                    ExecuteError::VmBootFailed(msg)
                }
            })?;

        let timeout_seconds = req
            .timeout_seconds
            .unwrap_or(self.settings.executor_timeout_seconds)
            .min(self.settings.executor_timeout_seconds);

        let guest_req = GuestRequest {
            code: req.code,
            stdin: req.stdin,
            timeout_seconds,
        };

        // Connect via Firecracker's vsock UDS proxy
        // 通过 Firecracker 的 vsock UDS 代理连接
        let result = execute_guest_via_uds(
            &vm.vsock_uds,
            self.settings.vsock_port,
            guest_req,
            timeout_seconds,
        )
        .await;

        match result {
            Ok(guest_resp) => {
                let tainted = !guest_resp.success;
                // Return workspace_path so caller can sync AFTER VM release
                // 返回 workspace_path，便于调用方在 VM 释放后同步
                Ok((
                    ExecuteResponse {
                        success: guest_resp.success,
                        stdout: guest_resp.stdout,
                        stderr: guest_resp.stderr,
                        exit_code: guest_resp.exit_code,
                        error: guest_resp.error,
                        duration_ms: None,
                    },
                    vm,
                    tainted,
                    translated_path,
                ))
            }
            Err(e) => {
                warn!(vm_id = %vm.id, "Guest execution failed: {}", e);
                Ok((
                    ExecuteResponse {
                        success: false,
                        stdout: String::new(),
                        stderr: String::new(),
                        exit_code: 1,
                        error: Some(e.to_string()),
                        duration_ms: None,
                    },
                    vm,
                    true, // Mark as tainted on error
                    translated_path,
                ))
            }
        }
    }
}
