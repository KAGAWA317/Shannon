use crate::config::Settings;
use crate::firecracker_api::FirecrackerApi;
use crate::workspace_sync;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;
use tokio::time::{interval, Duration};
use tracing::{debug, info, warn};
use uuid::Uuid;

/// A single Firecracker VM instance
/// 单个 Firecracker VM 实例
pub struct VmInstance {
    pub id: String,
    pub process: Child,
    pub api_socket: PathBuf,
    pub vsock_uds: PathBuf,
    pub vsock_cid: u32,  // Unique CID for this VM
    pub temp_dir: PathBuf,
    pub rootfs_copy: PathBuf,  // Per-VM rootfs copy
    pub workspace_dir: Option<PathBuf>,  // Mounted session workspace
    pub created_at: Instant,
    pub last_used: Instant,
    pub api: FirecrackerApi,
}

impl VmInstance {
    pub fn update_last_used(&mut self) {
        self.last_used = Instant::now();
    }
}

/// Thread-safe VM pool with warm pool and session affinity
/// 线程安全的 VM 池，支持预热池与 session 亲和
pub struct VmPool {
    settings: Settings,
    warm_pool: RwLock<Vec<VmInstance>>,
    session_map: RwLock<HashMap<String, VmInstance>>,
    active_count: AtomicUsize,
    cid_counter: AtomicU32,  // For unique CID allocation
    workspace_locks: RwLock<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl VmPool {
    pub fn new(settings: Settings) -> Arc<Self> {
        Arc::new(Self {
            cid_counter: AtomicU32::new(settings.vsock_cid_base),
            settings,
            warm_pool: RwLock::new(Vec::new()),
            session_map: RwLock::new(HashMap::new()),
            active_count: AtomicUsize::new(0),
            workspace_locks: RwLock::new(HashMap::new()),
        })
    }

    /// Get or create a workspace lock for a session.
    /// 获取或创建 session 的工作区锁。
    /// Caller is responsible for holding the guard across the full execution.
    /// 调用方需在整段执行期间持有该锁的 guard。
    pub async fn get_workspace_lock(&self, session_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.workspace_locks.write().await;
        locks.entry(session_id.to_string())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Allocate a unique vsock CID for a new VM
    /// 为新 VM 分配唯一的 vsock CID
    fn allocate_cid(&self) -> u32 {
        // CIDs 0-2 are reserved (host, local, hypervisor)
        // CID 0-2 为保留值（host、local、hypervisor）
        // We start from cid_base (default 100) and increment
        // 从 cid_base（默认 100）开始递增
        self.cid_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// Acquire a VM for execution. If session_id is provided, returns the same VM
    /// 获取用于执行的 VM。若提供了 session_id，则返回该 session 的同一个 VM
    /// for that session (session affinity).
    /// （session 亲和）。
    pub async fn acquire(
        &self,
        session_id: Option<String>,
        workspace_path: Option<&str>,
    ) -> Result<VmInstance> {
        // Session affinity is only valid when no workspace ext4 is involved.
        // 仅在不涉及 workspace ext4 时才能复用 session 亲和，
        // Workspace VMs must be spawned fresh so we can safely sync directory ↔ ext4.
        // 涉及工作区的 VM 必须新建，才能安全地在目录与 ext4 之间同步。
        if workspace_path.is_none() {
            if let Some(ref sid) = session_id {
                let mut sessions = self.session_map.write().await;
                if let Some(mut vm) = sessions.remove(sid) {
                    vm.update_last_used();
                    debug!(session_id = %sid, vm_id = %vm.id, "Returning session-affinity VM");
                    return Ok(vm);
                }
            }
        }

        // Try to get from warm pool - but only if no workspace required
        // 尝试从预热池获取——仅在不需工作区时可用
        // Warm pool VMs don't have workspace ext4 drives configured, so they can't
        // 预热池中的 VM 未配置 workspace ext4 盘，
        // mount session workspaces. When workspace_path is provided, we must spawn fresh.
        // 无法挂载 session 工作区；一旦提供 workspace_path，就必须新建 VM。
        if workspace_path.is_none() {
            let mut pool = self.warm_pool.write().await;
            if let Some(mut vm) = pool.pop() {
                vm.update_last_used();
                debug!(vm_id = %vm.id, "Acquired VM from warm pool (no workspace needed)");
                return Ok(vm);
            }
        } else {
            debug!(workspace_path = ?workspace_path, "Bypassing warm pool - workspace requires fresh VM with ext4 mount");
        }

        // Atomic check-and-increment to prevent race condition (P1 fix)
        // 原子地检查并自增，避免竞态（P1 修复）
        loop {
            let current = self.active_count.load(Ordering::SeqCst);
            if current >= self.settings.pool_max_count {
                return Err(anyhow!(
                    "VM pool exhausted: {} active VMs (max {})",
                    current,
                    self.settings.pool_max_count
                ));
            }

            // Compare-and-swap to prevent race
            // 用比较并交换（CAS）防止竞态
            if self
                .active_count
                .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                break;
            }
            // If CAS failed, another thread got there first - loop and retry
            // CAS 失败说明其他线程先一步成功，循环 retry
        }

        // Spawn a new VM
        // 新建 VM
        match self.spawn_vm(workspace_path).await {
            Ok(vm) => {
                info!(vm_id = %vm.id, cid = vm.vsock_cid, "Spawned new VM");
                Ok(vm)
            }
            Err(e) => {
                self.active_count.fetch_sub(1, Ordering::SeqCst);
                Err(e)
            }
        }
    }

    /// Release a VM back to the pool
    /// 将 VM 归还到池中
    pub async fn release(&self, mut vm: VmInstance, session_id: Option<String>, tainted: bool) {
        if tainted {
            info!(vm_id = %vm.id, "Terminating tainted VM");
            self.terminate_vm(&mut vm);
            self.active_count.fetch_sub(1, Ordering::SeqCst);
            return;
        }

        // If session_id provided, keep in session map for affinity
        // 若提供了 session_id，则留在 session 映射中以保持亲和
        if let Some(sid) = session_id {
            vm.update_last_used();
            let mut sessions = self.session_map.write().await;
            debug!(session_id = %sid, vm_id = %vm.id, "Storing VM for session affinity");
            sessions.insert(sid, vm);
            return;
        }

        // Session isolation: With ext4 block device approach, workspace state is in the
        // session 隔离：采用 ext4 块设备方案后，工作区状态保存在 EFS 上的
        // ext4 file on EFS, not in the rootfs. VMs can be safely returned to warm pool
        // ext4 文件中而非 rootfs 内，因此 VM 可安全归还预热池，
        // because /workspace is unmounted when VM stops, and each session gets its own ext4.
        // 因为 VM 停止时会卸载 /workspace，且每个 session 都有独立的 ext4。
        // Clear workspace_dir reference so next session gets fresh mount.
        // 清空 workspace_dir 引用，使下个 session 重新挂载。
        vm.workspace_dir = None;

        // Return to warm pool if not at capacity
        // 未达容量上限时归还预热池
        let mut pool = self.warm_pool.write().await;
        if pool.len() < self.settings.pool_warm_count {
            vm.update_last_used();
            debug!(vm_id = %vm.id, warm_pool_size = pool.len() + 1, "Returning VM to warm pool");
            pool.push(vm);
        } else {
            drop(pool);
            info!(vm_id = %vm.id, "Warm pool full, terminating VM");
            self.terminate_vm(&mut vm);
            self.active_count.fetch_sub(1, Ordering::SeqCst);
        }
    }

    /// Clean workspace directory to prevent session data leakage
    /// 清理工作区目录，防止 session 数据泄漏
    fn clean_workspace(&self, workspace: &PathBuf) -> Result<()> {
        // Remove all files in workspace but keep the directory
        // 删除工作区内的所有文件，但保留目录本身
        if workspace.exists() {
            for entry in std::fs::read_dir(workspace)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_dir() {
                    std::fs::remove_dir_all(&path)?;
                } else {
                    std::fs::remove_file(&path)?;
                }
            }
        }
        Ok(())
    }

    /// Background task to maintain the warm pool at the target size
    /// 后台 task，将预热池维持目标规模
    pub async fn maintain_warm_pool(self: Arc<Self>) {
        let mut ticker = interval(Duration::from_secs(5));
        loop {
            ticker.tick().await;

            // Clean up idle session VMs
            // 清理空闲的 session VM
            self.evict_idle_sessions().await;

            // Spawn VMs to maintain warm pool
            // 创建 VM 以补足预热池
            let current_warm = self.warm_pool.read().await.len();
            let current_active = self.active_count.load(Ordering::SeqCst);

            if current_warm < self.settings.pool_warm_count
                && current_active < self.settings.pool_max_count
            {
                let to_spawn = (self.settings.pool_warm_count - current_warm)
                    .min(self.settings.pool_max_count - current_active);

                for _ in 0..to_spawn {
                    // Use atomic CAS here too for safety
                    // 这里同样使用原子 CAS 以保证安全
                    let current = self.active_count.load(Ordering::SeqCst);
                    if current >= self.settings.pool_max_count {
                        break;
                    }
                    if self
                        .active_count
                        .compare_exchange(current, current + 1, Ordering::SeqCst, Ordering::SeqCst)
                        .is_err()
                    {
                        continue;
                    }

                    match self.spawn_vm(None).await {
                        Ok(vm) => {
                            info!(vm_id = %vm.id, cid = vm.vsock_cid, "Pre-warmed VM for pool");
                            self.warm_pool.write().await.push(vm);
                        }
                        Err(e) => {
                            warn!("Failed to pre-warm VM: {}", e);
                            self.active_count.fetch_sub(1, Ordering::SeqCst);
                        }
                    }
                }
            }
        }
    }

    /// Evict session VMs that have been idle for too long
    /// 淘汰空闲过久的 session VM
    async fn evict_idle_sessions(&self) {
        let timeout = Duration::from_secs(self.settings.session_idle_timeout_seconds);
        let mut sessions = self.session_map.write().await;
        let mut to_remove = Vec::new();

        for (sid, vm) in sessions.iter() {
            if vm.last_used.elapsed() > timeout {
                to_remove.push(sid.clone());
            }
        }

        for sid in to_remove {
            if let Some(mut vm) = sessions.remove(&sid) {
                info!(session_id = %sid, vm_id = %vm.id, "Evicting idle session VM");

                // Clear workspace reference - ext4 file persists on EFS independently
                // 清空工作区引用——ext4 文件独立保存在 EFS 上
                vm.workspace_dir = None;

                // Try to return to warm pool instead of terminating
                // 尽量归还预热池，而非直接终止
                let mut pool = self.warm_pool.write().await;
                if pool.len() < self.settings.pool_warm_count {
                    vm.update_last_used();
                    pool.push(vm);
                } else {
                    drop(pool);
                    self.terminate_vm(&mut vm);
                    self.active_count.fetch_sub(1, Ordering::SeqCst);
                }
            }
        }
    }

    /// Get current pool statistics
    /// 获取当前池统计信息
    pub async fn stats(&self) -> PoolStats {
        let warm_count = self.warm_pool.read().await.len();
        let session_count = self.session_map.read().await.len();
        let active_count = self.active_count.load(Ordering::SeqCst);

        PoolStats {
            warm_count,
            session_count,
            active_count,
            max_count: self.settings.pool_max_count,
        }
    }

    async fn spawn_vm(&self, workspace_path: Option<&str>) -> Result<VmInstance> {
        let instance_id = Uuid::new_v4().to_string();
        let temp_dir = PathBuf::from(&self.settings.socket_dir).join(&instance_id);
        std::fs::create_dir_all(&temp_dir)?;

        let api_socket = temp_dir.join("firecracker.sock");
        let vsock_uds = temp_dir.join("vsock.sock");

        // Allocate unique CID for this VM (P0 fix)
        // 为该 VM 分配唯一 CID（P0 修复）
        let vsock_cid = self.allocate_cid();

        // Create per-VM rootfs copy using copy-on-write if supported
        // 在支持时用写时复制（CoW）创建每 VM 独立的 rootfs 副本
        // Reflinks are instant and only allocate disk space for changed blocks
        // reflink 瞬时完成，且仅为变更块分配磁盘空间
        let rootfs_copy = temp_dir.join("rootfs.ext4");
        Self::copy_rootfs_cow(&self.settings.rootfs_image, &rootfs_copy)?;

        let process = self.spawn_firecracker_process(&api_socket)?;
        self.wait_for_socket(&api_socket)?;

        let api = FirecrackerApi::new(
            api_socket.to_string_lossy().to_string(),
            self.settings.api_timeout_ms,
        );

        // Configure the VM with unique CID and per-VM rootfs
        // 用唯一 CID 和独立 rootfs 配置 VM
        self.configure_vm(&api, &vsock_uds, vsock_cid, &rootfs_copy, workspace_path).await?;

        // Pre-sync directory → ext4 before VM boot to avoid mounting a filesystem that's
        // VM 启动前先将目录同步到 ext4，避免挂载一个
        // already in use by the guest (guest init mounts /dev/vdb at boot).
        // 已被 guest 占用的文件系统（guest init 会在启动时挂载 /dev/vdb）。
        if let Some(wp) = workspace_path {
            let ext4_path = format!("{}.ext4", wp.trim_end_matches('/'));

            // 1. Check consistency - fsck if state is missing/dirty
            // 1. 检查一致性——状态缺失或脏时执行 fsck
            if let Err(e) = workspace_sync::ensure_ext4_consistent(&ext4_path) {
                warn!(workspace = %wp, error = %e, "Failed to ensure ext4 consistency");
            }

            // 2. Mark dirty BEFORE VM boot (crash recovery marker)
            // 2. 在 VM 启动前标记为脏（崩溃恢复标记）
            if let Err(e) = workspace_sync::mark_dirty(&ext4_path) {
                warn!(workspace = %wp, error = %e, "Failed to mark workspace dirty");
            }

            // 3. Pre-sync directory → ext4
            // 3. 预同步目录 → ext4
            if let Err(e) = workspace_sync::sync_directory_to_ext4(wp) {
                warn!(workspace = %wp, error = %e, "Failed to pre-sync workspace to ext4");
            }
        }

        // THEN InstanceStart happens
        // 之后才执行 InstanceStart
        self.start_instance(&api).await?;

        // Minimal boot wait - the vsock_client now implements retry with exponential backoff
        // 仅做最短的启动等待——vsock_client 现已实现指数退避 retry，
        // to handle the case where guest-agent isn't ready yet (per Firecracker issue #1253).
        // 以处理 guest-agent 尚未就绪的情况（见 Firecracker issue #1253）。
        // We just give the kernel a moment to start, actual guest readiness is detected dynamically.
        // 这里只给内核一点启动时间，guest 是否就绪由后续动态探测。
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Determine workspace directory
        // 确定工作区目录
        let workspace_dir = workspace_path.map(|p| PathBuf::from(p));

        Ok(VmInstance {
            id: instance_id,
            process,
            api_socket,
            vsock_uds,
            vsock_cid,
            temp_dir,
            rootfs_copy,
            workspace_dir,
            created_at: Instant::now(),
            last_used: Instant::now(),
            api,
        })
    }

    fn spawn_firecracker_process(&self, api_socket: &PathBuf) -> Result<Child> {
        std::fs::create_dir_all(&self.settings.socket_dir).ok();

        let cmd = if self.settings.use_jailer {
            let jailer = self
                .settings
                .jailer_bin
                .clone()
                .ok_or_else(|| anyhow!("FIRECRACKER_JAILER_BIN not set"))?;
            let chroot = self.settings.socket_dir.join("jailer");
            Command::new(jailer)
                .arg("--id")
                .arg(Uuid::new_v4().to_string())
                .arg("--exec-file")
                .arg(self.settings.firecracker_bin.clone())
                .arg("--uid")
                .arg("0")
                .arg("--gid")
                .arg("0")
                .arg("--chroot-base-dir")
                .arg(chroot)
                .arg("--")
                .arg("--api-sock")
                .arg(api_socket)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?
        } else {
            Command::new(self.settings.firecracker_bin.clone())
                .arg("--api-sock")
                .arg(api_socket)
                .stdout(Stdio::null())
                .stderr(Stdio::inherit())
                .spawn()?
        };

        Ok(cmd)
    }

    fn wait_for_socket(&self, api_socket: &PathBuf) -> Result<()> {
        let start = Instant::now();
        let timeout = std::time::Duration::from_millis(self.settings.api_timeout_ms);
        while !api_socket.exists() {
            if start.elapsed() > timeout {
                return Err(anyhow!("firecracker API socket not ready"));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        Ok(())
    }

    async fn configure_vm(
        &self,
        api: &FirecrackerApi,
        vsock_uds: &PathBuf,
        vsock_cid: u32,
        rootfs_path: &PathBuf,
        workspace_path: Option<&str>,
    ) -> Result<()> {
        api.put_json(
            "/machine-config",
            &serde_json::json!({
                "vcpu_count": self.settings.vcpu_count,
                "mem_size_mib": self.settings.mem_size_mib
            }),
        )
        .await?;

        api.put_json(
            "/boot-source",
            &serde_json::json!({
                "kernel_image_path": self.settings.kernel_image.to_string_lossy(),
                "boot_args": self.settings.boot_args
            }),
        )
        .await?;

        // Per-VM rootfs copy, read-write but isolated (P0 fix)
        // 每 VM 独立 rootfs 副本，可读写但相互隔离（P0 修复）
        api.put_json(
            "/drives/rootfs",
            &serde_json::json!({
                "drive_id": "rootfs",
                "path_on_host": rootfs_path.to_string_lossy(),
                "is_root_device": true,
                "is_read_only": false  // rw needed for /tmp, /var, etc.
            }),
        )
        .await?;

        // Workspace persistence via per-session ext4 block device
        // 通过每 session 独立的 ext4 块设备实现工作区持久化
        // Creates a sparse ext4 file on EFS, mounted as /dev/vdb in VM
        // 在 EFS 上创建稀疏 ext4 文件，并在 VM 内挂载为 /dev/vdb
        // Guest init mounts /dev/vdb to /workspace
        // guest init 会把 /dev/vdb 挂载到 /workspace
        if let Some(wp) = workspace_path {
            let workspace_ext4 = format!("{}.ext4", wp.trim_end_matches('/'));

            // Create sparse ext4 file if it doesn't exist (first execution for this session)
            // 若稀疏 ext4 文件不存在则创建（该 session 首次执行）
            if !std::path::Path::new(&workspace_ext4).exists() {
                Self::create_workspace_ext4(&workspace_ext4, 1024)?; // 1GB sparse file
            }

            // Mount workspace ext4 as secondary drive
            // 将 workspace ext4 挂载为第二块盘
            info!(workspace_ext4 = %workspace_ext4, "Configuring workspace drive for VM");
            api.put_json(
                "/drives/workspace",
                &serde_json::json!({
                    "drive_id": "workspace",
                    "path_on_host": workspace_ext4,
                    "is_root_device": false,
                    "is_read_only": false
                }),
            )
            .await?;

            info!(workspace_ext4 = %workspace_ext4, "Workspace drive configured successfully");
        }

        // Unique CID per VM (P0 fix)
        // 每个 VM 使用唯一 CID（P0 修复）
        api.put_json(
            "/vsock",
            &serde_json::json!({
                "guest_cid": vsock_cid,
                "uds_path": vsock_uds.to_string_lossy()
            }),
        )
        .await?;

        Ok(())
    }

    async fn start_instance(&self, api: &FirecrackerApi) -> Result<()> {
        api.put_json(
            "/actions",
            &serde_json::json!({"action_type":"InstanceStart"}),
        )
        .await
    }

    /// Create a sparse ext4 file for workspace persistence
    /// 创建用于工作区持久化的稀疏 ext4 文件
    fn create_workspace_ext4(path: &str, size_mb: u64) -> Result<()> {
        use std::process::Command;

        // Ensure parent directory exists
        // 确保父目录存在
        if let Some(parent) = std::path::Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Create sparse file (doesn't allocate disk space until written)
        // 创建稀疏文件（写入前不占用实际磁盘空间）
        let file = std::fs::File::create(path)?;
        file.set_len(size_mb * 1024 * 1024)?;
        drop(file);

        // Format as ext4
        // 格式化为 ext4
        let output = Command::new("mkfs.ext4")
            .args(["-F", "-q", path])
            .output()?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow!("mkfs.ext4 failed: {}", stderr));
        }

        info!(path = %path, size_mb = size_mb, "Created workspace ext4 file");
        Ok(())
    }

    /// Copy rootfs using copy-on-write (reflink) if filesystem supports it.
    /// 在文件系统支持时用写时复制（reflink）复制 rootfs。
    /// Falls back to regular copy if reflinks aren't available.
    /// 若不支持 reflink，则回退为普通复制。
    /// Reflinks are instant and only allocate disk for changed blocks.
    /// reflink 瞬时完成，且仅为变更块分配磁盘空间。
    fn copy_rootfs_cow(src: &PathBuf, dst: &PathBuf) -> Result<()> {
        use std::process::Command;

        // Try reflink first (XFS, btrfs, APFS support this)
        // 优先尝试 reflink（XFS、btrfs、APFS 支持）
        let output = Command::new("cp")
            .args(["--reflink=auto", "-f"])
            .arg(src)
            .arg(dst)
            .output();

        match output {
            Ok(result) if result.status.success() => {
                debug!(src = %src.display(), dst = %dst.display(), "Copied rootfs (reflink if supported)");
                Ok(())
            }
            Ok(result) => {
                // cp failed, try regular copy
                // cp 失败，改用普通复制
                let stderr = String::from_utf8_lossy(&result.stderr);
                debug!("cp --reflink failed ({}), falling back to std::fs::copy", stderr.trim());
                std::fs::copy(src, dst)?;
                Ok(())
            }
            Err(_) => {
                // cp command not available, use Rust copy
                // 无 cp 命令，使用 Rust 复制
                std::fs::copy(src, dst)?;
                Ok(())
            }
        }
    }

    fn terminate_vm(&self, vm: &mut VmInstance) {
        if let Err(e) = vm.process.kill() {
            debug!(vm_id = %vm.id, "Failed to kill firecracker process: {}", e);
        }
        let _ = vm.process.wait();

        // Clean up temp directory including rootfs copy
        // 清理临时目录（含 rootfs 副本）
        if let Err(e) = std::fs::remove_dir_all(&vm.temp_dir) {
            debug!(vm_id = %vm.id, "Failed to clean up temp dir: {}", e);
        }
    }
}

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub warm_count: usize,
    pub session_count: usize,
    pub active_count: usize,
    pub max_count: usize,
}
