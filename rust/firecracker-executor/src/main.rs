mod config;
mod firecracker_api;
mod models;
mod vm_pool;
mod vm_runner;
mod vsock_client;
mod workspace_sync;

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use config::Settings;
use models::{
    CleanupRequest, CleanupResponse, DownloadRequest, DownloadResponse, ExecuteRequest, FileInfo,
    ListFilesRequest, ListFilesResponse,
};
use serde::Serialize;
use std::sync::Arc;
use tracing::{info, warn};
use vm_pool::VmPool;
use vm_runner::VmRunner;

#[derive(Clone)]
struct AppState {
    settings: Settings,
    pool: Arc<VmPool>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    pool_size: usize,
    warm_count: usize,
    session_count: usize,
    max_count: usize,
}

async fn execute_handler(
    State(state): State<AppState>,
    Json(req): Json<ExecuteRequest>,
) -> impl IntoResponse {
    info!(
        "Received execute request (session_id={:?}, timeout={:?}, workspace_path={:?})",
        req.session_id, req.timeout_seconds, req.workspace_path
    );

    let runner = VmRunner::new(state.settings, state.pool);
    let (result, is_transient) = runner.execute(req).await;

    // P1 fix: Return 503 for transient errors to trigger WASI fallback
    // P1 修复：瞬时错误返回 503，以触发 WASI 回退
    if is_transient {
        (StatusCode::SERVICE_UNAVAILABLE, Json(result))
    } else {
        (StatusCode::OK, Json(result))
    }
}

async fn health_handler(State(state): State<AppState>) -> Json<HealthResponse> {
    let stats = state.pool.stats().await;
    Json(HealthResponse {
        status: "healthy".to_string(),
        pool_size: stats.active_count,
        warm_count: stats.warm_count,
        session_count: stats.session_count,
        max_count: stats.max_count,
    })
}

async fn metrics_handler(State(state): State<AppState>) -> String {
    let stats = state.pool.stats().await;
    format!(
        "# HELP firecracker_pool_size Total number of active VMs\n\
         # TYPE firecracker_pool_size gauge\n\
         firecracker_pool_size {}\n\
         # HELP firecracker_warm_count Number of pre-warmed VMs ready for use\n\
         # TYPE firecracker_warm_count gauge\n\
         firecracker_warm_count {}\n\
         # HELP firecracker_session_count Number of VMs assigned to sessions\n\
         # TYPE firecracker_session_count gauge\n\
         firecracker_session_count {}\n\
         # HELP firecracker_pool_max Maximum pool capacity\n\
         # TYPE firecracker_pool_max gauge\n\
         firecracker_pool_max {}\n",
        stats.active_count, stats.warm_count, stats.session_count, stats.max_count
    )
}

/// Download a file from session workspace
/// 从 session 工作区下载文件
/// Uses the session's VM to read the file via Python code execution.
/// 借助 session 的 VM 执行 Python 代码读取文件。
async fn download_handler(
    State(state): State<AppState>,
    Json(req): Json<DownloadRequest>,
) -> impl IntoResponse {
    info!(
        "Received download request (session_id={}, file_path={}, workspace_path={})",
        req.session_id, req.file_path, req.workspace_path
    );

    // Security: Validate file_path doesn't escape /workspace
    // 安全：校验 file_path 不会逃出 /workspace
    if req.file_path.contains("..") || req.file_path.starts_with('/') {
        return (
            StatusCode::BAD_REQUEST,
            Json(DownloadResponse {
                success: false,
                content: None,
                content_type: None,
                size_bytes: None,
                error: Some("Invalid file path: must be relative and cannot contain '..'".into()),
            }),
        );
    }

    // Build safe path within /workspace
    // 在 /workspace 内构建安全路径
    let safe_path = format!("/workspace/{}", req.file_path);
    let safe_path_literal =
        serde_json::to_string(&safe_path).unwrap_or_else(|_| "\"/workspace\"".to_string());

    // Use base64 encoding for binary safety - read file via VM
    // 使用 base64 编码保证二进制安全——通过 VM 读取文件
    // Using `base64` command to encode, which handles binary files correctly
    // 使用 `base64` 命令编码，可正确处理二进制文件
    let code = format!(
        r#"import sys, base64, os
path = {}
if not os.path.exists(path):
    print('FILE_NOT_FOUND', file=sys.stderr)
    sys.exit(1)
if os.path.isdir(path):
    print('IS_DIRECTORY', file=sys.stderr)
    sys.exit(1)
with open(path, 'rb') as f:
    data = f.read()
print(base64.b64encode(data).decode(), end='')
print(len(data), file=sys.stderr)"#,
        safe_path_literal
    );

    let exec_req = ExecuteRequest {
        code,
        session_id: Some(req.session_id.clone()),
        timeout_seconds: Some(30),
        workspace_path: Some(req.workspace_path),
        stdin: None,
    };

    let runner = VmRunner::new(state.settings, state.pool);
    let (result, is_transient) = runner.execute(exec_req).await;

    if is_transient {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(DownloadResponse {
                success: false,
                content: None,
                content_type: None,
                size_bytes: None,
                error: Some(result.error.unwrap_or_else(|| "Executor unavailable".to_string())),
            }),
        );
    }

    if !result.success {
        let is_not_found =
            result.stderr.contains("FILE_NOT_FOUND") || result.stderr.contains("IS_DIRECTORY");
        let status = if is_not_found {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };

        let error_msg = if result.stderr.contains("FILE_NOT_FOUND") {
            format!("File not found: {}", req.file_path)
        } else if result.stderr.contains("IS_DIRECTORY") {
            format!("Path is a directory: {}", req.file_path)
        } else if let Some(err) = result.error {
            err
        } else if !result.stderr.trim().is_empty() {
            result.stderr
        } else {
            "Unknown error".to_string()
        };

        return (
            status,
            Json(DownloadResponse {
                success: false,
                content: None,
                content_type: None,
                size_bytes: None,
                error: Some(error_msg),
            }),
        );
    }

    // Parse size from stderr (our Python script prints it there)
    // 从 stderr 解析文件大小（Python 脚本会输出到那里）
    let size_bytes: Option<u64> = result.stderr.trim().parse().ok();

    // Infer content type from extension
    // 根据扩展名推断内容类型
    let content_type = infer_content_type(&req.file_path);

    (
        StatusCode::OK,
        Json(DownloadResponse {
            success: true,
            content: Some(result.stdout),
            content_type: Some(content_type),
            size_bytes,
            error: None,
        }),
    )
}

/// List files in session workspace
/// 列出 session 工作区中的文件
async fn list_files_handler(
    State(state): State<AppState>,
    Json(req): Json<ListFilesRequest>,
) -> impl IntoResponse {
    info!(
        "Received list files request (session_id={}, workspace_path={}, path={:?})",
        req.session_id, req.workspace_path, req.path
    );

    // Build path to list
    // 构建要列出的路径
    let list_path = match &req.path {
        Some(p) if !p.is_empty() => {
            // Security: Validate path doesn't escape /workspace
            // 安全：校验路径不会逃出 /workspace
            if p.contains("..") {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ListFilesResponse {
                        success: false,
                        files: vec![],
                        error: Some("Invalid path: cannot contain '..'".into()),
                    }),
                );
            }
            if p.starts_with('/') {
                format!("/workspace{}", p)
            } else {
                format!("/workspace/{}", p)
            }
        }
        _ => "/workspace".to_string(),
    };
    let list_path_literal =
        serde_json::to_string(&list_path).unwrap_or_else(|_| "\"/workspace\"".to_string());

    // Python script to list files with metadata
    // 列出文件及 metadata 的 Python 脚本
    let code = format!(
        r#"import os, json, sys
path = {}
if not os.path.exists(path):
    print('PATH_NOT_FOUND', file=sys.stderr)
    sys.exit(1)
if not os.path.isdir(path):
    print('NOT_A_DIRECTORY', file=sys.stderr)
    sys.exit(1)
files = []
for name in os.listdir(path):
    full = os.path.join(path, name)
    try:
        st = os.stat(full)
        files.append({{
            'name': name,
            'path': full.replace('/workspace/', ''),
            'is_dir': os.path.isdir(full),
            'size_bytes': st.st_size if not os.path.isdir(full) else 0
        }})
    except: pass
print(json.dumps(files))"#,
        list_path_literal
    );

    let exec_req = ExecuteRequest {
        code,
        session_id: Some(req.session_id.clone()),
        timeout_seconds: Some(30),
        workspace_path: Some(req.workspace_path),
        stdin: None,
    };

    let runner = VmRunner::new(state.settings, state.pool);
    let (result, is_transient) = runner.execute(exec_req).await;

    if is_transient {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ListFilesResponse {
                success: false,
                files: vec![],
                error: Some(result.error.unwrap_or_else(|| "Executor unavailable".to_string())),
            }),
        );
    }

    if !result.success {
        let is_not_found =
            result.stderr.contains("PATH_NOT_FOUND") || result.stderr.contains("NOT_A_DIRECTORY");
        let status = if is_not_found {
            StatusCode::NOT_FOUND
        } else {
            StatusCode::INTERNAL_SERVER_ERROR
        };

        let error_msg = if result.stderr.contains("PATH_NOT_FOUND") {
            "Path not found".to_string()
        } else if result.stderr.contains("NOT_A_DIRECTORY") {
            "Path is not a directory".to_string()
        } else if let Some(err) = result.error {
            err
        } else if !result.stderr.trim().is_empty() {
            result.stderr
        } else {
            "Unknown error".to_string()
        };

        return (
            status,
            Json(ListFilesResponse {
                success: false,
                files: vec![],
                error: Some(error_msg),
            }),
        );
    }

    // Parse JSON output
    // 解析 JSON 输出
    match serde_json::from_str::<Vec<FileInfo>>(&result.stdout) {
        Ok(files) => (
            StatusCode::OK,
            Json(ListFilesResponse {
                success: true,
                files,
                error: None,
            }),
        ),
        Err(e) => {
            warn!("Failed to parse file list JSON: {}", e);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ListFilesResponse {
                    success: false,
                    files: vec![],
                    error: Some(format!("Failed to parse file list: {}", e)),
                }),
            )
        }
    }
}

/// Known EKS mount point prefix - when paths start with this, we need to translate
/// 已知的 EKS 挂载点前缀——路径以此开头时需要转换，
/// them to use the local FIRECRACKER_EFS_MOUNT which includes the PVC subdirectory.
/// 改用包含 PVC 子目录的本地 FIRECRACKER_EFS_MOUNT。
const EKS_MOUNT_PREFIX: &str = "/mnt/shannon-sessions/";

/// Translate workspace path from EKS format to EC2 format.
/// 将 EKS 格式的工作区路径转换为 EC2 格式。
/// EKS pods mount EFS via Access Point at /mnt/shannon-sessions which maps to
/// EKS Pod 通过 Access Point 将 EFS 挂载到 /mnt/shannon-sessions，
/// a PVC subdirectory. EC2 mounts the EFS root at /mnt/shannon-sessions.
/// 映射到 PVC 子目录；EC2 则把 EFS 根挂载到 /mnt/shannon-sessions。
/// This translates /mnt/shannon-sessions/{session} → {efs_mount_point}/{session}
/// 此处将 /mnt/shannon-sessions/{session} → {efs_mount_point}/{session}
fn translate_workspace_path(path: &str, efs_mount_point: &std::path::Path) -> String {
    let efs_mount = efs_mount_point.to_string_lossy();
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
        tracing::debug!(
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

/// Clean up a session workspace (ext4 file, directory, state file)
/// 清理 session 工作区（ext4 文件、目录、状态文件）
async fn cleanup_handler(
    State(state): State<AppState>,
    Json(req): Json<CleanupRequest>,
) -> impl IntoResponse {
    info!(
        "Received cleanup request (session_id={}, workspace_path={})",
        req.session_id, req.workspace_path
    );

    // SECURITY: Derive workspace path from session_id instead of trusting input.
    // 安全：由 session_id 推导工作区路径，而非信任入参。
    // This prevents directory traversal attacks entirely - we never use the untrusted path.
    // 这样彻底杜绝目录穿越攻击——完全不使用不可信路径。
    let session_id = req.session_id.trim();

    // Validate session_id contains no path traversal characters
    // 校验 session_id 不含路径穿越字符
    if session_id.is_empty() || session_id.contains('/') || session_id.contains('\\') || session_id.contains("..") {
        warn!(session_id = %session_id, "Cleanup rejected: invalid session_id");
        return (
            StatusCode::BAD_REQUEST,
            Json(CleanupResponse {
                deleted_ext4: false,
                deleted_directory: false,
                deleted_state_file: false,
                freed_bytes: 0,
            }),
        );
    }

    let efs_mount = state.settings.efs_mount_point.to_string_lossy();
    let efs_mount_str = efs_mount.trim_end_matches('/');
    let workspace_path = format!("{}/{}", efs_mount_str, session_id);
    let ext4_path = format!("{}.ext4", workspace_path);
    let state_file_path = format!("{}.state", ext4_path);

    // Acquire workspace lock before deleting to prevent conflicts with running executions
    // 删除前获取工作区锁，避免与正在执行的 request 冲突
    let lock = state.pool.get_workspace_lock(session_id).await;
    let _guard = lock.lock().await;

    let mut response = CleanupResponse {
        deleted_ext4: false,
        deleted_directory: false,
        deleted_state_file: false,
        freed_bytes: 0,
    };

    // Delete ext4 file
    // 删除 ext4 文件
    if std::path::Path::new(&ext4_path).exists() {
        if let Ok(metadata) = std::fs::metadata(&ext4_path) {
            response.freed_bytes += metadata.len();
        }
        match std::fs::remove_file(&ext4_path) {
            Ok(()) => {
                response.deleted_ext4 = true;
                info!(ext4_path = %ext4_path, "Deleted workspace ext4 file");
            }
            Err(e) => {
                warn!(ext4_path = %ext4_path, error = %e, "Failed to delete ext4 file");
            }
        }
    }

    // Delete directory
    // 删除目录
    if std::path::Path::new(&workspace_path).exists() {
        match std::fs::remove_dir_all(&workspace_path) {
            Ok(()) => {
                response.deleted_directory = true;
                info!(workspace_path = %workspace_path, "Deleted workspace directory");
            }
            Err(e) => {
                warn!(workspace_path = %workspace_path, error = %e, "Failed to delete workspace directory");
            }
        }
    }

    // Delete state file
    // 删除状态文件
    if std::path::Path::new(&state_file_path).exists() {
        match std::fs::remove_file(&state_file_path) {
            Ok(()) => {
                response.deleted_state_file = true;
                info!(state_file_path = %state_file_path, "Deleted workspace state file");
            }
            Err(e) => {
                warn!(state_file_path = %state_file_path, error = %e, "Failed to delete state file");
            }
        }
    }

    (StatusCode::OK, Json(response))
}

/// Infer MIME type from file extension
/// 根据文件扩展名推断 MIME 类型
fn infer_content_type(path: &str) -> String {
    let ext = path.rsplit('.').next().unwrap_or("").to_lowercase();
    match ext.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "json" => "application/json",
        "csv" => "text/csv",
        "txt" => "text/plain",
        "html" | "htm" => "text/html",
        "xml" => "application/xml",
        "pptx" => "application/vnd.openxmlformats-officedocument.presentationml.presentation",
        "xlsx" => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "docx" => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "zip" => "application/zip",
        "py" => "text/x-python",
        _ => "application/octet-stream",
    }
    .to_string()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter("info")
        .with_target(false)
        .json()
        .init();

    let settings = Settings::from_env();
    let pool = VmPool::new(settings.clone());

    // Spawn background task to maintain warm pool
    // 启动后台 task 维护预热池
    let pool_clone = pool.clone();
    tokio::spawn(async move {
        pool_clone.maintain_warm_pool().await;
    });

    let state = AppState {
        settings,
        pool,
    };

    let bind_addr =
        std::env::var("FIRECRACKER_EXECUTOR_BIND").unwrap_or_else(|_| "0.0.0.0:9001".to_string());

    let app = Router::new()
        .route("/execute", post(execute_handler))
        .route("/workspace/download", post(download_handler))
        .route("/workspace/list", post(list_files_handler))
        .route("/workspace/cleanup", post(cleanup_handler))
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state);

    info!("Firecracker executor listening on {}", bind_addr);

    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .expect("failed to bind");
    axum::serve(listener, app).await.expect("server failed");
}
