use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct ExecuteRequest {
    pub code: String,
    pub session_id: Option<String>,
    pub timeout_seconds: Option<u32>,
    pub workspace_path: Option<String>,
    pub stdin: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct ExecuteResponse {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub error: Option<String>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GuestRequest {
    pub code: String,
    pub stdin: Option<String>,
    pub timeout_seconds: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct GuestResponse {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub error: Option<String>,
}

/// Request to download a file from session workspace
/// 从 session 工作区下载文件的 request
#[derive(Debug, Deserialize)]
pub struct DownloadRequest {
    pub session_id: String,
    pub file_path: String,
    pub workspace_path: String,
}

/// Response containing file content (base64 for binary safety)
/// 包含文件内容的 response（base64 编码以保证二进制安全）
#[derive(Debug, Serialize)]
pub struct DownloadResponse {
    pub success: bool,
    pub content: Option<String>,      // base64-encoded file content
    pub content_type: Option<String>, // MIME type hint
    pub size_bytes: Option<u64>,
    pub error: Option<String>,
}

/// Request to list files in session workspace
/// 列出 session 工作区中文件的 request
#[derive(Debug, Deserialize)]
pub struct ListFilesRequest {
    pub session_id: String,
    pub workspace_path: String,
    pub path: Option<String>, // subdirectory, defaults to /workspace
}

/// File metadata for listing
/// 用于列表展示的文件 metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct FileInfo {
    pub name: String,
    pub path: String,
    pub is_dir: bool,
    pub size_bytes: u64,
}

/// Response for file listing
/// 文件列表 response
#[derive(Debug, Serialize)]
pub struct ListFilesResponse {
    pub success: bool,
    pub files: Vec<FileInfo>,
    pub error: Option<String>,
}

/// Request to clean up a session workspace
/// 清理 session 工作区的 request
#[derive(Debug, Deserialize)]
pub struct CleanupRequest {
    pub session_id: String,
    pub workspace_path: String,
}

/// Response for workspace cleanup
/// 工作区清理 response
#[derive(Debug, Serialize)]
pub struct CleanupResponse {
    pub deleted_ext4: bool,
    pub deleted_directory: bool,
    pub deleted_state_file: bool,
    pub freed_bytes: u64,
}
