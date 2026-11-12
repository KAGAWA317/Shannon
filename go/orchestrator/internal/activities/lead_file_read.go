package activities

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.temporal.io/sdk/activity"
)

// ReadWorkspaceFileInput is the input for the ReadWorkspaceFile activity.
type ReadWorkspaceFileInput struct {
	SessionID string `json:"session_id"`
	Path      string `json:"path"`
	MaxChars  int    `json:"max_chars"`
}

// ReadWorkspaceFileResult is the output of the ReadWorkspaceFile activity.
type ReadWorkspaceFileResult struct {
	Path      string `json:"path"`
	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`
	Error     string `json:"error,omitempty"`
}

// ReadWorkspaceFile reads a single file from the session workspace.
// Used by Lead's file_read action to independently verify agent output.
// ReadWorkspaceFile 从 session workspace 读取单个文件。
// 供 Lead 的 file_read action 调用，用于独立核对 agent 输出。
func ReadWorkspaceFile(ctx context.Context, in ReadWorkspaceFileInput) (ReadWorkspaceFileResult, error) {
	logger := activity.GetLogger(ctx)

	if in.SessionID == "" || in.Path == "" {
		return ReadWorkspaceFileResult{Path: in.Path, Error: "empty session_id or path"}, nil
	}
	// Validate SessionID — only alphanumeric, hyphen, and underscore allowed.
	// Rejects path separators that would shift the workspace root before the
	// symlink containment check runs (e.g. session_id="../other-session").
	// 校验 SessionID —— 只允许字母数字、连字符和下划线。
	// 拒绝会改变项目根目录的 path separator，避免在 symlink containment
	// 检查执行之前就被绕过（例如 session_id="../other-session"）。
	if !isValidSessionID(in.SessionID) {
		return ReadWorkspaceFileResult{Path: in.Path, Error: "invalid session_id"}, nil
	}
	if in.MaxChars <= 0 {
		in.MaxChars = 4000
	}

	// Sanitize path — prevent directory traversal and symlink escape
	// 清洗 path —— 防止 directory traversal 和 symlink escape
	cleanPath := filepath.Clean(in.Path)
	// Strip common LLM-hallucinated prefixes — Lead sometimes outputs
	// "workspace/file.md" or "/workspace/file.md" because agent protocol
	// teaches /workspace/ prefix for python_executor. ReadWorkspaceFile
	// already roots at the session dir, so the prefix is redundant.
	// 去掉 LLM 常见的幻觉前缀 —— Lead 有时会输出 "workspace/file.md"
	// 或 "/workspace/file.md"，因为 agent protocol 教给 python_executor
	// 使用 /workspace/ 前缀。ReadWorkspaceFile 本身已以 session 目录为
	// 根，所以该前缀是多余的。
	cleanPath = strings.TrimPrefix(cleanPath, "workspace/")
	cleanPath = strings.TrimPrefix(cleanPath, "workspace")
	if cleanPath == "" || cleanPath == "." {
		cleanPath = "."
	}
	if strings.HasPrefix(cleanPath, "..") || filepath.IsAbs(cleanPath) {
		return ReadWorkspaceFileResult{Path: in.Path, Error: "invalid path"}, nil
	}

	baseDir := getenv("SHANNON_SESSION_WORKSPACES_DIR", "/tmp/shannon-sessions")
	fullPath := filepath.Join(baseDir, in.SessionID, cleanPath)

	// Resolve symlinks and verify the real path stays within the session workspace.
	// Both the session dir and the target are resolved so that path-level symlinks
	// (e.g. macOS /tmp -> /private/tmp) do not cause false prefix mismatches,
	// while symlink escapes (e.g. workspace/link -> /etc/passwd) are still blocked.
	// 解析 symlink 并确认真实路径仍在 session workspace 内。
	// session 目录和目标路径都要解析，避免路径层面的 symlink
	// （例如 macOS /tmp -> /private/tmp）造成前缀误判，
	// 同时仍能拦住 symlink 逃逸（例如 workspace/link -> /etc/passwd）。
	sessionDir := filepath.Join(baseDir, in.SessionID)
	realSessionDir, _ := filepath.EvalSymlinks(sessionDir)
	if realSessionDir == "" {
		realSessionDir = sessionDir
	}
	realPath, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		// File does not exist or cannot be resolved (e.g. broken symlink).
		// 文件不存在或无法解析（例如 broken symlink）。
		logger.Info("Lead file_read: file not found", "path", in.Path, "error", err)
		return ReadWorkspaceFileResult{Path: in.Path, Error: fmt.Sprintf("file not found: %s", in.Path)}, nil
	}
	if !strings.HasPrefix(realPath, realSessionDir+string(filepath.Separator)) {
		return ReadWorkspaceFileResult{Path: in.Path, Error: "invalid path"}, nil
	}

	data, err := os.ReadFile(realPath)
	if err != nil {
		logger.Info("Lead file_read: file not found", "path", in.Path, "error", err)
		return ReadWorkspaceFileResult{Path: in.Path, Error: fmt.Sprintf("file not found: %s", in.Path)}, nil
	}

	content := string(data)
	truncated := false
	if len(content) > in.MaxChars {
		content = content[:in.MaxChars]
		truncated = true
	}

	logger.Info("Lead file_read successful", "path", in.Path, "size", len(data), "truncated", truncated)
	return ReadWorkspaceFileResult{
		Path:      in.Path,
		Content:   content,
		Truncated: truncated,
	}, nil
}

// isValidSessionID returns true if the session ID is non-empty, at most 128
// characters, and contains only ASCII alphanumeric characters, hyphens, and
// underscores. This matches the validation in the Python file_ops layer
// (_validate_session_id) and prevents path traversal via the session ID itself.
// isValidSessionID 判断 session ID 是否非空、不超过 128 个字符，且只含
// ASCII 字母数字、连字符和下划线。该规则与 Python file_ops 层的校验
// （_validate_session_id）一致，用于防止通过 session ID 本身做 path traversal。
func isValidSessionID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, c := range id {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

// readFileContent is the pure logic extracted for testing without activity context.
// readFileContent 是抽出来的纯逻辑，便于在没有 activity context 时测试。
func readFileContent(sessionID, path, baseDir string, maxChars int) ReadWorkspaceFileResult {
	if !isValidSessionID(sessionID) {
		return ReadWorkspaceFileResult{Path: path, Error: "invalid session_id"}
	}
	cleanPath := filepath.Clean(path)
	// Strip LLM-hallucinated workspace/ prefix (mirrors ReadWorkspaceFile above)
	// 去掉 LLM 幻觉出的 workspace/ 前缀（与上面的 ReadWorkspaceFile 一致）
	cleanPath = strings.TrimPrefix(cleanPath, "workspace/")
	cleanPath = strings.TrimPrefix(cleanPath, "workspace")
	if cleanPath == "" || cleanPath == "." {
		cleanPath = "."
	}
	if strings.HasPrefix(cleanPath, "..") || filepath.IsAbs(cleanPath) {
		return ReadWorkspaceFileResult{Path: path, Error: "invalid path"}
	}
	fullPath := filepath.Join(baseDir, sessionID, cleanPath)
	sessionDir := filepath.Join(baseDir, sessionID)
	realSessionDir, _ := filepath.EvalSymlinks(sessionDir)
	if realSessionDir == "" {
		realSessionDir = sessionDir
	}
	realPath, err := filepath.EvalSymlinks(fullPath)
	if err != nil {
		return ReadWorkspaceFileResult{Path: path, Error: "file not found: " + path}
	}
	if !strings.HasPrefix(realPath, realSessionDir+string(filepath.Separator)) {
		return ReadWorkspaceFileResult{Path: path, Error: "invalid path"}
	}
	data, err := os.ReadFile(realPath)
	if err != nil {
		return ReadWorkspaceFileResult{Path: path, Error: "file not found: " + path}
	}
	content := string(data)
	truncated := false
	if len(content) > maxChars {
		content = content[:maxChars]
		truncated = true
	}
	return ReadWorkspaceFileResult{Path: path, Content: content, Truncated: truncated}
}
