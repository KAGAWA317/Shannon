package handlers

import (
	"encoding/base64"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"go.uber.org/zap"
)

// MemoryHandler handles user memory file operations.
type MemoryHandler struct {
	logger      *zap.Logger
	memoryBaseDir string
}

// NewMemoryHandler creates a new memory handler.
func NewMemoryHandler(logger *zap.Logger) *MemoryHandler {
	memoryBaseDir := os.Getenv("SHANNON_USER_MEMORY_DIR")
	if memoryBaseDir == "" {
		memoryBaseDir = "/tmp/shannon-users"
	}

	return &MemoryHandler{
		logger:      logger,
		memoryBaseDir: memoryBaseDir,
	}
}

// ListMemoryFiles handles GET /api/v1/memory/files
// Lists files in the authenticated user's memory directory.
// user_id is derived from the auth token — never from URL input.
// ListMemoryFiles 处理 GET /api/v1/memory/files：列出 authenticated 用户 memory 目录下的文件；
// user_id 只来自 auth token，绝不取自 URL 输入。
func (h *MemoryHandler) ListMemoryFiles(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	userID := userCtx.UserID.String()

	// Validate user_id for filesystem safety (UUID format is always safe, but defense in depth)
	// 校验 user_id 以保证文件系统安全（UUID 格式本身安全，这里是纵深防御）。
	if err := validateMemoryUserID(userID); err != nil {
		h.logger.Error("Invalid user_id from auth context", zap.String("user_id", userID), zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	memoryDir := filepath.Join(h.memoryBaseDir, userID, "memory")

	h.logger.Info("Listing user memory files",
		zap.String("user_id", userID),
		zap.String("memory_dir", memoryDir),
	)

	// Resolve and verify path stays within base
	// 解析路径并确认没有逃出基目录。
	absTarget, err := filepath.Abs(memoryDir)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	absBase, err := filepath.Abs(h.memoryBaseDir)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	if !strings.HasPrefix(absTarget, absBase+string(filepath.Separator)) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path escapes base directory"})
		return
	}

	// If memory directory doesn't exist, return empty list
	entries, err := os.ReadDir(absTarget)
	if err != nil {
		if os.IsNotExist(err) {
			writeJSON(w, http.StatusOK, ListFilesResponse{Success: true, Files: []FileInfo{}})
			return
		}
		h.logger.Error("Failed to read memory directory", zap.Error(err))
		errMsg := "failed to read directory"
		writeJSON(w, http.StatusInternalServerError, ListFilesResponse{Success: false, Error: &errMsg})
		return
	}

	files := make([]FileInfo, 0, len(entries))
	for _, entry := range entries {
		if entry.Name() == "lost+found" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, FileInfo{
			Name:      entry.Name(),
			Path:      entry.Name(),
			IsDir:     entry.IsDir(),
			SizeBytes: uint64(info.Size()),
		})
	}

	writeJSON(w, http.StatusOK, ListFilesResponse{Success: true, Files: files})
}

// DownloadMemoryFile handles GET /api/v1/memory/files/{path...}
// Downloads a file from the authenticated user's memory directory.
// DownloadMemoryFile 处理 GET /api/v1/memory/files/{path...}：从 authenticated 用户的 memory 目录下载文件。
func (h *MemoryHandler) DownloadMemoryFile(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}

	filePath := r.PathValue("path")
	if filePath == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "file path is required"})
		return
	}

	// Security: reject path traversal
	if strings.Contains(filePath, "..") || strings.HasPrefix(filePath, "/") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid path"})
		return
	}

	userID := userCtx.UserID.String()
	if err := validateMemoryUserID(userID); err != nil {
		h.logger.Error("Invalid user_id from auth context", zap.String("user_id", userID), zap.Error(err))
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}

	memoryDir := filepath.Join(h.memoryBaseDir, userID, "memory")

	h.logger.Info("Downloading memory file",
		zap.String("user_id", userID),
		zap.String("file_path", filePath),
	)

	// Reuse the workspace download pattern (text vs binary, path escape checks)
	// 复用 workspace 的下载逻辑（区分文本/二进制、路径逃逸检查）。
	h.downloadFileFromDir(w, memoryDir, filePath)
}

// downloadFileFromDir reads a file from a directory with security checks.
// downloadFileFromDir 在带安全检查的前提下从目录读取文件。
func (h *MemoryHandler) downloadFileFromDir(w http.ResponseWriter, baseDir, filePath string) {
	fullPath := filepath.Join(baseDir, filePath)

	absPath, err := filepath.Abs(fullPath)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid path"})
		return
	}
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	if !strings.HasPrefix(absPath, absBase+string(filepath.Separator)) && absPath != absBase {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "path escapes directory"})
		return
	}

	info, err := os.Stat(absPath)
	if err != nil {
		if os.IsNotExist(err) {
			errMsg := "file not found"
			writeJSON(w, http.StatusNotFound, DownloadResponse{Success: false, Error: &errMsg})
			return
		}
		h.logger.Error("Failed to stat file", zap.Error(err))
		errMsg := "failed to read file"
		writeJSON(w, http.StatusInternalServerError, DownloadResponse{Success: false, Error: &errMsg})
		return
	}

	if info.IsDir() {
		errMsg := "path is a directory"
		writeJSON(w, http.StatusBadRequest, DownloadResponse{Success: false, Error: &errMsg})
		return
	}

	data, err := os.ReadFile(absPath)
	if err != nil {
		h.logger.Error("Failed to read file", zap.Error(err))
		errMsg := "failed to read file"
		writeJSON(w, http.StatusInternalServerError, DownloadResponse{Success: false, Error: &errMsg})
		return
	}

	ct := detectContentType(filePath)
	size := uint64(len(data))

	var content string
	if isTextContentType(ct) {
		content = string(data)
	} else {
		content = base64.StdEncoding.EncodeToString(data)
	}

	writeJSON(w, http.StatusOK, DownloadResponse{
		Success:     true,
		Content:     &content,
		ContentType: &ct,
		SizeBytes:   &size,
	})
}

// validateMemoryUserID validates a user_id for filesystem path safety.
// validateMemoryUserID 校验 user_id 可安全用于文件系统路径。
func validateMemoryUserID(userID string) error {
	// UUIDs from auth context are always safe (hex + hyphens),
	// but validate defensively in case format changes.
	// auth context 里的 UUID 本身安全（十六进制 + 连字符），仍做防御性校验以防格式变化。
	return validateWorkspaceSessionID(userID)
}
