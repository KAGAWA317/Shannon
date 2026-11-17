package skills

import (
	"os"
	"path/filepath"
	"strings"
)

// DefaultSkillDirs are the default directories to search for skills.
// These are tried in order; missing directories are skipped.
// DefaultSkillDirs 是搜索 skill 的默认目录。
// 按顺序尝试；缺失的目录跳过。
var DefaultSkillDirs = []string{
	"config/skills/core",      // Development: relative to working directory 注：开发环境：相对于工作目录
	"/app/config/skills/core", // Container: mounted config path 注：容器环境：挂载的 config 路径
}

// ResolveSkillDirs returns the skill directories to scan.
//
// If SKILLS_PATH environment variable is set, it's used as a path-separated
// list of directories (like PATH). Otherwise, DefaultSkillDirs are used.
//
// Order matters for potential future override semantics. For now, we load
// everything and reject duplicates.
// ResolveSkillDirs 返回要扫描的 skill 目录。
// 若设置了 SKILLS_PATH 环境变量，则以其作为路径分隔的目录列表
// （类似 PATH）。否则使用 DefaultSkillDirs。
// 顺序对将来可能的覆盖语义有意义。目前会全部加载并拒绝重复项。
func ResolveSkillDirs() []string {
	if env := strings.TrimSpace(os.Getenv("SKILLS_PATH")); env != "" {
		return splitSearchPaths(env)
	}
	return DefaultSkillDirs
}

// splitSearchPaths splits a path-list string (like PATH) into individual paths.
// Empty entries are skipped and paths are cleaned.
// splitSearchPaths 把类似 PATH 的路径列表字符串拆成单个路径。
// 跳过空项并清洗路径。
func splitSearchPaths(value string) []string {
	parts := strings.Split(value, string(os.PathListSeparator))
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, filepath.Clean(p))
		}
	}
	return out
}

// SkillsPathEnvVar is the environment variable name for custom skill paths.
// SkillsPathEnvVar 是自定义 skill 路径的环境变量名。
const SkillsPathEnvVar = "SKILLS_PATH"
