// Package skills implements a markdown-based skill system compatible with
// Anthropic's Agent Skills specification.
//
// Skills are markdown files with YAML frontmatter that define reusable
// system prompts, tool requirements, and execution constraints.
// skills 包实现与 Anthropic Agent Skills 规范兼容的基于 markdown 的 skill 系统。
// skill 是带 YAML frontmatter 的 markdown 文件，定义可复用的
// system prompt、tool 需求与执行约束。
package skills

import (
	"sync"
	"time"
)

// Skill represents a parsed skill definition from a markdown file.
// Skill 表示从 markdown 文件中解析出的 skill 定义。
type Skill struct {
	Name          string         `yaml:"name"`
	Version       string         `yaml:"version"`
	Author        string         `yaml:"author"`
	Category      string         `yaml:"category"`
	Description   string         `yaml:"description"`
	RequiresTools []string       `yaml:"requires_tools"`
	RequiresRole  string         `yaml:"requires_role"`
	BudgetMax     int            `yaml:"budget_max"`
	Dangerous     bool           `yaml:"dangerous"`
	Enabled       bool           `yaml:"enabled"`
	Metadata      map[string]any `yaml:"metadata"`
	Content       string         `yaml:"-"` // Markdown content after frontmatter 注：frontmatter 之后的 Markdown 内容
}

// SkillRegistry manages loaded skills with thread-safe access.
// SkillRegistry 以线程安全方式管理已加载的 skill。
type SkillRegistry struct {
	mu         sync.RWMutex
	skills     map[string]SkillEntry   // Key: "name@version" or "name" for latest 注：键为 "name@version"，或 "name" 表示最新版本
	byCategory map[string][]string     // Category -> skill keys 注：类别 -> skill 键
	byName     map[string][]SkillEntry // Name -> all versions (sorted by version desc) 注：名称 -> 所有版本（按版本降序）
}

// SkillEntry wraps a skill with loading metadata.
// SkillEntry 用加载 metadata 包装 skill。
type SkillEntry struct {
	Key         string
	Skill       *Skill
	SourcePath  string
	ContentHash string // SHA256 of file content 注：文件内容的 SHA256
	LoadedAt    time.Time
}

// SkillSummary is a lightweight representation for API responses.
// SkillSummary 是用于 API response 的轻量表示。
type SkillSummary struct {
	Name          string   `json:"name"`
	Version       string   `json:"version"`
	Category      string   `json:"category"`
	Description   string   `json:"description"`
	RequiresTools []string `json:"requires_tools"`
	Dangerous     bool     `json:"dangerous"`
	Enabled       bool     `json:"enabled"`
}

// SkillDetail includes full skill information for API responses.
// SkillDetail 包含用于 API response 的完整 skill 信息。
type SkillDetail struct {
	Skill    *Skill         `json:"skill"`
	Metadata map[string]any `json:"metadata"`
}
