package skills

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// NewRegistry creates a new empty skill registry.
// NewRegistry 创建空的 skill 注册表。
func NewRegistry() *SkillRegistry {
	return &SkillRegistry{
		skills:     make(map[string]SkillEntry),
		byCategory: make(map[string][]string),
		byName:     make(map[string][]SkillEntry),
	}
}

// LoadDirectory scans a directory recursively for *.md skill files.
// LoadDirectory 递归扫描目录中的 *.md skill 文件。
func (r *SkillRegistry) LoadDirectory(root string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if directory exists
	info, err := os.Stat(root)
	if err != nil {
		if os.IsNotExist(err) {
			// Directory doesn't exist, skip silently (common for optional overlays)
			// 目录不存在时静默跳过（对可选覆盖层很常见）
			return nil
		}
		return fmt.Errorf("failed to stat directory %s: %w", root, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s is not a directory", root)
	}

	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-markdown files
		if d.IsDir() || filepath.Ext(path) != ".md" {
			return nil
		}

		// Skip README files
		if d.Name() == "README.md" {
			return nil
		}

		return r.loadFileLocked(path)
	})
}

// loadFileLocked loads a single skill file. Caller must hold the lock.
// loadFileLocked 加载单个 skill 文件。调用方必须持有锁。
func (r *SkillRegistry) loadFileLocked(path string) error {
	content, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read skill file %s: %w", path, err)
	}

	skill, err := LoadSkill(bytes.NewReader(content))
	if err != nil {
		return fmt.Errorf("failed to parse skill from %s: %w", path, err)
	}

	// Create registry entry
	key := fmt.Sprintf("%s@%s", skill.Name, skill.Version)
	entry := SkillEntry{
		Key:         key,
		Skill:       skill,
		SourcePath:  path,
		ContentHash: CalculateContentHash(content),
		LoadedAt:    time.Now(),
	}

	// Check for duplicate versioned key
	if existing, ok := r.skills[key]; ok {
		return fmt.Errorf("duplicate skill %s found in %s (already loaded from %s)",
			key, path, existing.SourcePath)
	}

	// Store by versioned key
	r.skills[key] = entry

	// Track all versions by name
	r.byName[skill.Name] = append(r.byName[skill.Name], entry)

	// Update latest pointer (highest version)
	if latest, ok := r.skills[skill.Name]; !ok || CompareVersions(skill.Version, latest.Skill.Version) > 0 {
		r.skills[skill.Name] = entry
	}

	// Index by category
	if skill.Category != "" {
		r.byCategory[skill.Category] = append(r.byCategory[skill.Category], key)
	}

	return nil
}

// Get retrieves a skill by key (name or name@version).
// Returns the skill entry and true if found, or empty entry and false if not.
// Get 按 key（name 或 name@version）获取 skill。
// 找到时返回 skill 条目与 true，否则返回空条目与 false。
func (r *SkillRegistry) Get(key string) (SkillEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, ok := r.skills[key]
	return entry, ok
}

// List returns all skills as summaries (latest versions only).
// List 以摘要形式返回所有 skill（仅最新版本）。
func (r *SkillRegistry) List() []SkillSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool)
	var summaries []SkillSummary

	for _, entry := range r.skills {
		// Only include each skill once (latest version)
		// 每个 skill 只包含一次（最新版本）
		if seen[entry.Skill.Name] {
			continue
		}

		// Skip if this is a versioned key and not the latest
		// 若为带版本的键且不是最新版本则跳过
		latest, ok := r.skills[entry.Skill.Name]
		if ok && latest.Key != entry.Key {
			continue
		}

		seen[entry.Skill.Name] = true
		summaries = append(summaries, SkillSummary{
			Name:          entry.Skill.Name,
			Version:       entry.Skill.Version,
			Category:      entry.Skill.Category,
			Description:   entry.Skill.Description,
			RequiresTools: entry.Skill.RequiresTools,
			Dangerous:     entry.Skill.Dangerous,
			Enabled:       entry.Skill.Enabled,
		})
	}

	// Sort by name for consistent output
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})

	return summaries
}

// ListByCategory filters skills by category.
func (r *SkillRegistry) ListByCategory(category string) []SkillSummary {
	r.mu.RLock()
	defer r.mu.RUnlock()

	keys, ok := r.byCategory[category]
	if !ok {
		return []SkillSummary{}
	}

	seen := make(map[string]bool)
	var summaries []SkillSummary

	for _, key := range keys {
		entry, ok := r.skills[key]
		if !ok {
			continue
		}

		// Deduplicate by name (in case multiple versions in same category)
		// 按名称去重（防止同一分类下存在多个版本）
		if seen[entry.Skill.Name] {
			continue
		}
		seen[entry.Skill.Name] = true

		summaries = append(summaries, SkillSummary{
			Name:          entry.Skill.Name,
			Version:       entry.Skill.Version,
			Category:      entry.Skill.Category,
			Description:   entry.Skill.Description,
			RequiresTools: entry.Skill.RequiresTools,
			Dangerous:     entry.Skill.Dangerous,
			Enabled:       entry.Skill.Enabled,
		})
	}

	// Sort by name for consistent output
	sort.Slice(summaries, func(i, j int) bool {
		return summaries[i].Name < summaries[j].Name
	})

	return summaries
}

// Categories returns all unique categories.
func (r *SkillRegistry) Categories() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	categories := make([]string, 0, len(r.byCategory))
	for cat := range r.byCategory {
		categories = append(categories, cat)
	}
	sort.Strings(categories)
	return categories
}

// Count returns the total number of unique skills (by name, not version).
// Count 返回去重后的 skill 总数（按名称，非按版本）。
func (r *SkillRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byName)
}

// Finalize validates all loaded skills after all directories are loaded.
// Call this after all LoadDirectory calls complete.
// Finalize 在所有目录加载完成后校验全部已加载的 skill。
// 需在所有 LoadDirectory 调用之后调用。
func (r *SkillRegistry) Finalize() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Sort versions for each skill name (descending)
	// 为每个 skill 名称的版本排序（降序）
	for name, entries := range r.byName {
		sort.Slice(entries, func(i, j int) bool {
			return CompareVersions(entries[i].Skill.Version, entries[j].Skill.Version) > 0
		})
		r.byName[name] = entries
	}

	// Validate that dangerous skills have descriptions
	for _, entry := range r.skills {
		if entry.Skill.Dangerous && entry.Skill.Description == "" {
			return fmt.Errorf("dangerous skill %q must have a description", entry.Skill.Name)
		}
	}

	return nil
}

// GetVersions returns all versions of a skill by name, sorted descending.
// GetVersions 按名称返回某个 skill 的所有版本，降序排列。
func (r *SkillRegistry) GetVersions(name string) []SkillEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entries, ok := r.byName[name]
	if !ok {
		return nil
	}

	// Return a copy to avoid mutation
	result := make([]SkillEntry, len(entries))
	copy(result, entries)
	return result
}
