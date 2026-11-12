package skills

import (
	"bufio"
	"bytes"
	"cmp"
	"crypto/sha256"
	"fmt"
	"io"
	"strings"

	"gopkg.in/yaml.v3"
)

// LoadSkill parses a Markdown file with YAML frontmatter.
// The file must start with "---", followed by YAML frontmatter,
// then another "---", and finally the markdown content.
// LoadSkill 解析带 YAML frontmatter 的 Markdown 文件。
// 文件必须以 "---" 开头，随后是 YAML frontmatter，
// 再接另一个 "---"，最后是 markdown 正文。
func LoadSkill(reader io.Reader) (*Skill, error) {
	// Use a large buffer to support skills >64KB
	// 使用较大缓冲区以支持超过 64KB 的 skill
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)

	// Expect first line to be "---"
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, fmt.Errorf("failed to read skill file: %w", err)
		}
		return nil, fmt.Errorf("skill file is empty")
	}

	firstLine := strings.TrimSpace(scanner.Text())
	if firstLine != "---" {
		return nil, fmt.Errorf("skill file must start with YAML frontmatter (---), got: %q", firstLine)
	}

	// Read YAML frontmatter until second "---"
	var frontmatter bytes.Buffer
	foundEnd := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "---" {
			foundEnd = true
			break
		}
		frontmatter.WriteString(line + "\n")
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading frontmatter: %w", err)
	}

	if !foundEnd {
		return nil, fmt.Errorf("unterminated YAML frontmatter (missing closing ---)")
	}

	// Parse YAML frontmatter
	// Set Enabled=true as default before unmarshaling since Go's zero value is false
	// 解析 YAML frontmatter
	// 反序列化前先把 Enabled 默认设为 true，因为 Go 的零值是 false
	skill := Skill{Enabled: true}
	if err := yaml.Unmarshal(frontmatter.Bytes(), &skill); err != nil {
		return nil, fmt.Errorf("failed to parse YAML frontmatter: %w", err)
	}

	// Read remaining content as Markdown
	var content bytes.Buffer
	for scanner.Scan() {
		content.WriteString(scanner.Text() + "\n")
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading markdown content: %w", err)
	}

	skill.Content = strings.TrimSpace(content.String())

	// Validate required fields
	if err := validateSkill(&skill); err != nil {
		return nil, err
	}

	return &skill, nil
}

// validateSkill checks that required fields are present and applies defaults.
// validateSkill 检查必需字段是否存在并应用默认值。
func validateSkill(skill *Skill) error {
	if skill.Name == "" {
		return fmt.Errorf("skill name is required")
	}

	// Validate name format (alphanumeric, hyphens, underscores)
	for _, r := range skill.Name {
		if !isValidNameChar(r) {
			return fmt.Errorf("skill name contains invalid character: %q (allowed: a-z, 0-9, -, _)", r)
		}
	}

	// Apply defaults
	if skill.Version == "" {
		skill.Version = "1.0.0"
	}

	// Note: Enabled defaults to true (set before YAML unmarshal above).
	// Explicit "enabled: false" in YAML will override the default.
	// 注意：Enabled 默认为 true（在上面的 YAML 反序列化之前已设置）。
	// YAML 中显式写 "enabled: false" 会覆盖该默认值。

	if skill.Content == "" {
		return fmt.Errorf("skill content is empty")
	}

	return nil
}

// isValidNameChar returns true if the character is valid in a skill name.
// isValidNameChar 判断字符是否为 skill 名称中的合法字符。
func isValidNameChar(r rune) bool {
	return (r >= 'a' && r <= 'z') ||
		(r >= 'A' && r <= 'Z') ||
		(r >= '0' && r <= '9') ||
		r == '-' || r == '_'
}

// CalculateContentHash computes SHA256 hash of skill file content.
// CalculateContentHash 计算 skill 文件内容的 SHA256 哈希。
func CalculateContentHash(content []byte) string {
	hash := sha256.Sum256(content)
	return fmt.Sprintf("%x", hash)
}

// ParseVersion extracts major, minor, patch from a semver string.
// Returns (0, 0, 0) if parsing fails.
// ParseVersion 从 semver 字符串中解析 major、minor、patch。
// 解析失败时返回 (0, 0, 0)。
func ParseVersion(version string) (major, minor, patch int) {
	_, _ = fmt.Sscanf(version, "%d.%d.%d", &major, &minor, &patch)
	return
}

// CompareVersions returns:
//
//	-1 if a < b
//	 0 if a == b
//	 1 if a > b
// CompareVersions 返回：
// -1 表示 a < b
// 0 表示 a == b
// 1 表示 a > b
func CompareVersions(a, b string) int {
	aMaj, aMin, aPat := ParseVersion(a)
	bMaj, bMin, bPat := ParseVersion(b)

	if c := cmp.Compare(aMaj, bMaj); c != 0 {
		return c
	}
	if c := cmp.Compare(aMin, bMin); c != 0 {
		return c
	}
	return cmp.Compare(aPat, bPat)
}
