package openai

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/attachments"
)

// RawAttachment holds extracted binary data from a content block.
// RawAttachment 保存从 content block 中提取出的二进制数据。
type RawAttachment struct {
	MediaType  string // e.g. "image/png", "application/pdf" 注：例如 "image/png"、"application/pdf"
	Data       []byte // decoded binary (for base64 sources) 注：解码后的二进制（针对 base64 来源）
	URL        string // original URL (for URL sources) 注：原始 URL（针对 URL 来源）
	SourceType string // "base64" or "url" 注：取值为 "base64" 或 "url"
	Filename   string // optional original filename 注：可选的原始文件名
}

// ParsedContent is the result of parsing a content field.
// ParsedContent 是解析 content 字段后的结果。
type ParsedContent struct {
	Text        string
	Attachments []RawAttachment
}

// ContentBlock represents one element of an OpenAI-compatible content array.
// ContentBlock 表示 OpenAI 兼容 content 数组中的一个元素。
type ContentBlock struct {
	Type     string    `json:"type"`
	Text     string    `json:"text,omitempty"`
	ImageURL *ImageURL `json:"image_url,omitempty"`
	File     *FileData `json:"file,omitempty"`
}

// ImageURL represents an image_url content block.
// ImageURL 表示一个 image_url content block。
type ImageURL struct {
	URL    string `json:"url"`
	Detail string `json:"detail,omitempty"`
}

// FileData represents a file content block.
// FileData 表示一个 file content block。
type FileData struct {
	FileData string `json:"file_data"`
	Filename string `json:"filename,omitempty"`
}

// ParseContent handles both string and []ContentBlock formats for the OpenAI content field.
// ParseContent 处理 OpenAI content 字段的 string 与 []ContentBlock 两种格式。
func ParseContent(raw json.RawMessage) (*ParsedContent, error) {
	if len(raw) == 0 {
		return &ParsedContent{}, nil
	}

	// Try string first
	// 先尝试按 string 解析。
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return &ParsedContent{Text: s}, nil
	}

	// Try array of content blocks
	// 再尝试按 content block 数组解析。
	var blocks []ContentBlock
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return nil, fmt.Errorf("content must be string or array of content blocks: %w", err)
	}

	result := &ParsedContent{}
	var textParts []string
	var totalBytes int

	for _, block := range blocks {
		switch block.Type {
		case "text":
			textParts = append(textParts, block.Text)

		case "image_url":
			if block.ImageURL == nil {
				continue
			}
			att, size, err := parseImageURL(block.ImageURL.URL)
			if err != nil {
				return nil, fmt.Errorf("parse image_url: %w", err)
			}
			totalBytes += size
			if totalBytes > attachments.MaxDecodedAttachmentBytes {
				return nil, fmt.Errorf("total attachment size %d bytes exceeds %d byte limit", totalBytes, attachments.MaxDecodedAttachmentBytes)
			}
			result.Attachments = append(result.Attachments, *att)

		case "file":
			if block.File == nil || block.File.FileData == "" {
				continue
			}
			att, size, err := parseDataURI(block.File.FileData)
			if err != nil {
				return nil, fmt.Errorf("parse file data: %w", err)
			}
			if block.File.Filename != "" {
				att.Filename = block.File.Filename
			}
			totalBytes += size
			if totalBytes > attachments.MaxDecodedAttachmentBytes {
				return nil, fmt.Errorf("total attachment size %d bytes exceeds %d byte limit", totalBytes, attachments.MaxDecodedAttachmentBytes)
			}
			result.Attachments = append(result.Attachments, *att)
		}
	}

	result.Text = strings.Join(textParts, "\n")
	return result, nil
}

// parseImageURL parses an image_url value, which can be a data URI or an external URL.
// parseImageURL 解析 image_url 取值，它可以是 data URI 或外部 URL。
func parseImageURL(url string) (*RawAttachment, int, error) {
	if strings.HasPrefix(url, "data:") {
		return parseDataURI(url)
	}
	mt := guessMediaType(url)
	// URL-based path: only reject dangerous types (executables / installers).
	// Unknown extensions fall through to a URL-only ref — the daemon fetches
	// the URL and uses local tools (bash, archive_extract) to make sense of
	// it. See attachment-format-parity §2 P0.
	// URL 路径：只拒绝危险类型（可执行文件/安装包）。未知扩展名按 URL 引用透传——daemon 会拉取该 URL，
	// 并用本地 tool（bash、archive_extract）解析。见 attachment-format-parity §2 P0。
	if attachments.IsDangerous(mt) {
		return nil, 0, fmt.Errorf("rejected attachment type from URL: %s", mt)
	}
	return &RawAttachment{
		URL:        url,
		SourceType: "url",
		MediaType:  mt,
	}, 0, nil
}

// parseDataURI decodes a data URI (data:<mediatype>;base64,<data>) into a RawAttachment.
// parseDataURI 把 data URI（data:<mediatype>;base64,<data>）解码为 RawAttachment。
func parseDataURI(uri string) (*RawAttachment, int, error) {
	parts := strings.SplitN(uri, ",", 2)
	if len(parts) != 2 {
		return nil, 0, fmt.Errorf("invalid data URI format")
	}
	header := parts[0]
	b64Data := parts[1]

	// Extract media type from header like "data:image/png;base64"
	// 从形如 "data:image/png;base64" 的 header 中提取 media type。
	mediaType := ""
	if after, ok := strings.CutPrefix(header, "data:"); ok {
		mediaType = after
		mediaType = strings.TrimSuffix(mediaType, ";base64")
	}

	// Reject empty MIME types early (e.g. "data:;base64,..."). Base64 inline
	// uploads must be IsExtractable — passthrough types (archives / a/v /
	// octet-stream) should arrive as URL-only refs, not inlined. Dangerous
	// types are always rejected.
	// 尽早拒绝空 MIME（如 "data:;base64,..."）。内联 base64 上传必须是 IsExtractable 类型；
	// passthrough 类型（归档/音视频/octet-stream）应以 URL 引用而非内联形式到达；危险类型一律拒绝。
	if mediaType == "" {
		return nil, 0, fmt.Errorf("missing media type in data URI (expected data:<type>;base64,...)")
	}
	if attachments.IsDangerous(mediaType) {
		return nil, 0, fmt.Errorf("rejected attachment type: %s (executables / installers are not accepted)", mediaType)
	}
	if !attachments.IsExtractable(mediaType) {
		return nil, 0, fmt.Errorf("unsupported attachment type for inline upload: %s (use URL-based attachment instead)", mediaType)
	}

	// Try standard base64 first, then raw (no padding) variant
	// 先尝试标准 base64，再尝试无 padding 的 raw 变体。
	data, err := base64.StdEncoding.DecodeString(b64Data)
	if err != nil {
		data, err = base64.RawStdEncoding.DecodeString(b64Data)
		if err != nil {
			return nil, 0, fmt.Errorf("base64 decode: %w", err)
		}
	}

	return &RawAttachment{
		MediaType:  mediaType,
		Data:       data,
		SourceType: "base64",
	}, len(data), nil
}

// guessMediaType infers a media type from a URL's file extension.
// guessMediaType 由 URL 的文件扩展名推断 media type。
func guessMediaType(url string) string {
	lower := strings.ToLower(url)
	// Strip query string and fragment for suffix matching
	// 去掉 query string 与 fragment，以便做后缀匹配。
	if idx := strings.IndexAny(lower, "?#"); idx != -1 {
		lower = lower[:idx]
	}
	switch {
	case strings.HasSuffix(lower, ".png"):
		return "image/png"
	case strings.HasSuffix(lower, ".jpg"), strings.HasSuffix(lower, ".jpeg"):
		return "image/jpeg"
	case strings.HasSuffix(lower, ".gif"):
		return "image/gif"
	case strings.HasSuffix(lower, ".webp"):
		return "image/webp"
	case strings.HasSuffix(lower, ".pdf"):
		return "application/pdf"
	default:
		return "" // Unknown extension; caller should reject or require explicit media_type 注：未知扩展名；调用方应拒绝或要求显式传入 media_type
	}
}
