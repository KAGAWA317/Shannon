package attachments

import (
	"path/filepath"
	"strings"
)

// dangerousExtensions lists filename extensions for executable / installer
// formats. Channel webhooks (Slack/Feishu/...) MUST check the filename in
// addition to the MIME type — Slack delivers many of these as
// `application/octet-stream`, which would otherwise slip past IsDangerous.
// dangerousExtensions 列出可执行文件/安装包格式的扩展名。
// 渠道 webhook（Slack/Feishu/…）除 MIME 类型外必须校验文件名——Slack 会把其中许多类型投递为 application/octet-stream，否则会绕过 IsDangerous。
var dangerousExtensions = map[string]struct{}{
	".exe":  {},
	".dll":  {},
	".bat":  {},
	".cmd":  {},
	".com":  {},
	".msi":  {},
	".scr":  {},
	".pif":  {},
	".vbs":  {},
	".vbe":  {},
	".ps1":  {},
	".psm1": {},
	".sh":   {},
	".bash": {},
	".zsh":  {},
	".csh":  {},
	".hta":  {},
	".jar":  {},
	".lnk":  {},
	".reg":  {},
}

// IsDangerousFilename reports whether the filename's extension is on the
// executable / installer denylist. Use alongside IsDangerous(mime) at any
// gateway boundary: a Slack-hosted .exe arrives as octet-stream + .exe
// basename, so the MIME check alone is insufficient.
// IsDangerousFilename 判断文件扩展名是否在可执行/安装包拒绝列表中。
// 任何网关边界都应与 IsDangerous(mime) 一起使用：Slack 上的 .exe 会以 octet-stream + .exe 文件名到达，仅靠 MIME 校验不足。
func IsDangerousFilename(name string) bool {
	if name == "" {
		return false
	}
	ext := strings.ToLower(filepath.Ext(name))
	if ext == "" {
		return false
	}
	_, ok := dangerousExtensions[ext]
	return ok
}

// MIME classification for the attachment-format-parity plan (§2 P0):
//
// Three layers, evaluated in order at the gateway:
//
//   1. IsDangerous  — executable / installer types. ALWAYS rejected.
//   2. IsExtractable — Cloud can extract text or transcode (PDF / DOCX /
//      XLSX / PPTX / CSV / TXT / JSON / HTML / HEIC / AVIF / TIFF / BMP /
//      image+text subtypes). Future Cloud extraction service handles these.
//   3. IsPassthrough — Cloud doesn't process but the daemon may still
//      handle (archives, video, audio, octet-stream). URL-only ref is
//      forwarded; the agent uses bash / archive_extract on its end.
//
// Anything that isn't dangerous and isn't on either list falls through
// to passthrough by default — "user can upload anything, daemon decides
// what to do with it". This is the "universal accept" principle from the
// plan.
//
// IsSupportedMediaType is retained as a deprecated thin wrapper for
// callers that haven't migrated to the explicit Extractable / Passthrough
// distinction yet. New code MUST NOT use it.
// 附件格式对齐计划（§2 P0）的 MIME 分类。网关按顺序评估三层：
// 1. IsDangerous —— 可执行/安装包类型，一律拒绝。
// 2. IsExtractable —— 云端可提取文本或转码（PDF/DOCX/XLSX/PPTX/CSV/TXT/JSON/HTML/HEIC/AVIF/TIFF/BMP/image+text 子类型），由未来的云端提取服务处理。
// 3. IsPassthrough —— 云端不处理但 daemon 仍可处理（压缩包、视频、音频、octet-stream）；仅转发 URL 引用，agent 在本地用 bash / archive_extract 处理。
// 既非危险也不在上述两类的类型默认按 passthrough 处理——“用户可上传任何内容，由 daemon 决定怎么处理”，即计划中的“通用接收”原则。
// IsSupportedMediaType 保留为已废弃的薄封装，供尚未迁移到 Extractable / Passthrough 明确区分的调用方使用；新代码不得使用。

// IsDangerous reports whether the media type is an executable / installer
// format that should never traverse the cloud → daemon pipeline regardless
// of size or origin. Anything matched here is rejected at the gateway.
//
// Filename-based detection (e.g. ".exe" basename when MIME says
// "application/octet-stream") is the responsibility of the caller — this
// helper only inspects the MIME string.
// IsDangerous 判断媒体类型是否属于可执行/安装包格式——无论大小或来源都不应经过云端 → daemon 管道，命中即在网关拒绝。
// 基于文件名的检测（如 MIME 为 application/octet-stream 但文件名为 .exe）由调用方负责；该辅助函数只检查 MIME 字符串。
func IsDangerous(mediaType string) bool {
	switch strings.ToLower(strings.TrimSpace(mediaType)) {
	case
		"application/x-msdownload",       // .exe / .dll
		"application/x-ms-installer",     // .msi
		"application/x-msi",              // .msi alt
		"application/vnd.microsoft.portable-executable",
		"application/x-dosexec",
		"application/x-executable",       // generic ELF / executable
		// .sh —— 严格来说不危险但有歧义，出于安全默认排除
		"application/x-sh",               // .sh — not strictly dangerous but ambiguous; opt out for safety
		"application/x-bat",              // .bat
		"application/x-msdos-program",    // .com / .bat (legacy)
		"application/x-msi-installer":
		return true
	}
	return false
}

// IsExtractable reports whether the cloud extraction service can read text
// out of (or transcode an image format from) this media type. When true,
// the orchestrator may populate FileAttachment.DocumentB64 (PDF) or
// FileAttachment.ExtractedText (everything else). Concrete extraction is
// implemented in Phase 3.
// IsExtractable 判断云端提取服务能否从该媒体类型读出文本（或转码图片格式）。为 true 时，orchestrator 可填充 FileAttachment.DocumentB64（PDF）或 FileAttachment.ExtractedText（其他类型）。具体提取在阶段 3 实现。
func IsExtractable(mediaType string) bool {
	mt := strings.ToLower(strings.TrimSpace(mediaType))
	// All text/* subtypes are trivially extractable (CSV, TXT, HTML, MD, JSON…).
	// 所有 text/* 子类型都可直接提取（CSV、TXT、HTML、MD、JSON…）。
	if strings.HasPrefix(mt, "text/") {
		return true
	}
	// Image subtypes the cloud may need to transcode (HEIC/AVIF → JPEG,
	// TIFF/BMP → PNG/JPEG) plus the four native Anthropic image MIMEs
	// (jpeg/png/gif/webp). Letting the native four match here is harmless
	// — the extraction path is a no-op for them, and IsExtractable just
	// gates whether the cloud is allowed to inspect.
	// 云端可能需要转码的图片子类型（HEIC/AVIF → JPEG，TIFF/BMP → PNG/JPEG），以及 Anthropic 原生支持的四种图片 MIME（jpeg/png/gif/webp）。让原生四种也命中此处无害——对它们提取路径是空操作，IsExtractable 只是控制云端是否允许检查。
	if strings.HasPrefix(mt, "image/") {
		switch mt {
		case "image/jpeg", "image/jpg", "image/png", "image/gif", "image/webp",
			"image/heic", "image/heif", "image/avif",
			"image/tiff", "image/bmp", "image/x-bmp",
			"image/svg+xml":
			return true
		}
		// Unknown image/* subtype falls through (passthrough).
		return false
	}
	switch mt {
	case
		"application/pdf",
		"application/json",
		"application/xml",
		"application/x-yaml",
		"application/javascript",
		"application/typescript",
		// Office document family (extraction Phase 3):
		"application/msword",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.ms-excel",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"application/vnd.ms-powerpoint",
		"application/vnd.openxmlformats-officedocument.presentationml.presentation",
		// OpenDocument family — claude.ai supports ODT.
		"application/vnd.oasis.opendocument.text",
		"application/vnd.oasis.opendocument.spreadsheet",
		"application/vnd.oasis.opendocument.presentation",
		// RTF / EPUB are in the claude.ai supported list.
		// RTF / EPUB 在 claude.ai 支持列表内。
		"application/rtf",
		"application/epub+zip",
		"text/rtf":
		return true
	}
	return false
}

// IsPassthrough reports whether the media type is one cloud can't extract
// from but the daemon should still receive as a URL-only file_ref. The
// agent then uses local tools (bash, archive_extract) on it.
//
// Anything not dangerous and not extractable defaults to passthrough at
// the caller — IsPassthrough is conservative and only lists the common
// types we explicitly expect (archives, video/audio, octet-stream). The
// gateway fallback should treat "not extractable AND not dangerous" as
// passthrough regardless of this allowlist.
// IsPassthrough 判断媒体类型是否属于云端无法提取、但 daemon 仍应以仅 URL 的 file_ref 接收的类型，随后 agent 用本地 tool（bash、archive_extract）处理。
// 既非危险也非可提取的类型在调用方默认走 passthrough——IsPassthrough 偏保守，只列出显式预期的常见类型（压缩包、视频/音频、octet-stream）。网关兜底应把“不可提取且不危险”一律视为 passthrough，与本白名单无关。
func IsPassthrough(mediaType string) bool {
	mt := strings.ToLower(strings.TrimSpace(mediaType))
	if IsDangerous(mt) {
		return false
	}
	if strings.HasPrefix(mt, "audio/") || strings.HasPrefix(mt, "video/") {
		return true
	}
	switch mt {
	case
		"application/zip",
		"application/x-zip-compressed",
		"application/x-tar",
		"application/gzip",
		"application/x-gzip",
		"application/x-bzip2",
		"application/x-7z-compressed",
		"application/x-rar-compressed",
		"application/vnd.rar",
		"application/x-apple-diskimage", // .dmg
		"application/octet-stream",
		"application/x-iso9660-image":
		return true
	}
	return false
}

// IsSupportedMediaType is retained as a thin wrapper over the explicit
// IsExtractable / IsPassthrough classification for callers that haven't
// migrated yet. New code should call the specific helper that matches
// the intent.
//
// Deprecated: use IsExtractable or IsPassthrough directly. The old
// "binary reject on unknown MIME" gate has been removed — the gateway
// now lets unknown non-dangerous types fall through to URL-only refs
// (see plan §2 P0).
// IsSupportedMediaType 保留为对 IsExtractable / IsPassthrough 明确分类的薄封装，供尚未迁移的调用方使用；新代码应调用与意图匹配的具体辅助函数。
// Deprecated: 直接使用 IsExtractable 或 IsPassthrough。旧的“未知 MIME 一律按二进制拒绝”闸门已移除——网关现在让未知的非危险类型落到仅 URL 引用（见计划 §2 P0）。
func IsSupportedMediaType(mediaType string) bool {
	return IsExtractable(mediaType) || IsPassthrough(mediaType)
}
