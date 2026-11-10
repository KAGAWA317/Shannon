package attachments

// MaxMultimodalBodyBytes is the maximum request body size for endpoints that
// may carry inline base64 attachments (images, PDFs). 40 MB accommodates
// several high-resolution images plus JSON overhead, leaving buffer below
// Anthropic's 32 MB request limit after extraction / re-encoding decisions.
//
// Raised from 30 MB → 40 MB per attachment-format-parity plan §2 P0.
// MaxMultimodalBodyBytes 是可能携带内联 base64 附件（图片、PDF）的端点所允许的最大 request body 大小。40 MB 可容纳多张高分辨率图片加 JSON 开销，并在提取/重编码决策后仍低于 Anthropic 的 32 MB request 上限。
// 按 attachment-format-parity 计划 §2 P0 从 30 MB 提升到 40 MB。
const MaxMultimodalBodyBytes = 40 * 1024 * 1024

// MaxAttachmentThumbnailBytes is the maximum thumbnail data URL size persisted
// in DB metadata. Oversized thumbnails are silently dropped.
// MaxAttachmentThumbnailBytes 是持久化到 DB metadata 中的缩略图 data URL 最大大小。
// 超限缩略图会被静默丢弃。
const MaxAttachmentThumbnailBytes = 50 * 1024

// MaxDecodedAttachmentBytes is the maximum total decoded size of all
// inline attachments in a single request (25 MB). "Inline" = base64 bytes
// that travel in the request body; URL-based file_ref attachments use
// MaxFileRefSizeBytes instead.
//
// Raised from 20 MB → 25 MB per attachment-format-parity plan §2 P0.
// MaxDecodedAttachmentBytes 是单次 request 中所有内联附件解码后的总大小上限（25 MB）。“内联”指随 request body 传输的 base64 字节；基于 URL 的 file_ref 附件改用 MaxFileRefSizeBytes。
// 按 attachment-format-parity 计划 §2 P0 从 20 MB 提升到 25 MB。
const MaxDecodedAttachmentBytes = 25 * 1024 * 1024

// MaxFileRefSizeBytes is the single-file upper bound on URL-based
// attachments (the daemon downloads via URL + AuthHeader and stages
// locally). 500 MB aligns with claude.ai's published per-file limit.
//
// Added in attachment-format-parity plan §2 P0. Channels (slack /
// feishu / line / etc.) should clamp their own per-file size cap to
// this constant so a single source can't bypass the cloud-wide policy.
// MaxFileRefSizeBytes 是基于 URL 的附件的单文件上限（daemon 通过 URL + AuthHeader 下载并在本地暂存）。500 MB 与 claude.ai 公布的每文件限制对齐。
// 按 attachment-format-parity 计划 §2 P0 新增。各渠道（slack / feishu / line 等）应把自身单文件上限收敛到该常量，避免单一来源绕过云侧统一策略。
const MaxFileRefSizeBytes = 500 * 1024 * 1024

// MaxInlineSingleFile is the per-file cap for inline base64 attachments
// (the actual body that travels through the API). Anything larger should
// either be extracted to text (Phase 3) or stay URL-only.
//
// Added in attachment-format-parity plan §2 P0.
// MaxInlineSingleFile 是内联 base64 附件的单文件上限（即真正经过 API 的 request body）。更大的内容应提取为文本（阶段 3）或仅保留 URL。
// 按 attachment-format-parity 计划 §2 P0 新增。
const MaxInlineSingleFile = 25 * 1024 * 1024
