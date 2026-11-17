"""
Attachment resolver: reads attachments from Redis, converts to provider-specific
content block formats (OpenAI, Anthropic, Gemini).

Supported file categories:
- Binary vision: image/png, image/jpeg, image/gif, image/webp → native image blocks
- Binary document: application/pdf → native document blocks
- Text files: text/*, application/json, application/xml → decoded to text, injected inline

附件解析器：从 Redis 读取附件，并转换为各 provider 专属的 content block 格式
（OpenAI、Anthropic、Gemini）。

支持的文件类别：
- 二进制视觉：image/png、image/jpeg、image/gif、image/webp → 原生 image block
- 二进制文档：application/pdf → 原生 document block
- 文本文件：text/*、application/json、application/xml → 解码为文本后内联注入
"""
import asyncio
import base64
import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

ATTACHMENT_TTL = 1800  # 30 minutes 注：30 分钟

# MIME types that should be decoded to text instead of passed as binary.
# These are readable by ALL providers without special content block support.
# 应解码为文本而非按二进制传递的 MIME 类型。
# 所有 provider 都能直接读取这些内容，无需特别的 content block 支持。
TEXT_MIME_TYPES = {
    "text/plain", "text/markdown", "text/csv", "text/html", "text/xml",
    "text/x-python", "text/x-java", "text/x-c", "text/x-go",
    "text/javascript", "text/css", "text/yaml", "text/x-toml",
    "application/json", "application/xml", "application/x-yaml",
    "application/javascript", "application/typescript",
}


def is_text_mime(media_type: str) -> bool:
    """Check if a MIME type represents a text-decodable file.

    判断某个 MIME 类型是否代表可解码为文本的文件。
    """
    if media_type in TEXT_MIME_TYPES:
        return True
    # Catch-all: any text/* subtype
    # 兜底：任意 text/* 子类型
    if media_type.startswith("text/"):
        return True
    return False


class AttachmentResolver:
    def __init__(self, redis_client):
        self.redis = redis_client

    async def resolve(self, attachment_id: str, session_id: str = "") -> Optional[dict]:
        """Resolve an attachment ID to its data from Redis.

        When session_id is provided, validates it against the stored record
        to prevent cross-session attachment access.

        从 Redis 把 attachment ID 解析为对应数据。

        提供 session_id 时，会与存储记录校验，防止跨 session 访问附件。
        """
        key = f"shannon:att:{attachment_id}"
        raw = await self.redis.get(key)
        if raw is None:
            logger.warning(f"Attachment {attachment_id} not found or expired")
            return None

        # Refresh TTL on access
        # 访问时刷新 TTL
        await self.redis.expire(key, ATTACHMENT_TTL)

        att = json.loads(raw)

        # Session isolation
        # session 隔离
        if session_id and att.get("session_id") and att["session_id"] != session_id:
            logger.warning(f"Attachment {attachment_id} session mismatch (expected {session_id})")
            return None
        media_type = att["media_type"]
        filename = att.get("filename", "")

        # Text files: decode base64 → plain text for universal provider compatibility
        # 文本文件：把 base64 解码为纯文本，以获得通用 provider 兼容性
        if is_text_mime(media_type):
            try:
                text_content = base64.b64decode(att["data"]).decode("utf-8")
                return {
                    "id": att["id"],
                    "media_type": media_type,
                    "filename": filename,
                    "text_content": text_content,
                    "source": "text",
                }
            except (UnicodeDecodeError, Exception) as e:
                logger.warning(f"Failed to decode text attachment {attachment_id}: {e}, falling back to base64")

        # Binary files (images, PDFs): keep as base64
        # 二进制文件（图片、PDF）：保留 base64
        return {
            "id": att["id"],
            "media_type": media_type,
            "filename": filename,
            "data": att["data"],  # base64 encoded string 注：base64 编码的字符串
            "source": "base64",
        }

    async def resolve_many(self, attachment_refs: list, session_id: str = "") -> list:
        """Resolve a list of attachment references in parallel.

        并行解析一组 attachment 引用。
        """
        # Separate Redis-backed refs (need async I/O) from URL refs (instant).
        # 区分需要 Redis 异步 I/O 的引用与即时可用的 URL 引用。
        url_results = []
        coros = []
        order: list[tuple[int, str]] = []  # (original_index, "id"|"url") 注：(原始下标, "id"|"url")

        for i, ref in enumerate(attachment_refs):
            if not isinstance(ref, dict):
                logger.warning(f"Skipping non-dict attachment ref at index {i}: {type(ref).__name__}")
                continue
            if "id" in ref:
                coros.append(self.resolve(ref["id"], session_id=session_id))
                order.append((i, "id"))
            elif "url" in ref:
                url_results.append({
                    "url": ref["url"],
                    "media_type": ref.get("media_type", "application/octet-stream"),
                    "source": "url",
                })
                order.append((i, "url"))

        # Resolve all Redis lookups concurrently.
        # 并发解析所有 Redis 查询。
        resolved = await asyncio.gather(*coros) if coros else []

        # Merge results preserving original order.
        # 合并结果并保持原始顺序。
        results = []
        id_iter = iter(resolved)
        url_iter = iter(url_results)
        for _, kind in order:
            if kind == "id":
                att = next(id_iter)
                if att is not None:
                    results.append(att)
            else:
                results.append(next(url_iter))
        return results

    def to_anthropic_block(self, att: dict) -> dict:
        """Convert attachment to Anthropic content block.

        将附件转换为 Anthropic content block。
        """
        media_type = att["media_type"]
        if media_type == "application/pdf":
            return {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": att["data"],
                },
            }
        elif is_text_mime(media_type):
            # Anthropic supports text/plain as document block natively
            # Anthropic 原生支持将 text/plain 作为 document block
            return {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": "text/plain",
                    "data": att["data"],
                },
            }
        else:
            return {
                "type": "image",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": att["data"],
                },
            }

    def to_openai_block(self, att: dict) -> dict:
        """Convert attachment to OpenAI content block.

        将附件转换为 OpenAI content block。
        """
        media_type = att["media_type"]
        data_uri = f"data:{media_type};base64,{att['data']}"
        if media_type == "application/pdf":
            return {
                "type": "file",
                "file": {
                    "filename": att.get("filename", "document.pdf"),
                    "file_data": data_uri,
                },
            }
        else:
            return {
                "type": "image_url",
                "image_url": {"url": data_uri},
            }

    def to_gemini_block(self, att: dict) -> dict:
        """Convert attachment to Gemini inline_data block.

        将附件转换为 Gemini 的 inline_data block。
        """
        return {
            "inlineData": {
                "mimeType": att["media_type"],
                "data": att["data"],
            }
        }

    def to_content_blocks(self, attachments: list, provider: str) -> list:
        """Convert a list of attachments to provider-specific content blocks.

        将一组附件转换为 provider 专属的 content block。
        """
        converters = {
            "anthropic": self.to_anthropic_block,
            "openai": self.to_openai_block,
            "gemini": self.to_gemini_block,
        }
        convert = converters.get(provider, self.to_openai_block)
        return [convert(att) for att in attachments]
