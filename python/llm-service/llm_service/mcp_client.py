from __future__ import annotations

import asyncio
import os
import time
from typing import Any, Awaitable, Callable, Dict, Optional
from urllib.parse import urlparse

import httpx

from .metrics import metrics


# --- Simple per-URL circuit breaker (process-local) ---
# --- 简单的按 URL circuit breaker（进程内）---
class _SimpleBreaker:
    def __init__(self, failure_threshold: int, recovery_timeout: float) -> None:
        self.failure_threshold = max(1, failure_threshold)
        self.recovery_timeout = max(1.0, recovery_timeout)
        self.failures = 0
        self.open_until: float = 0.0
        self.half_open = False

    def allow(self, now: float) -> bool:
        if self.open_until > now:
            return False
        if self.open_until != 0.0 and self.open_until <= now:
            # move to half-open and allow one trial
            # 转入 half-open 并允许一次试探
            self.half_open = True
            self.open_until = 0.0
        return True

    def on_success(self) -> None:
        self.failures = 0
        self.half_open = False
        self.open_until = 0.0

    def on_failure(self, now: float) -> None:
        self.failures += 1
        if self.failures >= self.failure_threshold:
            self.open_until = now + self.recovery_timeout
            self.half_open = False


_breakers: Dict[str, _SimpleBreaker] = {}


class HttpStatelessClient:
    """
    Minimal stateless MCP HTTP client with basic hardening.

    - URL allowlist via MCP_ALLOWED_DOMAINS (comma‑separated). Defaults to localhost,127.0.0.1
    - Response size limit via MCP_MAX_RESPONSE_BYTES (default 10 MB)
    - Retries via MCP_RETRIES (default 3)
    - Timeout via MCP_TIMEOUT_SECONDS (default 10s)

    Convention: POST to `url` with JSON body {"function": <name>, "args": {...}} and expect JSON response.

    最小化的无状态 MCP HTTP 客户端，带基础加固。

    - 通过 MCP_ALLOWED_DOMAINS（逗号分隔）配置 URL allowlist，默认 localhost,127.0.0.1
    - 通过 MCP_MAX_RESPONSE_BYTES 限制 response body 大小（默认 10 MB）
    - 通过 MCP_RETRIES 配置 retry 次数（默认 3）
    - 通过 MCP_TIMEOUT_SECONDS 配置 timeout（默认 10s）

    约定：向 `url` 发送 POST，JSON body 为 {"function": <name>, "args": {...}}，
    并期望返回 JSON response。
    """

    def __init__(
        self,
        name: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.name = name
        self.url = url
        self.headers = headers or {}
        # Config from env with safe defaults
        # 从环境变量读取配置，带安全默认值
        self.allowed_domains = [
            d.strip()
            for d in os.getenv("MCP_ALLOWED_DOMAINS", "localhost,127.0.0.1").split(",")
            if d.strip()
        ]
        self.max_response_bytes = int(
            os.getenv("MCP_MAX_RESPONSE_BYTES", str(10 * 1024 * 1024))
        )
        self.retries = max(1, int(os.getenv("MCP_RETRIES", "3")))
        self.timeout = float(
            os.getenv(
                "MCP_TIMEOUT_SECONDS", str(timeout if timeout is not None else 10.0)
            )
        )

        self._validate_url()
        # Circuit breaker config
        # circuit breaker 配置
        self.cb_failures = max(1, int(os.getenv("MCP_CB_FAILURES", "5")))
        self.cb_recovery = float(os.getenv("MCP_CB_RECOVERY_SECONDS", "60"))

    def _validate_url(self) -> None:
        host = urlparse(self.url).hostname or ""
        # Wildcard "*" bypasses domain validation (use cautiously in development)
        # 通配符 "*" 会跳过域名校验（开发环境谨慎使用）
        if "*" in self.allowed_domains:
            return
        # Allow exact match or suffix match (subdomains)
        # 允许精确匹配或后缀匹配（子域名）
        if not any(host == d or host.endswith("." + d) for d in self.allowed_domains):
            raise ValueError(
                f"MCP URL host '{host}' not in allowed domains: {self.allowed_domains}"
            )

    def _client(self) -> httpx.AsyncClient:
        # httpx 0.28+ doesn't support max_response_body_size, using defaults
        # httpx 0.28+ 不支持 max_response_body_size，使用默认值
        return httpx.AsyncClient(timeout=self.timeout)

    async def _invoke(self, func_name: str, **kwargs: Any) -> Any:
        payload = {"function": func_name, "args": kwargs}
        status = "error"
        start = time.time()
        try:
            async with self._client() as client:
                # Circuit breaker per URL
                # 按 URL 的 circuit breaker
                br = _breakers.setdefault(
                    self.url, _SimpleBreaker(self.cb_failures, self.cb_recovery)
                )
                for attempt in range(1, self.retries + 1):
                    try:
                        now = time.time()
                        if not br.allow(now):
                            raise httpx.RequestError("circuit_open")
                        resp = await client.post(
                            self.url, json=payload, headers=self.headers
                        )
                        resp.raise_for_status()
                        br.on_success()
                        status = "success"
                        return resp.json()
                    except Exception as e:  # network or HTTP errors
                        _ = e  # Reserved for logging
                        br.on_failure(time.time())
                        if attempt >= self.retries:
                            raise
                        # simple exponential backoff: 0.5, 1.0, 2.0 ...
                        # 简单的指数退避：0.5、1.0、2.0 ...
                        delay = min(2.0 ** (attempt - 1) * 0.5, 5.0)
                        await asyncio.sleep(delay)
        finally:
            duration = time.time() - start
            try:
                metrics.record_mcp_request(self.name, func_name, status, duration)
            except Exception:
                pass

    async def get_callable_function(
        self, func_name: str
    ) -> Callable[..., Awaitable[Any]]:
        async def _callable(**kwargs: Any) -> Any:
            return await self._invoke(func_name, **kwargs)

        return _callable
