"""
OpenAPI tool loader for Shannon.
Dynamically converts OpenAPI 3.x specifications into Shannon tools.
Shannon 的 OpenAPI tool 加载器。
将 OpenAPI 3.x 规范动态转换为 Shannon tool。
"""

from __future__ import annotations

import asyncio
import json as _json
import logging
import os
import threading
import time
from typing import Any
from urllib.parse import quote, urlparse

import httpx

from .base import Tool, ToolMetadata, ToolParameter, ToolParameterType, ToolResult
from .openapi_parser import (
    OpenAPIParseError,
    deduplicate_operation_ids,
    extract_base_url,
    extract_operations,
    extract_parameters,
    extract_request_body,
    validate_spec,
)
from .vendor_adapters import get_vendor_adapter

logger = logging.getLogger(__name__)


def _resolve_env_var(value: str) -> str:
    """
    Resolve environment variable references in string values.
    解析字符串值中的环境变量引用。

    Supports both ${VAR} and $VAR formats.
    Returns the environment variable value if found, otherwise empty string.
    支持 ${VAR} 与 $VAR 两种格式。
    找到时返回环境变量值，否则返回空字符串。

    Args:
        value: String that may contain env var reference
            可能包含环境变量引用的字符串

    Returns:
        Resolved value or empty string if not found
            解析后的值；未找到时返回空字符串
    """
    if not isinstance(value, str):
        return value

    # Handle ${VAR} format
    # 处理 ${VAR} 格式
    if value.startswith("${") and value.endswith("}"):
        env_var = value[2:-1]
        return os.getenv(env_var, "")

    # Handle $VAR format
    # 处理 $VAR 格式
    if value.startswith("$"):
        env_var = value[1:]
        return os.getenv(env_var, "")

    # Not an env var reference
    # 不是环境变量引用
    return value


# Simple circuit breaker (same as MCP)
# 简单 circuit breaker（与 MCP 相同）
class _SimpleBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 60.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failures = 0
        self.open_until: float = 0.0
        self.half_open = False
        self._lock = threading.Lock()  # Protect state from concurrent access 注：保护状态，避免并发访问
        # 保护状态免受并发访问

    def allow(self, now: float) -> bool:
        with self._lock:
            if self.open_until > now:
                return False
            if self.open_until != 0.0 and self.open_until <= now:
                # move to half-open and allow one trial
                # 进入半开状态并允许一次试探
                self.half_open = True
                self.open_until = 0.0
            return True

    def on_success(self) -> None:
        with self._lock:
            self.failures = 0
            self.half_open = False
            self.open_until = 0.0

    def on_failure(self, now: float) -> None:
        with self._lock:
            self.failures += 1
            if self.failures >= self.failure_threshold:
                self.open_until = now + self.recovery_timeout
                self.half_open = False


_breakers: dict[str, _SimpleBreaker] = {}


def _validate_domain(url: str, allowed_domains: list[str]) -> None:
    """
    Validate URL against allowed domains (mirrors MCP logic).
    校验 URL 是否在允许域名内（与 MCP 逻辑一致）。

    Args:
        url: URL to validate
            要校验的 URL
        allowed_domains: List of allowed domains or ["*"] for wildcard
            允许的域名列表，或 ["*"] 表示通配

    Raises:
        ValueError: If domain not allowed
        ValueError：域名不被允许时抛出
    """
    host = urlparse(url).hostname or ""

    # Wildcard bypasses validation
    # 通配符跳过校验
    if "*" in allowed_domains:
        return

    # Check exact match or suffix match (subdomains)
    # 检查精确匹配或后缀匹配（子域名）
    if not any(host == d or host.endswith("." + d) for d in allowed_domains):
        raise ValueError(
            f"OpenAPI URL host '{host}' not in allowed domains: {allowed_domains}"
        )


class OpenAPILoader:
    """
    Loads OpenAPI spec and generates Shannon Tool classes for each operation.
    加载 OpenAPI 规范并为每个操作生成 Shannon Tool 类。
    """

    def __init__(
        self,
        name: str,
        spec: dict[str, Any],
        auth_type: str = "none",
        auth_config: dict[str, str] | None = None,
        category: str = "api",
        base_cost_per_use: float = 0.001,
        operations_filter: list[str] | None = None,
        tags_filter: list[str] | None = None,
        base_url_override: str | None = None,
        rate_limit: int = 30,
        timeout_seconds: float = 30.0,
        max_response_bytes: int = 10 * 1024 * 1024,
        retries: int = 2,
        spec_url: str | None = None,
        vendor_name: str | None = None,
    ):
        """
        Initialize OpenAPI loader.
        初始化 OpenAPI 加载器。

        Args:
            name: Tool collection name (used as prefix)
                tool 集合名称（用作前缀）
            spec: OpenAPI specification dict
                OpenAPI 规范字典
            auth_type: Authentication type (none|api_key|bearer|basic)
                authentication 类型（none|api_key|bearer|basic）
            auth_config: Auth configuration dict
                authentication 配置字典
            category: Tool category
                tool 分类
            base_cost_per_use: Base cost per operation
                每次操作的基础成本
            operations_filter: Optional list of operationIds to include
                可选的要包含的 operationId 列表
            tags_filter: Optional list of tags to filter by
                可选的用于过滤的标签列表
            base_url_override: Optional base URL override
                可选的基础 URL 覆盖值
            rate_limit: Requests per minute (default: 30, enforceable)
                每分钟 request 数（默认 30，可强制执行）
            timeout_seconds: Request timeout in seconds (default: 30)
                request timeout 秒数（默认 30）
            max_response_bytes: Max response size (default: 10MB)
                response 大小上限（默认 10MB）
            retries: Max retry attempts (default: 2, reduced to avoid circuit breaker)
                最大 retry 次数（默认 2，已减少以避免触发 circuit breaker）
            spec_url: Optional URL where spec was fetched from (for relative server URLs)
                抓取规范的可选来源 URL（用于解析相对 server URL）
            vendor_name: Optional vendor adapter name
                可选的厂商适配器名称
        """
        self.name = name
        self.spec = spec
        self.auth_type = auth_type
        self.auth_config = auth_config or {}
        self.category = category
        self.base_cost_per_use = base_cost_per_use
        self.operations_filter = operations_filter
        self.tags_filter = tags_filter
        self.base_url_override = base_url_override
        self.retries = retries
        self.rate_limit = rate_limit
        self.timeout_seconds = timeout_seconds
        self.max_response_bytes = max_response_bytes
        self.spec_url = spec_url
        self.vendor_name = vendor_name

        # Load vendor adapter if specified
        # 若指定了厂商适配器则加载
        self.vendor_adapter = get_vendor_adapter(vendor_name) if vendor_name else None
        if vendor_name and not self.vendor_adapter:
            logger.warning(f"Vendor adapter '{vendor_name}' not found, proceeding without adapter")

        # Validate spec
        # 校验规范
        validate_spec(spec)

        # Extract base URL
        # 提取基础 URL
        self.base_url = extract_base_url(spec, base_url_override, spec_url)

        # Validate domain - require explicit configuration for security
        # 校验域名——出于安全考虑要求显式配置
        allowed_domains_str = os.getenv("OPENAPI_ALLOWED_DOMAINS")
        if not allowed_domains_str:
            raise ValueError(
                "OPENAPI_ALLOWED_DOMAINS environment variable must be explicitly configured. "
                "Set to specific domains (e.g., 'api.example.com,api.partner.com') or '*' for development only."
            )
        allowed_domains = [
            d.strip()
            for d in allowed_domains_str.split(",")
            if d.strip()
        ]
        _validate_domain(self.base_url, allowed_domains)

        # Extract operations
        # 提取操作
        operations = extract_operations(spec, operations_filter, tags_filter)
        self.operations = deduplicate_operation_ids(operations)

        logger.info(
            f"OpenAPI loader '{name}': loaded {len(self.operations)} operations from {self.base_url}"
        )

    def _build_auth_headers(self, session_context: dict | None = None) -> dict[str, str]:
        """
        Build authentication headers based on auth_type and auth_config.
        根据 auth_type 和 auth_config 构建 authentication 头。

        Args:
            session_context: Optional session context for dynamic header resolution
                用于动态解析 request header 的可选 session context

        Returns:
            Dict of HTTP headers for authentication
            用于 authentication 的 HTTP 头字典
        """
        headers = {}

        if self.auth_type == "bearer":
            token = self.auth_config.get("token", "")
            # Resolve environment variable references
            # 解析环境变量引用
            token = _resolve_env_var(token)
            if token:
                headers["Authorization"] = f"Bearer {token}"

        elif self.auth_type == "api_key":
            api_key_name = self.auth_config.get("api_key_name", "X-API-Key")
            api_key_location = self.auth_config.get("api_key_location", "header")
            api_key_value = self.auth_config.get("api_key_value", "")

            # Resolve environment variable references
            # 解析环境变量引用
            api_key_value = _resolve_env_var(api_key_value)

            if api_key_location == "header" and api_key_value:
                headers[api_key_name] = api_key_value
            # Note: query parameters are handled separately in tool execution
            # 注意：查询参数在 tool 执行时单独处理

        elif self.auth_type == "basic":
            username = self.auth_config.get("username", "")
            password = self.auth_config.get("password", "")

            # Resolve environment variable references
            # 解析环境变量引用
            username = _resolve_env_var(username)
            password = _resolve_env_var(password)

            if username and password:
                import base64
                credentials = f"{username}:{password}"
                encoded = base64.b64encode(credentials.encode()).decode()
                headers["Authorization"] = f"Basic {encoded}"

        # Handle extra_headers if present
        # 若存在 extra_headers 则处理
        extra_headers = self.auth_config.get("extra_headers", {})
        if isinstance(extra_headers, dict):
            for header_name, header_value in extra_headers.items():
                # Resolve environment variable references
                # 解析环境变量引用
                header_value = _resolve_env_var(header_value)

                # Only static and environment variable values are supported
                # Dynamic body field references ({{body.field}}) are NOT supported
                # 仅支持静态值和环境变量值
                # 不支持动态 request body 字段引用（{{body.field}}）
                if header_value:
                    headers[header_name] = str(header_value)

        return headers

    def generate_tools(self) -> list[type[Tool]]:
        """
        Generate Tool classes for each operation.
        为每个操作生成 Tool 类。

        Returns:
            List of Tool class types
            Tool 类类型列表
        """
        tool_classes = []

        for op_data in self.operations:
            tool_class = self._create_tool_class(op_data)
            tool_classes.append(tool_class)

        return tool_classes

    def _create_tool_class(self, op_data: dict[str, Any]) -> type[Tool]:
        """
        Create a Tool class for a single OpenAPI operation.
        为单个 OpenAPI 操作创建 Tool 类。

        Args:
            op_data: Operation data dict from parser
                解析器输出的操作数据字典

        Returns:
            Tool class type
            Tool 类类型
        """
        operation_id = op_data["operation_id"]
        method = op_data["method"]
        path = op_data["path"]
        operation = op_data["operation"]

        # Extract parameters (pass spec for $ref resolution)
        # 提取参数（传入 spec 以解析 $ref）
        params = extract_parameters(operation, self.spec)
        body_param = extract_request_body(operation, self.spec)
        if body_param:
            params.extend(body_param)

        # Build tool parameters
        # 构建 tool 参数
        tool_params = []
        for p in params:
            param_type_str = p["type"]
            param_type = getattr(
                ToolParameterType, param_type_str.upper(), ToolParameterType.STRING
            )

            tool_params.append(
                ToolParameter(
                    name=p["name"],
                    type=param_type,
                    description=p["description"],
                    required=p["required"],
                    enum=p.get("enum"),
                )
            )

        # Human-readable summary for progress events (falls back to operationId)
        # 用于进度 event 的易读摘要（回退到 operationId）
        op_summary = operation.get("summary", operation_id)

        # Capture variables for closure
        # 为闭包捕获变量
        base_url = self.base_url
        auth_type = self.auth_type
        auth_config = self.auth_config
        category = self.category
        base_cost = self.base_cost_per_use
        rate_limit = self.rate_limit
        timeout_seconds = self.timeout_seconds
        max_response_bytes = self.max_response_bytes
        default_retries = self.retries
        loader_name = self.name
        build_auth_headers = self._build_auth_headers
        vendor_adapter = self.vendor_adapter

        # Get or create circuit breaker for this base_url
        # 获取或创建此 base_url 的 circuit breaker
        breaker_key = f"openapi:{base_url}"
        if breaker_key not in _breakers:
            _breakers[breaker_key] = _SimpleBreaker()
        breaker = _breakers[breaker_key]

        # Create Tool class dynamically
        # 动态创建 Tool 类
        class _OpenAPITool(Tool):
            def _get_metadata(self) -> ToolMetadata:
                summary = operation.get("summary", "")
                description = operation.get("description", summary)
                if not description:
                    description = f"{method} {path}"

                return ToolMetadata(
                    name=operation_id,
                    version="1.0.0",
                    description=description,
                    category=category,
                    author=f"OpenAPI:{loader_name}",
                    requires_auth=False,
                    timeout_seconds=int(timeout_seconds),
                    memory_limit_mb=128,
                    sandboxed=False,
                    session_aware=True,
                    dangerous=False,
                    cost_per_use=base_cost,
                    rate_limit=rate_limit,
                )

            def _get_parameters(self) -> list[ToolParameter]:
                return tool_params

            async def _execute_impl(
                self, session_context: dict | None = None, **kwargs
            ) -> ToolResult:
                now = time.time()

                logger.info(f"OpenAPI tool {operation_id} _execute_impl called with session_context: {session_context is not None}, kwargs keys: {list(kwargs.keys())}")

                # Circuit breaker check
                # circuit breaker 检查
                if not breaker.allow(now):
                    return ToolResult(
                        success=False,
                        output=None,
                        error=f"Circuit breaker open for {base_url} (too many failures)",
                    )

                # Build auth headers with session context for dynamic resolution
                # 结合 session context 构建 authentication 头，以支持动态解析
                headers = build_auth_headers(session_context)

                # Build URL with path parameters (URL-encoded)
                # 构建带路径参数的 URL（已 URL 编码）
                url = base_url + path
                for param in params:
                    if param["location"] == "path" and param["name"] in kwargs:
                        placeholder = "{" + param["name"] + "}"
                        # URL-encode path parameter value
                        # 对路径参数值进行 URL 编码
                        encoded_value = quote(str(kwargs[param["name"]]), safe="")
                        url = url.replace(placeholder, encoded_value)

                # Build query parameters
                # 构建查询参数
                query_params = {}
                for param in params:
                    if param["location"] == "query" and param["name"] in kwargs:
                        query_params[param["name"]] = kwargs[param["name"]]

                # Build request body
                # 构建 request body
                json_body = None
                if body_param and "body" in kwargs:
                    json_body = kwargs["body"]

                # Extract prompt_params from session_context
                # 从 session_context 提取 prompt_params
                prompt_params = None
                if session_context and "prompt_params" in session_context:
                    prompt_params = session_context["prompt_params"]

                # Apply vendor-specific body transformations if adapter is available
                # 若有适配器，则应用厂商特定的 request body 转换
                if vendor_adapter and json_body is not None:
                    try:
                        json_body = vendor_adapter.transform_body(
                            json_body,
                            operation_id,
                            prompt_params
                        )
                        logger.info(f"OpenAPI tool {operation_id}: Applied vendor adapter body transformations")
                    except Exception as e:
                        logger.warning(f"OpenAPI tool {operation_id}: Vendor adapter body transform failed: {e}")

                # Build request headers
                # 构建 request header
                request_headers = dict(headers)
                request_headers["Accept"] = "application/json"

                # Apply vendor-specific header transformations if adapter is available
                # 若有适配器，则应用厂商特定的 request header 转换
                if vendor_adapter and hasattr(vendor_adapter, 'transform_headers'):
                    try:
                        request_headers = vendor_adapter.transform_headers(
                            request_headers,
                            operation_id,
                            prompt_params
                        )
                        logger.info(f"OpenAPI tool {operation_id}: Applied vendor adapter header transformations")
                    except Exception as e:
                        logger.warning(f"OpenAPI tool {operation_id}: Vendor adapter header transform failed: {e}")

                if (
                    auth_type == "api_key"
                    and auth_config.get("api_key_location") == "query"
                ):
                    api_key_name = auth_config.get("api_key_name", "api_key")
                    api_key_value = auth_config.get("api_key_value", "")
                    # Resolve environment variable references
                    # 解析环境变量引用
                    api_key_value = _resolve_env_var(api_key_value)
                    query_params[api_key_name] = api_key_value

                # Optional debug logging of the final outbound request (sanitized)
                # 可选的最终出站 request 调试日志（已脱敏）
                try:
                    log_requests = os.getenv("OPENAPI_LOG_REQUESTS", "0").lower() not in {"0", "false", "no", "off", ""}
                    if log_requests:
                        def _mask(data):
                            if isinstance(data, dict):
                                out = {}
                                for k, v in data.items():
                                    lk = str(k).lower()
                                    if any(s in lk for s in ["authorization", "token", "secret", "password", "api_key", "apikey"]):
                                        out[k] = "***"
                                    else:
                                        out[k] = _mask(v)
                                return out
                            if isinstance(data, list):
                                return [_mask(x) for x in data]
                            return data

                        masked_headers = _mask(dict(request_headers))
                        masked_query = _mask(dict(query_params))
                        masked_body = _mask(json_body) if isinstance(json_body, (dict, list)) else json_body
                        try:
                            body_str = _json.dumps(masked_body, ensure_ascii=False) if masked_body is not None else None
                        except Exception:
                            body_str = str(masked_body) if masked_body is not None else None
                        if body_str and len(body_str) > 2000:
                            body_str = body_str[:2000] + "...(truncated)"
                        logger.info(
                            f"OpenAPI tool {operation_id}: Request {method} {url} query={masked_query} headers={masked_headers} body={body_str}"
                        )
                except Exception:
                    # Never fail on logging
                    # 日志记录绝不影响主流程
                    pass

                # Retry logic with exponential backoff
                # Allow env var override, otherwise use configured retries (default: 2)
                # 指数退避 retry 逻辑
                # 允许环境变量覆盖，否则使用配置的 retry 次数（默认 2）
                retries = int(os.getenv("OPENAPI_RETRIES", str(default_retries)))
                last_exception = None

                for attempt in range(1, retries + 1):
                    try:
                        # Emit progress
                        # 发送进度
                        observer = kwargs.get("observer")
                        if observer:
                            try:
                                msg = op_summary
                                if attempt > 1:
                                    msg += f" (retry {attempt}/{retries})"
                                observer("progress", {"message": msg})
                            except Exception:
                                pass

                        # Execute request with configured timeout
                        # 以配置的 timeout 执行 request
                        start_time = time.time()
                        async with httpx.AsyncClient(timeout=timeout_seconds) as client:
                            response = await client.request(
                                method=method,
                                url=url,
                                params=query_params,
                                json=json_body,
                                headers=request_headers,
                            )
                            response.raise_for_status()

                            # Check response size
                            # 检查 response 大小
                            content_length = len(response.content)
                            if content_length > max_response_bytes:
                                breaker.on_failure(now)
                                return ToolResult(
                                    success=False,
                                    output=None,
                                    error=f"Response too large: {content_length} bytes (max {max_response_bytes})",
                                )

                            # Parse response
                            # 解析 response
                            content_type = response.headers.get("content-type", "")
                            if "application/json" in content_type:
                                result = response.json()
                            else:
                                # Plain text for MVP
                                # MVP 阶段按纯文本处理
                                result = response.text

                            # Log response if OPENAPI_LOG_REQUESTS is enabled
                            # 若启用 OPENAPI_LOG_REQUESTS 则记录 response
                            if log_requests:
                                logger.info(f"OpenAPI tool {operation_id}: Response status={response.status_code} body={result}")

                            duration_ms = int((time.time() - start_time) * 1000)

                            # Success: reset breaker
                            # 成功：重置 circuit breaker
                            breaker.on_success()

                            return ToolResult(
                                success=True,
                                output=result,
                                execution_time_ms=duration_ms,
                            )

                    except httpx.HTTPStatusError as e:
                        last_exception = e
                        breaker.on_failure(now)
                        try:
                            log_requests = os.getenv("OPENAPI_LOG_REQUESTS", "0").lower() not in {"0", "false", "no", "off", ""}
                            if log_requests:
                                resp_text = e.response.text if hasattr(e, "response") and e.response is not None else ""
                                if resp_text and len(resp_text) > 2000:
                                    resp_text = resp_text[:2000] + "...(truncated)"
                                logger.warning(
                                    "OpenAPI tool %s: HTTP %s error on %s %s: %s",
                                    operation_id,
                                    getattr(e.response, "status_code", "?"),
                                    method,
                                    url,
                                    resp_text,
                                )
                        except Exception:
                            pass
                        if attempt >= retries:
                            return ToolResult(
                                success=False,
                                output=None,
                                error=f"HTTP {e.response.status_code}: {e.response.text[:200]}",
                            )
                        # Exponential backoff: 0.5s, 1s, 2s
                        # 指数退避：0.5s、1s、2s
                        delay = min(2.0 ** (attempt - 1) * 0.5, 5.0)
                        await asyncio.sleep(delay)

                    except Exception as e:
                        last_exception = e
                        breaker.on_failure(now)
                        if attempt >= retries:
                            return ToolResult(success=False, output=None, error=str(e))
                        # Exponential backoff
                        # 指数退避
                        delay = min(2.0 ** (attempt - 1) * 0.5, 5.0)
                        await asyncio.sleep(delay)

                # Fallback (should never reach here)
                # 兜底（正常不应到达此处）
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Request failed after {retries} retries: {str(last_exception)}",
                )

        _OpenAPITool.__name__ = f"OpenAPITool_{operation_id}"
        return _OpenAPITool


def load_openapi_tools_from_config(config: dict[str, Any]) -> list[type[Tool]]:
    """
    Load OpenAPI tools from shannon.yaml configuration.
    从 shannon.yaml 配置加载 OpenAPI tool。

    Args:
        config: Configuration dict from shannon.yaml
        来自 shannon.yaml 的配置字典

    Returns:
        List of Tool class types
        Tool 类类型列表
    """
    tool_classes = []
    openapi_tools_config = config.get("openapi_tools")

    # Handle None or empty config (all tools commented out)
    # 处理 None 或空配置（所有 tool 均被注释掉）
    if not openapi_tools_config:
        return tool_classes

    for tool_name, tool_config in openapi_tools_config.items():
        if not tool_config.get("enabled", False):
            logger.info(f"OpenAPI tool collection '{tool_name}' is disabled, skipping")
            continue

        try:
            # Get spec (URL or inline)
            # 获取规范（URL 或内联）
            spec_url = tool_config.get("spec_url")
            spec_inline = tool_config.get("spec_inline")

            spec_url_for_loader = None
            if spec_url:
                spec = _fetch_spec_from_url(spec_url)
                spec_url_for_loader = spec_url
            elif spec_inline:
                import yaml

                spec = yaml.safe_load(spec_inline)
            else:
                logger.error(
                    f"OpenAPI tool '{tool_name}': must provide spec_url or spec_inline"
                )
                continue

            # Create loader
            # 创建加载器
            loader = OpenAPILoader(
                name=tool_name,
                spec=spec,
                auth_type=tool_config.get("auth_type", "none"),
                auth_config=tool_config.get("auth_config", {}),
                category=tool_config.get("category", "api"),
                base_cost_per_use=tool_config.get("base_cost_per_use", 0.001),
                operations_filter=tool_config.get("operations"),
                tags_filter=tool_config.get("tags"),
                base_url_override=tool_config.get("base_url"),
                # Default to 30 so Tool base rate limiting is enforced
                # 默认 30，以启用 Tool 基类的 rate limit
                rate_limit=tool_config.get("rate_limit", 30),
                timeout_seconds=tool_config.get("timeout_seconds", 30.0),
                max_response_bytes=tool_config.get(
                    "max_response_bytes", 10 * 1024 * 1024
                ),
                spec_url=spec_url_for_loader,
                vendor_name=tool_config.get("vendor_adapter"),
            )

            # Generate tools
            # 生成 tool
            tools = loader.generate_tools()
            tool_classes.extend(tools)

            logger.info(
                f"Loaded {len(tools)} tools from OpenAPI collection '{tool_name}'"
            )

        except OpenAPIParseError as e:
            logger.error(f"Failed to parse OpenAPI spec for '{tool_name}': {e}")
        except Exception as e:
            logger.error(f"Failed to load OpenAPI tools from '{tool_name}': {e}")

    return tool_classes


def _fetch_spec_from_url(url: str) -> dict[str, Any]:
    """
    Fetch OpenAPI spec from URL with size limit and domain validation.
    从 URL 抓取 OpenAPI 规范，带大小限制与域名校验。

    Args:
        url: URL to OpenAPI spec (JSON or YAML), supports http://, https://, and file:// protocols
            OpenAPI 规范的 URL（JSON 或 YAML），支持 http://、https:// 和 file:// 协议

    Returns:
        Parsed spec dict
        解析后的规范字典

    Raises:
        ValueError: If fetch fails or spec too large
        ValueError：抓取失败或规范过大时抛出
    """
    import yaml

    # Handle file:// URLs
    # 处理 file:// URL
    if url.startswith("file://"):
        file_path = url.replace("file://", "")
        max_size = int(os.getenv("OPENAPI_MAX_SPEC_SIZE", str(5 * 1024 * 1024)))

        try:
            file_size = os.path.getsize(file_path)
            if file_size > max_size:
                raise ValueError(
                    f"Spec size ({file_size} bytes) exceeds max ({max_size} bytes)"
                )

            with open(file_path) as f:
                content = f.read()

            # Try JSON first, fall back to YAML
            # 先尝试 JSON，失败则回退到 YAML
            try:
                import json
                return json.loads(content)
            except Exception:
                return yaml.safe_load(content)

        except Exception as e:
            raise ValueError(f"Failed to load OpenAPI spec from file {file_path}: {e}")

    # Validate domain for HTTP(S) URLs - require explicit configuration (security fix)
    # 校验 HTTP(S) URL 的域名——要求显式配置（安全修复）
    allowed_domains_str = os.getenv("OPENAPI_ALLOWED_DOMAINS")
    if not allowed_domains_str:
        raise ValueError(
            "OPENAPI_ALLOWED_DOMAINS environment variable must be explicitly configured"
        )
    allowed_domains = [d.strip() for d in allowed_domains_str.split(",") if d.strip()]
    _validate_domain(url, allowed_domains)

    # Fetch with size limit
    # 带大小限制地抓取
    # 默认 5MB
    max_size = int(
        os.getenv("OPENAPI_MAX_SPEC_SIZE", str(5 * 1024 * 1024))
    )  # 5MB default 注：默认 5MB
    timeout = float(os.getenv("OPENAPI_FETCH_TIMEOUT", "30"))

    try:
        import httpx

        with httpx.Client(timeout=timeout) as client:
            response = client.get(url)
            response.raise_for_status()

            content_length = len(response.content)
            if content_length > max_size:
                raise ValueError(
                    f"Spec size ({content_length} bytes) exceeds max ({max_size} bytes)"
                )

            # Try JSON first, fall back to YAML
            # 先尝试 JSON，失败则回退到 YAML
            try:
                return response.json()
            except Exception:
                return yaml.safe_load(response.text)

    except Exception as e:
        raise ValueError(f"Failed to fetch OpenAPI spec from {url}: {e}")
