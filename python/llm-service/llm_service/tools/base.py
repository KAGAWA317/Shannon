"""
Base Tool interface for Shannon platform
Shannon 平台的基础 tool 接口
"""

import asyncio
import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Union

# Rate limiting constants
# rate limit 常量
RATE_LIMIT_HIGH_THROUGHPUT_THRESHOLD = 60  # req/min - skip per-session tracking above this 注：req/min——超过此值跳过按 session 跟踪
RATE_LIMIT_SKIP_THRESHOLD = 100  # req/min - skip rate limiting entirely above this 注：req/min——超过此值完全跳过 rate limit
TRACKER_MAX_ENTRIES = 100  # Maximum entries in execution tracker before cleanup 注：执行跟踪器清理前的最大条目数


class ToolParameterType(Enum):
    """Supported parameter types for tools
    tool 支持的参数类型"""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    ARRAY = "array"
    OBJECT = "object"
    FILE = "file"  # For file paths or file content 注：用于文件路径或文件内容


@dataclass
class ToolParameter:
    """Definition of a tool parameter
    tool 参数的定义"""

    name: str
    type: ToolParameterType
    description: str
    required: bool = True
    default: Any = None
    enum: list[Any] | None = None  # For enumerated values 注：用于枚举值
    min_value: Union[int, float] | None = None
    max_value: Union[int, float] | None = None
    pattern: str | None = None  # Regex pattern for validation 注：用于校验的正则模式
    items: dict[str, Any] | None = None  # For ARRAY types: {"type": "string"} 注：用于 ARRAY 类型：{"type": "string"}


@dataclass
class ToolMetadata:
    """Metadata about a tool
    tool 的 metadata"""

    name: str
    version: str
    description: str
    category: str  # e.g., "search", "calculation", "file", "database" 注：例如 "search"、"calculation"、"file"、"database"
    author: str = "Shannon"
    requires_auth: bool = False
    rate_limit: int | None = None  # Requests per minute 注：每分钟请求数
    timeout_seconds: int = 30
    memory_limit_mb: int = 512
    sandboxed: bool = True  # Whether to run in sandbox 注：是否在 sandbox 中运行
    session_aware: bool = True  # Whether tool uses session context (default True to avoid footgun) 注：tool 是否使用 session context（默认 True 以避免误用）
    dangerous: bool = False  # Requires extra confirmation 注：需要额外确认
    cost_per_use: float = 0.0  # Cost in USD per invocation 注：每次调用的费用（USD）
    input_examples: list[dict[str, Any]] | None = None  # Examples for tool usage (Anthropic-specific) 注：tool 用法示例（Anthropic 专用）


@dataclass
class ToolResult:
    """Result from tool execution
    tool 执行结果"""

    success: bool
    output: Any
    error: str | None = None
    metadata: dict[str, Any] | None = None
    execution_time_ms: int | None = None
    tokens_used: int | None = None
    cost_usd: float | None = None
    cost_model: str | None = None  # synthetic model name for cost attribution 注：用于费用归因的虚拟模型名称

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization
        转换为字典以便序列化"""
        d = {
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "metadata": self.metadata or {},
            "execution_time_ms": self.execution_time_ms,
            "tokens_used": self.tokens_used,
        }
        if self.cost_usd is not None:
            d["cost_usd"] = self.cost_usd
        if self.cost_model is not None:
            d["cost_model"] = self.cost_model
        return d

    def to_json(self) -> str:
        """Convert to JSON string
        转换为 JSON 字符串"""
        return json.dumps(self.to_dict())


class Tool(ABC):
    """Abstract base class for all tools
    所有 tool 的抽象基类"""

    def __init__(self):
        self.metadata = self._get_metadata()
        self.parameters = self._get_parameters()
        self._execution_count = 0
        # Track rate limits per session/user instead of globally
        # # 按 session/user 分别跟踪 rate limit，而不是全局跟踪。
        self._execution_tracker: dict[str, datetime] = {}
        # Request-scoped ID for rate limiting when no session/agent context
        # # 无 session/agent context 时用于 rate limit 的 request 级 ID。
        self._current_request_id: str | None = None

    @abstractmethod
    def _get_metadata(self) -> ToolMetadata:
        """Return tool metadata"""
        pass

    @abstractmethod
    def _get_parameters(self) -> list[ToolParameter]:
        """Return list of tool parameters"""
        pass

    @abstractmethod
    async def _execute_impl(
        self, session_context: dict | None = None, **kwargs
    ) -> ToolResult:
        """
        Actual tool execution implementation.
        All parameters are passed as keyword arguments.
        Session context is optionally provided for session-aware tools.
        实际 tool 执行实现。
        所有参数均以关键字参数传入。
        对 session-aware 的 tool，可选提供 session context。
        """
        pass

    async def execute(
        self,
        session_context: dict | None = None,
        observer: Any | None = None,
        **kwargs,
    ) -> ToolResult:
        """
        Execute the tool with given parameters.
        Handles validation, logging, and error handling.
        使用给定参数执行 tool。
        负责校验、日志记录和错误处理。

        Args:
            session_context: Optional session context containing history, user info, etc.
                可选的 session context，包含历史记录、用户信息等。
            observer: Optional callback(event_name, payload) for intermediate status updates
                可选回调 callback(event_name, payload)，用于中间状态更新
            **kwargs: Tool-specific parameters
                tool 特定的参数
        """
        start_time = time.time()

        # Reset request ID for this execution (ensures unique key per execute call)
        # 为本次执行重置 request ID（确保每次 execute 调用都有唯一键）
        self._reset_request_id()

        try:
            # Coerce and validate parameters
            # 强制转换并校验参数
            kwargs = self._coerce_parameters(kwargs)
            self._validate_parameters(kwargs)

            # Extract session_id and agent_id from context
            # 从 context 中提取 session_id 和 agent_id
            session_id = None
            agent_id = None
            if session_context and isinstance(session_context, dict):
                session_id = session_context.get("session_id")
                agent_id = session_context.get("agent_id")

            # Get consistent tracker key for this execution
            # 获取本次执行一致的跟踪器键
            tracker_key = self._get_tracker_key(session_id, agent_id)

            # Check rate limits (skip for high-rate tools like calculator)
            # 检查 rate limit（对计算器等高频 tool 跳过）
            if self.metadata.rate_limit and self.metadata.rate_limit < RATE_LIMIT_SKIP_THRESHOLD:
                retry_after = self._get_retry_after(tracker_key)
                if retry_after is not None:
                    # Auto-wait instead of rejecting — saves an LLM call round-trip
                    # 自动等待而非直接拒绝——节省一次 LLM 调用往返
                    await asyncio.sleep(min(retry_after, 30))

            # Execute the tool with session context if tool is session-aware
            # 若 tool 是 session-aware 的，则携带 session context 执行
            if self.metadata.session_aware:
                result = await self._execute_impl(
                    session_context=session_context, observer=observer, **kwargs
                )
            else:
                result = await self._execute_impl(
                    session_context=None, observer=observer, **kwargs
                )

            # Track execution (both success and failure from _execute_impl)
            # 跟踪执行（包括 _execute_impl 的成功与失败）
            self._execution_count += 1
            # Only track session/agent-scoped keys (request-scoped UUIDs can't be
            # rate-limited across requests anyway, so tracking them wastes memory)
            # # 只跟踪 session/agent 级别的键（request 级 UUID 反正无法跨 request
            # # 做 rate limit，跟踪它们只会浪费内存）
            if not tracker_key.startswith("request:"):
                self._execution_tracker[tracker_key] = datetime.now()

                # Clean up old entries to prevent memory growth
                # 清理旧条目以避免内存增长
                if len(self._execution_tracker) > TRACKER_MAX_ENTRIES:
                    sorted_keys = sorted(
                        self._execution_tracker.items(), key=lambda x: x[1]
                    )
                    for key, _ in sorted_keys[: len(sorted_keys) - TRACKER_MAX_ENTRIES]:
                        del self._execution_tracker[key]

            # Add execution time
            # 补充执行耗时
            execution_time = int((time.time() - start_time) * 1000)
            result.execution_time_ms = execution_time

            return result

        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error=str(e),
                execution_time_ms=int((time.time() - start_time) * 1000),
            )

    def _coerce_parameters(self, kwargs: dict[str, Any]) -> dict[str, Any]:
        """Best-effort coercion of incoming parameters to expected types.
        - INTEGER: accept floats with integral value (e.g., 3.0) and numeric strings
        - FLOAT: accept ints and numeric strings
        - BOOLEAN: accept common string forms ("true"/"false")
        Other types pass through unchanged.

        尽力将传入参数转换为期望类型。
        - INTEGER：接受整数值浮点数（如 3.0）和数字字符串
        - FLOAT：接受整数和数字字符串
        - BOOLEAN：接受常见字符串形式（"true"/"false"）
        其他类型原样透传。
        """
        if not kwargs:
            return {}
        out = dict(kwargs)
        spec = {p.name: p for p in self.parameters}
        for name, param in spec.items():
            if name not in out:
                continue
            val = out[name]
            try:
                if param.type == ToolParameterType.INTEGER:
                    if isinstance(val, float) and float(val).is_integer():
                        out[name] = int(val)
                    elif isinstance(val, str):
                        s = val.strip()
                        # Handle simple numeric strings (no locale/separators)
                        # 处理简单数字字符串（不含区域格式/分隔符）
                        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
                            out[name] = int(s)
                    # Clamp to allowed min/max to avoid validation failures on oversized inputs
                    # 按允许的最小/最大值裁剪，避免超限输入导致校验失败
                    if isinstance(out[name], int):
                        if param.max_value is not None and out[name] > param.max_value:
                            out[name] = param.max_value
                        if param.min_value is not None and out[name] < param.min_value:
                            out[name] = param.min_value
                elif param.type == ToolParameterType.FLOAT:
                    if isinstance(val, int):
                        out[name] = float(val)
                    elif isinstance(val, str):
                        s = val.strip()
                        out[name] = float(s)
                    if isinstance(out[name], float):
                        if param.max_value is not None and out[name] > param.max_value:
                            out[name] = float(param.max_value)
                        if param.min_value is not None and out[name] < param.min_value:
                            out[name] = float(param.min_value)
                elif param.type == ToolParameterType.BOOLEAN:
                    if isinstance(val, str):
                        s = val.strip().lower()
                        if s in ("true", "1", "yes", "y"):
                            out[name] = True
                        elif s in ("false", "0", "no", "n"):
                            out[name] = False
            except Exception:
                # If coercion fails, keep original and let validation raise
                # 转换失败则保留原值，交由校验抛错
                pass
        return out

    def _validate_parameters(self, kwargs: dict[str, Any]) -> None:
        """Validate input parameters against tool definition
        按 tool 定义校验输入参数"""
        # Check required parameters
        # 检查必填参数
        for param in self.parameters:
            if param.required and param.name not in kwargs:
                raise ValueError(f"Required parameter '{param.name}' is missing")

            if param.name in kwargs:
                value = kwargs[param.name]

                # Type validation
                # 类型校验
                if not self._validate_type(value, param.type):
                    raise TypeError(
                        f"Parameter '{param.name}' expects type {param.type.value}, "
                        f"got {type(value).__name__}"
                    )

                # Enum validation
                # 枚举校验
                if param.enum and value not in param.enum:
                    raise ValueError(
                        f"Parameter '{param.name}' must be one of {param.enum}"
                    )

                # Range validation
                # 范围校验
                if param.min_value is not None and value < param.min_value:
                    raise ValueError(
                        f"Parameter '{param.name}' must be >= {param.min_value}"
                    )
                if param.max_value is not None and value > param.max_value:
                    raise ValueError(
                        f"Parameter '{param.name}' must be <= {param.max_value}"
                    )

                # Pattern validation
                # 模式校验
                if param.pattern:
                    import re

                    if not re.match(param.pattern, str(value)):
                        raise ValueError(
                            f"Parameter '{param.name}' does not match pattern {param.pattern}"
                        )

        # Check for unknown parameters
        # 检查未知参数
        known_params = {p.name for p in self.parameters}
        unknown = set(kwargs.keys()) - known_params
        if unknown:
            raise ValueError(f"Unknown parameters: {unknown}")

    def _validate_type(self, value: Any, expected_type: ToolParameterType) -> bool:
        """Validate value against expected type
        校验值是否符合期望类型"""
        type_map = {
            ToolParameterType.STRING: str,
            ToolParameterType.INTEGER: int,
            ToolParameterType.FLOAT: (int, float),
            ToolParameterType.BOOLEAN: bool,
            ToolParameterType.ARRAY: list,
            ToolParameterType.OBJECT: dict,
            ToolParameterType.FILE: str,  # File paths are strings 注：文件路径用字符串表示
        }

        expected = type_map.get(expected_type)
        if expected:
            return isinstance(value, expected)
        return False

    def _get_tracker_key(
        self, session_id: str | None, agent_id: str | None = None
    ) -> str:
        """Generate consistent tracker key for rate limiting.
        为 rate limit 生成一致的跟踪器键。

        Uses session_id if available, then agent_id as fallback.
        This avoids the threading.get_ident() issue where all asyncio coroutines
        share the same thread ID in a single-threaded event loop.
        优先使用 session_id，其次回退到 agent_id。
        这样可避免 threading.get_ident() 的问题：单线程 event loop 中所有 asyncio
        协程共享同一线程 ID。

        Args:
            session_id: Optional session ID from context
                context 中可选的 session ID
            agent_id: Optional agent ID for fallback tracking
                用于回退跟踪的可选 agent ID

        Returns:
            A unique key for tracking rate limits
            用于跟踪 rate limit 的唯一键
        """
        if session_id:
            return f"session:{session_id}"
        if agent_id:
            return f"agent:{agent_id}"
        # For requests without session_id or agent_id, use request-scoped tracking
        # This prevents false rate limiting across concurrent requests
        # NOTE: Per-request UUID means no cross-request rate limiting without context
        # 对没有 session_id 或 agent_id 的 request，使用 request 级跟踪
        # 这可避免并发 request 之间产生错误的 rate limit
        # 注意：按 request 生成的 UUID 意味着无 context 时无法跨 request rate limit
        if self._current_request_id is None:
            self._current_request_id = uuid.uuid4().hex[:8]
        return f"request:{self._current_request_id}"

    def _reset_request_id(self) -> None:
        """Reset request ID for next execution.
        为下一次执行重置 request ID。

        Called at the start of each execute() to ensure each execution
        gets a unique tracker key when no session_id is available.
        在每次 execute() 开始时调用，确保没有 session_id 时
        每次执行都获得唯一的跟踪器键。
        """
        self._current_request_id = None

    def _get_retry_after(self, tracker_key: str) -> float | None:
        """Check rate limit and return remaining wait time if limited.
        检查 rate limit；若已超限则返回剩余等待时间。

        Args:
            tracker_key: The key to use for tracking (session or request based)
                用于跟踪的键（基于 session 或 request）

        Returns:
            None if execution is allowed, or remaining seconds to wait if rate limited
            允许执行时返回 None，被 rate limit 限制时返回剩余等待秒数
        """
        if not self.metadata.rate_limit:
            return None

        # Skip rate limiting for high-throughput tools
        # 对高吞吐 tool 跳过 rate limit
        if self.metadata.rate_limit >= RATE_LIMIT_HIGH_THROUGHPUT_THRESHOLD:
            return None

        if tracker_key not in self._execution_tracker:
            return None

        last_execution = self._execution_tracker[tracker_key]
        min_interval = timedelta(seconds=60.0 / self.metadata.rate_limit)
        elapsed = datetime.now() - last_execution

        if elapsed >= min_interval:
            return None

        # Return remaining time to wait
        # 返回剩余等待时间
        remaining = min_interval - elapsed
        return remaining.total_seconds()

    def get_schema(self) -> dict[str, Any]:
        """
        Get JSON schema for this tool (compatible with OpenAI function calling)
        获取此 tool 的 JSON schema（兼容 OpenAI function calling）
        """
        properties = {}
        required = []

        for param in self.parameters:
            prop = {
                "type": param.type.value,
                "description": param.description,
            }

            # Add items for ARRAY type (required by OpenAI schema validation)
            # 为 ARRAY 类型添加 items（OpenAI schema 校验要求）
            if param.type == ToolParameterType.ARRAY:
                prop["items"] = {"type": "string"}  # Default to string, can be extended 注：默认字符串，可扩展

            if param.enum:
                prop["enum"] = param.enum
            if param.min_value is not None:
                prop["minimum"] = param.min_value
            if param.max_value is not None:
                prop["maximum"] = param.max_value
            if param.pattern:
                prop["pattern"] = param.pattern
            if param.default is not None:
                prop["default"] = param.default
            if param.items:
                prop["items"] = param.items

            properties[param.name] = prop

            if param.required:
                required.append(param.name)

        return {
            "name": self.metadata.name,
            "description": self.metadata.description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": required,
            },
        }

    def __repr__(self) -> str:
        return f"<Tool: {self.metadata.name} v{self.metadata.version}>'"
