"""
Python WASI Executor Tool - Production Implementation
Python WASI Executor tool——生产实现

This tool provides secure Python code execution via WebAssembly System Interface (WASI).
It uses a full CPython 3.11.4 interpreter compiled to WebAssembly for true sandboxing.
该 tool 通过 WebAssembly System Interface（WASI）提供安全的 Python 代码执行能力。
它使用编译为 WebAssembly 的完整 CPython 3.11.4 解释器，实现真正的 sandbox 隔离。

Features:
特性：
- Full Python standard library support
  - 完整支持 Python 标准库
- Memory and CPU resource limits
  - memory 与 CPU 资源限制
- Timeout protection
  - timeout 保护
- Secure filesystem isolation
  - 安全的文件系统隔离
- Output streaming capability
  - 输出流式能力
- Session persistence (optional)
  - session 持久化（可选）
"""

import ast
import asyncio
import hashlib
import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any

import grpc
from google.protobuf import struct_pb2

from ...config import Settings
from ...grpc_gen.agent import agent_pb2, agent_pb2_grpc
from ...grpc_gen.common import common_pb2
from ..base import (
    Tool,
    ToolMetadata,
    ToolParameter,
    ToolParameterType,
    ToolResult,
)

logger = logging.getLogger(__name__)


@dataclass
class ExecutionSession:
    """Represents a persistent Python execution session
    表示一个持久化的 Python 执行 session"""

    session_id: str
    variables: dict[str, Any] = field(default_factory=dict)
    imports: list[str] = field(default_factory=list)
    last_accessed: float = field(default_factory=time.time)
    execution_count: int = 0


class PythonWasiExecutorTool(Tool):
    """
    Production Python executor using WASI sandbox.
    使用 WASI sandbox 的生产级 Python executor。

    This tool executes Python code in a secure WebAssembly sandbox using
    a full CPython interpreter compiled to WASM. It provides complete
    Python standard library support while maintaining strict security isolation.
    该 tool 在安全的 WebAssembly sandbox 中执行 Python 代码，使用编译为 WASM 的
    完整 CPython 解释器。它在提供完整 Python 标准库支持的同时保持严格的安全隔离。
    """

    # Class-level interpreter cache for performance
    # 类级别的解释器 cache，用于提升性能
    _interpreter_cache: bytes | None = None
    _cache_hash: str | None = None
    _sessions: dict[str, ExecutionSession] = {}
    _session_lock: asyncio.Lock = asyncio.Lock()  # Thread-safe session access 注：线程安全的 session 访问
    _max_sessions: int = 100
    _session_timeout: int = int(
        os.getenv("PYTHON_WASI_SESSION_TIMEOUT", "3600")
    )  # Default 1 hour 注：默认 1 小时

    def __init__(self):
        # Initialize settings before calling super().__init__()
        # because _get_metadata() needs it
        # 必须在调用 super().__init__() 之前初始化 settings，
        # 因为 _get_metadata() 需要用到它
        self.settings = Settings()
        self.executor_mode = os.getenv("PYTHON_EXECUTOR_MODE", "wasi").strip().lower()
        self.interpreter_path = os.getenv(
            "PYTHON_WASI_WASM_PATH", "/opt/wasm-interpreters/python-3.11.4.wasm"
        )
        self.agent_core_addr = os.getenv("AGENT_CORE_ADDR", "agent-core:50051")
        super().__init__()
        self._load_interpreter_cache()

    def _get_metadata(self) -> ToolMetadata:
        if self.executor_mode == "firecracker":
            desc = (
                "Execute Python code in secure Firecracker microVM. "
                "Full Python environment with common packages (numpy, pandas, matplotlib, scipy, etc.). "
                "Save files to /workspace/ for persistence. Files in /tmp are ephemeral."
            )
        else:
            desc = (
                "Execute Python code in secure WASI sandbox. "
                "ONLY Python standard library is available (math, json, csv, re, datetime, statistics, hashlib, etc.). "
                "NO third-party packages (numpy, pandas, matplotlib, scipy, requests are NOT installed). "
                "Save files to /workspace/ for persistence. Files in /tmp are ephemeral."
            )
        return ToolMetadata(
            name="python_executor",
            version="2.0.0",
            description=desc,
            category="code",
            author="Shannon",
            requires_auth=False,
            rate_limit=self.settings.python_executor_rate_limit,  # Configurable via PYTHON_EXECUTOR_RATE_LIMIT env var 注：可通过 PYTHON_EXECUTOR_RATE_LIMIT 环境变量配置
            timeout_seconds=30,
            memory_limit_mb=256,
            sandboxed=True,
            dangerous=False,  # Safe due to WASI isolation 注：因 WASI 隔离而安全
            cost_per_use=0.001,  # Minimal cost for compute 注：计算资源的最低成本
            session_aware=True,  # Required for Firecracker workspace isolation 注：Firecracker 的 workspace 隔离所必需
        )

    def _get_parameters(self) -> list[ToolParameter]:
        return [
            ToolParameter(
                name="code",
                type=ToolParameterType.STRING,
                description=(
                    "Python code to execute (full environment with pip packages)"
                    if self.executor_mode == "firecracker"
                    else "Python code to execute (stdlib only — no pip packages like numpy/pandas/matplotlib)"
                ),
                required=True,
            ),
            ToolParameter(
                name="session_id",
                type=ToolParameterType.STRING,
                description="Optional session ID for persistent variables across executions",
                required=False,
            ),
            ToolParameter(
                name="timeout_seconds",
                type=ToolParameterType.INTEGER,
                description="Execution timeout in seconds (default: 30, max: 60)",
                required=False,
                default=30,
            ),
            ToolParameter(
                name="stdin",
                type=ToolParameterType.STRING,
                description="Optional input data to provide via stdin",
                required=False,
                default="",
            ),
        ]

    def _load_interpreter_cache(self):
        """Load and cache the Python WASM interpreter for performance
        加载并 cache Python WASM 解释器以提升性能"""
        try:
            # Check if interpreter exists
            # 检查解释器是否存在
            if not os.path.exists(self.interpreter_path):
                logger.warning(
                    f"Python WASI interpreter not found at {self.interpreter_path}"
                )
                return

            # Calculate hash to detect changes
            # 计算 hash 以检测变更
            with open(self.interpreter_path, "rb") as f:
                content = f.read()
                new_hash = hashlib.sha256(content).hexdigest()

            # Cache if new or changed
            # 若为新增或已变更则 cache
            if self._cache_hash != new_hash:
                self._interpreter_cache = content
                self._cache_hash = new_hash
                logger.info(f"Cached Python WASI interpreter ({len(content)} bytes)")
        except Exception as e:
            logger.error(f"Failed to cache interpreter: {e}")

    async def _get_or_create_session(
        self, session_id: str | None
    ) -> ExecutionSession | None:
        """Get or create a persistent execution session (thread-safe)
        获取或创建持久化的执行 session（线程安全）"""
        if not session_id:
            return None

        async with self._session_lock:
            # Clean expired sessions
            # 清理已过期的 session
            current_time = time.time()
            expired = [
                sid
                for sid, sess in self._sessions.items()
                if current_time - sess.last_accessed > self._session_timeout
            ]
            for sid in expired:
                del self._sessions[sid]

            # Get or create session
            # 获取或创建 session
            if session_id not in self._sessions:
                if len(self._sessions) >= self._max_sessions:
                    # Remove oldest session
                    # 移除最久未使用的 session
                    oldest = min(
                        self._sessions.items(), key=lambda x: x[1].last_accessed
                    )
                    del self._sessions[oldest[0]]

                self._sessions[session_id] = ExecutionSession(session_id=session_id)

            session = self._sessions[session_id]
            session.last_accessed = current_time
            session.execution_count += 1

            return session

    def _prepare_code_with_session(
        self, code: str, session: ExecutionSession | None
    ) -> str:
        """Prepare code with session context if available
        若存在 session 则为其准备带 session context 的代码"""
        if not session:
            return code

        # Build session context
        # 构建 session context
        context_lines = []

        # Restore imports
        # 恢复 import
        for imp in session.imports:
            context_lines.append(imp)

        # Restore variables
        # 恢复变量
        if session.variables:
            context_lines.append("# Restored session variables")
            for name, value in session.variables.items():
                if isinstance(value, (int, float, str, bool, list, dict)):
                    context_lines.append(f"{name} = {repr(value)}")

        # Combine with new code
        # 与新代码合并
        if context_lines:
            full_code = "\n".join(context_lines) + "\n\n" + code
        else:
            full_code = code

        # Add code to capture new variables
        # 追加用于捕获新变量的代码
        capture_code = """
# Capture session state
import sys
import json
_session_vars = {k: v for k, v in globals().items()
                 if not k.startswith('_') and k not in ['sys', 'json']}
print("__SESSION_STATE__", json.dumps({
    k: repr(v) for k, v in _session_vars.items()
    if isinstance(v, (int, float, str, bool, list, dict))
}), sep=":", end="__END_SESSION__")
"""

        return full_code + "\n" + capture_code

    async def _extract_session_state(
        self, output: str, session: ExecutionSession | None
    ) -> str:
        """Extract and store session state from output (thread-safe).
        从输出中提取并存储 session 状态（线程安全）。

        Note: Session state persistence is limited to Python literals that can be
        evaluated with ast.literal_eval (int, float, str, bool, list, dict, tuple, None).
        Complex objects (classes, functions, etc.) will be stored as string representations
        but won't be restored as functional objects in future sessions.
        注意：session 状态的持久化仅限于可用 ast.literal_eval 求值的 Python 字面量
        （int、float、str、bool、list、dict、tuple、None）。复杂对象（类、函数等）
        会以字符串表示形式存储，但在后续 session 中不会被恢复为可用的对象。
        """
        if not session or "__SESSION_STATE__" not in output:
            return output

        try:
            # Extract session state
            # 提取 session 状态
            parts = output.split("__SESSION_STATE__:")
            if len(parts) == 2:
                clean_output = parts[0]
                state_part = parts[1].split("__END_SESSION__")[0]

                # Parse state
                # 解析状态
                state = json.loads(state_part)

                # Use lock when modifying session variables
                # 修改 session 变量时加锁
                async with self._session_lock:
                    for name, repr_value in state.items():
                        try:
                            # Safely evaluate the repr'd value using ast.literal_eval
                            # This limits persistence to Python literals only
                            # 使用 ast.literal_eval 安全地求值 repr 后的值
                            # 这将持久化范围限制为 Python 字面量
                            session.variables[name] = ast.literal_eval(repr_value)
                        except (ValueError, SyntaxError):
                            # For complex objects, store string representation
                            # 对复杂对象存储字符串表示
                            session.variables[name] = repr_value
                            logger.debug(
                                f"Session variable '{name}' stored as string (not a Python literal)"
                            )

                return clean_output
        except Exception as e:
            logger.debug(f"Failed to extract session state: {e}")

        return output

    async def _execute_impl(
        self, session_context: dict | None = None, **kwargs
    ) -> ToolResult:
        """Execute Python code in WASI sandbox
        在 WASI sandbox 中执行 Python 代码"""

        code = kwargs.get("code", "")
        # Get session_id from kwargs first, then fallback to session_context
        # 优先从 kwargs 获取 session_id，其次回退到 session_context
        session_id = kwargs.get("session_id")
        if not session_id and session_context:
            session_id = session_context.get("session_id")

        # Log session_id source for debugging
        # 记录 session_id 来源以便调试
        if session_id:
            logger.info(f"python_executor: session_id={session_id} (from {'kwargs' if kwargs.get('session_id') else 'session_context'})")
        else:
            logger.warning("python_executor: no session_id available - workspace isolation disabled")
        timeout = min(kwargs.get("timeout_seconds", 30), 60)  # Max 60 seconds 注：最长 60 秒
        # stdin = kwargs.get("stdin", "")  # Not used currently, but kept for future use

        if not code:
            return ToolResult(
                success=False,
                output=None,
                error="No code provided to execute",
            )

        try:
            # Get or create session
            # 获取或创建 session
            session = await self._get_or_create_session(session_id)

            # Prepare code with session context
            # 准备带 session context 的代码
            if session:
                code = self._prepare_code_with_session(code, session)
                logger.debug(
                    f"Executing in session {session_id} (run #{session.execution_count})"
                )

            # Emit progress via observer if available
            # 若存在 observer 则上报进度
            observer = kwargs.get("observer")
            if observer:
                try:
                    observer("progress", {"message": "Preparing Python environment..."})
                except Exception:
                    pass

            if self.executor_mode == "firecracker":
                tool_params = {
                    "tool": "firecracker_executor",
                    "code": code,
                    "session_id": session_id,
                    "timeout_seconds": timeout,
                    "exec_mode": "firecracker",
                }
            else:
                # Prepare execution request with proper structure for agent-core
                # 为 agent-core 准备结构正确的执行 request
                # Note: Using file path instead of base64 due to gRPC 4MB message limit
                # 注意：因 gRPC 4MB 消息上限，使用文件路径而非 base64
                # Python WASM needs argv[0] to be program name, then -c flag to execute stdin
                # Python WASM 需要 argv[0] 作为程序名，再用 -c 参数执行 stdin
                tool_params = {
                    "tool": "code_executor",  # Required field for agent-core 注：agent-core 必填字段
                    "wasm_path": self.interpreter_path,  # Use file path (Python.wasm is 20MB) 注：使用文件路径（Python.wasm 为 20MB）
                    "stdin": code,  # Pass Python code as stdin 注：将 Python 代码作为 stdin 传入
                    "argv": [
                        "python",
                        "-c",
                        "import sys; exec(sys.stdin.read())",
                    ],  # Python argv format 注：Python argv 格式
                }

            # Build gRPC request - agent-core expects tool_parameters directly in context
            # 构建 gRPC request——agent-core 期望 tool_parameters 直接位于 context 中
            ctx = struct_pb2.Struct()
            ctx.update({"tool_parameters": tool_params})

            available_tools = (
                ["firecracker_executor"]
                if self.executor_mode == "firecracker"
                else ["code_executor"]
            )

            req = agent_pb2.ExecuteTaskRequest(
                query=f"Execute Python code (session: {session_id or 'none'})",
                context=ctx,
                available_tools=available_tools,
            )

            # Set session_context for workspace and memory isolation
            # 设置 session_context 以实现 workspace 与 memory 隔离
            if session_id:
                req.session_context.session_id = session_id
                # Also set in metadata for defense-in-depth
                # 同时写入 metadata 作为纵深防御
                req.metadata.session_id = session_id

            # Set user_id for /memory mount in WASI sandbox
            # 设置 user_id 以供 WASI sandbox 挂载 /memory
            user_id = session_context.get("user_id") if session_context else None
            if user_id:
                req.session_context.user_id = user_id
                req.metadata.user_id = user_id

            logger.info(f"python_executor: gRPC request session_id={session_id} user_id={user_id or 'none'}")

            if hasattr(common_pb2, "ExecutionMode"):
                req.mode = int(common_pb2.ExecutionMode.EXECUTION_MODE_SIMPLE)

            # Execute via Agent Core with timeout
            # 通过 Agent Core 执行，并设置 timeout
            start_time = time.time()

            async with grpc.aio.insecure_channel(self.agent_core_addr) as channel:
                stub = agent_pb2_grpc.AgentServiceStub(channel)

                # Use asyncio timeout for better control
                # 使用 asyncio timeout 以获得更好的控制
                try:
                    resp = await asyncio.wait_for(
                        stub.ExecuteTask(req), timeout=timeout
                    )
                except asyncio.TimeoutError:
                    return ToolResult(
                        success=False,
                        output=None,
                        error=f"Execution timeout after {timeout} seconds",
                        metadata={"timeout": True, "session_id": session_id},
                    )

            execution_time = time.time() - start_time

            # Process response
            # 处理 response
            if hasattr(resp, "result") and resp.result:
                output = resp.result

                # Extract session state if applicable
                # 若适用则提取 session 状态
                if session:
                    output = await self._extract_session_state(output, session)

                return ToolResult(
                    success=True,
                    output=output,
                    metadata={
                        "execution_time_ms": int(execution_time * 1000),
                        "session_id": session_id,
                        "execution_count": session.execution_count if session else 1,
                        "interpreter": (
                            "Firecracker (Linux)"
                            if self.executor_mode == "firecracker"
                            else "CPython 3.11.4 (WASI)"
                        ),
                    },
                )
            else:
                error_msg = (
                    resp.error_message
                    if hasattr(resp, "error_message")
                    else "Unknown error"
                )
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Execution failed: {error_msg}",
                    metadata={"session_id": session_id},
                )

        except grpc.RpcError as e:
            logger.error(f"gRPC error: {e.code()}: {e.details()}")
            return ToolResult(
                success=False,
                output=None,
                error=f"Communication error: {e.details()}",
            )
        except Exception as e:
            logger.error(f"Execution error: {e}", exc_info=True)
            return ToolResult(
                success=False,
                output=None,
                error=f"Execution failed: {str(e)}",
            )

    @classmethod
    def clear_sessions(cls):
        """Clear all execution sessions
        清空所有执行 session"""
        cls._sessions.clear()
        logger.info("Cleared all Python execution sessions")

    @classmethod
    def get_session_info(cls, session_id: str) -> dict[str, Any] | None:
        """Get information about a session
        获取某个 session 的信息"""
        if session_id in cls._sessions:
            session = cls._sessions[session_id]
            return {
                "session_id": session.session_id,
                "variables": list(session.variables.keys()),
                "imports": session.imports,
                "execution_count": session.execution_count,
                "last_accessed": session.last_accessed,
            }
        return None


# Export the tool class
# 导出该 tool 类
__all__ = ["PythonWasiExecutorTool"]
