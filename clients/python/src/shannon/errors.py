"""Exception hierarchy for Shannon SDK.

Shannon SDK 的 exception 层次结构。
"""

from typing import Any, Dict, Optional


class ShannonError(Exception):
    """Base exception for all Shannon SDK errors.

    所有 Shannon SDK 错误的 base exception。
    """

    def __init__(
        self,
        message: str,
        code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(message)
        self.message = message
        self.code = code
        self.details = details or {}

    def __str__(self) -> str:
        if self.code:
            return f"[{self.code}] {self.message}"
        return self.message


class ConnectionError(ShannonError):
    """Connection to Shannon service failed.

    连接 Shannon 服务失败。
    """

    pass


class AuthenticationError(ShannonError):
    """Authentication failed (invalid token or credentials).

    authentication 失败（token 或凭据无效）。
    """

    pass


class TaskError(ShannonError):
    """Base exception for task-related errors.

    task 相关错误的基类。
    """

    pass


class TaskNotFoundError(TaskError):
    """Task does not exist.

    task 不存在。
    """

    pass


class TaskTimeoutError(TaskError):
    """Task exceeded timeout.

    task 超过 timeout。
    """

    pass


class TaskCancelledError(TaskError):
    """Task was cancelled.

    task 已被取消。
    """

    pass


class SessionError(ShannonError):
    """Base exception for session-related errors.

    session 相关错误的基类。
    """

    pass


class SessionNotFoundError(SessionError):
    """Session does not exist.

    session 不存在。
    """

    pass


class SessionExpiredError(SessionError):
    """Session has expired.

    session 已过期。
    """

    pass


class ValidationError(ShannonError):
    """Invalid parameters provided.

    传入的参数无效。
    """

    pass

class PermissionDeniedError(ShannonError):
    """Forbidden (authorization) error.

    禁止访问（authorization）错误。
    """

    pass

class RateLimitError(ShannonError):
    """Too many requests / rate limited.

    request 过多 / 触发 rate limit。
    """

    pass

class ServerError(ShannonError):
    """Upstream server error (5xx).

    上游服务端错误（5xx）。
    """

    pass

class TemplateError(ShannonError):
    """Base exception for template-related errors.

    模板相关错误的基类。
    """

    pass


class TemplateNotFoundError(TemplateError):
    """Template does not exist.

    模板不存在。
    """

    pass
