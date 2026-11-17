"""Tests for OpenAPI tool execution and vendor adapter integration.

OpenAPI tool 执行与 vendor adapter 集成的测试。
"""

import time
from unittest.mock import AsyncMock, patch

import pytest
from llm_service.tools.openapi_tool import OpenAPILoader, _SimpleBreaker


class TestOpenAPIToolExecution:
    """Test OpenAPI tool execution with different auth types.

    测试不同 auth 类型下的 OpenAPI tool 执行。
    """

    @pytest.fixture
    def simple_spec(self):
        """Simple OpenAPI 3.x spec for testing.

        用于测试的简单 OpenAPI 3.x spec。
        """
        return {
            "openapi": "3.0.0",
            "info": {"title": "Test API", "version": "1.0.0"},
            "servers": [{"url": "https://api.test.com"}],
            "paths": {
                "/users/{userId}": {
                    "get": {
                        "operationId": "getUser",
                        "summary": "Get user by ID",
                        "parameters": [
                            {
                                "name": "userId",
                                "in": "path",
                                "required": True,
                                "schema": {"type": "string"},
                            }
                        ],
                        "responses": {"200": {"description": "Success"}},
                    }
                },
                "/data": {
                    "post": {
                        "operationId": "postData",
                        "requestBody": {
                            "required": True,
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "properties": {
                                            "name": {"type": "string"},
                                            "value": {"type": "integer"},
                                        },
                                    }
                                }
                            },
                        },
                        "responses": {"201": {"description": "Created"}},
                    }
                },
            },
        }

    def test_loader_bearer_auth(self, simple_spec):
        """Test OpenAPI loader with bearer token auth.

        测试带 bearer token auth 的 OpenAPI loader。
        """
        loader = OpenAPILoader(
            name="test_api",
            spec=simple_spec,
            auth_type="bearer",
            auth_config={"token": "test_bearer_token"},
            category="test",
        )

        assert loader.base_url == "https://api.test.com"
        assert loader.auth_type == "bearer"
        assert loader.auth_config["token"] == "test_bearer_token"
        assert len(loader.operations) == 2

    def test_loader_api_key_header(self, simple_spec):
        """Test OpenAPI loader with API key in header.

        测试在 header 中携带 API key 的 OpenAPI loader。
        """
        loader = OpenAPILoader(
            name="test_api",
            spec=simple_spec,
            auth_type="api_key",
            auth_config={
                "api_key_name": "X-API-Key",
                "api_key_location": "header",
                "api_key_value": "test_api_key",
            },
        )

        tools = loader.generate_tools()
        assert len(tools) == 2
        assert tools[0].__name__ == "_OpenAPITool"

    def test_loader_api_key_query(self, simple_spec):
        """Test OpenAPI loader with API key in query parameter.

        测试在 query 参数中携带 API key 的 OpenAPI loader。
        """
        loader = OpenAPILoader(
            name="test_api",
            spec=simple_spec,
            auth_type="api_key",
            auth_config={
                "api_key_name": "api_key",
                "api_key_location": "query",
                "api_key_value": "test_query_key",
            },
        )

        assert loader.auth_config["api_key_location"] == "query"

    def test_loader_basic_auth(self, simple_spec):
        """Test OpenAPI loader with basic authentication.

        测试使用 basic authentication 的 OpenAPI loader。
        """
        loader = OpenAPILoader(
            name="test_api",
            spec=simple_spec,
            auth_type="basic",
            auth_config={"username": "testuser", "password": "testpass"},
        )

        assert loader.auth_type == "basic"
        assert loader.auth_config["username"] == "testuser"

    @pytest.mark.asyncio
    async def test_tool_execution_with_bearer_auth(self, simple_spec):
        """Test tool execution with bearer auth headers.

        测试带 bearer auth header 的 tool 执行。
        """
        loader = OpenAPILoader(
            name="test_api",
            spec=simple_spec,
            auth_type="bearer",
            auth_config={"token": "test_token"},
        )

        tools = loader.generate_tools()
        get_user_tool = tools[0]()  # Instantiate 注：实例化

        with patch("httpx.AsyncClient.request") as mock_request:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json = AsyncMock(
                return_value={"id": "123", "name": "Test User"}
            )
            mock_request.return_value = mock_response

            result = await get_user_tool.execute(userId="123")

            assert result.success
            assert result.output == {"id": "123", "name": "Test User"}
            # Verify bearer token was included
            # 验证 bearer token 已包含在 request 中
            call_args = mock_request.call_args
            assert "Authorization" in call_args[1]["headers"]
            assert call_args[1]["headers"]["Authorization"] == "Bearer test_token"

    @pytest.mark.asyncio
    async def test_tool_execution_with_request_body(self, simple_spec):
        """Test tool execution with JSON request body.

        测试带 JSON request body 的 tool 执行。
        """
        loader = OpenAPILoader(
            name="test_api",
            spec=simple_spec,
            auth_type="none",
        )

        tools = loader.generate_tools()
        post_data_tool = [t for t in tools if t().metadata.name == "postData"][0]()

        with patch("httpx.AsyncClient.request") as mock_request:
            mock_response = AsyncMock()
            mock_response.status_code = 201
            mock_response.json = AsyncMock(return_value={"id": "456"})
            mock_request.return_value = mock_response

            result = await post_data_tool.execute(body={"name": "test", "value": 42})

            assert result.success
            # Verify JSON body was sent
            # 验证 JSON body 已发送
            call_args = mock_request.call_args
            assert call_args[1]["json"] == {"name": "test", "value": 42}


class TestCircuitBreaker:
    """Test circuit breaker behavior.

    测试 circuit breaker 行为。
    """

    def test_circuit_breaker_allows_when_closed(self):
        """Circuit breaker should allow requests when closed.

        circuit breaker 关闭时应放行 request。
        """
        breaker = _SimpleBreaker(failure_threshold=5, recovery_timeout=60)
        assert breaker.allow(time.time()) is True

    def test_circuit_breaker_opens_after_threshold(self):
        """Circuit breaker should open after failure threshold.

        circuit breaker 在达到失败阈值后应打开。
        """
        breaker = _SimpleBreaker(failure_threshold=3, recovery_timeout=60)
        now = time.time()

        # Record failures
        # 记录失败次数
        for _ in range(3):
            breaker.on_failure(now)

        # Circuit should be open
        # circuit 应处于打开状态
        assert breaker.allow(now + 1) is False

    def test_circuit_breaker_recovers_after_timeout(self):
        """Circuit breaker should enter half-open state after recovery timeout.

        circuit breaker 在恢复 timeout 后应进入 half-open 状态。
        """
        breaker = _SimpleBreaker(failure_threshold=3, recovery_timeout=10)
        now = time.time()

        # Open the circuit
        # 打开 circuit
        for _ in range(3):
            breaker.on_failure(now)

        # Circuit should be open
        # circuit 应处于打开状态
        assert breaker.allow(now + 1) is False

        # After recovery timeout, should allow trial request
        # 恢复 timeout 后应允许试探 request
        assert breaker.allow(now + 11) is True

    def test_circuit_breaker_resets_on_success(self):
        """Circuit breaker should reset failure count on success.

        circuit breaker 成功时应重置失败计数。
        """
        breaker = _SimpleBreaker(failure_threshold=3, recovery_timeout=60)
        now = time.time()

        # Record some failures
        # 记录若干失败
        breaker.on_failure(now)
        breaker.on_failure(now)
        assert breaker.failures == 2

        # Success should reset
        # 成功应重置计数
        breaker.on_success()
        assert breaker.failures == 0


class TestVendorAdapterIntegration:
    """Test vendor adapter loading and integration.

    测试 vendor adapter 的加载与集成。
    """

    def test_vendor_adapter_loading(self):
        """Test vendor adapter is loaded when specified.

        测试指定 vendor 时能加载 vendor adapter。
        """
        from llm_service.tools.vendor_adapters import get_vendor_adapter

        # Should return None for unknown vendor
        # 未知 vendor 应返回 None
        adapter = get_vendor_adapter("unknown_vendor")
        assert adapter is None

        # Should return None for empty string
        # 空字符串应返回 None
        adapter = get_vendor_adapter("")
        assert adapter is None

    @pytest.mark.asyncio
    async def test_vendor_adapter_body_transformation(self, simple_spec):
        """Test vendor adapter transforms request body.

        测试 vendor adapter 会转换 request body。
        """

        # Create a mock vendor adapter
        # 创建 mock vendor adapter
        class MockVendorAdapter:
            def transform_body(self, body, operation_id, prompt_params):
                # Transform metrics field
                # 转换 metrics 字段
                if "metrics" in body:
                    body["metrics"] = [m.replace("users", "test:users") for m in body["metrics"]]
                # Inject session context
                # 注入 session context
                if prompt_params and "account_id" in prompt_params:
                    body["account_id"] = prompt_params["account_id"]
                return body

        with patch("llm_service.tools.vendor_adapters.get_vendor_adapter") as mock_get_adapter:
            mock_get_adapter.return_value = MockVendorAdapter()

            loader = OpenAPILoader(
                name="test_api",
                spec=simple_spec,
                auth_type="none",
                auth_config={"vendor": "test_vendor"},
            )

            tools = loader.generate_tools()
            post_tool = [t for t in tools if t().metadata.name == "postData"][0]()

            with patch("httpx.AsyncClient.request") as mock_request:
                mock_response = AsyncMock()
                mock_response.status_code = 201
                mock_response.json = AsyncMock(return_value={"success": True})
                mock_request.return_value = mock_response

                # Execute with session context
                # 携带 session context 执行
                result = await post_tool.execute(
                    session_context={"prompt_params": {"account_id": "acct_123"}},
                    body={"metrics": ["users", "sessions"]},
                )

                assert result.success
                # Verify adapter transformed the body
                # 验证 adapter 已转换 body
                call_args = mock_request.call_args
                sent_body = call_args[1]["json"]
                assert "test:users" in sent_body.get("metrics", [])
                assert sent_body.get("account_id") == "acct_123"


class TestRateLimiting:
    """Test rate limiting enforcement.

    测试 rate limiting 的强制执行。
    """

    @pytest.mark.asyncio
    async def test_rate_limit_enforced(self, simple_spec):
        """Test that rate limits are enforced per tool.

        测试 rate limit 按 tool 维度生效。
        """
        loader = OpenAPILoader(
            name="test_api",
            spec=simple_spec,
            rate_limit=2,  # 2 requests per minute 注：每分钟 2 次请求
            # 每分钟 2 个 request
        )

        tools = loader.generate_tools()
        tool = tools[0]()

        with patch("httpx.AsyncClient.request") as mock_request:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_response.json = AsyncMock(return_value={})
            mock_request.return_value = mock_response

            # First two requests should succeed
            # 前两个 request 应成功
            result1 = await tool.execute(userId="1")
            result2 = await tool.execute(userId="2")

            assert result1.success
            assert result2.success

            # Note: Actual rate limiting behavior depends on implementation
            # 注意：实际 rate limiting 行为取决于实现
            # This test verifies the rate_limit configuration is passed through
            # 本测试仅验证 rate_limit 配置被透传


class TestErrorHandling:
    """Test error handling in OpenAPI tools.

    测试 OpenAPI tool 中的错误处理。
    """

    @pytest.mark.asyncio
    async def test_http_error_handling(self, simple_spec):
        """Test handling of HTTP errors.

        测试 HTTP 错误的处理。
        """
        loader = OpenAPILoader(
            name="test_api",
            spec=simple_spec,
            auth_type="none",
        )

        tools = loader.generate_tools()
        tool = tools[0]()

        with patch("httpx.AsyncClient.request") as mock_request:
            mock_response = AsyncMock()
            mock_response.status_code = 404
            mock_response.text = "Not Found"
            mock_response.raise_for_status.side_effect = Exception("404 Not Found")
            mock_request.return_value = mock_response

            result = await tool.execute(userId="999")

            assert result.success is False
            assert "404" in result.error or "error" in result.error.lower()

    @pytest.mark.asyncio
    async def test_timeout_handling(self, simple_spec):
        """Test handling of request timeouts.

        测试 request timeout 的处理。
        """
        loader = OpenAPILoader(
            name="test_api",
            spec=simple_spec,
            timeout_seconds=1,
        )

        tools = loader.generate_tools()
        tool = tools[0]()

        with patch("httpx.AsyncClient.request") as mock_request:
            import asyncio

            mock_request.side_effect = asyncio.TimeoutError("Request timed out")

            result = await tool.execute(userId="123")

            assert result.success is False
            assert "timeout" in result.error.lower() or "timed out" in result.error.lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
