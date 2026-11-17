"""Tests for structured output (output_config) support.

结构化输出 (output_config) 支持的测试：字段默认值、Anthropic provider 在
complete()/stream_complete() 中注入 extra_body，以及 Lead 决策 schema 的契约。
"""

from unittest.mock import MagicMock, patch

import pytest
from llm_provider.base import CompletionRequest, ModelTier


class TestCompletionRequestOutputConfig:
    """Test output_config field on CompletionRequest.

    测试 CompletionRequest 上的 output_config 字段。
    """

    def test_output_config_default_none(self):
        # 未显式传入时必须为 None，provider 才不会往 API request 里注入 extra_body
        req = CompletionRequest(messages=[{"role": "user", "content": "hi"}])
        assert req.output_config is None

    def test_output_config_accepts_dict(self):
        # Anthropic 风格：format.type=json_schema + 一份 JSON Schema 主体
        schema = {
            "format": {
                "type": "json_schema",
                "schema": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                    "required": ["name"],
                    "additionalProperties": False,
                },
            }
        }
        req = CompletionRequest(
            messages=[{"role": "user", "content": "hi"}],
            output_config=schema,
        )
        assert req.output_config == schema
        assert req.output_config["format"]["type"] == "json_schema"


class TestAnthropicOutputConfig:
    """Test Anthropic provider injects output_config into API request.

    测试 Anthropic provider 把 output_config 注入 API request。
    """

    @pytest.mark.asyncio
    async def test_output_config_injected_into_api_request(self):
        """output_config should appear in the Anthropic API request dict.

        output_config 应出现在 Anthropic 的 API request dict 中。
        """
        from llm_provider.anthropic_provider import AnthropicProvider
        from llm_provider.base import CompletionRequest

        schema = {
            "format": {
                "type": "json_schema",
                "schema": {
                    "type": "object",
                    "properties": {"decision": {"type": "string"}},
                    "required": ["decision"],
                    "additionalProperties": False,
                },
            }
        }

        request = CompletionRequest(
            messages=[
                {"role": "system", "content": "You are a helper."},
                {"role": "user", "content": "Decide something."},
            ],
            model_tier=ModelTier.MEDIUM,
            output_config=schema,
            max_tokens=1024,
        )

        # Capture what gets passed to client.messages.create
        # 捕获传给 client.messages.create 的实参，用于断言注入位置
        captured_kwargs = {}

        async def mock_create(**kwargs):
            captured_kwargs.update(kwargs)
            mock_resp = MagicMock()
            mock_resp.content = [MagicMock(type="text", text='{"decision": "ok"}')]
            mock_resp.usage = MagicMock(
                input_tokens=10, output_tokens=5,
                cache_read_input_tokens=0, cache_creation_input_tokens=0,
            )
            mock_resp.model = "claude-sonnet-4-6"
            mock_resp.stop_reason = "end_turn"
            return mock_resp

        # 绕过 __init__ 直接装配 provider，避免真实 API key 与网络客户端
        provider = AnthropicProvider.__new__(AnthropicProvider)
        provider.config = {}
        provider.models = {
            "claude-sonnet-4-6": MagicMock(
                model_id="claude-sonnet-4-6",
                context_window=200000,
                max_tokens=64000,
                supports_functions=True,
                tier=ModelTier.MEDIUM,
            )
        }
        provider.client = MagicMock()
        provider.client.messages.create = mock_create
        provider.estimate_cost = MagicMock(return_value=0.001)

        # 固定 tier → model 的选择，避免依赖真实模型目录
        with patch.object(provider, "select_model_for_tier", return_value=provider.models["claude-sonnet-4-6"]):
            await provider.complete(request)

        assert "extra_body" in captured_kwargs, f"extra_body missing from API request. Keys: {list(captured_kwargs.keys())}"
        assert captured_kwargs["extra_body"]["output_config"] == schema

    @pytest.mark.asyncio
    async def test_no_output_config_when_not_set(self):
        """output_config should NOT appear in API request when not provided.

        未提供 output_config 时，API request 中不应出现该字段。
        """
        from llm_provider.anthropic_provider import AnthropicProvider
        from llm_provider.base import CompletionRequest

        request = CompletionRequest(
            messages=[{"role": "user", "content": "Hello"}],
            model_tier=ModelTier.MEDIUM,
            max_tokens=1024,
        )

        captured_kwargs = {}

        async def mock_create(**kwargs):
            captured_kwargs.update(kwargs)
            mock_resp = MagicMock()
            mock_resp.content = [MagicMock(type="text", text="Hello!")]
            mock_resp.usage = MagicMock(
                input_tokens=10, output_tokens=5,
                cache_read_input_tokens=0, cache_creation_input_tokens=0,
            )
            mock_resp.model = "claude-sonnet-4-5-20250929"
            mock_resp.stop_reason = "end_turn"
            return mock_resp

        provider = AnthropicProvider.__new__(AnthropicProvider)
        provider.config = {}
        provider.models = {
            "claude-sonnet-4-5-20250929": MagicMock(
                model_id="claude-sonnet-4-5-20250929",
                context_window=200000,
                max_tokens=64000,
                supports_functions=True,
                tier=ModelTier.MEDIUM,
            )
        }
        provider.client = MagicMock()
        provider.client.messages.create = mock_create
        provider.estimate_cost = MagicMock(return_value=0.001)

        with patch.object(provider, "select_model_for_tier", return_value=provider.models["claude-sonnet-4-5-20250929"]):
            await provider.complete(request)

        assert "extra_body" not in captured_kwargs


class TestAnthropicStreamOutputConfig:
    """Test Anthropic provider injects output_config into stream_complete() API request.

    测试 Anthropic provider 在 stream_complete() 的 API request 中注入 output_config。
    """

    @pytest.mark.asyncio
    async def test_output_config_injected_into_stream_request(self):
        """output_config should appear in the stream_complete() API request via extra_body.

        output_config 应通过 extra_body 出现在 stream_complete() 的 API request 中。
        """
        from llm_provider.anthropic_provider import AnthropicProvider
        from llm_provider.base import CompletionRequest

        schema = {
            "format": {
                "type": "json_schema",
                "schema": {
                    "type": "object",
                    "properties": {"answer": {"type": "string"}},
                    "required": ["answer"],
                    "additionalProperties": False,
                },
            }
        }

        request = CompletionRequest(
            messages=[
                {"role": "system", "content": "You are a helper."},
                {"role": "user", "content": "Answer something."},
            ],
            model_tier=ModelTier.MEDIUM,
            output_config=schema,
            max_tokens=1024,
        )

        # Capture what gets passed to client.messages.stream
        # 捕获传给 client.messages.stream 的实参
        captured_kwargs = {}

        # Build a mock async context manager for client.messages.stream()
        # 为 client.messages.stream() 构造 mock async context manager
        mock_final_message = MagicMock()
        mock_final_message.content = [MagicMock(type="text", text='{"answer": "ok"}')]
        mock_final_message.usage = MagicMock(
            input_tokens=10, output_tokens=5,
            cache_read_input_tokens=0, cache_creation_input_tokens=0,
        )
        mock_final_message.model = "claude-sonnet-4-6"

        class MockStreamCtx:
            """Mock async context manager for client.messages.stream().

            client.messages.stream() 的 mock async context manager。
            """

            def __init__(self, **kwargs):
                captured_kwargs.update(kwargs)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            @property
            def text_stream(self):
                return self._text_iter()

            async def _text_iter(self):
                yield '{"answer": "ok"}'

            async def get_final_message(self):
                return mock_final_message

        provider = AnthropicProvider.__new__(AnthropicProvider)
        provider.config = {}
        provider.models = {
            "claude-sonnet-4-6": MagicMock(
                model_id="claude-sonnet-4-6",
                context_window=200000,
                max_tokens=64000,
                supports_functions=True,
                tier=ModelTier.MEDIUM,
            )
        }
        provider.client = MagicMock()
        provider.client.messages.stream = MockStreamCtx
        provider.estimate_cost = MagicMock(return_value=0.001)

        with patch.object(provider, "select_model_for_tier", return_value=provider.models["claude-sonnet-4-6"]):
            chunks = []
            async for chunk in provider.stream_complete(request):
                chunks.append(chunk)

        assert "extra_body" in captured_kwargs, f"extra_body missing from stream API request. Keys: {list(captured_kwargs.keys())}"
        assert captured_kwargs["extra_body"]["output_config"] == schema

    @pytest.mark.asyncio
    async def test_no_output_config_in_stream_when_not_set(self):
        """output_config should NOT appear in stream API request when not provided.

        未提供 output_config 时，stream API request 中不应出现该字段。
        """
        from llm_provider.anthropic_provider import AnthropicProvider
        from llm_provider.base import CompletionRequest

        request = CompletionRequest(
            messages=[{"role": "user", "content": "Hello"}],
            model_tier=ModelTier.MEDIUM,
            max_tokens=1024,
        )

        captured_kwargs = {}

        mock_final_message = MagicMock()
        mock_final_message.content = [MagicMock(type="text", text="Hello!")]
        mock_final_message.usage = MagicMock(
            input_tokens=10, output_tokens=5,
            cache_read_input_tokens=0, cache_creation_input_tokens=0,
        )
        mock_final_message.model = "claude-sonnet-4-6"

        class MockStreamCtx:
            # 同上：只捕获 stream() 的实参，不实际发起网络 request
            def __init__(self, **kwargs):
                captured_kwargs.update(kwargs)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            @property
            def text_stream(self):
                return self._text_iter()

            async def _text_iter(self):
                yield "Hello!"

            async def get_final_message(self):
                return mock_final_message

        provider = AnthropicProvider.__new__(AnthropicProvider)
        provider.config = {}
        provider.models = {
            "claude-sonnet-4-6": MagicMock(
                model_id="claude-sonnet-4-6",
                context_window=200000,
                max_tokens=64000,
                supports_functions=True,
                tier=ModelTier.MEDIUM,
            )
        }
        provider.client = MagicMock()
        provider.client.messages.stream = MockStreamCtx
        provider.estimate_cost = MagicMock(return_value=0.001)

        with patch.object(provider, "select_model_for_tier", return_value=provider.models["claude-sonnet-4-6"]):
            chunks = []
            async for chunk in provider.stream_complete(request):
                chunks.append(chunk)

        assert "extra_body" not in captured_kwargs


class TestLeadDecisionSchema:
    """Test the Lead decision JSON schema is valid and matches Pydantic models.

    校验 Lead 决策 JSON schema 的结构：必填字段、action 枚举、
    以及所有 object 上的 additionalProperties: false。
    """

    def test_schema_importable(self):
        # schema 必须能从 lead.py import，且是 json_schema 形式
        from llm_service.api.lead import LEAD_DECISION_SCHEMA
        assert LEAD_DECISION_SCHEMA is not None
        assert LEAD_DECISION_SCHEMA["format"]["type"] == "json_schema"

    def test_schema_has_required_fields(self):
        # decision_summary 与 actions 是必填字段，比较用 set，顺序无关
        from llm_service.api.lead import LEAD_DECISION_SCHEMA
        schema = LEAD_DECISION_SCHEMA["format"]["schema"]
        assert "decision_summary" in schema["properties"]
        assert "actions" in schema["properties"]
        assert set(schema["required"]) == {"decision_summary", "actions"}

    def test_schema_action_type_enum(self):
        # action.type 的枚举取值必须与 Lead 协议约定的动作集合一致
        from llm_service.api.lead import LEAD_DECISION_SCHEMA
        action_schema = LEAD_DECISION_SCHEMA["format"]["schema"]["properties"]["actions"]["items"]
        action_types = action_schema["properties"]["type"]["enum"]
        expected = [
            "interim_reply", "spawn_agent", "assign_task", "send_message",
            "broadcast", "revise_plan", "file_read", "shutdown_agent",
            "noop", "done", "reply", "synthesize",
        ]
        assert set(action_types) == set(expected)

    def test_schema_all_objects_have_additional_properties_false(self):
        """Anthropic requires additionalProperties: false on all objects.

        Anthropic 要求所有 object 都显式声明 additionalProperties: false。
        """
        from llm_service.api.lead import LEAD_DECISION_SCHEMA

        def check_object(obj, path="root"):
            # 递归遍历 schema：dict 看 type=object，list 逐项下钻，path 用于报错定位
            if isinstance(obj, dict):
                if obj.get("type") == "object":
                    assert obj.get("additionalProperties") is False, \
                        f"Object at {path} missing additionalProperties: false"
                for k, v in obj.items():
                    check_object(v, f"{path}.{k}")
            elif isinstance(obj, list):
                for i, v in enumerate(obj):
                    check_object(v, f"{path}[{i}]")

        check_object(LEAD_DECISION_SCHEMA)
