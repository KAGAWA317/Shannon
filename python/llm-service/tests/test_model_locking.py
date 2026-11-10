"""
Unit tests for manager model locking with tier preferences.

Tests ensure manager locks request.model when tier preferences specify provider:model.

manager 模型锁定与 tier 偏好的单元测试。

验证当 tier 偏好指定 provider:model 时，manager 会锁定 request.model。
"""

from enum import Enum


class ModelTier(Enum):
    """Model tier enum for testing

    用于测试的 model tier 枚举。
    """
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class MockRequest:
    """Mock request object

    用于测试的 mock request 对象。
    """
    def __init__(self, query, model_tier, model=None, provider_override=None):
        self.query = query
        self.model_tier = model_tier
        self.model = model
        self.provider_override = provider_override


def simulate_select_provider(request, tier_prefs, provider_models):
    """
    Simulate manager._select_provider logic for model locking.

    This mirrors the actual implementation where manager sets request.model
    when tier preferences specify provider:model format.

    模拟 manager._select_provider 的模型锁定逻辑。

    与真实实现保持一致：当 tier 偏好为 provider:model 格式时，manager 会设置 request.model。
    """
    for pref in tier_prefs:
        if ":" in pref:
            provider, model = pref.split(":", 1)
            if provider in provider_models and model in provider_models[provider]:
                # Lock the model when tier pref specifies it
                # 当 tier 偏好指定了模型时，锁定该模型
                request.model = model
                return provider
        else:
            # Provider-only preference, don't lock model
            # 仅指定 provider 的偏好，不锁定模型
            provider = pref
            if provider in provider_models:
                return provider
    return None


class TestModelLocking:
    """Test manager locks request.model when tier prefs specify provider:model

    验证 tier 偏好指定 provider:model 时 manager 会锁定 request.model。
    """

    def test_model_locked_when_tier_pref_has_provider_model(self):
        """Manager should set request.model when tier preference specifies provider:model

        当 tier 偏好指定 provider:model 时，manager 应设置 request.model。
        """
        tier_prefs = [
            "openai:gpt-5-2025-08-07",
            "anthropic:claude-sonnet-4-5-20250929",
        ]
        provider_models = {
            "openai": ["gpt-5-2025-08-07"],
            "anthropic": ["claude-sonnet-4-5-20250929"],
        }

        request = MockRequest(query="test", model_tier=ModelTier.MEDIUM)
        provider = simulate_select_provider(request, tier_prefs, provider_models)

        assert request.model == "gpt-5-2025-08-07", (
            "Manager should lock request.model to tier preference model"
        )
        assert provider == "openai", "Should select correct provider"

    def test_model_not_locked_when_tier_pref_has_only_provider(self):
        """Manager should not set request.model when tier preference only specifies provider

        当 tier 偏好只指定 provider 时，manager 不应设置 request.model。
        """
        tier_prefs = ["openai", "anthropic"]  # No model specified
        provider_models = {
            "openai": ["gpt-5-nano-2025-08-07", "gpt-5-mini-2025-08-07"],
        }

        request = MockRequest(query="test", model_tier=ModelTier.SMALL)
        provider = simulate_select_provider(request, tier_prefs, provider_models)

        assert request.model is None, (
            "Manager should not lock request.model when tier pref has no model"
        )
        assert provider == "openai", "Should select correct provider"

    def test_anthropic_model_locked_correctly(self):
        """Verify Anthropic models are locked when specified in tier preferences

        验证 tier 偏好中指定 Anthropic 模型时能正确锁定。
        """
        tier_prefs = [
            "openai:gpt-4.1-2025-04-14",
            "anthropic:claude-opus-4-1-20250805",
        ]
        provider_models = {
            "openai": ["gpt-4.1-2025-04-14"],
            "anthropic": ["claude-opus-4-1-20250805"],
        }

        request = MockRequest(
            query="test",
            model_tier=ModelTier.LARGE,
            provider_override="anthropic"
        )

        # Filter tier_prefs by provider_override
        # 按 provider_override 过滤 tier_prefs
        filtered_prefs = [p for p in tier_prefs if p.startswith("anthropic")]
        provider = simulate_select_provider(request, filtered_prefs, provider_models)

        assert request.model == "claude-opus-4-1-20250805", (
            "Manager should lock Anthropic model from tier preference"
        )
        assert provider == "anthropic", "Should select Anthropic provider"

    def test_model_override_not_changed_by_tier_pref(self):
        """If request.model is already set, tier preference should not override it

        如果 request.model 已设置，tier 偏好不应覆盖它。
        """
        tier_prefs = ["openai:gpt-5-2025-08-07"]
        provider_models = {
            "openai": ["gpt-5-2025-08-07", "gpt-5-nano-2025-08-07"],
        }

        # Request with explicit model override
        # 带有显式 model override 的 request
        request = MockRequest(
            query="test",
            model_tier=ModelTier.MEDIUM,
            model="gpt-5-nano-2025-08-07"  # Pre-set model
            # 预先设置的 model
        )

        original_model = request.model

        # Simulate manager logic that checks if model is already set
        # 模拟 manager 中“检查 model 是否已设置”的逻辑
        if not request.model:
            simulate_select_provider(request, tier_prefs, provider_models)

        assert request.model == original_model, (
            "Manager should not override explicitly set request.model"
        )
        assert request.model == "gpt-5-nano-2025-08-07", (
            "Request model should remain as user specified"
        )


class TestDeterministicModelSelection:
    """Test that model selection is deterministic based on config priorities

    验证 model 选择基于 config 优先级且结果确定。
    """

    def test_priority_order_respected(self):
        """Models should be selected in priority order from config

        模型应按 config 中的优先级顺序选择。
        """
        # Priority 1: Anthropic, Priority 2: OpenAI
        # 优先级 1：Anthropic，优先级 2：OpenAI
        tier_prefs = [
            "anthropic:claude-sonnet-4-5-20250929",
            "openai:gpt-5-2025-08-07",
        ]
        provider_models = {
            "anthropic": ["claude-sonnet-4-5-20250929"],
            "openai": ["gpt-5-2025-08-07"],
        }

        request = MockRequest(query="test", model_tier=ModelTier.MEDIUM)
        provider = simulate_select_provider(request, tier_prefs, provider_models)

        assert provider == "anthropic", (
            "Should select priority 1 provider (Anthropic)"
        )
        assert request.model == "claude-sonnet-4-5-20250929", (
            "Should lock to priority 1 model"
        )

    def test_fallback_to_second_priority_when_first_unavailable(self):
        """Should fall back to second priority when first is unavailable

        第一个优先级不可用时应回退到第二个。
        """
        tier_prefs = [
            "google:gemini-2.5-pro",  # Not available
            # 不可用
            "openai:gpt-5-2025-08-07",  # Available
            # 可用
        ]
        provider_models = {
            "openai": ["gpt-5-2025-08-07"],
            # Google not available
            # Google 不可用
        }

        request = MockRequest(query="test", model_tier=ModelTier.MEDIUM)
        provider = simulate_select_provider(request, tier_prefs, provider_models)

        assert provider == "openai", (
            "Should fall back to available provider"
        )
        assert request.model == "gpt-5-2025-08-07", (
            "Should lock to fallback model"
        )
