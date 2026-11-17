"""
Unified LLM Manager
Orchestrates multiple providers with caching, routing, and token management

统一的 LLM Manager
编排多个 provider，负责 caching、routing 与 token 管理
"""

import os
import random
import asyncio
import time
import json
import yaml
from dataclasses import replace
from typing import Dict, List, Any, Optional
from datetime import datetime
import logging

# Optional instrumentation (prometheus)
# 可选埋点（prometheus）
try:
    from prometheus_client import Counter, Histogram

    _METRICS_ENABLED = True
    LLM_MANAGER_REQUESTS = Counter(
        "llm_manager_requests_total",
        "Total LLM manager requests",
        labelnames=("provider", "model", "status"),
    )
    LLM_MANAGER_TOKENS = Counter(
        "llm_manager_tokens_total",
        "Total tokens processed by manager",
        labelnames=("provider", "model", "direction"),
    )
    LLM_MANAGER_COST = Counter(
        "llm_manager_cost_usd_total",
        "Accumulated cost tracked by manager (USD)",
        labelnames=("provider", "model"),
    )
    LLM_MANAGER_LATENCY = Histogram(
        "llm_manager_latency_ms",
        "LLM manager request latency (ms)",
        buckets=(100, 200, 500, 1000, 3000, 10000, 30000),
        labelnames=("provider", "model"),
    )
    LLM_MANAGER_CB_OPEN_TOTAL = Counter(
        "llm_manager_circuit_breaker_open_total",
        "Circuit breaker opened",
        labelnames=("provider",),
    )
    LLM_MANAGER_CB_CLOSE_TOTAL = Counter(
        "llm_manager_circuit_breaker_close_total",
        "Circuit breaker closed",
        labelnames=("provider",),
    )
    LLM_MANAGER_CB_PROBES_TOTAL = Counter(
        "llm_manager_circuit_breaker_half_open_probes_total",
        "Half-open probe attempts",
        labelnames=("provider",),
    )
    LLM_MANAGER_HEDGED_WINS = Counter(
        "llm_manager_hedged_wins_total",
        "Hedged request winner",
        labelnames=("winner",),  # primary|fallback 注：取值 primary|fallback
    )
except Exception:
    _METRICS_ENABLED = False

# Optional redis cache
# 可选的 redis cache
try:
    import redis  # type: ignore

    _REDIS_AVAILABLE = True
except Exception:
    _REDIS_AVAILABLE = False

from .base import (
    LLMProvider,
    LLMProviderRegistry,
    CompletionRequest,
    CompletionResponse,
    ModelTier,
    CacheManager,
    RateLimiter,
    TokenUsage,
)
# Provider implementations (optional at import time to ease testing without deps)
# provider 实现（导入时可选，便于在缺少依赖时测试）
try:
    from .openai_provider import OpenAIProvider
except Exception:  # pragma: no cover
    OpenAIProvider = None  # type: ignore
try:
    from .anthropic_provider import AnthropicProvider
except Exception:  # pragma: no cover
    AnthropicProvider = None  # type: ignore
try:
    from .openai_compatible import OpenAICompatibleProvider
except Exception:  # pragma: no cover
    OpenAICompatibleProvider = None  # type: ignore
try:
    from .google_provider import GoogleProvider
except Exception:  # pragma: no cover
    GoogleProvider = None  # type: ignore
try:
    from .groq_provider import GroqProvider
except Exception:  # pragma: no cover
    GroqProvider = None  # type: ignore
try:
    from .xai_provider import XAIProvider
except Exception:  # pragma: no cover
    XAIProvider = None  # type: ignore
try:
    from .minimax_provider import MiniMaxProvider
except Exception:  # pragma: no cover
    MiniMaxProvider = None  # type: ignore
try:
    from .litellm_provider import LiteLLMProvider
except Exception:  # pragma: no cover
    LiteLLMProvider = None  # type: ignore


class LLMManager:
    """
    Main LLM management class that handles:
    - Provider registration and routing
    - Model tiering and selection
    - Caching and rate limiting
    - Token budget enforcement
    - Usage tracking and reporting

    主要的 LLM 管理类，负责：
    - provider 注册与 routing
    - model 分层与选择
    - caching 与 rate limit
    - token budget 约束
    - 用量跟踪与报表
    """

    def __init__(self, config_path: Optional[str] = None):
        self.logger = logging.getLogger(__name__)
        self.registry = LLMProviderRegistry()
        self.cache = CacheManager(max_size=1000)
        self.rate_limiters: Dict[str, RateLimiter] = {}
        self._pricing_overrides: Optional[Dict[str, Any]] = None
        self._config_path: Optional[str] = None
        self._cache_cfg: Dict[str, Any] = {
            "enabled": True,
            "default_ttl": 3600,
            "max_size": 1000,
        }
        self._resilience_cfg: Dict[str, Any] = {
            "circuit_breakers": {
                "enabled": False,
                "failure_threshold": 5,
                "recovery_seconds": 60,
            },
            "hedged_requests": {"enabled": False, "delay_ms": 500},
        }
        self._breakers: Dict[str, "_CircuitBreaker"] = {}

        # Token budget tracking
        # token budget 跟踪
        self.session_usage: Dict[str, TokenUsage] = {}
        self.task_usage: Dict[str, TokenUsage] = {}

        # Load configuration
        # 加载配置
        if config_path:
            self.load_config(config_path)
        else:
            # Try unified config first (MODELS_CONFIG_PATH → /app/config/models.yaml → ./config/models.yaml)
            # 优先尝试统一配置（MODELS_CONFIG_PATH → /app/config/models.yaml → ./config/models.yaml）
            auto_paths = [
                os.getenv("MODELS_CONFIG_PATH", "").strip(),
                "/app/config/models.yaml",
                "./config/models.yaml",
            ]
            cfg_path = next((p for p in auto_paths if p and os.path.exists(p)), None)
            if cfg_path:
                self.load_config(cfg_path)
            else:
                self.load_default_config()
        # Apply centralized pricing overrides after providers are loaded
        # provider 加载后应用集中的 pricing 覆盖
        try:
            self._load_and_apply_pricing_overrides()
        except Exception as e:
            self.logger.warning(f"Pricing overrides not applied: {e}")

    # Backward compatibility aliases removed intentionally to keep logic simple
    # 为保持逻辑简单，有意移除了向后兼容的别名

    def load_config(self, config_path: str):
        """Load configuration from YAML file. Supports both unified and legacy formats.

        从 YAML 文件加载配置。同时支持统一格式与旧格式。
        """
        self._config_path = config_path
        with open(config_path, "r") as f:
            config = yaml.safe_load(f) or {}

        if "model_catalog" in config or "model_tiers" in config:
            # Unified config format (config/models.yaml)
            # 统一配置格式（config/models.yaml）
            providers_cfg, routing_cfg, caching_cfg = self._translate_unified_config(
                config
            )
            # Resilience configuration (optional)
            # resilience 配置（可选）
            try:
                self._resilience_cfg = dict(
                    config.get("resilience", {}) or self._resilience_cfg
                )
            except Exception:
                pass
            self._initialize_providers(providers_cfg)
            self._configure_routing(routing_cfg)
            self._configure_caching(caching_cfg)
        else:
            # Legacy format
            # 旧格式
            self._initialize_providers(config.get("providers", {}))
            self._configure_routing(config.get("routing", {}))
            self._configure_caching(config.get("caching", {}))

    def load_default_config(self):
        """Minimal fallback configuration - requires models.yaml for production use.
        
        This fallback only initializes providers from environment variables without
        model definitions. All model metadata, pricing, and routing preferences must
        come from config/models.yaml (see MODELS_CONFIG_PATH env var).

        最小兜底配置 —— 生产环境必须提供 models.yaml。

        该兜底仅从环境变量初始化 provider，不含 model 定义。所有 model metadata、
        pricing 与 routing 偏好都必须来自 config/models.yaml（见 MODELS_CONFIG_PATH
        环境变量）。
        """
        self.logger.warning(
            "Loading minimal fallback config - models.yaml not found. "
            "Set MODELS_CONFIG_PATH or place models.yaml in /app/config/ or ./config/"
        )
        
        config = {
            "providers": {},
            "routing": {
                "default_provider": "openai",
                "tier_preferences": {},  # Empty - requires models.yaml 注：为空，需要 models.yaml
            },
            "caching": {"enabled": True, "max_size": 1000, "default_ttl": 3600},
        }

        # Initialize providers from environment (model definitions come from models.yaml)
        # 从环境变量初始化 provider（model 定义来自 models.yaml）
        if os.getenv("OPENAI_API_KEY"):
            config["providers"]["openai"] = {
                "type": "openai",
                "api_key": os.getenv("OPENAI_API_KEY"),
            }

        if os.getenv("ANTHROPIC_API_KEY"):
            config["providers"]["anthropic"] = {
                "type": "anthropic",
                "api_key": os.getenv("ANTHROPIC_API_KEY"),
            }

        if os.getenv("GOOGLE_API_KEY"):
            config["providers"]["google"] = {
                "type": "google",
                "api_key": os.getenv("GOOGLE_API_KEY"),
            }

        if os.getenv("GROQ_API_KEY"):
            config["providers"]["groq"] = {
                "type": "groq",
                "api_key": os.getenv("GROQ_API_KEY"),
            }

        if os.getenv("XAI_API_KEY"):
            config["providers"]["xai"] = {
                "type": "xai",
                "api_key": os.getenv("XAI_API_KEY"),
            }

        # OpenAI-compatible providers (require base_url + models from models.yaml)
        # OpenAI 兼容 provider（需要 base_url，以及来自 models.yaml 的 model 定义）
        for name, env_key, base_url in [
            ("deepseek", "DEEPSEEK_API_KEY", "https://api.deepseek.com"),
            ("qwen", "QWEN_API_KEY", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
            ("zai", "ZAI_API_KEY", "https://api.z.ai/api/paas/v4"),
            ("kimi", "KIMI_API_KEY", "https://api.moonshot.ai/v1"),
            ("ollama", "OLLAMA_API_KEY", "http://host.docker.internal:11434/v1"),
        ]:
            if os.getenv(env_key):
                config["providers"][name] = {
                    "type": "openai_compatible",
                    "api_key": os.getenv(env_key),
                    "base_url": base_url,
                }

        if os.getenv("MINIMAX_API_KEY"):
            config["providers"]["minimax"] = {
                "type": "minimax",
                "api_key": os.getenv("MINIMAX_API_KEY"),
                "base_url": "https://api.minimax.io/v1",
            }

        self._initialize_providers(config["providers"])
        self._configure_routing(config["routing"])
        self._configure_caching(config["caching"])

    def _initialize_providers(self, providers_config: Dict):
        """Initialize all configured providers

        初始化所有已配置的 provider。
        """
        # Reset registry and limiters when re-initializing
        # 重新初始化时重置 registry 与 limiter
        self.registry = LLMProviderRegistry()
        self.rate_limiters = {}
        self._breakers = {}
        for name, config in providers_config.items():
            config = dict(config or {})
            config.setdefault("name", name)
            provider_type = config.get("type")

            try:
                if provider_type == "openai":
                    if OpenAIProvider is None:
                        self.logger.warning("OpenAI provider unavailable (missing dependency)")
                        continue
                    provider = OpenAIProvider(config)
                elif provider_type == "anthropic":
                    if AnthropicProvider is None:
                        self.logger.warning("Anthropic provider unavailable (missing dependency)")
                        continue
                    provider = AnthropicProvider(config)
                elif provider_type == "openai_compatible":
                    if OpenAICompatibleProvider is None:
                        self.logger.warning("OpenAI-compatible provider unavailable (missing dependency)")
                        continue
                    provider = OpenAICompatibleProvider(config)
                elif provider_type == "google":
                    if GoogleProvider is None:
                        self.logger.warning("Google provider unavailable (missing dependency)")
                        continue
                    provider = GoogleProvider(config)
                elif provider_type == "groq":
                    if GroqProvider is None:
                        self.logger.warning("Groq provider unavailable (missing dependency)")
                        continue
                    provider = GroqProvider(config)
                elif provider_type == "xai":
                    if XAIProvider is None:
                        self.logger.warning("XAI provider unavailable (missing dependency)")
                        continue
                    provider = XAIProvider(config)
                elif provider_type == "minimax":
                    if MiniMaxProvider is None:
                        self.logger.warning("MiniMax provider unavailable (missing dependency)")
                        continue
                    provider = MiniMaxProvider(config)
                elif provider_type == "litellm":
                    if LiteLLMProvider is None:
                        self.logger.warning(
                            "LiteLLM provider unavailable — install via "
                            '`pip install "litellm>=1.60,<1.85"`'
                        )
                        continue
                    provider = LiteLLMProvider(config)
                else:
                    self.logger.warning(f"Unknown provider type: {provider_type}")
                    continue

                self.registry.register_provider(
                    name, provider, is_default=(name == config.get("default"))
                )

                # Initialize rate limiter for provider (0 = disabled, rely on gateway layer)
                # 为 provider 初始化 rate limiter（0 = 禁用，交由 gateway 层处理）
                rpm = int(config.get("requests_per_minute", 0) or 0)
                max_conc = int(config.get("max_concurrent", 0) or 0)
                if rpm > 0 or max_conc > 0:
                    self.rate_limiters[name] = RateLimiter(rpm, max_concurrent=max_conc)

                self.logger.info(f"Initialized provider: {name}")
                # Initialize circuit breaker for provider
                # 为 provider 初始化 circuit breaker
                cb_cfg = (
                    (self._resilience_cfg.get("circuit_breakers") or {})
                    if hasattr(self, "_resilience_cfg")
                    else {}
                )
                self._breakers[name] = _CircuitBreaker(
                    name,
                    failure_threshold=int(cb_cfg.get("failure_threshold", 5) or 5),
                    recovery_timeout=float(cb_cfg.get("recovery_seconds", 60) or 60.0),
                    metrics_enabled=_METRICS_ENABLED,
                )
                # If pricing overrides already loaded, apply immediately
                # 若 pricing 覆盖已加载，立即应用
                try:
                    self._apply_pricing_overrides_for_provider(name, provider)
                except Exception as e:
                    self.logger.warning(
                        f"Failed to apply pricing overrides for {name}: {e}"
                    )

            except Exception as e:
                self.logger.error(f"Failed to initialize provider {name}: {e}")

    def _translate_unified_config(
        self, cfg: Dict[str, Any]
    ) -> tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
        """Translate unified config (model_catalog/model_tiers/selection_strategy) to internal structures.

        将统一配置（model_catalog/model_tiers/selection_strategy）转换为内部结构。
        """
        model_catalog = cfg.get("model_catalog", {}) or {}
        provider_settings = cfg.get("provider_settings", {}) or {}
        model_tiers = cfg.get("model_tiers", {}) or {}
        selection = cfg.get("selection_strategy", {}) or {}
        prompt_cache = cfg.get("prompt_cache", {}) or {}
        rate_limits = cfg.get("rate_limits", {}) or {}
        capabilities_cfg = cfg.get("model_capabilities", {}) or {}

        # Provider type + env var mapping
        # provider 类型与环境变量映射
        type_map = {
            "openai": ("openai", "OPENAI_API_KEY"),
            "anthropic": ("anthropic", "ANTHROPIC_API_KEY"),
            "google": ("google", "GOOGLE_API_KEY"),
            "groq": ("groq", "GROQ_API_KEY"),
            "xai": ("xai", "XAI_API_KEY"),
            "zai": ("openai_compatible", "ZAI_API_KEY"),
            "kimi": ("openai_compatible", "KIMI_API_KEY"),
            "minimax": ("minimax", "MINIMAX_API_KEY"),
            # OpenAI-compatible providers we support
            # 我们支持的 OpenAI 兼容 provider
            "deepseek": ("openai_compatible", "DEEPSEEK_API_KEY"),
            "qwen": ("openai_compatible", "QWEN_API_KEY"),
            "ollama": ("openai_compatible", "OLLAMA_API_KEY"),
            # Others exist in config but not yet implemented here: meta/bedrock
            # 配置中还有其他项但此处尚未实现：meta/bedrock
        }

        providers_cfg: Dict[str, Any] = {}
        for prov_name, models in model_catalog.items():
            if prov_name not in type_map:
                # Skip providers without a concrete implementation in this service
                # 跳过本服务中没有具体实现的 provider
                continue
            ptype, env_key = type_map[prov_name]
            p_cfg: Dict[str, Any] = {"type": ptype, "models": {}}

            # API key from env if present
            # 若环境变量中存在 API key 则使用
            api_key = os.getenv(env_key)
            if api_key:
                p_cfg["api_key"] = api_key

            provider_cfg = provider_settings.get(prov_name, {}) or {}
            p_cfg.setdefault("name", prov_name)

            # Base URL + timeouts for HTTP providers we instantiate locally
            # 本地实例化的 HTTP provider 的 base URL 与 timeout
            if ptype in ("openai_compatible", "xai", "anthropic", "minimax"):
                base_url = provider_cfg.get("base_url")
                if base_url:
                    p_cfg["base_url"] = base_url

            if provider_cfg.get("timeout") is not None:
                p_cfg.setdefault("timeout", provider_cfg.get("timeout"))
            if provider_cfg.get("max_retries") is not None:
                p_cfg.setdefault("max_retries", provider_cfg.get("max_retries"))

            # Copy over model metadata (honor enabled flag)
            # 复制 model metadata（遵守 enabled 标志）
            for alias, meta in (models or {}).items():
                meta = dict(meta or {})
                if str(meta.get("enabled", "true")).lower() == "false":
                    continue
                # Augment with capabilities from top-level capabilities lists
                # 用顶层 capabilities 列表补充能力标记
                try:
                    if alias in (capabilities_cfg.get("multimodal_models", []) or []):
                        meta["supports_vision"] = True
                    if alias in (capabilities_cfg.get("thinking_models", []) or []):
                        meta["supports_reasoning"] = True
                    # JSON mode support defaults per provider type
                    # 各 provider 类型的 JSON mode 支持默认值
                    if ptype in (
                        "openai",
                        "openai_compatible",
                        "groq",
                        "google",
                        "xai",
                    ):
                        meta.setdefault("supports_json_mode", True)
                    else:
                        meta.setdefault("supports_json_mode", False)
                    # Default max parallel tools
                    # 默认的最大并行 tool 数
                    meta.setdefault("max_parallel_tools", 1)
                except Exception:
                    pass
                p_cfg["models"][alias] = meta

            providers_cfg[prov_name] = p_cfg

        # Build routing preferences from model_tiers (ordered by priority)
        # 依据 model_tiers 构建 routing 偏好（按优先级排序）
        tier_prefs: Dict[str, List[str]] = {}
        for tier_name, tier_cfg in model_tiers.items():
            items = tier_cfg.get("providers", []) or []
            # Sort by 'priority' (lower is higher priority); if absent, keep order
            # 按 'priority' 排序（数值越小优先级越高）；缺失时保持原顺序
            try:
                items = sorted(items, key=lambda x: int(x.get("priority", 9999)))
            except Exception:
                pass
            tier_prefs[tier_name] = [
                f"{it.get('provider')}:{it.get('model')}"
                for it in items
                if it.get("provider") and it.get("model")
            ]

        routing_cfg = {
            "default_provider": selection.get("default_provider", "openai"),
            "tier_preferences": tier_prefs,
        }

        caching_cfg = {
            "enabled": bool(prompt_cache.get("enabled", True)),
            "default_ttl": int(prompt_cache.get("ttl_seconds", 3600) or 3600),
            # Keep default size; unified file tracks size in MB for a different cache
            # 保持默认大小；统一文件里的 size（MB）针对的是另一个 cache
            "max_size": 1000,
        }

        # Rate limits (optional): apply per-provider RPM only if explicitly configured.
        # default_rpm=0 means disabled — rely on gateway layer for user-facing rate limits.
        # rate limit（可选）：仅在显式配置时应用各 provider 的 RPM。
        # default_rpm=0 表示禁用 —— 面向用户的 rate limit 交由 gateway 层处理。
        default_rpm = int(rate_limits.get("default_rpm", 0) or 0)
        if default_rpm > 0:
            for name, pcfg in providers_cfg.items():
                pcfg.setdefault("requests_per_minute", default_rpm)

        return providers_cfg, routing_cfg, caching_cfg

    def _configure_routing(self, routing_config: Dict):
        """Configure routing preferences

        配置 routing 偏好。
        """
        self.routing_config = routing_config
        self.tier_preferences = routing_config.get("tier_preferences", {})

    def _configure_caching(self, caching_config: Dict):
        """Configure caching settings

        配置 caching 设置。
        """
        self._cache_cfg = dict(caching_config or {})
        if not self._cache_cfg.get("enabled", True):
            self.cache = None
            self.default_cache_ttl = int(self._cache_cfg.get("default_ttl", 3600))
            return

        # Prefer Redis cache if REDIS_URL (or REDIS_HOST/PORT) is present and library available
        # 若存在 REDIS_URL（或 REDIS_HOST/PORT）且库可用，优先使用 Redis cache
        redis_url = os.getenv("REDIS_URL") or os.getenv("LLM_REDIS_URL")
        if not redis_url and os.getenv("REDIS_HOST"):
            host = os.getenv("REDIS_HOST")
            port = os.getenv("REDIS_PORT", "6379")
            pwd = os.getenv("REDIS_PASSWORD")
            if pwd:
                redis_url = f"redis://:{pwd}@{host}:{port}"
            else:
                redis_url = f"redis://{host}:{port}"

        if redis_url and _REDIS_AVAILABLE:
            try:
                self.cache = _RedisCacheManager(redis_url)
                self.logger.info("Using Redis cache backend for LLM responses")
            except Exception as e:
                self.logger.warning(
                    f"Redis cache unavailable, falling back to memory: {e}"
                )
                self.cache = CacheManager(
                    max_size=self._cache_cfg.get("max_size", 1000)
                )
        else:
            max_size = int(self._cache_cfg.get("max_size", 1000))
            self.cache = CacheManager(max_size=max_size)

        self.default_cache_ttl = int(self._cache_cfg.get("default_ttl", 3600))

    def _load_and_apply_pricing_overrides(self):
        """Load pricing overrides from /app/config/models.yaml and apply to providers.

        从 /app/config/models.yaml 加载 pricing 覆盖并应用到 provider。
        """
        config_path = os.getenv("MODELS_CONFIG_PATH", "/app/config/models.yaml")
        if not os.path.exists(config_path):
            return
        with open(config_path, "r") as f:
            cfg = yaml.safe_load(f) or {}
        pricing = cfg.get("pricing") or {}
        models = pricing.get("models") or {}
        if not models:
            return
        self._pricing_overrides = models
        for name, provider in self.registry.providers.items():
            self._apply_pricing_overrides_for_provider(name, provider)

    def _apply_pricing_overrides_for_provider(
        self, provider_name: str, provider: LLMProvider
    ):
        if not self._pricing_overrides:
            return
        prov_map = self._pricing_overrides.get(provider_name)
        if not prov_map:
            return
        # Update known models' pricing if present
        # 若存在则更新已知 model 的 pricing
        for key, model_cfg in provider.models.items():
            override = prov_map.get(model_cfg.model_id) or prov_map.get(key)
            if not override:
                continue
            ip = override.get("input_per_1k")
            op = override.get("output_per_1k")
            if isinstance(ip, (int, float)):
                model_cfg.input_price_per_1k = float(ip)
            if isinstance(op, (int, float)):
                model_cfg.output_price_per_1k = float(op)

    async def complete(
        self,
        messages: List[Dict[str, Any]],
        model_tier: ModelTier = ModelTier.SMALL,
        **kwargs,
    ) -> CompletionResponse:
        """
        Main completion method with automatic provider selection,
        caching, and rate limiting

        主 completion 方法，自动完成 provider 选择、caching 与 rate limit。
        """

        # Create request object
        # 构造 request 对象
        request = CompletionRequest(messages=messages, model_tier=model_tier, **kwargs)

        # No model aliasing: expect callers to provide canonical model IDs
        # 不做 model 别名转换：调用方应提供规范化的 model ID

        # Check cache if enabled
        # 若启用则检查 cache
        cache_key = None
        if self.cache and not request.stream:
            cache_key = request.generate_cache_key()
            cached_response = self.cache.get(cache_key)
            if cached_response:
                # Guard against cache poisoning when strict JSON is expected
                # 期望严格 JSON 时防止 cache 被污染
                if _is_strict_json_mode(request):
                    try:
                        parsed = json.loads(cached_response.content or "")
                        if not isinstance(parsed, dict):
                            raise ValueError("cached JSON is not an object")
                    except Exception:
                        try:
                            if hasattr(self.cache, "delete"):
                                self.cache.delete(cache_key)  # type: ignore[attr-defined]
                        except Exception:
                            pass
                        cached_response = None
                # Avoid serving cached truncated/filtered outputs
                # 避免返回被截断/被过滤的 cache 输出
                if cached_response and (getattr(cached_response, "finish_reason", "") or "").lower() in {"length", "content_filter"}:
                    try:
                        if hasattr(self.cache, "delete"):
                            self.cache.delete(cache_key)  # type: ignore[attr-defined]
                    except Exception:
                        pass
                    cached_response = None

                if cached_response:
                    hit_rate = getattr(self.cache, "hit_rate", None)
                    if isinstance(hit_rate, float):
                        self.logger.info(
                            f"Cache hit for request (hit rate: {hit_rate:.2%})"
                        )
                    else:
                        self.logger.info("Cache hit for request")
                    return cached_response

        # Select provider based on request
        # 依据 request 选择 provider
        provider_name, provider = self._select_provider(request)

        # Track token budget if session/task specified
        # 指定了 session/task 时跟踪 token budget
        if request.session_id:
            await self._check_session_budget(request)

        # Make the actual API call (supports hedging when enabled)
        # 发起实际 API 调用（启用时支持 hedging）
        hedge_cfg = self._resilience_cfg.get("hedged_requests") or {}
        allow_hedge = bool(
            hedge_cfg.get("enabled", False)
        ) and self._is_hedge_candidate(request)
        try:
            if allow_hedge:
                fb = self._get_fallback_provider(provider_name, request.model_tier)
                if fb:
                    delay_ms = int(hedge_cfg.get("delay_ms", 500) or 500)
                    response, winner = await self._hedged_complete(
                        request, (provider_name, provider), fb, delay_ms
                    )
                    if _METRICS_ENABLED:
                        LLM_MANAGER_HEDGED_WINS.labels(
                            "primary" if winner == provider_name else "fallback"
                        ).inc()
                else:
                    response = await self._call_provider_with_cb(
                        provider_name, provider, request
                    )
            else:
                response = await self._call_provider_with_cb(
                    provider_name, provider, request
                )

            # Update usage tracking
            # 更新用量跟踪
            self._update_usage_tracking(request, response)

            # Cache the response if applicable and safe
            # 适用且安全时 cache 该 response
            if self.cache and not request.stream:
                try:
                    cache_ttl = request.cache_ttl or self.default_cache_ttl
                    if self._should_cache_response(request, response):
                        ck = cache_key or request.generate_cache_key()
                        self.cache.set(ck, response, cache_ttl)
                    else:
                        self.logger.info("Skip caching response (unsafe/invalid for cache)")
                except Exception:
                    # Never fail due to cache issues
                    # 绝不因 cache 问题导致失败
                    self.logger.debug("Cache set failed; continuing without cache", exc_info=True)

            # Instrumentation
            # 埋点上报
            if _METRICS_ENABLED:
                LLM_MANAGER_REQUESTS.labels(
                    response.provider, response.model, "ok"
                ).inc()
                LLM_MANAGER_TOKENS.labels(
                    response.provider, response.model, "prompt"
                ).inc(response.usage.input_tokens)
                LLM_MANAGER_TOKENS.labels(
                    response.provider, response.model, "completion"
                ).inc(response.usage.output_tokens)
                LLM_MANAGER_COST.labels(response.provider, response.model).inc(
                    max(0.0, float(response.usage.estimated_cost))
                )
                if response.latency_ms is not None:
                    LLM_MANAGER_LATENCY.labels(
                        response.provider, response.model
                    ).observe(max(0, int(response.latency_ms)))

            return response

        except Exception as e:
            self.logger.error(f"Provider {provider_name} failed: {e}", exc_info=True)
            if _METRICS_ENABLED:
                try:
                    LLM_MANAGER_REQUESTS.labels(provider_name, "", "error").inc()
                except Exception:
                    pass

            # If provider_override was explicitly set, do NOT fallback — let caller handle.
            # 若显式设置了 provider_override，则不要 fallback —— 交由调用方处理。
            po = getattr(request, "provider_override", None)
            if isinstance(po, str) and po.strip():
                raise

            # Try fallback provider if available
            # 若有可用的 fallback provider 则尝试
            fallback = self._get_fallback_provider(provider_name, request.model_tier)
            if fallback:
                self.logger.info(f"Trying fallback provider: {fallback[0]}")
                # Clear model selection so fallback provider can choose its own compatible model
                # 清空 model 选择，让 fallback provider 自行选择兼容的 model
                original_model = request.model
                request.model = None
                try:
                    return await self._call_provider_with_cb(
                        fallback[0], fallback[1], request
                    )
                except Exception:
                    # Restore original model before re-raising
                    # 重新抛出前恢复原始 model
                    request.model = original_model
                    raise

            raise

    async def stream_complete(
        self,
        messages: List[Dict[str, Any]],
        model_tier: ModelTier = ModelTier.SMALL,
        **kwargs,
    ):
        """Unified streaming API: yields text chunks from the selected provider.

        Backward compatible: this method is additive and does not alter existing
        response formats, protobuf contracts, or event emission.

        统一的 streaming API：从选中的 provider 产出文本 chunk。

        向后兼容：该方法为增量新增，不改变既有 response 格式、protobuf 契约或 event 发射。
        """
        # Build request (mark stream=True for clarity to providers)
        # 构造 request（stream=True，向 provider 明确表示流式）
        request = CompletionRequest(
            messages=messages, model_tier=model_tier, stream=True, **kwargs
        )

        # If a cached response exists and caller requested streaming, emit as a single chunk
        # 若存在 cache 的 response 且调用方要求 streaming，则作为单个 chunk 输出
        cache_key = None
        if self.cache:
            try:
                cache_key = request.generate_cache_key()
                cached = self.cache.get(cache_key)
                if cached and cached.content:
                    yield cached.content
                    return
            except Exception:
                pass

        # Select provider and apply rate limiting
        # 选择 provider 并应用 rate limit
        provider_name, provider = self._select_provider(request)
        if provider_name in self.rate_limiters:
            await self.rate_limiters[provider_name].acquire()

        # Stream from provider
        # 从 provider 流式获取
        try:
            req = self._normalize_function_call_for_provider(provider_name, request)
            async for chunk in provider.stream_complete(req):
                # Pass through strings and dicts (for usage metadata)
                # 原样传出字符串与 dict（用于 usage metadata）
                if isinstance(chunk, (str, dict)) and chunk:
                    yield chunk
        except Exception as e:
            self.logger.error(f"Provider {provider_name} streaming failed: {e}")
            # If provider_override was explicitly set, do NOT fallback
            # 若显式设置了 provider_override，则不要 fallback
            po = getattr(request, "provider_override", None)
            if isinstance(po, str) and po.strip():
                raise
            # Attempt fallback provider for streaming if configured
            # 若已配置，则为 streaming 尝试 fallback provider
            fallback = self._get_fallback_provider(provider_name, request.model_tier)
            if fallback:
                # Clear model selection so fallback provider can choose its own compatible model
                # 清空 model 选择，让 fallback provider 自行选择兼容的 model
                original_model = request.model
                request.model = None
                try:
                    req = self._normalize_function_call_for_provider(fallback[0], request)
                    async for chunk in fallback[1].stream_complete(req):
                        # Pass through strings and dicts (for usage metadata)
                        # 原样传出字符串与 dict（用于 usage metadata）
                        if isinstance(chunk, (str, dict)) and chunk:
                            yield chunk
                except Exception:
                    # Restore original model before re-raising
                    # 重新抛出前恢复原始 model
                    request.model = original_model
                    raise
            else:
                raise

    def _select_provider(self, request: CompletionRequest) -> tuple[str, LLMProvider]:
        """Select the best provider for a request

        为 request 选择最合适的 provider。
        """
        # Provider override (explicit control)
        # provider 覆盖（显式控制）
        po = getattr(request, "provider_override", None)
        if isinstance(po, str) and po:
            provider_name = str(po).strip()
            if provider_name not in self.registry.providers:
                raise ValueError(f"Invalid provider_override: {provider_name}")
            if self._is_breaker_open(provider_name):
                raise RuntimeError(f"Provider '{provider_name}' circuit breaker is open")
            provider = self.registry.providers[provider_name]
            # If a specific model was requested, ensure this provider has it
            # 若请求了具体 model，确认该 provider 拥有它
            if request.model and request.model not in provider.models:
                raise ValueError(
                    f"Model '{request.model}' not available for provider '{provider_name}'"
                )
            return provider_name, provider

        # If specific model is explicitly set (e.g., via specific_model parameter),
        # find and use the provider that has this model, bypassing tier preferences
        # 若显式指定了具体 model（如通过 specific_model 参数），
        # 找到并直接使用拥有该 model 的 provider，绕过 tier 偏好
        if request.model:
            self.logger.debug(f"Specific model requested: {request.model}, searching for provider")
            for pname, pprovider in self.registry.providers.items():
                if self._is_breaker_open(pname):
                    continue
                if request.model in pprovider.models:
                    self.logger.info(f"Using provider '{pname}' for specific model '{request.model}'")
                    return pname, pprovider
            # If no provider has this model, log warning and fall through to tier-based selection
            # 若无 provider 拥有该 model，记录告警并转入基于 tier 的选择
            self.logger.warning(
                f"Specific model '{request.model}' not found in any available provider, "
                f"falling back to tier-based selection (tier={request.model_tier.value})"
            )
            request.model = None  # Clear it so tier selection can proceed 注：清空以便继续进行 tier 选择

        # Check tier preferences
        # 检查 tier 偏好
        tier_prefs = self.tier_preferences.get(request.model_tier.value, [])

        for pref in tier_prefs:
            if ":" in pref:
                provider_name, model_id = pref.split(":", 1)
                if provider_name in self.registry.providers:
                    if self._is_breaker_open(provider_name):
                        self.logger.warning(
                            f"Provider routing: skipping {provider_name}:{model_id} "
                            f"(circuit breaker OPEN, tier={request.model_tier.value}, "
                            f"agent_id={getattr(request, 'agent_id', 'N/A')})"
                        )
                        continue
                    provider = self.registry.providers[provider_name]
                    # Check if provider has the model; if so, lock request.model explicitly
                    # 检查 provider 是否拥有该 model；若有则显式锁定 request.model
                    if model_id in provider.models:
                        try:
                            # Set explicit model to ensure exact match downstream
                            # 设置显式 model，确保下游精确匹配
                            request.model = model_id
                        except Exception:
                            pass
                        return provider_name, provider
                    else:
                        self.logger.warning(
                            f"Provider routing: {provider_name} missing model {model_id} "
                            f"(tier={request.model_tier.value}, available={list(provider.models.keys())[:3]})"
                        )
            else:
                # Just provider name, use any model in tier
                # 只有 provider 名时，使用该 tier 中任意 model
                if pref in self.registry.providers and not self._is_breaker_open(pref):
                    return pref, self.registry.providers[pref]

        # Fall back to registry's selection
        # 回退到 registry 的选择
        provider_name, provider = self.registry.select_provider_for_request(request)
        if self._is_breaker_open(provider_name):
            fb = self._get_fallback_provider(provider_name, request.model_tier)
            if fb:
                return fb
        return provider_name, provider

    def _get_fallback_provider(
        self, failed_provider: str, tier: ModelTier
    ) -> Optional[tuple[str, LLMProvider]]:
        """Get a fallback provider if the primary fails

        主 provider 失败时获取一个 fallback provider。
        """

        tier_prefs = self.tier_preferences.get(tier.value, [])

        for pref in tier_prefs:
            provider_name = pref.split(":")[0] if ":" in pref else pref
            if (
                provider_name != failed_provider
                and provider_name in self.registry.providers
                and not self._is_breaker_open(provider_name)
            ):
                return provider_name, self.registry.providers[provider_name]

        return None

    def _is_breaker_open(self, provider_name: str) -> bool:
        cb = self._breakers.get(provider_name)
        if not cb:
            return False
        if cb.state != "open":
            return False
        return (time.time() - cb.opened_at) < cb.recovery_timeout

    def _normalize_function_call_for_provider(
        self, provider_name: str, request: CompletionRequest
    ) -> CompletionRequest:
        """Normalize internal tool forcing semantics to provider-compatible values.

        将内部“强制使用 tool”的语义归一化为各 provider 兼容的取值。
        """
        if not (isinstance(request.function_call, str) and request.function_call == "any"):
            return request

        # Anthropic supports tool_choice={"type":"any"} directly.
        # Anthropic 直接支持 tool_choice={"type":"any"}。
        if provider_name == "anthropic":
            return request

        tool_name = None
        for raw_fn in request.functions or []:
            fn = None
            if (
                isinstance(raw_fn, dict)
                and raw_fn.get("type") == "function"
                and isinstance(raw_fn.get("function"), dict)
            ):
                fn = raw_fn.get("function")
            elif isinstance(raw_fn, dict):
                fn = raw_fn
            if isinstance(fn, dict):
                name = fn.get("name")
                if isinstance(name, str) and name.strip():
                    tool_name = name.strip()
                    break

        if not tool_name:
            return replace(request, function_call=None)

        # OpenAI-compatible APIs don't support a direct "any" string; forcing one tool is
        # the closest equivalent that prevents "tool-optional" behavior.
        # OpenAI 兼容 API 不支持直接的 "any" 字符串；强制指定一个 tool 是最接近的等价做法，
        # 可避免 “tool 可选” 的行为。
        return replace(request, function_call={"name": tool_name})

    async def _call_provider_with_cb(
        self, provider_name: str, provider: LLMProvider, request: CompletionRequest
    ) -> CompletionResponse:
        # Circuit breaker gate
        # circuit breaker 闸门
        cb = self._breakers.get(provider_name)
        if cb and not cb.allow():
            raise RuntimeError(f"Circuit open for provider {provider_name}")

        # Apply rate limiting per provider
        # 按 provider 应用 rate limit
        if provider_name in self.rate_limiters:
            await self.rate_limiters[provider_name].acquire()

        # Validate and cap max_tokens to model's limit
        # 校验 max_tokens 并限制到 model 上限
        if request.model and request.model in provider.models:
            model_config = provider.models[request.model]
            if hasattr(request, 'max_tokens') and request.max_tokens:
                model_max = model_config.max_tokens
                if request.max_tokens > model_max:
                    self.logger.warning(
                        f"Request max_tokens ({request.max_tokens}) exceeds model limit ({model_max}) "
                        f"for {request.model}. Capping to {model_max}."
                    )
                    request.max_tokens = model_max

        # Call provider
        # 调用 provider
        try:
            req = self._normalize_function_call_for_provider(provider_name, request)
            resp = await provider.complete(req)
            if cb:
                cb.on_success()
            return resp
        except Exception as e:
            if cb:
                cb.on_failure(_is_transient_error(e))
            raise

    def _is_hedge_candidate(self, request: CompletionRequest) -> bool:
        # Non-streaming, no tools, not strict JSON
        # 非 streaming、无 tool、非严格 JSON
        if getattr(request, "stream", False):
            return False
        if getattr(request, "functions", None):
            return False
        rf = getattr(request, "response_format", None)
        if isinstance(rf, dict) and rf.get("type") == "json_object":
            return False
        return True

    def _should_cache_response(
        self, request: CompletionRequest, response: CompletionResponse
    ) -> bool:
        """Return True if response is safe to cache.

        - For strict JSON, only cache if content parses to a JSON object.
        - Do not cache when finish_reason indicates truncation or content filtering.
        - Require non-empty content unless a function_call is present.

        response 可以安全 cache 时返回 True。

        - 严格 JSON 场景下，仅当 content 能解析为 JSON 对象时才 cache。
        - finish_reason 表示截断或内容过滤时不要 cache。
        - 除非存在 function_call，否则要求 content 非空。
        """
        try:
            # Check finish_reason FIRST (most important guard)
            # 首先检查 finish_reason（最重要的保护）
            fr = (getattr(response, "finish_reason", "") or "").lower()
            if fr in {"length", "content_filter"}:
                self.logger.info(f"Skip caching: finish_reason={fr}")
                return False

            if _is_strict_json_mode(request):
                try:
                    obj = json.loads(response.content or "")
                    if not isinstance(obj, dict):
                        self.logger.info("Skip caching: JSON mode but content is not a dict")
                        return False
                except Exception as e:
                    self.logger.info(f"Skip caching: JSON mode but invalid JSON: {e}")
                    return False

            if not (isinstance(response.content, str) and response.content.strip()) and not response.function_call:
                self.logger.info("Skip caching: empty content and no function_call")
                return False

            return True
        except Exception as e:
            self.logger.warning(f"Cache guard exception: {e}")
            return False

    async def _hedged_complete(
        self,
        request: CompletionRequest,
        primary: tuple[str, LLMProvider],
        fallback: tuple[str, LLMProvider],
        delay_ms: int = 500,
    ) -> tuple[CompletionResponse, str]:
        p_name, p = primary
        f_name, f = fallback

        async def run_one(name: str, prov: LLMProvider):
            return await self._call_provider_with_cb(name, prov, request)

        async def delayed_run(name: str, prov: LLMProvider, delay: int):
            await asyncio.sleep(max(0, delay) / 1000)
            return await run_one(name, prov)

        t1 = asyncio.create_task(run_one(p_name, p))
        t2 = asyncio.create_task(delayed_run(f_name, f, delay_ms))

        done, pending = await asyncio.wait(
            {t1, t2}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        first = done.pop()
        try:
            resp = first.result()
            winner = p_name if first is t1 else f_name
            return resp, winner
        except Exception:
            # If first failed immediately, wait for the other
            # 若首选立即失败，则等待另一个
            if pending:
                rest_done, _ = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )
                r = rest_done.pop().result()
                winner = f_name if first is t1 else p_name
                return r, winner
            raise

    async def _check_session_budget(self, request: CompletionRequest):
        """Check and enforce session-level token budget

        检查并执行 session 级 token budget。
        """
        # Shannon architecture: budget enforcement is owned by the Go orchestrator.
        # Disable Python-side budget enforcement by default to avoid duplication/conflicts.
        # Override by setting LLM_DISABLE_BUDGETS=0 (or false) if you explicitly want
        # the LLM service to enforce its own per-session budget.
        # Shannon 架构：budget 的执行由 Go orchestrator 负责。
        # 默认关闭 Python 侧的 budget 执行，避免重复与冲突。
        # 若明确希望 LLM 服务执行自己的 per-session budget，可设置 LLM_DISABLE_BUDGETS=0（或 false）。
        try:
            if str(os.getenv("LLM_DISABLE_BUDGETS", "1")).lower() in (
                "1",
                "true",
                "yes",
            ):  # default disabled 注：默认禁用
                return
        except Exception:
            # If env parsing fails, fail open (do not enforce here)
            # 环境变量解析失败时 fail open（此处不执行约束）
            return

        if request.session_id not in self.session_usage:
            self.session_usage[request.session_id] = TokenUsage(0, 0, 0, 0.0)

        # Get current usage
        # 获取当前用量
        current_usage = self.session_usage[request.session_id]

        # Enforce token budget limits
        # 执行 token budget 限制
        max_tokens_per_session = 100000  # Default limit, should be configurable 注：默认限制，应可配置
        if hasattr(request, "max_tokens_budget"):
            max_tokens_per_session = request.max_tokens_budget

        if current_usage.total_tokens >= max_tokens_per_session:
            raise ValueError(
                f"Session {request.session_id} exceeded token budget: "
                f"{current_usage.total_tokens}/{max_tokens_per_session} tokens used"
            )

        self.logger.info(
            f"Session {request.session_id} usage: "
            f"{current_usage.total_tokens} tokens, "
            f"${current_usage.estimated_cost:.4f}"
        )

    def _update_usage_tracking(
        self, request: CompletionRequest, response: CompletionResponse
    ):
        """Update usage tracking for sessions and tasks

        更新 session 与 task 的用量跟踪。
        """

        # Update session usage
        # 更新 session 用量
        if request.session_id:
            if request.session_id not in self.session_usage:
                self.session_usage[request.session_id] = TokenUsage(0, 0, 0, 0.0)
            self.session_usage[request.session_id] += response.usage

        # Update task usage
        # 更新 task 用量
        if request.task_id:
            if request.task_id not in self.task_usage:
                self.task_usage[request.task_id] = TokenUsage(0, 0, 0, 0.0)
            self.task_usage[request.task_id] += response.usage

    def get_usage_report(
        self, session_id: Optional[str] = None, task_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get usage report for a session or task

        获取 session 或 task 的用量报表。
        """

        report = {
            "timestamp": datetime.utcnow().isoformat(),
            "cache_hit_rate": (
                getattr(self.cache, "hit_rate", 0.0) if self.cache else 0.0
            ),
        }

        if session_id and session_id in self.session_usage:
            usage = self.session_usage[session_id]
            report["session"] = {
                "id": session_id,
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "total_tokens": usage.total_tokens,
                "estimated_cost": usage.estimated_cost,
            }

        if task_id and task_id in self.task_usage:
            usage = self.task_usage[task_id]
            report["task"] = {
                "id": task_id,
                "input_tokens": usage.input_tokens,
                "output_tokens": usage.output_tokens,
                "total_tokens": usage.total_tokens,
                "estimated_cost": usage.estimated_cost,
            }

        return report

    def get_provider_status(self) -> Dict[str, Any]:
        """Get status of all registered providers

        获取所有已注册 provider 的状态。
        """

        status = {}
        for name, provider in self.registry.providers.items():
            status[name] = {
                "available": True,  # Could add health checks 注：后续可加入健康检查
                "models": list(provider.models.keys()),
                "rate_limit": {
                    "requests_per_minute": self.rate_limiters[name].requests_per_minute
                    if name in self.rate_limiters
                    else None
                },
            }

        return status

    async def reload(self) -> None:
        """Hot-reload configuration if a config path was provided or discovered.

        若提供了或已发现配置路径，则热重载配置。
        """
        try:
            if self._config_path and os.path.exists(self._config_path):
                self.load_config(self._config_path)
            else:
                # Fall back to auto-detection or env defaults
                # 回退到自动探测或环境变量默认值
                auto_paths = [
                    os.getenv("MODELS_CONFIG_PATH", "").strip(),
                    "/app/config/models.yaml",
                    "./config/models.yaml",
                ]
                cfg_path = next(
                    (p for p in auto_paths if p and os.path.exists(p)), None
                )
                if cfg_path:
                    self.load_config(cfg_path)
                else:
                    self.load_default_config()

            # Re-apply centralized pricing if available
            # 若可用则重新应用集中的 pricing
            try:
                self._load_and_apply_pricing_overrides()
            except Exception as e:
                self.logger.warning(f"Pricing overrides not applied on reload: {e}")
        except Exception as e:
            self.logger.error(f"Reload failed: {e}")

    async def generate_embedding(
        self, text: str, model: Optional[str] = None
    ) -> List[float]:
        """Generate embeddings via the first capable provider (prefers OpenAI).

        通过第一个具备能力的 provider 生成 embedding（优先 OpenAI）。
        """
        # Prefer OpenAI if available
        # 若可用则优先 OpenAI
        if "openai" in self.registry.providers:
            provider = self.registry.providers["openai"]
            gen = getattr(provider, "generate_embedding", None)
            if gen:
                return await gen(text, model or "text-embedding-3-small")

        # Fallback to any provider exposing generate_embedding
        # 回退到任何暴露 generate_embedding 的 provider
        for provider in self.registry.providers.values():
            gen = getattr(provider, "generate_embedding", None)
            if gen:
                return await gen(text, model)

        raise ValueError("No embedding-capable providers are configured")


# Singleton instance
# 单例实例
_manager_instance: Optional[LLMManager] = None


def get_llm_manager(config_path: Optional[str] = None) -> LLMManager:
    """Get or create the singleton LLM manager instance

    获取或创建单例 LLM manager 实例。
    """
    global _manager_instance

    if _manager_instance is None:
        _manager_instance = LLMManager(config_path)

    return _manager_instance


# --- Redis cache backend (optional) ---
# --- Redis cache 后端（可选）---
class _RedisCacheManager:
    """Redis-backed cache storing serialized CompletionResponse.

    基于 Redis 的 cache，存放序列化后的 CompletionResponse。
    """

    def __init__(self, url: str):
        # Lazy import guarded above
        # 上方已做懒加载保护
        self._r = redis.Redis.from_url(url, decode_responses=True)

    def get(self, key: str) -> Optional[CompletionResponse]:
        raw = self._r.get(self._mk(key))
        if not raw:
            return None
        try:
            data = json.loads(raw)
            return _deserialize_response(data)
        except Exception:
            return None

    def set(self, key: str, response: CompletionResponse, ttl: int = 3600):
        data = _serialize_response(response)
        self._r.setex(self._mk(key), ttl, json.dumps(data))

    def delete(self, key: str) -> None:
        try:
            self._r.delete(self._mk(key))
        except Exception:
            pass

    @staticmethod
    def _mk(key: str) -> str:
        return f"llm:cache:{key}"

    @property
    def hit_rate(self) -> float:
        # Redis backend does not track hit/miss in this service; return 0.0 for compatibility
        # 本服务中 Redis 后端不跟踪 hit/miss；为兼容性返回 0.0
        return 0.0


def _serialize_response(resp: CompletionResponse) -> Dict[str, Any]:
    data = {
        "content": resp.content,
        "model": resp.model,
        "provider": resp.provider,
        "usage": {
            "input_tokens": getattr(resp.usage, "input_tokens", 0),
            "output_tokens": getattr(resp.usage, "output_tokens", 0),
            "total_tokens": getattr(resp.usage, "total_tokens", 0),
            "estimated_cost": float(getattr(resp.usage, "estimated_cost", 0.0)),
        },
        "finish_reason": resp.finish_reason,
        "function_call": resp.function_call,
        "request_id": resp.request_id,
        "latency_ms": resp.latency_ms,
        "cached": True,
        "created_at": resp.created_at.isoformat()
        if getattr(resp, "created_at", None)
        else None,
    }
    if getattr(resp, "tool_calls", None):
        data["tool_calls"] = resp.tool_calls
    # Persist the ordered content_blocks list so a Redis cache hit
    # round-trips thinking blocks back to the client. Without this, every
    # cached response silently reverts to the legacy {output_text, tool_calls}
    # shape and Anthropic's "preserve thinking across trajectory" contract
    # breaks for any session that touches a cached upstream call.
    # 持久化有序的 content_blocks 列表，使 Redis cache 命中时能把 thinking block
    # 原样回传给客户端。否则每个 cache 的 response 都会静默退回旧的 {output_text, tool_calls}
    # 形状，任何触及 cache 上游调用的 session 都会破坏 Anthropic 的
    # “在轨迹中保留 thinking”契约。
    if getattr(resp, "content_blocks", None):
        data["content_blocks"] = resp.content_blocks
    return data


def _deserialize_response(data: Dict[str, Any]) -> CompletionResponse:
    usage = TokenUsage(
        input_tokens=int(data.get("usage", {}).get("input_tokens", 0)),
        output_tokens=int(data.get("usage", {}).get("output_tokens", 0)),
        total_tokens=int(data.get("usage", {}).get("total_tokens", 0)),
        estimated_cost=float(data.get("usage", {}).get("estimated_cost", 0.0)),
    )
    resp = CompletionResponse(
        content=str(data.get("content", "")),
        model=str(data.get("model", "")),
        provider=str(data.get("provider", "")),
        usage=usage,
        finish_reason=str(data.get("finish_reason", "stop")),
        function_call=data.get("function_call"),
        tool_calls=data.get("tool_calls"),
        content_blocks=data.get("content_blocks"),
        request_id=data.get("request_id"),
        latency_ms=int(data.get("latency_ms"))
        if data.get("latency_ms") is not None
        else None,
    )
    resp.cached = True
    return resp


def _is_strict_json_mode(request: CompletionRequest) -> bool:
    rf = getattr(request, "response_format", None)
    return bool(isinstance(rf, dict) and rf.get("type") == "json_object")


# --- Resilience helpers ---
# --- resilience 辅助函数 ---
def _is_transient_error(err: Exception) -> bool:
    txt = str(err).lower()
    if "timeout" in txt or "timed out" in txt:
        return True
    if "429" in txt or "rate limit" in txt:
        return True
    # Heuristic for 5xx
    # 针对 5xx 的启发式判断
    if " 5" in txt or "internal server error" in txt:
        return True
    # SDKs may attach status_code
    # SDK 可能附带 status_code
    code = getattr(err, "status_code", None)
    try:
        if code is not None:
            code = int(code)
            if code == 429 or code >= 500:
                return True
    except Exception:
        pass
    return False


class _CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int,
        recovery_timeout: float,
        metrics_enabled: bool = False,
    ):
        self.name = name
        self.failure_threshold = max(1, int(failure_threshold))
        self.recovery_timeout = float(recovery_timeout)
        self.failures = 0
        self.state = "closed"  # closed | open | half-open 注：状态 closed | open | half-open
        # closed | open | half-open
        # 状态：closed | open | half-open
        self.opened_at = 0.0
        self._metrics = metrics_enabled

    def allow(self) -> bool:
        if self.state == "closed":
            return True
        if self.state == "open":
            # Transition to half-open after cooldown
            # Add small jitter (±10%) to avoid thundering herd
            # 冷却后转入 half-open
            # 加入小幅抖动（±10%）以避免惊群
            jitter = self.recovery_timeout * random.uniform(-0.1, 0.1)
            if (time.time() - self.opened_at) >= (self.recovery_timeout + jitter):
                self.state = "half-open"
                if self._metrics:
                    try:
                        LLM_MANAGER_CB_PROBES_TOTAL.labels(self.name).inc()
                    except Exception:
                        pass
                return True
            return False
        # half-open allows one probe
        # half-open 状态允许一次探测
        return True

    def on_success(self):
        if self.state in ("open", "half-open"):
            self._close()
        self.failures = 0

    def on_failure(self, transient: bool):
        if not transient:
            return
        self.failures += 1
        if self.failures >= self.failure_threshold and self.state != "open":
            self._open()

    def _open(self):
        self.state = "open"
        self.opened_at = time.time()
        import logging
        logging.getLogger("llm_provider.manager").warning(
            f"Circuit breaker OPENED for provider={self.name} "
            f"(failures={self.failures}/{self.failure_threshold}, "
            f"recovery_timeout={self.recovery_timeout}s)"
        )
        if self._metrics:
            try:
                LLM_MANAGER_CB_OPEN_TOTAL.labels(self.name).inc()
            except Exception:
                pass

    def _close(self):
        prev = self.state
        self.state = "closed"
        self.failures = 0
        self.opened_at = 0.0
        if self._metrics and prev != "closed":
            try:
                LLM_MANAGER_CB_CLOSE_TOTAL.labels(self.name).inc()
            except Exception:
                pass
