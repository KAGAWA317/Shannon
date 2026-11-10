"""
Integration tests for Shannon Python SDK.

Shannon Python SDK 的集成测试。

These tests require a running Shannon stack (localhost:8080) and are
SKIPPED by default when running pytest. They test control signal
features (pause/resume/cancel).

这些测试需要运行中的 Shannon 服务栈（localhost:8080），默认在 pytest
下会被跳过（SKIPPED）。它们测试控制信号功能（pause/resume/cancel）。

To run with pytest (requires running Shannon stack):
    SHANNON_INTEGRATION_TESTS=1 pytest tests/integration/ -v

To run directly (requires running Shannon stack):
    python tests/integration/test_control_signals.py
    python tests/integration/test_control_simple.py
"""
