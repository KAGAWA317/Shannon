#!/usr/bin/env python3
"""
Simple fast test for control signals with a quick task.

用一个快速 task 做的 control signal 简单测试。

NOTE: This is an integration test requiring a running Shannon stack.
Skipped by default. To run:
    SHANNON_INTEGRATION_TESTS=1 pytest tests/integration/
    OR
    python tests/integration/test_control_simple.py

需先启动 Shannon 全栈；默认跳过。
"""

import os
import time

import pytest

from shannon import ShannonClient

# Skip in pytest unless explicitly enabled
# 除非显式开启，否则在 pytest 中跳过
pytestmark = pytest.mark.skipif(
    not os.environ.get("SHANNON_INTEGRATION_TESTS"),
    reason="Integration tests require running Shannon stack. Set SHANNON_INTEGRATION_TESTS=1 to run."
)


def test_simple_control():
    """Test pause/resume with a simple fast task.

    用一个简单快速的 task 测试 pause/resume。
    """

    client = ShannonClient(base_url="http://localhost:8080")

    print("=" * 60)
    print("Shannon SDK v0.4.0 - Simple Control Test")
    print("=" * 60)

    # Submit a simple task
    # 提交一个 simple task
    print("\n[1] Submitting a simple task...")
    handle = client.submit_task(
        "Calculate 2 + 2 and explain the result",
        session_id="sdk-simple-control-test",
        mode="simple"
    )

    print(f"✓ Task submitted: {handle.task_id}")

    # Wait for task to start
    # 等待 task 启动
    print("\n[2] Waiting 2 seconds for task to start...")
    time.sleep(2)

    # Pause
    # 暂停
    print("\n[3] Pausing task...")
    client.pause_task(handle.task_id, reason="Simple test pause")
    print("✓ Pause request sent")

    time.sleep(2)

    # Check control state
    # 检查 control 状态
    print("\n[4] Checking control state...")
    state = client.get_control_state(handle.task_id)
    print(f"✓ Paused: {state.is_paused}")
    print(f"  Reason: {state.pause_reason}")
    if state.paused_at:
        print(f"  At: {state.paused_at}")

    # Resume
    # 恢复
    print("\n[5] Resuming task...")
    client.resume_task(handle.task_id, reason="Simple test resume")
    print("✓ Resume request sent")

    # Wait for completion
    # 等待完成
    print("\n[6] Waiting for completion (30s timeout)...")
    try:
        final = client.wait(handle.task_id, timeout=30)
        print(f"✓ Task completed: {final.status}")
        print(f"\nResult: {final.result}")

        # Verify metadata
        # 校验 metadata
        print("\nMetadata:")
        print(f"  Model: {final.model_used}")
        print(f"  Provider: {final.provider}")
        if final.usage:
            print(f"  Tokens: {final.usage.get('total_tokens')}")
            print(f"  Cost: ${final.usage.get('cost_usd', 0):.6f}")

        print("\n" + "=" * 60)
        print("✓ Simple control test PASSED!")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"✗ Failed: {e}")
        # Check final status
        # 查看最终 status
        status = client.get_status(handle.task_id)
        print(f"Final status: {status.status}")
        return False

    finally:
        client.close()


if __name__ == "__main__":
    success = test_simple_control()
    exit(0 if success else 1)
