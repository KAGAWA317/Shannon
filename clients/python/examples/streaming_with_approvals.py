"""Example of streaming events and handling approvals.

示例：流式接收 event 并处理 approval。
"""

import os
import re

from shannon import EventType, ShannonClient

# Initialize client
# 初始化客户端
client = ShannonClient(
    base_url="http://localhost:8080",
    api_key=os.getenv("SHANNON_API_KEY", ""),
)

# Submit a task requiring approval
# 提交一个需要 approval 的 task
print("Submitting task with approval requirement...")
handle = client.submit_task(
    "Analyze market data and execute a trade if conditions are favorable",
    session_id="trading-session",
)

print("✓ Task submitted:")
print(f"  Task ID: {handle.task_id}")
print(f"  Workflow ID: {handle.workflow_id}")
print()

# Stream events and handle approvals
# 流式接收 event 并处理 approval
print("Streaming events...")
print("-" * 60)

for event in client.stream(
    handle.workflow_id,
    types=[
        EventType.AGENT_STARTED,
        EventType.LLM_PARTIAL,
        EventType.TOOL_INVOKED,
        EventType.APPROVAL_REQUESTED,
        EventType.APPROVAL_DECISION,
        EventType.WORKFLOW_COMPLETED,
    ],
):
    # Display event
    # 展示 event
    prefix = "🤖" if event.type == EventType.AGENT_STARTED else \
             "💭" if event.type == EventType.LLM_PARTIAL else \
             "🔧" if event.type == EventType.TOOL_INVOKED else \
             "⏸️ " if event.type == EventType.APPROVAL_REQUESTED else \
             "✅" if event.type == EventType.APPROVAL_DECISION else \
             "🏁" if event.type == EventType.WORKFLOW_COMPLETED else "📡"

    print(f"{prefix} [{event.type}] {event.message}")

    # Handle approval requests
    # 处理 approval request
    if event.type == EventType.APPROVAL_REQUESTED:
        print()
        print("⏸️  APPROVAL REQUIRED")
        print("-" * 60)

        approval_id = None
        if event.payload and isinstance(event.payload, dict):
            approval_id = event.payload.get("approval_id")
        if not approval_id:
            match = re.search(r'id=([a-f0-9-]{36})', event.message)
            if match:
                approval_id = match.group(1)
                print(f"Parsed approval ID: {approval_id}")

        if approval_id:
            # Auto-approve for demo
            # demo 中自动批准
            success = client.approve(
                approval_id=approval_id,
                workflow_id=handle.workflow_id,
                approved=True,
                feedback="Auto-approved for demo"
            )
            print(f"✓ Auto-approved: {success}")
        else:
            print("Could not determine approval_id from event payload or message.")

        print("-" * 60)
        print()

    # Exit on completion
    # 完成后退出
    if event.type == EventType.WORKFLOW_COMPLETED:
        break

print()
print("-" * 60)
print("Stream complete. Getting final status...")

# Get final status
# 获取最终状态
status = client.get_status(handle.task_id)
print(f"Final status: {status.status.value}")
if status.result:
    print(f"Result: {status.result}")
# Optional: fetch usage totals from task listings
# 可选：从 task 列表中获取用量汇总
tasks, _ = client.list_tasks(limit=50)
usage = next((t.total_token_usage for t in tasks if t.task_id == handle.task_id), None)
if usage:
    print(f"Tokens: total={usage.total_tokens}, prompt={usage.prompt_tokens}, completion={usage.completion_tokens}")
    print(f"Cost: ${usage.cost_usd:.6f}")

client.close()
