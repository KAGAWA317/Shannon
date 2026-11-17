"""Simple streaming example.

简单的流式示例。
"""

import os

from shannon import EventType, ShannonClient

# Initialize client
# 初始化客户端
client = ShannonClient(
    base_url="http://localhost:8080",
    api_key=os.getenv("SHANNON_API_KEY", ""),
)

# Submit a task
# 提交 task
print("Submitting task...")
handle = client.submit_task(
    "Research recent developments in quantum computing and summarize key findings",
)

print(f"Task ID: {handle.task_id}")
print(f"Workflow ID: {handle.workflow_id}")
print()

# Stream only interesting events
# 只流式订阅感兴趣的 event
print("Streaming events (LLM outputs and tool calls)...")
print("-" * 60)

for event in client.stream(
    handle.workflow_id,
    types=[EventType.LLM_PARTIAL, EventType.TOOL_INVOKED, EventType.WORKFLOW_COMPLETED],
):
    if event.type == EventType.LLM_PARTIAL:
        print(f"💭 {event.message}")
    elif event.type == EventType.TOOL_INVOKED:
        print(f"🔧 Tool: {event.message}")
    elif event.type == EventType.WORKFLOW_COMPLETED:
        print(f"✓ {event.message}")
        break

print("-" * 60)

# Get final result
# 获取最终结果
final = client.wait(handle.task_id, timeout=120)
print(f"\nFinal result:\n{final.result}")

client.close()
