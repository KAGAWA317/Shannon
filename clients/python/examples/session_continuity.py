"""Example of session continuity using HTTP session endpoints.

示例：使用 HTTP session endpoint 实现 session 连续性。
"""

import os
from datetime import datetime
from shannon import ShannonClient

# Initialize client
# 初始化客户端
client = ShannonClient(
    base_url="http://localhost:8080",
    api_key=os.getenv("SHANNON_API_KEY", ""),
)

print("=" * 60)
print("Session Management Demo - Multi-turn Conversation")
print("=" * 60)
print()

# Use a stable session ID across turns
# 多轮对话中复用同一个 session ID
session_id = f"demo-session-{int(datetime.now().timestamp())}"
print(f"Using session_id={session_id}")

# Turn 1
# 第 1 轮
print("-" * 60)
print("Turn 1: Establishing context")
print("-" * 60)
handle1 = client.submit_task(
    "My name is Alice and I'm working on a Python analytics dashboard. "
    "I need help optimizing database queries.",
    session_id=session_id,
    model_tier="small",
    mode="simple",
)
print(f"Task 1 ID: {handle1.task_id}")
print(handle1.result(timeout=60))

# Turn 2
# 第 2 轮
print("-" * 60)
print("Turn 2: Follow-up")
print("-" * 60)
handle2 = client.submit_task(
    "What specific optimization techniques would work best for my use case?",
    session_id=session_id,
)
print(f"Task 2 ID: {handle2.task_id}")
print(handle2.result(timeout=60))

# Turn 3
# 第 3 轮
print("-" * 60)
print("Turn 3: Code example")
print("-" * 60)
handle3 = client.submit_task(
    "Can you show me a code example for connection pooling?",
    session_id=session_id,
)
print(f"Task 3 ID: {handle3.task_id}")
print(handle3.result(timeout=60))

# Fetch session metadata and history
# 获取 session metadata 与历史
print("\nFetching session info and history...")
sess = client.get_session(session_id)
history = client.get_session_history(session_id)
print(f"Session: {sess.session_id} created {sess.created_at}")
print(f"Turns in history: {len(history)}")

# Update session title
# 更新 session 标题
print("\nUpdating session title...")
client.update_session_title(session_id, "Demo Session Title")
print("✓ Title updated")

# List sessions (first page)
# 列出 session（第一页）
print("\nListing sessions...")
sessions, total = client.list_sessions(limit=5, offset=0)
print(f"Total sessions: {total}, showing {len(sessions)}")
for s in sessions:
    print(f"  - {s.session_id} (messages={s.message_count})")

print("\n✓ Session demo completed!")
client.close()
