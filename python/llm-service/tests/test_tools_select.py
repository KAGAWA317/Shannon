from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_tools_select_basic():
    # Ensure startup events ran and tools registered
    # 确保启动 event 已执行且 tool 已注册
    resp = client.get("/tools/list")
    assert resp.status_code == 200
    tools = resp.json()
    assert isinstance(tools, list)

    # Call selector without providers configured (heuristic fallback)
    # 未配置 provider 时调用选择器（走启发式回退）
    body = {
        "task": "calculate 2 + 2 and search latest news",
        "exclude_dangerous": True,
        "max_tools": 2,
    }
    r = client.post("/tools/select", json=body)
    assert r.status_code == 200
    data = r.json()
    assert "selected_tools" in data
    assert "calls" in data
    assert isinstance(data["selected_tools"], list)
    assert isinstance(data["calls"], list)

    # Response should be cacheable and consistent on second call
    # response 应可 cache，且第二次调用结果保持一致
    r2 = client.post("/tools/select", json=body)
    assert r2.status_code == 200
    data2 = r2.json()
    assert data == data2
