import asyncio

from llm_provider.base import RateLimiter


def test_rate_limiter_throttles_and_advances(monkeypatch):
    # Simulate time progression without real sleeps
    # 在不真正 sleep 的情况下模拟时间推进
    current = [0.0]  # mutable container 注：可变容器

    def fake_time():
        return current[0]

    async def fake_sleep(dt):
        # fast-forward time instead of real sleep
        # 直接快进时间，而不是真正 sleep
        current[0] += dt

    # Patch time and asyncio.sleep used inside RateLimiter
    # 替换 RateLimiter 内部使用的 time 与 asyncio.sleep
    import llm_provider.base as base

    monkeypatch.setattr(base.time, "time", fake_time)
    monkeypatch.setattr(base.asyncio, "sleep", fake_sleep)

    async def _run():
        rl = RateLimiter(requests_per_minute=3)

        # First 3 acquire calls should pass without wait (t=0)
        # 前 3 次 acquire 调用应无需等待直接通过（t=0）
        await rl.acquire()
        await rl.acquire()
        await rl.acquire()

        # 4th should force a wait ≈ 60s because window is full
        # 第 4 次因窗口已满，应强制等待约 60 秒
        start = fake_time()
        await rl.acquire()
        waited = fake_time() - start
        assert waited >= 59.9

        # Now window has advanced; next two should not need to wait further
        # 此时窗口已推进；接下来两次不应再需要等待
        t_before = fake_time()
        await rl.acquire()
        await rl.acquire()
        assert fake_time() == t_before  # no extra waiting 注：没有额外等待

    asyncio.run(_run())
