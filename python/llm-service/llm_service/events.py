import asyncio
import contextlib
import json
import os
import time
from typing import Any

import httpx


class EventEmitter:
    """Async, non-blocking event emitter posting to orchestrator /events.

    异步、非阻塞的 event emitter，向 orchestrator 的 /events 投递。
    """

    def __init__(
        self,
        ingest_url: str = "http://orchestrator:8081/events",
        auth_token: str | None = None,
        queue_max: int = 1000,
        timeout_seconds: float = 2.0,
    ):
        self.ingest_url = ingest_url
        self.auth_token = auth_token
        self.timeout = timeout_seconds
        self._q: asyncio.Queue = asyncio.Queue(maxsize=queue_max)
        self._task: asyncio.Task | None = None
        self._client: httpx.AsyncClient | None = None

    async def start(self):
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout)
        if self._task is None:
            self._task = asyncio.create_task(self._worker())

    async def close(self):
        if self._task:
            self._task.cancel()
            with contextlib.suppress(Exception):
                await self._task
            self._task = None
        if self._client:
            await self._client.aclose()
            self._client = None

    def emit(
        self,
        workflow_id: str,
        etype: str,
        agent_id: str | None = None,
        message: str = "",
        timestamp: float | None = None,
        payload: dict[str, Any] | None = None,
    ):
        if not workflow_id or not etype:
            return
        ts = timestamp if timestamp is not None else time.time()
        data = {
            "workflow_id": workflow_id,
            "type": etype,
            "agent_id": agent_id or "",
            "message": message or "",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)),
            "payload": payload or {},
        }
        try:
            self._q.put_nowait(data)
        except asyncio.QueueFull:
            pass  # drop silently 注：静默丢弃

    async def _worker(self):
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"
        while True:
            try:
                batch = [await self._q.get()]
                # small batch drain
                # 小批量取出
                for _ in range(50):
                    try:
                        batch.append(self._q.get_nowait())
                    except asyncio.QueueEmpty:
                        break
                if len(batch) == 1:
                    data = batch[0]
                else:
                    data = batch
                if self._client is None:
                    self._client = httpx.AsyncClient(timeout=self.timeout)
                await self._client.post(
                    self.ingest_url, headers=headers, content=json.dumps(data)
                )
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(0.1)


def build_default_emitter_from_env() -> EventEmitter:
    url = os.getenv("EVENTS_INGEST_URL", "http://orchestrator:8081/events")
    token = os.getenv("EVENTS_AUTH_TOKEN", "") or None
    return EventEmitter(ingest_url=url, auth_token=token)
