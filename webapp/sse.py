from __future__ import annotations

import asyncio
import json
from collections import defaultdict
from typing import Any, AsyncIterator


class TurnStreamBroker:
    def __init__(self) -> None:
        self._queues: dict[str, list[asyncio.Queue[dict[str, Any]]]] = defaultdict(list)
        self._events: dict[str, list[dict[str, Any]]] = defaultdict(list)

    def subscribe(self, turn_id: str) -> asyncio.Queue[dict[str, Any]]:
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        events = self._events.get(turn_id, [])
        for event in events:
            queue.put_nowait(event)
        if events and events[-1].get("event") in {"done", "error"}:
            return queue
        self._queues[turn_id].append(queue)
        return queue

    async def publish(self, turn_id: str, event: dict[str, Any]) -> None:
        self._events[turn_id].append(event)
        for queue in list(self._queues.get(turn_id, [])):
            await queue.put(event)
        if event.get("event") in {"done", "error"}:
            self._queues.pop(turn_id, None)

    async def stream(self, turn_id: str) -> AsyncIterator[str]:
        queue = self.subscribe(turn_id)
        while True:
            event = await queue.get()
            name = str(event.get("event") or "message")
            data = json.dumps(event.get("data", {}), ensure_ascii=False)
            yield f"event: {name}\ndata: {data}\n\n"
            if name in {"done", "error"}:
                return
