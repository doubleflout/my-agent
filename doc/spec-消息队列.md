# 消息队列改造 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将项目现有进程内 `asyncio.Queue` 升级为统一 MessageQueue 抽象，后续可平滑切换到 RabbitMQ/Redis Streams，并服务被动消息、主动推送、Subagent 结果回灌和观测事件。

**Architecture:** 第一阶段不直接把业务代码绑死到 MQ，而是在现有 `bus/queue.py` 之上抽象统一消息模型和发布接口。第二阶段增加 PostgreSQL outbox，保证消息持久化和可重试。第三阶段再把 outbox dispatcher 接到 RabbitMQ，实现跨进程 worker、ACK、retry、dead letter 和水平扩展。

**Tech Stack:** Python asyncio, PostgreSQL, SQLAlchemy/psycopg, RabbitMQ, aio-pika, FastAPI, SSE, existing `bus/queue.py`, existing `bus/event_bus.py`.

## Global Constraints

- 不删除现有 `bus.queue.MessageBus`，第一阶段保持兼容。
- 不让 Agent、Proactive、Subagent 直接依赖 RabbitMQ SDK。
- 所有业务消息必须携带 `user_id`、`session_key`、`conversation_id`、`turn_id` 中可获得的字段。
- `session_key` 仍然是 Agent 会话隔离核心字段。
- Web 多用户链路优先，Telegram 兼容后移。
- 被动链路最后迁移，避免一次性改坏核心回复流程。
- `bus.event_bus.EventBus` 保持观测/生命周期事件职责，不承担可靠业务投递。

---

## 1. 当前问题

项目现在已经有队列，但它更像单进程内的协程通道：

- `bus/queue.py` 里有 `asyncio.Queue[InboundItem]` 和 `asyncio.Queue[OutboundMessage]`。
- `webapp/sse.py` 里 `TurnStreamBroker` 也用 `asyncio.Queue` 做 turn 内 SSE 事件分发。
- `agent/background/subagent_manager.py` 在后台 Subagent 完成后调用 `publish_inbound()`，把 `SpawnCompletionItem` 回灌给主 Agent。
- `bus/event_bus.py` 也有 `enqueue()`，但它负责插件、trace、observe 这类旁路事件，不是业务消息投递。

这些队列的问题不是“不能用”，而是：

- 只能在当前 Python 进程内使用。
- 进程重启后消息丢失。
- 没有统一 envelope，主动消息、被动消息、Subagent 结果格式不一致。
- 没有 ACK、retry、dead letter。
- 后续 FastAPI、AgentWorker、Telegram adapter 拆进程时无法共享。

所以改造目标不是把 `asyncio.Queue` 立刻删掉，而是把它降级成 `MessageQueue` 的一个 backend。

## 2. 目标架构

```text
Web / Telegram / Proactive / Subagent
  -> MessageQueue.publish(envelope)
  -> backend: asyncio | postgres_outbox | rabbitmq
  -> Consumer / Worker
  -> AgentLoop / SSE / Channel Adapter
```

建议拆四类业务 topic：

```text
agent.inbound
  用户消息、Subagent 结果、内部继续处理事件进入 Agent。

agent.outbound
  Agent 回复、主动推送、工具产生的可投递消息。

agent.tasks
  长任务、Subagent 后台任务、未来定时任务。

agent.events
  业务级事件流，可用于 trace/日志桥接，但不替代 event_bus。
```

`bus.event_bus.EventBus` 继续负责：

```text
ToolCallStarted
ToolCallFinished
TurnCommitted
TraceEvent
Plugin observe/fanout
```

`MessageQueue` 负责：

```text
用户消息投递
主动推送投递
Subagent 任务和结果回灌
跨进程 worker 调度
```

## 3. 统一消息结构

新增统一 envelope，建议文件：

```text
bus/message_queue_types.py
```

核心字段：

```python
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

QueueTopic = Literal[
    "agent.inbound",
    "agent.outbound",
    "agent.tasks",
    "agent.events",
]

QueueEventType = Literal[
    "user_message",
    "assistant_message",
    "proactive_push",
    "subagent_task",
    "subagent_result",
    "turn_event",
    "tool_event",
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class QueueEnvelope:
    event_id: str
    topic: QueueTopic
    event_type: QueueEventType
    user_id: str
    session_key: str
    conversation_id: str = ""
    turn_id: str = ""
    source: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)

    @classmethod
    def new(
        cls,
        *,
        topic: QueueTopic,
        event_type: QueueEventType,
        user_id: str,
        session_key: str,
        conversation_id: str = "",
        turn_id: str = "",
        source: str = "",
        payload: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> "QueueEnvelope":
        return cls(
            event_id=str(uuid4()),
            topic=topic,
            event_type=event_type,
            user_id=user_id,
            session_key=session_key,
            conversation_id=conversation_id,
            turn_id=turn_id,
            source=source,
            payload=payload or {},
            metadata=metadata or {},
        )
```

## 4. MessageQueue 接口

新增接口文件：

```text
bus/message_queue.py
```

```python
from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol

from bus.message_queue_types import QueueEnvelope, QueueTopic


class MessageQueueBackend(Protocol):
    async def publish(self, message: QueueEnvelope) -> None: ...

    async def subscribe(
        self,
        topic: QueueTopic,
        *,
        consumer_name: str,
    ) -> AsyncIterator[QueueEnvelope]: ...

    async def ack(self, message: QueueEnvelope) -> None: ...

    async def nack(
        self,
        message: QueueEnvelope,
        *,
        retry: bool = True,
        reason: str = "",
    ) -> None: ...

    async def close(self) -> None: ...
```

业务层只依赖这个协议，不直接 import `aio_pika` 或 Redis 客户端。

## 5. 第一阶段：兼容现有 asyncio.Queue

新增：

```text
bus/backends/asyncio_message_queue.py
```

用途：

- 单进程开发环境继续可用。
- 不要求 RabbitMQ 启动。
- 先让业务代码统一调用 `publish(QueueEnvelope)`。

实现原则：

- 内部使用 `dict[topic, asyncio.Queue[QueueEnvelope]]`。
- `ack()` 第一版可以是 no-op。
- `nack(retry=True)` 可以重新放回 queue。
- 不保证进程重启恢复，这一点写入日志和文档。

第一阶段迁移后链路：

```text
push_message / subagent completion / web inbound
  -> QueueEnvelope
  -> AsyncioMessageQueue
```

## 6. 第二阶段：PostgreSQL Outbox

新增表：

```sql
CREATE TABLE IF NOT EXISTS message_outbox (
    id UUID PRIMARY KEY,
    topic TEXT NOT NULL,
    event_type TEXT NOT NULL,
    user_id UUID NOT NULL REFERENCES users(id),
    session_key TEXT NOT NULL,
    conversation_id UUID NULL,
    turn_id UUID NULL,
    source TEXT NOT NULL DEFAULT '',
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    next_retry_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_at TIMESTAMPTZ NULL,
    published_at TIMESTAMPTZ NULL,
    error TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_message_outbox_pending
    ON message_outbox(status, next_retry_at, created_at);

CREATE INDEX IF NOT EXISTS idx_message_outbox_user_session
    ON message_outbox(user_id, session_key, created_at);

CREATE INDEX IF NOT EXISTS idx_message_outbox_turn
    ON message_outbox(turn_id)
    WHERE turn_id IS NOT NULL;
```

为什么需要 outbox：

- Agent 生成主动消息后先落库，进程崩了也不丢。
- dispatcher 可以独立重试。
- 后面 RabbitMQ 挂了，业务仍可先把消息记下来。
- 与 `chat_messages`、`agent_turns` 可以在同一个数据库事务里提交。

状态流转：

```text
pending
  -> publishing
  -> published
  -> failed
```

失败重试：

```text
retry_count = retry_count + 1
next_retry_at = now() + backoff_seconds
```

超过最大次数：

```text
status = failed
error = last_error
```

## 7. 第三阶段：RabbitMQ Backend

建议 exchange：

```text
akashic.agent
```

routing key：

```text
agent.inbound
agent.outbound
agent.tasks
agent.events
```

队列：

```text
akashic.agent.inbound
akashic.agent.outbound.web
akashic.agent.outbound.telegram
akashic.agent.tasks.subagent
akashic.agent.events.observe
```

RabbitMQ message body 使用 `QueueEnvelope` JSON：

```json
{
  "event_id": "uuid",
  "topic": "agent.outbound",
  "event_type": "proactive_push",
  "user_id": "uuid",
  "session_key": "web:proactive:<user_id>:<conversation_id>",
  "conversation_id": "uuid",
  "turn_id": "",
  "source": "proactive",
  "payload": {
    "content": "..."
  },
  "metadata": {},
  "created_at": "2026-08-11T10:00:00+00:00"
}
```

RabbitMQ 只在 backend/dispatcher 层出现：

```text
bus/backends/rabbitmq_message_queue.py
bus/outbox_dispatcher.py
```

Agent、Proactive、Subagent 不直接 import RabbitMQ。

## 8. 主动链路迁移

当前 Web proactive 大致链路：

```text
WebProactiveScheduler
  -> UserRuntimeProactiveRunner
  -> AgentTick
  -> TurnOrchestrator
  -> _WebProactiveOutboundPort.dispatch()
  -> WebStore.add_message()
```

第一阶段建议改为：

```text
_WebProactiveOutboundPort.dispatch()
  -> WebStore.add_message()
  -> MessageQueue.publish(agent.outbound / proactive_push)
```

为什么仍然先写 `chat_messages`：

- Web 页面刷新后能看到历史。
- MQ 消费失败不会导致消息完全丢失。
- 后续可以让 Web SSE consumer 监听 `agent.outbound` 做实时推送。

主动消息 envelope：

```python
QueueEnvelope.new(
    topic="agent.outbound",
    event_type="proactive_push",
    user_id=user_id,
    session_key=session_key,
    conversation_id=conversation_id,
    source="proactive",
    payload={
        "role": "assistant",
        "content": content,
        "media": media,
    },
    metadata=metadata,
)
```

## 9. 被动链路迁移

当前 Web 被动链路：

```text
POST /api/conversations/{conversation_id}/messages
  -> 写用户消息
  -> 创建 agent_turns
  -> 直接启动 AgentExecutor
  -> TurnStreamBroker 发布 SSE
```

后续目标：

```text
POST /messages
  -> 写 chat_messages
  -> 写 agent_turns(status=queued)
  -> publish agent.inbound/user_message
  -> 立即返回 turn_id

AgentWorker
  -> consume agent.inbound
  -> runtime.loop.process_direct/process_stream
  -> publish agent.outbound/assistant_message
  -> 更新 agent_turns(status=completed/failed)

SSE endpoint
  -> 按 turn_id 读取 broker 或消息事件
```

被动链路最后迁移，原因：

- 它是用户实时聊天主路径。
- 现在已经跑通多用户，不要先动最核心的地方。
- 先让 proactive 和 subagent 证明消息模型可用。

## 10. Subagent 迁移

当前：

```text
SubagentManager._run_subagent()
  -> SpawnCompletionItem
  -> self._bus.publish_inbound(item)
```

目标：

```text
SubagentManager._run_subagent()
  -> QueueEnvelope(event_type=subagent_result)
  -> MessageQueue.publish(agent.inbound)
```

消息 payload：

```json
{
  "job_id": "...",
  "label": "...",
  "task": "...",
  "status": "completed",
  "exit_reason": "completed",
  "result": "...",
  "retry_count": 0,
  "profile": "research"
}
```

关键点：

- `origin_channel` + `origin_chat_id` 仍可用于兼容旧路由。
- 新增时优先携带 `user_id`、`session_key`、`conversation_id`。
- 如果暂时拿不到 `user_id`，不要在数据库层兜底生成随机用户，应该在创建 subagent 任务时从 tool registry context 传入。

## 11. SSE 迁移

当前 `webapp/sse.py` 的 `TurnStreamBroker` 是进程内队列：

```text
turn_id -> list[asyncio.Queue]
```

第一阶段继续保留。

第二阶段可以增加：

```text
agent.outbound -> TurnStreamBroker.publish(turn_id, event)
```

第三阶段多进程时：

```text
RabbitMQ/Redis pubsub
  -> Web SSE process
  -> TurnStreamBroker
```

注意：

- SSE 连接本身仍然由 FastAPI 进程维护。
- MQ 不直接替代 SSE，它只负责跨进程传递 turn event。

## 12. 配置设计

建议增加：

```toml
[messaging]
backend = "asyncio" # asyncio | postgres_outbox | rabbitmq

[messaging.rabbitmq]
url = "amqp://guest:guest@localhost:5672/"
exchange = "akashic.agent"
prefetch = 10
max_retries = 5
dead_letter_exchange = "akashic.agent.dlx"

[messaging.outbox]
enabled = true
poll_interval_seconds = 1
batch_size = 50
max_retries = 5
```

MVP 默认：

```toml
[messaging]
backend = "asyncio"
```

多进程部署：

```toml
[messaging]
backend = "postgres_outbox"
```

生产可靠投递：

```toml
[messaging]
backend = "rabbitmq"
```

## 13. 开发任务拆分

### Task 1: 定义统一消息模型

**Files:**
- Create: `bus/message_queue_types.py`
- Test: `tests/test_message_queue_types.py`

**Interfaces:**
- Produces: `QueueEnvelope.new(...)`
- Produces: `QueueTopic`, `QueueEventType`

- [ ] **Step 1: 写 envelope 创建测试**

```python
from bus.message_queue_types import QueueEnvelope


def test_message_envelope_new_sets_required_fields():
    msg = QueueEnvelope.new(
        topic="agent.outbound",
        event_type="proactive_push",
        user_id="u1",
        session_key="web:proactive:u1:c1",
        conversation_id="c1",
        source="proactive",
        payload={"content": "hello"},
    )

    assert msg.event_id
    assert msg.topic == "agent.outbound"
    assert msg.event_type == "proactive_push"
    assert msg.user_id == "u1"
    assert msg.session_key == "web:proactive:u1:c1"
    assert msg.payload == {"content": "hello"}
```

- [ ] **Step 2: 运行测试确认失败**

```powershell
D:\codesoft\ancoda\envs\jianshenai\python.exe -m pytest tests/test_message_queue_types.py -q
```

- [ ] **Step 3: 实现 `bus/message_queue_types.py`**

按本文第 3 节代码实现。

- [ ] **Step 4: 运行测试确认通过**

```powershell
D:\codesoft\ancoda\envs\jianshenai\python.exe -m pytest tests/test_message_queue_types.py -q
```

### Task 2: 定义 MessageQueue 协议

**Files:**
- Create: `bus/message_queue.py`
- Test: `tests/test_message_queue_protocol.py`

**Interfaces:**
- Consumes: `QueueEnvelope`
- Produces: `MessageQueueBackend`

- [ ] **Step 1: 写协议导入测试**

```python
from bus.message_queue import MessageQueueBackend


def test_message_bus_protocol_importable():
    assert MessageQueueBackend is not None
```

- [ ] **Step 2: 运行测试确认失败**

```powershell
D:\codesoft\ancoda\envs\jianshenai\python.exe -m pytest tests/test_message_queue_protocol.py -q
```

- [ ] **Step 3: 实现 `bus/message_queue.py`**

按本文第 4 节代码实现。

- [ ] **Step 4: 运行测试确认通过**

```powershell
D:\codesoft\ancoda\envs\jianshenai\python.exe -m pytest tests/test_message_queue_protocol.py -q
```

### Task 3: 实现 Asyncio backend

**Files:**
- Create: `bus/backends/__init__.py`
- Create: `bus/backends/asyncio_message_queue.py`
- Test: `tests/test_asyncio_message_queue.py`

**Interfaces:**
- Consumes: `QueueEnvelope`
- Produces: `AsyncioMessageQueue.publish()`
- Produces: `AsyncioMessageQueue.subscribe()`
- Produces: `AsyncioMessageQueue.ack()`
- Produces: `AsyncioMessageQueue.nack()`

- [ ] **Step 1: 写发布消费测试**

```python
import pytest

from bus.backends.asyncio_message_queue import AsyncioMessageQueue
from bus.message_queue_types import QueueEnvelope


@pytest.mark.asyncio
async def test_asyncio_message_queue_publish_and_subscribe():
    bus = AsyncioMessageQueue()
    msg = QueueEnvelope.new(
        topic="agent.outbound",
        event_type="assistant_message",
        user_id="u1",
        session_key="web:u1:c1",
        payload={"content": "ok"},
    )

    await bus.publish(msg)
    stream = bus.subscribe("agent.outbound", consumer_name="test")
    received = await anext(stream)

    assert received.event_id == msg.event_id
    await bus.ack(received)
    await bus.close()
```

- [ ] **Step 2: 写 nack 重试测试**

```python
import pytest

from bus.backends.asyncio_message_queue import AsyncioMessageQueue
from bus.message_queue_types import QueueEnvelope


@pytest.mark.asyncio
async def test_asyncio_message_queue_nack_requeues_when_retry_true():
    bus = AsyncioMessageQueue()
    msg = QueueEnvelope.new(
        topic="agent.tasks",
        event_type="subagent_task",
        user_id="u1",
        session_key="web:u1:c1",
    )

    await bus.publish(msg)
    stream = bus.subscribe("agent.tasks", consumer_name="worker")
    first = await anext(stream)
    await bus.nack(first, retry=True, reason="temporary")
    second = await anext(stream)

    assert second.event_id == msg.event_id
    await bus.close()
```

- [ ] **Step 3: 实现 AsyncioMessageQueue**

核心逻辑：

```python
class AsyncioMessageQueue:
    def __init__(self) -> None:
        self._queues: dict[QueueTopic, asyncio.Queue[QueueEnvelope]] = {}
        self._closed = False

    async def publish(self, message: QueueEnvelope) -> None:
        if self._closed:
            raise RuntimeError("message bus is closed")
        await self._queue(message.topic).put(message)

    async def subscribe(self, topic: QueueTopic, *, consumer_name: str):
        queue = self._queue(topic)
        while not self._closed:
            yield await queue.get()

    async def ack(self, message: QueueEnvelope) -> None:
        return None

    async def nack(self, message: QueueEnvelope, *, retry: bool = True, reason: str = "") -> None:
        if retry:
            await self.publish(message)

    async def close(self) -> None:
        self._closed = True
```

- [ ] **Step 4: 运行测试**

```powershell
D:\codesoft\ancoda\envs\jianshenai\python.exe -m pytest tests/test_asyncio_message_queue.py -q
```

### Task 4: 主动推送接 MessageQueue

**Files:**
- Modify: `webapp/runtime_manager.py`
- Modify: `bootstrap/web_server.py`
- Test: `tests/test_web_proactive_message_queue.py`

**Interfaces:**
- Consumes: `MessageQueueBackend`
- Produces: `_WebProactiveOutboundPort.dispatch()` 写库后发布 `proactive_push`

- [ ] **Step 1: 给 `_WebProactiveOutboundPort` 增加可选 `message_bus`**

签名目标：

```python
class _WebProactiveOutboundPort(OutboundPort):
    def __init__(
        self,
        *,
        store: WebStore,
        user_id: str,
        conversation_id: str,
        session_key: str,
        message_queue: MessageQueueBackend | None = None,
    ) -> None:
        ...
```

- [ ] **Step 2: dispatch 写库后发布 envelope**

```python
if self._message_queue is not None:
    await self._message_queue.publish(
        QueueEnvelope.new(
            topic="agent.outbound",
            event_type="proactive_push",
            user_id=self._user_id,
            session_key=self._session_key,
            conversation_id=self._conversation_id,
            source="proactive",
            payload={
                "role": "assistant",
                "content": content,
                "media": media,
            },
            metadata=metadata,
        )
    )
```

- [ ] **Step 3: 测试消息发布**

测试思路：

```python
class FakeMessageQueue:
    def __init__(self):
        self.messages = []

    async def publish(self, message):
        self.messages.append(message)
```

断言：

```python
assert fake_queue.messages[0].event_type == "proactive_push"
assert fake_queue.messages[0].user_id == user_id
assert fake_queue.messages[0].session_key == session_key
```

### Task 5: Subagent 结果接 MessageQueue

**Files:**
- Modify: `agent/background/subagent_manager.py`
- Modify: `agent/tools/spawn.py`
- Test: `tests/test_subagent_message_queue.py`

**Interfaces:**
- Consumes: `MessageQueueBackend`
- Produces: `subagent_result` envelope

- [ ] **Step 1: 创建兼容桥**

为了不一次性破坏旧 `bus.queue.MessageBus.publish_inbound()`，先做桥接：

```python
async def publish_subagent_result(
    *,
    legacy_bus: LegacyMessageBus,
    message_queue: MessageQueueBackend | None,
    item: SpawnCompletionItem,
    user_id: str,
    conversation_id: str = "",
) -> None:
    if message_bus is not None:
        await message_bus.publish(...)
    await legacy_bus.publish_inbound(item)
```

- [ ] **Step 2: 从 tool registry context 获取 user_id/session_key**

`spawn` 工具创建后台任务时已经能拿 `channel/chat_id`。需要继续传：

```python
ctx = self._tool_registry.get_context()
user_id = str(ctx.get("user_id", "") or "").strip()
session_key = str(ctx.get("session_key", "") or "").strip()
conversation_id = str(ctx.get("conversation_id", "") or "").strip()
```

不要在 SQL 层生成兜底用户。

- [ ] **Step 3: 后台任务完成后发布 `subagent_result`**

```python
QueueEnvelope.new(
    topic="agent.inbound",
    event_type="subagent_result",
    user_id=user_id,
    session_key=session_key,
    conversation_id=conversation_id,
    source="subagent",
    payload={
        "job_id": job_id,
        "label": label,
        "task": task,
        "status": status,
        "exit_reason": exit_reason,
        "result": result,
        "retry_count": retry_count,
        "profile": profile,
    },
)
```

### Task 6: PostgreSQL outbox

**Files:**
- Create: `scripts/message_outbox_pg_schema.sql`
- Create: `bus/outbox_store.py`
- Create: `bus/outbox_dispatcher.py`
- Test: `tests/test_message_outbox_store.py`

**Interfaces:**
- Produces: `PostgresOutboxStore.enqueue(message)`
- Produces: `PostgresOutboxStore.claim_batch(limit)`
- Produces: `PostgresOutboxStore.mark_published(event_id)`
- Produces: `PostgresOutboxStore.mark_failed(event_id, error, retry=True)`

- [ ] **Step 1: 创建 SQL**

使用本文第 6 节 SQL。

- [ ] **Step 2: 实现 enqueue**

将 `QueueEnvelope` 转成 `message_outbox` 一行。

- [ ] **Step 3: 实现 claim**

使用 PostgreSQL 锁：

```sql
SELECT *
FROM message_outbox
WHERE status = 'pending'
  AND next_retry_at <= now()
ORDER BY created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT %s
```

- [ ] **Step 4: 测试并发 claim 不重复**

两个连接同时 claim，断言同一条消息不会被两个 dispatcher 拿到。

### Task 7: RabbitMQ dispatcher

**Files:**
- Create: `bus/backends/rabbitmq_message_queue.py`
- Modify: `bus/outbox_dispatcher.py`
- Modify: `requirements.txt`
- Test: `tests/test_rabbitmq_message_mapping.py`

**Interfaces:**
- Consumes: `QueueEnvelope`
- Produces: RabbitMQ exchange publish

- [ ] **Step 1: 增加依赖**

```text
aio-pika
```

- [ ] **Step 2: 实现 JSON 序列化**

```python
def envelope_to_json(message: QueueEnvelope) -> bytes:
    ...

def envelope_from_json(data: bytes) -> QueueEnvelope:
    ...
```

- [ ] **Step 3: 实现 publish**

```python
await exchange.publish(
    aio_pika.Message(
        body=envelope_to_json(message),
        content_type="application/json",
        message_id=message.event_id,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    ),
    routing_key=message.topic,
)
```

- [ ] **Step 4: 只测试 mapping，不强依赖本机 RabbitMQ**

第一批 CI 不要求 RabbitMQ 在线，只测 envelope serialization 和 routing key。

## 14. 推荐落地顺序

```text
1. QueueEnvelope
2. MessageQueueBackend 协议
3. AsyncioMessageQueue backend
4. 主动推送 dispatch 接 MessageQueue
5. Subagent result 接 MessageQueue
6. PostgreSQL outbox
7. RabbitMQ dispatcher
8. Web 被动链路改 queued turn
9. Telegram adapter 接 outbound consumer
```

现在最适合先做 1-5。  
6-7 是 MQ 工程化。  
8 是核心链路重构，最后做。

## 15. 简历说法

可以写成：

> 设计 Agent 统一消息总线，将被动消息、主动推送和 Subagent 结果回灌抽象为携带 user_id/session_key/turn_id 的 QueueEnvelope，先兼容进程内 asyncio.Queue，后续通过 PostgreSQL outbox + RabbitMQ 实现可靠投递、ACK、重试和死信队列，解耦协议网关与 AgentWorker，为多用户并发和横向扩展提供基础。

更工程化一点：

> 基于 Transactional Outbox Pattern 改造 Agent 消息投递链路，将 proactive push、Subagent completion 和用户入站消息统一接入 MessageQueue；消息按 user_id/session_key 隔离，并支持 RabbitMQ backend、consumer ACK、失败重试和 dead letter，提升多用户场景下消息链路的可靠性与可扩展性。

## 16. 注意事项

- 不要把 `event_bus.enqueue()` 当成业务 MQ，它是 observe/fanout。
- 不要让 LLM tool 直接知道 RabbitMQ。
- 不要在 SQL 层为缺失 `user_id` 生成随机兜底用户。
- 不要一开始就迁移被动链路，先迁 proactive 和 subagent。
- 不要把 SSE 当成 MQ；SSE 是浏览器连接，MQ 是后端进程间投递。
- 不要为了接 RabbitMQ 删除现有 `asyncio.Queue`，先作为 backend 保留。

