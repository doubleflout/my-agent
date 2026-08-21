# PostgreSQL-First Multi-User Backend Migration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将 Akashic Agent 从单 workspace/多 SQLite 状态库演进为多用户 PostgreSQL 后端服务，优先迁移非向量状态表，向量相关表暂不迁移。

**Architecture:** 保留 `AgentLoop.process_direct()`、Phase 生命周期、工具系统、插件系统和现有向量记忆检索；把会话、消息、主动任务状态、trace/observe、consolidation 幂等索引、Web 用户和 Turn 状态迁到本地 PostgreSQL。多用户隔离采用 `users.id` + `sessions.session_key` 双层模型：产品/权限层用 `user_id`，Agent runtime/记忆/主动任务链路继续用 `session_key = web:{user_id}:{conversation_id}`。

**Tech Stack:** PostgreSQL 5432, SQLAlchemy, Alembic, psycopg3, FastAPI, JWT, Redis, asyncio, SSE, OpenTelemetry/Prometheus.

## Global Constraints

- PostgreSQL 使用本地 `localhost:5432`。
- 推荐开发库连接串：`postgresql+psycopg://akashic:akashic@localhost:5432/akashic_agent`。
- 不再新增新的业务 SQLite 状态库。
- 有向量检索依赖的表先不迁移：`memory2.db`、`memory_items.embedding`、`vec_items`、sqlite-vec/vec0 相关内部表。
- Web 用户身份只能从 JWT 得到，前端/API 请求体不能传 `user_id`。
- Web Agent 会话隔离键固定为 `web:{user_id}:{conversation_id}`。
- Dashboard 仍然是管理/观测入口，但底层读取 PostgreSQL。
- 第一阶段不做 Telegram/QQ 与 Web 账号绑定；非 Web channel 的 `user_id` 可以为空。

---

## Existing Database Inventory

### 1. `sessions.db`

**Code:** `session/store.py`

Current tables:

- `sessions`
- `messages`
- `messages_fts` and FTS triggers

Migration decision:

- 必须迁移到 PostgreSQL。
- `sessions` 增加 `user_id NULL`。
- `sessions.key` 保留为 `session_key` 主键或唯一键。
- `messages` 使用 `session_key` 外键关联 `sessions.session_key`。
- `messages` 可冗余 `user_id NULL`，用于多用户查询提速和权限过滤。
- FTS 不迁 SQLite FTS5；PostgreSQL 使用 `tsvector` 或先用 `ILIKE` 兼容。

### 2. `proactive.db`

**Code:** `proactive_v2/state.py`

Current tables:

- `seen_items`
- `deliveries`
- `rejection_cooldown`
- `semantic_items`
- `kv_state`
- `session_state`
- `context_only_timestamps`
- `tick_log`
- `tick_step_log`

Migration decision:

- 全部迁移到 PostgreSQL。
- 带用户会话语义的表使用 `session_key` 外键：`deliveries`、`session_state`、`context_only_timestamps`、`tick_log`。
- `tick_step_log` 通过 `tick_id` 外键关联 `tick_log.tick_id`，不需要单独加 `user_id`。
- 全局去重表 `seen_items`、`rejection_cooldown`、`semantic_items` 第一阶段保持全局；如果后续不同用户需要独立去重，再加 `user_id` 或 `tenant_id`。
- `kv_state` 保持全局配置状态；如果 key 是 session 维度，迁入 `session_state`。

### 3. `memory/consolidation_writes.db`

**Code:** `agent/memory.py`

Current table:

- `consolidation_writes`

Migration decision:

- 迁移到 PostgreSQL。
- 这个表是 Markdown 写入幂等索引，本身用 `source_ref + kind` 去重。
- 增加 `session_key NULL`，用于后续按会话清理/审计。
- 不强制加 `user_id`，因为旧 channel 没有用户系统；Web 场景可从 `session_key` 解析 user。

### 4. `observe/observe.db`

**Code:** `plugins/observe/db.py`

Current tables:

- `turns`
- `rag_queries`
- `memory_writes`

Migration decision:

- 迁移到 PostgreSQL。
- trace 类表必须带 `session_key`。
- 可以冗余 `user_id NULL`，方便按用户筛选 dashboard。
- `rag_queries`、`memory_writes` 通过 `turn_id` 外键关联 `observe_turns.id`。

### 5. `memory/memory2.db`

**Code:** `memory2/store.py`

Current tables:

- `memory_items`
- `consolidation_events`
- `memory_replacements`
- `vec_items` via sqlite-vec/vec0

Migration decision:

- 第一阶段不迁移。
- 原因：`memory_items.embedding`、`vec_items`、sqlite-vec 查询强绑定；直接迁 PostgreSQL 会引入 pgvector、embedding 类型、召回 SQL 重写和效果回归风险。
- 现阶段只要求所有 Web 调用传入隔离后的 `session_key`，让 `extra_json.scope_channel/scope_chat_id` 继续生效。

### 6. `schedules.json`

**Code:** scheduler/toolset related

Migration decision:

- 如果目标是“所有状态统一到 PostgreSQL”，则迁移。
- 新表 `schedules` 使用 `session_key` 作为会话归属，`user_id NULL` 作为 Web 用户冗余字段。

---

## user_id vs session_key Rule

核心判断：

- `user_id` 表示产品账号、权限、计费、限流、API ownership。
- `session_key` 表示 Agent runtime 的上下文隔离、记忆召回范围、主动任务运行范围。

因此：

| Table | Primary isolation | Add `user_id`? | Add `session_key`? | Reason |
| --- | --- | --- | --- | --- |
| `users` | `user_id` | yes, primary | no | 产品账号根表 |
| `conversations` | `user_id` | yes | generate `session_key` | Web 会话归属 |
| `agent_turns` | `user_id` + `conversation_id` | yes | yes | API 权限 + Agent 执行上下文 |
| `sessions` | `session_key` | nullable | yes, primary | Agent runtime 原生按 session 隔离 |
| `messages` | `session_key` | nullable denormalized | yes | 原有消息属于 session |
| `deliveries` | `session_key` | no | yes | 主动推送按会话去重 |
| `session_state` | `session_key` | no | yes | 主动消息会话状态 |
| `context_only_timestamps` | `session_key` | no | yes | 会话级频控 |
| `tick_log` | `session_key` | nullable denormalized | yes | 主动 tick trace |
| `tick_step_log` | `tick_id` | no | via tick_log | tick 子步骤 |
| `seen_items` | global/source | no initially | no | 信息源全局去重 |
| `rejection_cooldown` | global/source | no initially | no | 信息源全局冷却 |
| `semantic_items` | global/source | no initially | no | 非向量候选内容缓存 |
| `kv_state` | global | no | no | 全局运行状态 |
| `consolidation_writes` | source_ref + kind | no | nullable | 幂等写入索引 |
| `observe_turns` | `turn_id` | nullable | yes | trace 根事件 |
| `rag_queries` | `turn_id` | no | via observe_turns | trace 子事件 |
| `memory_writes` | `turn_id` | no | via observe_turns | trace 子事件 |
| `schedules` | `session_key` | nullable | yes | 定时任务归属 |

---

## Target PostgreSQL Schema

### Identity and Web API Tables

```sql
CREATE TABLE users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name TEXT,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE conversations (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_key TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_conversations_user_updated
ON conversations(user_id, updated_at DESC);

CREATE TABLE agent_turns (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    session_key TEXT NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX ix_agent_turns_user_status
ON agent_turns(user_id, status, created_at DESC);
```

### Runtime Session Tables

```sql
CREATE TABLE sessions (
    session_key TEXT PRIMARY KEY,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    channel TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    last_consolidated INTEGER NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_user_at TIMESTAMPTZ,
    last_proactive_at TIMESTAMPTZ,
    next_seq INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX ix_sessions_user_updated
ON sessions(user_id, updated_at DESC);

CREATE INDEX ix_sessions_channel_chat
ON sessions(channel, chat_id);

CREATE TABLE messages (
    id TEXT PRIMARY KEY,
    session_key TEXT NOT NULL REFERENCES sessions(session_key) ON DELETE CASCADE,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    seq INTEGER NOT NULL,
    role TEXT NOT NULL,
    content TEXT,
    tool_chain JSONB,
    extra JSONB,
    ts TIMESTAMPTZ NOT NULL,
    UNIQUE(session_key, seq)
);

CREATE INDEX ix_messages_session_seq
ON messages(session_key, seq);

CREATE INDEX ix_messages_user_ts
ON messages(user_id, ts DESC);
```

### Proactive State Tables

```sql
CREATE TABLE seen_items (
    source_key TEXT NOT NULL,
    item_id TEXT NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_key, item_id)
);

CREATE TABLE deliveries (
    session_key TEXT NOT NULL REFERENCES sessions(session_key) ON DELETE CASCADE,
    delivery_key TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_key, delivery_key)
);

CREATE INDEX ix_deliveries_session_sent
ON deliveries(session_key, sent_at);

CREATE TABLE rejection_cooldown (
    source_key TEXT NOT NULL,
    item_id TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_key, item_id)
);

CREATE TABLE semantic_items (
    id BIGSERIAL PRIMARY KEY,
    source_key TEXT NOT NULL,
    item_id TEXT NOT NULL,
    text TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL
);

CREATE INDEX ix_semantic_items_ts
ON semantic_items(ts);

CREATE TABLE kv_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE session_state (
    session_key TEXT NOT NULL REFERENCES sessions(session_key) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (session_key, key)
);

CREATE TABLE context_only_timestamps (
    id BIGSERIAL PRIMARY KEY,
    session_key TEXT NOT NULL REFERENCES sessions(session_key) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL
);

CREATE INDEX ix_context_only_session_ts
ON context_only_timestamps(session_key, ts);

CREATE TABLE tick_log (
    id BIGSERIAL PRIMARY KEY,
    tick_id TEXT NOT NULL UNIQUE,
    session_key TEXT NOT NULL REFERENCES sessions(session_key) ON DELETE CASCADE,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    gate_exit TEXT,
    terminal_action TEXT,
    skip_reason TEXT,
    steps_taken INTEGER,
    alert_count INTEGER,
    content_count INTEGER,
    context_count INTEGER,
    interesting_ids JSONB,
    discarded_ids JSONB,
    cited_ids JSONB,
    drift_entered BOOLEAN NOT NULL DEFAULT FALSE,
    final_message TEXT
);

CREATE INDEX ix_tick_log_session_started
ON tick_log(session_key, started_at);

CREATE TABLE tick_step_log (
    id BIGSERIAL PRIMARY KEY,
    tick_id TEXT NOT NULL REFERENCES tick_log(tick_id) ON DELETE CASCADE,
    step_index INTEGER NOT NULL,
    phase TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    tool_call_id TEXT NOT NULL,
    tool_args_json JSONB NOT NULL,
    tool_result_text TEXT NOT NULL,
    terminal_action_after TEXT,
    skip_reason_after TEXT,
    interesting_ids_after JSONB NOT NULL,
    discarded_ids_after JSONB NOT NULL,
    cited_ids_after JSONB NOT NULL,
    final_message_after TEXT NOT NULL
);

CREATE INDEX ix_tick_step_log_tick_step
ON tick_step_log(tick_id, step_index);
```

### Consolidation and Observe Tables

```sql
CREATE TABLE consolidation_writes (
    source_ref TEXT NOT NULL,
    kind TEXT NOT NULL,
    session_key TEXT NULL REFERENCES sessions(session_key) ON DELETE SET NULL,
    payload TEXT,
    trailing_blank_line BOOLEAN NOT NULL DEFAULT FALSE,
    done_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_ref, kind)
);

CREATE TABLE observe_turns (
    id TEXT PRIMARY KEY,
    session_key TEXT NOT NULL REFERENCES sessions(session_key) ON DELETE CASCADE,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    input TEXT,
    output TEXT,
    error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE observe_rag_queries (
    id BIGSERIAL PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES observe_turns(id) ON DELETE CASCADE,
    query TEXT NOT NULL,
    result_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE observe_memory_writes (
    id BIGSERIAL PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES observe_turns(id) ON DELETE CASCADE,
    memory_type TEXT NOT NULL,
    summary TEXT NOT NULL,
    source_ref TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### Schedules

```sql
CREATE TABLE schedules (
    id UUID PRIMARY KEY,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    session_key TEXT NOT NULL REFERENCES sessions(session_key) ON DELETE CASCADE,
    name TEXT NOT NULL,
    spec_json JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ix_schedules_session_enabled
ON schedules(session_key, enabled);
```

---

## Implementation Tasks

### Task 1: Add PostgreSQL Configuration

**Files:**

- Modify: `requirements.txt`
- Create: `core/db/postgres.py`
- Modify: `config.example.toml`

**Plan:**

- [ ] Add `psycopg[binary]` and `alembic`.
- [ ] Add env/config key `AKASHIC_DATABASE_URL`.
- [ ] Default local DSN:

```text
postgresql+psycopg://akashic:akashic@localhost:5432/akashic_agent
```

- [ ] Add `create_postgres_engine(database_url: str) -> Engine`.

### Task 2: Add Alembic Migration for Non-Vector Tables

**Files:**

- Create: `alembic.ini`
- Create: `migrations/env.py`
- Create: `migrations/versions/0001_postgres_runtime_state.py`

**Plan:**

- [ ] Create all target tables listed above.
- [ ] Do not create `memory_items`, `vec_items`, or pgvector extension.
- [ ] Use `JSONB` for metadata/tool/extra fields.
- [ ] Use `TIMESTAMPTZ` for time columns.

### Task 3: Build PostgreSQL SessionStore Compatible With Existing Interface

**Files:**

- Create: `session/postgres_store.py`
- Modify: `bootstrap/tools.py`
- Modify: `bootstrap/dashboard_api.py`
- Test: `tests/test_session_postgres_store.py`

**Plan:**

- [ ] Implement the same public methods used from `session/store.py`.
- [ ] Keep method signatures stable, so AgentLoop and dashboard code do not change broadly.
- [ ] Map SQLite `key` to PostgreSQL `session_key`.
- [ ] Parse `channel` and `chat_id` from `session_key` for old channels.
- [ ] For Web sessions, set `user_id` from `web:{user_id}:{conversation_id}`.

### Task 4: Build PostgreSQL ProactiveStateStore Compatible With Existing Interface

**Files:**

- Create: `proactive_v2/postgres_state.py`
- Modify: `bootstrap/proactive.py`
- Modify: `bootstrap/dashboard_api.py`
- Test: `tests/test_proactive_postgres_state.py`

**Plan:**

- [ ] Port `ProactiveStateStore` methods one by one.
- [ ] Keep `source_key/item_id` global tables global.
- [ ] Keep session-scoped tables keyed by `session_key`.
- [ ] Convert SQLite `INSERT ... ON CONFLICT` to PostgreSQL `ON CONFLICT`.
- [ ] Convert JSON string list columns in tick logs to `JSONB`.

### Task 5: Move Consolidation Writes Index to PostgreSQL

**Files:**

- Create: `agent/consolidation_index.py`
- Modify: `agent/memory.py`
- Test: `tests/test_consolidation_postgres_index.py`

**Plan:**

- [ ] Extract current `consolidation_writes` access behind an interface.
- [ ] Implement PostgreSQL-backed `ConsolidationWriteIndex`.
- [ ] Preserve idempotency: primary key remains `(source_ref, kind)`.
- [ ] Keep Markdown files themselves on disk for now.

### Task 6: Move Observe Plugin DB to PostgreSQL

**Files:**

- Create: `plugins/observe/postgres_db.py`
- Modify: `plugins/observe/plugin.py`
- Modify: `plugins/status_commands/dashboard.py`
- Test: `tests/test_observe_postgres_writer.py`

**Plan:**

- [ ] Replace `observe.db` writer with repository interface.
- [ ] Store root turn records in `observe_turns`.
- [ ] Store RAG and memory write child records by `turn_id`.
- [ ] Add dashboard query compatibility.

### Task 7: Migrate `schedules.json` to PostgreSQL

**Files:**

- Create: `agent/scheduler_postgres_store.py`
- Modify: `bootstrap/toolsets/schedule.py`
- Test: `tests/test_scheduler_postgres_store.py`

**Plan:**

- [ ] Store schedules in `schedules`.
- [ ] Keep schedule payload in `spec_json`.
- [ ] Use `session_key` for ownership.
- [ ] Set nullable `user_id` when the session key is Web.

### Task 8: Keep Memory2 on SQLite and Document the Boundary

**Files:**

- Modify: `memory2/store.py`
- Create: `docs/postgres-migration.md`
- Test: `tests/test_memory2_still_sqlite.py`

**Plan:**

- [ ] Keep `MemoryStore2(workspace / "memory" / "memory2.db")`.
- [ ] Do not change vector search SQL.
- [ ] Add a clear comment that memory2 migrates later with pgvector.
- [ ] Verify Agent recall still works with PostgreSQL sessions and SQLite memory2.

### Task 9: Data Migration Script From Existing Workspace

**Files:**

- Create: `scripts/migrate_workspace_to_postgres.py`
- Test: `tests/test_migrate_workspace_to_postgres.py`

**Plan:**

- [ ] Read `workspace/sessions.db` and insert into PostgreSQL `sessions/messages`.
- [ ] Read `workspace/proactive.db` and insert proactive tables.
- [ ] Read `workspace/memory/consolidation_writes.db` and insert `consolidation_writes`.
- [ ] Read `workspace/observe/observe.db` and insert observe tables if present.
- [ ] Read `workspace/schedules.json` and insert `schedules`.
- [ ] Skip `workspace/memory/memory2.db`.

### Task 10: Runtime Switch and Rollback

**Files:**

- Modify: `bootstrap/tools.py`
- Modify: `bootstrap/proactive.py`
- Modify: `bootstrap/dashboard_api.py`
- Modify: `main.py`

**Plan:**

- [ ] Add config flag `storage.backend = "postgres" | "sqlite"`.
- [ ] Default new Web backend to PostgreSQL.
- [ ] Keep SQLite fallback for emergency rollback during migration.
- [ ] Add startup check that PostgreSQL is reachable on 5432 when backend is postgres.

---

## Minimal Migration Order

1. Add PostgreSQL config and Alembic migration.
2. Migrate `SessionStore` first, because AgentLoop、dashboard、presence、message lookup 都依赖它。
3. Migrate Web product tables together with sessions, so `users -> conversations -> sessions -> messages -> agent_turns` 能形成完整链路。
4. Migrate `proactive.db` after sessions, because proactive tables mostly reference `session_key`.
5. Migrate `consolidation_writes` and `observe` after主链路稳定。
6. Migrate `schedules.json` last。
7. Keep `memory2.db` untouched until pgvector phase。

## Resume Framing

```text
主导 Agent 后端存储层 PostgreSQL 化改造：梳理 sessions、messages、proactive tick、observe trace、consolidation 幂等索引和 schedule 等多类 SQLite/JSON 状态表，按 user_id 与 session_key 划分产品权限和 Agent 上下文边界，设计 PostgreSQL 关系模型、Alembic 迁移和兼容型 Store 接口；在保留 sqlite-vec 向量记忆链路的前提下，完成多用户 API 服务的数据隔离与后续横向扩展基础。
```
