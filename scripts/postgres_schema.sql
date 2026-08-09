-- PostgreSQL schema for Akashic multi-user state.
-- This script creates new PostgreSQL tables only. It does not modify existing
-- SQLite databases or memory2 vector tables.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name TEXT,
    disabled BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS conversations (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_key TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_conversations_user_updated
ON conversations(user_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS sessions (
    key TEXT PRIMARY KEY,
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

CREATE INDEX IF NOT EXISTS ix_sessions_user_updated
ON sessions(user_id, updated_at DESC);

CREATE INDEX IF NOT EXISTS ix_sessions_channel_chat
ON sessions(channel, chat_id);

CREATE INDEX IF NOT EXISTS ix_sessions_key
ON sessions(key);

CREATE TABLE IF NOT EXISTS messages (
    id TEXT PRIMARY KEY,
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    seq INTEGER NOT NULL,
    role TEXT NOT NULL,
    content TEXT,
    tool_chain JSONB,
    extra JSONB NOT NULL DEFAULT '{}'::jsonb,
    ts TIMESTAMPTZ NOT NULL,
    UNIQUE(session_key, seq)
);

CREATE INDEX IF NOT EXISTS ix_messages_session_seq
ON messages(session_key, seq);

CREATE INDEX IF NOT EXISTS ix_messages_user_ts
ON messages(user_id, ts DESC);

CREATE TABLE IF NOT EXISTS chat_messages (
    id UUID PRIMARY KEY,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    content TEXT NOT NULL,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_chat_messages_conversation_created
ON chat_messages(conversation_id, created_at);

CREATE INDEX IF NOT EXISTS ix_chat_messages_user_created
ON chat_messages(user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS agent_turns (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    conversation_id UUID NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    status TEXT NOT NULL,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_agent_turns_user_status
ON agent_turns(user_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS proactive_sessions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    conversation_id UUID NOT NULL UNIQUE REFERENCES conversations(id) ON DELETE CASCADE,
    session_key TEXT NOT NULL UNIQUE REFERENCES sessions(key) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    last_tick_at TIMESTAMPTZ,
    next_tick_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    interval_seconds INTEGER NOT NULL DEFAULT 4800,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_proactive_sessions_due
ON proactive_sessions(enabled, next_tick_at);

CREATE INDEX IF NOT EXISTS ix_proactive_sessions_user
ON proactive_sessions(user_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS seen_items (
    source_key TEXT NOT NULL,
    item_id TEXT NOT NULL,
    seen_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_key, item_id)
);

CREATE TABLE IF NOT EXISTS deliveries (
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    delivery_key TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_key, delivery_key)
);

CREATE INDEX IF NOT EXISTS ix_deliveries_session_sent
ON deliveries(session_key, sent_at);

CREATE TABLE IF NOT EXISTS rejection_cooldown (
    source_key TEXT NOT NULL,
    item_id TEXT NOT NULL,
    rejected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (source_key, item_id)
);

CREATE TABLE IF NOT EXISTS semantic_items (
    id BIGSERIAL PRIMARY KEY,
    source_key TEXT NOT NULL,
    item_id TEXT NOT NULL,
    text TEXT NOT NULL,
    ts TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_semantic_items_source_item_ts
ON semantic_items(source_key, item_id, ts);

CREATE INDEX IF NOT EXISTS ix_semantic_items_ts
ON semantic_items(ts);

CREATE TABLE IF NOT EXISTS kv_state (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS session_state (
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    key TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (session_key, key)
);

CREATE TABLE IF NOT EXISTS context_only_timestamps (
    id BIGSERIAL PRIMARY KEY,
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    ts TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_context_only_session_ts
ON context_only_timestamps(session_key, ts);

CREATE INDEX IF NOT EXISTS ix_context_only_session_ts
ON context_only_timestamps(session_key, ts);

CREATE TABLE IF NOT EXISTS tick_log (
    id BIGSERIAL PRIMARY KEY,
    tick_id TEXT NOT NULL UNIQUE,
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
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

CREATE INDEX IF NOT EXISTS ix_tick_log_session_started
ON tick_log(session_key, started_at);

CREATE TABLE IF NOT EXISTS tick_step_log (
    id BIGSERIAL PRIMARY KEY,
    tick_id TEXT NOT NULL REFERENCES tick_log(tick_id) ON DELETE CASCADE,
    step_index INTEGER NOT NULL,
    phase TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    tool_call_id TEXT NOT NULL,
    tool_args_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    tool_result_text TEXT NOT NULL,
    terminal_action_after TEXT,
    skip_reason_after TEXT,
    interesting_ids_after JSONB NOT NULL DEFAULT '[]'::jsonb,
    discarded_ids_after JSONB NOT NULL DEFAULT '[]'::jsonb,
    cited_ids_after JSONB NOT NULL DEFAULT '[]'::jsonb,
    final_message_after TEXT NOT NULL,
    UNIQUE(tick_id, step_index, tool_call_id)
);

CREATE INDEX IF NOT EXISTS ix_tick_step_log_tick_step
ON tick_step_log(tick_id, step_index);

CREATE TABLE IF NOT EXISTS consolidation_writes (
    source_ref TEXT NOT NULL,
    kind TEXT NOT NULL,
    session_key TEXT NULL REFERENCES sessions(key) ON DELETE SET NULL,
    payload TEXT,
    trailing_blank_line BOOLEAN NOT NULL DEFAULT FALSE,
    done_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (source_ref, kind)
);

CREATE TABLE IF NOT EXISTS turns (
    id BIGINT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    source TEXT NOT NULL,
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    user_msg TEXT,
    llm_output TEXT NOT NULL DEFAULT '',
    raw_llm_output TEXT,
    meme_tag TEXT,
    meme_media_count INTEGER,
    tool_calls JSONB,
    tool_chain_json JSONB,
    history_window INTEGER,
    history_messages INTEGER,
    history_chars INTEGER,
    history_tokens INTEGER,
    prompt_tokens INTEGER,
    next_turn_baseline_tokens INTEGER,
    react_iteration_count INTEGER,
    react_input_sum_tokens INTEGER,
    react_input_peak_tokens INTEGER,
    react_final_input_tokens INTEGER,
    react_cache_prompt_tokens INTEGER,
    react_cache_hit_tokens INTEGER,
    error TEXT
);

CREATE INDEX IF NOT EXISTS ix_turns_sk_ts
ON turns(session_key, ts);

CREATE INDEX IF NOT EXISTS ix_turns_source
ON turns(source, ts);

CREATE TABLE IF NOT EXISTS rag_queries (
    id BIGINT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    caller TEXT NOT NULL,
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    query TEXT NOT NULL,
    orig_query TEXT,
    aux_queries JSONB,
    hits_json JSONB,
    injected_count INTEGER NOT NULL DEFAULT 0,
    route_decision TEXT,
    error TEXT
);

CREATE INDEX IF NOT EXISTS ix_rq_sk_ts
ON rag_queries(session_key, ts);

CREATE INDEX IF NOT EXISTS ix_rq_caller
ON rag_queries(caller, ts);

CREATE TABLE IF NOT EXISTS memory_writes (
    id BIGINT PRIMARY KEY,
    ts TIMESTAMPTZ NOT NULL,
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    source_ref TEXT,
    action TEXT NOT NULL,
    memory_type TEXT,
    item_id TEXT,
    summary TEXT,
    superseded_ids JSONB,
    error TEXT
);

CREATE INDEX IF NOT EXISTS ix_mw_sk_ts
ON memory_writes(session_key, ts);

CREATE INDEX IF NOT EXISTS ix_mw_action
ON memory_writes(action, ts);

CREATE TABLE IF NOT EXISTS schedules (
    id UUID PRIMARY KEY,
    user_id UUID NULL REFERENCES users(id) ON DELETE SET NULL,
    session_key TEXT NOT NULL REFERENCES sessions(key) ON DELETE CASCADE,
    name TEXT NOT NULL,
    spec_json JSONB NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_schedules_session_enabled
ON schedules(session_key, enabled);
