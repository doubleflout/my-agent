CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS memory_items (
    id                TEXT PRIMARY KEY,
    user_id           UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_key       TEXT NULL REFERENCES sessions(key) ON DELETE SET NULL,
    memory_type       TEXT NOT NULL,
    summary           TEXT NOT NULL,
    content_hash      TEXT NOT NULL,
    embedding         vector(1024),
    reinforcement     INTEGER NOT NULL DEFAULT 1,
    emotional_weight  INTEGER NOT NULL DEFAULT 0,
    extra_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_ref        TEXT,
    happened_at       TIMESTAMPTZ,
    status            TEXT NOT NULL DEFAULT 'active',
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_memory_items_user_hash_type
ON memory_items (user_id, content_hash, memory_type);

CREATE INDEX IF NOT EXISTS ix_memory_items_user_status
ON memory_items (user_id, status);

CREATE INDEX IF NOT EXISTS ix_memory_items_user_type_status
ON memory_items (user_id, memory_type, status);

CREATE INDEX IF NOT EXISTS ix_memory_items_user_session
ON memory_items (user_id, session_key);

CREATE INDEX IF NOT EXISTS ix_memory_items_user_happened
ON memory_items (user_id, happened_at DESC);

CREATE INDEX IF NOT EXISTS ix_memory_items_user_source_ref
ON memory_items (user_id, source_ref);

CREATE INDEX IF NOT EXISTS ix_memory_items_scope_channel
ON memory_items ((extra_json->>'scope_channel'));

CREATE INDEX IF NOT EXISTS ix_memory_items_scope_chat_id
ON memory_items ((extra_json->>'scope_chat_id'));

CREATE INDEX IF NOT EXISTS ix_memory_items_summary_fts
ON memory_items
USING GIN (to_tsvector('simple', coalesce(summary, '')));

CREATE INDEX IF NOT EXISTS ix_memory_items_embedding_hnsw
ON memory_items
USING hnsw (embedding vector_cosine_ops);

CREATE TABLE IF NOT EXISTS consolidation_events (
    user_id           UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    source_ref        TEXT NOT NULL,
    item_id           TEXT REFERENCES memory_items(id) ON DELETE SET NULL,
    session_key       TEXT NULL REFERENCES sessions(key) ON DELETE SET NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, source_ref)
);

CREATE INDEX IF NOT EXISTS ix_consolidation_events_user_created
ON consolidation_events (user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_consolidation_events_user_session
ON consolidation_events (user_id, session_key);

CREATE TABLE IF NOT EXISTS memory_replacements (
    id                BIGSERIAL PRIMARY KEY,
    user_id           UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_key       TEXT NULL REFERENCES sessions(key) ON DELETE SET NULL,
    old_item_id       TEXT NOT NULL,
    old_memory_type   TEXT NOT NULL,
    old_summary       TEXT NOT NULL,
    old_source_ref    TEXT,
    old_happened_at   TIMESTAMPTZ,
    old_extra_json    JSONB NOT NULL DEFAULT '{}'::jsonb,
    new_item_id       TEXT NOT NULL,
    new_memory_type   TEXT NOT NULL,
    new_summary       TEXT NOT NULL,
    new_source_ref    TEXT,
    new_happened_at   TIMESTAMPTZ,
    new_extra_json    JSONB NOT NULL DEFAULT '{}'::jsonb,
    relation_type     TEXT NOT NULL DEFAULT 'supersede',
    source_ref        TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_memory_replacements_user_old
ON memory_replacements (user_id, old_item_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_memory_replacements_user_new
ON memory_replacements (user_id, new_item_id, created_at DESC);

CREATE INDEX IF NOT EXISTS ix_memory_replacements_user_session
ON memory_replacements (user_id, session_key);
