-- ============================================================================
-- LLM and AI Infrastructure Tables
-- ============================================================================

-- LLM Models registry
CREATE TABLE IF NOT EXISTS core.llm_models (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    provider TEXT NOT NULL, -- 'openai', 'anthropic', 'local', etc.
    model_name TEXT NOT NULL, -- 'gpt-4', 'claude-3-opus', etc.
    model_version TEXT,
    capabilities TEXT[] NOT NULL DEFAULT '{}', -- ['chat', 'completion', 'embeddings', 'vision']
    context_window INTEGER,
    max_output_tokens INTEGER,
    cost_per_1k_input_tokens DECIMAL(10, 6),
    cost_per_1k_output_tokens DECIMAL(10, 6),
    is_active BOOLEAN NOT NULL DEFAULT true,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deprecated_at TIMESTAMPTZ,
    CONSTRAINT unique_provider_model UNIQUE(provider, model_name, model_version)
);

-- Prompt templates registry
CREATE TABLE IF NOT EXISTS core.prompts (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    name TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL, -- 'summarization', 'extraction', 'analysis', etc.
    template TEXT NOT NULL, -- Template with {{placeholders}}
    system_prompt TEXT,
    variables JSONB NOT NULL DEFAULT '{}', -- Expected variables and their types
    model_constraints JSONB, -- Constraints on which models can use this
    version INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT true,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Prompt execution history
CREATE TABLE IF NOT EXISTS core.prompt_executions (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    prompt_id ulid NOT NULL REFERENCES core.prompts(id),
    model_id ulid NOT NULL REFERENCES core.llm_models(id),
    input_variables JSONB NOT NULL,
    rendered_prompt TEXT NOT NULL,
    response TEXT NOT NULL,
    usage_stats JSONB, -- tokens used, processing time, etc.
    cost DECIMAL(10, 6),
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    execution_time_ms INTEGER,
    error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'
);

-- ============================================================================
-- Embeddings Infrastructure
-- ============================================================================

-- Embedding models registry
CREATE TABLE IF NOT EXISTS core.embedding_models (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    provider TEXT NOT NULL,
    model_name TEXT NOT NULL,
    dimensions INTEGER NOT NULL,
    max_input_tokens INTEGER,
    cost_per_1k_tokens DECIMAL(10, 6),
    is_active BOOLEAN NOT NULL DEFAULT true,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_embedding_model UNIQUE(provider, model_name)
);

-- Artifact embeddings
CREATE TABLE IF NOT EXISTS core.artifact_embeddings (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    artifact_id ulid NOT NULL REFERENCES core.artifacts(id) ON DELETE CASCADE,
    artifact_content_id ulid REFERENCES core.artifact_contents(id) ON DELETE CASCADE,
    embedding_model_id ulid NOT NULL REFERENCES core.embedding_models(id),
    chunk_index INTEGER NOT NULL DEFAULT 0, -- For documents split into chunks
    chunk_text TEXT NOT NULL, -- The actual text that was embedded
    embedding vector(1536) NOT NULL, -- pgvector type, 1536 dims for OpenAI ada-002
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_artifact_chunk_embedding UNIQUE(artifact_id, embedding_model_id, chunk_index)
);

-- Event embeddings
CREATE TABLE IF NOT EXISTS core.event_embeddings (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    event_id ulid NOT NULL REFERENCES raw.events(id) ON DELETE CASCADE,
    embedding_model_id ulid NOT NULL REFERENCES core.embedding_models(id),
    embedded_text TEXT NOT NULL, -- What was actually embedded (could be summary)
    embedding vector(1536) NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_event_embedding UNIQUE(event_id, embedding_model_id)
);

-- Embedding cache for deduplication
CREATE TABLE IF NOT EXISTS core.embedding_cache (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    text_hash TEXT NOT NULL, -- SHA256 of the text
    embedding_model_id ulid NOT NULL REFERENCES core.embedding_models(id),
    embedding vector(1536) NOT NULL,
    text_sample TEXT, -- First 1000 chars for debugging
    use_count INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT unique_text_model_embedding UNIQUE(text_hash, embedding_model_id)
);

-- ============================================================================
-- AI-Generated Content
-- ============================================================================

-- Generated summaries and analyses
CREATE TABLE IF NOT EXISTS core.ai_generated_content (
    id ulid PRIMARY KEY DEFAULT gen_ulid(),
    source_type TEXT NOT NULL CHECK (source_type IN ('artifact', 'event', 'entity', 'event_cluster')),
    source_id ulid NOT NULL, -- References the appropriate table based on source_type
    content_type TEXT NOT NULL, -- 'summary', 'analysis', 'extraction', 'narrative', etc.
    prompt_execution_id ulid REFERENCES core.prompt_executions(id),
    content TEXT NOT NULL,
    confidence_score FLOAT CHECK (confidence_score >= 0 AND confidence_score <= 1),
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_current BOOLEAN NOT NULL DEFAULT true -- Latest version for this source/type combo
);

-- ============================================================================
-- Indexes
-- ============================================================================

-- LLM indexes
CREATE INDEX idx_llm_models_active ON core.llm_models(is_active, provider);
CREATE INDEX idx_prompts_category ON core.prompts(category) WHERE is_active = true;
CREATE INDEX idx_prompts_name ON core.prompts(name) WHERE is_active = true;
CREATE INDEX idx_prompt_executions_prompt ON core.prompt_executions(prompt_id);
CREATE INDEX idx_prompt_executions_model ON core.prompt_executions(model_id);
CREATE INDEX idx_prompt_executions_time ON core.prompt_executions(executed_at);

-- Embedding indexes
CREATE INDEX idx_embedding_models_active ON core.embedding_models(is_active, provider);

-- Vector similarity indexes (for semantic search)
CREATE INDEX idx_artifact_embeddings_vector ON core.artifact_embeddings 
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX idx_event_embeddings_vector ON core.event_embeddings 
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

CREATE INDEX idx_embedding_cache_vector ON core.embedding_cache 
    USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = 100);

-- Regular indexes
CREATE INDEX idx_artifact_embeddings_artifact ON core.artifact_embeddings(artifact_id);
CREATE INDEX idx_event_embeddings_event ON core.event_embeddings(event_id);
CREATE INDEX idx_embedding_cache_hash ON core.embedding_cache(text_hash);
CREATE INDEX idx_embedding_cache_lru ON core.embedding_cache(last_used_at);

-- AI content indexes
CREATE INDEX idx_ai_content_source ON core.ai_generated_content(source_type, source_id) WHERE is_current = true;
CREATE INDEX idx_ai_content_type ON core.ai_generated_content(content_type) WHERE is_current = true;

-- ============================================================================
-- Triggers
-- ============================================================================

CREATE TRIGGER set_prompts_updated_at 
    BEFORE UPDATE ON core.prompts 
    FOR EACH ROW 
    EXECUTE FUNCTION core.set_updated_at_trigger_func_generic();

-- Update last_used_at on embedding cache hits
CREATE OR REPLACE FUNCTION update_embedding_cache_last_used()
RETURNS TRIGGER AS $$
BEGIN
    NEW.last_used_at = NOW();
    NEW.use_count = OLD.use_count + 1;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_cache_on_use 
    BEFORE UPDATE ON core.embedding_cache 
    FOR EACH ROW 
    WHEN (OLD.embedding IS NOT DISTINCT FROM NEW.embedding)
    EXECUTE FUNCTION update_embedding_cache_last_used();

-- ============================================================================
-- Comments
-- ============================================================================

COMMENT ON TABLE core.llm_models IS 'Registry of available LLM models and their capabilities';
COMMENT ON TABLE core.prompts IS 'Reusable prompt templates for various AI tasks';
COMMENT ON TABLE core.prompt_executions IS 'History of all LLM prompt executions';
COMMENT ON TABLE core.embedding_models IS 'Registry of embedding models for semantic search';
COMMENT ON TABLE core.artifact_embeddings IS 'Vector embeddings for knowledge artifacts';
COMMENT ON TABLE core.event_embeddings IS 'Vector embeddings for events';
COMMENT ON TABLE core.embedding_cache IS 'Cache to avoid re-computing embeddings for identical text';
COMMENT ON TABLE core.ai_generated_content IS 'AI-generated summaries, analyses, and extractions';