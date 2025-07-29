-- ============================================================================
-- LLM Infrastructure Tables
-- ============================================================================
--
-- Technical Implementation Module: LLM Resource Orchestration
--
-- This migration establishes the AI infrastructure for Sinex, including
-- LLM model management, prompt orchestration, and AI-generated content tracking.
--
-- Note: Embedding models and cache tables were moved to migration 2 to fix
-- dependency ordering issues with artifact tables.
--
-- ============================================================================
-- LLM Models Registry
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.llm_models (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    provider TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_version TEXT,
    capabilities TEXT[] NOT NULL DEFAULT '{}',
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

CREATE INDEX idx_llm_models_active ON core.llm_models(is_active, provider);

-- ============================================================================
-- Prompt Templates Registry
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.prompts (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    name TEXT NOT NULL UNIQUE,
    category TEXT NOT NULL,
    template TEXT NOT NULL,
    system_prompt TEXT,
    variables JSONB NOT NULL DEFAULT '{}',
    model_constraints JSONB,
    version INTEGER NOT NULL DEFAULT 1,
    is_active BOOLEAN NOT NULL DEFAULT true,
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_prompts_category ON core.prompts(category) WHERE is_active = true;
CREATE INDEX idx_prompts_name ON core.prompts(name) WHERE is_active = true;

-- ============================================================================
-- Prompt Execution History
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.prompt_executions (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    prompt_id ULID NOT NULL REFERENCES core.prompts(id),
    model_id ULID NOT NULL REFERENCES core.llm_models(id),
    input_variables JSONB NOT NULL,
    rendered_prompt TEXT NOT NULL,
    response TEXT NOT NULL,
    usage_stats JSONB,
    cost DECIMAL(10, 6),
    executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    execution_time_ms INTEGER,
    error TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX idx_prompt_executions_prompt ON core.prompt_executions(prompt_id);
CREATE INDEX idx_prompt_executions_model ON core.prompt_executions(model_id);
CREATE INDEX idx_prompt_executions_time ON core.prompt_executions(executed_at);


-- ============================================================================
-- AI-Generated Content
-- ============================================================================
CREATE TABLE IF NOT EXISTS core.ai_generated_content (
    id ULID PRIMARY KEY DEFAULT gen_ulid(),
    source_type TEXT NOT NULL CHECK (source_type IN ('artifact', 'event', 'entity', 'event_cluster')),
    source_id ULID NOT NULL,
    content_type TEXT NOT NULL,
    prompt_execution_id ULID REFERENCES core.prompt_executions(id),
    content TEXT NOT NULL,
    confidence_score FLOAT CHECK (confidence_score >= 0 AND confidence_score <= 1),
    metadata JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    is_current BOOLEAN NOT NULL DEFAULT true
);

CREATE INDEX idx_ai_content_source ON core.ai_generated_content(source_type, source_id) WHERE is_current = true;
CREATE INDEX idx_ai_content_type ON core.ai_generated_content(content_type) WHERE is_current = true;

-- ============================================================================
-- Triggers
-- ============================================================================
CREATE TRIGGER set_prompts_updated_at 
    BEFORE UPDATE ON core.prompts 
    FOR EACH ROW 
    EXECUTE FUNCTION set_current_timestamp();


-- ============================================================================
-- Comments
-- ============================================================================
COMMENT ON TABLE core.llm_models IS 'Registry of available LLM models and their capabilities';
COMMENT ON TABLE core.prompts IS 'Reusable prompt templates for various AI tasks';
COMMENT ON TABLE core.prompt_executions IS 'History of all LLM prompt executions';
COMMENT ON TABLE core.ai_generated_content IS 'AI-generated summaries, analyses, and extractions';