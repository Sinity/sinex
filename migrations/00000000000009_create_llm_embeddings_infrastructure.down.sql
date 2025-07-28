-- Rollback LLM and embeddings infrastructure
-- Migration: 00000000000009_create_llm_embeddings_infrastructure.down.sql

-- Drop triggers
DROP TRIGGER IF EXISTS update_cache_on_use ON core.embedding_cache;
DROP TRIGGER IF EXISTS set_prompts_updated_at ON core.prompts;

-- Drop functions
DROP FUNCTION IF EXISTS update_embedding_cache_last_used();

-- Drop indexes
DROP INDEX IF EXISTS idx_ai_content_type;
DROP INDEX IF EXISTS idx_ai_content_source;
DROP INDEX IF EXISTS idx_embedding_cache_vector;
DROP INDEX IF EXISTS idx_embedding_cache_lru;
DROP INDEX IF EXISTS idx_embedding_cache_hash;
DROP INDEX IF EXISTS idx_embedding_models_active;
DROP INDEX IF EXISTS idx_prompt_executions_time;
DROP INDEX IF EXISTS idx_prompt_executions_model;
DROP INDEX IF EXISTS idx_prompt_executions_prompt;
DROP INDEX IF EXISTS idx_prompts_name;
DROP INDEX IF EXISTS idx_prompts_category;
DROP INDEX IF EXISTS idx_llm_models_active;

-- Drop tables (order matters due to foreign keys)
DROP TABLE IF EXISTS core.ai_generated_content;
DROP TABLE IF EXISTS core.embedding_cache;
DROP TABLE IF EXISTS core.prompt_executions;
DROP TABLE IF EXISTS core.prompts;
DROP TABLE IF EXISTS core.embedding_models;
DROP TABLE IF EXISTS core.llm_models;