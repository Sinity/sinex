-- Rollback LLM infrastructure
-- Migration: 00000000000009_create_llm_infrastructure.down.sql

-- Drop triggers
DROP TRIGGER IF EXISTS set_prompts_updated_at ON core.prompts;

-- Drop functions
-- (embedding cache function was moved to migration 2)

-- Drop indexes
DROP INDEX IF EXISTS idx_ai_content_type;
DROP INDEX IF EXISTS idx_ai_content_source;
DROP INDEX IF EXISTS idx_prompt_executions_time;
DROP INDEX IF EXISTS idx_prompt_executions_model;
DROP INDEX IF EXISTS idx_prompt_executions_prompt;
DROP INDEX IF EXISTS idx_prompts_name;
DROP INDEX IF EXISTS idx_prompts_category;
DROP INDEX IF EXISTS idx_llm_models_active;

-- Drop tables (order matters due to foreign keys)
DROP TABLE IF EXISTS core.ai_generated_content;
DROP TABLE IF EXISTS core.prompt_executions;
DROP TABLE IF EXISTS core.prompts;
DROP TABLE IF EXISTS core.llm_models;