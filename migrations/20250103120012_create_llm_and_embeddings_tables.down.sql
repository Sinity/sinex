-- Drop triggers first
DROP TRIGGER IF EXISTS update_cache_on_use ON core.embedding_cache;
DROP TRIGGER IF EXISTS set_prompts_updated_at ON core.prompts;

-- Drop functions
DROP FUNCTION IF EXISTS update_embedding_cache_last_used();

-- Drop tables in reverse dependency order
DROP TABLE IF EXISTS core.ai_generated_content;
DROP TABLE IF EXISTS core.embedding_cache;
DROP TABLE IF EXISTS core.event_embeddings;
DROP TABLE IF EXISTS core.artifact_embeddings;
DROP TABLE IF EXISTS core.embedding_models;
DROP TABLE IF EXISTS core.prompt_executions;
DROP TABLE IF EXISTS core.prompts;
DROP TABLE IF EXISTS core.llm_models;