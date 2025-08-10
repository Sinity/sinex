use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // LLM Models Registry
        manager
            .get_connection()
            .execute_unprepared(
                r#"
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
                "#,
            )
            .await?;

        // LLM Prompts Library
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS core.llm_prompts (
                    id ULID PRIMARY KEY DEFAULT gen_ulid(),
                    name TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    category TEXT NOT NULL,
                    template TEXT NOT NULL,
                    input_schema JSONB,
                    output_schema JSONB,
                    model_constraints JSONB,
                    is_active BOOLEAN NOT NULL DEFAULT true,
                    metadata JSONB NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT unique_prompt_version UNIQUE(name, version)
                );
                "#,
            )
            .await?;

        // LLM Interactions Log
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS core.llm_interactions (
                    id ULID PRIMARY KEY DEFAULT gen_ulid(),
                    model_id ULID NOT NULL REFERENCES core.llm_models(id),
                    prompt_id ULID REFERENCES core.llm_prompts(id),
                    request_payload JSONB NOT NULL,
                    response_payload JSONB NOT NULL,
                    input_tokens INTEGER,
                    output_tokens INTEGER,
                    total_cost DECIMAL(10, 6),
                    duration_ms INTEGER,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    metadata JSONB NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                "#,
            )
            .await?;

        // LLM Generated Content
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS core.llm_generated_content (
                    id ULID PRIMARY KEY DEFAULT gen_ulid(),
                    interaction_id ULID NOT NULL REFERENCES core.llm_interactions(id),
                    content_type TEXT NOT NULL,
                    content TEXT NOT NULL,
                    confidence_score DECIMAL(3, 2),
                    validation_status TEXT,
                    validation_errors JSONB,
                    metadata JSONB NOT NULL DEFAULT '{}',
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                "#,
            )
            .await?;

        // LLM Context Windows
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE TABLE IF NOT EXISTS core.llm_context_windows (
                    id ULID PRIMARY KEY DEFAULT gen_ulid(),
                    session_id UUID NOT NULL,
                    window_content JSONB NOT NULL,
                    token_count INTEGER NOT NULL,
                    model_id ULID NOT NULL REFERENCES core.llm_models(id),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL
                );
                "#,
            )
            .await?;

        // Create indexes
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE INDEX idx_llm_models_active ON core.llm_models (provider, model_name) WHERE is_active = true;
                CREATE INDEX idx_llm_prompts_active ON core.llm_prompts (category, name) WHERE is_active = true;
                CREATE INDEX idx_llm_interactions_model_created ON core.llm_interactions (model_id, created_at);
                CREATE INDEX idx_llm_interactions_prompt_created ON core.llm_interactions (prompt_id, created_at);
                CREATE INDEX idx_llm_generated_content_interaction ON core.llm_generated_content (interaction_id);
                CREATE INDEX idx_llm_context_windows_session ON core.llm_context_windows (session_id, created_at);
                "#
            )
            .await?;

        // Add check constraints
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.llm_interactions
                ADD CONSTRAINT check_interaction_status 
                CHECK (status IN ('pending', 'success', 'failure', 'timeout', 'cancelled'));

                ALTER TABLE core.llm_generated_content
                ADD CONSTRAINT check_content_type 
                CHECK (content_type IN ('text', 'code', 'json', 'markdown', 'html', 'sql', 'other'));

                ALTER TABLE core.llm_generated_content
                ADD CONSTRAINT check_validation_status 
                CHECK (validation_status IN ('pending', 'valid', 'invalid', 'partial', 'skipped'));
                "#
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Drop tables in reverse order
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                DROP TABLE IF EXISTS core.llm_context_windows;
                DROP TABLE IF EXISTS core.llm_generated_content;
                DROP TABLE IF EXISTS core.llm_interactions;
                DROP TABLE IF EXISTS core.llm_prompts;
                DROP TABLE IF EXISTS core.llm_models;
                "#,
            )
            .await?;

        Ok(())
    }
}
