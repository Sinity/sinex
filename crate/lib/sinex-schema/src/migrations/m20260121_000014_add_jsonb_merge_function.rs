use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        // Create the jsonb_merge_deep function
        // Source: Based on common PL/pgSQL implementations for deep merging JSONB
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                CREATE OR REPLACE FUNCTION core.jsonb_merge_deep(a jsonb, b jsonb)
                RETURNS jsonb LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
                    SELECT CASE
                        WHEN a IS NULL THEN b
                        WHEN b IS NULL THEN a
                        WHEN jsonb_typeof(a) = 'object' AND jsonb_typeof(b) = 'object' THEN
                            (
                                SELECT
                                    jsonb_object_agg(
                                        k,
                                        CASE
                                            WHEN e2.value IS NULL THEN e1.value
                                            WHEN e1.value IS NULL THEN e2.value
                                            ELSE core.jsonb_merge_deep(e1.value, e2.value)
                                        END
                                    )
                                FROM jsonb_each(a) e1(k, value)
                                FULL JOIN jsonb_each(b) e2(k, value) USING (k)
                            )
                        ELSE b
                    END
                $$;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared("DROP FUNCTION IF EXISTS core.jsonb_merge_deep(jsonb, jsonb);")
            .await?;

        Ok(())
    }
}
