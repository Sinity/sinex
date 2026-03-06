//! Rename `core.events.ts_ingest` to `ts_coided`, normalize the supporting index,
//! and remove the legacy `id DEFAULT uuidv7()` safety net.

use sea_orm_migration::prelude::*;

#[derive(DeriveMigrationName)]
pub(crate) struct Migration;

#[async_trait::async_trait]
impl MigrationTrait for Migration {
    async fn up(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.events
                  ALTER COLUMN id DROP DEFAULT;

                DO $$
                BEGIN
                  IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'core'
                      AND table_name = 'events'
                      AND column_name = 'ts_ingest'
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'core'
                      AND table_name = 'events'
                      AND column_name = 'ts_coided'
                  ) THEN
                    ALTER TABLE core.events RENAME COLUMN ts_ingest TO ts_coided;
                  END IF;
                END $$;

                DO $$
                BEGIN
                  IF to_regclass('core.ix_events_ts_coided') IS NULL THEN
                    IF to_regclass('core.ix_events_ts_ingest') IS NOT NULL THEN
                      ALTER INDEX core.ix_events_ts_ingest RENAME TO ix_events_ts_coided;
                    ELSE
                      BEGIN
                        CREATE INDEX IF NOT EXISTS ix_events_ts_coided
                          ON core.events (ts_coided DESC);
                      EXCEPTION
                        WHEN feature_not_supported THEN
                          NULL;
                      END;
                    END IF;
                  END IF;
                END $$;
                "#,
            )
            .await?;

        Ok(())
    }

    async fn down(&self, manager: &SchemaManager) -> Result<(), DbErr> {
        manager
            .get_connection()
            .execute_unprepared(
                r#"
                ALTER TABLE core.events
                  ALTER COLUMN id SET DEFAULT uuidv7();

                DO $$
                BEGIN
                  IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'core'
                      AND table_name = 'events'
                      AND column_name = 'ts_coided'
                  )
                  AND NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'core'
                      AND table_name = 'events'
                      AND column_name = 'ts_ingest'
                  ) THEN
                    ALTER TABLE core.events RENAME COLUMN ts_coided TO ts_ingest;
                  END IF;
                END $$;

                DO $$
                BEGIN
                  IF to_regclass('core.ix_events_ts_ingest') IS NULL
                     AND to_regclass('core.ix_events_ts_coided') IS NOT NULL THEN
                    ALTER INDEX core.ix_events_ts_coided RENAME TO ix_events_ts_ingest;
                  END IF;
                END $$;
                "#,
            )
            .await?;

        Ok(())
    }
}
