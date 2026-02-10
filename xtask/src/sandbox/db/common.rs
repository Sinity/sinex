use crate::sandbox::prelude::*;
use sqlx::PgConnection;
use std::collections::HashMap;

pub async fn verify_clean_state(pool: &DbPool) -> TestResult<()> {
    let counts = get_row_counts(pool).await?;
    let mut failures = Vec::new();
    for (table, count) in counts {
        // raw.source_material_registry is managed by seed_test_fixtures() which
        // inserts well-known fixture rows after every cleanup cycle. Skip it entirely
        // since its contents are always re-established.
        if table == "raw.source_material_registry" {
            continue;
        }
        if count > 0 {
            failures.push(format!("{table}: {count}"));
        }
    }

    if !failures.is_empty() {
        return Err(eyre!(
            "Database not clean! Residual data found: {:?}",
            failures
        ));
    }
    Ok(())
}

pub async fn reset_database(pool: &DbPool) -> TestResult<()> {
    let mut conn = pool.acquire().await?;
    let config = super::cleanup_config::CleanupConfig::default();

    // Track whether we need to restore session_replication_role
    let mut triggers_disabled = false;

    for table in config.ordered_tables() {
        match table.method {
            super::cleanup_config::CleanupMethod::Truncate => {
                sqlx::query(&format!(
                    "TRUNCATE TABLE {} RESTART IDENTITY CASCADE",
                    table.table_name
                ))
                .execute(&mut *conn)
                .await?;
            }
            super::cleanup_config::CleanupMethod::Delete => {
                // Disable triggers if required (e.g. append-only constraints)
                if table.disable_triggers && !triggers_disabled {
                    sqlx::query("SET session_replication_role = 'replica'")
                        .execute(&mut *conn)
                        .await?;
                    triggers_disabled = true;
                } else if !table.disable_triggers && triggers_disabled {
                    // Re-enable triggers before operating on tables that expect them
                    sqlx::query("SET session_replication_role = 'origin'")
                        .execute(&mut *conn)
                        .await?;
                    triggers_disabled = false;
                }

                sqlx::query(&format!("DELETE FROM {}", table.table_name))
                    .execute(&mut *conn)
                    .await?;
            }
            super::cleanup_config::CleanupMethod::Skip => {}
        }
    }

    // Always restore default trigger behavior
    if triggers_disabled {
        sqlx::query("SET session_replication_role = 'origin'")
            .execute(&mut *conn)
            .await?;
    }

    Ok(())
}

pub async fn get_row_counts(pool: &DbPool) -> TestResult<HashMap<String, i64>> {
    let mut conn = pool.acquire().await?;
    let config = super::cleanup_config::CleanupConfig::default();
    let mut counts = HashMap::new();

    for table in config.tables_to_clean() {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {}", table.table_name))
            .fetch_one(&mut *conn)
            .await?;
        counts.insert(table.table_name.to_string(), count);
    }

    Ok(counts)
}

pub async fn apply_test_optimizations(pool: &DbPool) -> TestResult<()> {
    let mut conn = pool.acquire().await?;
    // Example optimization: turn off synchronous commit for tests
    sqlx::query("SET synchronous_commit TO OFF")
        .execute(&mut *conn)
        .await?;
    Ok(())
}

pub async fn with_cleanup_session<F>(
    conn: &mut PgConnection,
    _config: &super::cleanup_config::CleanupConfig,
    f: F,
) -> TestResult<()>
where
    F: for<'a> FnOnce(&'a mut PgConnection) -> futures::future::BoxFuture<'a, TestResult<()>>,
{
    // Implementation for session-based cleanup if needed
    f(conn).await
}
