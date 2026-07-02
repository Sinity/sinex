//! Comprehensive tests for database schema definitions
//!
//! These tests validate that all schema definitions are correct and can be
//! executed against a real `PostgreSQL` database with the required extensions.

use sea_query::*;
use sinex_primitives::DynamicPayload;
use sinex_db::schema::defs::*;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use xtask::sandbox::prelude::*;

#[cfg(test)]
#[path = "schema_tests_table_creation_tests.rs"]
mod table_creation_tests;

#[cfg(test)]
#[path = "schema_tests_constraint_tests.rs"]
mod constraint_tests;

#[cfg(test)]
#[path = "schema_tests_index_tests.rs"]
mod index_tests;

#[cfg(test)]
#[path = "schema_tests_migration_tests.rs"]
mod migration_tests;

// Helper functions for testing

#[derive(Debug)]
struct ColumnInfo {
    data_type: String,
    is_nullable: bool,
    is_primary_key: bool,
}

async fn get_table_columns(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> color_eyre::Result<HashMap<String, ColumnInfo>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            c.column_name,
            c.data_type,
            c.udt_name,
            c.is_nullable = 'YES' as is_nullable,
            COALESCE(pk.is_primary, false) as is_primary_key
        FROM information_schema.columns c
        LEFT JOIN (
            SELECT
                kcu.column_name,
                true as is_primary
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
                AND tc.table_name = kcu.table_name
            WHERE tc.constraint_type = 'PRIMARY KEY'
                AND tc.table_schema = $1
                AND tc.table_name = $2
        ) pk ON c.column_name = pk.column_name
        WHERE c.table_schema = $1 AND c.table_name = $2
        ORDER BY c.ordinal_position
        "#,
        schema,
        table
    )
    .fetch_all(pool)
    .await?;

    let columns = rows
        .into_iter()
        .map(|row| {
            let name = row
                .column_name
                .expect("information_schema.columns must expose column_name");
            let mut dtype = row
                .data_type
                .expect("information_schema.columns must expose data_type");
            if dtype == "USER-DEFINED" {
                dtype = row
                    .udt_name
                    .expect("USER-DEFINED column should expose udt_name");
            }
            let info = ColumnInfo {
                data_type: dtype,
                is_nullable: row
                    .is_nullable
                    .expect("information_schema.columns must expose is_nullable"),
                is_primary_key: row.is_primary_key.unwrap_or(false),
            };
            (name, info)
        })
        .collect();
    Ok(columns)
}

#[derive(Debug)]
struct IndexInfo {
    index_name: String,
}

async fn get_table_indexes(
    pool: &PgPool,
    schema: &str,
    table: &str,
) -> color_eyre::Result<Vec<IndexInfo>> {
    let rows = sqlx::query!(
        r#"
        SELECT
            i.relname as index_name,
            ix.indisunique as is_unique,
            array_agg(a.attname ORDER BY a.attnum) as column_names
        FROM pg_class t
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1 AND t.relname = $2
        GROUP BY i.relname, ix.indisunique
        ORDER BY i.relname
        "#,
        schema,
        table
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|row| IndexInfo {
            index_name: row.index_name,
        })
        .collect())
}
