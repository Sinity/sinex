//! SQLite operation helpers to reduce boilerplate
//!
//! This module provides utilities for common SQLite operations with
//! consistent error handling and context.

use rusqlite::{Connection, OpenFlags, Statement};
use sinex_core_types::{CoreError, Result};
use std::path::Path;

/// Helper for opening SQLite databases with consistent error handling
pub struct SqliteConnection;

impl SqliteConnection {
    /// Open a read-only SQLite connection with error context
    pub fn open_readonly<P: AsRef<Path>>(path: P, operation: &str) -> Result<Connection> {
        let path_ref = path.as_ref();

        Connection::open_with_flags(path_ref, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(|e| {
            CoreError::Database(format!(
                "Failed to open database {} (operation: {}, access_mode: read_only): {}",
                path_ref.display(),
                operation,
                e
            ))
        })
    }

    /// Open a read-write SQLite connection with error context
    pub fn open_readwrite<P: AsRef<Path>>(path: P, operation: &str) -> Result<Connection> {
        let path_ref = path.as_ref();

        Connection::open_with_flags(
            path_ref,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(|e| {
            CoreError::Database(format!(
                "Failed to open database {} (operation: {}, access_mode: read_write): {}",
                path_ref.display(),
                operation,
                e
            ))
        })
    }
}

/// Helper for preparing statements with consistent error handling
pub trait SqliteStatementExt {
    fn prepare_with_context(&self, sql: &str, operation: &str) -> Result<Statement>;
}

impl SqliteStatementExt for Connection {
    fn prepare_with_context(&self, sql: &str, operation: &str) -> Result<Statement> {
        self.prepare(sql).map_err(|e| {
            CoreError::Database(format!(
                "Failed to prepare statement (operation: {}, sql_length: {}): {}",
                operation,
                sql.len(),
                e
            ))
        })
    }
}

/// Builder for SQLite queries with error context
pub struct SqliteQueryBuilder<'a> {
    operation: &'a str,
    query_type: Option<&'a str>,
    context: Vec<(&'a str, String)>,
}

impl<'a> SqliteQueryBuilder<'a> {
    pub fn new(operation: &'a str) -> Self {
        Self {
            operation,
            query_type: None,
            context: Vec::new(),
        }
    }

    pub fn query_type(mut self, query_type: &'a str) -> Self {
        self.query_type = Some(query_type);
        self
    }

    pub fn context(mut self, key: &'a str, value: impl ToString) -> Self {
        self.context.push((key, value.to_string()));
        self
    }

    /// Wrap a database operation with consistent error handling
    pub fn wrap_error<T, E: std::fmt::Display>(
        self,
        result: std::result::Result<T, E>,
    ) -> Result<T> {
        result.map_err(|e| {
            let mut error_msg = format!("Database error (operation: {}): {}", self.operation, e);

            if let Some(qt) = self.query_type {
                error_msg.push_str(&format!(", query_type: {}", qt));
            }

            for (key, value) in self.context {
                error_msg.push_str(&format!(", {}: {}", key, value));
            }

            CoreError::Database(error_msg)
        })
    }
}

/// Extension trait for rusqlite query results
pub trait QueryResultExt<T> {
    fn with_context(self, builder: SqliteQueryBuilder) -> Result<T>;
}

impl<T, E: std::fmt::Display> QueryResultExt<T> for std::result::Result<T, E> {
    fn with_context(self, builder: SqliteQueryBuilder) -> Result<T> {
        builder.wrap_error(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_sqlite_connection_helpers() {
        // Create a temporary database
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path();

        // Test read-write connection
        let conn = SqliteConnection::open_readwrite(path, "test_operation").unwrap();
        conn.execute("CREATE TABLE test (id INTEGER)", []).unwrap();
        drop(conn);

        // Test read-only connection
        let conn = SqliteConnection::open_readonly(path, "test_operation").unwrap();
        let count: i32 = conn
            .query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_statement_prepare_helper() {
        let temp_file = NamedTempFile::new().unwrap();
        let conn = Connection::open(temp_file.path()).unwrap();

        conn.execute("CREATE TABLE test (id INTEGER)", []).unwrap();

        // Test prepare with context
        let stmt = conn
            .prepare_with_context("SELECT * FROM test", "test_query")
            .unwrap();
        drop(stmt);

        // Test prepare error
        let result = conn.prepare_with_context("SELECT * FROM nonexistent", "test_query");
        assert!(result.is_err());
    }
}
