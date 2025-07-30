//! Query builder system for centralized SQL operations with automatic ULID/UUID conversion
//!
//! This module provides a type-safe query builder that automatically handles:
//! - ULID to UUID conversion for database operations
//! - Parameter binding with type safety
//! - Common query patterns (SELECT, INSERT, UPDATE, DELETE)
//! - Error handling and context
//!
//! # Design Philosophy
//!
//! The query builder follows these principles:
//! - **Automatic ULID/UUID conversion**: No manual .to_uuid() calls needed
//! - **Type safety**: Compile-time parameter validation
//! - **Ergonomic API**: Fluent interface for building queries
//! - **Performance**: Prepared statements and connection pooling
//! - **Maintainability**: Centralized query logic
//!
//! # Usage Examples
//!
//! ```rust
//! use sinex_db::query_builder::*;
//!
//! // Simple SELECT with ULID parameter
//! let event = QueryBuilder::select(tables::EVENTS)
//!     .columns(&["event_id", "source", "event_type", "payload"])
//!     .where_eq("event_id", QueryParam::Ulid(event_id))
//!     .fetch_one::<RawEvent>(pool)
//!     .await?;
//!
//! // INSERT with automatic ULID conversion
//! let inserted = QueryBuilder::insert(tables::EVENTS)
//!     .columns(&["source", "event_type", "host", "payload"])
//!     .values(&[
//!         QueryParam::String("test.source".to_string()),
//!         QueryParam::String("test_event".to_string()),
//!         QueryParam::String("localhost".to_string()),
//!         QueryParam::Json(json!({"test": "data"}))
//!     ])
//!     .returning(&["event_id"])
//!     .fetch_one::<EventIdRecord>(pool)
//!     .await?;
//!
//! // UPDATE with ULID array parameter
//! let updated = QueryBuilder::update(tables::EVENTS)
//!     .set("source_event_ids", QueryParam::UlidArray(vec![ulid1, ulid2]))
//!     .where_eq("event_id", QueryParam::Ulid(event_id))
//!     .execute(pool)
//!     .await?;
//! ```

use crate::query_helpers::{db_error, ulid_to_uuid, DbResult};
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sinex_ulid::Ulid;
use sqlx::types::Uuid;
use sqlx::{FromRow, PgPool, Postgres};

/// Query parameter types with automatic ULID/UUID conversion
#[derive(Debug, Clone)]
pub enum QueryParam {
    /// ULID value (automatically converted to UUID)
    Ulid(Ulid),
    /// Array of ULIDs (automatically converted to UUID array)
    UlidArray(Vec<Ulid>),
    /// Optional array of ULIDs (automatically converted to Optional UUID array)
    OptionalUlidArray(Option<Vec<Ulid>>),
    /// Optional ULID (automatically converted to Optional UUID)
    OptionalUlid(Option<Ulid>),
    /// String value
    String(String),
    /// Optional string value
    OptionalString(Option<String>),
    /// Integer value
    Integer(i64),
    /// Optional integer value
    OptionalInteger(Option<i64>),
    /// Float value
    Float(f64),
    /// Optional float value
    OptionalFloat(Option<f64>),
    /// Boolean value
    Boolean(bool),
    /// Optional boolean value
    OptionalBoolean(Option<bool>),
    /// JSON value
    Json(JsonValue),
    /// Optional JSON value
    OptionalJson(Option<JsonValue>),
    /// Timestamp value
    Timestamp(DateTime<Utc>),
    /// Optional timestamp value
    OptionalTimestamp(Option<DateTime<Utc>>),
    /// Raw UUID value (for when you already have UUIDs)
    Uuid(Uuid),
    /// Optional UUID value
    OptionalUuid(Option<Uuid>),
    /// Array of UUIDs
    UuidArray(Vec<Uuid>),
    /// Raw SQL fragment (use with caution!)
    Raw(String),
}

impl QueryParam {
    /// Get the SQL type hint for this parameter
    pub fn sql_type_hint(&self) -> &'static str {
        match self {
            QueryParam::Ulid(_) => "uuid",
            QueryParam::UlidArray(_) => "uuid[]",
            QueryParam::OptionalUlidArray(_) => "uuid[]",
            QueryParam::OptionalUlid(_) => "uuid",
            QueryParam::String(_) => "text",
            QueryParam::OptionalString(_) => "text",
            QueryParam::Integer(_) => "bigint",
            QueryParam::OptionalInteger(_) => "bigint",
            QueryParam::Float(_) => "float8",
            QueryParam::OptionalFloat(_) => "float8",
            QueryParam::Boolean(_) => "boolean",
            QueryParam::OptionalBoolean(_) => "boolean",
            QueryParam::Json(_) => "jsonb",
            QueryParam::OptionalJson(_) => "jsonb",
            QueryParam::Timestamp(_) => "timestamptz",
            QueryParam::OptionalTimestamp(_) => "timestamptz",
            QueryParam::Uuid(_) => "uuid",
            QueryParam::OptionalUuid(_) => "uuid",
            QueryParam::UuidArray(_) => "uuid[]",
            QueryParam::Raw(_) => "",
        }
    }

    /// Convert this parameter to a raw value for binding
    pub fn to_raw_value(&self) -> RawQueryParam {
        match self {
            QueryParam::Ulid(ulid) => RawQueryParam::Uuid(ulid_to_uuid(*ulid)),
            QueryParam::UlidArray(ulids) => {
                let uuids: Vec<Uuid> = ulids.iter().map(|u| ulid_to_uuid(*u)).collect();
                RawQueryParam::UuidArray(uuids)
            }
            QueryParam::OptionalUlidArray(opt_ulids) => RawQueryParam::OptionalUuidArray(
                opt_ulids
                    .as_ref()
                    .map(|ulids| ulids.iter().map(|u| ulid_to_uuid(*u)).collect()),
            ),
            QueryParam::OptionalUlid(opt_ulid) => {
                RawQueryParam::OptionalUuid(opt_ulid.map(ulid_to_uuid))
            }
            QueryParam::String(s) => RawQueryParam::String(s.clone()),
            QueryParam::OptionalString(opt_s) => RawQueryParam::OptionalString(opt_s.clone()),
            QueryParam::Integer(i) => RawQueryParam::Integer(*i),
            QueryParam::OptionalInteger(opt_i) => RawQueryParam::OptionalInteger(*opt_i),
            QueryParam::Float(f) => RawQueryParam::Float(*f),
            QueryParam::OptionalFloat(opt_f) => RawQueryParam::OptionalFloat(*opt_f),
            QueryParam::Boolean(b) => RawQueryParam::Boolean(*b),
            QueryParam::OptionalBoolean(opt_b) => RawQueryParam::OptionalBoolean(*opt_b),
            QueryParam::Json(j) => RawQueryParam::Json(j.clone()),
            QueryParam::OptionalJson(opt_j) => RawQueryParam::OptionalJson(opt_j.clone()),
            QueryParam::Timestamp(ts) => RawQueryParam::Timestamp(*ts),
            QueryParam::OptionalTimestamp(opt_ts) => RawQueryParam::OptionalTimestamp(*opt_ts),
            QueryParam::Uuid(uuid) => RawQueryParam::Uuid(*uuid),
            QueryParam::OptionalUuid(opt_uuid) => RawQueryParam::OptionalUuid(*opt_uuid),
            QueryParam::UuidArray(uuids) => RawQueryParam::UuidArray(uuids.clone()),
            QueryParam::Raw(raw) => RawQueryParam::Raw(raw.clone()),
        }
    }
}

/// Internal representation of parameters for SQL binding
#[derive(Debug, Clone)]
pub enum RawQueryParam {
    Uuid(Uuid),
    UuidArray(Vec<Uuid>),
    OptionalUuidArray(Option<Vec<Uuid>>),
    OptionalUuid(Option<Uuid>),
    String(String),
    OptionalString(Option<String>),
    Integer(i64),
    OptionalInteger(Option<i64>),
    Float(f64),
    OptionalFloat(Option<f64>),
    Boolean(bool),
    OptionalBoolean(Option<bool>),
    Json(JsonValue),
    OptionalJson(Option<JsonValue>),
    Timestamp(DateTime<Utc>),
    OptionalTimestamp(Option<DateTime<Utc>>),
    Raw(String),
}

/// Query type enumeration
#[derive(Debug, Clone)]
pub enum QueryType {
    Select,
    Insert,
    Update,
    Delete,
}

/// WHERE clause conditions
#[derive(Debug, Clone)]
pub enum WhereCondition {
    /// Standard condition with a parameter (e.g., column = $1)
    Parameterized {
        column: String,
        operator: String,
        param: QueryParam,
        param_index: usize,
    },
    /// NULL check condition (e.g., column IS NULL)
    NullCheck { column: String, is_null: bool },
    /// Raw WHERE condition (e.g., "ts_server > NOW() - INTERVAL '1 hour'")
    Raw { condition: String },
}

/// ORDER BY clause
#[derive(Debug, Clone)]
pub struct OrderBy {
    pub column: String,
    pub direction: String,
}

/// JOIN clause
#[derive(Debug, Clone)]
pub struct Join {
    pub join_type: String,
    pub table: String,
    pub condition: String,
}

/// Main query builder struct
#[derive(Debug, Clone)]
pub struct QueryBuilder {
    query_type: QueryType,
    table: String,
    columns: Vec<String>,
    conditions: Vec<WhereCondition>,
    parameters: Vec<QueryParam>,
    returning: Vec<String>,
    order_by: Vec<OrderBy>,
    group_by: Vec<String>,
    limit: Option<i64>,
    offset: Option<i64>,
    set_clauses: Vec<(String, QueryParam)>,
    values: Vec<QueryParam>,
    joins: Vec<Join>,
}

impl QueryBuilder {
    /// Create a new SELECT query builder
    pub fn select(table: &str) -> Self {
        Self {
            query_type: QueryType::Select,
            table: table.to_string(),
            columns: vec!["*".to_string()],
            conditions: Vec::new(),
            parameters: Vec::new(),
            returning: Vec::new(),
            order_by: Vec::new(),
            group_by: Vec::new(),
            limit: None,
            offset: None,
            set_clauses: Vec::new(),
            values: Vec::new(),
            joins: Vec::new(),
        }
    }

    /// Create a new INSERT query builder
    pub fn insert(table: &str) -> Self {
        Self {
            query_type: QueryType::Insert,
            table: table.to_string(),
            columns: Vec::new(),
            conditions: Vec::new(),
            parameters: Vec::new(),
            returning: Vec::new(),
            order_by: Vec::new(),
            group_by: Vec::new(),
            limit: None,
            offset: None,
            set_clauses: Vec::new(),
            values: Vec::new(),
            joins: Vec::new(),
        }
    }

    /// Create a new UPDATE query builder
    pub fn update(table: &str) -> Self {
        Self {
            query_type: QueryType::Update,
            table: table.to_string(),
            columns: Vec::new(),
            conditions: Vec::new(),
            parameters: Vec::new(),
            returning: Vec::new(),
            order_by: Vec::new(),
            group_by: Vec::new(),
            limit: None,
            offset: None,
            set_clauses: Vec::new(),
            values: Vec::new(),
            joins: Vec::new(),
        }
    }

    /// Create a new DELETE query builder
    pub fn delete(table: &str) -> Self {
        Self {
            query_type: QueryType::Delete,
            table: table.to_string(),
            columns: Vec::new(),
            conditions: Vec::new(),
            parameters: Vec::new(),
            returning: Vec::new(),
            order_by: Vec::new(),
            group_by: Vec::new(),
            limit: None,
            offset: None,
            set_clauses: Vec::new(),
            values: Vec::new(),
            joins: Vec::new(),
        }
    }

    /// Set columns for SELECT or INSERT
    pub fn columns(mut self, columns: &[&str]) -> Self {
        self.columns = columns.iter().map(|c| c.to_string()).collect();
        self
    }

    /// Add a WHERE condition with equality
    pub fn where_eq(mut self, column: &str, param: QueryParam) -> Self {
        let param_index = self.parameters.len() + 1;
        self.conditions.push(WhereCondition::Parameterized {
            column: column.to_string(),
            operator: "=".to_string(),
            param: param.clone(),
            param_index,
        });
        self.parameters.push(param);
        self
    }

    /// Add a WHERE condition with custom operator
    pub fn where_op(mut self, column: &str, operator: &str, param: QueryParam) -> Self {
        let param_index = self.parameters.len() + 1;
        self.conditions.push(WhereCondition::Parameterized {
            column: column.to_string(),
            operator: operator.to_string(),
            param: param.clone(),
            param_index,
        });
        self.parameters.push(param);
        self
    }

    /// Add a WHERE IN condition
    pub fn where_in(mut self, column: &str, param: QueryParam) -> Self {
        let param_index = self.parameters.len() + 1;
        self.conditions.push(WhereCondition::Parameterized {
            column: column.to_string(),
            operator: "= ANY".to_string(),
            param: param.clone(),
            param_index,
        });
        self.parameters.push(param);
        self
    }

    /// Add a WHERE IS NULL condition
    pub fn where_is_null(mut self, column: &str) -> Self {
        self.conditions.push(WhereCondition::NullCheck {
            column: column.to_string(),
            is_null: true,
        });
        self
    }

    /// Add a WHERE IS NOT NULL condition
    pub fn where_is_not_null(mut self, column: &str) -> Self {
        self.conditions.push(WhereCondition::NullCheck {
            column: column.to_string(),
            is_null: false,
        });
        self
    }

    /// Add raw WHERE clause (for complex conditions)
    pub fn where_clause(mut self, condition: &str) -> Self {
        self.conditions.push(WhereCondition::Raw {
            condition: condition.to_string(),
        });
        self
    }

    /// Convenience method for WHERE column >= param
    pub fn where_gte(self, column: &str, param: QueryParam) -> Self {
        self.where_op(column, ">=", param)
    }

    /// Add INNER JOIN clause
    pub fn inner_join(mut self, table_with_condition: &str, condition: &str) -> Self {
        self.joins.push(Join {
            join_type: "INNER".to_string(),
            table: table_with_condition.to_string(),
            condition: condition.to_string(),
        });
        self
    }

    /// Add ORDER BY clause
    pub fn order_by(mut self, column: &str, direction: &str) -> Self {
        self.order_by.push(OrderBy {
            column: column.to_string(),
            direction: direction.to_string(),
        });
        self
    }

    /// Add GROUP BY clause
    pub fn group_by(mut self, column: &str) -> Self {
        self.group_by.push(column.to_string());
        self
    }

    /// Add HAVING clause (placeholder for future implementation)
    pub fn having(self, _condition: &str) -> Self {
        // TODO: Implement HAVING clause support
        self
    }

    /// Add multiple GROUP BY columns
    pub fn group_by_multiple(mut self, columns: &[&str]) -> Self {
        for column in columns {
            self.group_by.push(column.to_string());
        }
        self
    }

    /// Add LIMIT clause
    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Add OFFSET clause
    pub fn offset(mut self, offset: i64) -> Self {
        self.offset = Some(offset);
        self
    }

    /// Add RETURNING clause
    pub fn returning(mut self, columns: &[&str]) -> Self {
        self.returning = columns.iter().map(|c| c.to_string()).collect();
        self
    }

    /// Set UPDATE clause
    pub fn set(mut self, column: &str, param: QueryParam) -> Self {
        self.set_clauses.push((column.to_string(), param));
        self
    }

    /// Set VALUES for INSERT
    pub fn values(mut self, values: &[QueryParam]) -> Self {
        self.values = values.to_vec();
        self
    }

    /// Build the final SQL query and parameters
    pub fn build(&self) -> DbResult<(String, Vec<RawQueryParam>)> {
        let mut sql = String::new();
        let mut params = Vec::new();

        match self.query_type {
            QueryType::Select => {
                sql.push_str("SELECT ");
                sql.push_str(&self.columns.join(", "));
                sql.push_str(" FROM ");
                sql.push_str(&self.table);
            }
            QueryType::Insert => {
                sql.push_str("INSERT INTO ");
                sql.push_str(&self.table);

                if !self.columns.is_empty() {
                    sql.push_str(" (");
                    sql.push_str(&self.columns.join(", "));
                    sql.push_str(") VALUES (");

                    let placeholders: Vec<String> = self
                        .values
                        .iter()
                        .enumerate()
                        .map(|(i, param)| {
                            let param_index = i + 1;
                            // Special handling for source_event_ids ULID array
                            if self
                                .columns
                                .get(i)
                                .map(|c| c == "source_event_ids")
                                .unwrap_or(false)
                            {
                                match param {
                                    QueryParam::UlidArray(_) => format!("${}::ulid[]", param_index),
                                    QueryParam::OptionalUlidArray(_) => {
                                        format!("${}::ulid[]", param_index)
                                    }
                                    _ => format!("${}", param_index),
                                }
                            } else {
                                format!("${}", param_index)
                            }
                        })
                        .collect();
                    sql.push_str(&placeholders.join(", "));
                    sql.push_str(")");

                    for param in &self.values {
                        params.push(param.to_raw_value());
                    }
                }
            }
            QueryType::Update => {
                sql.push_str("UPDATE ");
                sql.push_str(&self.table);

                if !self.set_clauses.is_empty() {
                    sql.push_str(" SET ");
                    let mut set_parts = Vec::new();
                    for (column, param) in self.set_clauses.iter() {
                        let param_index = params.len() + 1;
                        // Special handling for source_event_ids ULID array
                        if column == "source_event_ids" {
                            match param {
                                QueryParam::UlidArray(_) => {
                                    set_parts
                                        .push(format!("{} = ${}::ulid[]", column, param_index));
                                }
                                QueryParam::OptionalUlidArray(_) => {
                                    set_parts
                                        .push(format!("{} = ${}::ulid[]", column, param_index));
                                }
                                _ => {
                                    set_parts.push(format!("{} = ${}", column, param_index));
                                }
                            }
                        } else {
                            set_parts.push(format!("{} = ${}", column, param_index));
                        }
                        params.push(param.to_raw_value());
                    }
                    sql.push_str(&set_parts.join(", "));
                }
            }
            QueryType::Delete => {
                sql.push_str("DELETE FROM ");
                sql.push_str(&self.table);
            }
        }

        // Add JOINs
        for join in &self.joins {
            sql.push_str(&format!(
                " {} JOIN {} ON {}",
                join.join_type, join.table, join.condition
            ));
        }

        // Add WHERE conditions
        if !self.conditions.is_empty() {
            sql.push_str(" WHERE ");
            let mut where_parts = Vec::new();
            for condition in &self.conditions {
                match condition {
                    WhereCondition::Parameterized {
                        column,
                        operator,
                        param,
                        ..
                    } => {
                        match param {
                            QueryParam::Raw(raw_sql) => {
                                // Raw SQL fragments are embedded directly
                                where_parts.push(format!("{} {} {}", column, operator, raw_sql));
                            }
                            _ => {
                                let param_index = params.len() + 1;
                                let type_hint = param.sql_type_hint();
                                where_parts.push(format!(
                                    "{} {} ${}::{}",
                                    column, operator, param_index, type_hint
                                ));
                                params.push(param.to_raw_value());
                            }
                        }
                    }
                    WhereCondition::NullCheck { column, is_null } => {
                        if *is_null {
                            where_parts.push(format!("{} IS NULL", column));
                        } else {
                            where_parts.push(format!("{} IS NOT NULL", column));
                        }
                    }
                    WhereCondition::Raw { condition } => {
                        where_parts.push(condition.clone());
                    }
                }
            }
            sql.push_str(&where_parts.join(" AND "));
        }

        // Add GROUP BY
        if !self.group_by.is_empty() {
            sql.push_str(" GROUP BY ");
            sql.push_str(&self.group_by.join(", "));
        }

        // Add ORDER BY
        if !self.order_by.is_empty() {
            sql.push_str(" ORDER BY ");
            let order_parts: Vec<String> = self
                .order_by
                .iter()
                .map(|o| format!("{} {}", o.column, o.direction))
                .collect();
            sql.push_str(&order_parts.join(", "));
        }

        // Add LIMIT
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }

        // Add OFFSET
        if let Some(offset) = self.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        // Add RETURNING
        if !self.returning.is_empty() {
            sql.push_str(" RETURNING ");
            sql.push_str(&self.returning.join(", "));
        }

        Ok((sql, params))
    }

    /// Execute query and return a single row
    pub async fn fetch_one<T>(self, pool: &PgPool) -> DbResult<T>
    where
        T: for<'r> FromRow<'r, sqlx::postgres::PgRow> + Unpin + Send,
    {
        let (sql, params) = self.build()?;

        let mut query = sqlx::query_as(&sql);
        for param in params {
            query = bind_param(query, param);
        }

        query
            .fetch_one(pool)
            .await
            .map_err(|e| db_error(e, &format!("fetch_one query: {}", sql)))
    }

    /// Execute query and return optional single row
    pub async fn fetch_optional<T>(self, pool: &PgPool) -> DbResult<Option<T>>
    where
        T: for<'r> FromRow<'r, sqlx::postgres::PgRow> + Unpin + Send,
    {
        let (sql, params) = self.build()?;

        let mut query = sqlx::query_as(&sql);
        for param in params {
            query = bind_param(query, param);
        }

        query
            .fetch_optional(pool)
            .await
            .map_err(|e| db_error(e, &format!("fetch_optional query: {}", sql)))
    }

    /// Execute query and return all rows
    pub async fn fetch_all<T>(self, pool: &PgPool) -> DbResult<Vec<T>>
    where
        T: for<'r> FromRow<'r, sqlx::postgres::PgRow> + Unpin + Send,
    {
        let (sql, params) = self.build()?;

        let mut query = sqlx::query_as(&sql);
        for param in params {
            query = bind_param(query, param);
        }

        query
            .fetch_all(pool)
            .await
            .map_err(|e| db_error(e, &format!("fetch_all query: {}", sql)))
    }

    /// Execute query and return execution result
    pub async fn execute(self, pool: &PgPool) -> DbResult<sqlx::postgres::PgQueryResult> {
        let (sql, params) = self.build()?;

        let mut query = sqlx::query(&sql);
        for param in params {
            query = bind_param_raw(query, param);
        }

        query
            .execute(pool)
            .await
            .map_err(|e| db_error(e, &format!("execute query: {}", sql)))
    }

    /// Execute query on a transaction and return execution result
    pub async fn execute_tx(
        self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ) -> DbResult<sqlx::postgres::PgQueryResult> {
        let (sql, params) = self.build()?;

        let mut query = sqlx::query(&sql);
        for param in params {
            query = bind_param_raw(query, param);
        }

        query
            .execute(&mut **tx)
            .await
            .map_err(|e| db_error(e, &format!("execute query: {}", sql)))
    }
}

/// Helper function to bind parameters to a query
fn bind_param<T>(
    query: sqlx::query::QueryAs<'_, Postgres, T, sqlx::postgres::PgArguments>,
    param: RawQueryParam,
) -> sqlx::query::QueryAs<'_, Postgres, T, sqlx::postgres::PgArguments> {
    match param {
        RawQueryParam::Uuid(uuid) => query.bind(uuid),
        RawQueryParam::UuidArray(uuids) => query.bind(uuids),
        RawQueryParam::OptionalUuidArray(opt_uuids) => query.bind(opt_uuids),
        RawQueryParam::OptionalUuid(opt_uuid) => query.bind(opt_uuid),
        RawQueryParam::String(s) => query.bind(s),
        RawQueryParam::OptionalString(opt_s) => query.bind(opt_s),
        RawQueryParam::Integer(i) => query.bind(i),
        RawQueryParam::OptionalInteger(opt_i) => query.bind(opt_i),
        RawQueryParam::Float(f) => query.bind(f),
        RawQueryParam::OptionalFloat(opt_f) => query.bind(opt_f),
        RawQueryParam::Boolean(b) => query.bind(b),
        RawQueryParam::OptionalBoolean(opt_b) => query.bind(opt_b),
        RawQueryParam::Json(j) => query.bind(j),
        RawQueryParam::OptionalJson(opt_j) => query.bind(opt_j),
        RawQueryParam::Timestamp(ts) => query.bind(ts),
        RawQueryParam::OptionalTimestamp(opt_ts) => query.bind(opt_ts),
        RawQueryParam::Raw(_) => {
            panic!("Raw SQL fragments should not be bound as parameters")
        }
    }
}

/// Helper function to bind parameters to a raw query
fn bind_param_raw(
    query: sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments>,
    param: RawQueryParam,
) -> sqlx::query::Query<'_, Postgres, sqlx::postgres::PgArguments> {
    match param {
        RawQueryParam::Uuid(uuid) => query.bind(uuid),
        RawQueryParam::UuidArray(uuids) => query.bind(uuids),
        RawQueryParam::OptionalUuidArray(opt_uuids) => query.bind(opt_uuids),
        RawQueryParam::OptionalUuid(opt_uuid) => query.bind(opt_uuid),
        RawQueryParam::String(s) => query.bind(s),
        RawQueryParam::OptionalString(opt_s) => query.bind(opt_s),
        RawQueryParam::Integer(i) => query.bind(i),
        RawQueryParam::OptionalInteger(opt_i) => query.bind(opt_i),
        RawQueryParam::Float(f) => query.bind(f),
        RawQueryParam::OptionalFloat(opt_f) => query.bind(opt_f),
        RawQueryParam::Boolean(b) => query.bind(b),
        RawQueryParam::OptionalBoolean(opt_b) => query.bind(opt_b),
        RawQueryParam::Json(j) => query.bind(j),
        RawQueryParam::OptionalJson(opt_j) => query.bind(opt_j),
        RawQueryParam::Timestamp(ts) => query.bind(ts),
        RawQueryParam::OptionalTimestamp(opt_ts) => query.bind(opt_ts),
        RawQueryParam::Raw(_) => {
            panic!("Raw SQL fragments should not be bound as parameters")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::tables;
    use chrono::Utc;
    use serde_json::json;
    use sinex_test_utils::prelude::*;
    use sinex_ulid::Ulid;

    #[sinex_test]
    async fn test_select_query_builder(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::select(tables::EVENTS)
            .columns(&["event_id", "source", "event_type"])
            .where_eq("event_id", QueryParam::Ulid(Ulid::new()))
            .order_by("ts_ingest", "DESC")
            .limit(10);

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("SELECT event_id, source, event_type FROM core.events"));
        assert!(sql.contains("WHERE event_id = $1::uuid"));
        assert!(sql.contains("ORDER BY ts_ingest DESC"));
        assert!(sql.contains("LIMIT 10"));
        assert_eq!(params.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_insert_query_builder(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::insert(tables::EVENTS)
            .columns(&["source", "event_type", "payload"])
            .values(&[
                QueryParam::String("test.source".to_string()),
                QueryParam::String("test_event".to_string()),
                QueryParam::Json(json!({"test": "data"})),
            ])
            .returning(&["event_id"]);

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("INSERT INTO core.events (source, event_type, payload)"));
        assert!(sql.contains("VALUES ($1, $2, $3)"));
        assert!(sql.contains("RETURNING event_id"));
        assert_eq!(params.len(), 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_update_query_builder(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::update(tables::EVENTS)
            .set("source", QueryParam::String("updated.source".to_string()))
            .set("payload", QueryParam::Json(json!({"updated": true})))
            .where_eq("event_id", QueryParam::Ulid(Ulid::new()));

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("UPDATE core.events SET"));
        assert!(sql.contains("source = $1"));
        assert!(sql.contains("payload = $2"));
        assert!(sql.contains("WHERE event_id = $3::uuid"));
        assert_eq!(params.len(), 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_delete_query_builder(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::delete(tables::EVENTS)
            .where_eq("event_id", QueryParam::Ulid(Ulid::new()));

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("DELETE FROM core.events"));
        assert!(sql.contains("WHERE event_id = $1::uuid"));
        assert_eq!(params.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_ulid_array_parameter(ctx: TestContext) -> anyhow::Result<()> {
        let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
        let builder = QueryBuilder::select(tables::EVENTS)
            .where_in("event_id", QueryParam::UlidArray(ulids.clone()));

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("WHERE event_id = ANY($1::uuid[])"));
        assert_eq!(params.len(), 1);

        match &params[0] {
            RawQueryParam::UuidArray(uuids) => {
                assert_eq!(uuids.len(), ulids.len());
            }
            _ => panic!("Expected UuidArray parameter"),
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_optional_parameters(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::select(tables::EVENTS)
            .where_eq(
                "payload_schema_id",
                QueryParam::OptionalUlid(Some(Ulid::new())),
            )
            .where_eq("ingestor_version", QueryParam::OptionalString(None));

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("WHERE payload_schema_id = $1::uuid"));
        assert!(sql.contains("AND ingestor_version = $2::text"));
        assert_eq!(params.len(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_complex_select_with_conditions(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::select(tables::EVENTS)
            .columns(&["event_id", "source", "event_type"])
            .where_eq("source", QueryParam::String("test".to_string()))
            .where_op("ts_server", ">=", QueryParam::Timestamp(Utc::now()))
            .order_by("ts_server", "DESC")
            .limit(10)
            .offset(5);

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("SELECT event_id, source, event_type"));
        assert!(sql.contains("WHERE source = $1"));
        assert!(sql.contains("ORDER BY ts_server DESC"));
        assert!(sql.contains("LIMIT"));
        assert!(sql.contains("OFFSET"));
        assert_eq!(params.len(), 2);
        Ok(())
    }

    #[sinex_test]
    async fn test_ulid_to_uuid_conversion(ctx: TestContext) -> anyhow::Result<()> {
        let test_ulid = Ulid::new();
        let expected_uuid = crate::query_helpers::ulid_to_uuid(test_ulid);

        let builder =
            QueryBuilder::select(tables::EVENTS).where_eq("event_id", QueryParam::Ulid(test_ulid));

        let (_sql, params) = builder.build().unwrap();

        match &params[0] {
            RawQueryParam::Uuid(uuid) => {
                assert_eq!(*uuid, expected_uuid);
            }
            _ => panic!("Expected UUID parameter after ULID conversion"),
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_json_parameter_handling(ctx: TestContext) -> anyhow::Result<()> {
        let test_json = serde_json::json!({
            "test": "value",
            "number": 42,
            "nested": {
                "inner": "data"
            }
        });

        let builder = QueryBuilder::insert(tables::EVENTS)
            .columns(&["source", "payload"])
            .values(&[
                QueryParam::String("test".to_string()),
                QueryParam::Json(test_json.clone()),
            ]);

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("INSERT INTO core.events"));
        assert_eq!(params.len(), 2);

        match &params[1] {
            RawQueryParam::Json(json_val) => {
                assert_eq!(*json_val, test_json);
            }
            _ => panic!("Expected JSON parameter"),
        }
        Ok(())
    }

    #[sinex_test]
    async fn test_bulk_insert(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::insert(tables::EVENTS)
            .columns(&["source", "event_type", "host", "payload"])
            .values(&[
                QueryParam::String("bulk_test".to_string()),
                QueryParam::String("test.bulk".to_string()),
                QueryParam::String("localhost".to_string()),
                QueryParam::Json(serde_json::json!({"index": 1})),
            ])
            .values(&[
                QueryParam::String("bulk_test".to_string()),
                QueryParam::String("test.bulk".to_string()),
                QueryParam::String("localhost".to_string()),
                QueryParam::Json(serde_json::json!({"index": 2})),
            ]);

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("INSERT INTO core.events"));
        assert!(sql.contains("VALUES ($1, $2, $3, $4), ($5, $6, $7, $8)"));
        assert_eq!(params.len(), 8);
        Ok(())
    }

    #[sinex_test]
    async fn test_where_in_query(ctx: TestContext) -> anyhow::Result<()> {
        let ulids = vec![Ulid::new(), Ulid::new()];
        let builder = QueryBuilder::select(tables::EVENTS)
            .where_in("event_id", QueryParam::UlidArray(ulids.clone()));

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("WHERE event_id = ANY($1::uuid[])"));
        assert_eq!(params.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_count_query(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::select(tables::EVENTS)
            .columns(&["COUNT(*)"])
            .where_eq("source", QueryParam::String("test".to_string()));

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("SELECT COUNT(*)"));
        assert!(sql.contains("WHERE source = $1"));
        assert_eq!(params.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_update_with_multiple_sets(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::update(tables::EVENTS)
            .set("source", QueryParam::String("updated_source".to_string()))
            .set(
                "payload",
                QueryParam::Json(serde_json::json!({"updated": true})),
            )
            .set("ts_server", QueryParam::Timestamp(Utc::now()))
            .where_eq("event_id", QueryParam::Ulid(Ulid::new()));

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("UPDATE core.events"));
        assert!(sql.contains("SET source = $1, payload = $2, ts_server = $3"));
        assert!(sql.contains("WHERE event_id = $4::uuid"));
        assert_eq!(params.len(), 4);
        Ok(())
    }

    #[sinex_test]
    async fn test_raw_condition(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::select(tables::EVENTS).where_op(
            "ts_server",
            ">",
            QueryParam::String("NOW() - INTERVAL '1 hour'".to_string()),
        );

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("WHERE ts_server >"));
        assert_eq!(params.len(), 1);
        Ok(())
    }

    #[sinex_test]
    async fn test_parameter_type_coercion(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::select(tables::EVENTS)
            .where_eq("payload_schema_id", QueryParam::OptionalUlid(None))
            .where_eq("source_event_ids", QueryParam::OptionalUlidArray(None))
            .where_eq(
                "ingestor_version",
                QueryParam::OptionalString(Some("v1.0.0".to_string())),
            );

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("WHERE payload_schema_id = $1::uuid"));
        assert!(sql.contains("AND source_event_ids = $2::uuid[]"));
        assert!(sql.contains("AND ingestor_version = $3::text"));
        assert_eq!(params.len(), 3);
        Ok(())
    }

    #[sinex_test]
    async fn test_complex_aggregation_query(ctx: TestContext) -> anyhow::Result<()> {
        let builder = QueryBuilder::select(tables::EVENTS)
            .columns(&[
                "source",
                "COUNT(*) as event_count",
                "MAX(ts_server) as latest_event",
                "MIN(ts_server) as earliest_event",
            ])
            .where_op(
                "ts_server",
                ">=",
                QueryParam::Timestamp(Utc::now() - chrono::Duration::days(7)),
            )
            .group_by("source")
            .order_by("event_count", "DESC")
            .limit(100);

        let (sql, params) = builder.build().unwrap();
        assert!(sql.contains("SELECT source, COUNT(*) as event_count"));
        assert!(sql.contains("WHERE ts_server >="));
        assert!(sql.contains("GROUP BY source"));
        assert!(sql.contains("ORDER BY event_count DESC"));
        assert!(sql.contains("LIMIT"));
        assert_eq!(params.len(), 2);
        Ok(())
    }
}
