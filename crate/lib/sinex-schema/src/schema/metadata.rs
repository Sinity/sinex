//! Schema metadata types for compile-time validation and future code generation

use std::marker::PhantomData;

/// Represents a SQL type in the schema
#[derive(Debug, Clone, PartialEq)]
pub enum SqlType {
    /// Custom type (e.g., ULID)
    Custom(&'static str),
    /// TEXT
    Text,
    /// JSON/JSONB
    Json,
    /// BIGINT
    BigInteger,
    /// TIMESTAMP WITH TIME ZONE
    TimestampWithTimeZone,
    /// UUID
    Uuid,
    /// ARRAY of another type - use specific variants to avoid Box
    UlidArray,
    UuidArray,
    TextArray,
    /// VECTOR(dimensions)
    Vector(usize),
}

/// Constraint on a column
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Constraint {
    PrimaryKey,
    NotNull,
    Unique,
    Index,
    Generated(&'static str),
}

/// Schema metadata for a single column
#[derive(Debug, Clone)]
pub struct ColumnSchema {
    /// Column name in database (snake_case)
    pub name: &'static str,
    /// Rust type as string (for validation)
    pub rust_type: &'static str,
    /// SQL type
    pub sql_type: SqlType,
    /// Whether the column is nullable
    pub nullable: bool,
    /// Column constraints
    pub constraints: &'static [Constraint],
}

/// Schema metadata for a table
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// Table name
    pub name: &'static str,
    /// Schema name (e.g., "core", "raw")
    pub schema: &'static str,
    /// Column definitions
    pub columns: &'static [ColumnSchema],
    /// Table-level constraints (e.g., CHECK constraints)
    pub table_constraints: &'static [&'static str],
}

/// Trait for types that have schema metadata
pub trait HasSchema {
    /// Get the table schema metadata
    fn schema() -> &'static TableSchema;
}

/// Type-safe column reference
pub struct Column<T> {
    pub name: &'static str,
    _phantom: PhantomData<T>,
}

impl<T> Column<T> {
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _phantom: PhantomData,
        }
    }
}

/// Helper macro to define column schemas more ergonomically
#[macro_export]
macro_rules! column_schema {
    (
        $name:literal : $rust_type:literal = $sql_type:expr
        $(, nullable: $nullable:literal)?
        $(, constraints: [$($constraint:expr),* $(,)?])?
    ) => {
        $crate::schema::metadata::ColumnSchema {
            name: $name,
            rust_type: $rust_type,
            sql_type: $sql_type,
            nullable: false $(|| $nullable)?,
            constraints: &[$($($constraint),*)?],
        }
    };
}

/// Helper macro to define table schemas
#[macro_export]
macro_rules! table_schema {
    (
        table: $table:literal,
        schema: $schema:literal,
        columns: [$($column:expr),* $(,)?],
        constraints: [$($constraint:literal),* $(,)?]
    ) => {
        $crate::schema::metadata::TableSchema {
            name: $table,
            schema: $schema,
            columns: &[$($column),*],
            table_constraints: &[$($constraint),*],
        }
    };
}
