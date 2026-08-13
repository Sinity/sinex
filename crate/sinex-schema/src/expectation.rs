//! Typed schema expectations and the PostgreSQL catalog interpreter.
//!
//! The table definitions, convergence registry, and strict-diff checker used
//! to each carry a partial description of the same schema.  This module is
//! the bridge between those declarations and the live catalog: it extracts
//! expectations from the existing sea-query/convergence definitions and
//! interprets them against PostgreSQL without relying on object names alone.

use std::collections::HashMap;

use crate::apply::ApplyError;
use crate::converge::convergible_tables;
use crate::defs::{
    ArchivedEvents, Blobs, Entities, EntityRelations, EventPayloadSchemas, Events,
    SourceMaterialLinks, SourceMaterialRegistry, TemporalLedger,
};
use sea_query::{
    ColumnSpec, ColumnType, IndexCreateStatement, PostgresQueryBuilder, Query, TableCreateStatement,
};
use sqlx::PgPool;

/// The catalog object kinds interpreted by the shared schema substrate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpectationKind {
    Table,
    Column,
    Constraint,
    ForeignKey,
    Index,
    Trigger,
    Function,
}

impl std::fmt::Display for ExpectationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Table => "table",
            Self::Column => "column",
            Self::Constraint => "constraint",
            Self::ForeignKey => "foreign_key",
            Self::Index => "index",
            Self::Trigger => "trigger",
            Self::Function => "function",
        })
    }
}

/// A single source-declared PostgreSQL column contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnExpectation {
    pub name: String,
    pub type_sql: String,
    pub nullable: bool,
    pub default_sql: Option<String>,
    pub generated_sql: Option<String>,
    /// `None` means the source intentionally accepts the database default
    /// collation. An explicit value is compared exactly.
    pub collation: Option<String>,
}

/// A named CHECK expectation, or an anonymous inline expectation identified
/// by body markers when PostgreSQL generated the constraint name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConstraintExpectation {
    pub name: Option<String>,
    pub body: String,
    pub markers: Vec<String>,
}

/// A foreign-key contract. Anonymous source FKs are matched by normalized
/// definition; named FKs are additionally keyed by their stable name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForeignKeyExpectation {
    pub name: Option<String>,
    pub definition: String,
}

/// An index contract based on `pg_get_indexdef`, including predicates,
/// expressions, opclasses, and sort direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexExpectation {
    pub name: String,
    pub definition: String,
}

/// A table-level expectation assembled from one canonical table definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableExpectation {
    pub schema: String,
    pub table: String,
    pub columns: Vec<ColumnExpectation>,
    pub constraints: Vec<ConstraintExpectation>,
    pub foreign_keys: Vec<ForeignKeyExpectation>,
    pub indexes: Vec<IndexExpectation>,
}

/// Trigger properties which PostgreSQL exposes independently of the trigger
/// name. Checking all of these closes same-name disabled/retargeted drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerExpectation {
    pub schema: &'static str,
    pub table: &'static str,
    pub name: &'static str,
    pub enabled: &'static str,
    pub definition_markers: &'static [&'static str],
}

/// A source-declared stored-function body. The body hash is calculated from
/// the function SQL after removing formatting/comments, then compared with
/// `pg_proc.prosrc`; it does not trust a stale COMMENT stamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FunctionExpectation {
    pub signature: &'static str,
    pub body_hash: String,
}

/// One catalog mismatch returned by the verify interpreter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpectationDrift {
    pub kind: ExpectationKind,
    pub location: String,
    pub declared: String,
    pub observed: String,
}

impl std::fmt::Display for ExpectationDrift {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[{}] {}: declared {{{}}} vs observed {{{}}}",
            self.kind, self.location, self.declared, self.observed
        )
    }
}

/// Normalize catalog/source SQL for comparison. PostgreSQL changes quoting,
/// whitespace, and the optional `NOT VALID` suffix while preserving the
/// semantics represented here.
#[must_use]
pub fn normalize_sql(sql: &str) -> String {
    let mut without_comments = String::with_capacity(sql.len());
    for line in sql.lines() {
        let line = line.split_once("--").map_or(line, |(code, _)| code);
        without_comments.push_str(line);
        without_comments.push(' ');
    }
    without_comments
        .replace('"', "")
        .replace('`', "")
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(';')
        .trim()
        .to_ascii_lowercase()
        .replace(" not valid", "")
}

/// Compare a source index definition with PostgreSQL's `pg_get_indexdef`.
#[must_use]
pub fn index_definition_matches(expected: &str, observed: &str) -> bool {
    canonical_index_sql(observed) == canonical_index_sql(expected)
}

fn canonical_index_sql(sql: &str) -> String {
    let mut normalized = normalize_sql(sql).replace(" using btree", "");
    for prefix in ["create unique index ", "create index "] {
        let qualified = format!("{prefix}if not exists ");
        if normalized.starts_with(prefix) && !normalized.starts_with(&qualified) {
            normalized = normalized.replacen(prefix, &qualified, 1);
        }
    }
    for prefix in [
        "create unique index if not exists ",
        "create index if not exists ",
    ] {
        if let Some(start) = normalized.find(prefix) {
            let name_start = start + prefix.len();
            if let Some(end) = normalized[name_start..].find(' ') {
                let name_end = name_start + end;
                let name = &normalized[name_start..name_end];
                if let Some(unqualified) = name.rsplit('.').next() {
                    let unqualified = unqualified.to_string();
                    normalized.replace_range(name_start..name_end, &unqualified);
                }
            }
            break;
        }
    }
    normalized
}

/// Compare a trigger's enabled flag and catalog definition with its typed
/// expectation. Kept pure so tests can mutate one property at a time without
/// needing a live database.
#[must_use]
pub fn trigger_definition_matches(
    expected: &TriggerExpectation,
    enabled: &str,
    observed: &str,
) -> bool {
    enabled == expected.enabled
        && expected
            .definition_markers
            .iter()
            .all(|marker| normalize_sql(observed).contains(&normalize_sql(marker)))
}

/// Compare a live `pg_proc.prosrc` body against a source declaration hash.
#[must_use]
pub fn function_body_matches(expected_hash: &str, observed_body: &str) -> bool {
    body_hash(observed_body) == expected_hash
}

/// Check whether one live CHECK definition contains the complete declared
/// body-marker set. Markers must occur on the same constraint, preventing a
/// weakened CHECK from passing because its clauses were split across rows.
#[must_use]
pub fn inline_check_matches(markers: &[String], definitions: &[String]) -> bool {
    definitions
        .iter()
        .any(|definition| markers.iter().all(|marker| definition.contains(marker)))
}

fn render_expr(expr: &sea_query::SimpleExpr) -> String {
    let rendered = Query::select()
        .expr(expr.clone())
        .to_string(PostgresQueryBuilder);
    rendered
        .strip_prefix("SELECT ")
        .unwrap_or(&rendered)
        .to_string()
}

fn render_column_type(ty: &ColumnType) -> String {
    match ty {
        ColumnType::Char(None) => "char".to_string(),
        ColumnType::Char(Some(n)) => format!("char({n})"),
        ColumnType::String(_) => "varchar".to_string(),
        ColumnType::Text => "text".to_string(),
        ColumnType::TinyInteger | ColumnType::SmallInteger => "smallint".to_string(),
        ColumnType::Integer | ColumnType::Unsigned => "integer".to_string(),
        ColumnType::BigInteger | ColumnType::BigUnsigned => "bigint".to_string(),
        ColumnType::TinyUnsigned => "smallint".to_string(),
        ColumnType::SmallUnsigned => "smallint".to_string(),
        ColumnType::Float => "real".to_string(),
        ColumnType::Double => "double precision".to_string(),
        ColumnType::Decimal(None) => "numeric".to_string(),
        ColumnType::Decimal(Some((p, s))) => format!("numeric({p},{s})"),
        ColumnType::DateTime => "timestamp without time zone".to_string(),
        ColumnType::Timestamp => "timestamp".to_string(),
        ColumnType::TimestampWithTimeZone => "timestamp with time zone".to_string(),
        ColumnType::Time => "time".to_string(),
        ColumnType::Date => "date".to_string(),
        ColumnType::Interval(_, _) => "interval".to_string(),
        ColumnType::Binary(_) | ColumnType::VarBinary(_) => "bytea".to_string(),
        ColumnType::Bit(None) => "bit".to_string(),
        ColumnType::Bit(Some(n)) => format!("bit({n})"),
        ColumnType::VarBit(n) => format!("bit varying({n})"),
        ColumnType::Boolean => "boolean".to_string(),
        ColumnType::Money(_) => "money".to_string(),
        ColumnType::Json => "json".to_string(),
        ColumnType::JsonBinary => "jsonb".to_string(),
        ColumnType::Uuid => "uuid".to_string(),
        ColumnType::Custom(name) => name.to_string(),
        ColumnType::Enum { name, .. } => name.to_string(),
        ColumnType::Array(inner) => format!("{}[]", render_column_type(inner)),
        ColumnType::Vector(None) => "vector".to_string(),
        ColumnType::Vector(Some(n)) => format!("vector({n})"),
        ColumnType::Cidr => "cidr".to_string(),
        ColumnType::Inet => "inet".to_string(),
        ColumnType::MacAddr => "macaddr".to_string(),
        ColumnType::LTree => "ltree".to_string(),
        _ => "unknown".to_string(),
    }
}

fn generated_expression(spec: &[ColumnSpec]) -> Option<String> {
    spec.iter().find_map(|item| match item {
        ColumnSpec::Generated { expr, .. } => Some(render_expr(expr)),
        ColumnSpec::Extra(extra) if extra.to_ascii_uppercase().contains("GENERATED") => {
            let upper = extra.to_ascii_uppercase();
            let start = upper.find("AS (")? + 4;
            let end = extra[start..].rfind(')')? + start;
            Some(extra[start..end].to_string())
        }
        _ => None,
    })
}

fn default_expression(spec: &[ColumnSpec]) -> Option<String> {
    spec.iter().find_map(|item| match item {
        ColumnSpec::Default(expr) => Some(render_expr(expr)),
        ColumnSpec::Extra(extra) => {
            let upper = extra.to_ascii_uppercase();
            let start = upper.find("DEFAULT ")? + 8;
            Some(extra[start..].trim().to_string())
        }
        _ => None,
    })
}

fn column_expectations(stmt: &TableCreateStatement) -> Vec<ColumnExpectation> {
    stmt.get_columns()
        .iter()
        .map(|column| {
            let mut nullable = true;
            for spec in column.get_column_spec() {
                match spec {
                    ColumnSpec::NotNull | ColumnSpec::PrimaryKey => nullable = false,
                    ColumnSpec::Null => nullable = true,
                    _ => {}
                }
            }
            ColumnExpectation {
                name: column.get_column_name(),
                type_sql: column
                    .get_column_type()
                    .map_or_else(|| "unknown".to_string(), render_column_type),
                nullable,
                default_sql: default_expression(column.get_column_spec()),
                generated_sql: generated_expression(column.get_column_spec()),
                collation: None,
            }
        })
        .collect()
}

fn index_expectation(stmt: IndexCreateStatement) -> Option<IndexExpectation> {
    let definition = stmt.to_string(PostgresQueryBuilder);
    let name = extract_index_name(&definition)?;
    Some(IndexExpectation {
        name,
        definition: normalize_sql(&definition),
    })
}

fn raw_index_expectation(sql: String) -> Option<IndexExpectation> {
    let name = extract_index_name(&sql)?;
    Some(IndexExpectation {
        name,
        definition: normalize_sql(&sql),
    })
}

fn extract_index_name(sql: &str) -> Option<String> {
    let lower = sql.to_ascii_lowercase();
    let after = lower
        .find("if not exists")
        .map(|pos| &sql[pos + "if not exists".len()..])
        .or_else(|| {
            lower
                .find(" index ")
                .map(|pos| &sql[pos + " index ".len()..])
        })?;
    let name = after
        .split_whitespace()
        .next()?
        .trim_matches('"')
        .to_string();
    (!name.is_empty()).then_some(name)
}

fn indexes_for(qualified: &str) -> Vec<IndexExpectation> {
    let mut indexes = Vec::new();
    let mut push_sea = |items: Vec<IndexCreateStatement>| {
        indexes.extend(items.into_iter().filter_map(index_expectation));
    };
    match qualified {
        "core.events" => {
            push_sea(Events::create_indexes());
            indexes.extend(
                Events::create_gin_indexes_sql()
                    .into_iter()
                    .filter_map(raw_index_expectation),
            );
            for sql in [
                Events::create_claim_adjudication_index_sql(),
                Events::create_text_search_index_sql(),
            ] {
                if let Some(index) = raw_index_expectation(sql) {
                    indexes.push(index);
                }
            }
        }
        "raw.source_material_registry" => push_sea(SourceMaterialRegistry::create_indexes()),
        "raw.source_material_links" => push_sea(SourceMaterialLinks::create_indexes()),
        "raw.temporal_ledger" => push_sea(TemporalLedger::create_indexes()),
        "core.blobs" => push_sea(Blobs::create_indexes()),
        "core.entities" => push_sea(Entities::create_indexes()),
        "core.entity_relations" => push_sea(EntityRelations::create_indexes()),
        "sinex_schemas.event_payload_schemas" => {
            push_sea(EventPayloadSchemas::create_indexes());
        }
        "audit.archived_events" => indexes.extend(
            ArchivedEvents::create_indexes_sql()
                .into_iter()
                .filter_map(raw_index_expectation),
        ),
        _ => {}
    }
    indexes
}

/// Build typed expectations from the canonical convergence/table definitions.
pub fn table_expectations() -> Result<Vec<TableExpectation>, ApplyError> {
    let mut expectations = Vec::new();
    for table in convergible_tables()? {
        let statement = (table.statement_fn)();
        let mut foreign_keys: Vec<ForeignKeyExpectation> = statement
            .get_foreign_key_create_stmts()
            .iter()
            .map(|foreign_key| ForeignKeyExpectation {
                name: None,
                definition: normalize_sql(&foreign_key.to_string(PostgresQueryBuilder)),
            })
            .chain(
                table
                    .foreign_keys
                    .iter()
                    .map(|foreign_key| ForeignKeyExpectation {
                        name: Some(foreign_key.name.to_string()),
                        definition: normalize_sql(
                            &(foreign_key.statement_fn)().to_string(PostgresQueryBuilder),
                        ),
                    }),
            )
            .collect();
        foreign_keys.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then(left.definition.cmp(&right.definition))
        });
        foreign_keys.dedup();
        let constraints = table
            .named_constraints
            .iter()
            .map(|constraint| ConstraintExpectation {
                name: Some(constraint.name.to_string()),
                body: normalize_sql(&format!("CHECK ({})", constraint.expression)),
                markers: Vec::new(),
            })
            .collect();
        expectations.push(TableExpectation {
            schema: table.meta.schema.to_string(),
            table: table.meta.name.to_string(),
            columns: column_expectations(&statement),
            constraints,
            foreign_keys,
            indexes: indexes_for(table.meta.qualified_name),
        });
    }
    Ok(expectations)
}

/// The inline checks are intentionally explicit because PostgreSQL generates
/// their names from table state and sea-query exposes no stable name for them.
pub fn inline_check_expectations() -> Vec<ConstraintExpectation> {
    let all = [
        (
            "core.events",
            "xor_provenance",
            [
                "source_material_id IS NOT NULL",
                "source_event_ids IS NULL",
                "source_material_id IS NULL",
                "source_event_ids IS NOT NULL",
            ],
        ),
        (
            "reflection.events",
            "xor_provenance",
            [
                "source_material_id IS NOT NULL",
                "source_event_ids IS NULL",
                "source_material_id IS NULL",
                "source_event_ids IS NOT NULL",
            ],
        ),
        (
            "audit.archived_events",
            "xor_provenance",
            [
                "source_material_id IS NOT NULL",
                "source_event_ids IS NULL",
                "source_material_id IS NULL",
                "source_event_ids IS NOT NULL",
            ],
        ),
    ];
    all.into_iter()
        .map(|(location, label, markers)| ConstraintExpectation {
            name: Some(format!("{location}::{label}")),
            body: String::new(),
            markers: markers.into_iter().map(str::to_string).collect(),
        })
        .collect()
}

const TRIGGERS: &[TriggerExpectation] = &[
    TriggerExpectation {
        schema: "core",
        table: "events",
        name: "trg_events_no_update",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "core.fn_events_no_update()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "events",
        name: "trg_events_validate_payload",
        enabled: "O",
        definition_markers: &[
            "before insert",
            "for each row",
            "core.fn_events_validate_payload()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "events",
        name: "trg_events_validate_material_bounds",
        enabled: "O",
        definition_markers: &[
            "before insert",
            "for each row",
            "core.fn_events_validate_material_bounds()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "events",
        name: "trg_events_archive_before_delete",
        enabled: "O",
        definition_markers: &[
            "before delete",
            "for each row",
            "core.fn_archive_before_delete()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "events",
        name: "trg_events_maintain_material_event_count_insert",
        enabled: "O",
        definition_markers: &[
            "after insert",
            "for each statement",
            "core.fn_events_increment_material_event_count()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "events",
        name: "trg_events_maintain_material_event_count_delete",
        enabled: "O",
        definition_markers: &[
            "after delete",
            "for each row",
            "core.fn_events_decrement_material_event_count()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "events",
        name: "trg_document_projection",
        enabled: "O",
        definition_markers: &[
            "after insert",
            "for each row",
            "core.fn_document_projection()",
            "when",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "events",
        name: "trg_events_enforce_product_declaration",
        enabled: "O",
        definition_markers: &[
            "before insert or update",
            "for each row",
            "derivation.enforce_event_product_declaration()",
        ],
    },
    TriggerExpectation {
        schema: "reflection",
        table: "events",
        name: "trg_events_no_update",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "reflection.fn_events_no_update()",
        ],
    },
    TriggerExpectation {
        schema: "reflection",
        table: "events",
        name: "trg_events_validate_payload",
        enabled: "O",
        definition_markers: &[
            "before insert",
            "for each row",
            "reflection.fn_events_validate_payload()",
        ],
    },
    TriggerExpectation {
        schema: "reflection",
        table: "events",
        name: "trg_events_validate_material_bounds",
        enabled: "O",
        definition_markers: &[
            "before insert",
            "for each row",
            "reflection.fn_events_validate_material_bounds()",
        ],
    },
    TriggerExpectation {
        schema: "reflection",
        table: "events",
        name: "trg_events_enforce_product_declaration",
        enabled: "O",
        definition_markers: &[
            "before insert or update",
            "for each row",
            "derivation.enforce_event_product_declaration()",
        ],
    },
    TriggerExpectation {
        schema: "raw",
        table: "source_material_registry",
        name: "trg_source_material_validate_event_bounds",
        enabled: "O",
        definition_markers: &[
            "before insert or update",
            "for each row",
            "raw.fn_source_material_validate_event_bounds()",
        ],
    },
    TriggerExpectation {
        schema: "raw",
        table: "temporal_ledger",
        name: "trg_tl_no_update_delete",
        enabled: "O",
        definition_markers: &[
            "before update or delete",
            "for each row",
            "raw.fn_temporal_ledger_append_only()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "entities",
        name: "trg_entities_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "entity_relations",
        name: "trg_entity_relations_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "event_annotations",
        name: "trg_event_annotations_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "sinex_schemas",
        table: "event_payload_schemas",
        name: "trg_event_payload_schemas_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "sinex_schemas",
        table: "dlq_events",
        name: "set_timestamp",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "email_provider_state",
        name: "trg_email_provider_state_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "email_mailbox_projection",
        name: "trg_email_mailbox_projection_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "source_session_state",
        name: "trg_source_session_state_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "core",
        table: "embedding_models",
        name: "trg_embedding_model_create_index",
        enabled: "O",
        definition_markers: &[
            "after insert",
            "for each row",
            "core.embedding_model_index_trigger()",
        ],
    },
    TriggerExpectation {
        schema: "privacy",
        table: "recognizer_backends",
        name: "trg_privacy_recognizer_backends_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "privacy",
        table: "dictionaries",
        name: "trg_privacy_dictionaries_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
    TriggerExpectation {
        schema: "privacy",
        table: "rules",
        name: "trg_privacy_rules_updated_at",
        enabled: "O",
        definition_markers: &[
            "before update",
            "for each row",
            "public.set_current_timestamp_updated_at()",
        ],
    },
];

/// All schema-managed trigger declarations used by both apply's object-drift
/// report and strict-diff's exact catalog verification.
#[must_use]
pub fn trigger_expectations() -> &'static [TriggerExpectation] {
    TRIGGERS
}

fn function_expectation(
    signature: &'static str,
    function_name: &str,
    sql: &str,
) -> Option<FunctionExpectation> {
    let lower = sql.to_ascii_lowercase();
    let marker = format!("function {function_name}").to_ascii_lowercase();
    let start = lower.find(&marker)?;
    let tail = &sql[start..];
    let lower_tail = tail.to_ascii_lowercase();
    let (open, close) = if let Some(pos) = lower_tail.find("as $$") {
        (pos + 5, "$$")
    } else if let Some(pos) = lower_tail.find("as $function$") {
        (pos + 13, "$function$")
    } else {
        return None;
    };
    let body = &tail[open..];
    let end = body.find(close)?;
    Some(FunctionExpectation {
        signature,
        body_hash: body_hash(&body[..end]),
    })
}

/// Hash a function body after the same canonicalization used by the live
/// catalog verifier. This is public so tests can prove body mutation is not
/// accepted by a marker-only check.
#[must_use]
pub fn body_hash(body: &str) -> String {
    blake3::hash(normalize_sql(body).as_bytes())
        .to_hex()
        .to_string()
}

/// Source-declared trigger/function bodies. These are all defined by the
/// schema modules, so strict-diff never maintains a second marker list.
pub fn function_expectations() -> Vec<FunctionExpectation> {
    let sources: &[(&str, &'static str, &'static str)] = &[
        (
            "core.fn_events_no_update",
            "core.fn_events_no_update()",
            Events::create_no_update_trigger_sql(),
        ),
        (
            "core.fn_events_validate_payload",
            "core.fn_events_validate_payload()",
            Events::create_payload_validation_trigger_sql(),
        ),
        (
            "core.fn_events_validate_material_bounds",
            "core.fn_events_validate_material_bounds()",
            Events::create_material_bounds_trigger_sql(),
        ),
        (
            "derivation.enforce_event_product_declaration",
            "derivation.enforce_event_product_declaration()",
            Events::create_product_declaration_trigger_sql(),
        ),
        (
            "core.fn_events_increment_material_event_count",
            "core.fn_events_increment_material_event_count()",
            Events::create_material_event_count_trigger_sql(),
        ),
        (
            "core.fn_events_decrement_material_event_count",
            "core.fn_events_decrement_material_event_count()",
            Events::create_material_event_count_trigger_sql(),
        ),
        (
            "core.fn_archive_before_delete",
            "core.fn_archive_before_delete()",
            ArchivedEvents::create_archive_trigger_sql(),
        ),
        (
            "raw.fn_source_material_validate_event_bounds",
            "raw.fn_source_material_validate_event_bounds()",
            SourceMaterialRegistry::create_event_bounds_trigger_sql(),
        ),
        (
            "raw.fn_temporal_ledger_append_only",
            "raw.fn_temporal_ledger_append_only()",
            TemporalLedger::create_append_only_trigger_sql(),
        ),
        (
            "core.fn_document_projection",
            "core.fn_document_projection()",
            crate::defs::DocumentChunks::create_projection_trigger_sql(),
        ),
    ];
    let mut expectations: Vec<_> = sources
        .iter()
        .filter_map(|(name, signature, sql)| function_expectation(*signature, name, sql))
        .collect();
    let reflection_sql = [
        Events::create_no_update_trigger_sql(),
        Events::create_payload_validation_trigger_sql(),
        Events::create_material_bounds_trigger_sql(),
    ]
    .join("\n")
    .replace("core.fn_events", "reflection.fn_events")
    .replace("core.events", "reflection.events");
    for (name, signature) in [
        (
            "reflection.fn_events_no_update",
            "reflection.fn_events_no_update()",
        ),
        (
            "reflection.fn_events_validate_payload",
            "reflection.fn_events_validate_payload()",
        ),
        (
            "reflection.fn_events_validate_material_bounds",
            "reflection.fn_events_validate_material_bounds()",
        ),
    ] {
        if let Some(expectation) = function_expectation(signature, name, &reflection_sql) {
            expectations.push(expectation);
        }
    }
    let updated_at_sql = r#"
        CREATE OR REPLACE FUNCTION public.set_current_timestamp_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.updated_at = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#;
    if let Some(expectation) = function_expectation(
        "public.set_current_timestamp_updated_at()",
        "public.set_current_timestamp_updated_at",
        updated_at_sql,
    ) {
        expectations.push(expectation);
    }
    expectations
}

async fn live_columns(
    pool: &PgPool,
    expectation: &TableExpectation,
) -> Result<
    Option<HashMap<String, (String, bool, Option<String>, Option<String>, Option<String>)>>,
    ApplyError,
> {
    let rows = sqlx::query_as::<
        _,
        (
            String,
            String,
            bool,
            String,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
    >(
        r#"SELECT a.attname,
                  format_type(a.atttypid, a.atttypmod),
                  NOT a.attnotnull,
                  a.attgenerated::text,
                  pg_get_expr(d.adbin, d.adrelid),
                  ic.collation_name,
                  c.relname
             FROM pg_attribute a
             JOIN pg_class c ON c.oid = a.attrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
             LEFT JOIN information_schema.columns ic
               ON ic.table_schema = n.nspname
              AND ic.table_name = c.relname
              AND ic.column_name = a.attname
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attnum > 0
              AND NOT a.attisdropped
            ORDER BY a.attnum"#,
    )
    .bind(&expectation.schema)
    .bind(&expectation.table)
    .fetch_all(pool)
    .await?;
    if rows.is_empty() {
        let exists: bool =
            sqlx::query_scalar("SELECT to_regclass(format('%I.%I', $1, $2)) IS NOT NULL")
                .bind(&expectation.schema)
                .bind(&expectation.table)
                .fetch_one(pool)
                .await?;
        if !exists {
            return Ok(None);
        }
    }
    Ok(Some(
        rows.into_iter()
            .map(
                |(name, ty, nullable, generated, expression, collation, _)| {
                    (
                        name,
                        (
                            ty,
                            nullable,
                            (!generated.is_empty()).then_some(generated),
                            expression,
                            collation,
                        ),
                    )
                },
            )
            .collect(),
    ))
}

async fn check_table(
    pool: &PgPool,
    table: &TableExpectation,
) -> Result<Vec<ExpectationDrift>, ApplyError> {
    let mut drifts = Vec::new();
    let Some(live) = live_columns(pool, table).await? else {
        drifts.push(ExpectationDrift {
            kind: ExpectationKind::Table,
            location: format!("{}.{}", table.schema, table.table),
            declared: "present".to_string(),
            observed: "missing".to_string(),
        });
        return Ok(drifts);
    };
    for expected in &table.columns {
        let location = format!("{}.{}.{}", table.schema, table.table, expected.name);
        let Some((ty, nullable, generated, default, collation)) = live.get(&expected.name) else {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Column,
                location,
                declared: expected.type_sql.clone(),
                observed: "missing".to_string(),
            });
            continue;
        };
        if normalize_sql(ty) != normalize_sql(&expected.type_sql) {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Column,
                location: location.clone(),
                declared: format!("type {}", expected.type_sql),
                observed: format!("type {ty}"),
            });
        }
        if *nullable != expected.nullable {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Column,
                location: location.clone(),
                declared: format!("nullable={}", expected.nullable),
                observed: format!("nullable={nullable}"),
            });
        }
        let actual_generated = generated.as_deref().filter(|s| !s.is_empty());
        if expected.generated_sql.is_some() != actual_generated.is_some() {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Column,
                location: location.clone(),
                declared: format!("generated={:?}", expected.generated_sql),
                observed: format!("generated={actual_generated:?}"),
            });
        }
        if let Some(expected_default) = &expected.default_sql {
            if default.as_deref().map(normalize_sql) != Some(normalize_sql(expected_default)) {
                drifts.push(ExpectationDrift {
                    kind: ExpectationKind::Column,
                    location: location.clone(),
                    declared: format!("default {expected_default}"),
                    observed: format!("default {default:?}"),
                });
            }
        } else if default.is_some() {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Column,
                location: location.clone(),
                declared: "no default".to_string(),
                observed: format!("default {default:?}"),
            });
        }
        if let Some(expected_collation) = &expected.collation {
            if collation.as_deref().map(normalize_sql) != Some(normalize_sql(expected_collation)) {
                drifts.push(ExpectationDrift {
                    kind: ExpectationKind::Column,
                    location,
                    declared: format!("collation {expected_collation}"),
                    observed: format!("collation {collation:?}"),
                });
            }
        }
    }

    let constraints = sqlx::query_as::<_, (String, String)>(
        "SELECT c.conname, pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid JOIN pg_namespace n ON n.oid = r.relnamespace WHERE n.nspname = $1 AND r.relname = $2 AND c.contype = 'c'",
    )
    .bind(&table.schema)
    .bind(&table.table)
    .fetch_all(pool)
    .await?;
    for expected in &table.constraints {
        let Some(name) = &expected.name else { continue };
        let Some((_, actual)) = constraints
            .iter()
            .find(|(actual_name, _)| actual_name == name)
        else {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Constraint,
                location: format!("{}.{}::{name}", table.schema, table.table),
                declared: expected.body.clone(),
                observed: "missing".to_string(),
            });
            continue;
        };
        if normalize_sql(actual) != expected.body {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Constraint,
                location: format!("{}.{}::{name}", table.schema, table.table),
                declared: expected.body.clone(),
                observed: actual.clone(),
            });
        }
    }
    for expected in &table.foreign_keys {
        let foreign_keys = sqlx::query_as::<_, (Option<String>, String)>(
            "SELECT c.conname, pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid JOIN pg_namespace n ON n.oid = r.relnamespace WHERE n.nspname = $1 AND r.relname = $2 AND c.contype = 'f'",
        )
        .bind(&table.schema)
        .bind(&table.table)
        .fetch_all(pool)
        .await?;
        let matched = if let Some(name) = &expected.name {
            foreign_keys
                .iter()
                .find(|(actual_name, _)| actual_name.as_deref() == Some(name))
        } else {
            foreign_keys
                .iter()
                .find(|(_, actual)| normalize_sql(actual) == expected.definition)
        };
        if let Some((_, actual)) = matched {
            if normalize_sql(actual) != expected.definition {
                drifts.push(ExpectationDrift {
                    kind: ExpectationKind::ForeignKey,
                    location: format!(
                        "{}.{}::{}",
                        table.schema,
                        table.table,
                        expected.name.as_deref().unwrap_or("anonymous")
                    ),
                    declared: expected.definition.clone(),
                    observed: actual.clone(),
                });
            }
        } else {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::ForeignKey,
                location: format!(
                    "{}.{}::{}",
                    table.schema,
                    table.table,
                    expected.name.as_deref().unwrap_or("anonymous")
                ),
                declared: expected.definition.clone(),
                observed: "missing or definition changed".to_string(),
            });
        }
    }
    for expected in &table.indexes {
        let actual: Option<String> = sqlx::query_scalar(
            "SELECT pg_get_indexdef(i.indexrelid) FROM pg_class t JOIN pg_namespace n ON n.oid = t.relnamespace JOIN pg_index i ON i.indrelid = t.oid JOIN pg_class idx ON idx.oid = i.indexrelid WHERE n.nspname = $1 AND t.relname = $2 AND idx.relname = $3",
        )
        .bind(&table.schema)
        .bind(&table.table)
        .bind(&expected.name)
        .fetch_optional(pool)
        .await?;
        match actual {
            Some(actual) if !index_definition_matches(&expected.definition, &actual) => drifts
                .push(ExpectationDrift {
                    kind: ExpectationKind::Index,
                    location: format!("{}.{}::{}", table.schema, table.table, expected.name),
                    declared: expected.definition.clone(),
                    observed: actual,
                }),
            None => drifts.push(ExpectationDrift {
                kind: ExpectationKind::Index,
                location: format!("{}.{}::{}", table.schema, table.table, expected.name),
                declared: expected.definition.clone(),
                observed: "missing".to_string(),
            }),
            _ => {}
        }
    }
    Ok(drifts)
}

async fn check_triggers(pool: &PgPool) -> Result<Vec<ExpectationDrift>, ApplyError> {
    let mut drifts = Vec::new();
    for expected in TRIGGERS {
        let row: Option<(String, String)> = sqlx::query_as(
            "SELECT t.tgenabled::text, pg_get_triggerdef(t.oid, true) FROM pg_trigger t JOIN pg_class r ON r.oid = t.tgrelid JOIN pg_namespace n ON n.oid = r.relnamespace WHERE n.nspname = $1 AND r.relname = $2 AND t.tgname = $3 AND NOT t.tgisinternal",
        )
        .bind(expected.schema)
        .bind(expected.table)
        .bind(expected.name)
        .fetch_optional(pool)
        .await?;
        let location = format!("{}.{}::{}", expected.schema, expected.table, expected.name);
        let Some((enabled, definition)) = row else {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Trigger,
                location,
                declared: expected.definition_markers.join("; "),
                observed: "missing".to_string(),
            });
            continue;
        };
        if !trigger_definition_matches(expected, &enabled, &definition) {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Trigger,
                location,
                declared: format!(
                    "enabled={} {}",
                    expected.enabled,
                    expected.definition_markers.join("; ")
                ),
                observed: format!("enabled={enabled} {definition}"),
            });
        }
    }
    Ok(drifts)
}

async fn check_functions(pool: &PgPool) -> Result<Vec<ExpectationDrift>, ApplyError> {
    let mut drifts = Vec::new();
    for expected in function_expectations() {
        let body: Option<String> =
            sqlx::query_scalar("SELECT p.prosrc FROM pg_proc p WHERE p.oid = to_regprocedure($1)")
                .bind(expected.signature)
                .fetch_optional(pool)
                .await?;
        let location = expected.signature.to_string();
        let Some(body) = body else {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Function,
                location,
                declared: format!("body_hash={}", expected.body_hash),
                observed: "missing".to_string(),
            });
            continue;
        };
        let actual_hash = body_hash(&body);
        if !function_body_matches(&expected.body_hash, &body) {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Function,
                location,
                declared: format!("body_hash={}", expected.body_hash),
                observed: format!("body_hash={actual_hash}"),
            });
        }
    }
    Ok(drifts)
}

/// Run the shared verify interpreter over every migrated expectation kind.
pub async fn check_catalog(pool: &PgPool) -> Result<Vec<ExpectationDrift>, ApplyError> {
    let mut drifts = Vec::new();
    for table in table_expectations()? {
        drifts.extend(check_table(pool, &table).await?);
    }
    for inline in inline_check_expectations() {
        let Some(location) = inline.name else {
            continue;
        };
        let (qualified, _) = location.split_once("::").unwrap_or((&location, ""));
        let Some((schema, table)) = qualified.split_once('.') else {
            continue;
        };
        let definitions: Vec<String> = sqlx::query_scalar(
            "SELECT pg_get_constraintdef(c.oid) FROM pg_constraint c JOIN pg_class r ON r.oid = c.conrelid JOIN pg_namespace n ON n.oid = r.relnamespace WHERE n.nspname = $1 AND r.relname = $2 AND c.contype = 'c'",
        )
        .bind(schema)
        .bind(table)
        .fetch_all(pool)
        .await?;
        if !inline_check_matches(&inline.markers, &definitions) {
            drifts.push(ExpectationDrift {
                kind: ExpectationKind::Constraint,
                location,
                declared: inline.markers.join(" AND "),
                observed: if definitions.is_empty() {
                    "no CHECK constraints".to_string()
                } else {
                    format!(
                        "{} CHECK constraint(s); no exact body match",
                        definitions.len()
                    )
                },
            });
        }
    }
    drifts.extend(check_triggers(pool).await?);
    drifts.extend(check_functions(pool).await?);
    Ok(drifts)
}

#[cfg(test)]
#[path = "expectation_test.rs"]
mod tests;
