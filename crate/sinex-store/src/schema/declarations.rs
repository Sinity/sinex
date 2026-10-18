//! DDL and fingerprints for each schema version. A version is the number of
//! upgrade changes applied (0 is the first release); published versions keep
//! their fingerprints, so older declarations are derived here from the current ones.
use super::tables::{Column, Definition, TABLES};
use super::{CAPTURED_FUTURE_SKEW_SECONDS, TIMESCALEDB_VERSION};
use crate::{Error, Result};
use sea_query::{Alias, ColumnDef, Expr, PostgresQueryBuilder, Table};
use sinex_model::events::TIME_CLOCK_LABELS;
use sqlx::{Executor, PgConnection};

pub(super) fn enum_check(column: &str, labels: &[&str]) -> String {
    let values: Vec<String> = labels
        .iter()
        .map(|label| ["'", &label.replace("'", "''"), "'"].concat())
        .collect();
    ["CHECK (", column, " IN (", &values.join(","), "))"].concat()
}

pub(super) fn record_state_check() -> Result<String> {
    use sinex_model::events::RecordState;
    let mut labels = Vec::new();
    for state in [RecordState::Present, RecordState::Removed] {
        match serde_json::to_value(state)
            .map_err(|_| Error::Schema("record-state catalog cannot serialize".into()))?
        {
            serde_json::Value::String(label) => labels.push(label),
            _ => {
                return Err(Error::Schema(
                    "record-state catalog must use string labels".into(),
                ));
            }
        }
    }
    Ok(enum_check(
        "record_state",
        &labels.iter().map(String::as_str).collect::<Vec<_>>(),
    ))
}

// Version 0 is the first release (4b5bf0619). Older versions reuse the current
// declarations except where a difference is spelled out.
pub(super) const OLD_SHELL_CHECK: &str = "CHECK (event_type<>'command.canonical' OR (payload_schema_id IS NOT DISTINCT FROM 3 AND source='fusion.shell-command' AND cardinality(source_event_ids)=1 AND jsonb_typeof(payload->'command') IS NOT DISTINCT FROM 'string' AND jsonb_typeof(payload->'observed_by') IS NOT DISTINCT FROM 'array'))";
pub(super) const SEARCH_TOKEN_COLUMNS: &[Column] = &[
    col!("generation", "uuid"),
    col!("id", "uuid"),
    col!("ts_orig", "timestamp with time zone", null),
    col!("ts_orig_tiebreak", "integer", null),
    col!("source", "text"),
    col!("event_type", "text"),
    col!("tokens", "text", null),
    col!("text", "text"),
    col!("command", "text"),
    col!("command_normalized", "text"),
    col!("cwd_normalized", "text", null),
    col!("cwd", "text", null),
    col!("exit_code", "bigint", null),
];
pub(super) fn installed_tables(stage: usize) -> Vec<Definition> {
    TABLES
        .iter()
        .filter(|d| {
            !(stage == 0 && d.name == "applied_changes"
                || stage < 2 && d.name == "shell_progress"
                || stage < 3 && d.name.starts_with("session_"))
        })
        .map(|d| Definition {
            schema: d.schema,
            name: d.name,
            columns: if stage < 2 && d.name == "shell_dirty" {
                &d.columns[..1]
            } else if stage >= 7 && d.name == "search_text" {
                SEARCH_TOKEN_COLUMNS
            } else {
                d.columns
            },
            constraints: d.constraints,
        })
        .collect()
}
pub(super) fn nullable_at(stage: usize, table: &Definition, column: &Column) -> bool {
    column.nullable && !(stage < 5 && table.name == "search_text" && column.name == "tsv")
}
// Search indexes of versions 5 and 6, part of their published fingerprints.
pub(super) const LEGACY_UNAVAILABLE_INDEX: &str =
    "CREATE INDEX search_text_unavailable ON core.search_text(generation,id) WHERE tsv IS NULL";
pub(super) const LEGACY_VECTOR_INDEX: &str =
    "CREATE INDEX search_text_tsv ON core.search_text USING gin(tsv)";
pub(super) fn storage_at(stage: usize) -> Vec<String> {
    super::storage::declarations()
        .into_iter()
        .filter_map(|sql| {
            if sql == super::storage::FULL_TEXT_UNAVAILABLE_INDEX {
                if stage < 5 {
                    return None;
                }
                if stage < 7 {
                    return Some(LEGACY_UNAVAILABLE_INDEX.to_owned());
                }
            }
            if stage < 7 && sql.starts_with("CREATE INDEX search_text_tokens ") {
                return Some(LEGACY_VECTOR_INDEX.to_owned());
            }
            Some(sql)
        })
        .collect()
}
pub(super) fn declarations() -> Result<Vec<String>> {
    declarations_at(7)
}
pub(super) fn declarations_at(stage: usize) -> Result<Vec<String>> {
    installed_tables(stage)
        .iter()
        .map(|d| {
            let mut table = Table::create();
            table.table((Alias::new(d.schema), Alias::new(d.name)));
            for c in d.columns {
                let mut column = ColumnDef::new(Alias::new(c.name));
                column.custom(Alias::new(c.ty));
                if !nullable_at(stage, d, c) {
                    column.not_null();
                }
                if let Some(expr) = c.default {
                    column.default(Expr::cust(expr));
                }
                if let Some(expr) = c.generated {
                    column.generated(Expr::cust(expr), true);
                }
                table.col(&mut column);
            }
            let mut sql = table.to_string(PostgresQueryBuilder);
            // Only constants from this module enter DDL, never operator input.
            sql.pop();
            for (name, body) in d.constraints {
                if stage==0 && *name=="registered_zsh_payload" || stage<4 && *name=="registered_browser_payload" { continue; }
                sql.push_str(", CONSTRAINT \"");
                sql.push_str(name);
                sql.push_str("\" ");
                let generated = match *name {
                    "registered_shell_payload" if stage==0 => OLD_SHELL_CHECK.to_owned(),
                    "input_kind" if stage==0 => "CHECK (kind='sqlite')".to_owned(),
                    "event_quality" => enum_check("ts_quality",sinex_model::events::TIME_QUALITY_LABELS),
                    "event_precision" => enum_check("ts_precision",sinex_model::events::TIME_PRECISION_LABELS),
                    "event_record_state" => record_state_check()?,
                    "event_clock" => enum_check("ts_clock",TIME_CLOCK_LABELS),
                    "captured_not_ahead" => ["CHECK (ts_quality IS DISTINCT FROM 'captured' OR ts_clock <> 'sinex' OR ts_orig IS NULL OR ts_orig <= uuid_extract_timestamp(id)+interval '",&CAPTURED_FUTURE_SKEW_SECONDS.to_string()," seconds')"].concat(),
                    _ => (*body).to_owned(),
                };
                sql.push_str(&generated);
            }
            sql.push(')');
            Ok(sql)
        })
        .collect()
}

pub(super) async fn configure_events(conn: &mut PgConnection) -> Result<()> {
    for sql in super::storage::declarations() {
        conn.execute(sql.as_str()).await?;
    }
    Ok(())
}

pub(super) fn fingerprint() -> Result<String> {
    fingerprint_at(7)
}
pub(super) fn fingerprint_at(stage: usize) -> Result<String> {
    Ok(blake3::hash(
        [
            declarations_at(stage)?,
            storage_at(stage),
            vec![TIMESCALEDB_VERSION.to_owned()],
            if stage >= 6 {
                // The Atuin payload schema enters the fingerprint from version 6, so
                // earlier fingerprints stay as published.
                let mut schema = atuin_payload_schema_at(stage);
                schema.sort_all_objects();
                vec![schema.to_string()]
            } else {
                vec![]
            },
        ]
        .concat()
        .join("\n")
        .as_bytes(),
    )
    .to_hex()
    .to_string())
}

pub(super) fn atuin_payload_schema_at(stage: usize) -> serde_json::Value {
    let mut schema = crate::atuin::payload_schema();
    if stage < 6 {
        schema["properties"]["exit_code"]["minimum"] = serde_json::json!(0);
    }
    schema
}

pub(super) fn first_release_shell_payload() -> serde_json::Value {
    use serde_json::json;
    let mut schema = atuin_payload_schema_at(5);
    let properties = schema["properties"].as_object_mut().unwrap();
    properties.insert("canonical".into(), json!({"type":["string","null"]}));
    properties.insert(
        "observed_by".into(),
        json!({"type":"array","items":{"const":crate::ATUIN_SOURCE},"minItems":1,"maxItems":1}),
    );
    properties.insert(
        "association".into(),
        json!({"type":"object","required":["basis","rule","version","ambiguous","candidates"]}),
    );
    properties.insert(
        "field_evidence".into(),
        json!({"type":"object","required":["all"]}),
    );
    for field in ["canonical", "observed_by", "association", "field_evidence"] {
        schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!(field));
    }
    schema
}
