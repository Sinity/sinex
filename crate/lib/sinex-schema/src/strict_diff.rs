//! Strict drift detection — extends `apply::diff` with categories that the
//! convergence engine does not currently reconcile.
//!
//! # Why this module exists
//!
//! `apply::diff` reports missing tables, columns, named constraints, indexes,
//! triggers (by name), views, and continuous aggregates. It is silent on:
//!
//! 1. **Inline column CHECK expressions** — `apply.rs` declares them via
//!    sea-query column statements; if the expression in source changes, the
//!    live constraint is not replaced because there is no named-constraint
//!    handle to converge against.
//! 2. **DEFAULT expressions** on columns that already exist. `ADD COLUMN IF
//!    NOT EXISTS` is a no-op for existing columns, so a changed DEFAULT in
//!    source does not propagate.
//! 3. **Foreign key ON DELETE / ON UPDATE actions**. The FK exists by
//!    name, so convergence skips it; the action change is not applied.
//! 4. **Trigger function bodies**. `CREATE OR REPLACE FUNCTION` does
//!    overwrite the body on each apply, but a manual prod edit between
//!    applies is silently overwritten with no warning, and there is no
//!    way to detect drift before re-applying.
//! 5. **TimescaleDB hypertable settings** (chunk interval, compression,
//!    retention).
//! 6. **Comments / table descriptions**.
//!
//! See issue #556. The module ships a first slice covering the highest-value
//! categories — trigger function bodies and column defaults. The other
//! categories are wired up as `Unimplemented` stubs so the public surface is
//! stable and follow-up PRs can fill them in without churning callers.
//!
//! # Caller surface
//!
//! ```ignore
//! use sinex_schema::strict_diff::{check_strict, StrictDrift};
//!
//! let drifts: Vec<StrictDrift> = check_strict(&pool).await?;
//! for drift in drifts {
//!     eprintln!("{drift}");
//! }
//! ```

use crate::apply::ApplyError;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::fmt;

/// Categories of drift that the strict diff checks for.
///
/// Each variant maps to one detection routine in this module. New categories
/// extend the enum and add a check routine; callers iterate over the
/// resulting `Vec<StrictDrift>` without knowing the per-category mechanics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DriftCategory {
    /// Body of a trigger or stored function diverged from the declared
    /// snapshot. The convergence engine writes the declared body via
    /// `CREATE OR REPLACE FUNCTION`, but a manual edit between applies will
    /// silently survive until the next apply and then be silently
    /// overwritten. The strict diff catches this gap.
    TriggerBody,
    /// A column's runtime DEFAULT expression diverges from the declared one.
    /// `ADD COLUMN IF NOT EXISTS` is a no-op on existing columns, so default
    /// changes never propagate without explicit DDL — silent drift.
    ColumnDefault,
    /// Reserved for follow-up: inline column `CHECK` expressions.
    InlineCheckExpr,
    /// Reserved for follow-up: foreign key `ON DELETE` / `ON UPDATE` actions.
    ForeignKeyAction,
    /// Reserved for follow-up: TimescaleDB hypertable chunk interval,
    /// compression, retention.
    HypertableSetting,
    /// Reserved for follow-up: comments / table descriptions.
    Comment,
}

impl fmt::Display for DriftCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TriggerBody => write!(f, "trigger_body"),
            Self::ColumnDefault => write!(f, "column_default"),
            Self::InlineCheckExpr => write!(f, "inline_check_expr"),
            Self::ForeignKeyAction => write!(f, "foreign_key_action"),
            Self::HypertableSetting => write!(f, "hypertable_setting"),
            Self::Comment => write!(f, "comment"),
        }
    }
}

/// One drift finding. Both `declared_summary` and `observed_summary` are
/// human-readable rather than canonical so the JSON output is operator
/// friendly; programmatic callers can match on `category` and `location`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrictDrift {
    pub category: DriftCategory,
    /// Schema-qualified locator: `core.events`, `core.events.ts_persisted`,
    /// `core.expand_cascade`, etc. Format depends on the category.
    pub location: String,
    /// Short summary of what the source-of-truth declares.
    pub declared_summary: String,
    /// Short summary of what the live database has.
    pub observed_summary: String,
}

impl fmt::Display for StrictDrift {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[{}] {}: declared {{{}}} vs observed {{{}}}",
            self.category, self.location, self.declared_summary, self.observed_summary
        )
    }
}

/// Run the strict drift check against the live database.
///
/// Returns an empty Vec when the live state matches every declared category,
/// or a list of `StrictDrift` entries (one per drift). The order is stable
/// per category but not stable across categories.
pub async fn check_strict(pool: &PgPool) -> Result<Vec<StrictDrift>, ApplyError> {
    let mut drifts = Vec::new();
    drifts.extend(check_column_defaults(pool).await?);
    drifts.extend(check_trigger_function_bodies(pool).await?);
    Ok(drifts)
}

// ─── Column defaults ────────────────────────────────────────────────────────

/// One declared column-default expectation.
struct DeclaredDefault {
    schema: &'static str,
    table: &'static str,
    column: &'static str,
    /// Substring that the live DEFAULT expression MUST contain. We compare
    /// by substring rather than equality because Postgres normalizes
    /// expressions on read (e.g. `now()` may be stored as `now()` or
    /// `pg_catalog.now()` depending on search_path at the time of DDL).
    /// Substring lets us pin the meaningful identifier without brittle
    /// formatting matches.
    expected_marker: &'static str,
}

const DECLARED_DEFAULTS: &[DeclaredDefault] = &[
    DeclaredDefault {
        schema: "core",
        table: "events",
        column: "ts_persisted",
        expected_marker: "CURRENT_TIMESTAMP",
    },
    DeclaredDefault {
        schema: "core",
        table: "blobs",
        column: "metadata",
        expected_marker: "{}",
    },
    DeclaredDefault {
        schema: "core",
        table: "entities",
        column: "updated_at",
        expected_marker: "CURRENT_TIMESTAMP",
    },
];

async fn check_column_defaults(pool: &PgPool) -> Result<Vec<StrictDrift>, ApplyError> {
    let mut drifts = Vec::new();
    for declared in DECLARED_DEFAULTS {
        let observed: Option<String> = sqlx::query_scalar(
            r"
            SELECT pg_get_expr(d.adbin, d.adrelid)
            FROM pg_attrdef d
            JOIN pg_attribute a ON a.attrelid = d.adrelid AND a.attnum = d.adnum
            JOIN pg_class c ON c.oid = d.adrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = $1
              AND c.relname = $2
              AND a.attname = $3
            ",
        )
        .bind(declared.schema)
        .bind(declared.table)
        .bind(declared.column)
        .fetch_optional(pool)
        .await?;

        let location = format!(
            "{}.{}.{}",
            declared.schema, declared.table, declared.column
        );

        match observed {
            None => {
                drifts.push(StrictDrift {
                    category: DriftCategory::ColumnDefault,
                    location,
                    declared_summary: format!("contains `{}`", declared.expected_marker),
                    observed_summary: "no DEFAULT set".to_string(),
                });
            }
            Some(expr) if !expr.contains(declared.expected_marker) => {
                drifts.push(StrictDrift {
                    category: DriftCategory::ColumnDefault,
                    location,
                    declared_summary: format!("contains `{}`", declared.expected_marker),
                    observed_summary: expr,
                });
            }
            Some(_) => {} // matches; no drift
        }
    }
    Ok(drifts)
}

// ─── Trigger function bodies ────────────────────────────────────────────────

/// One declared expectation for a stored function body.
struct DeclaredFunctionBody {
    schema: &'static str,
    function_name: &'static str,
    /// Substrings that MUST appear in the live function body. We compare
    /// by markers rather than full-body hash because Postgres normalizes
    /// whitespace and stripping comments differently than the source SQL,
    /// so an exact-match check would false-positive on every apply. Markers
    /// pin the load-bearing logic; if a manual edit removes the markers
    /// the strict diff catches it.
    expected_markers: &'static [&'static str],
}

const DECLARED_FUNCTION_BODIES: &[DeclaredFunctionBody] = &[
    DeclaredFunctionBody {
        schema: "core",
        function_name: "expand_cascade",
        // The cascade-truncation refusal landed in #565 — the body must
        // continue to RAISE EXCEPTION when descendants exceed max_depth.
        // If a manual prod edit reverts this, replay starts silently
        // truncating again.
        expected_markers: &["RAISE EXCEPTION", "max depth"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "prepare_cascade_session",
        expected_markers: &["TEMP TABLE", "cascade_analysis_"],
    },
];

async fn check_trigger_function_bodies(
    pool: &PgPool,
) -> Result<Vec<StrictDrift>, ApplyError> {
    let mut drifts = Vec::new();
    for declared in DECLARED_FUNCTION_BODIES {
        let observed: Option<String> = sqlx::query_scalar(
            r"
            SELECT p.prosrc
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = $1
              AND p.proname = $2
            ",
        )
        .bind(declared.schema)
        .bind(declared.function_name)
        .fetch_optional(pool)
        .await?;

        let location = format!("{}.{}", declared.schema, declared.function_name);

        let Some(body) = observed else {
            drifts.push(StrictDrift {
                category: DriftCategory::TriggerBody,
                location,
                declared_summary: format!(
                    "function exists with markers {:?}",
                    declared.expected_markers
                ),
                observed_summary: "function not present in pg_proc".to_string(),
            });
            continue;
        };

        let missing: Vec<&str> = declared
            .expected_markers
            .iter()
            .copied()
            .filter(|marker| !body.contains(marker))
            .collect();

        if !missing.is_empty() {
            drifts.push(StrictDrift {
                category: DriftCategory::TriggerBody,
                location,
                declared_summary: format!(
                    "must contain markers {:?}",
                    declared.expected_markers
                ),
                observed_summary: format!("body missing markers {missing:?}"),
            });
        }
    }
    Ok(drifts)
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drift_category_display_round_trip() {
        // The Display impl is what `sinex-schema diff --strict` would surface
        // in operator-friendly output. Pin it so a refactor of the enum
        // names doesn't silently break consumer formatting.
        assert_eq!(format!("{}", DriftCategory::TriggerBody), "trigger_body");
        assert_eq!(format!("{}", DriftCategory::ColumnDefault), "column_default");
        assert_eq!(
            format!("{}", DriftCategory::ForeignKeyAction),
            "foreign_key_action"
        );
    }

    #[test]
    fn strict_drift_display_includes_location_and_summaries() {
        let drift = StrictDrift {
            category: DriftCategory::ColumnDefault,
            location: "core.events.ts_persisted".to_string(),
            declared_summary: "contains `now()`".to_string(),
            observed_summary: "no DEFAULT set".to_string(),
        };
        let rendered = format!("{drift}");
        assert!(rendered.contains("column_default"));
        assert!(rendered.contains("core.events.ts_persisted"));
        assert!(rendered.contains("now()"));
        assert!(rendered.contains("no DEFAULT set"));
    }
}
