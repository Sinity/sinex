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
//! 5. **`TimescaleDB` hypertable settings** (chunk interval, compression,
//!    retention).
//! 6. **Comments / table descriptions**.
//!
//! See issue #556. The module covers trigger function bodies, column defaults,
//! selected inline checks, selected foreign-key actions, selected TimescaleDB
//! hypertable settings, and orphan columns. Comment/table-description drift is
//! deliberately not active yet because comments are not a runtime contract.
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
use crate::converge::{convergible_tables, declared_columns_for};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::collections::HashSet;
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
    /// Selected inline column `CHECK` expressions.
    InlineCheckExpr,
    /// Selected foreign key `ON DELETE` / `ON UPDATE` actions.
    ForeignKeyAction,
    /// Selected `TimescaleDB` hypertable settings such as chunk interval and
    /// retention.
    HypertableSetting,
    /// Reserved for follow-up: comments / table descriptions.
    Comment,
    /// A column exists in the live database but is not declared in the source
    /// schema. This indicates a rename was performed without a corresponding
    /// `columns_to_drop` entry, or a manual `ALTER TABLE ADD COLUMN` was run
    /// without updating the source. Columns listed in the table's `pending_drop`
    /// allow-list are excluded from this check.
    OrphanColumn,
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
            Self::OrphanColumn => write!(f, "orphan_column"),
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

/// Strict-diff categories that are intentionally not emitted yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct UnsupportedStrictDiffCategory {
    pub category: DriftCategory,
    pub reason: &'static str,
}

/// Operator-visible support gate for categories that are acknowledged as
/// blind spots but are not part of the active strict-diff contract.
pub fn unsupported_strict_diff_categories() -> &'static [UnsupportedStrictDiffCategory] {
    &[UnsupportedStrictDiffCategory {
        category: DriftCategory::Comment,
        reason: "comments are not a runtime contract; strict diff does not report comment drift",
    }]
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
    drifts.extend(check_inline_check_exprs(pool).await?);
    drifts.extend(check_db_check_constraints(pool).await?);
    drifts.extend(check_foreign_key_actions(pool).await?);
    drifts.extend(check_hypertable_settings(pool).await?);
    drifts.extend(check_orphan_columns(pool).await?);
    Ok(drifts)
}

/// Verify each `#[derive(DbCheck)]`-registered enum has its expected
/// versioned CHECK constraint live in the database with the correct allowed
/// values. Drifts here usually mean an enum variant was renamed without
/// bumping `version`, or the column has not had `schema apply` re-run since
/// the rename. See issue #1236.
async fn check_db_check_constraints(pool: &PgPool) -> Result<Vec<StrictDrift>, ApplyError> {
    let mut drifts = Vec::new();
    for spec in sinex_primitives::schema_constraints::registered_specs() {
        let qualified = spec.qualified_table();
        // Column-existence probe parallels the apply engine's behavior:
        // a not-yet-materialized column is not drift, it is forward-state.
        let table_exists: bool = sqlx::query_scalar(
            r"SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = $1 AND table_name = $2
            )",
        )
        .bind(spec.schema)
        .bind(spec.table)
        .fetch_one(pool)
        .await?;
        if !table_exists {
            continue;
        }
        let col_exists: bool = sqlx::query_scalar(
            r"SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
            )",
        )
        .bind(spec.schema)
        .bind(spec.table)
        .bind(spec.column)
        .fetch_one(pool)
        .await?;
        if !col_exists {
            continue;
        }

        let definition: Option<String> = sqlx::query_scalar(
            r"
            SELECT pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            JOIN pg_class r ON c.conrelid = r.oid
            JOIN pg_namespace n ON r.relnamespace = n.oid
            WHERE n.nspname = $1 AND r.relname = $2 AND c.conname = $3
            ",
        )
        .bind(spec.schema)
        .bind(spec.table)
        .bind(spec.constraint_name())
        .fetch_optional(pool)
        .await?;

        let observed = match definition {
            None => "missing".to_string(),
            Some(def) => {
                let mut ok = true;
                for value in spec.allowed_values {
                    let needle = format!("'{}'", value.replace('\'', "''"));
                    if !def.contains(&needle) {
                        ok = false;
                        break;
                    }
                }
                if ok {
                    continue;
                }
                def
            }
        };

        drifts.push(StrictDrift {
            category: DriftCategory::InlineCheckExpr,
            location: format!("{qualified}.{}::{}", spec.column, spec.enum_name),
            declared_summary: format!(
                "CHECK {} (from #[derive(DbCheck)] on {})",
                spec.check_clause(),
                spec.enum_name
            ),
            observed_summary: observed,
        });
    }
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
    /// `pg_catalog.now()` depending on `search_path` at the time of DDL).
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

        let location = format!("{}.{}.{}", declared.schema, declared.table, declared.column);

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
//
// # Classification of all schema-apply-installed functions (#1133)
//
// ## Strict body check (DECLARED_FUNCTION_BODIES — below)
//
// These functions have load-bearing logic where a body change could cause
// silent data corruption, broken provenance, or audit gaps. The strict diff
// checks that expected markers appear in the live function body.
//
//   Trigger functions:
//   - core.fn_events_no_update          — immutability guard
//   - core.fn_events_validate_payload   — payload validation gate
//   - core.fn_events_validate_material_bounds — anchor/offset boundary check
//   - core.fn_archive_before_delete     — cascade-archive on DELETE
//   - raw.fn_source_material_validate_event_bounds — shrink-prevention
//   - raw.fn_temporal_ledger_append_only — append-only guard
//
//   Operation management:
//   - core.start_operation              — audit trail entry
//   - core.complete_operation           — audit trail closure
//   - core.fail_operation               — audit trail failure record
//
//   Cascade support:
//   - core.expand_cascade               — BFS expansion with depth limit
//   - core.prepare_cascade_session      — temp table setup
//   - core.cascade_populate_roots       — root discovery
//   - core.cascade_find_integrity_violations — provenance-chain validation
//
//   Lifecycle:
//   - core.execute_cascade_tombstone    — permanent deletion
//   - core.execute_cascade_restore      — restore from archive
//
// ## Required presence only (apply::diff checks trigger existence)
//
// These triggers are installed by CREATE OR REPLACE on every apply.
// apply::diff detects missing triggers; body drift is not checked because
// the functions are straightforward updated_at helpers or projection logic.
//
//   - trg_entities_updated_at           — updated_at convenience
//   - trg_entity_relations_updated_at   — updated_at convenience
//   - trg_event_annotations_updated_at  — updated_at convenience
//   - trg_event_payload_schemas_updated_at — updated_at convenience
//   - set_timestamp (dlq_events)        — updated_at convenience
//   - trg_embedding_model_create_index  — HNSW index management
//   - public.set_current_timestamp_updated_at() — shared updated_at helper
//   - core.embedding_model_index_trigger() — index creation trigger fn
//
// ## Explicitly non-goal (query-only or idempotent recreation)
//
// These functions are query-only (no mutation), diagnostic, or idempotent
// (re-running produces the same state). Body drift here can't corrupt data.
//
//   - core.cascade_count_nodes           — diagnostic
//   - core.cascade_depth_histogram       — diagnostic
//   - core.cascade_find_integrity_violations_paginated — diagnostic variant
//   - core.cleanup_cascade_session       — temp table cleanup
//   - core.lifecycle_tier_status()       — query-only (STABLE, SQL)
//   - core.jsonb_merge_deep()            — pure utility (IMMUTABLE)
//   - core.create_embedding_model_index  — idempotent DDL
//   - core.drop_embedding_model_index    — idempotent DDL
//   - core.hybrid_search()               — query-only (read path)
//   - core.fn_document_projection()      — projection (non-critical)

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
    // ── Trigger functions (critical — body drift = silent data corruption) ──────
    DeclaredFunctionBody {
        schema: "core",
        function_name: "fn_events_no_update",
        // Prevents UPDATE on core.events — immutability is a core invariant.
        expected_markers: &["UPDATE on core.events is forbidden"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "fn_events_validate_payload",
        // Payload validation gate — removal means invalid payloads persist silently.
        expected_markers: &["jsonb_matches_schema", "sinex.payload_validation"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "fn_events_validate_material_bounds",
        // Prevents anchor_byte/offset overflow beyond material total_bytes.
        expected_markers: &["anchor_byte > material_total_bytes", "offset_kind"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "fn_archive_before_delete",
        // THE archive trigger — cascade-archives annotations, embeddings, cluster
        // members, and validation cache before allowing DELETE on core.events.
        expected_markers: &[
            "sinex.operation_id",
            "archived_events",
            "archived_annotations",
            "archived_embeddings",
        ],
    },
    DeclaredFunctionBody {
        schema: "raw",
        function_name: "fn_source_material_validate_event_bounds",
        // Prevents shrinking source material total_bytes below existing event anchors.
        expected_markers: &["total_bytes would invalidate existing event anchors"],
    },
    DeclaredFunctionBody {
        schema: "raw",
        function_name: "fn_temporal_ledger_append_only",
        // Append-only invariant on temporal_ledger — prevents mutation of observation log.
        expected_markers: &["append-only", "is forbidden"],
    },
    // ── Operation management (critical — audit trail integrity) ─────────────────
    DeclaredFunctionBody {
        schema: "core",
        function_name: "start_operation",
        expected_markers: &["operations_log", "uuidv7()", "operation_type"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "complete_operation",
        expected_markers: &["result_status = 'success'", "operations_log"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "fail_operation",
        expected_markers: &["result_status = 'failure'", "operations_log"],
    },
    // ── Cascade support ─────────────────────────────────────────────────────────
    DeclaredFunctionBody {
        schema: "core",
        function_name: "expand_cascade",
        // The cascade-truncation refusal landed in #565 — the body must
        // continue to RAISE EXCEPTION when descendants exceed max_depth.
        expected_markers: &["RAISE EXCEPTION", "max depth"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "prepare_cascade_session",
        expected_markers: &["TEMP TABLE", "cascade_analysis_"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "cascade_populate_roots",
        expected_markers: &["source_event_ids", "cascade"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "cascade_find_integrity_violations",
        // Detects broken provenance chains — critical for replay safety.
        expected_markers: &["violations", "source_event_ids"],
    },
    // ── Lifecycle (critical — archive/restore/tombstone) ────────────────────────
    DeclaredFunctionBody {
        schema: "core",
        function_name: "execute_cascade_tombstone",
        // Permanent deletion from archive — irrecoverable if the body is wrong.
        expected_markers: &["event_tombstones", "archived_events", "ON CONFLICT"],
    },
    DeclaredFunctionBody {
        schema: "core",
        function_name: "execute_cascade_restore",
        // Restore from archive with side-table recovery (#1134).
        // Must track restored IDs, restore side-tables, and only delete
        // archive rows that were actually restored.
        expected_markers: &[
            "_restored_ids",
            "ON CONFLICT (id) DO NOTHING",
            "archived_annotations",
            "archived_embeddings",
            "archived_tagged_items",
        ],
    },
];

async fn check_trigger_function_bodies(pool: &PgPool) -> Result<Vec<StrictDrift>, ApplyError> {
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
                declared_summary: format!("must contain markers {:?}", declared.expected_markers),
                observed_summary: format!("body missing markers {missing:?}"),
            });
        }
    }
    Ok(drifts)
}

// ─── Inline CHECK expressions ───────────────────────────────────────────────

/// One declared expectation for an anonymous inline CHECK constraint on a
/// table. The check is identified by markers it must contain in the body
/// returned by `pg_get_constraintdef`. We accept matches on ANY constraint
/// of the table — there is no stable name to key on (sea-query emits the
/// CHECK without a `CONSTRAINT name` clause and Postgres synthesizes one
/// like `events_check2`, which renumbers across applies).
struct DeclaredInlineCheck {
    schema: &'static str,
    table: &'static str,
    /// Short label for the location string. Two checks on the same table need
    /// distinct labels to disambiguate the drift report.
    label: &'static str,
    /// Substrings that some CHECK constraint on the table MUST collectively
    /// contain (all markers on a single constraint definition). Markers that
    /// appear across two different constraints do not satisfy the
    /// expectation — the matcher seeks one constraint that contains every
    /// marker.
    expected_markers: &'static [&'static str],
}

const DECLARED_INLINE_CHECKS: &[DeclaredInlineCheck] = &[
    DeclaredInlineCheck {
        schema: "core",
        table: "events",
        label: "xor_provenance",
        // The XOR provenance invariant — exactly one of source_material_id
        // or source_event_ids set. This is THE load-bearing in-DB check; if
        // it disappears, the provenance contract is gone.
        // Markers span both OR-branches of the constraint so a partial
        // rewrite that removes the derived side is also detected.
        expected_markers: &[
            "source_material_id IS NOT NULL",
            "source_event_ids IS NULL",
            "source_material_id IS NULL",
            "source_event_ids IS NOT NULL",
        ],
    },
    DeclaredInlineCheck {
        schema: "core",
        table: "events",
        label: "anchor_byte_non_negative",
        expected_markers: &["anchor_byte", ">= 0"],
    },
    DeclaredInlineCheck {
        schema: "core",
        table: "events",
        label: "synthesis_non_empty",
        expected_markers: &["source_event_ids", "cardinality"],
    },
    DeclaredInlineCheck {
        schema: "core",
        table: "events",
        label: "offset_kind_enum",
        // All four wire-format values declared in schema/events.rs:
        // 'byte', 'line', 'rowid', 'logical' (maps to OffsetKind variants in builder.rs).
        expected_markers: &["offset_kind", "'byte'", "'line'", "'rowid'", "'logical'"],
    },
];

async fn check_inline_check_exprs(pool: &PgPool) -> Result<Vec<StrictDrift>, ApplyError> {
    let mut drifts = Vec::new();
    for declared in DECLARED_INLINE_CHECKS {
        let definitions: Vec<String> = sqlx::query_scalar(
            r"
            SELECT pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1
              AND t.relname = $2
              AND c.contype = 'c'
            ",
        )
        .bind(declared.schema)
        .bind(declared.table)
        .fetch_all(pool)
        .await?;

        if let Some(drift) = inline_check_drift(declared, &definitions) {
            drifts.push(drift);
        }
    }
    Ok(drifts)
}

fn inline_check_drift(
    declared: &DeclaredInlineCheck,
    definitions: &[String],
) -> Option<StrictDrift> {
    let location = format!("{}.{}::{}", declared.schema, declared.table, declared.label);

    let any_match = definitions.iter().any(|def| {
        declared
            .expected_markers
            .iter()
            .all(|marker| def.contains(marker))
    });

    (!any_match).then(|| StrictDrift {
        category: DriftCategory::InlineCheckExpr,
        location,
        declared_summary: format!(
            "some CHECK on {}.{} contains all of {:?}",
            declared.schema, declared.table, declared.expected_markers
        ),
        observed_summary: if definitions.is_empty() {
            "table has no CHECK constraints".to_string()
        } else {
            format!("{} CHECK constraint(s); none match", definitions.len())
        },
    })
}

// ─── Foreign key actions ────────────────────────────────────────────────────

/// One declared FK ON DELETE / ON UPDATE expectation. We pin the action on
/// the `pg_get_constraintdef` text since it surfaces the action explicitly
/// (`FOREIGN KEY (col) REFERENCES other(id) ON DELETE CASCADE`). Substring
/// match on the action keyword keeps the check robust to formatting.
struct DeclaredForeignKeyAction {
    schema: &'static str,
    table: &'static str,
    /// Substring that uniquely identifies the FK definition we want to pin.
    /// The first FK whose `pg_get_constraintdef` contains this substring is
    /// matched. Keep this specific to one FK — pinning by referenced column
    /// (`FOREIGN KEY (parent_tag_id)`) is the natural choice.
    fk_marker: &'static str,
    /// Required text for the ON DELETE action (e.g. `ON DELETE CASCADE`).
    /// `None` means the check does not assert a specific ON DELETE action.
    expected_delete_action_marker: Option<&'static str>,
    /// Required text for the ON UPDATE action (e.g. `ON UPDATE CASCADE`).
    /// `None` means the check does not assert a specific ON UPDATE action.
    expected_update_action_marker: Option<&'static str>,
}

const DECLARED_FK_ACTIONS: &[DeclaredForeignKeyAction] = &[
    // TaggedItems(tag_id) → Tags(id) — schema/annotations.rs declares
    // ON DELETE CASCADE. Reverting to NO ACTION or RESTRICT would block
    // tag deletion when items still reference it; reverting to SET NULL
    // would orphan associations into a `(NULL, item_id, item_type)` row,
    // which violates the assumption that `tagged_items` rows always
    // resolve to a real tag.
    DeclaredForeignKeyAction {
        schema: "core",
        table: "tagged_items",
        fk_marker: "FOREIGN KEY (tag_id)",
        expected_delete_action_marker: Some("ON DELETE CASCADE"),
        expected_update_action_marker: None,
    },
    // Tags(parent_tag_id) → Tags(id) — schema/annotations.rs installs
    // this self-FK through a raw fixup because sea-query emitted CASCADE
    // for the self-referential SET NULL declaration. Pin the action here
    // so regressions in that fixup are visible before apply overwrites a
    // manually drifted database.
    DeclaredForeignKeyAction {
        schema: "core",
        table: "tags",
        fk_marker: "FOREIGN KEY (parent_tag_id)",
        expected_delete_action_marker: Some("ON DELETE SET NULL"),
        expected_update_action_marker: None,
    },
    //
    // Note: `core.event_annotations(event_id)` → `core.events` was
    // previously listed here as #579. That FK declaration has been
    // removed from the source (TimescaleDB does not allow hypertables
    // as FK targets — timescale/timescaledb#865). Cascade-on-delete is
    // now enforced by `core.fn_archive_before_delete` instead.
];

async fn check_foreign_key_actions(pool: &PgPool) -> Result<Vec<StrictDrift>, ApplyError> {
    let mut drifts = Vec::new();
    for declared in DECLARED_FK_ACTIONS {
        let definitions: Vec<String> = sqlx::query_scalar(
            r"
            SELECT pg_get_constraintdef(c.oid)
            FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE n.nspname = $1
              AND t.relname = $2
              AND c.contype = 'f'
            ",
        )
        .bind(declared.schema)
        .bind(declared.table)
        .fetch_all(pool)
        .await?;

        drifts.extend(foreign_key_action_drifts(declared, &definitions));
    }
    Ok(drifts)
}

fn foreign_key_action_drifts(
    declared: &DeclaredForeignKeyAction,
    definitions: &[String],
) -> Vec<StrictDrift> {
    let mut drifts = Vec::new();
    let location = format!(
        "{}.{} {}",
        declared.schema, declared.table, declared.fk_marker
    );

    let Some(matching) = definitions
        .iter()
        .find(|def| def.contains(declared.fk_marker))
    else {
        drifts.push(StrictDrift {
            category: DriftCategory::ForeignKeyAction,
            location,
            declared_summary: format!(
                "FK with `{}`{}{}",
                declared.fk_marker,
                declared
                    .expected_delete_action_marker
                    .map(|a| format!(", delete action `{a}`"))
                    .unwrap_or_default(),
                declared
                    .expected_update_action_marker
                    .map(|a| format!(", update action `{a}`"))
                    .unwrap_or_default(),
            ),
            observed_summary: format!(
                "no FK on {}.{} matches `{}`",
                declared.schema, declared.table, declared.fk_marker
            ),
        });
        return drifts;
    };

    if let Some(delete_marker) = declared.expected_delete_action_marker
        && !matching.contains(delete_marker)
    {
        drifts.push(StrictDrift {
            category: DriftCategory::ForeignKeyAction,
            location: format!("{location} (ON DELETE)"),
            declared_summary: format!("contains `{delete_marker}`"),
            observed_summary: matching.clone(),
        });
    }

    if let Some(update_marker) = declared.expected_update_action_marker
        && !matching.contains(update_marker)
    {
        drifts.push(StrictDrift {
            category: DriftCategory::ForeignKeyAction,
            location: format!("{location} (ON UPDATE)"),
            declared_summary: format!("contains `{update_marker}`"),
            observed_summary: matching.clone(),
        });
    }

    drifts
}

// ─── TimescaleDB hypertable settings ────────────────────────────────────────

/// `core.events` hypertable invariants. The chunk interval is set in
/// `apply::configure_timescaledb` to 7 days; the retention policy is
/// explicitly removed there. Both decisions matter for replay/archive
/// behavior — a different chunk interval changes archival pressure, a
/// retention policy silently drops events. The strict diff catches manual
/// drift either way.
const HYPERTABLE_CHUNK_INTERVAL_MICROS: i64 = 7 * 24 * 60 * 60 * 1_000_000;

async fn check_hypertable_settings(pool: &PgPool) -> Result<Vec<StrictDrift>, ApplyError> {
    let mut drifts = Vec::new();

    // Skip if TimescaleDB extension isn't installed. Test databases without
    // the extension still pass this category (no drift to report).
    let timescaledb_present: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb')",
    )
    .fetch_one(pool)
    .await?;
    if !timescaledb_present {
        return Ok(drifts);
    }

    // Hypertable presence + chunk interval. `_timescaledb_catalog.dimension`
    // stores `interval_length` in microseconds for time-partitioned tables;
    // `by_range('id')` over UUIDv7 uses the same time-derived range.
    let row: Option<(Option<i64>,)> = sqlx::query_as(
        r"
        SELECT d.interval_length
        FROM _timescaledb_catalog.hypertable h
        JOIN _timescaledb_catalog.dimension d ON d.hypertable_id = h.id
        WHERE h.schema_name = 'core' AND h.table_name = 'events'
        ",
    )
    .fetch_optional(pool)
    .await
    .map_err(ApplyError::from)?;

    if let Some(drift) = hypertable_chunk_interval_drift(row) {
        drifts.push(drift);
    }

    // Retention policy must NOT exist on core.events. The bgw_job table is
    // the public entry point for jobs (compression, retention, reorder).
    let retention_count: i64 = sqlx::query_scalar(
        r"
        SELECT count(*)::bigint
        FROM timescaledb_information.jobs
        WHERE proc_name = 'policy_retention'
          AND hypertable_schema = 'core'
          AND hypertable_name = 'events'
        ",
    )
    .fetch_one(pool)
    .await
    .map_err(ApplyError::from)?;

    if let Some(drift) = hypertable_retention_policy_drift(retention_count) {
        drifts.push(drift);
    }

    Ok(drifts)
}

fn hypertable_chunk_interval_drift(row: Option<(Option<i64>,)>) -> Option<StrictDrift> {
    match row {
        None => Some(StrictDrift {
            category: DriftCategory::HypertableSetting,
            location: "core.events".to_string(),
            declared_summary: "hypertable with 7d chunk interval".to_string(),
            observed_summary: "core.events is not a hypertable".to_string(),
        }),
        Some((Some(observed),)) if observed != HYPERTABLE_CHUNK_INTERVAL_MICROS => {
            Some(StrictDrift {
                category: DriftCategory::HypertableSetting,
                location: "core.events::chunk_interval".to_string(),
                declared_summary: format!(
                    "interval_length = {HYPERTABLE_CHUNK_INTERVAL_MICROS} (7 days in µs)"
                ),
                observed_summary: format!("interval_length = {observed}"),
            })
        }
        Some((None,)) => Some(StrictDrift {
            category: DriftCategory::HypertableSetting,
            location: "core.events::chunk_interval".to_string(),
            declared_summary: format!(
                "interval_length = {HYPERTABLE_CHUNK_INTERVAL_MICROS} (7 days in µs)"
            ),
            observed_summary: "interval_length is NULL".to_string(),
        }),
        Some(_) => None,
    }
}

fn hypertable_retention_policy_drift(retention_count: i64) -> Option<StrictDrift> {
    (retention_count > 0).then(|| StrictDrift {
        category: DriftCategory::HypertableSetting,
        location: "core.events::retention_policy".to_string(),
        declared_summary: "no retention policy".to_string(),
        observed_summary: format!("{retention_count} retention policy job(s) present"),
    })
}

// ─── Orphan column detection ─────────────────────────────────────────────────

/// Detects columns that exist in the live database but are absent from the
/// source declaration.
///
/// A column is "orphan" if:
/// 1. It appears in `information_schema.columns` for a convergible table, AND
/// 2. It is NOT in the source declaration (`statement_fn` column names), AND
/// 3. It is NOT in `columns_to_drop` (those are known pending removals), AND
/// 4. It is NOT in `pending_drop` (explicitly allow-listed transitional columns).
///
/// This catches rename-without-drop, manual `ALTER TABLE ADD COLUMN` without
/// source update, and other unintentional schema drift.
async fn check_orphan_columns(pool: &PgPool) -> Result<Vec<StrictDrift>, ApplyError> {
    let tables = convergible_tables()?;
    let mut drifts = Vec::new();

    for ct in &tables {
        let qname = ct.meta.qualified_name;

        if !crate::apply::relation_exists(pool, qname).await? {
            continue;
        }

        // Collect all column names from live DB.
        let live_cols: Vec<String> = sqlx::query_scalar(
            "SELECT column_name
             FROM information_schema.columns
             WHERE table_schema = $1 AND table_name = $2",
        )
        .bind(ct.meta.schema)
        .bind(ct.meta.name)
        .fetch_all(pool)
        .await?;

        // Build the declared-column set plus all allow-listed columns.
        let (declared_names, pending_drop) = declared_columns_for(ct);
        let mut allowed: HashSet<&str> = declared_names.iter().map(String::as_str).collect();
        // columns_to_drop are known-pending removals — not orphans.
        for col in ct.columns_to_drop {
            allowed.insert(col);
        }
        // pending_drop is the explicit allow-list for in-flight renames.
        for col in pending_drop {
            allowed.insert(col);
        }

        for live_col in &live_cols {
            if !allowed.contains(live_col.as_str()) {
                drifts.push(StrictDrift {
                    category: DriftCategory::OrphanColumn,
                    location: format!("{qname}.{live_col}"),
                    declared_summary: "column not in source declaration".to_string(),
                    observed_summary: format!(
                        "column `{live_col}` exists in live {qname} but is not declared in source \
                         (not in columns_to_drop or pending_drop allow-list)"
                    ),
                });
            }
        }
    }

    Ok(drifts)
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::{
        DECLARED_FK_ACTIONS, DECLARED_INLINE_CHECKS, DriftCategory,
        HYPERTABLE_CHUNK_INTERVAL_MICROS, StrictDrift, foreign_key_action_drifts,
        hypertable_chunk_interval_drift, hypertable_retention_policy_drift, inline_check_drift,
        unsupported_strict_diff_categories,
    };
    use xtask::sandbox::prelude::sinex_test;

    #[sinex_test]
    async fn drift_category_display_round_trip() -> xtask::sandbox::TestResult<()> {
        // The Display impl is what `sinex-schema diff --strict` would surface
        // in operator-friendly output. Pin it so a refactor of the enum
        // names doesn't silently break consumer formatting.
        assert_eq!(format!("{}", DriftCategory::TriggerBody), "trigger_body");
        assert_eq!(
            format!("{}", DriftCategory::ColumnDefault),
            "column_default"
        );
        assert_eq!(
            format!("{}", DriftCategory::ForeignKeyAction),
            "foreign_key_action"
        );
        assert_eq!(
            format!("{}", DriftCategory::InlineCheckExpr),
            "inline_check_expr"
        );
        assert_eq!(
            format!("{}", DriftCategory::HypertableSetting),
            "hypertable_setting"
        );
        assert_eq!(format!("{}", DriftCategory::Comment), "comment");
        assert_eq!(format!("{}", DriftCategory::OrphanColumn), "orphan_column");
        Ok(())
    }

    #[sinex_test]
    async fn strict_drift_display_includes_location_and_summaries() -> xtask::sandbox::TestResult<()>
    {
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
        Ok(())
    }

    #[test]
    fn inline_check_drift_reports_when_required_markers_are_split() {
        let declared = DECLARED_INLINE_CHECKS
            .iter()
            .find(|check| check.label == "xor_provenance")
            .expect("xor provenance strict-diff expectation is declared");
        let partial_definitions = vec![
            "CHECK ((source_material_id IS NOT NULL) AND (source_event_ids IS NULL))".to_string(),
            "CHECK ((source_material_id IS NULL))".to_string(),
        ];

        let drift = inline_check_drift(declared, &partial_definitions)
            .expect("split markers across constraints must not satisfy one inline check");

        assert_eq!(drift.category, DriftCategory::InlineCheckExpr);
        assert_eq!(drift.location, "core.events::xor_provenance");
        assert!(drift.declared_summary.contains("source_material_id"));
        assert_eq!(drift.observed_summary, "2 CHECK constraint(s); none match");

        let matching_definition = vec![declared.expected_markers.join(" AND ")];
        assert!(
            inline_check_drift(declared, &matching_definition).is_none(),
            "one CHECK containing every declared marker is not drift"
        );
    }

    #[test]
    fn inline_check_drift_reports_missing_constraints() {
        let declared = DECLARED_INLINE_CHECKS
            .iter()
            .find(|check| check.label == "anchor_byte_non_negative")
            .expect("anchor-byte strict-diff expectation is declared");

        let drift = inline_check_drift(declared, &[])
            .expect("absence of inline CHECK definitions must be reported");

        assert_eq!(drift.category, DriftCategory::InlineCheckExpr);
        assert_eq!(drift.location, "core.events::anchor_byte_non_negative");
        assert_eq!(drift.observed_summary, "table has no CHECK constraints");
    }

    #[test]
    fn foreign_key_action_drift_reports_missing_delete_action() {
        let declared = DECLARED_FK_ACTIONS
            .iter()
            .find(|fk| fk.table == "tagged_items")
            .expect("tagged_items FK action strict-diff expectation is declared");
        let definitions =
            vec!["FOREIGN KEY (tag_id) REFERENCES core.tags(id) ON DELETE NO ACTION".to_string()];

        let drifts = foreign_key_action_drifts(declared, &definitions);

        assert_eq!(drifts.len(), 1);
        let drift = &drifts[0];
        assert_eq!(drift.category, DriftCategory::ForeignKeyAction);
        assert_eq!(
            drift.location,
            "core.tagged_items FOREIGN KEY (tag_id) (ON DELETE)"
        );
        assert_eq!(drift.declared_summary, "contains `ON DELETE CASCADE`");
        assert!(drift.observed_summary.contains("ON DELETE NO ACTION"));
    }

    #[test]
    fn foreign_key_action_drift_reports_missing_fk_definition() {
        let declared = DECLARED_FK_ACTIONS
            .iter()
            .find(|fk| fk.table == "tags")
            .expect("tags self-FK action strict-diff expectation is declared");
        let definitions = vec!["FOREIGN KEY (other_id) REFERENCES core.tags(id)".to_string()];

        let drifts = foreign_key_action_drifts(declared, &definitions);

        assert_eq!(drifts.len(), 1);
        let drift = &drifts[0];
        assert_eq!(drift.category, DriftCategory::ForeignKeyAction);
        assert_eq!(drift.location, "core.tags FOREIGN KEY (parent_tag_id)");
        assert!(drift.declared_summary.contains("ON DELETE SET NULL"));
        assert!(
            drift
                .observed_summary
                .contains("no FK on core.tags matches")
        );
    }

    #[test]
    fn hypertable_setting_drift_reports_chunk_interval_states() {
        assert!(
            hypertable_chunk_interval_drift(Some((Some(HYPERTABLE_CHUNK_INTERVAL_MICROS),)))
                .is_none(),
            "declared 7-day chunk interval is not drift"
        );

        let drift = hypertable_chunk_interval_drift(Some((Some(60_000_000),)))
            .expect("wrong chunk interval must be reported");
        assert_eq!(drift.category, DriftCategory::HypertableSetting);
        assert_eq!(drift.location, "core.events::chunk_interval");
        assert!(drift.declared_summary.contains("7 days"));
        assert_eq!(drift.observed_summary, "interval_length = 60000000");

        let missing =
            hypertable_chunk_interval_drift(None).expect("missing hypertable must be reported");
        assert_eq!(missing.location, "core.events");
        assert_eq!(missing.observed_summary, "core.events is not a hypertable");
    }

    #[test]
    fn hypertable_setting_drift_reports_retention_policy() {
        assert!(
            hypertable_retention_policy_drift(0).is_none(),
            "declared state has no retention-policy drift"
        );

        let drift =
            hypertable_retention_policy_drift(2).expect("retention policy jobs must be reported");
        assert_eq!(drift.category, DriftCategory::HypertableSetting);
        assert_eq!(drift.location, "core.events::retention_policy");
        assert_eq!(drift.declared_summary, "no retention policy");
        assert_eq!(drift.observed_summary, "2 retention policy job(s) present");
    }

    #[test]
    fn comment_drift_is_an_explicitly_unsupported_category() {
        let unsupported = unsupported_strict_diff_categories();

        assert_eq!(unsupported.len(), 1);
        assert_eq!(unsupported[0].category, DriftCategory::Comment);
        assert!(unsupported[0].reason.contains("not a runtime contract"));
        assert!(
            unsupported[0]
                .reason
                .contains("does not report comment drift")
        );
    }
}
