//! Absorb a foreign xtask history database into the canonical ledger.
//!
//! Every workspace records into one shared database (see
//! [`crate::config::canonical_history_db_path`]), but databases written before
//! that rule — or by a worktree still running an older xtask — exist as
//! separate files. This module folds such a file into the canonical ledger
//! without losing the provenance of the workspace that produced it.
//!
//! Two properties make the merge safe to re-run:
//!
//! - **Row ids are re-minted.** Source ids are meaningless outside their own
//!   file (each fresh database restarts `AUTOINCREMENT` at 1), so every
//!   invocation is inserted fresh and every child row is rewritten to point at
//!   the new parent id.
//! - **Invocations dedupe on what identifies a run in the world** — the host,
//!   working directory, process id, start instant and command. Re-importing a
//!   file, or importing a preserved copy of a database already absorbed,
//!   inserts nothing. Child rows follow their parent: a duplicate parent
//!   carries no children, so they cannot double up either.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};

use color_eyre::eyre::{Result, WrapErr, bail};
use rusqlite::types::Value;

use super::HistoryDb;

/// Tables holding rows owned by an invocation, keyed by the column carrying the
/// invocation id. Order matters only for readability; each is remapped from the
/// invocation id map independently.
const INVOCATION_CHILD_TABLES: &[&str] = &[
    "invocation_packages",
    "stage_timings",
    "test_results",
    "build_diagnostics",
    "invocation_progress",
    "invocation_eta_samples",
    "proof_evidence",
    "test_proof_units",
    "test_dependency_edges",
    "coverage_regions",
    "test_execution_manifests",
    "trace_events",
    "impact_audit_runs",
];

/// Which workspace an imported run should be attributed to when the source
/// database predates the workspace columns.
#[derive(Debug, Clone)]
pub struct WorkspaceAttribution {
    pub root: String,
    pub name: String,
}

/// What one `import_history` call moved.
#[derive(Debug, Clone)]
pub struct ImportReport {
    pub source: PathBuf,
    pub attribution: WorkspaceAttribution,
    pub invocations_seen: usize,
    pub invocations_inserted: usize,
    pub invocations_duplicate: usize,
    /// Child rows inserted, per table. Absent tables are simply not listed.
    pub rows_by_table: BTreeMap<String, usize>,
}

impl ImportReport {
    /// Total rows written, across invocations and every child table.
    #[must_use]
    pub fn total_rows(&self) -> usize {
        self.invocations_inserted + self.rows_by_table.values().sum::<usize>()
    }
}

/// Identity of a run as it happened, independent of any database's row ids.
type InvocationKey = (String, String, i64, String, String);

fn invocation_key(host: &str, cwd: &str, pid: Option<i64>, started_at: &str, command: &str) -> InvocationKey {
    (
        host.to_string(),
        cwd.to_string(),
        pid.unwrap_or(-1),
        started_at.to_string(),
        command.to_string(),
    )
}

/// Fold `source_path` into the ledger behind `dest`.
///
/// `attribution` supplies workspace provenance for rows that carry none of
/// their own; rows that already name a workspace keep it.
pub fn import_history(
    dest: &HistoryDb,
    source_path: &Path,
    attribution: &WorkspaceAttribution,
) -> Result<ImportReport> {
    if !source_path.is_file() {
        bail!("history source is not a file: {}", source_path.display());
    }
    if dest.conn.is_readonly(rusqlite::DatabaseName::Main)? {
        bail!("cannot import into a read-only history database");
    }

    // Work from a private copy so the source file — which may still be open by
    // a live writer, and which the caller may want to keep byte-for-byte — is
    // never attached read-write or checkpointed by this process.
    let staging = tempfile::Builder::new()
        .prefix("xtask-history-import-")
        .suffix(".db")
        .tempfile()
        .context("failed to create staging file for history import")?;
    copy_sqlite_snapshot(source_path, staging.path())?;

    dest.conn
        .execute("ATTACH DATABASE ?1 AS src", [staging.path().to_string_lossy()])
        .with_context(|| format!("failed to attach history source {}", source_path.display()))?;
    let result = import_attached(dest, source_path, attribution);
    // Detach regardless of outcome so the connection stays usable.
    let detached = dest
        .conn
        .execute("DETACH DATABASE src", [])
        .context("failed to detach history source");
    match (result, detached) {
        (Ok(report), Ok(_)) => Ok(report),
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
    }
}

/// Copy a live SQLite database through the online backup API.
///
/// A byte copy of a WAL-mode database with an active writer can capture a torn
/// page or miss committed data still in the WAL; the backup API cannot.
fn copy_sqlite_snapshot(source_path: &Path, destination: &Path) -> Result<()> {
    let source = rusqlite::Connection::open_with_flags(
        source_path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .with_context(|| format!("failed to open history source {}", source_path.display()))?;
    let mut staging = rusqlite::Connection::open(destination)
        .context("failed to open staging database for history import")?;
    let backup = rusqlite::backup::Backup::new(&source, &mut staging)
        .context("failed to start history source snapshot")?;
    backup
        .run_to_completion(1024, std::time::Duration::from_millis(25), None)
        .context("failed to snapshot history source")?;
    Ok(())
}

fn import_attached(
    dest: &HistoryDb,
    source_path: &Path,
    attribution: &WorkspaceAttribution,
) -> Result<ImportReport> {
    let existing = existing_invocation_keys(dest)?;
    let mut report = ImportReport {
        source: source_path.to_path_buf(),
        attribution: attribution.clone(),
        invocations_seen: 0,
        invocations_inserted: 0,
        invocations_duplicate: 0,
        rows_by_table: BTreeMap::new(),
    };

    dest.conn
        .execute("BEGIN IMMEDIATE", [])
        .context("failed to open history import transaction")?;
    let outcome = import_rows(dest, attribution, &existing, &mut report);
    match outcome {
        Ok(()) => {
            dest.conn
                .execute("COMMIT", [])
                .context("failed to commit history import")?;
            Ok(report)
        }
        Err(error) => {
            let _ = dest.conn.execute("ROLLBACK", []);
            Err(error)
        }
    }
}

fn import_rows(
    dest: &HistoryDb,
    attribution: &WorkspaceAttribution,
    existing: &HashSet<InvocationKey>,
    report: &mut ImportReport,
) -> Result<()> {
    let invocation_ids = import_invocations(dest, attribution, existing, report)?;

    let record = |report: &mut ImportReport, table: &str, inserted: usize| {
        if inserted > 0 {
            report.rows_by_table.insert(table.to_string(), inserted);
        }
    };

    for table in INVOCATION_CHILD_TABLES {
        let (inserted, _) = copy_table(dest, table, &[("invocation_id", &invocation_ids)], attribution)?;
        record(report, table, inserted);
    }

    // Two levels of ownership: remap the parent first so the grandchild can
    // point at the new parent id.
    let (inserted, job_ids) = copy_table(
        dest,
        "background_jobs",
        &[("invocation_id", &invocation_ids)],
        attribution,
    )?;
    record(report, "background_jobs", inserted);
    let (inserted, impact_run_ids) = copy_table(
        dest,
        "impact_runs",
        &[("invocation_id", &invocation_ids)],
        attribution,
    )?;
    record(report, "impact_runs", inserted);
    let (inserted, exercise_run_ids) = copy_table(
        dest,
        "exercise_runs",
        &[("invocation_id", &invocation_ids)],
        attribution,
    )?;
    record(report, "exercise_runs", inserted);

    for (table, parent_column, map) in [
        ("background_job_logs", "job_id", &job_ids),
        ("impact_decisions", "impact_run_id", &impact_run_ids),
        ("exercise_results", "run_id", &exercise_run_ids),
    ] {
        let (inserted, _) = copy_table(dest, table, &[(parent_column, map)], attribution)?;
        record(report, table, inserted);
    }

    Ok(())
}

/// Read the identity of every invocation already in the destination.
fn existing_invocation_keys(dest: &HistoryDb) -> Result<HashSet<InvocationKey>> {
    let mut stmt = dest
        .conn
        .prepare("SELECT host, cwd, pid, started_at, command FROM main.invocations")
        .context("failed to prepare destination invocation identity scan")?;
    let rows = stmt
        .query_map([], |row| {
            Ok(invocation_key(
                &row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                &row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                row.get::<_, Option<i64>>(2)?,
                &row.get::<_, String>(3)?,
                &row.get::<_, String>(4)?,
            ))
        })
        .context("failed to scan destination invocation identities")?;
    let mut keys = HashSet::new();
    for row in rows {
        keys.insert(row?);
    }
    Ok(keys)
}

/// Insert the source's invocations, returning `source id -> destination id`.
fn import_invocations(
    dest: &HistoryDb,
    attribution: &WorkspaceAttribution,
    existing: &HashSet<InvocationKey>,
    report: &mut ImportReport,
) -> Result<HashMap<i64, i64>> {
    let columns = shared_columns(dest, "invocations")?;
    let id_index = columns
        .iter()
        .position(|column| column == "id")
        .ok_or_else(|| color_eyre::eyre::eyre!("source invocations table has no id column"))?;
    let insert_columns: Vec<&String> = columns
        .iter()
        .enumerate()
        .filter(|(index, _)| *index != id_index)
        .map(|(_, column)| column)
        .collect();

    let select_sql = format!(
        "SELECT {} FROM src.invocations ORDER BY id",
        columns.join(", ")
    );
    let insert_sql = format!(
        "INSERT INTO main.invocations ({}) VALUES ({})",
        insert_columns
            .iter()
            .map(|column| column.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        (1..=insert_columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut select = dest.conn.prepare(&select_sql)?;
    let mut insert = dest.conn.prepare(&insert_sql)?;
    let mut rows = select.query([])?;
    let mut id_map = HashMap::new();
    let mut seen_in_source: HashSet<InvocationKey> = HashSet::new();

    while let Some(row) = rows.next()? {
        report.invocations_seen += 1;
        let source_id: i64 = row.get(id_index)?;
        let mut values: Vec<Value> = Vec::with_capacity(insert_columns.len());
        for (index, _) in columns.iter().enumerate() {
            if index != id_index {
                values.push(row.get::<_, Value>(index)?);
            }
        }
        apply_attribution(&insert_columns, &mut values, attribution);

        let key = row_invocation_key(&insert_columns, &values);
        if existing.contains(&key) || !seen_in_source.insert(key) {
            report.invocations_duplicate += 1;
            continue;
        }

        insert
            .execute(rusqlite::params_from_iter(values.iter()))
            .context("failed to insert imported invocation")?;
        id_map.insert(source_id, dest.conn.last_insert_rowid());
        report.invocations_inserted += 1;
    }

    Ok(id_map)
}

/// Fill workspace provenance for a row whose source database predates it.
fn apply_attribution(
    columns: &[&String],
    values: &mut [Value],
    attribution: &WorkspaceAttribution,
) {
    for (index, column) in columns.iter().enumerate() {
        let fill = match column.as_str() {
            "workspace_root" => attribution.root.as_str(),
            "workspace_name" => attribution.name.as_str(),
            _ => continue,
        };
        let empty = matches!(&values[index], Value::Null)
            || matches!(&values[index], Value::Text(text) if text.is_empty());
        if empty {
            values[index] = Value::Text(fill.to_string());
        }
    }
}

fn row_invocation_key(columns: &[&String], values: &[Value]) -> InvocationKey {
    let text = |name: &str| -> String {
        columns
            .iter()
            .position(|column| column.as_str() == name)
            .and_then(|index| match &values[index] {
                Value::Text(text) => Some(text.clone()),
                _ => None,
            })
            .unwrap_or_default()
    };
    let pid = columns
        .iter()
        .position(|column| column.as_str() == "pid")
        .and_then(|index| match &values[index] {
            Value::Integer(pid) => Some(*pid),
            _ => None,
        });
    invocation_key(&text("host"), &text("cwd"), pid, &text("started_at"), &text("command"))
}

/// Column names present in both the source and destination copies of `table`.
///
/// Source databases can predate destination columns and vice versa; copying the
/// intersection keeps an older ledger importable without a schema rewrite.
fn shared_columns(dest: &HistoryDb, table: &str) -> Result<Vec<String>> {
    let source = table_columns(dest, "src", table)?;
    if source.is_empty() {
        return Ok(Vec::new());
    }
    let destination: HashSet<String> = table_columns(dest, "main", table)?.into_iter().collect();
    Ok(source
        .into_iter()
        .filter(|column| destination.contains(column))
        .collect())
}

fn table_columns(dest: &HistoryDb, schema: &str, table: &str) -> Result<Vec<String>> {
    let mut stmt = dest
        .conn
        .prepare(&format!("PRAGMA {schema}.table_info({table})"))
        .with_context(|| format!("failed to inspect {schema}.{table}"))?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
    let mut columns = Vec::new();
    for row in rows {
        columns.push(row?);
    }
    Ok(columns)
}

/// Copy rows whose parent survived the invocation dedupe, remapping foreign keys.
///
/// `parents` names the foreign-key columns and the source-to-destination id map
/// each must be rewritten through. A row whose parent id is absent from its map
/// is skipped: its parent was a duplicate, so the row is already present.
fn copy_table(
    dest: &HistoryDb,
    table: &str,
    parents: &[(&str, &HashMap<i64, i64>)],
    attribution: &WorkspaceAttribution,
) -> Result<(usize, HashMap<i64, i64>)> {
    let columns = shared_columns(dest, table)?;
    if columns.is_empty() {
        return Ok((0, HashMap::new()));
    }
    let has_rowid_pk = columns.iter().any(|column| column == "id");
    let id_index = columns.iter().position(|column| column == "id");
    let insert_columns: Vec<&String> = columns
        .iter()
        .enumerate()
        .filter(|(index, _)| Some(*index) != id_index)
        .map(|(_, column)| column)
        .collect();

    let select_sql = format!("SELECT {} FROM src.{table}", columns.join(", "));
    let insert_sql = format!(
        "INSERT OR IGNORE INTO main.{table} ({}) VALUES ({})",
        insert_columns
            .iter()
            .map(|column| column.as_str())
            .collect::<Vec<_>>()
            .join(", "),
        (1..=insert_columns.len())
            .map(|index| format!("?{index}"))
            .collect::<Vec<_>>()
            .join(", ")
    );

    let mut select = dest.conn.prepare(&select_sql)?;
    let mut insert = dest.conn.prepare(&insert_sql)?;
    let mut rows = select.query([])?;
    let mut inserted = 0usize;
    let mut id_map = HashMap::new();

    'row: while let Some(row) = rows.next()? {
        let source_id: Option<i64> = match id_index {
            Some(index) => row.get(index)?,
            None => None,
        };
        let mut values: Vec<Value> = Vec::with_capacity(insert_columns.len());
        for (index, _) in columns.iter().enumerate() {
            if Some(index) != id_index {
                values.push(row.get::<_, Value>(index)?);
            }
        }

        for (parent_column, map) in parents {
            let Some(position) = insert_columns
                .iter()
                .position(|column| column.as_str() == *parent_column)
            else {
                continue;
            };
            match values[position] {
                Value::Integer(source_parent) => match map.get(&source_parent) {
                    Some(destination_parent) => {
                        values[position] = Value::Integer(*destination_parent);
                    }
                    // Parent deduped away (already imported) or never imported.
                    None => continue 'row,
                },
                // A nullable parent link carries no ownership to remap.
                Value::Null => {}
                _ => continue 'row,
            }
        }

        apply_attribution(&insert_columns, &mut values, attribution);

        let changed = insert
            .execute(rusqlite::params_from_iter(values.iter()))
            .with_context(|| format!("failed to insert imported {table} row"))?;
        if changed > 0 {
            inserted += 1;
            if has_rowid_pk && let Some(source_id) = source_id {
                id_map.insert(source_id, dest.conn.last_insert_rowid());
            }
        }
    }

    Ok((inserted, id_map))
}

/// What `backfill_workspace_attribution` resolved.
#[derive(Debug, Clone, Default)]
pub struct BackfillReport {
    /// Rows that had no workspace provenance before the pass.
    pub unattributed_before: usize,
    /// Rows given provenance, keyed by workspace name.
    pub attributed_by_workspace: BTreeMap<String, usize>,
    /// Rows whose working directory matched no known workspace root.
    pub unresolved: usize,
}

impl BackfillReport {
    #[must_use]
    pub fn attributed(&self) -> usize {
        self.attributed_by_workspace.values().sum()
    }
}

/// Distinct workspace roots already named by rows in the ledger.
pub fn recorded_workspace_roots(dest: &HistoryDb) -> Result<Vec<String>> {
    let mut stmt = dest
        .conn
        .prepare("SELECT DISTINCT workspace_root FROM invocations WHERE workspace_root IS NOT NULL")
        .context("failed to read recorded workspace roots")?;
    let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
    let mut roots = Vec::new();
    for row in rows {
        roots.push(row?);
    }
    Ok(roots)
}

/// Give workspace provenance to rows recorded before the ledger carried it.
///
/// The only evidence a legacy row holds about where it ran is its `cwd`, so
/// each row is matched to the longest known workspace root that is a prefix of
/// it. `candidate_roots` is supplied by the caller rather than probed from
/// disk: a worktree that has since been removed still owns its rows, and no
/// filesystem check could confirm a directory that no longer exists.
///
/// A row whose `cwd` matches nothing keeps its NULL. Guessing would put a run
/// in a workspace it never belonged to, which is worse than an honest gap.
pub fn backfill_workspace_attribution(
    dest: &HistoryDb,
    candidate_roots: &[String],
) -> Result<BackfillReport> {
    let mut roots: Vec<&String> = candidate_roots.iter().collect();
    // Longest first, so `/realm/worktrees/sinex-q102` wins over a shorter root
    // that happens to also be a prefix.
    roots.sort_by_key(|root| std::cmp::Reverse(root.len()));

    let mut report = BackfillReport::default();
    let mut stmt = dest
        .conn
        .prepare("SELECT id, cwd FROM invocations WHERE workspace_root IS NULL")
        .context("failed to scan unattributed invocations")?;
    let rows: Vec<(i64, String)> = stmt
        .query_map([], |row| {
            Ok((row.get(0)?, row.get::<_, Option<String>>(1)?.unwrap_or_default()))
        })?
        .collect::<std::result::Result<Vec<_>, _>>()?;
    drop(stmt);
    report.unattributed_before = rows.len();

    let mut update = dest.conn.prepare(
        "UPDATE invocations SET workspace_root = ?1, workspace_name = ?2 WHERE id = ?3",
    )?;
    for (id, cwd) in rows {
        let Some(root) = roots
            .iter()
            .find(|root| cwd == ***root || cwd.starts_with(&format!("{root}/")))
        else {
            report.unresolved += 1;
            continue;
        };
        let name = crate::config::workspace_name_for(Path::new(root.as_str()));
        update.execute(rusqlite::params![root, name, id])?;
        *report
            .attributed_by_workspace
            .entry(name)
            .or_insert(0usize) += 1;
    }
    Ok(report)
}

/// Attribute a source database found at `<root>/.sinex/state/xtask-history.db`
/// to the workspace at `root`.
#[must_use]
pub fn attribution_for_workspace_db(source_path: &Path) -> Option<WorkspaceAttribution> {
    let root = source_path.parent()?.parent()?.parent()?;
    if root.file_name().is_none() {
        return None;
    }
    Some(WorkspaceAttribution {
        root: root.display().to_string(),
        name: crate::config::workspace_name_for(root),
    })
}

#[cfg(test)]
#[path = "merge_test.rs"]
mod tests;
