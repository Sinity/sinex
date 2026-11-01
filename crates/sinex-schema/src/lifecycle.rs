//! The lifecycle's storage: material status and its history, operations with
//! their counts, positions, result and progress, tombstone skeletons with the
//! record's place, and dirty keys held while an operation runs. The upgrade
//! also removes the remembered hidden records and excised digests, whose
//! behaviour the lifecycle does not have, and renames their reasons.
use crate::Checks;
use sinex_model::lifecycle::{MaterialStatus, OperationResult};
use sinex_model::{ArchiveReason, DbCheck};

/// The tombstone columns that name the record's place and why it left.
const TOMBSTONE_PLACE: &str = ",\n  ts_quality text,\n  reason text,\n  material_id uuid,\n  anchor bigint,\n  output_index bigint,\n  key_hash text";

pub(crate) fn statements() -> Vec<String> {
    let checks = Checks::from_model();
    vec![
        "DO $$ BEGIN IF EXISTS (SELECT 1 FROM core.ingress_frames WHERE outcome->>'refusal_kind' IN ('excised', 'partially_excised')) THEN RAISE EXCEPTION 'ingress frames record excision refusals; decide how a redelivered frame is answered before upgrading'; END IF; END $$".into(),
        format!(
            "ALTER TABLE raw.materials ADD COLUMN status text NOT NULL DEFAULT 'live' {}",
            MaterialStatus::check("status")
        ),
        "ALTER TABLE raw.materials ADD COLUMN status_operation uuid".into(),
        crate::tables::material_changes(&checks),
        crate::tables::MATERIAL_CHANGES_OPERATION.into(),
        crate::tables::ARCHIVED_OPERATION.into(),
        crate::tables::ARCHIVE_CHANGES_RESTORED.into(),
        "ALTER TABLE core.operations ADD COLUMN start_seq bigint".into(),
        "ALTER TABLE core.operations ADD COLUMN end_seq bigint".into(),
        format!(
            "ALTER TABLE core.operations ADD COLUMN result text {}",
            OperationResult::check("result")
        ),
        "ALTER TABLE core.operations ADD COLUMN counts jsonb NOT NULL DEFAULT '{}'::jsonb".into(),
        "ALTER TABLE core.operations ADD COLUMN progress jsonb NOT NULL DEFAULT '{}'::jsonb".into(),
        "ALTER TABLE core.operations ADD COLUMN refusals jsonb NOT NULL DEFAULT '[]'::jsonb".into(),
        "ALTER TABLE derivation.dirty ADD COLUMN held_by uuid".into(),
        "ALTER TABLE core.event_tombstones ADD COLUMN ts_quality text, ADD COLUMN reason text, ADD COLUMN material_id uuid, ADD COLUMN anchor bigint, ADD COLUMN output_index bigint, ADD COLUMN key_hash text".into(),
        "ALTER TABLE audit.archived DROP CONSTRAINT archived_reason_check".into(),
        "UPDATE audit.archived SET reason = 'archive' WHERE reason = 'hide'".into(),
        format!(
            "ALTER TABLE audit.archived ADD CONSTRAINT archived_reason_check {}",
            ArchiveReason::check("reason")
        ),
        "ALTER TABLE audit.archive_changes DROP CONSTRAINT archive_changes_reason_check, DROP CONSTRAINT archive_changes_check".into(),
        "UPDATE audit.archive_changes SET reason = CASE reason WHEN 'hide' THEN 'archive' ELSE 'restore' END WHERE reason IN ('hide', 'unhide')".into(),
        format!(
            "ALTER TABLE audit.archive_changes ADD CONSTRAINT archive_changes_reason_check {}, ADD CONSTRAINT archive_changes_check CHECK (archived = (reason <> 'restore'))",
            checks.archive_change_reason
        ),
        "ALTER TABLE core.blobs DROP CONSTRAINT blobs_status_check, ADD CONSTRAINT blobs_status_check CHECK (status IN ('present', 'deleting', 'removed'))".into(),
        "DROP TABLE core.hidden_records".into(),
        "DROP TABLE core.excised_materials".into(),
        "UPDATE core.counters SET name = 'tombstoned' WHERE name = 'excised'".into(),
        // Comparisons record the tombstone count their inputs were read at.
        r#"UPDATE derivation.comparisons SET context = replace(context::text, '"excised":', '"tombstoned":')::jsonb WHERE context::text LIKE '%"excised":%'"#.into(),
        crate::tables::event_guard().into(),
    ]
}

/// Statements of the current description that did not exist before this
/// upgrade.
pub(crate) fn before(statement: &str) -> bool {
    !statement.starts_with("CREATE TABLE raw.material_changes (")
        && !statement.starts_with("CREATE INDEX material_changes_operation ")
        && !statement.starts_with("CREATE INDEX archived_operation ")
        && !statement.starts_with("CREATE INDEX archive_changes_restored ")
}

/// The tables this upgrade removes, as they were declared before it.
pub(crate) fn removed() -> Vec<String> {
    vec![
        "CREATE TABLE core.hidden_records (
  id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  source text NOT NULL,
  event_type text NOT NULL,
  equivalence_key text,
  material_id uuid,
  anchor bigint,
  output_index bigint,
  operation_id uuid NOT NULL,
  hidden_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT output_index_in_range CHECK (output_index >= 0),
  CONSTRAINT keyed_or_located CHECK ((equivalence_key IS NOT NULL AND material_id IS NULL AND anchor IS NULL AND output_index IS NULL) OR (equivalence_key IS NULL AND material_id IS NOT NULL AND anchor IS NOT NULL AND output_index IS NOT NULL))
)".into(),
        "CREATE UNIQUE INDEX hidden_by_key ON core.hidden_records (source, event_type, equivalence_key) WHERE equivalence_key IS NOT NULL".into(),
        "CREATE UNIQUE INDEX hidden_by_location ON core.hidden_records (material_id, anchor, event_type, output_index) WHERE material_id IS NOT NULL".into(),
        crate::description::version_nine::EXCISED_MATERIALS.into(),
        crate::description::version_nine::EXCISED_MATERIALS_BLOB.into(),
    ]
}

/// Rewrite a current statement to its form before this upgrade.
pub(crate) fn previous(statement: &mut String) {
    let checks = Checks::from_model();
    if statement.starts_with("CREATE TABLE raw.materials (") {
        *statement = statement.replace(
            &format!(
                ",\n  status text NOT NULL DEFAULT 'live' {},\n  status_operation uuid",
                checks.material_status
            ),
            "",
        );
    } else if statement.starts_with("CREATE TABLE core.operations (") {
        *statement = "CREATE TABLE core.operations (
  id uuid PRIMARY KEY,
  kind text NOT NULL,
  actor text NOT NULL,
  scope jsonb NOT NULL,
  at timestamptz NOT NULL DEFAULT now()
)"
        .into();
    } else if statement.starts_with("CREATE TABLE derivation.dirty (") {
        *statement = statement.replace("\n  held_by uuid,", "");
    } else if statement.starts_with("CREATE TABLE core.event_tombstones (") {
        *statement = statement.replace(TOMBSTONE_PLACE, "");
    } else if statement.starts_with("CREATE TABLE audit.archived (") {
        *statement = statement.replace(
            &checks.archive_reason,
            "CHECK (reason IN ('replay', 'newer_reading', 'hide', 'recomputed'))",
        );
    } else if statement.starts_with("CREATE TABLE audit.archive_changes (") {
        *statement = statement
            .replace(
                &checks.archive_change_reason,
                "CHECK (reason IN ('replay', 'newer_reading', 'hide', 'recomputed', 'unhide'))",
            )
            .replace(
                "CHECK (archived = (reason <> 'restore'))",
                "CHECK (archived = (reason <> 'unhide'))",
            );
    } else if statement.starts_with("CREATE TABLE core.blobs (") {
        *statement = statement.replace(
            "CHECK (status IN ('present', 'deleting', 'removed'))",
            "CHECK (status IN ('present', 'deleting'))",
        );
    } else if statement.starts_with("CREATE OR REPLACE FUNCTION core.event_guard()") {
        *statement = statement
            .replace("sinex.tombstone_operation", "sinex.excise_operation")
            .replace(
                "events are deleted only by a tombstone",
                "events are deleted only by an excision",
            );
    }
    *statement = statement.replace(
        "VALUES ('commit_seq', 0), ('tombstoned', 0)",
        "VALUES ('commit_seq', 0), ('excised', 0)",
    );
}
