//! Scope/output event collection, cascade resolution, archive/restore, and
//! abort handling for `ReplayExecutionEngine`. See `execution/mod.rs` for the
//! engine type itself and the public-API entry points.

use super::{
    ExpectedReplayOutput, ExpectedReplayOutputs, ExtendedMaterialOccurrenceKey,
    OperationOutputEvent, REPLAY_OUTPUT_VISIBILITY_TIMEOUT, ReplayExecutionEngine,
    ScopeInvalidationBucket,
};
use crate::runtime::automaton::invalidation::{DerivedScopeInvalidation, INVALIDATION_SUBJECT};
use crate::runtime::nats_payload::ensure_nats_payload_fits;
use crate::runtime::stream::{ReplayMaterialOccurrence, ResolvedReplayMaterial};
use sinex_db::repositories::replay::{
    ArchivedReplayScopeMetadataRow, REPLAY_ARCHIVE_PAGE_SIZE, REPLAY_SCOPE_METADATA_PAGE_SIZE,
};
use sinex_db::repositories::{DbPoolExt, EventRepositoryTx};
use sinex_primitives::domain::{EventSource, EventType, SourceIdentifier};
use sinex_primitives::events::{Event as StoredEvent, Provenance};
use sinex_primitives::{Id, Result, SinexError, Timestamp, Uuid, transport};
use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::debug;

use sinex_db::replay::state_machine::ReplayScope;

pub(crate) struct ArchivedReplayCascade {
    pub(crate) archive_reason: String,
    pub(crate) archived_count: u64,
    pub(crate) scoped_event_count: u64,
}

/// The largest root/material context sent in one source-scan command.
///
/// Keep this aligned with the event repository's bounded ID hydration limit.
/// A replay operation may contain arbitrarily many of these batches, but no
/// command or execution handoff owns the complete root set.
pub(crate) const REPLAY_EXECUTION_ROOT_BATCH_SIZE: i64 = 1_000;

pub(crate) fn replay_archive_reason(operation_id: Uuid) -> String {
    format!("superseded by replay re-execution (operation {operation_id})")
}

pub(crate) struct ReplayExecutionBatch {
    pub(crate) material_roots: Vec<StoredEvent>,
    pub(crate) replay_materials: Vec<ResolvedReplayMaterial>,
    pub(crate) replay_occurrences: Vec<ReplayMaterialOccurrence>,
    pub(crate) expected_outputs: ExpectedReplayOutputs,
    pub(crate) last_root_id: Uuid,
}

#[derive(Debug, Default)]
pub(crate) struct ReplayOutputValidation {
    matching_count: u64,
    missing_count: u64,
    unexpected_count: u64,
}

impl ReplayOutputValidation {
    pub(crate) fn complete(&self) -> bool {
        self.missing_count == 0 && self.unexpected_count == 0
    }
}

impl ReplayExecutionEngine {
    /// Stale any `derivation.projection_registry` row keyed to a scope whose
    /// events were just archived (sinex-68c.4).
    ///
    /// Runs unconditionally once the archive itself has committed —
    /// deliberately before the invalidation-signal publish step and its
    /// possible compensating restore, so a publish failure (which restores
    /// the archived cascade) can at worst leave a projection spuriously
    /// stale rather than risk one silently staying `ready` over data that
    /// really was archived and recomputed.
    pub(super) async fn stale_projection_registry_for_scopes(
        &self,
        pool: &sqlx::PgPool,
        archive_reason: &str,
        expected_scoped_event_count: u64,
        operation_id: Uuid,
    ) -> Result<()> {
        let reason = format!("replay scope invalidation (operation {operation_id})");
        let mut after_id = None;
        let mut processed = 0_u64;
        loop {
            let (scope_metadata, next_after_id, page_count) = self
                .archived_scope_metadata_page(archive_reason, after_id)
                .await?;
            if page_count == 0 {
                break;
            }
            for bucket in scope_metadata {
                for scope_key in bucket.scope_keys {
                    let staled = pool
                        .projection_registry()
                        .mark_stale_by_scope_key(&scope_key, &reason)
                        .await
                        .map_err(|error| {
                            SinexError::database(
                                "Failed to stale projection registry rows after replay archive",
                            )
                            .with_context("scope_key", scope_key.clone())
                            .with_context("operation_id", operation_id.to_string())
                            .with_source(error)
                        })?;
                    if staled > 0 {
                        debug!(
                            operation_id = %operation_id,
                            scope_key,
                            staled_rows = staled,
                            "Staled projection registry rows for archived replay scope"
                        );
                    }
                }
            }
            processed += page_count;
            after_id = next_after_id;
        }
        self.ensure_scope_metadata_complete(
            archive_reason,
            expected_scoped_event_count,
            processed,
            "stale projection registry",
        )
    }

    /// Read one bounded replay batch from the operation's archive journal.
    ///
    /// The direct roots are selected from the archive after the full cascade is
    /// committed.  This keeps scan input available while never retaining a
    /// scope-wide root-ID or `StoredEvent` vector across archive/dispatch.
    pub(crate) async fn collect_archived_replay_root_batch(
        &self,
        pool: &sqlx::PgPool,
        archive_reason: &str,
        after_id: Option<Uuid>,
    ) -> Result<Option<ReplayExecutionBatch>> {
        let material_roots = pool
            .events()
            .get_archived_replay_material_root_page(
                archive_reason,
                after_id,
                REPLAY_EXECUTION_ROOT_BATCH_SIZE,
            )
            .await
            .map_err(|error| {
                SinexError::database("Failed to hydrate archived replay-root batch")
                    .with_source(error)
            })?;
        let Some(last_root_id) = material_roots
            .last()
            .and_then(|event| event.id.map(|id| *id.as_uuid()))
        else {
            return Ok(None);
        };

        let material_ids: Vec<Uuid> = material_roots
            .iter()
            .filter_map(|event| match &event.provenance {
                Provenance::Material { id, .. } => Some(*id.as_uuid()),
                Provenance::Derived { .. } => None,
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let replay_materials = self.resolve_replay_materials(pool, &material_ids).await?;
        let replay_occurrences = Self::replay_material_occurrences(&material_roots)?;
        let expected_outputs = Self::with_logical_source_identifiers(
            Self::expected_replay_outputs(&material_roots)?,
            &replay_materials,
        )?;
        self.validate_material_authority(&expected_outputs.source_material_ids)
            .await?;

        Ok(Some(ReplayExecutionBatch {
            material_roots,
            replay_materials,
            replay_occurrences,
            expected_outputs,
            last_root_id,
        }))
    }

    pub(crate) fn merge_expected_replay_outputs(
        aggregate: &mut ExpectedReplayOutputs,
        batch: ExpectedReplayOutputs,
    ) {
        aggregate.minimum_visible_count += batch.minimum_visible_count;
        aggregate.sources.extend(batch.sources);
        aggregate.event_types.extend(batch.event_types);
        aggregate
            .logical_source_identifiers
            .extend(batch.logical_source_identifiers);
        aggregate.sources.sort_unstable();
        aggregate.sources.dedup();
        aggregate.event_types.sort_unstable();
        aggregate.event_types.dedup();
        aggregate.logical_source_identifiers.sort_unstable();
        aggregate.logical_source_identifiers.dedup();
        // Expected occurrence keys and material IDs are batch-local evidence.
        // Output matching is performed against the archive journal in the
        // database, and material authority is checked before this batch is
        // returned, so neither vector may grow with total replay scope size.
    }

    pub(super) async fn collect_operation_output_events(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
    ) -> Result<Vec<OperationOutputEvent>> {
        let rows = sqlx::query!(
            r#"
            SELECT
                id AS "id!",
                source AS "source!",
                event_type AS "event_type!",
                source_material_id,
                anchor_byte,
                offset_start,
                offset_end,
                offset_kind,
                anchor_payload_hash AS "anchor_payload_hash: Vec<u8>"
            FROM core.events
            WHERE created_by_operation_id = $1::uuid
            ORDER BY id
            "#,
            operation_id,
        )
        .fetch_all(pool)
        .await
        .map_err(|err| {
            SinexError::database("Failed to query replay operation outputs").with_std_error(&err)
        })?;

        Ok(rows
            .into_iter()
            .map(|row| OperationOutputEvent {
                id: row.id,
                source: row.source,
                event_type: row.event_type,
                source_material_id: row.source_material_id,
                anchor_byte: row.anchor_byte,
                offset_start: row.offset_start,
                offset_end: row.offset_end,
                offset_kind: row.offset_kind,
                anchor_payload_hash: row.anchor_payload_hash,
            })
            .collect())
    }

    pub(crate) fn expected_replay_outputs(
        material_roots: &[StoredEvent],
    ) -> Result<ExpectedReplayOutputs> {
        if material_roots.is_empty() {
            return Err(SinexError::invalid_state(
                "Replay output expectations require at least one material root",
            ));
        }

        let mut sources = HashSet::new();
        let mut event_types = HashSet::new();
        let mut expected_outputs = Vec::with_capacity(material_roots.len());
        let mut source_material_ids = HashSet::new();

        for event in material_roots {
            sources.insert(event.source.as_ref().to_string());
            event_types.insert(event.event_type.as_ref().to_string());
            match &event.provenance {
                Provenance::Material {
                    id,
                    anchor_byte,
                    offset_start,
                    offset_end,
                    offset_kind,
                } => {
                    source_material_ids.insert(*id.as_uuid());
                    expected_outputs.push(ExpectedReplayOutput {
                        occurrence: ExtendedMaterialOccurrenceKey {
                            source_material_id: *id.as_uuid(),
                            anchor_byte: *anchor_byte,
                            offset_start: *offset_start,
                            offset_end: *offset_end,
                            // The database representation only stores the
                            // offset kind when both range endpoints exist;
                            // mirror extract_provenance so replay validation
                            // compares canonical persisted occurrence keys.
                            offset_kind: if offset_start.is_some() && offset_end.is_some() {
                                Some(offset_kind.as_wire_str().to_string())
                            } else {
                                None
                            },
                        },
                        source: event.source.as_ref().to_string(),
                        event_type: event.event_type.as_ref().to_string(),
                    });
                }
                Provenance::Derived { .. } => {
                    return Err(SinexError::invalid_state(format!(
                        "Replay scope included non-material root '{}' / '{}'",
                        event.source, event.event_type
                    )));
                }
            }
        }

        let mut sources: Vec<_> = sources.into_iter().collect();
        sources.sort_unstable();
        let mut event_types: Vec<_> = event_types.into_iter().collect();
        event_types.sort_unstable();
        let mut source_material_ids: Vec<_> = source_material_ids.into_iter().collect();
        source_material_ids.sort_unstable();

        Ok(ExpectedReplayOutputs {
            minimum_visible_count: expected_outputs.len() as u64,
            sources,
            event_types,
            logical_source_identifiers: Vec::new(),
            expected_outputs,
            source_material_ids,
        })
    }

    /// Extract the original material occurrence coordinates before replay
    /// archives the live roots. FileDrop's append-stream material can hold
    /// multiple records, so material metadata alone cannot recover these.
    pub(crate) fn replay_material_occurrences(
        material_roots: &[StoredEvent],
    ) -> Result<Vec<ReplayMaterialOccurrence>> {
        let mut occurrences = Vec::with_capacity(material_roots.len());

        for event in material_roots {
            let Provenance::Material {
                id,
                anchor_byte,
                offset_start,
                offset_end,
                ..
            } = &event.provenance
            else {
                return Err(SinexError::invalid_state(
                    "Replay occurrence context included a non-material root",
                ));
            };

            occurrences.push(ReplayMaterialOccurrence {
                source_material_id: *id.as_uuid(),
                anchor_byte: *anchor_byte,
                offset_start: *offset_start,
                offset_end: *offset_end,
                record_metadata: file_drop_record_metadata(event)?,
            });
        }

        occurrences.sort_by_key(|occurrence| {
            (
                occurrence.source_material_id,
                occurrence.anchor_byte,
                occurrence.offset_start,
                occurrence.offset_end,
            )
        });
        occurrences.dedup_by(|left, right| {
            left.source_material_id == right.source_material_id
                && left.anchor_byte == right.anchor_byte
                && left.offset_start == right.offset_start
                && left.offset_end == right.offset_end
        });
        Ok(occurrences)
    }

    /// Validate the byte coordinates that the CAS replay route requires before
    /// it can safely cross the archive boundary.
    pub(crate) fn validate_replay_material_occurrences(
        occurrences: &[ReplayMaterialOccurrence],
    ) -> Result<()> {
        for occurrence in occurrences {
            if occurrence.anchor_byte < 0 {
                return Err(SinexError::invalid_state(
                    "Replay occurrence anchor_byte must be non-negative",
                )
                .with_context(
                    "source_material_id",
                    occurrence.source_material_id.to_string(),
                )
                .with_context("anchor_byte", occurrence.anchor_byte.to_string()));
            }
            let Some(offset_start) = occurrence.offset_start else {
                return Err(SinexError::invalid_state(
                    "Replay occurrence is missing offset_start; cannot safely recover bytes",
                )
                .with_context(
                    "source_material_id",
                    occurrence.source_material_id.to_string(),
                )
                .with_context("anchor_byte", occurrence.anchor_byte.to_string()));
            };
            let Some(offset_end) = occurrence.offset_end else {
                return Err(SinexError::invalid_state(
                    "Replay occurrence is missing offset_end; cannot safely recover bytes",
                )
                .with_context(
                    "source_material_id",
                    occurrence.source_material_id.to_string(),
                )
                .with_context("anchor_byte", occurrence.anchor_byte.to_string()));
            };
            if offset_start != occurrence.anchor_byte || offset_end < offset_start {
                return Err(SinexError::invalid_state(
                    "Replay occurrence byte coordinates are inconsistent",
                )
                .with_context(
                    "source_material_id",
                    occurrence.source_material_id.to_string(),
                )
                .with_context("anchor_byte", occurrence.anchor_byte.to_string())
                .with_context("offset_start", offset_start.to_string())
                .with_context("offset_end", offset_end.to_string()));
            }
        }
        Ok(())
    }

    pub(crate) fn logical_source_identifier(material: &ResolvedReplayMaterial) -> String {
        material
            .material_metadata
            .get("logical_source_identifier")
            .and_then(serde_json::Value::as_str)
            .map_or_else(
                || {
                    SourceIdentifier::from_wire(&material.source_identifier)
                        .map_or_else(|_| material.source_identifier.clone(), |si| si.logical_id)
                },
                str::to_string,
            )
    }

    pub(crate) fn with_logical_source_identifiers(
        mut expected: ExpectedReplayOutputs,
        replay_materials: &[ResolvedReplayMaterial],
    ) -> Result<ExpectedReplayOutputs> {
        let mut logical_source_identifiers = replay_materials
            .iter()
            .map(Self::logical_source_identifier)
            .collect::<Vec<_>>();
        logical_source_identifiers.sort_unstable();
        logical_source_identifiers.dedup();

        if logical_source_identifiers.is_empty() {
            return Err(SinexError::invalid_state(
                "Replay output expectations require at least one logical source identifier",
            ));
        }

        expected.logical_source_identifiers = logical_source_identifiers;
        Ok(expected)
    }

    pub(crate) fn scan_control_source_name(
        scope: &ReplayScope,
        replay_materials: &[ResolvedReplayMaterial],
    ) -> Result<String> {
        let mut control_sources = replay_materials
            .iter()
            .filter_map(|material| {
                material
                    .material_metadata
                    .get("logical_source_identifier")
                    .and_then(serde_json::Value::as_str)
                    .filter(|source| !source.trim().is_empty())
                    .map(str::to_string)
            })
            .collect::<Vec<_>>();
        control_sources.sort_unstable();
        control_sources.dedup();

        match control_sources.as_slice() {
            [] => Ok(scope.source_name.clone()),
            [source] => Ok(source.clone()),
            sources => Err(SinexError::invalid_state(format!(
                "Replay scope spans multiple source runtime identities ({}) but scan dispatch requires one control subject",
                sources.join(", ")
            ))),
        }
    }

    pub(crate) async fn count_visible_replay_outputs(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        archive_reason: &str,
        expected: &ExpectedReplayOutputs,
    ) -> Result<i64> {
        Ok(self
            .validate_replay_outputs(pool, operation_id, archive_reason, expected)
            .await?
            .matching_count as i64)
    }

    pub(crate) async fn validate_replay_outputs(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        archive_reason: &str,
        expected: &ExpectedReplayOutputs,
    ) -> Result<ReplayOutputValidation> {
        let counts = pool
            .replay()
            .replay_output_match_counts(archive_reason, operation_id)
            .await?;
        Ok(ReplayOutputValidation {
            matching_count: counts.matching_count,
            missing_count: counts.missing_count,
            unexpected_count: counts.unexpected_count,
        })
    }

    pub(crate) async fn wait_for_replay_outputs_visible(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        archive_reason: &str,
        expected: &ExpectedReplayOutputs,
    ) -> Result<()> {
        let timeout = self
            .scan_completion_timeout
            .min(REPLAY_OUTPUT_VISIBILITY_TIMEOUT);

        let wait_result = tokio::time::timeout(timeout, async {
            loop {
                let validation = self
                    .validate_replay_outputs(pool, operation_id, archive_reason, expected)
                    .await?;
                if validation.complete() {
                    debug!(
                        operation_id = %operation_id,
                        visible_count = validation.matching_count,
                        minimum_visible_count = expected.minimum_visible_count,
                        "Replay outputs are query-visible"
                    );
                    return Ok::<(), SinexError>(());
                }

                tokio::time::sleep(Self::EXECUTION_STATE_POLL_INTERVAL).await;
            }
        })
        .await;

        match wait_result {
            Ok(result) => result,
            Err(_timeout) => {
                // sinex-xixl: a genuine DB failure on this final probe must not
                // be collapsed into a synthetic "-1 visible" timeout message —
                // that misclassifies a persistence/availability outage as mere
                // visibility lag and discards the real error entirely.
                match self
                    .validate_replay_outputs(pool, operation_id, archive_reason, expected)
                    .await
                {
                    Ok(validation) => Err(SinexError::timeout(format!(
                        "Replay outputs did not match the archived source-material occurrence scope after successful scan within {:?} (matching={}, missing={}, unexpected={}, expected={})",
                        timeout,
                        validation.matching_count,
                        validation.missing_count,
                        validation.unexpected_count,
                        expected.minimum_visible_count,
                    ))),
                    Err(probe_error) => Err(SinexError::database(format!(
                        "Replay outputs were not query-visible after successful scan within {timeout:?}, and the final visibility probe itself failed"
                    ))
                    .with_source(probe_error)),
                }
            }
        }
    }

    pub(crate) async fn resolve_replay_materials(
        &self,
        pool: &sqlx::PgPool,
        material_ids: &[Uuid],
    ) -> Result<Vec<ResolvedReplayMaterial>> {
        let mut resolved = Vec::with_capacity(material_ids.len());
        let mut missing = Vec::new();

        for material_id in material_ids {
            let record = pool
                .source_materials()
                .get_by_id(Id::from_uuid(*material_id))
                .await
                .map_err(|err| {
                    SinexError::database("Failed to resolve source material for replay")
                        .with_source(err)
                })?;

            match record {
                Some(record) => resolved.push(ResolvedReplayMaterial::from(record)),
                None => missing.push(*material_id),
            }
        }

        if !missing.is_empty() {
            return Err(SinexError::not_found(format!(
                "Replay scope referenced missing source materials: {}",
                missing
                    .iter()
                    .map(Uuid::to_string)
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }

        Ok(resolved)
    }

    /// Validate every material selected by the live scope before archiving its
    /// roots. The source scan may only run after this durable authority check.
    pub(crate) async fn validate_scope_material_authority(
        &self,
        scope: &ReplayScope,
    ) -> Result<()> {
        if self.material_authority.is_none() {
            return Ok(());
        }

        let mut after_id = None;
        let mut validated = HashSet::new();
        loop {
            let root_ids = self
                .replay
                .scope_root_ids_page(scope, after_id, REPLAY_EXECUTION_ROOT_BATCH_SIZE)
                .await?;
            let Some(last_id) = root_ids.last().copied() else {
                break;
            };
            after_id = Some(last_id);
            let material_ids = sqlx::query_scalar::<_, Uuid>(
                "SELECT DISTINCT source_material_id FROM core.events WHERE id = ANY($1::uuid[]) AND source_material_id IS NOT NULL",
            )
            .bind(&root_ids)
            .fetch_all(self.replay.pool())
            .await
            .map_err(|error| {
                SinexError::database("Failed to collect replay source-material authority scope")
                    .with_source(error)
            })?;
            let unseen = material_ids
                .into_iter()
                .filter(|material_id| validated.insert(*material_id))
                .collect::<Vec<_>>();
            self.validate_material_authority(&unseen).await?;
        }
        Ok(())
    }

    /// Validate the replay coordinates and adapter metadata for every selected
    /// material root before the archive boundary.  The source scan consumes
    /// these coordinates after archiving; discovering a missing range or a
    /// malformed FileDrop path only after archive would turn a caller error
    /// into a compensation/recovery operation.
    pub(crate) async fn validate_scope_replay_inputs(
        &self,
        pool: &sqlx::PgPool,
        scope: &ReplayScope,
    ) -> Result<()> {
        let mut after_id = None;
        loop {
            let root_ids = self
                .replay
                .scope_root_ids_page(scope, after_id, REPLAY_EXECUTION_ROOT_BATCH_SIZE)
                .await?;
            let Some(last_root_id) = root_ids.last().copied() else {
                break;
            };

            let typed_ids = root_ids
                .iter()
                .copied()
                .map(sinex_primitives::Id::from_uuid)
                .collect::<Vec<_>>();
            let roots = pool
                .events()
                .get_by_ids(&typed_ids)
                .await
                .map_err(|error| {
                    SinexError::database("Failed to hydrate replay roots before archive")
                        .with_source(error)
                })?;
            if roots.len() != root_ids.len() {
                return Err(SinexError::invalid_state(
                    "Replay root set changed while validating material coordinates before archive",
                )
                .with_context("expected_root_count", root_ids.len().to_string())
                .with_context("actual_root_count", roots.len().to_string()));
            }

            // This validates material provenance and FileDrop path metadata
            // without retaining the whole replay scope in memory.
            let occurrences = Self::replay_material_occurrences(&roots)?;
            Self::validate_replay_material_occurrences(&occurrences)?;
            after_id = Some(last_root_id);
        }
        Ok(())
    }

    /// Re-check the same authority after re-emission and before success.
    pub(crate) async fn validate_material_authority(&self, material_ids: &[Uuid]) -> Result<()> {
        let Some(authority) = &self.material_authority else {
            return Ok(());
        };
        for material_id in material_ids {
            authority
                .retrieve_material_replay_content(*material_id)
                .await
                .map_err(|error| {
                    SinexError::validation(
                        "Replay source material authority is unreadable or inconsistent",
                    )
                    .with_context("source_material_id", material_id.to_string())
                    .with_source(error)
                })?;
        }
        Ok(())
    }

    pub(crate) async fn archive_replay_cascade_atomically(
        &self,
        pool: &sqlx::PgPool,
        operation_id: Uuid,
        scope: &ReplayScope,
        execution_window: (Timestamp, Timestamp),
        expected_root_count: u64,
        archived_by: &str,
    ) -> Result<ArchivedReplayCascade> {
        if expected_root_count == 0 {
            return Ok(ArchivedReplayCascade {
                archive_reason: replay_archive_reason(operation_id),
                archived_count: 0,
                scoped_event_count: 0,
            });
        }

        self.maybe_fail_scope_metadata_collection().map_err(|err| {
            SinexError::service("Failed to collect replay cascade scope metadata").with_source(err)
        })?;

        let session_id = format!("replay_{}", operation_id.simple());
        let reason = replay_archive_reason(operation_id);
        let operation_id_string = operation_id.to_string();
        let archived_by = archived_by.to_string();

        pool.with_transaction(async |tx| {
            sqlx::query!("LOCK TABLE core.events IN SHARE MODE")
                .execute(&mut **tx)
                .await
                .map_err(|err| {
                    SinexError::database("Failed to lock replay archive event set").with_source(err)
                })?;

            let mut repo_tx = EventRepositoryTx::new(tx);
            let table_name = repo_tx
                .prepare_cascade_session(&session_id, false)
                .await
                .map_err(|e| e.with_context("operation", "prepare replay cascade session"))?;
            let direct_root_count = repo_tx
                .populate_cascade_roots_for_replay_scope(&table_name, scope, execution_window)
                .await
                .map_err(|e| e.with_context("operation", "populate replay cascade roots"))?;
            if u64::try_from(direct_root_count).unwrap_or(u64::MAX) != expected_root_count {
                repo_tx
                    .cleanup_cascade_session(&table_name)
                    .await
                    .map_err(|error| {
                        error.with_context("operation", "cleanup stale replay cascade session")
                    })?;
                return Err(SinexError::invalid_state(
                    "Replay root count changed while execution was starting; refresh preview before execution",
                )
                .with_context("operation_id", operation_id.to_string())
                .with_context("expected_root_event_count", expected_root_count.to_string())
                .with_context("actual_root_event_count", direct_root_count.to_string()));
            }
            repo_tx
                .expand_cascade(
                    &table_name,
                    i32::try_from(sinex_primitives::constants::replay::DEFAULT_CASCADE_MAX_DEPTH)
                        .unwrap_or(i32::MAX),
                )
                .await
                .map_err(|e| e.with_context("operation", "expand replay cascade"))?;

            let (scoped_event_count, bucket_count, scope_key_count) = repo_tx
                .cascade_scope_invalidation_counts(&table_name)
                .await
                .map_err(|e| e.with_context("operation", "count replay cascade scope metadata"))?;

            let archived_count = repo_tx
                .execute_cascade_archive_from_table(
                    &table_name,
                    reason.as_str(),
                    &operation_id_string,
                    archived_by.as_str(),
                )
                .await
                .map_err(|e| e.with_context("operation", "archive replay cascade"))?;

            self.replay
                .record_scope_invalidations_pending_with_tx(
                    repo_tx.transaction(),
                    operation_id,
                    archived_count,
                    bucket_count,
                    scope_key_count,
                    usize::try_from(scoped_event_count).map_err(|_| {
                        SinexError::invalid_state("replay cascade scoped event count exceeds usize")
                    })?,
                    // Durable replay journal (#2194): persist only the
                    // operation-unique archive reason. Recovery pages the
                    // archive instead of serializing every cascade UUID.
                    reason.as_str(),
                )
                .await
                .map_err(|e| {
                    e.with_context("operation", "record replay invalidation recovery marker")
                })?;

            repo_tx
                .cleanup_cascade_session(&table_name)
                .await
                .map_err(|e| e.with_context("operation", "cleanup replay cascade session"))?;

            Ok(ArchivedReplayCascade {
                archive_reason: reason.clone(),
                archived_count,
                scoped_event_count,
            })
        })
        .await
        .map_err(|err| {
            SinexError::database("Failed to archive replay cascade atomically").with_source(err)
        })
    }

    /// Collect scope metadata from events about to be archived.
    ///
    /// Returns `(event_type, scope_keys)` pairs grouped by `event_type`.
    /// Use when a caller needs scope invalidation metadata before moving events
    /// out of `core.events`.
    #[cfg(test)]
    pub(crate) async fn collect_cascade_scope_metadata(
        &self,
        pool: &sqlx::PgPool,
        cascade_ids: &[Uuid],
    ) -> Result<Vec<ScopeInvalidationBucket>> {
        if cascade_ids.is_empty() {
            return Ok(Vec::new());
        }

        self.maybe_fail_scope_metadata_collection().map_err(|err| {
            SinexError::service("Failed to collect replay cascade scope metadata").with_source(err)
        })?;

        // Query scope metadata for cascade events that have scope_keys so invalidations
        // stay bucketed by the archived event source + type pair.
        let rows = sqlx::query!(
            "SELECT id, source, event_type, scope_key, \
                    (source_event_ids IS NOT NULL) AS \"has_lineage!: bool\" \
             FROM core.events \
             WHERE id = ANY($1::uuid[]) AND scope_key IS NOT NULL",
            cascade_ids,
        )
        .fetch_all(pool)
        .await
        .map_err(|err| {
            SinexError::database("Failed to collect cascade scope metadata").with_std_error(&err)
        })?;

        let mut grouped: HashMap<(EventSource, EventType, bool), ScopeInvalidationBucket> =
            HashMap::new();
        for row in rows {
            if let Some(sk) = row.scope_key {
                let event_source = EventSource::new(row.source.clone()).map_err(|error| {
                    SinexError::validation(format!(
                        "Invalid event source '{}' in replay cascade scope metadata: {error}",
                        row.source
                    ))
                })?;
                let event_type = EventType::new(row.event_type.clone()).map_err(|error| {
                    SinexError::validation(format!(
                        "Invalid event type '{}' in replay cascade scope metadata: {error}",
                        row.event_type
                    ))
                })?;
                let bucket = grouped
                    .entry((event_source.clone(), event_type.clone(), row.has_lineage))
                    .or_insert_with(|| ScopeInvalidationBucket {
                        event_ids: Vec::new(),
                        event_source,
                        event_type,
                        has_lineage: row.has_lineage,
                        scope_keys: Vec::new(),
                    });
                bucket.event_ids.push(row.id);
                bucket.scope_keys.push(sk);
            }
        }

        for bucket in grouped.values_mut() {
            bucket.event_ids.sort_unstable();
            bucket.event_ids.dedup();
            bucket.scope_keys.sort_unstable();
            bucket.scope_keys.dedup();
        }

        Ok(grouped.into_values().collect())
    }

    /// Publish scope invalidation signals for archived events.
    ///
    /// Notifies automatons that scopes need recomputation because events
    /// were archived. Only publishes for events that had `scope_keys`.
    pub(crate) async fn publish_scope_invalidations(
        &self,
        archive_reason: &str,
        expected_scoped_event_count: u64,
        operation_id: Uuid,
    ) -> Result<()> {
        let invalidation_subject = self.env.nats_subject(INVALIDATION_SUBJECT);
        let mut after_id = None;
        let mut processed = 0_u64;
        loop {
            let (scope_metadata, next_after_id, page_count) = self
                .archived_scope_metadata_page(archive_reason, after_id)
                .await?;
            if page_count == 0 {
                break;
            }
            for bucket in scope_metadata {
                let scope_count = bucket.scope_keys.len();
                let event_type = bucket.event_type.clone();
                let invalidation = DerivedScopeInvalidation::archived(
                    bucket.event_ids,
                    bucket.event_source,
                    bucket.event_type,
                )
                .with_has_lineage(bucket.has_lineage)
                .with_operation(operation_id)
                .with_scope_keys(bucket.scope_keys);

                let payload = serde_json::to_vec(&invalidation).map_err(|error| {
                    SinexError::serialization(format!(
                        "Failed to serialize replay scope invalidation for event type '{event_type}' (scope_count={scope_count}): {error}"
                    ))
                    .with_std_error(&error)
                })?;
                self.maybe_fail_scope_invalidation_publish()?;
                ensure_nats_payload_fits(
                    "replay scope invalidation",
                    &invalidation_subject,
                    payload.len(),
                )?;
                let mut headers = async_nats::HeaderMap::new();
                transport::insert_transport_class_headers(
                    &mut headers,
                    transport::Class::Invalidation,
                );
                if let Err(error) = self
                    .js
                    .publish_with_headers(invalidation_subject.clone(), headers, payload.into())
                    .await
                {
                    return Err(SinexError::nats_publish(format!(
                        "Failed to publish replay scope invalidation for event type '{event_type}' (scope_count={scope_count}): {error}"
                    ))
                    .with_std_error(&error));
                }
                debug!(
                    operation_id = %operation_id,
                    event_type = %event_type,
                    scope_count,
                    "Published scope invalidation"
                );
            }
            processed += page_count;
            after_id = next_after_id;
        }
        self.ensure_scope_metadata_complete(
            archive_reason,
            expected_scoped_event_count,
            processed,
            "publish scope invalidations",
        )
    }

    pub(crate) async fn restore_cascade(
        &self,
        pool: &sqlx::PgPool,
        archive_reason: &str,
        expected_archived_count: u64,
        operation_id: Uuid,
    ) -> Result<u64> {
        if expected_archived_count == 0 {
            return Ok(0);
        }

        let mut restored = 0_u64;
        loop {
            let page = self
                .replay
                .pool()
                .replay()
                .archived_replay_event_ids_page(archive_reason, REPLAY_ARCHIVE_PAGE_SIZE)
                .await?;
            if page.is_empty() {
                break;
            }
            let restored_page = pool
                .events()
                .execute_cascade_restore(&page, &operation_id.to_string())
                .await
                .map_err(|err| {
                    SinexError::database(
                        "Failed to restore archived replay cascade after replay dispatch failure",
                    )
                    .with_source(err)
                })?;
            if restored_page == 0 {
                return Err(SinexError::service(format!(
                    "Replay cascade restoration made no progress for archive page ({} rows); conflicting archived rows remain and operator recovery is required for operation {operation_id}",
                    page.len()
                ))
                .with_context("archive_reason", archive_reason.to_string()));
            }
            restored += restored_page;
        }
        Ok(restored)
    }

    pub(crate) async fn abort_before_scan_ack(
        &self,
        pool: &sqlx::PgPool,
        archive_reason: &str,
        archived_count: u64,
        scoped_event_count: u64,
        operation_id: Uuid,
        error: SinexError,
    ) -> Result<u64> {
        // The archive is the only durable source for scope metadata. Publish
        // compensation before restoring rows (which removes their archive
        // records), then restore only after the complete bounded pass has
        // succeeded. A failed page therefore leaves the authoritative archive
        // intact for operator recovery instead of silently dropping scopes.
        if let Err(invalidation_error) = self
            .publish_scope_invalidations(archive_reason, scoped_event_count, operation_id)
            .await
        {
            return Err(SinexError::service(format!(
                "Replay dispatch failed before source acknowledgement, and publishing compensating scope invalidations from the archive also failed: {invalidation_error}"
            ))
            .with_source(error)
            .with_source(invalidation_error));
        }
        let restored = match self
            .restore_cascade(pool, archive_reason, archived_count, operation_id)
            .await
        {
            Ok(restored) => restored,
            Err(restore_error) => {
                return Err(SinexError::service(format!(
                    "Replay dispatch failed before source acknowledgement, and restoring the archived cascade also failed: {restore_error}"
                ))
                .with_source(error)
                .with_source(restore_error));
            }
        };
        if restored != archived_count {
            return Err(SinexError::service(format!(
                "Replay dispatch failed before source acknowledgement; restored only {restored}/{} archived cascade members and operator recovery is required",
                archived_count
            ))
            .with_source(error));
        }

        Err(SinexError::service(
            "Replay dispatch failed before source acknowledgement; restored archived cascade and published compensating scope invalidations",
        )
        .with_source(error))
    }

    async fn archived_scope_metadata_page(
        &self,
        archive_reason: &str,
        after_id: Option<Uuid>,
    ) -> Result<(Vec<ScopeInvalidationBucket>, Option<Uuid>, u64)> {
        let rows = self
            .replay
            .pool()
            .replay()
            .archived_replay_scope_metadata_page(
                archive_reason,
                after_id,
                REPLAY_SCOPE_METADATA_PAGE_SIZE,
            )
            .await?;
        let page_count = u64::try_from(rows.len())
            .map_err(|_| SinexError::invalid_state("replay scope metadata page exceeds u64"))?;
        let next_after_id = rows.last().map(|row| row.id);
        let buckets = Self::group_scope_metadata_page(rows)?;
        Ok((buckets, next_after_id, page_count))
    }

    fn group_scope_metadata_page(
        rows: Vec<ArchivedReplayScopeMetadataRow>,
    ) -> Result<Vec<ScopeInvalidationBucket>> {
        if rows.len() > REPLAY_SCOPE_METADATA_PAGE_SIZE as usize {
            return Err(SinexError::invalid_state(format!(
                "Replay scope metadata repository returned {} rows for a {}-row page",
                rows.len(),
                REPLAY_SCOPE_METADATA_PAGE_SIZE
            )));
        }

        let mut grouped: HashMap<(EventSource, EventType, bool), ScopeInvalidationBucket> =
            HashMap::new();
        for row in rows {
            let event_source = EventSource::new(row.source.clone()).map_err(|error| {
                SinexError::validation(format!(
                    "Invalid event source '{}' in replay archive scope metadata: {error}",
                    row.source
                ))
            })?;
            let event_type = EventType::new(row.event_type.clone()).map_err(|error| {
                SinexError::validation(format!(
                    "Invalid event type '{}' in replay archive scope metadata: {error}",
                    row.event_type
                ))
            })?;
            let bucket = grouped
                .entry((event_source.clone(), event_type.clone(), row.has_lineage))
                .or_insert_with(|| ScopeInvalidationBucket {
                    event_ids: Vec::new(),
                    event_source,
                    event_type,
                    has_lineage: row.has_lineage,
                    scope_keys: Vec::new(),
                });
            bucket.event_ids.push(row.id);
            bucket.scope_keys.push(row.scope_key);
        }

        for bucket in grouped.values_mut() {
            bucket.event_ids.sort_unstable();
            bucket.event_ids.dedup();
            bucket.scope_keys.sort_unstable();
            bucket.scope_keys.dedup();
        }
        Ok(grouped.into_values().collect())
    }

    fn ensure_scope_metadata_complete(
        &self,
        archive_reason: &str,
        expected: u64,
        processed: u64,
        action: &str,
    ) -> Result<()> {
        if processed == expected {
            return Ok(());
        }
        Err(SinexError::invalid_state(format!(
            "Replay {action} processed {processed}/{expected} archived scope metadata rows; refusing incomplete cascade handling"
        ))
        .with_context("archive_reason", archive_reason.to_string()))
    }

    /// Timeout for the source to acknowledge the scan command.
    pub(crate) const SCAN_ACK_TIMEOUT: Duration = Duration::from_secs(10);
    /// Timeout for the entire scan operation to complete.
    pub(crate) const SCAN_COMPLETION_TIMEOUT: Duration = Duration::from_mins(10);
}

fn file_drop_record_metadata(event: &StoredEvent) -> Result<serde_json::Value> {
    let event_type = event.event_type.as_ref().to_string();
    let payload = &event.payload;
    let (event_kind, path) = match event_type.as_str() {
        "file.created" | "file.modified" | "file.deleted" => (
            match event_type.as_str() {
                "file.created" => "Created",
                "file.modified" => "Modified",
                _ => "Deleted",
            },
            payload.get("path"),
        ),
        "file.moved" => ("Moved", payload.get("new_path")),
        _ => return Ok(serde_json::Value::Null),
    };
    let Some(path) = path else {
        return Err(SinexError::invalid_state(
            "FileDrop replay root payload is missing its path",
        ));
    };

    let mut metadata = serde_json::json!({
        "event_kind": event_kind,
        "path": path,
    });
    if event_type == "file.modified"
        && let Some(size) = payload.get("size")
    {
        metadata["content_size_bytes"] = size.clone();
    }
    if event_type == "file.moved" {
        if let Some(old_path) = payload.get("old_path") {
            metadata["move_from_path"] = old_path.clone();
        }
        metadata["move_to_path"] = path.clone();
        metadata["move_role"] = serde_json::Value::String("To".to_string());
    }
    Ok(metadata)
}
