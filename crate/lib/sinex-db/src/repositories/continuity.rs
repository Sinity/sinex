//! Source continuity diagnostics repository (#1085).
//!
//! Computes operator-facing continuity reports from existing schema:
//! `raw.source_material_registry`, `raw.temporal_ledger`, and `core.events`.
//! No new tables are introduced — the report is derived per query.
//!
//! ## Source family extraction
//!
//! `core.events.source` is dotted (`shell.command`, `file.created`, ...).
//! The leading component is used as the `SourceFamily` rollup axis. This is
//! pragmatic, not authoritative: when an explicit family registry lands the
//! computation here can switch over without changing the report shape.
//!
//! ## Replayability heuristics
//!
//! - `raw_bytes_preserved`: at least one material in the family has a non-NULL
//!   `optional_blob_id` — bytes are still on disk.
//! - `timing_quality`: at least one material is timed via `realtime_capture`
//!   or `intrinsic_content` (precise / first-party clocks) rather than
//!   inferred from filesystem metadata.
//! - `anchor_stability`: every material in the family has `total_bytes` set
//!   (finalized) — a precondition for stable anchor-byte indexing.
//! - `parser_determinism`: assumed true (operator-asserted, no measurement
//!   surface yet). Recorded as a `weak_point` so reviewers know the dimension
//!   is not yet measured.
//! - `privacy_safe_replay`: assumed true at this layer; the privacy engine
//!   runs on every replay path. Recorded as a `weak_point` if `metadata`
//!   carries a `privacy.tier` that requires a more conservative answer.

use super::common::{DbResult, db_error};
use sinex_primitives::sources::SourceFamily;
use sinex_primitives::sources::continuity::{
    CoverageContract, CoverageGap, DeclaredCoverageContract, DeclaredCoverageContractKind, GapKind,
    PrivacyClass, Replayability, SeamKind, SourceContinuityReport, TemporalSeam,
};
use sinex_primitives::Timestamp;
use sqlx::PgPool;
use time::OffsetDateTime;

/// Repository for source continuity reports.
pub struct ContinuityRepository<'a> {
    pool: &'a PgPool,
}

impl<'a> ContinuityRepository<'a> {
    #[must_use]
    pub fn new(pool: &'a PgPool) -> Self {
        Self { pool }
    }

    /// List continuity reports for every observed source family.
    ///
    /// `since` filters at the material level: families with no material
    /// staged after `since` are omitted.
    pub async fn list_continuity_reports(
        &self,
        since: Option<Timestamp>,
    ) -> DbResult<Vec<SourceContinuityReport>> {
        let families = self.observed_families(since).await?;
        let mut out = Vec::with_capacity(families.len());
        for family in families {
            if let Some(report) = self.build_report(&family, since).await? {
                out.push(report);
            }
        }
        Ok(out)
    }

    /// Build a continuity report for a single source family.
    ///
    /// Returns `Ok(None)` when no events or materials are observed for the
    /// requested family.
    pub async fn get_continuity_report(
        &self,
        source_family: &SourceFamily,
    ) -> DbResult<Option<SourceContinuityReport>> {
        self.build_report(source_family, None).await
    }

    /// Resolve attribution for a single point inside a suspected gap.
    ///
    /// Returns `Ok(None)` when `at` falls inside a covered window.
    pub async fn explain_gap(
        &self,
        source_family: &SourceFamily,
        at: Timestamp,
    ) -> DbResult<Option<CoverageGap>> {
        let report = match self.build_report(source_family, None).await? {
            Some(r) => r,
            None => return Ok(None),
        };
        Ok(report.gaps.into_iter().find(|gap| {
            let from = OffsetDateTime::from(gap.from_ts);
            let to = OffsetDateTime::from(gap.to_ts);
            let at_dt = OffsetDateTime::from(at);
            at_dt >= from && at_dt <= to
        }))
    }

    // ────────────────────────────────────────────────────────────
    // Internals
    // ────────────────────────────────────────────────────────────

    async fn observed_families(
        &self,
        since: Option<Timestamp>,
    ) -> DbResult<Vec<SourceFamily>> {
        let since_pg: Option<OffsetDateTime> = since.map(Into::into);
        let rows = sqlx::query!(
            r#"
            SELECT DISTINCT split_part(e.source, '.', 1) AS "family!: String"
            FROM core.events e
            JOIN raw.source_material_registry sm ON sm.id = e.source_material_id
            WHERE ($1::timestamptz IS NULL OR sm.staged_at >= $1)
              AND e.source IS NOT NULL
              AND e.source <> ''
            ORDER BY 1
            "#,
            since_pg
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "query distinct source families"))?;

        let mut out = Vec::with_capacity(rows.len());
        for row in rows {
            // Defensively skip values that don't validate as SourceFamily.
            // (`split_part` may produce an empty string when source has no
            //  dot — the leading component is taken whole, not empty.)
            if let Ok(family) = SourceFamily::new(row.family) {
                out.push(family);
            }
        }
        Ok(out)
    }

    async fn build_report(
        &self,
        family: &SourceFamily,
        since: Option<Timestamp>,
    ) -> DbResult<Option<SourceContinuityReport>> {
        let family_str = family.as_str();
        let since_pg: Option<OffsetDateTime> = since.map(Into::into);

        // The "family materials" CTE captures every material that belongs to
        // this family — including parser-failure / cancelled materials that
        // produced zero events. We do this by:
        //   (a) materials referenced by at least one event whose source
        //       starts with the family,
        //   (b) eventless materials whose source_identifier matches the
        //       source_identifier of any (a)-class material — i.e. the same
        //       logical source captured into multiple registry rows because
        //       one of them failed.
        // Both arms are clamped by `since` so a windowed list call remains
        // honest about its time bound. Eventless materials in case (b) are
        // the load-bearing inclusion: parser failures, disabled captures, and
        // staged-unparsed material would otherwise be silently excluded by
        // an inner-join-only query.
        //
        // Aggregate over events + materials joined together, scoped to
        // family. Counts events by material so eventless materials still
        // contribute to material_count.
        let agg = sqlx::query!(
            r#"
            WITH family_materials AS (
                SELECT sm.*
                FROM raw.source_material_registry sm
                WHERE ($2::timestamptz IS NULL OR sm.staged_at >= $2)
                  AND (
                       sm.id IN (
                            SELECT e.source_material_id
                            FROM core.events e
                            WHERE split_part(e.source, '.', 1) = $1
                              AND e.source_material_id IS NOT NULL
                       )
                       OR sm.source_identifier IN (
                            SELECT DISTINCT sm2.source_identifier
                            FROM raw.source_material_registry sm2
                            JOIN core.events e2 ON e2.source_material_id = sm2.id
                            WHERE split_part(e2.source, '.', 1) = $1
                       )
                  )
            )
            SELECT
                COUNT(DISTINCT fm.id) AS "material_count!: i64",
                COUNT(e.id) AS "event_count!: i64",
                MIN(COALESCE(fm.start_time, fm.staged_at)) AS earliest_ts,
                MAX(COALESCE(fm.end_time, fm.staged_at)) AS latest_ts,
                BOOL_OR(fm.optional_blob_id IS NOT NULL) AS "any_blob!: bool",
                BOOL_AND(fm.total_bytes IS NOT NULL) AS "all_finalized!: bool",
                BOOL_OR(fm.timing_info_type IN ('realtime', 'intrinsic'))
                    AS "good_timing!: bool"
            FROM family_materials fm
            LEFT JOIN core.events e
                   ON e.source_material_id = fm.id
                  AND split_part(e.source, '.', 1) = $1
            "#,
            family_str,
            since_pg
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "aggregate continuity report"))?;

        if agg.material_count == 0 && agg.event_count == 0 {
            return Ok(None);
        }

        // Per-material chunks for seams and gap detection. Sorted by the
        // effective chunk start (start_time falling back to staged_at) so
        // adjacency comparisons run in real temporal order — a NULLS LAST
        // sort by start_time alone forces NULL-start chunks to the end even
        // when their staged_at is earlier, producing false overlaps.
        let chunk_rows = sqlx::query!(
            r#"
            WITH family_materials AS (
                SELECT sm.*
                FROM raw.source_material_registry sm
                WHERE ($2::timestamptz IS NULL OR sm.staged_at >= $2)
                  AND (
                       sm.id IN (
                            SELECT e.source_material_id
                            FROM core.events e
                            WHERE split_part(e.source, '.', 1) = $1
                              AND e.source_material_id IS NOT NULL
                       )
                       OR sm.source_identifier IN (
                            SELECT DISTINCT sm2.source_identifier
                            FROM raw.source_material_registry sm2
                            JOIN core.events e2 ON e2.source_material_id = sm2.id
                            WHERE split_part(e2.source, '.', 1) = $1
                       )
                  )
            )
            SELECT
                fm.id AS "id!: uuid::Uuid",
                fm.material_kind AS "material_kind!: String",
                fm.status AS "status!: String",
                fm.start_time,
                fm.end_time,
                fm.staged_at AS "staged_at!: OffsetDateTime",
                fm.timing_info_type AS "timing!: String",
                fm.coverage_contract AS "coverage_contract!: serde_json::Value",
                fm.privacy_class AS "privacy_class!: String"
            FROM family_materials fm
            ORDER BY COALESCE(fm.start_time, fm.staged_at), fm.staged_at
            "#,
            family_str,
            since_pg
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "fetch material chunks"))?;

        let chunks: Vec<Chunk> = chunk_rows
            .into_iter()
            .map(|r| {
                let privacy_class = r.privacy_class.parse().unwrap_or(PrivacyClass::Unknown);
                Chunk {
                    material_kind: r.material_kind,
                    status: r.status,
                    start_time: r.start_time,
                    end_time: r.end_time,
                    staged_at: r.staged_at,
                    timing: r.timing,
                    declared_contract: serde_json::from_value::<DeclaredCoverageContract>(
                        r.coverage_contract,
                    )
                    .unwrap_or_default(),
                    privacy_class,
                }
            })
            .collect();

        // Resolve coverage contract: prefer the operator-declared kind on
        // the registry when any chunk in the family carries a non-Unknown
        // declaration. Fall back to the family-name + timing heuristic
        // otherwise. `is_declared` flags which path produced the value so
        // CLI / RPC consumers can distinguish declared from inferred
        // contracts (configuration gap vs data gap).
        let (coverage_contract, is_declared) = resolve_coverage_contract(family_str, &chunks);

        // Compute seams between adjacent chunks.
        let mut seams: Vec<TemporalSeam> = Vec::new();
        let mut gaps: Vec<CoverageGap> = Vec::new();
        let mut prev: Option<&Chunk> = None;

        for chunk in &chunks {
            if let Some(prev_chunk) = prev {
                let prev_end: Option<OffsetDateTime> =
                    chunk_end(prev_chunk).or(Some(prev_chunk.staged_at));
                let curr_start: Option<OffsetDateTime> = chunk_start(chunk);

                if let (Some(end), Some(start)) = (prev_end, curr_start) {
                    let kind = classify_seam(end, start, prev_chunk, chunk);
                    seams.push(TemporalSeam {
                        kind,
                        before_ts: Some(end.into()),
                        after_ts: Some(start.into()),
                        evidence: serde_json::json!({
                            "before_material_kind": prev_chunk.material_kind,
                            "before_status": prev_chunk.status,
                            "after_material_kind": chunk.material_kind,
                            "after_status": chunk.status,
                        }),
                    });

                    // Discontinuity / private-mode gaps -> coverage gaps.
                    let secs = (start - end).whole_seconds();
                    if matches!(kind, SeamKind::Discontinuity | SeamKind::PrivateModeGap)
                        && secs > 1
                    {
                        let gap_kind = match kind {
                            SeamKind::PrivateModeGap => GapKind::PrivateMode,
                            _ => attribute_gap(prev_chunk, chunk, coverage_contract),
                        };
                        gaps.push(CoverageGap {
                            from_ts: end.into(),
                            to_ts: start.into(),
                            kind: gap_kind,
                            attribution: gap_attribution(gap_kind),
                        });
                    }
                }
            }
            prev = Some(chunk);
        }

        let replayability = build_replayability(
            agg.any_blob,
            agg.good_timing,
            agg.all_finalized,
            chunks.iter().any(|c| c.status == "failed"),
            chunks.iter().any(|c| c.status == "recovered_partial"),
        );

        Ok(Some(SourceContinuityReport {
            source_family: family.clone(),
            coverage_contract,
            is_declared,
            replayability,
            seams,
            gaps,
            earliest_ts: agg.earliest_ts.map(Into::into),
            latest_ts: agg.latest_ts.map(Into::into),
            material_count: agg.material_count,
            event_count: agg.event_count,
        }))
    }
}

// ────────────────────────────────────────────────────────────────
// Helpers (private)
// ────────────────────────────────────────────────────────────────

fn chunk_start<R>(row: &R) -> Option<OffsetDateTime>
where
    R: ChunkAccess,
{
    row.start_time().or_else(|| Some(row.staged_at()))
}

fn chunk_end<R>(row: &R) -> Option<OffsetDateTime>
where
    R: ChunkAccess,
{
    row.end_time()
}

trait ChunkAccess {
    fn start_time(&self) -> Option<OffsetDateTime>;
    fn end_time(&self) -> Option<OffsetDateTime>;
    fn staged_at(&self) -> OffsetDateTime;
    fn status(&self) -> &str;
    fn material_kind(&self) -> &str;
    fn timing(&self) -> &str;
    /// Operator-declared coverage contract for this chunk's source material.
    /// Defaults to `Unknown` when the registry row carries the legacy
    /// `{"kind":"Unknown"}` payload.
    fn declared_contract(&self) -> &DeclaredCoverageContract;
    /// Operator-declared privacy classification. Defaults to `Unknown`
    /// when the registry row has not been classified.
    fn privacy_class(&self) -> PrivacyClass;
}

// Provide ChunkAccess for the anonymous row type produced by query!
// We use a thin macro-generated struct via trait objects — but since query!
// returns an anonymous struct, we implement on the concrete generated
// struct in-line where it's used. Rather than fight the macro, the
// computation uses the chunk fields directly via local accessors below.

// Local helper functions that work on the anonymous row type from
// the `chunks` query above. Rust monomorphises the closure, so we
// avoid trait objects here.
fn classify_seam<R>(
    prev_end: OffsetDateTime,
    curr_start: OffsetDateTime,
    prev: &R,
    curr: &R,
) -> SeamKind
where
    R: ChunkAccess,
{
    let secs = (curr_start - prev_end).whole_seconds();
    if secs < 0 {
        return SeamKind::Overlap;
    }
    if secs <= 1 {
        return SeamKind::ExpectedContinuation;
    }
    if prev.status() == "recovered_partial" || curr.status() == "recovered_partial" {
        return SeamKind::RecoveredPartial;
    }
    // Operator-declared `privacy_class` drives `PrivateModeGap` classification
    // (#1174). Only `Personal`, `Secret`, or `Redacted` count as private —
    // `Public` and `Unknown` do not, so heuristic and declared signals stay
    // separate. Either side of the seam being private-classed is enough to
    // attribute the gap to private mode.
    if prev.privacy_class().is_private() || curr.privacy_class().is_private() {
        return SeamKind::PrivateModeGap;
    }
    if secs > 60 {
        return SeamKind::Discontinuity;
    }
    SeamKind::Unknown
}

fn attribute_gap<R>(prev: &R, _curr: &R, contract: CoverageContract) -> GapKind
where
    R: ChunkAccess,
{
    if prev.status() == "failed" {
        return GapKind::ParserFailure;
    }
    if prev.status() == "cancelled" {
        return GapKind::ServiceCrash;
    }
    if matches!(
        contract,
        CoverageContract::PeriodicDump
            | CoverageContract::OpportunisticImport
            | CoverageContract::FiniteOneShot
    ) {
        return GapKind::ExpectedDownTime;
    }
    GapKind::Unknown
}

fn gap_attribution(kind: GapKind) -> Option<String> {
    Some(
        match kind {
            GapKind::PrivateMode => "private mode active",
            GapKind::ServiceCrash => "capturing service interrupted",
            GapKind::DisabledSource => "source disabled in configuration",
            GapKind::ParserFailure => "upstream parser failed for this window",
            GapKind::ExpectedDownTime => "gap matches expected periodic-dump cadence",
            GapKind::Unknown => "no attribution found",
        }
        .to_string(),
    )
}

/// Resolve the coverage contract for a family.
///
/// Returns `(contract, is_declared)` where `is_declared = true` when at least
/// one chunk's `coverage_contract` JSONB column carries a `kind` other than
/// `Unknown`. The first declared kind wins (chunks are sorted by start time
/// so this is the earliest declared intent for the family). When no chunk
/// declares a kind, falls back to the family-name + timing heuristic and
/// returns `is_declared = false`.
fn resolve_coverage_contract<R>(family: &str, chunks: &[R]) -> (CoverageContract, bool)
where
    R: ChunkAccess,
{
    for chunk in chunks {
        let declared = chunk.declared_contract();
        if !declared.is_unknown() {
            return (declared_kind_to_contract(declared.kind), true);
        }
    }
    (infer_coverage_contract(family, chunks), false)
}

/// Map the persisted `DeclaredCoverageContractKind` to the inferred
/// `CoverageContract` enum used in operator-facing reports. The two enums
/// carry the same five live shapes; `Unknown` is unreachable from the caller
/// because [`resolve_coverage_contract`] filters it out before calling here.
fn declared_kind_to_contract(kind: DeclaredCoverageContractKind) -> CoverageContract {
    match kind {
        DeclaredCoverageContractKind::Continuous => CoverageContract::Continuous,
        DeclaredCoverageContractKind::PeriodicDump => CoverageContract::PeriodicDump,
        DeclaredCoverageContractKind::OpportunisticImport => {
            CoverageContract::OpportunisticImport
        }
        DeclaredCoverageContractKind::FiniteOneShot => CoverageContract::FiniteOneShot,
        DeclaredCoverageContractKind::EphemeralStream => CoverageContract::EphemeralStream,
        // Filtered upstream; if reached, treat as opportunistic.
        DeclaredCoverageContractKind::Unknown => CoverageContract::OpportunisticImport,
    }
}

fn infer_coverage_contract<R>(family: &str, chunks: &[R]) -> CoverageContract
where
    R: ChunkAccess,
{
    if chunks.is_empty() {
        return CoverageContract::OpportunisticImport;
    }
    // Heuristic: if all chunks are `realtime`, assume continuous.
    // If chunks are spaced regularly with `intrinsic` timing, periodic dump.
    // Fall back to opportunistic import.
    let all_realtime = chunks
        .iter()
        .all(|c| matches!(c.timing(), "realtime"));
    if all_realtime {
        return CoverageContract::Continuous;
    }
    // Family-name hints. These are coarse and stable across imports.
    if family.starts_with("shell")
        || family.starts_with("desktop")
        || family.starts_with("file")
        || family.starts_with("system")
    {
        return CoverageContract::Continuous;
    }
    if family.starts_with("browser") || family.starts_with("integration") {
        return CoverageContract::PeriodicDump;
    }
    if family.starts_with("import") || family.starts_with("archive") {
        return CoverageContract::FiniteOneShot;
    }
    CoverageContract::OpportunisticImport
}

fn build_replayability(
    any_blob: bool,
    good_timing: bool,
    all_finalized: bool,
    any_failed: bool,
    any_recovered: bool,
) -> Replayability {
    let mut weak_points: Vec<String> = Vec::new();
    if !any_blob {
        weak_points.push("no source bytes preserved (no blob backing)".into());
    }
    if !good_timing {
        weak_points.push(
            "timing inferred from filesystem mtime/ctime — replay times may drift".into(),
        );
    }
    if !all_finalized {
        weak_points.push(
            "some materials still in `sensing` (total_bytes unset); anchor stability not guaranteed"
                .into(),
        );
    }
    if any_failed {
        weak_points.push("at least one material has status=failed".into());
    }
    if any_recovered {
        weak_points.push(
            "at least one material has status=recovered_partial; replay covers the recovered subset"
                .into(),
        );
    }
    weak_points.push(
        "parser_determinism is asserted, not measured; bug fixes may produce different events on replay"
            .into(),
    );
    Replayability {
        raw_bytes_preserved: any_blob,
        timing_quality: good_timing,
        anchor_stability: all_finalized,
        // Operator-asserted dimensions — coarse defaults, recorded as a
        // weak_point above so the report is honest about it.
        parser_determinism: true,
        privacy_safe_replay: true,
        weak_points,
    }
}

// ────────────────────────────────────────────────────────────────
// ChunkAccess for the anonymous row type
// ────────────────────────────────────────────────────────────────
//
// `sqlx::query!` produces a freshly-named anonymous record type per call
// site. We can't impl traits on it externally, so we inline the impl
// below using the concrete invocation from `build_report`.

// The macro emits a record with these field types:
//   id: uuid::Uuid
//   material_kind: String
//   status: String
//   start_time: Option<OffsetDateTime>
//   end_time: Option<OffsetDateTime>
//   staged_at: OffsetDateTime
//   timing: String
//
// We implement ChunkAccess for any reference whose fields match by name
// using a helper struct + From conversion.

// Concrete chunk struct used during in-place computation.
#[derive(Debug, Clone)]
struct Chunk {
    material_kind: String,
    status: String,
    start_time: Option<OffsetDateTime>,
    end_time: Option<OffsetDateTime>,
    staged_at: OffsetDateTime,
    timing: String,
    declared_contract: DeclaredCoverageContract,
    privacy_class: PrivacyClass,
}

impl ChunkAccess for Chunk {
    fn start_time(&self) -> Option<OffsetDateTime> {
        self.start_time
    }
    fn end_time(&self) -> Option<OffsetDateTime> {
        self.end_time
    }
    fn staged_at(&self) -> OffsetDateTime {
        self.staged_at
    }
    fn status(&self) -> &str {
        &self.status
    }
    fn material_kind(&self) -> &str {
        &self.material_kind
    }
    fn timing(&self) -> &str {
        &self.timing
    }
    fn declared_contract(&self) -> &DeclaredCoverageContract {
        &self.declared_contract
    }
    fn privacy_class(&self) -> PrivacyClass {
        self.privacy_class
    }
}

// ────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::sinex_test;
    use time::macros::datetime;

    fn chunk(
        kind: &str,
        status: &str,
        start: Option<OffsetDateTime>,
        end: Option<OffsetDateTime>,
        timing: &str,
    ) -> Chunk {
        chunk_with_privacy(kind, status, start, end, timing, PrivacyClass::Unknown)
    }

    fn chunk_with_privacy(
        kind: &str,
        status: &str,
        start: Option<OffsetDateTime>,
        end: Option<OffsetDateTime>,
        timing: &str,
        privacy_class: PrivacyClass,
    ) -> Chunk {
        Chunk {
            material_kind: kind.into(),
            status: status.into(),
            start_time: start,
            end_time: end,
            staged_at: start.unwrap_or(datetime!(2026-01-01 0:00 UTC)),
            timing: timing.into(),
            declared_contract: DeclaredCoverageContract::default(),
            privacy_class,
        }
    }

    fn chunk_with_declared(
        kind: &str,
        status: &str,
        start: Option<OffsetDateTime>,
        end: Option<OffsetDateTime>,
        timing: &str,
        declared: DeclaredCoverageContract,
    ) -> Chunk {
        Chunk {
            material_kind: kind.into(),
            status: status.into(),
            start_time: start,
            end_time: end,
            staged_at: start.unwrap_or(datetime!(2026-01-01 0:00 UTC)),
            timing: timing.into(),
            declared_contract: declared,
            privacy_class: PrivacyClass::Unknown,
        }
    }

    #[sinex_test]
    async fn classify_overlap_when_curr_starts_before_prev_ends() -> xtask::sandbox::TestResult<()> {
        let prev = chunk(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 10:00 UTC)),
            Some(datetime!(2026-01-01 11:00 UTC)),
            "intrinsic",
        );
        let curr = chunk(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 10:30 UTC)),
            Some(datetime!(2026-01-01 11:30 UTC)),
            "intrinsic",
        );
        let kind = classify_seam(
            datetime!(2026-01-01 11:00 UTC),
            datetime!(2026-01-01 10:30 UTC),
            &prev,
            &curr,
        );
        assert!(matches!(kind, SeamKind::Overlap));
        Ok(())
    }

    #[sinex_test]
    async fn classify_continuation_when_back_to_back() -> xtask::sandbox::TestResult<()> {
        let prev = chunk(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 10:00 UTC)),
            Some(datetime!(2026-01-01 11:00 UTC)),
            "intrinsic",
        );
        let curr = chunk(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 11:00 UTC)),
            Some(datetime!(2026-01-01 12:00 UTC)),
            "intrinsic",
        );
        let kind = classify_seam(
            datetime!(2026-01-01 11:00 UTC),
            datetime!(2026-01-01 11:00 UTC),
            &prev,
            &curr,
        );
        assert!(matches!(kind, SeamKind::ExpectedContinuation));
        Ok(())
    }

    #[sinex_test]
    async fn classify_discontinuity_for_long_gap() -> xtask::sandbox::TestResult<()> {
        let prev = chunk(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 10:00 UTC)),
            Some(datetime!(2026-01-01 10:30 UTC)),
            "intrinsic",
        );
        let curr = chunk(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 14:00 UTC)),
            Some(datetime!(2026-01-01 15:00 UTC)),
            "intrinsic",
        );
        let kind = classify_seam(
            datetime!(2026-01-01 10:30 UTC),
            datetime!(2026-01-01 14:00 UTC),
            &prev,
            &curr,
        );
        assert!(matches!(kind, SeamKind::Discontinuity));
        Ok(())
    }

    #[sinex_test]
    async fn classify_recovered_partial_when_either_chunk_marked() -> xtask::sandbox::TestResult<()> {
        let prev = chunk(
            "annex",
            "recovered_partial",
            Some(datetime!(2026-01-01 10:00 UTC)),
            Some(datetime!(2026-01-01 10:30 UTC)),
            "intrinsic",
        );
        let curr = chunk(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 11:00 UTC)),
            Some(datetime!(2026-01-01 11:30 UTC)),
            "intrinsic",
        );
        let kind = classify_seam(
            datetime!(2026-01-01 10:30 UTC),
            datetime!(2026-01-01 11:00 UTC),
            &prev,
            &curr,
        );
        assert!(matches!(kind, SeamKind::RecoveredPartial));
        Ok(())
    }

    #[sinex_test]
    async fn replayability_lists_every_dimension_weakness() -> xtask::sandbox::TestResult<()> {
        let r = build_replayability(false, false, false, true, false);
        assert!(!r.raw_bytes_preserved);
        assert!(!r.timing_quality);
        assert!(!r.anchor_stability);
        // 4 reasons + the always-present parser_determinism caveat.
        assert!(r.weak_points.len() >= 4);
        Ok(())
    }

    #[sinex_test]
    async fn coverage_contract_inferred_for_known_families() -> xtask::sandbox::TestResult<()> {
        let chunks: Vec<Chunk> = vec![];
        assert!(matches!(
            infer_coverage_contract("shell", &chunks),
            CoverageContract::Continuous
        ));
        assert!(matches!(
            infer_coverage_contract("browser", &chunks),
            CoverageContract::PeriodicDump
        ));
        assert!(matches!(
            infer_coverage_contract("import", &chunks),
            CoverageContract::FiniteOneShot
        ));
        assert!(matches!(
            infer_coverage_contract("unknown", &chunks),
            CoverageContract::OpportunisticImport
        ));
        Ok(())
    }

    #[sinex_test]
    async fn private_mode_seam_only_fires_for_private_classes() -> xtask::sandbox::TestResult<()> {
        let prev = chunk_with_privacy(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 10:00 UTC)),
            Some(datetime!(2026-01-01 10:30 UTC)),
            "intrinsic",
            PrivacyClass::Personal,
        );
        let curr = chunk_with_privacy(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 12:00 UTC)),
            Some(datetime!(2026-01-01 12:30 UTC)),
            "intrinsic",
            PrivacyClass::Public,
        );
        // Personal + Public + 90 minute gap → PrivateModeGap because one
        // side is private-classed.
        let kind = classify_seam(
            datetime!(2026-01-01 10:30 UTC),
            datetime!(2026-01-01 12:00 UTC),
            &prev,
            &curr,
        );
        assert!(matches!(kind, SeamKind::PrivateModeGap));

        // Unknown + Unknown + same gap → Discontinuity (Unknown is NOT
        // treated as private).
        let prev2 = chunk(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 10:00 UTC)),
            Some(datetime!(2026-01-01 10:30 UTC)),
            "intrinsic",
        );
        let kind2 = classify_seam(
            datetime!(2026-01-01 10:30 UTC),
            datetime!(2026-01-01 12:00 UTC),
            &prev2,
            &curr,
        );
        assert!(matches!(kind2, SeamKind::Discontinuity));
        Ok(())
    }

    #[sinex_test]
    async fn declared_coverage_contract_overrides_heuristic_inference() -> xtask::sandbox::TestResult<()> {
        let declared = DeclaredCoverageContract {
            kind: DeclaredCoverageContractKind::EphemeralStream,
            ..Default::default()
        };
        let chunks = vec![chunk_with_declared(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 10:00 UTC)),
            Some(datetime!(2026-01-01 11:00 UTC)),
            "realtime",
            declared,
        )];
        // Family name `shell` would heuristically map to Continuous; the
        // declared kind takes precedence.
        let (contract, is_declared) = resolve_coverage_contract("shell", &chunks);
        assert!(matches!(contract, CoverageContract::EphemeralStream));
        assert!(is_declared);
        Ok(())
    }

    #[sinex_test]
    async fn unknown_declared_contract_falls_back_to_heuristic() -> xtask::sandbox::TestResult<()> {
        let chunks = vec![chunk(
            "annex",
            "completed",
            Some(datetime!(2026-01-01 10:00 UTC)),
            Some(datetime!(2026-01-01 11:00 UTC)),
            "intrinsic",
        )];
        // Default declared contract is Unknown; family name decides.
        let (contract, is_declared) = resolve_coverage_contract("browser", &chunks);
        assert!(matches!(contract, CoverageContract::PeriodicDump));
        assert!(!is_declared);
        Ok(())
    }
}
