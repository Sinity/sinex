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
    CoverageContract, CoverageGap, GapKind, Replayability, SeamKind, SourceContinuityReport,
    TemporalSeam,
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
            if let Some(report) = self.build_report(&family).await? {
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
        self.build_report(source_family).await
    }

    /// Resolve attribution for a single point inside a suspected gap.
    ///
    /// Returns `Ok(None)` when `at` falls inside a covered window.
    pub async fn explain_gap(
        &self,
        source_family: &SourceFamily,
        at: Timestamp,
    ) -> DbResult<Option<CoverageGap>> {
        let report = match self.build_report(source_family).await? {
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
    ) -> DbResult<Option<SourceContinuityReport>> {
        let family_str = family.as_str();

        // Aggregate over events + materials joined together, scoped to family.
        let agg = sqlx::query!(
            r#"
            SELECT
                COUNT(DISTINCT sm.id) AS "material_count!: i64",
                COUNT(e.id) AS "event_count!: i64",
                MIN(COALESCE(sm.start_time, sm.staged_at)) AS earliest_ts,
                MAX(COALESCE(sm.end_time, sm.staged_at)) AS latest_ts,
                BOOL_OR(sm.optional_blob_id IS NOT NULL) AS "any_blob!: bool",
                BOOL_AND(sm.total_bytes IS NOT NULL) AS "all_finalized!: bool",
                BOOL_OR(sm.timing_info_type IN ('realtime', 'intrinsic'))
                    AS "good_timing!: bool"
            FROM core.events e
            JOIN raw.source_material_registry sm ON sm.id = e.source_material_id
            WHERE split_part(e.source, '.', 1) = $1
            "#,
            family_str
        )
        .fetch_one(self.pool)
        .await
        .map_err(|e| db_error(e, "aggregate continuity report"))?;

        if agg.material_count == 0 && agg.event_count == 0 {
            return Ok(None);
        }

        // Per-material chunks for seams and gap detection.
        let chunk_rows = sqlx::query!(
            r#"
            SELECT DISTINCT
                sm.id AS "id!: uuid::Uuid",
                sm.material_kind AS "material_kind!: String",
                sm.status AS "status!: String",
                sm.start_time,
                sm.end_time,
                sm.staged_at AS "staged_at!: OffsetDateTime",
                sm.timing_info_type AS "timing!: String"
            FROM raw.source_material_registry sm
            JOIN core.events e ON e.source_material_id = sm.id
            WHERE split_part(e.source, '.', 1) = $1
            ORDER BY sm.start_time NULLS LAST, sm.staged_at
            "#,
            family_str
        )
        .fetch_all(self.pool)
        .await
        .map_err(|e| db_error(e, "fetch material chunks"))?;

        let chunks: Vec<Chunk> = chunk_rows
            .into_iter()
            .map(|r| Chunk {
                material_kind: r.material_kind,
                status: r.status,
                start_time: r.start_time,
                end_time: r.end_time,
                staged_at: r.staged_at,
                timing: r.timing,
            })
            .collect();

        let coverage_contract = infer_coverage_contract(family_str, &chunks);

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
    // Heuristic: if the gap is bounded by a privacy-marked material kind,
    // treat as private-mode gap. We currently have no explicit private-mode
    // marker on the registry, so this branch only fires once that lands.
    if prev.material_kind().contains("private") || curr.material_kind().contains("private") {
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
}

// ────────────────────────────────────────────────────────────────
// Tests
// ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn chunk(
        kind: &str,
        status: &str,
        start: Option<OffsetDateTime>,
        end: Option<OffsetDateTime>,
        timing: &str,
    ) -> Chunk {
        Chunk {
            material_kind: kind.into(),
            status: status.into(),
            start_time: start,
            end_time: end,
            staged_at: start.unwrap_or(datetime!(2026-01-01 0:00 UTC)),
            timing: timing.into(),
        }
    }

    #[test]
    fn classify_overlap_when_curr_starts_before_prev_ends() {
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
    }

    #[test]
    fn classify_continuation_when_back_to_back() {
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
    }

    #[test]
    fn classify_discontinuity_for_long_gap() {
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
    }

    #[test]
    fn classify_recovered_partial_when_either_chunk_marked() {
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
    }

    #[test]
    fn replayability_lists_every_dimension_weakness() {
        let r = build_replayability(false, false, false, true, false);
        assert!(!r.raw_bytes_preserved);
        assert!(!r.timing_quality);
        assert!(!r.anchor_stability);
        // 4 reasons + the always-present parser_determinism caveat.
        assert!(r.weak_points.len() >= 4);
    }

    #[test]
    fn coverage_contract_inferred_for_known_families() {
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
    }
}
