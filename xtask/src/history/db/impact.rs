use color_eyre::eyre::{Result, WrapErr};
use rusqlite::params;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use super::{HistoryDb, with_sqlite_lock_retry};

#[derive(Debug, Deserialize)]
struct TestDependencyEdgeArtifact {
    test_name: String,
    package: Option<String>,
    edge_kind: String,
    subject: String,
    fingerprint: Option<String>,
    origin: String,
}

#[derive(Debug, Deserialize)]
struct TestExecutionManifestArtifact {
    test_name: String,
    package: Option<String>,
    module_path: String,
    source_file: String,
    source_line: u32,
    binary_id: Option<String>,
    pid: u32,
    attempt_id: String,
    planner_version: String,
}

#[derive(Debug, Deserialize)]
struct TestCoverageRegionArtifact {
    test_name: String,
    package: Option<String>,
    file_path: String,
    function_name: Option<String>,
    line_start: Option<u32>,
    line_end: Option<u32>,
    region_hash: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "artifact_kind", rename_all = "snake_case")]
enum ImpactArtifactEnvelope {
    DependencyEdges {
        edges: Vec<TestDependencyEdgeArtifact>,
    },
    TestExecutionManifest {
        manifest: TestExecutionManifestArtifact,
    },
    CoverageRegions {
        regions: Vec<TestCoverageRegionArtifact>,
    },
}

impl HistoryDb {
    pub fn record_impact_plan(
        &self,
        invocation_id: Option<i64>,
        mode: &str,
        plan: &crate::impact::ImpactPlan,
    ) -> Result<i64> {
        let changed_json = serde_json::to_string(&plan.changed)?;
        let plan_json = serde_json::to_string(plan)?;
        let accepted_risk_json = serde_json::to_string(&plan.accepted_risks)?;
        with_sqlite_lock_retry("record impact plan", || {
            self.conn.execute(
                r"
                INSERT INTO impact_runs (
                    invocation_id,
                    mode,
                    changed_json,
                    plan_json,
                    accepted_risk_json
                )
                VALUES (?1, ?2, ?3, ?4, ?5)
                ",
                params![
                    invocation_id,
                    mode,
                    changed_json,
                    plan_json,
                    accepted_risk_json
                ],
            )?;
            let run_id = self.conn.last_insert_rowid();
            for decision in &plan.decisions {
                self.conn.execute(
                    r"
                    INSERT INTO impact_decisions (
                        impact_run_id,
                        action,
                        subject,
                        reason
                    )
                    VALUES (?1, ?2, ?3, ?4)
                    ",
                    params![
                        run_id,
                        format!("{:?}", decision.action),
                        decision.subject.as_deref(),
                        decision.reason.as_str()
                    ],
                )?;
            }
            Ok(run_id)
        })
    }

    pub fn record_impact_audit_run(
        &self,
        invocation_id: Option<i64>,
        impact_run_id: Option<i64>,
        sample_size: usize,
        sampled_json: &str,
        command_json: &str,
        status: &str,
        false_negative_count: usize,
        output_json: Option<&str>,
    ) -> Result<i64> {
        with_sqlite_lock_retry("record impact audit run", || {
            self.conn.execute(
                r"
                INSERT INTO impact_audit_runs (
                    invocation_id,
                    impact_run_id,
                    sample_size,
                    sampled_json,
                    command_json,
                    status,
                    false_negative_count,
                    output_json
                )
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                ",
                params![
                    invocation_id,
                    impact_run_id,
                    i64::try_from(sample_size).unwrap_or(i64::MAX),
                    sampled_json,
                    command_json,
                    status,
                    i64::try_from(false_negative_count).unwrap_or(i64::MAX),
                    output_json,
                ],
            )?;
            Ok(self.conn.last_insert_rowid())
        })
    }

    pub fn impacted_tests_for_changed_files(
        &self,
        changed_files: &[String],
    ) -> Result<Vec<crate::impact::ImpactedTest>> {
        self.impacted_tests_for_changed_files_and_hunks(changed_files, &[])
    }

    pub fn impacted_tests_for_changed_files_and_hunks(
        &self,
        changed_files: &[String],
        changed_hunks: &[crate::impact::FileChangedHunks],
    ) -> Result<Vec<crate::impact::ImpactedTest>> {
        if changed_files.is_empty()
            || !self.table_exists("coverage_regions")?
            || !self.table_exists("test_dependency_edges")?
        {
            return Ok(Vec::new());
        }

        let mut tests: BTreeMap<(Option<String>, String), Vec<crate::impact::ImpactEvidence>> =
            BTreeMap::new();
        for path in changed_files {
            let hunks = changed_hunks
                .iter()
                .find(|hunks| hunks.path == *path)
                .map_or(&[][..], |hunks| hunks.hunks.as_slice());
            self.collect_coverage_impacts(path, hunks, &mut tests)?;
            self.collect_dependency_edge_impacts(path, &mut tests)?;
            self.collect_manifest_impacts(path, hunks, &mut tests)?;
        }

        Ok(tests
            .into_iter()
            .map(
                |((package, test_name), evidence)| crate::impact::ImpactedTest {
                    package,
                    test_name,
                    evidence,
                },
            )
            .collect())
    }

    fn collect_coverage_impacts(
        &self,
        path: &str,
        hunks: &[crate::impact::ChangedHunk],
        tests: &mut BTreeMap<(Option<String>, String), Vec<crate::impact::ImpactEvidence>>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT DISTINCT
                test_name,
                package,
                COALESCE(function_name, ''),
                COALESCE(line_start, -1),
                COALESCE(line_end, -1)
            FROM coverage_regions
            WHERE file_path = ?1 OR file_path = ?2
            ORDER BY package, test_name
            ",
        )?;
        let dotted = format!("./{path}");
        let rows = stmt.query_map(params![path, dotted], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })?;
        for row in rows {
            let (test_name, package, function_name, line_start, line_end) = row?;
            let line_start_u32 = u32::try_from(line_start).ok();
            let line_end_u32 = u32::try_from(line_end).ok();
            if !hunks.is_empty() {
                let Some((region_start, region_end)) = line_start_u32.zip(line_end_u32) else {
                    continue;
                };
                if !hunks.iter().any(|hunk| {
                    crate::impact::ranges_overlap(
                        hunk.line_start,
                        hunk.line_end,
                        region_start,
                        region_end,
                    )
                }) {
                    continue;
                }
            }
            let reason = if function_name.is_empty() {
                "LLVM coverage touched this file".to_string()
            } else if line_start >= 0 && line_end >= 0 {
                format!("LLVM coverage touched {function_name}:{line_start}-{line_end}")
            } else {
                format!("LLVM coverage touched {function_name}")
            };
            tests
                .entry((package, test_name))
                .or_default()
                .push(crate::impact::ImpactEvidence {
                    source: crate::impact::ImpactEvidenceSource::CoverageRegion,
                    subject: path.to_string(),
                    reason,
                    line_start: line_start_u32,
                    line_end: line_end_u32,
                });
        }
        Ok(())
    }

    fn collect_dependency_edge_impacts(
        &self,
        path: &str,
        tests: &mut BTreeMap<(Option<String>, String), Vec<crate::impact::ImpactEvidence>>,
    ) -> Result<()> {
        let mut stmt = self.conn.prepare(
            r"
            SELECT DISTINCT test_name, package, edge_kind, origin
            FROM test_dependency_edges
            WHERE subject = ?1
              AND edge_kind IN ('file', 'rust_item', 'rust_module', 'runtime_file')
            ORDER BY package, test_name
            ",
        )?;
        let rows = stmt.query_map(params![path], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            let (test_name, package, edge_kind, origin) = row?;
            tests
                .entry((package, test_name))
                .or_default()
                .push(crate::impact::ImpactEvidence {
                    source: crate::impact::ImpactEvidenceSource::DependencyEdge,
                    subject: path.to_string(),
                    reason: format!("test declared {edge_kind} dependency from {origin}"),
                    line_start: None,
                    line_end: None,
                });
        }
        Ok(())
    }

    fn collect_manifest_impacts(
        &self,
        path: &str,
        hunks: &[crate::impact::ChangedHunk],
        tests: &mut BTreeMap<(Option<String>, String), Vec<crate::impact::ImpactEvidence>>,
    ) -> Result<()> {
        if !self.table_exists("test_execution_manifests")? {
            return Ok(());
        }
        let mut stmt = self.conn.prepare(
            r"
            SELECT DISTINCT test_name, package, source_line, module_path
            FROM test_execution_manifests
            WHERE source_file = ?1 OR source_file = ?2
            ORDER BY package, test_name
            ",
        )?;
        let dotted = format!("./{path}");
        let rows = stmt.query_map(params![path, dotted], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;
        for row in rows {
            let (test_name, package, source_line, module_path) = row?;
            let Some(source_line) = u32::try_from(source_line).ok() else {
                continue;
            };
            if !hunks.is_empty()
                && !hunks.iter().any(|hunk| {
                    crate::impact::ranges_overlap(
                        hunk.line_start,
                        hunk.line_end,
                        source_line,
                        source_line,
                    )
                })
            {
                continue;
            }
            tests
                .entry((package, test_name))
                .or_default()
                .push(crate::impact::ImpactEvidence {
                    source: crate::impact::ImpactEvidenceSource::TestExecutionManifest,
                    subject: path.to_string(),
                    reason: format!(
                        "test entrypoint manifest recorded {module_path}:{source_line}"
                    ),
                    line_start: Some(source_line),
                    line_end: Some(source_line),
                });
        }
        Ok(())
    }

    pub fn import_test_dependency_artifacts(
        &self,
        invocation_id: i64,
        artifact_dir: &Path,
    ) -> Result<usize> {
        if !artifact_dir.exists() {
            return Ok(0);
        }
        let mut imported = 0;
        with_sqlite_lock_retry("import test dependency artifacts", || {
            for entry in fs::read_dir(artifact_dir).wrap_err_with(|| {
                format!(
                    "failed to read impact artifact directory {}",
                    artifact_dir.display()
                )
            })? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(std::ffi::OsStr::to_str) != Some("json") {
                    continue;
                }
                let rendered = fs::read_to_string(&path).wrap_err_with(|| {
                    format!("failed to read impact artifact {}", path.display())
                })?;
                if let Ok(envelope) = serde_json::from_str::<ImpactArtifactEnvelope>(&rendered) {
                    imported += self.import_impact_artifact_envelope(invocation_id, envelope)?;
                } else {
                    let edges: Vec<TestDependencyEdgeArtifact> = serde_json::from_str(&rendered)
                        .wrap_err_with(|| {
                            format!("failed to parse impact artifact {}", path.display())
                        })?;
                    for edge in edges {
                        imported += self.insert_test_dependency_edge(invocation_id, &edge)?;
                    }
                }
            }
            Ok(imported)
        })
    }

    fn import_impact_artifact_envelope(
        &self,
        invocation_id: i64,
        envelope: ImpactArtifactEnvelope,
    ) -> Result<usize> {
        match envelope {
            ImpactArtifactEnvelope::DependencyEdges { edges } => {
                let mut imported = 0;
                for edge in edges {
                    imported += self.insert_test_dependency_edge(invocation_id, &edge)?;
                }
                Ok(imported)
            }
            ImpactArtifactEnvelope::TestExecutionManifest { manifest } => {
                self.conn.execute(
                    r"
                    INSERT INTO test_execution_manifests (
                        invocation_id,
                        test_name,
                        package,
                        module_path,
                        source_file,
                        source_line,
                        binary_id,
                        pid,
                        attempt_id,
                        planner_version
                    )
                    VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
                    ON CONFLICT(invocation_id, test_name, module_path, source_file, source_line)
                    DO UPDATE SET
                        package = excluded.package,
                        binary_id = excluded.binary_id,
                        pid = excluded.pid,
                        attempt_id = excluded.attempt_id,
                        planner_version = excluded.planner_version
                    ",
                    params![
                        invocation_id,
                        manifest.test_name,
                        manifest.package,
                        manifest.module_path,
                        manifest.source_file,
                        i64::from(manifest.source_line),
                        manifest.binary_id,
                        i64::from(manifest.pid),
                        manifest.attempt_id,
                        manifest.planner_version,
                    ],
                )?;
                Ok(1)
            }
            ImpactArtifactEnvelope::CoverageRegions { regions } => {
                let mut imported = 0;
                for region in regions {
                    self.conn.execute(
                        r"
                        INSERT OR REPLACE INTO coverage_regions (
                            invocation_id,
                            test_name,
                            package,
                            file_path,
                            function_name,
                            line_start,
                            line_end,
                            region_hash
                        )
                        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                        ",
                        params![
                            invocation_id,
                            region.test_name,
                            region.package,
                            region.file_path,
                            region.function_name,
                            region.line_start.map(i64::from),
                            region.line_end.map(i64::from),
                            region.region_hash,
                        ],
                    )?;
                    imported += 1;
                }
                Ok(imported)
            }
        }
    }

    fn insert_test_dependency_edge(
        &self,
        invocation_id: i64,
        edge: &TestDependencyEdgeArtifact,
    ) -> Result<usize> {
        let changed = self.conn.execute(
            r"
            INSERT INTO test_dependency_edges (
                invocation_id,
                test_name,
                package,
                edge_kind,
                subject,
                fingerprint,
                origin
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(invocation_id, test_name, edge_kind, subject, origin)
            DO UPDATE SET
                package = excluded.package,
                fingerprint = excluded.fingerprint
            ",
            params![
                invocation_id,
                edge.test_name,
                edge.package,
                edge.edge_kind,
                edge.subject,
                edge.fingerprint,
                edge.origin
            ],
        )?;
        Ok(changed)
    }
}
