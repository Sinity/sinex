//! Current-state next-action planner (#1144).
//!
//! Reads workspace state and produces ranked, evidence-backed recommendations
//! for what to run next. The planner is read-only — it never mutates state.
//!
//! ## Signal sources (in priority order)
//!
//! 1. **Active/queued jobs** — if work is already running, wait for it
//! 2. **Dirty files** — uncommitted changes should be checked
//! 3. **Generated surface drift** — docs/schema/snapshots may be stale
//! 4. **Resource pressure** — warn when CPU/memory are under load (#1145)
//! 5. **Idle** — no action needed, checkout is freshly proven

use color_eyre::eyre::Result;
use serde::Serialize;

/// One recommended action.
#[derive(Debug, Clone, Serialize)]
pub struct PlannedAction {
    /// Exact command to run (e.g. "xtask check -p sinex-db")
    pub command: String,
    /// Human-readable reason for this recommendation
    pub reason: String,
    /// Priority: "now", "soon", or "idle"
    pub priority: Priority,
    /// Confidence 0.0–1.0
    pub confidence: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Priority {
    Now,
    Soon,
    Idle,
}

/// Read current workspace state and produce ranked recommendations.
pub fn plan_next_actions() -> Result<Vec<PlannedAction>> {
    let mut actions = Vec::new();

    // ── Signal 1: active jobs ────────────────────────────────────────────
    if let Some(job_actions) = check_active_jobs()? {
        actions.extend(job_actions);
    }

    // ── Signal 2: dirty files ────────────────────────────────────────────
    if let Some(dirty_actions) = check_dirty_files()? {
        actions.extend(dirty_actions);
    }

    // ── Signal 3: generated surface drift ────────────────────────────────
    if let Some(drift_actions) = check_generated_drift()? {
        actions.extend(drift_actions);
    }

    // ── Signal 4: resource pressure (#1145) ───────────────────────────────
    if let Some(pressure_actions) = check_resource_pressure() {
        actions.extend(pressure_actions);
    }

    // ── Signal 5: idle — nothing to do ───────────────────────────────────
    if actions.is_empty() {
        actions.push(PlannedAction {
            command: "xtask check".to_string(),
            reason: "no signals detected — baseline verification".to_string(),
            priority: Priority::Idle,
            confidence: 0.5,
        });
    }

    // Sort: Now first, then Soon, then Idle
    actions.sort_by_key(|a| a.priority);
    Ok(actions)
}

// ── Signal probes ──────────────────────────────────────────────────────────

fn check_active_jobs() -> Result<Option<Vec<PlannedAction>>> {
    let coordinator_dir = crate::config::config().state_dir.join("coordinator");
    let Ok(entries) = std::fs::read_dir(&coordinator_dir) else {
        return Ok(None);
    };

    let mut actions = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            if let Ok(content) = std::fs::read_to_string(&path) {
                if let Ok(state) =
                    serde_json::from_str::<crate::coordinator::CoordinationState>(&content)
                {
                    if state.job_id > 0 && state.pid > 0 {
                        actions.push(PlannedAction {
                            command: format!("xtask jobs status {}", state.job_id),
                            reason: format!(
                                "active job {} ({}) is running (pid {})",
                                state.job_id, state.command, state.pid
                            ),
                            priority: Priority::Now,
                            confidence: 0.9,
                        });
                    }
                }
            }
        }
    }

    if actions.is_empty() {
        Ok(None)
    } else {
        Ok(Some(actions))
    }
}

fn check_dirty_files() -> Result<Option<Vec<PlannedAction>>> {
    let output = std::process::Command::new("git")
        .args(["diff", "--name-only"])
        .output()?;

    if !output.status.success() {
        return Ok(None);
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let dirty: Vec<&str> = stdout.lines().filter(|line| !line.is_empty()).collect();

    // Also check untracked files (excluding .sinex/).
    let untracked_output = std::process::Command::new("git")
        .args([
            "ls-files",
            "--others",
            "--exclude-standard",
            "--",
            ":!.sinex",
        ])
        .output()?;

    let untracked: Vec<String> = if untracked_output.status.success() {
        let ustdout = String::from_utf8_lossy(&untracked_output.stdout);
        ustdout
            .lines()
            .filter(|line| !line.is_empty())
            .map(String::from)
            .collect()
    } else {
        Vec::new()
    };

    let total_changes = dirty.len() + untracked.len();
    if total_changes == 0 {
        return Ok(None);
    }

    let packages = affected_packages(&dirty);
    let package_list = if packages.is_empty() {
        String::new()
    } else {
        format!(" -p {}", packages.join(" -p "))
    };

    let mut reason = format!("{total_changes} uncommitted change(s)");
    if !packages.is_empty() {
        reason.push_str(&format!(", affecting: {}", packages.join(", ")));
    }
    if !untracked.is_empty() {
        reason.push_str(&format!(" (+{} untracked)", untracked.len()));
    }

    Ok(Some(vec![PlannedAction {
        command: format!("xtask check{}", package_list),
        reason,
        priority: Priority::Now,
        confidence: 0.95,
    }]))
}

fn check_generated_drift() -> Result<Option<Vec<PlannedAction>>> {
    let output = std::process::Command::new("git")
        .args([
            "diff",
            "--name-only",
            "--",
            "docs/command-guide.md",
            "docs/command-reference.md",
            "docs/source-units.json",
            "docs/proof-catalog.json",
        ])
        .output()?;

    if !output.status.success() {
        return Ok(None);
    }

    let changed = !String::from_utf8_lossy(&output.stdout).trim().is_empty();
    if changed {
        return Ok(Some(vec![PlannedAction {
            command: "xtask docs sync".to_string(),
            reason: "generated docs/schema surfaces are stale".to_string(),
            priority: Priority::Soon,
            confidence: 0.85,
        }]));
    }

    Ok(None)
}

fn check_resource_pressure() -> Option<Vec<PlannedAction>> {
    // Read CPU pressure stall information (Linux only).
    // /proc/pressure/cpu has lines like: some avg10=5.23 avg60=2.10 avg300=1.05
    let cpu_pressure = std::fs::read_to_string("/proc/pressure/cpu").ok()?;
    let cpu_10s = cpu_pressure
        .lines()
        .find(|l| l.starts_with("some"))
        .and_then(|l| {
            l.split_whitespace()
                .find(|w| w.starts_with("avg10="))
                .and_then(|w| w.strip_prefix("avg10=")?.parse::<f64>().ok())
        })?;

    // Load average
    let loadavg = std::fs::read_to_string("/proc/loadavg").ok()?;
    let load_1m: f64 = loadavg.split_whitespace().next()?.parse().ok()?;
    let ncpus = num_cpus();

    if cpu_10s > 30.0 || (load_1m / ncpus as f64) > 4.0 {
        return Some(vec![PlannedAction {
            command: "xtask status".to_string(),
            reason: format!(
                "system under pressure: CPU pressure {cpu_10s:.1}% (10s), load {load_1m:.1} / {ncpus} CPUs"
            ),
            priority: Priority::Soon,
            confidence: 0.7,
        }]);
    }

    None
}

fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn affected_packages(dirty: &[&str]) -> Vec<String> {
    let mut packages = std::collections::BTreeSet::new();

    for path in dirty {
        let pkg = if path.starts_with("crate/lib/sinex-primitives") {
            "sinex-primitives"
        } else if path.starts_with("crate/lib/sinex-db") {
            "sinex-db"
        } else if path.starts_with("crate/lib/sinex-schema") {
            "sinex-schema"
        } else if path.starts_with("crate/lib/sinex-macros") {
            "sinex-macros"
        } else if path.starts_with("crate/lib/sinex-node-sdk") {
            "sinex-node-sdk"
        } else if path.starts_with("crate/core/sinex-gateway") {
            "sinex-gateway"
        } else if path.starts_with("crate/core/sinex-ingestd") {
            "sinex-ingestd"
        } else if path.starts_with("crate/core/sinex-source-worker") {
            "sinex-source-worker"
        } else if path.starts_with("crate/nodes/sinex-fs-ingestor") {
            "sinex-fs-ingestor"
        } else if path.starts_with("crate/nodes/sinex-terminal-ingestor") {
            "sinex-terminal-ingestor"
        } else if path.starts_with("crate/nodes/sinex-desktop-ingestor") {
            "sinex-desktop-ingestor"
        } else if path.starts_with("crate/nodes/sinex-system-ingestor") {
            "sinex-system-ingestor"
        } else if path.starts_with("crate/nodes/sinex-document-ingestor") {
            "sinex-document-ingestor"
        } else if path.starts_with("crate/nodes/sinex-browser-ingestor") {
            "sinex-browser-ingestor"
        } else if path.starts_with("crate/core/sinex-process") {
            "sinex-process"
        } else if path.starts_with("crate/cli") {
            "sinexctl"
        } else if path.starts_with("xtask/") {
            "xtask"
        } else if path.starts_with("tests/") {
            "sinex-e2e-tests"
        } else if path.starts_with("nixos/") {
            continue;
        } else {
            return vec!["--workspace".to_string()];
        };

        packages.insert(pkg.to_string());
    }

    packages.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_affected_packages_maps_crate_paths() {
        let dirty = &[
            "crate/lib/sinex-db/src/repositories/events.rs",
            "crate/lib/sinex-primitives/src/lib.rs",
        ];
        let pkgs = affected_packages(dirty);
        assert!(pkgs.contains(&"sinex-db".to_string()));
        assert!(pkgs.contains(&"sinex-primitives".to_string()));
        assert_eq!(pkgs.len(), 2);
    }

    #[test]
    fn test_affected_packages_unknown_path_returns_workspace() {
        let pkgs = affected_packages(&["src/main.rs"]);
        assert_eq!(pkgs, vec!["--workspace"]);
    }

    #[test]
    fn test_affected_packages_skips_nixos() {
        let pkgs = affected_packages(&["nixos/modules/services/sinex.nix"]);
        assert!(pkgs.is_empty());
    }

    #[test]
    fn test_plan_next_actions_produces_result() {
        let result = plan_next_actions();
        assert!(result.is_ok());
    }

    #[test]
    fn test_planned_action_serialization() {
        let action = PlannedAction {
            command: "xtask check -p sinex-db".to_string(),
            reason: "test reason".to_string(),
            priority: Priority::Now,
            confidence: 0.95,
        };
        let json = serde_json::to_string(&action).unwrap();
        assert!(json.contains("xtask check"));
        assert!(json.contains("now"));
    }
}
