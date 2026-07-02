use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

/// Expanded git state for rich MOTD.
pub(super) struct GitState {
    pub(super) branch: Option<String>,
    pub(super) dirty: bool,
    pub(super) ahead: u32,
    pub(super) behind: u32,
    pub(super) probe_message: Option<String>,
    pub(super) last_commit_hash: Option<String>,
    pub(super) last_commit_message: Option<String>,
    pub(super) last_commit_age_mins: Option<i64>,
    pub(super) stash_count: Option<usize>,
    pub(super) files_changed: Option<String>,
    pub(super) uncommitted_count: Option<usize>,
}

fn summarize_git_probe_output(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if !stderr.is_empty() {
        return stderr;
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !stdout.is_empty() {
        return stdout;
    }

    format!("exit status {}", output.status)
}

fn record_git_probe_issue(
    probe_issues: &mut Vec<String>,
    args: &[&str],
    detail: impl Into<String>,
) {
    probe_issues.push(format!("git {} failed: {}", args.join(" "), detail.into()));
}

fn run_git_output(
    cwd: &Path,
    probe_issues: &mut Vec<String>,
    args: &[&str],
) -> Option<std::process::Output> {
    match std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
    {
        Ok(output) if output.status.success() => Some(output),
        Ok(output) => {
            record_git_probe_issue(probe_issues, args, summarize_git_probe_output(&output));
            None
        }
        Err(error) => {
            record_git_probe_issue(probe_issues, args, error.to_string());
            None
        }
    }
}

pub(super) fn probe_git_state(cwd: &Path) -> GitState {
    let mut probe_issues = Vec::new();

    let (branch, dirty, uncommitted_count, ahead, behind) = run_git_output(
        cwd,
        &mut probe_issues,
        &["status", "--porcelain=v2", "--branch"],
    )
    .map_or((None, false, None, 0, 0), |output| {
        parse_git_status_branch_porcelain(
            &String::from_utf8_lossy(&output.stdout),
            &mut probe_issues,
        )
    });

    let commit = run_git_output(
        cwd,
        &mut probe_issues,
        &["log", "-1", "--format=%h\t%s\t%ct"],
    )
    .and_then(|output| {
        let text = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let parts: Vec<&str> = text.splitn(3, '\t').collect();
        if parts.len() == 3 {
            Some((
                parts[0].to_string(),
                parts[1].to_string(),
                parts[2].to_string(),
            ))
        } else {
            record_git_probe_issue(
                &mut probe_issues,
                &["log", "-1", "--format=%h\t%s\t%cr"],
                format!("unexpected output: {text}"),
            );
            None
        }
    });

    let stash_count = run_git_output(cwd, &mut probe_issues, &["stash", "list"]).map(|output| {
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|line| !line.is_empty())
            .count()
    });

    let files_changed = run_git_output(cwd, &mut probe_issues, &["diff", "--shortstat", "HEAD"])
        .and_then(|output| {
            let shortstat = String::from_utf8_lossy(&output.stdout).trim().to_string();
            (!shortstat.is_empty()).then_some(shortstat)
        });

    let now_unix_ts = current_unix_timestamp_secs();
    let last_age = commit.as_ref().and_then(|(_, _, commit_unix_ts)| {
        if let Some(now_unix_ts) = now_unix_ts {
            parse_git_commit_age_mins(commit_unix_ts, now_unix_ts).or_else(|| {
                record_git_probe_issue(
                    &mut probe_issues,
                    &["log", "-1", "--format=%h\t%s\t%ct"],
                    format!("unexpected commit timestamp: {commit_unix_ts}"),
                );
                None
            })
        } else {
            record_git_probe_issue(
                &mut probe_issues,
                &["log", "-1", "--format=%h\t%s\t%ct"],
                "system clock is before the Unix epoch".to_string(),
            );
            None
        }
    });
    let last_hash = commit.as_ref().map(|(hash, _, _)| hash.clone());
    let last_msg = commit.as_ref().map(|(_, message, _)| message.clone());

    GitState {
        branch,
        dirty,
        ahead,
        behind,
        probe_message: (!probe_issues.is_empty()).then(|| probe_issues.join("; ")),
        last_commit_hash: last_hash,
        last_commit_message: last_msg,
        last_commit_age_mins: last_age,
        stash_count,
        files_changed,
        uncommitted_count,
    }
}

fn current_unix_timestamp_secs() -> Option<i64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
}

/// Parse a git commit timestamp (`%ct`) into a relative age in minutes.
fn parse_git_commit_age_mins(commit_unix_ts: &str, now_unix_ts: i64) -> Option<i64> {
    let commit_unix_ts = commit_unix_ts.parse::<i64>().ok()?;
    Some((now_unix_ts - commit_unix_ts).max(0) / 60)
}

fn parse_git_status_branch_porcelain(
    output: &str,
    probe_issues: &mut Vec<String>,
) -> (Option<String>, bool, Option<usize>, u32, u32) {
    let mut branch = None;
    let mut ahead = 0u32;
    let mut behind = 0u32;
    let mut entry_count = 0usize;

    for line in output.lines() {
        if let Some(head) = line.strip_prefix("# branch.head ") {
            let head = head.trim();
            if !head.is_empty() && head != "(detached)" {
                branch = Some(head.to_string());
            }
            continue;
        }

        if let Some(ab) = line.strip_prefix("# branch.ab ") {
            let parts: Vec<&str> = ab.split_whitespace().collect();
            if parts.len() != 2 {
                record_git_probe_issue(
                    probe_issues,
                    &["status", "--porcelain=v2", "--branch"],
                    format!("unexpected branch.ab payload: {ab}"),
                );
                continue;
            }

            let parsed_ahead = parts[0]
                .strip_prefix('+')
                .and_then(|value| value.parse::<u32>().ok());
            let parsed_behind = parts[1]
                .strip_prefix('-')
                .and_then(|value| value.parse::<u32>().ok());

            match (parsed_ahead, parsed_behind) {
                (Some(parsed_ahead), Some(parsed_behind)) => {
                    ahead = parsed_ahead;
                    behind = parsed_behind;
                }
                _ => record_git_probe_issue(
                    probe_issues,
                    &["status", "--porcelain=v2", "--branch"],
                    format!("invalid branch.ab payload: {ab}"),
                ),
            }
            continue;
        }

        if line.starts_with('#') {
            continue;
        }

        if !line.trim().is_empty() {
            entry_count += 1;
        }
    }

    (branch, entry_count > 0, Some(entry_count), ahead, behind)
}

#[cfg(test)]
#[path = "git_test.rs"]
mod tests;
