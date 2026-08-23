use std::sync::LazyLock;

#[derive(Debug, Clone)]
pub(super) struct GitSnapshot {
    pub(super) commit: Option<String>,
    pub(super) dirty: bool,
}

static CURRENT_PROCESS_GIT_SNAPSHOT: LazyLock<GitSnapshot> = LazyLock::new(|| GitSnapshot {
    commit: get_git_commit_uncached(),
    dirty: is_git_dirty_uncached(),
});

/// Get current git commit hash and dirty state for this xtask process.
pub(super) fn current_git_snapshot() -> &'static GitSnapshot {
    &CURRENT_PROCESS_GIT_SNAPSHOT
}

fn get_git_commit_uncached() -> Option<String> {
    std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
}

fn is_git_dirty_uncached() -> bool {
    std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()
        .is_some_and(|o| !o.stdout.is_empty())
}
