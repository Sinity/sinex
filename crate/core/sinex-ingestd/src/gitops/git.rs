//! Git operations for cloning, fetching, and inspecting repositories.
//!
//! All `git2` calls are wrapped in `tokio::task::spawn_blocking` because
//! `git2` is a synchronous-only library.

use crate::{IngestdResult, SinexError};
use std::path::PathBuf;
use tracing::{debug, info};

/// Handles Git repository operations for the `GitOps` sync service.
pub struct GitOperations {
    work_dir: PathBuf,
}

impl GitOperations {
    /// Create a new `GitOperations` instance.
    ///
    /// `work_dir` is the parent directory where cloned repos are stored.
    /// Each repository gets a subdirectory derived from its URL.
    #[must_use]
    pub fn new(work_dir: PathBuf) -> Self {
        Self { work_dir }
    }

    /// Ensure a repository is cloned locally and return the path to the checkout.
    ///
    /// If the repo already exists on disk, it is opened. Otherwise, it is cloned.
    pub async fn ensure_repo(&self, url: &str, branch: &str) -> IngestdResult<PathBuf> {
        validate_url(url)?;

        let repo_dir = self.repo_dir(url);
        let url = url.to_string();
        let branch = branch.to_string();

        tokio::task::spawn_blocking(move || {
            if repo_dir.join(".git").exists() || repo_dir.join("HEAD").exists() {
                debug!(path = %repo_dir.display(), "Opening existing repository");
                // Validate it can be opened
                git2::Repository::open(&repo_dir).map_err(|e| {
                    SinexError::service(format!(
                        "Failed to open existing repository at {}: {e}",
                        repo_dir.display()
                    ))
                    .with_operation("gitops.open_repo")
                })?;
                Ok(repo_dir)
            } else {
                info!(url = %url, branch = %branch, path = %repo_dir.display(), "Cloning repository");
                std::fs::create_dir_all(&repo_dir).map_err(|e| {
                    SinexError::io(format!(
                        "Failed to create repo directory {}: {e}",
                        repo_dir.display()
                    ))
                })?;

                let mut builder = git2::build::RepoBuilder::new();
                builder.branch(&branch);

                // Shallow clone for efficiency
                let mut fetch_opts = git2::FetchOptions::new();
                fetch_opts.depth(1);
                builder.fetch_options(fetch_opts);

                builder.clone(&url, &repo_dir).map_err(|e| {
                    SinexError::service(format!("Failed to clone {url}: {e}"))
                        .with_operation("gitops.clone_repo")
                        .with_context("url", url.clone())
                        .with_context("branch", branch.clone())
                })?;

                Ok(repo_dir)
            }
        })
        .await
        .map_err(|e| {
            SinexError::service(format!("Git task panicked: {e}"))
                .with_operation("gitops.ensure_repo")
        })?
    }

    /// Fetch the latest changes from origin and reset to the target branch tip.
    pub async fn fetch_and_checkout(&self, repo_path: PathBuf, branch: &str) -> IngestdResult<()> {
        let branch = branch.to_string();

        tokio::task::spawn_blocking(move || {
            let repo = git2::Repository::open(&repo_path).map_err(|e| {
                SinexError::service(format!(
                    "Failed to open repository at {}: {e}",
                    repo_path.display()
                ))
            })?;

            // Fetch from origin
            let mut remote = repo.find_remote("origin").map_err(|e| {
                SinexError::service("Failed to find remote 'origin'").with_source(e)
            })?;

            let mut fetch_opts = git2::FetchOptions::new();
            fetch_opts.depth(1);
            remote
                .fetch(&[&branch], Some(&mut fetch_opts), None)
                .map_err(|e| {
                    SinexError::service(format!("Failed to fetch branch '{branch}': {e}"))
                        .with_operation("gitops.fetch")
                })?;
            drop(remote);

            // Reset HEAD to origin/<branch>
            let refname = format!("refs/remotes/origin/{branch}");
            let reference = repo.find_reference(&refname).map_err(|e| {
                SinexError::service(format!("Failed to find reference {refname}: {e}"))
            })?;
            let commit = reference.peel_to_commit().map_err(|e| {
                SinexError::service(format!("Failed to peel reference to commit: {e}"))
            })?;
            repo.reset(commit.as_object(), git2::ResetType::Hard, None)
                .map_err(|e| {
                    SinexError::service(format!("Failed to reset to {refname}: {e}"))
                        .with_operation("gitops.checkout")
                })?;

            debug!(branch = %branch, commit = %commit.id(), "Checked out latest commit");
            Ok(())
        })
        .await
        .map_err(|e| {
            SinexError::service(format!("Git task panicked: {e}"))
                .with_operation("gitops.fetch_and_checkout")
        })?
    }

    /// Get the HEAD commit SHA for a repository.
    pub async fn get_head_commit_sha(repo_path: PathBuf) -> IngestdResult<String> {
        tokio::task::spawn_blocking(move || {
            let repo = git2::Repository::open(&repo_path).map_err(|e| {
                SinexError::service(format!(
                    "Failed to open repository at {}: {e}",
                    repo_path.display()
                ))
            })?;

            let head = repo
                .head()
                .map_err(|e| SinexError::service("Failed to get HEAD reference").with_source(e))?;

            let commit = head
                .peel_to_commit()
                .map_err(|e| SinexError::service("Failed to peel HEAD to commit").with_source(e))?;

            Ok(commit.id().to_string())
        })
        .await
        .map_err(|e| {
            SinexError::service(format!("Git task panicked: {e}"))
                .with_operation("gitops.head_commit_sha")
        })?
    }

    /// Compute a deterministic directory name for a repository URL.
    fn repo_dir(&self, url: &str) -> PathBuf {
        // Use blake3 hash of the URL to get a stable, filesystem-safe name
        let hash = blake3::hash(url.as_bytes());
        let short = &hash.to_hex()[..16];
        self.work_dir.join(format!("gitops-{short}"))
    }
}

/// Validate that a URL is safe for cloning.
///
/// Rejects `file://` scheme to prevent local file access attacks.
fn validate_url(url: &str) -> IngestdResult<()> {
    if url.starts_with("file://") {
        return Err(
            SinexError::validation("file:// URLs are not allowed for gitops sources")
                .with_operation("gitops.validate_url")
                .with_context("url", url.to_string()),
        );
    }

    // Basic sanity: must contain at least a host-like component
    if url.is_empty() {
        return Err(
            SinexError::validation("Empty repository URL").with_operation("gitops.validate_url")
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use xtask::sandbox::prelude::*;

    #[sinex_test]
    async fn validate_url_rejects_file_scheme() -> TestResult<()> {
        assert!(validate_url("file:///etc/passwd").is_err());
        Ok(())
    }

    #[sinex_test]
    async fn validate_url_rejects_empty() -> TestResult<()> {
        assert!(validate_url("").is_err());
        Ok(())
    }

    #[sinex_test]
    async fn validate_url_accepts_https() -> TestResult<()> {
        assert!(validate_url("https://github.com/org/repo.git").is_ok());
        Ok(())
    }

    #[sinex_test]
    async fn validate_url_accepts_ssh() -> TestResult<()> {
        assert!(validate_url("git@github.com:org/repo.git").is_ok());
        Ok(())
    }
}
