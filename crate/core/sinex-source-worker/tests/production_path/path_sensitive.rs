//! Production-path obligation tests for parsers that require logical paths.

#[cfg(test)]
mod tests {
    use std::process::Command;

    use xtask::sandbox::prelude::*;

    const KNOWLEDGEBASE_NOTE: &[u8] = b"\
---
id: permanent.concept.test
created: 2025-03-15
tags:
  - concept
---
This is the body. It has a [[wikilink]] and a #body-tag.
";

    #[sinex_test]
    async fn knowledgebase_vault_obligations(_ctx: TestContext) -> TestResult<()> {
        let failures = crate::_run_case_with_logical_path(
            "knowledgebase-vault",
            crate::AdapterKind::StaticFile,
            KNOWLEDGEBASE_NOTE,
            "notes/permanent.concept.test.md",
            &["note.observed"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "knowledgebase-vault obligations failed: {failures:#?}"
        );
        Ok(())
    }

    #[sinex_test]
    async fn git_commit_history_obligations(_ctx: TestContext) -> TestResult<()> {
        let repo = tempfile::tempdir()?;
        initialize_git_repo(repo.path())?;

        let repo_path = repo.path().to_string_lossy().to_string();
        let failures = crate::_run_case_with_logical_path(
            "git-commit-history",
            crate::AdapterKind::StaticFile,
            b"",
            &repo_path,
            &["commit.created"],
            crate::ALL_OBLIGATIONS,
        )
        .await;
        assert!(
            failures.is_empty(),
            "git-commit-history obligations failed: {failures:#?}"
        );
        Ok(())
    }

    fn initialize_git_repo(path: &std::path::Path) -> TestResult<()> {
        run_git(path, &["init"])?;
        run_git(path, &["config", "user.name", "Sinex Test"])?;
        run_git(
            path,
            &["config", "user.email", "sinex-test@example.invalid"],
        )?;

        std::fs::write(path.join("README.md"), "hello\n")?;
        run_git(path, &["add", "README.md"])?;
        run_git(
            path,
            &[
                "-c",
                "commit.gpgsign=false",
                "commit",
                "-m",
                "test: seed repository",
            ],
        )?;
        Ok(())
    }

    fn run_git(path: &std::path::Path, args: &[&str]) -> TestResult<()> {
        let output = Command::new("git").current_dir(path).args(args).output()?;
        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            color_eyre::eyre::bail!("git {:?} failed: {stderr}", args);
        }
        Ok(())
    }
}
