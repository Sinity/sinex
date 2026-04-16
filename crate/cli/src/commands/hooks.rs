use clap::{Args, Subcommand};
use color_eyre::Result;
use sinex_primitives::rpc::ingest::EventIngestRequest;

use crate::client::GatewayClient;

#[derive(Debug, Args)]
pub struct HooksCommand {
    #[command(subcommand)]
    pub cmd: HookCommands,
}

impl HooksCommand {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        self.cmd.execute(client).await
    }
}

#[derive(Debug, Subcommand)]
pub enum HookCommands {
    /// Capture a git commit event (call from post-commit hook)
    GitCommit(GitCommitArgs),
}

#[derive(Debug, Args)]
pub struct GitCommitArgs {
    /// Git repository path (defaults to current directory)
    #[arg(long)]
    pub repo: Option<String>,
}

impl HookCommands {
    pub async fn execute(&self, client: &GatewayClient) -> Result<()> {
        match self {
            Self::GitCommit(args) => capture_git_commit(client, args).await,
        }
    }
}

async fn capture_git_commit(client: &GatewayClient, args: &GitCommitArgs) -> Result<()> {
    let repo_path = args.repo.clone().unwrap_or_else(|| ".".to_string());

    let output = std::process::Command::new("git")
        .args(["log", "-1", "--format=%H%n%s%n%an%n%ae%n%aI%n%D"])
        .current_dir(&repo_path)
        .output()?;

    if !output.status.success() {
        return Err(color_eyre::eyre::eyre!(
            "git log failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }

    let text = String::from_utf8_lossy(&output.stdout);
    let lines: Vec<&str> = text.trim().split('\n').collect();
    if lines.len() < 5 {
        return Err(color_eyre::eyre::eyre!(
            "unexpected git log output format"
        ));
    }

    let commit_hash = lines[0];
    let subject = lines[1];
    let author_name = lines[2];
    let author_email = lines[3];
    let author_date = lines[4];
    let refs = if lines.len() > 5 { lines[5] } else { "" };

    let stat_output = std::process::Command::new("git")
        .args(["diff", "--shortstat", "HEAD~1..HEAD"])
        .current_dir(&repo_path)
        .output()?;
    let shortstat = String::from_utf8_lossy(&stat_output.stdout)
        .trim()
        .to_string();

    let repo_name_output = std::process::Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(&repo_path)
        .output()?;
    let repo_name = String::from_utf8_lossy(&repo_name_output.stdout)
        .trim()
        .split('/')
        .last()
        .unwrap_or("unknown")
        .to_string();

    let payload = serde_json::json!({
        "commit_hash": commit_hash,
        "subject": subject,
        "author_name": author_name,
        "author_email": author_email,
        "author_date": author_date,
        "refs": refs,
        "shortstat": shortstat,
        "repository": repo_name,
        "repo_path": repo_path,
    });

    let now = time::OffsetDateTime::now_utc();
    #[allow(clippy::expect_used)]
    let ts_orig = now
        .format(&time::format_description::well_known::Rfc3339)
        .expect("RFC3339 format always valid");

    let req = EventIngestRequest {
        source: "git.activity".to_string(),
        event_type: "git.commit".to_string(),
        payload,
        ts_orig,
        host: None,
    };

    let result = client.ingest_event(req).await?;

    eprintln!(
        "[sinex] captured git commit {} in {} (event: {})",
        &commit_hash[..std::cmp::min(8, commit_hash.len())],
        repo_name,
        result.event_id,
    );

    Ok(())
}
