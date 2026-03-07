use clap::Subcommand;
use serde::Serialize;
use sinex_primitives::rpc::gitops::GitOpsSourceInfo;

use crate::Result;
use crate::client::GatewayClient;
use crate::fmt::CommandOutput;
use crate::model::OutputFormat;
use tabled::{builder::Builder, settings::Style};

/// `GitOps` schema source management
#[derive(Debug, Subcommand)]
pub enum GitOpsCommands {
    /// List configured gitops sources
    List {
        /// Include disabled sources
        #[arg(long)]
        all: bool,
    },

    /// Create a new gitops source
    Create {
        /// Git repository URL (https:// or git@)
        url: String,

        /// Branch to sync (default: main)
        #[arg(long)]
        branch: Option<String>,

        /// Glob pattern for schema files (default: schemas/**/*.json)
        #[arg(long)]
        pattern: Option<String>,

        /// Sync frequency in minutes (default: 60)
        #[arg(long)]
        interval: Option<i32>,
    },

    /// Delete a gitops source
    Delete {
        /// Source ID (`UUIDv7`)
        id: String,
    },

    /// Trigger immediate sync
    Sync {
        /// Source ID (`UUIDv7`)
        id: String,
    },
}

impl GitOpsCommands {
    pub async fn execute(&self, client: &GatewayClient, format: OutputFormat) -> Result<()> {
        match self {
            Self::List { all } => {
                let sources = client.gitops_list(*all).await?;
                let views: Vec<GitOpsSourceView> =
                    sources.into_iter().map(GitOpsSourceView::from).collect();
                CommandOutput::list(views, "No gitops sources found.", format_gitops_table)
                    .display(&format)?;
            }
            Self::Create {
                url,
                branch,
                pattern,
                interval,
            } => {
                let response = client
                    .gitops_create(url.clone(), branch.clone(), pattern.clone(), *interval)
                    .await?;

                if format == OutputFormat::Table {
                    println!("Created gitops source: {}", response.id);
                } else {
                    CommandOutput::single(response, |r| r.id.to_string()).display(&format)?;
                }
            }
            Self::Delete { id } => {
                let deleted = client.gitops_delete(id.clone()).await?;
                if format == OutputFormat::Table {
                    if deleted {
                        println!("Deleted gitops source {id}");
                    } else {
                        println!("Failed to delete source (not found?)");
                    }
                } else {
                    CommandOutput::single(serde_json::json!({ "deleted": deleted }), |v| {
                        v.to_string()
                    })
                    .display(&format)?;
                }
            }
            Self::Sync { id } => {
                let response = client.gitops_sync(id.clone()).await?;
                if format == OutputFormat::Table {
                    println!("{}", response.message);
                } else {
                    CommandOutput::single(response, |r| r.message.clone()).display(&format)?;
                }
            }
        }
        Ok(())
    }
}

#[derive(Serialize)]
struct GitOpsSourceView {
    id: String,
    url: String,
    branch: String,
    pattern: String,
    enabled: bool,
    last_sync: String,
    frequency: String,
}

impl From<GitOpsSourceInfo> for GitOpsSourceView {
    fn from(source: GitOpsSourceInfo) -> Self {
        Self {
            id: source.id.to_string(),
            url: source.repository_url,
            branch: source.branch,
            pattern: source.path_pattern,
            enabled: source.sync_enabled,
            last_sync: source
                .last_sync_at.map_or_else(|| "never".to_string(), |ts| ts.format_rfc3339()),
            frequency: format!("{}m", source.sync_frequency_minutes),
        }
    }
}

fn format_gitops_table(sources: &[GitOpsSourceView]) -> String {
    let mut builder = Builder::new();
    builder.push_record([
        "ID",
        "URL",
        "Branch",
        "Pattern",
        "Enabled",
        "Last Sync",
        "Frequency",
    ]);

    for source in sources {
        builder.push_record([
            source.id.as_str(),
            source.url.as_str(),
            source.branch.as_str(),
            source.pattern.as_str(),
            if source.enabled { "yes" } else { "no" },
            source.last_sync.as_str(),
            source.frequency.as_str(),
        ]);
    }

    let mut table = builder.build();
    table.with(Style::rounded());
    table.to_string()
}
