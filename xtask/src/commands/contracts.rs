//! Event payload schema (contracts) management
//!
//! Only `contracts info` remains here; schema readiness/compat checks moved to `xtask ci`.

use color_eyre::eyre::Result;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Contracts (event payload schema) command variants
#[derive(Debug, Clone, clap::Subcommand)]
pub enum ContractsSubcommand {
    /// Show schema information
    Info {
        #[arg(value_enum)]
        query: ContractsInfoQuery,
    },
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum ContractsInfoQuery {
    /// List all schema names
    ListSchemas,
    /// List schemas requiring grants
    ListGrantableSchemas,
    /// Show detailed schema information
    DescribeSchemas,
}

/// Contracts management command (event payload schemas)
#[derive(Debug, Clone, clap::Args)]
pub struct ContractsCommand {
    #[command(subcommand)]
    pub subcommand: ContractsSubcommand,
}

impl XtaskCommand for ContractsCommand {
    fn name(&self) -> &'static str {
        "contracts"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            ContractsSubcommand::Info { query } => Ok(execute_info(query, ctx)),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::database()
    }
}

fn execute_info(query: &ContractsInfoQuery, ctx: &CommandContext) -> CommandResult {
    use sinex_schema::schema_registry::{SINEX_SCHEMAS, schema_names, schemas_requiring_grants};

    match query {
        ContractsInfoQuery::ListSchemas => {
            let names: Vec<_> = schema_names().collect();
            if ctx.is_human() {
                for name in &names {
                    println!("{name}");
                }
            }
            CommandResult::success()
                .with_message("Listed all contract names")
                .with_duration(ctx.elapsed())
                .with_data(serde_json::json!({ "schemas": names }))
        }
        ContractsInfoQuery::ListGrantableSchemas => {
            let grantable: Vec<_> = schemas_requiring_grants().map(|s| s.name).collect();
            if ctx.is_human() {
                for name in &grantable {
                    println!("{name}");
                }
            }
            CommandResult::success()
                .with_message("Listed grantable schemas")
                .with_duration(ctx.elapsed())
                .with_data(serde_json::json!({ "grantable_schemas": grantable }))
        }
        ContractsInfoQuery::DescribeSchemas => {
            let descriptions: Vec<_> = SINEX_SCHEMAS
                .iter()
                .map(|s| serde_json::json!({ "name": s.name, "description": s.description }))
                .collect();
            if ctx.is_human() {
                for schema in SINEX_SCHEMAS {
                    println!("{:20} - {}", schema.name, schema.description);
                }
            }
            CommandResult::success()
                .with_message("Described all contracts")
                .with_duration(ctx.elapsed())
                .with_data(serde_json::json!({ "schemas": descriptions }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sandbox::sinex_test;

    #[sinex_test]
    async fn test_contracts_command_metadata() -> ::xtask::sandbox::TestResult<()> {
        let cmd = ContractsCommand {
            subcommand: ContractsSubcommand::Info {
                query: ContractsInfoQuery::ListSchemas,
            },
        };

        let metadata = cmd.metadata();
        assert_eq!(metadata.category, Some("database"));
        assert!(metadata.timeout.is_some());
        Ok(())
    }

    #[sinex_test]
    async fn test_contracts_command_name() -> ::xtask::sandbox::TestResult<()> {
        let cmd = ContractsCommand {
            subcommand: ContractsSubcommand::Info {
                query: ContractsInfoQuery::ListSchemas,
            },
        };

        assert_eq!(cmd.name(), "contracts");
        Ok(())
    }
}
