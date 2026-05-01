use clap::Subcommand;
use color_eyre::Result;
use std::process::Command;

use crate::config::Config;
use crate::model::OutputFormat;
use crate::prompt;

/// Config subcommands
#[derive(Debug, Subcommand)]
#[command(after_help = "\
EXAMPLES:
    # Initialize local preferences file
    sinexctl config init

    # Force overwrite existing preferences
    sinexctl config init --force

    # Show effective configuration
    sinexctl config show

    # Show config as JSON
    sinexctl config show -f json

    # Show config file path
    sinexctl config path

    # Edit config in $EDITOR
    sinexctl config edit
")]
pub enum ConfigCommands {
    /// Initialize local preferences file
    Init {
        /// Overwrite existing config file
        #[arg(long)]
        force: bool,
    },

    /// Show effective configuration (runtime env/CLI + local preferences)
    Show {
        /// Output format
        #[arg(long, short = 'f', value_enum, default_value = "yaml")]
        format: OutputFormat,
    },

    /// Show config file path
    Path,

    /// Open config file in $EDITOR
    Edit,
}

impl ConfigCommands {
    pub fn execute(&self) -> Result<()> {
        match self {
            Self::Init { force } => config_init(*force),
            Self::Show { format } => config_show(*format),
            Self::Path => config_path(),
            Self::Edit => config_edit(),
        }
    }
}

/// Initialize user preference file
fn config_init(force: bool) -> Result<()> {
    let config_path = Config::config_file_path()?;

    if config_path.exists() && !force {
        println!("Config file already exists at: {}", config_path.display());
        println!();
        println!("Use --force to overwrite, or edit with:");
        println!("  sinexctl config edit");
        return Ok(());
    }

    println!("Sinex CLI Preferences Wizard");
    println!("============================");
    println!();

    // Default output format
    let format_options = [
        "table (human-readable)",
        "json (for scripting)",
        "yaml (for config files)",
    ];
    let default_format = prompt::select("Default output format:", &format_options, 0)?;
    let default_format = match default_format.as_str() {
        "json (for scripting)" => OutputFormat::Json,
        "yaml (for config files)" => OutputFormat::Yaml,
        _ => OutputFormat::Table,
    };

    let default_editor = std::env::var("EDITOR")
        .unwrap_or_else(|_| std::env::var("VISUAL").unwrap_or_else(|_| "vim".to_string()));
    let editor = prompt::text(
        "Preferred editor",
        Some(&default_editor),
        Some("Used by 'sinexctl config edit'"),
    )?;

    let table_style = prompt::select(
        "Table style:",
        &["rounded", "ascii", "modern", "minimal"],
        0,
    )?;

    let config_content = Config::render_user_preferences_toml(default_format, editor, table_style)?;

    // Create parent directories
    if let Some(parent) = config_path.parent() {
        std::fs::create_dir_all(parent)?;
    }

    // Write config file
    std::fs::write(&config_path, config_content)?;

    println!();
    println!("Configuration saved to: {}", config_path.display());
    println!();
    println!("You can edit it anytime with:");
    println!("  sinexctl config edit");

    Ok(())
}

/// Show current configuration
fn config_show(format: OutputFormat) -> Result<()> {
    let config = Config::load().unwrap_or_else(|_| Config::default());

    match format {
        OutputFormat::Json | OutputFormat::Dot => {
            let json = serde_json::to_string(&config)?;
            println!("{json}");
            return Ok(());
        }
        OutputFormat::Yaml | OutputFormat::Table => {
            // YAML is more readable for config display
            let yaml = serde_yaml::to_string(&config)?;
            println!("{yaml}");
        }
    }

    // Show config sources
    let config_path = Config::config_file_path()?;
    println!("---");
    println!("# Config sources (in priority order):");
    println!("#   1. CLI arguments (highest)");
    println!("#   2. Runtime environment variables (SINEX_RPC_*, DATABASE_URL)");
    if config_path.exists() {
        println!(
            "#   3. User preference file (format/theme/editor/aliases only): {}",
            config_path.display()
        );
    } else {
        println!("#   3. User preference file: (not found)");
    }
    println!("#   4. Defaults (lowest)");

    Ok(())
}

/// Show config file path
fn config_path() -> Result<()> {
    let config_path = Config::config_file_path()?;
    println!("{}", config_path.display());

    if !config_path.exists() {
        eprintln!();
        eprintln!("(File does not exist yet. Run 'sinexctl config init' to create it.)");
    }

    Ok(())
}

/// Open config file in $EDITOR
fn config_edit() -> Result<()> {
    let config_path = Config::config_file_path()?;

    // Create config if it doesn't exist
    if !config_path.exists() {
        println!("Config file does not exist. Creating default config...");
        Config::init_config_file()?;
    }

    // Get editor from environment
    let editor = std::env::var("EDITOR")
        .unwrap_or_else(|_| std::env::var("VISUAL").unwrap_or_else(|_| "vi".to_string()));

    println!("Opening {} with {}...", config_path.display(), editor);

    // Open editor
    let status = Command::new(&editor).arg(&config_path).status()?;

    if !status.success() {
        return Err(color_eyre::eyre::eyre!(
            "Editor exited with status: {}",
            status
        ));
    }

    println!("Config file saved.");

    Ok(())
}
