//! Privacy engine command - CLI access to sensitive data detection and handling
//!
//! Provides utilities for:
//! - Listing and filtering privacy rules from the built-in catalog
//! - Testing input text against the privacy engine
//! - Decrypting encrypted privacy tokens
//! - Viewing privacy key configuration
//! - Inspecting per-rule match statistics
//! - Generating and inspecting TOML configuration

use clap::{Args, Subcommand};
use color_eyre::eyre::{Result, eyre};
use console::style;
use serde_json::json;
use sinex_primitives::privacy::{
    Matcher, PrivacyConfig, PrivacyEngine, ProcessingContext, RuleCategory, Strategy,
};

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Privacy subcommand variants
#[derive(Debug, Clone, Subcommand)]
pub enum PrivacySubcommand {
    /// List all privacy rules in the built-in catalog
    Catalog {
        /// Filter by category (secret, pii, privacy, custom)
        #[arg(short, long)]
        category: Option<String>,

        /// Show disabled rules
        #[arg(long)]
        include_disabled: bool,
    },

    /// Test input text against the privacy engine
    Test {
        /// Input text to process
        input: String,

        /// Processing context (command, clipboard, window_title, journal, dbus, notification, document, metadata)
        #[arg(short, long, default_value = "command")]
        context: String,
    },

    /// Decrypt an encrypted privacy token
    Decrypt {
        /// The encrypted token (starts with ⌜enc:)
        token: String,
    },

    /// Show privacy key information
    Key {
        /// Generate a new random 256-bit key (hex-encoded)
        #[arg(long)]
        generate: bool,
    },

    /// Show per-rule match statistics
    Stats,

    /// Show or generate privacy configuration
    Config {
        /// Generate an example TOML config file to stdout
        #[arg(long)]
        init: bool,
    },
}

/// Privacy engine command
#[derive(Debug, Clone, Args)]
pub struct PrivacyCommand {
    #[command(subcommand)]
    pub subcommand: PrivacySubcommand,
}

#[async_trait::async_trait]
impl XtaskCommand for PrivacyCommand {
    fn name(&self) -> &'static str {
        "privacy"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        match &self.subcommand {
            PrivacySubcommand::Catalog {
                category,
                include_disabled,
            } => execute_catalog(category.as_deref(), *include_disabled, ctx),
            PrivacySubcommand::Test { input, context } => execute_test(input, context, ctx),
            PrivacySubcommand::Decrypt { token } => execute_decrypt(token, ctx),
            PrivacySubcommand::Key { generate } => execute_key(*generate, ctx),
            PrivacySubcommand::Stats => execute_stats(ctx),
            PrivacySubcommand::Config { init } => execute_config(*init, ctx),
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::utility()
    }
}

/// Execute catalog subcommand: list privacy rules
fn execute_catalog(
    category: Option<&str>,
    include_disabled: bool,
    ctx: &CommandContext,
) -> Result<CommandResult> {
    let engine = PrivacyEngine::new(PrivacyConfig::from_env())?;
    let rules = engine.catalog();

    // Parse category filter
    let category_filter = category.and_then(|c| match c.to_lowercase().as_str() {
        "secret" => Some(RuleCategory::Secret),
        "pii" => Some(RuleCategory::Pii),
        "privacy" => Some(RuleCategory::Privacy),
        "custom" => Some(RuleCategory::Custom),
        _ => None,
    });

    // Filter rules
    let filtered: Vec<_> = rules
        .iter()
        .filter(|r| {
            if !include_disabled && !r.enabled {
                return false;
            }
            category_filter.is_none() || Some(r.category) == category_filter
        })
        .collect();

    if ctx.is_human() {
        // Print human-readable table
        println!(
            "{}",
            style(format!("Privacy Rules Catalog ({})", filtered.len()))
                .bold()
                .cyan()
        );
        println!();

        if filtered.is_empty() {
            println!("  No rules found");
            return Ok(CommandResult::success().with_message("0 rules"));
        }

        for rule in &filtered {
            let status = if rule.enabled {
                style("✓").green()
            } else {
                style("✗").red()
            };

            println!("  {} {}", status, style(&rule.name).bold().yellow());
            println!("    Category:    {}", format_category(rule.category));
            println!("    Description: {}", rule.description);
            println!("    Strategy:    {}", format_strategy(&rule.strategy));
            println!("    Matcher:     {}", format_matcher(&rule.matcher));
            println!(
                "    Contexts:    {}",
                if rule.contexts.is_empty() {
                    "all".to_string()
                } else {
                    rule.contexts
                        .iter()
                        .map(format_context)
                        .collect::<Vec<_>>()
                        .join(", ")
                }
            );
            println!();
        }
    }

    // JSON output
    let data = json!(
        filtered
            .iter()
            .map(|rule| json!({
                "name": rule.name,
                "category": format!("{:?}", rule.category).to_lowercase(),
                "description": rule.description,
                "strategy": format_strategy(&rule.strategy),
                "matcher_type": format_matcher(&rule.matcher),
                "contexts": if rule.contexts.is_empty() {
                    json!(["all"])
                } else {
                    json!(rule.contexts.iter().map(format_context).collect::<Vec<_>>())
                },
                "enabled": rule.enabled,
            }))
            .collect::<Vec<_>>()
    );

    Ok(CommandResult::success()
        .with_message(format!("{} rules", filtered.len()))
        .with_data(data)
        .with_duration(ctx.elapsed()))
}

/// Execute test subcommand: process text through the privacy engine
fn execute_test(input: &str, context_str: &str, ctx: &CommandContext) -> Result<CommandResult> {
    let context = parse_context(context_str)?;

    let mut config = PrivacyConfig::from_env();
    config.track_stats = true;
    let engine = PrivacyEngine::new(config)?;

    let result = engine.process(input, context);

    if ctx.is_human() {
        println!("{}", style("Privacy Engine Test Result").bold().cyan());
        println!();
        println!("  Context:       {}", format_context(&context));
        println!("  Input length:  {} bytes", input.len());
        println!();

        if result.suppressed {
            println!(
                "  {}",
                style("SUPPRESSED — input would be dropped").red().bold()
            );
        } else if result.any_matched() {
            println!(
                "  {} {}",
                style("MATCHED:").yellow().bold(),
                result.matched_rules.join(", ")
            );
            println!(
                "  {} {}",
                style("Output:").bold(),
                style(result.text.as_ref()).yellow()
            );
        } else {
            println!(
                "  {}",
                style("CLEAN — no sensitive data detected").green().bold()
            );
        }
        println!();
        println!("  Original:      {}", style(input).dim());
        println!("  Processed:     {}", style(result.text.as_ref()).dim());
    }

    let data = json!({
        "original": input,
        "processed": result.text.as_ref(),
        "suppressed": result.suppressed,
        "matched_rules": result.matched_rules,
        "changed": input != result.text.as_ref(),
        "context": format_context(&context),
    });

    Ok(CommandResult::success()
        .with_message(if result.any_matched() {
            format!("Matched {} rule(s)", result.matched_rules.len())
        } else {
            "No matches".to_string()
        })
        .with_data(data)
        .with_duration(ctx.elapsed()))
}

/// Execute decrypt subcommand: decrypt an encrypted token
fn execute_decrypt(token: &str, ctx: &CommandContext) -> Result<CommandResult> {
    let engine = PrivacyEngine::new(PrivacyConfig::from_env())?;

    match engine.decrypt(token) {
        Ok(decrypted) => {
            if ctx.is_human() {
                println!("{}", style("Decrypted Token").bold().cyan());
                println!();
                println!("  {}", style(&decrypted).yellow());
            }

            Ok(CommandResult::success()
                .with_message("Token decrypted successfully")
                .with_data(json!({
                    "decrypted": decrypted,
                    "token_type": "encrypted",
                }))
                .with_duration(ctx.elapsed()))
        }
        Err(e) => {
            let msg = format!("Failed to decrypt token: {e}");
            if ctx.is_human() {
                eprintln!("{}", style(&msg).red());
                eprintln!();
                eprintln!(
                    "  Ensure SINEX_PRIVACY_KEY is set or use `xtask privacy key --generate`"
                );
            }

            Ok(CommandResult::success()
                .with_message("Decryption failed")
                .with_data(json!({
                    "error": e.to_string(),
                    "hint": "Ensure SINEX_PRIVACY_KEY is configured",
                }))
                .with_duration(ctx.elapsed()))
        }
    }
}

/// Execute key subcommand: show or generate privacy key
fn execute_key(generate: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if generate {
        // Generate a new random 256-bit key using blake3
        let seed = format!(
            "{}:{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let hash = blake3::hash(seed.as_bytes());
        let hex_key = hash.to_hex().to_string();

        if ctx.is_human() {
            println!("{}", style("Generated Privacy Key").bold().cyan());
            println!();
            println!("  {}", style(&hex_key).yellow().bold());
            println!();
            println!("  Set with:");
            println!("    export SINEX_PRIVACY_KEY={}", style(&hex_key).yellow());
            println!();
            println!("  Or write to file:");
            println!(
                "    echo {} > ~/.sinex/privacy.key",
                style(&hex_key).yellow()
            );
        }

        Ok(CommandResult::success()
            .with_message("Generated new privacy key")
            .with_data(json!({
                "key": hex_key,
                "bits": 256,
            }))
            .with_duration(ctx.elapsed()))
    } else {
        let config = PrivacyConfig::from_env();
        let has_key = config.key.resolve().is_some();
        let source = if config.key.key_file.is_some() {
            Some("file")
        } else if config.key.key_hex.is_some() {
            Some("environment")
        } else {
            None
        };

        if ctx.is_human() {
            println!("{}", style("Privacy Key Status").bold().cyan());
            println!();

            if has_key {
                println!("  {} Key configured", style("✓").green());
                if let Some(src) = source {
                    println!("    Source: {}", style(src).dim());
                }
            } else {
                println!("  {} Key not configured", style("✗").red());
                println!(
                    "    Generate with: {}",
                    style("xtask privacy key --generate").yellow()
                );
                println!();
                println!("  Note: Encrypt and Hash strategies will degrade to Redact");
            }
        }

        Ok(CommandResult::success()
            .with_message(if has_key {
                "Key configured"
            } else {
                "Key not configured"
            })
            .with_data(json!({
                "configured": has_key,
                "source": source,
            }))
            .with_duration(ctx.elapsed()))
    }
}

/// Execute stats subcommand: show per-rule match statistics
fn execute_stats(ctx: &CommandContext) -> Result<CommandResult> {
    let mut config = PrivacyConfig::from_env();
    config.track_stats = true;

    let engine = PrivacyEngine::new(config)?;
    let mut stats = engine.stats_snapshot();
    stats.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by count descending

    if ctx.is_human() {
        println!("{}", style("Privacy Engine Statistics").bold().cyan());
        println!();
        println!(
            "  {}",
            style("(Per-process stats — this is a fresh engine)").dim()
        );
        println!();

        if stats.is_empty() {
            println!("  No statistics available");
        } else {
            for (rule, count) in &stats {
                if *count > 0 {
                    println!("  {} {}", style(count).yellow().bold(), rule);
                }
            }
        }
    }

    let data = json!(
        stats
            .iter()
            .filter(|(_, count)| *count > 0)
            .map(|(rule, count)| json!({
                "rule": rule,
                "hits": count,
            }))
            .collect::<Vec<_>>()
    );

    Ok(CommandResult::success()
        .with_message(format!(
            "{} rules with matches",
            stats.iter().filter(|(_, c)| *c > 0).count()
        ))
        .with_data(data)
        .with_duration(ctx.elapsed()))
}

/// Execute config subcommand: show or generate configuration
fn execute_config(init: bool, ctx: &CommandContext) -> Result<CommandResult> {
    if init {
        // Generate example TOML config
        let example = r#"# Sinex Privacy Engine Configuration
# Place at: $SINEX_STATE_DIR/privacy.toml
# Or set:   SINEX_PRIVACY_CONFIG=/path/to/this/file

# Master switch (default: true)
enabled = true

# Built-in rule categories to activate:
#   "all"  — all categories (default)
#   "none" — no built-in rules
#   ["secret", "pii", "privacy"] — only listed categories
builtin_categories = "all"

# Default strategy for rules that don't specify one.
# Options: { action = "redact" }, { action = "encrypt" },
#          { action = "hash" }, { action = "suppress" },
#          { action = "mask", char = "*", keep_prefix = 4, keep_suffix = 4 }
default_strategy = { action = "redact" }

# Optional: override default strategy for Secret category rules.
# Uncomment to encrypt all secrets (requires a configured key):
# secret_strategy = { action = "encrypt" }

# Enable per-rule match counting (default: false)
track_stats = false

# Encryption key for Encrypt and Hash strategies
[key]
# Path to a file containing a 256-bit key (32 raw bytes or 64-char hex)
# file = "/path/to/privacy.key"
#
# Hex-encoded key (development only — prefer file in production)
# hex = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

# Override built-in rules by name.
# Supported fields: enabled, strategy, contexts
#
# [overrides.email_address]
# enabled = false
#
# [overrides.ipv4_address]
# strategy = { action = "hash" }
#
# [overrides.ssn]
# contexts = ["document", "clipboard"]

# Add custom rules (merged with built-in rules, not replacing them).
#
# [[extra_rules]]
# name = "internal_project_code"
# description = "Internal project codes (PROJ-XXXX)"
# category = "custom"
# matcher = { type = "regex", pattern = "PROJ-\\d{4}" }
# strategy = { action = "redact", label = "<PROJECT_CODE>" }
# contexts = ["command", "clipboard"]
"#;

        if ctx.is_human() {
            print!("{example}");
        }

        Ok(CommandResult::success()
            .with_message("Example config generated")
            .with_data(json!({ "example": example }))
            .with_duration(ctx.elapsed()))
    } else {
        // Show current config source and status
        let config_path = std::env::var("SINEX_PRIVACY_CONFIG").ok();
        let state_dir_path = std::env::var("SINEX_STATE_DIR")
            .ok()
            .map(|d| format!("{d}/privacy.toml"));

        let active_path = config_path
            .as_deref()
            .filter(|p| std::path::Path::new(p).exists())
            .or_else(|| {
                state_dir_path
                    .as_deref()
                    .filter(|p| std::path::Path::new(p).exists())
            });

        let config = PrivacyConfig::from_env();
        let rule_count = PrivacyEngine::new(config.clone())
            .map(|e| e.catalog().len())
            .unwrap_or(0);

        if ctx.is_human() {
            println!("{}", style("Privacy Engine Configuration").bold().cyan());
            println!();
            println!(
                "  Enabled:          {}",
                if config.enabled {
                    style("yes").green()
                } else {
                    style("no").red()
                }
            );
            println!("  Active rules:     {}", style(rule_count).yellow());
            println!(
                "  Default strategy: {}",
                format_strategy(&config.default_strategy)
            );
            if let Some(ref s) = config.secret_strategy {
                println!("  Secret strategy:  {}", format_strategy(s));
            }
            println!(
                "  Stats tracking:   {}",
                if config.track_stats { "on" } else { "off" }
            );
            println!();

            if let Some(path) = active_path {
                println!("  {} Config file: {}", style("✓").green(), style(path).dim());
            } else {
                println!("  {} No config file found", style("·").dim());
                println!(
                    "    Generate with: {}",
                    style("xtask privacy config --init > privacy.toml").yellow()
                );
            }
        }

        Ok(CommandResult::success()
            .with_message(format!("{rule_count} active rules"))
            .with_data(json!({
                "enabled": config.enabled,
                "active_rules": rule_count,
                "config_file": active_path,
                "default_strategy": format_strategy(&config.default_strategy),
                "secret_strategy": config.secret_strategy.as_ref().map(format_strategy),
                "track_stats": config.track_stats,
            }))
            .with_duration(ctx.elapsed()))
    }
}

// ─── Formatting helpers ──────────────────────────────────────────

fn format_category(cat: RuleCategory) -> String {
    match cat {
        RuleCategory::Secret => "Secret".to_string(),
        RuleCategory::Pii => "PII".to_string(),
        RuleCategory::Privacy => "Privacy".to_string(),
        RuleCategory::Custom => "Custom".to_string(),
    }
}

fn format_context(ctx: &ProcessingContext) -> String {
    match ctx {
        ProcessingContext::Command => "command",
        ProcessingContext::Clipboard => "clipboard",
        ProcessingContext::WindowTitle => "window_title",
        ProcessingContext::Journal => "journal",
        ProcessingContext::Dbus => "dbus",
        ProcessingContext::Notification => "notification",
        ProcessingContext::Document => "document",
        ProcessingContext::Metadata => "metadata",
    }
    .to_string()
}

fn format_strategy(strategy: &Strategy) -> String {
    match strategy {
        Strategy::Redact { label } => {
            if let Some(lbl) = label {
                format!("Redact (label: {lbl})")
            } else {
                "Redact (default)".to_string()
            }
        }
        Strategy::Encrypt => "Encrypt (XChaCha20-Poly1305)".to_string(),
        Strategy::Hash => "Hash (BLAKE3 MAC)".to_string(),
        Strategy::Suppress => "Suppress (drop field)".to_string(),
        Strategy::Mask {
            char,
            keep_prefix,
            keep_suffix,
        } => {
            let ch = char.unwrap_or('*');
            let prefix = keep_prefix.unwrap_or(0);
            let suffix = keep_suffix.unwrap_or(0);
            format!(
                "Mask (prefix: {prefix}, suffix: {suffix}, char: '{ch}')"
            )
        }
    }
}

fn format_matcher(matcher: &Matcher) -> String {
    match matcher {
        Matcher::Regex { .. } => "regex".to_string(),
        Matcher::Structural { detector } => format!("structural:{detector:?}"),
        Matcher::Literal { .. } => "literal".to_string(),
        Matcher::All(_) => "all".to_string(),
        Matcher::Any(_) => "any".to_string(),
    }
}

fn parse_context(s: &str) -> Result<ProcessingContext> {
    match s.to_lowercase().as_str() {
        "command" => Ok(ProcessingContext::Command),
        "clipboard" => Ok(ProcessingContext::Clipboard),
        "window_title" | "window" => Ok(ProcessingContext::WindowTitle),
        "journal" => Ok(ProcessingContext::Journal),
        "dbus" => Ok(ProcessingContext::Dbus),
        "notification" => Ok(ProcessingContext::Notification),
        "document" => Ok(ProcessingContext::Document),
        "metadata" => Ok(ProcessingContext::Metadata),
        _ => Err(eyre!(
            "Unknown context '{}'. Valid values: command, clipboard, window_title, journal, dbus, notification, document, metadata",
            s
        )),
    }
}
