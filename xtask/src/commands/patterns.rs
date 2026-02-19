//! Code pattern search command - promoted from analyze patterns

use color_eyre::eyre::{Result, WrapErr};
use serde::Serialize;
use std::path::PathBuf;
use std::process::Command;

use crate::command::{CommandContext, CommandMetadata, CommandResult, XtaskCommand};

/// Search for code patterns using ast-grep (promoted from analyze patterns)
#[derive(Debug, Clone, clap::Args)]
pub struct PatternsCommand {
    /// The pattern to search for (ast-grep syntax)
    #[arg(short, long)]
    pub pattern: String,
    /// Language to search (rust, typescript, etc)
    #[arg(short, long, default_value = "rust")]
    pub lang: String,
    /// Limit results
    #[arg(long, default_value = "100")]
    pub limit: usize,
    /// Directory to search (default: crate/)
    #[arg(long)]
    pub dir: Option<PathBuf>,
}

/// Result of a pattern search match
#[derive(Debug, Serialize)]
struct PatternMatch {
    file: String,
    line: u32,
    column: u32,
    text: String,
}

/// Summary of pattern search
#[derive(Debug, Serialize)]
struct PatternSearchResult {
    pattern: String,
    language: String,
    match_count: usize,
    matches: Vec<PatternMatch>,
}

#[async_trait::async_trait]
impl XtaskCommand for PatternsCommand {
    fn name(&self) -> &'static str {
        "patterns"
    }

    async fn execute(&self, ctx: &CommandContext) -> Result<CommandResult> {
        let search_dir = self
            .dir
            .as_ref()
            .map_or_else(|| "crate".to_string(), |p| p.to_string_lossy().to_string());

        // Run ast-grep with JSON output
        let output = Command::new("ast-grep")
            .args([
                "run",
                "--pattern",
                &self.pattern,
                "--lang",
                &self.lang,
                "--json=stream",
            ])
            .arg(&search_dir)
            .output()
            .context("Failed to run ast-grep. Is it installed?")?;

        if !output.status.success() && output.stdout.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // ast-grep returns non-zero if no matches, but that's OK
            if !stderr.contains("No files matched") && !stderr.is_empty() {
                return Ok(CommandResult::success()
                    .with_message("No matches found")
                    .with_duration(ctx.elapsed()));
            }
        }

        // Parse JSON output - ast-grep outputs one JSON object per line
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut matches = Vec::new();

        for line in stdout.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(line) {
                // Extract match info
                if let (Some(file), Some(range), Some(text)) = (
                    json.get("file").and_then(|v| v.as_str()),
                    json.get("range"),
                    json.get("text").and_then(|v| v.as_str()),
                ) {
                    let line_num = range
                        .get("start")
                        .and_then(|s| s.get("line"))
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0) as u32;
                    let col = range
                        .get("start")
                        .and_then(|s| s.get("column"))
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0) as u32;

                    matches.push(PatternMatch {
                        file: file.to_string(),
                        line: line_num,
                        column: col,
                        text: text.to_string(),
                    });

                    if matches.len() >= self.limit {
                        break;
                    }
                }
            }
        }

        let result = PatternSearchResult {
            pattern: self.pattern.clone(),
            language: self.lang.clone(),
            match_count: matches.len(),
            matches,
        };

        if ctx.is_human() {
            println!("Pattern: {}", self.pattern);
            println!("Language: {}", self.lang);
            println!("Matches: {}\n", result.match_count);

            for m in &result.matches {
                println!("{}:{}:{}", m.file, m.line, m.column);
                // Print the matched text, truncated
                let display_text = if m.text.len() > 100 {
                    format!("{}...", &m.text[..100])
                } else {
                    m.text.clone()
                };
                println!("  {}\n", display_text.replace('\n', " "));
            }

            Ok(CommandResult::success()
                .with_message(format!("Found {} matches", result.match_count))
                .with_duration(ctx.elapsed()))
        } else {
            Ok(CommandResult::success()
                .with_data(serde_json::to_value(&result)?)
                .with_duration(ctx.elapsed()))
        }
    }

    fn metadata(&self) -> CommandMetadata {
        CommandMetadata::build()
    }
}
