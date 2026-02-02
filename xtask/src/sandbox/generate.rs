//! Node generator with LLM integration
//!
//! This module implements `cargo xtask dev generate "spec..."` which uses an LLM
//! to generate SimpleProcessor implementations from natural language specs.
//!
//! The workflow:
//! 1. User describes what they want: "detect git commands from terminal events"
//! 2. LLM generates a complete SimpleProcessor implementation
//! 3. Code is written to a new crate in the workspace
//! 4. Hot reload picks up the change

use anyhow::{bail, Context, Result};
use camino::Utf8PathBuf;
use serde::{Deserialize, Serialize};

/// Configuration for the node generator
#[derive(Debug, Clone)]
pub struct GeneratorConfig {
    /// Workspace root path
    pub workspace_root: Utf8PathBuf,
    /// Output directory for generated nodes (relative to workspace)
    pub output_dir: String,
    /// LLM API endpoint
    pub llm_endpoint: String,
    /// LLM API key (from environment)
    pub llm_api_key: String,
    /// Model to use (e.g., "claude-3-opus", "gpt-4")
    pub model: String,
}

impl GeneratorConfig {
    /// Create config from environment
    pub fn from_env(workspace_root: Utf8PathBuf) -> Result<Self> {
        let llm_api_key = std::env::var("ANTHROPIC_API_KEY")
            .or_else(|_| std::env::var("OPENAI_API_KEY"))
            .context("ANTHROPIC_API_KEY or OPENAI_API_KEY must be set for node generation")?;

        let llm_endpoint = std::env::var("SINEX_LLM_ENDPOINT")
            .unwrap_or_else(|_| "https://api.anthropic.com/v1/messages".to_string());

        let model = std::env::var("SINEX_LLM_MODEL")
            .unwrap_or_else(|_| "claude-sonnet-4-20250514".to_string());

        Ok(Self {
            workspace_root,
            output_dir: "crate/nodes".to_string(),
            llm_endpoint,
            llm_api_key,
            model,
        })
    }
}

/// Node specification parsed from user input
#[derive(Debug, Clone, Serialize)]
pub struct NodeSpec {
    /// Name for the node (derived from spec or provided)
    pub name: String,
    /// User's natural language description
    pub description: String,
    /// Input event type (if specified)
    pub input_type: Option<String>,
    /// Output event type (if specified)
    pub output_type: Option<String>,
}

impl NodeSpec {
    /// Parse a node spec from user input
    pub fn parse(input: &str, name: Option<&str>) -> Self {
        // Extract name from input or use provided
        let name = name.map_or_else(|| Self::derive_name_from_spec(input), |s| s.to_string());

        Self {
            name,
            description: input.to_string(),
            input_type: Self::extract_input_type(input),
            output_type: Self::extract_output_type(input),
        }
    }

    fn derive_name_from_spec(spec: &str) -> String {
        // Extract meaningful words and create a name
        let words: Vec<&str> = spec
            .split_whitespace()
            .filter(|w| w.len() > 2)
            .filter(|w| !["the", "and", "for", "from", "that", "with", "into", "when"].contains(w))
            .take(3)
            .collect();

        if words.is_empty() {
            "generated-node".to_string()
        } else {
            words.join("-").to_lowercase().replace('.', "-")
        }
    }

    fn extract_input_type(spec: &str) -> Option<String> {
        // Look for patterns like "from X events" or "X.Y events"
        let spec_lower = spec.to_lowercase();
        if spec_lower.contains("terminal") {
            Some("terminal.command.executed".to_string())
        } else if spec_lower.contains("desktop") || spec_lower.contains("window") {
            Some("desktop.window.focused".to_string())
        } else if spec_lower.contains("file") || spec_lower.contains("fs") {
            Some("fs.file.modified".to_string())
        } else {
            None
        }
    }

    fn extract_output_type(spec: &str) -> Option<String> {
        // Derive output type from spec
        let name = Self::derive_name_from_spec(spec);
        Some(format!("{}.detected", name.replace('-', ".")))
    }
}

/// The prompt template for generating SimpleProcessor code
pub const SIMPLE_PROCESSOR_TEMPLATE: &str = r#"# Sinex SimpleProcessor Generator

You are generating a Rust SimpleProcessor implementation for the Sinex event processing system.

## SimpleProcessor Trait

```rust
#[async_trait]
pub trait SimpleProcessor: Send + Sync + 'static {
    /// State persisted across restarts
    type State: Serialize + DeserializeOwned + Default + Send + Sync;

    /// Input event type (must be Deserialize)
    type Input: DeserializeOwned + Send;

    /// Output event type (must be Serialize)
    type Output: Serialize + Send;

    /// Node name for logging/metrics
    fn name(&self) -> &'static str;

    /// Input event type string (e.g., "terminal.command.executed")
    fn input_event_type(&self) -> &'static str;

    /// Output event type string (e.g., "git.activity.detected")
    fn output_event_type(&self) -> &'static str;

    /// Process a single input event
    /// Returns None to filter (no output), Some(output) to emit
    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, SimpleProcessorError>;
}
```

## Available Event Types

### Input Events
- `terminal.command.executed`: Terminal commands with { command: String, cwd: String, exit_code: i32, timestamp: DateTime }
- `desktop.window.focused`: Window focus events with { app_name: String, window_title: String, timestamp: DateTime }
- `fs.file.modified`: File system changes with { path: String, event_type: String, timestamp: DateTime }
- `system.process.started`: Process spawn events with { pid: u32, command: String, parent_pid: u32 }

### Output Events
You can define any output event type. Common patterns:
- `{domain}.{entity}.{action}` (e.g., "git.commit.detected", "project.activity.summarized")

## User Specification

{spec}

## Requirements

1. Generate a complete, compilable Rust module
2. Include all necessary imports
3. Define State, Input, and Output structs with appropriate fields
4. Implement the SimpleProcessor trait
5. Add Serialize/Deserialize derives to all structs
6. Include brief doc comments
7. Return None from process() when input should be filtered out

## Output Format

Generate a single Rust file with the following structure:

```rust
//! {node_name} - {brief_description}
//!
//! Generated by cargo xtask dev generate

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sinex_primitives::Timestamp;
use sinex_node_sdk::simple_processor::{SimpleProcessor, SimpleProcessorError};

// Input event struct
#[derive(Debug, Clone, Deserialize)]
pub struct {InputEvent} {
    // fields...
    pub timestamp: Timestamp,
}

// Output event struct
#[derive(Debug, Clone, Serialize)]
pub struct {OutputEvent} {
    // fields...
}

// Processor state
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct {Name}State {
    // state fields...
}

// The processor
pub struct {Name};

#[async_trait]
impl SimpleProcessor for {Name} {
    type State = {Name}State;
    type Input = {InputEvent};
    type Output = {OutputEvent};

    fn name(&self) -> &'static str {
        "{node_name}"
    }

    fn input_event_type(&self) -> &'static str {
        "{input_event_type}"
    }

    fn output_event_type(&self) -> &'static str {
        "{output_event_type}"
    }

    async fn process(
        &mut self,
        state: &mut Self::State,
        input: Self::Input,
    ) -> Result<Option<Self::Output>, SimpleProcessorError> {
        // Implementation...
    }
}
```

Generate the complete implementation now:
"#;

/// Result of the generation process
#[derive(Debug, Clone)]
pub struct GeneratedNode {
    /// Name of the node
    pub name: String,
    /// Path to the generated crate
    pub path: Utf8PathBuf,
    /// Generated code content
    #[allow(dead_code)]
    pub code: String,
}

/// The node generator
pub struct NodeGenerator {
    config: GeneratorConfig,
    http_client: reqwest::Client,
}

impl NodeGenerator {
    /// Create a new node generator
    pub fn new(config: GeneratorConfig) -> Result<Self> {
        let http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_mins(2))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            config,
            http_client,
        })
    }

    /// Generate a SimpleProcessor from a spec
    pub async fn generate(&self, spec: &NodeSpec) -> Result<GeneratedNode> {
        println!(
            "[generate] Creating SimpleProcessor '{}' from spec...",
            spec.name
        );

        // Build the prompt
        let prompt = self.build_prompt(spec);

        // Call LLM
        let code = self.call_llm(&prompt).await?;

        // Extract just the Rust code from the response
        let code = self.extract_rust_code(&code)?;

        // Create the crate
        let crate_path = self.create_crate(spec, &code)?;

        println!(
            "[generate] Generated node '{}' at {}",
            spec.name, crate_path
        );

        Ok(GeneratedNode {
            name: spec.name.clone(),
            code,
            path: crate_path,
        })
    }

    /// Build the prompt (public for dry-run preview)
    pub fn build_prompt(&self, spec: &NodeSpec) -> String {
        SIMPLE_PROCESSOR_TEMPLATE.replace("{spec}", &spec.description)
    }

    async fn call_llm(&self, prompt: &str) -> Result<String> {
        // Determine if using Anthropic or OpenAI based on endpoint
        if self.config.llm_endpoint.contains("anthropic") {
            self.call_anthropic(prompt).await
        } else {
            self.call_openai(prompt).await
        }
    }

    async fn call_anthropic(&self, prompt: &str) -> Result<String> {
        #[derive(Serialize)]
        struct AnthropicRequest {
            model: String,
            max_tokens: u32,
            messages: Vec<AnthropicMessage>,
        }

        #[derive(Serialize)]
        struct AnthropicMessage {
            role: String,
            content: String,
        }

        #[derive(Deserialize)]
        struct AnthropicResponse {
            content: Vec<AnthropicContent>,
        }

        #[derive(Deserialize)]
        struct AnthropicContent {
            text: String,
        }

        let request = AnthropicRequest {
            model: self.config.model.clone(),
            max_tokens: 4096,
            messages: vec![AnthropicMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            }],
        };

        let response = self
            .http_client
            .post(&self.config.llm_endpoint)
            .header("Content-Type", "application/json")
            .header("x-api-key", &self.config.llm_api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&request)
            .send()
            .await
            .context("Failed to call Anthropic API")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("Anthropic API error {}: {}", status, body);
        }

        let response: AnthropicResponse = response
            .json()
            .await
            .context("Failed to parse Anthropic response")?;

        response
            .content
            .first()
            .map(|c| c.text.clone())
            .ok_or_else(|| anyhow::anyhow!("Empty response from Anthropic"))
    }

    async fn call_openai(&self, prompt: &str) -> Result<String> {
        #[derive(Serialize)]
        struct OpenAIRequest {
            model: String,
            messages: Vec<OpenAIMessage>,
            max_tokens: u32,
        }

        #[derive(Serialize)]
        struct OpenAIMessage {
            role: String,
            content: String,
        }

        #[derive(Deserialize)]
        struct OpenAIResponse {
            choices: Vec<OpenAIChoice>,
        }

        #[derive(Deserialize)]
        struct OpenAIChoice {
            message: OpenAIMessageResponse,
        }

        #[derive(Deserialize)]
        struct OpenAIMessageResponse {
            content: String,
        }

        let request = OpenAIRequest {
            model: self.config.model.clone(),
            messages: vec![OpenAIMessage {
                role: "user".to_string(),
                content: prompt.to_string(),
            }],
            max_tokens: 4096,
        };

        let response = self
            .http_client
            .post(&self.config.llm_endpoint)
            .header("Content-Type", "application/json")
            .header(
                "Authorization",
                format!("Bearer {}", self.config.llm_api_key),
            )
            .json(&request)
            .send()
            .await
            .context("Failed to call OpenAI API")?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            bail!("OpenAI API error {}: {}", status, body);
        }

        let response: OpenAIResponse = response
            .json()
            .await
            .context("Failed to parse OpenAI response")?;

        response
            .choices
            .first()
            .map(|c| c.message.content.clone())
            .ok_or_else(|| anyhow::anyhow!("Empty response from OpenAI"))
    }

    fn extract_rust_code(&self, response: &str) -> Result<String> {
        // Find code blocks in the response
        if let Some(start) = response.find("```rust") {
            let code_start = start + 7; // len("```rust")
            if let Some(end) = response[code_start..].find("```") {
                return Ok(response[code_start..code_start + end].trim().to_string());
            }
        }

        // If no code blocks, check if it looks like Rust code directly
        if response.contains("impl SimpleProcessor") {
            return Ok(response.trim().to_string());
        }

        bail!(
            "Could not extract Rust code from LLM response. Response:\n{}",
            &response[..response.len().min(500)]
        )
    }

    fn create_crate(&self, spec: &NodeSpec, code: &str) -> Result<Utf8PathBuf> {
        let crate_name = format!("sinex-{}", spec.name);
        let crate_path = self
            .config
            .workspace_root
            .join(&self.config.output_dir)
            .join(&crate_name);

        // Create directory structure
        std::fs::create_dir_all(crate_path.join("src"))
            .with_context(|| format!("Failed to create crate directory: {}", crate_path))?;

        // Write Cargo.toml
        let cargo_toml = format!(
            r#"[package]
name = "{crate_name}"
version = "0.1.0"
edition = "2021"
description = "Generated SimpleProcessor: {description}"
license = "MIT"

[dependencies]
async-trait = "0.1"
serde = {{ version = "1", features = ["derive"] }}
serde_json = "1"
sinex-primitives = {{ path = "../../lib/sinex-primitives" }}
sinex-node-sdk = {{ path = "../../lib/sinex-node-sdk" }}
tokio = {{ version = "1", features = ["full"] }}
tracing = "0.1"
"#,
            crate_name = crate_name,
            description = spec.description.replace('"', "'"),
        );
        std::fs::write(crate_path.join("Cargo.toml"), cargo_toml)
            .context("Failed to write Cargo.toml")?;

        // Write lib.rs
        std::fs::write(crate_path.join("src/lib.rs"), code).context("Failed to write lib.rs")?;

        Ok(crate_path)
    }
}

/// Arguments for the generate command
#[derive(Debug, Clone)]
pub struct GenerateArgs {
    /// Natural language specification
    pub spec: String,
    /// Optional explicit name for the node
    pub name: Option<String>,
    /// Dry run (don't create files, just show what would be generated)
    pub dry_run: bool,
}

/// Run the generate command
pub async fn run_generate(args: GenerateArgs, workspace_root: Utf8PathBuf) -> Result<()> {
    let config = GeneratorConfig::from_env(workspace_root)?;
    let generator = NodeGenerator::new(config)?;
    let spec = NodeSpec::parse(&args.spec, args.name.as_deref());

    println!(
        "[generate] Creating SimpleProcessor '{}' from: \"{}\"",
        spec.name, spec.description
    );

    if args.dry_run {
        println!("\n--- DRY RUN ---");
        println!("Would generate node: {}", spec.name);
        println!("Input type: {:?}", spec.input_type);
        println!("Output type: {:?}", spec.output_type);
        println!("\nPrompt preview:\n{}", generator.build_prompt(&spec));
        return Ok(());
    }

    let result = generator.generate(&spec).await?;

    println!("\nGenerated node: {}", result.name);
    println!("Created at: {}", result.path);
    println!("\nNext steps:");
    println!("  1. Add to Cargo.toml workspace members");
    println!("  2. Run: cargo build -p sinex-{}", result.name);
    println!("  3. Start with: cargo xtask dev run sinex-{}", result.name);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_spec_parsing() {
        let spec = NodeSpec::parse("detect git commands from terminal events", None);
        assert!(spec.name.contains("git") || spec.name.contains("detect"));
        assert_eq!(
            spec.input_type,
            Some("terminal.command.executed".to_string())
        );
    }

    #[test]
    fn test_derive_name() {
        assert_eq!(
            NodeSpec::derive_name_from_spec("detect git activity"),
            "detect-git-activity"
        );
        assert_eq!(
            NodeSpec::derive_name_from_spec("track file changes"),
            "track-file-changes"
        );
    }
}
