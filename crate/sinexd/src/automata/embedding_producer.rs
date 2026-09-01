//! Embedding producer — Transducer for document chunk embeddings (#1076).

use crate::runtime::automaton::{AutomatonContext, DerivedOutput, TransducerAdapter};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, Transducer};
use async_trait::async_trait;
use serde_json::Value as JsonValue;
use sinex_db::repositories::{DbPoolExt, EmbeddingTarget, EventEmbeddingRow};
use sinex_primitives::derivation::{
    ClaimSupportTemplate, DerivationOutputDeclaration, DerivationWriteSurface, DerivedProductClass,
    InputEligibility,
};
use sinex_primitives::llm::{ModelEffectRequest, hash_model_input};
use sinex_primitives::privacy::{PrivacyConfig, PrivacyEngine, ProcessingContext};
use sinex_primitives::sources::{SourceRole, source_role};
use sinex_primitives::{Result, SinexError, Uuid};
use std::collections::{BTreeMap, BTreeSet};

const DEFAULT_MAX_EVENTS_PER_RUN: u64 = 1_000;
const DEFAULT_MAX_MATERIALS_PER_RUN: u64 = 100;
const DEFAULT_MAX_ESTIMATED_TOKENS: u64 = 100_000;
const DEFAULT_MAX_ESTIMATED_COST_MICROUSD: u64 = 500_000;
const DEFAULT_ESTIMATED_COST_PER_1K_TOKENS_MICROUSD: u64 = 0;
const DEFAULT_MODEL_PROVIDER: &str = "ollama";
const DEFAULT_MODEL: &str = "bge-base-en-v1.5";
const DEFAULT_MODEL_DIMENSIONS: i32 = 768;

/// The first-run embedding rule is deliberately fail-closed. New event or
/// material families do not become provider inputs merely by appearing in the
/// database.
const DEFAULT_ALLOWED_EVENT_TYPES: &[&str] = &["document.chunked"];
const DEFAULT_ALLOWED_SOURCE_FAMILIES: &[&str] = &["document-parser"];
const DEFAULT_ALLOWED_MATERIAL_PREFIXES: &[&str] = &["document"];

/// Provider boundary for one real embedding batch. The worker owns all
/// selection and fuse checks; implementations only receive already-approved
/// text after the complete preflight has passed.
#[async_trait]
pub trait EmbeddingProvider: Send + Sync + 'static {
    fn provider(&self) -> &str;
    fn model(&self) -> &str;
    fn dimensions(&self) -> i32;

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>>;
}

/// Local Ollama provider used by the production embedding route.
#[derive(Clone)]
pub struct OllamaEmbeddingProvider {
    http: reqwest::Client,
    base_url: String,
    model: String,
    dimensions: i32,
}

impl OllamaEmbeddingProvider {
    #[must_use]
    pub fn new(base_url: impl Into<String>, model: impl Into<String>, dimensions: i32) -> Self {
        Self {
            http: reqwest::Client::new(),
            base_url: base_url.into(),
            model: model.into(),
            dimensions,
        }
    }
}

#[derive(serde::Deserialize)]
struct OllamaEmbeddingResponse {
    embeddings: Vec<Vec<f32>>,
}

#[async_trait]
impl EmbeddingProvider for OllamaEmbeddingProvider {
    fn provider(&self) -> &str {
        "ollama"
    }

    fn model(&self) -> &str {
        &self.model
    }

    fn dimensions(&self) -> i32 {
        self.dimensions
    }

    async fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>> {
        let endpoint = format!("{}/api/embed", self.base_url.trim_end_matches('/'));
        let response = self
            .http
            .post(endpoint)
            .json(&serde_json::json!({
                "model": self.model,
                "input": texts,
            }))
            .send()
            .await
            .map_err(|error| {
                SinexError::io("embedding provider request failed").with_std_error(&error)
            })?
            .error_for_status()
            .map_err(|error| {
                SinexError::io("embedding provider returned an error").with_std_error(&error)
            })?
            .json::<OllamaEmbeddingResponse>()
            .await
            .map_err(|error| {
                SinexError::serialization("invalid embedding provider response")
                    .with_std_error(&error)
            })?;

        Ok(response.embeddings)
    }
}

/// Static limits and the first-activation allowlist for the worker.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EmbeddingWorkerConfig {
    pub max_events_per_run: u64,
    pub max_materials_per_run: u64,
    pub allowed_models: Vec<String>,
    pub allowed_event_types: Vec<String>,
    pub allowed_source_families: Vec<String>,
    pub allowed_material_source_prefixes: Vec<String>,
    pub max_estimated_tokens: u64,
    pub max_estimated_cost_microusd: u64,
    pub estimated_cost_per_1k_tokens_microusd: u64,
    pub model_provider: String,
    pub model: String,
    pub model_dimensions: i32,
    pub batch_size: u64,
    pub ollama_base_url: String,
}

impl Default for EmbeddingWorkerConfig {
    fn default() -> Self {
        Self {
            max_events_per_run: DEFAULT_MAX_EVENTS_PER_RUN,
            max_materials_per_run: DEFAULT_MAX_MATERIALS_PER_RUN,
            allowed_models: vec![format!("{DEFAULT_MODEL_PROVIDER}/{DEFAULT_MODEL}")],
            allowed_event_types: DEFAULT_ALLOWED_EVENT_TYPES
                .iter()
                .map(|s| (*s).into())
                .collect(),
            allowed_source_families: DEFAULT_ALLOWED_SOURCE_FAMILIES
                .iter()
                .map(|s| (*s).into())
                .collect(),
            allowed_material_source_prefixes: DEFAULT_ALLOWED_MATERIAL_PREFIXES
                .iter()
                .map(|s| (*s).into())
                .collect(),
            max_estimated_tokens: DEFAULT_MAX_ESTIMATED_TOKENS,
            max_estimated_cost_microusd: DEFAULT_MAX_ESTIMATED_COST_MICROUSD,
            estimated_cost_per_1k_tokens_microusd: DEFAULT_ESTIMATED_COST_PER_1K_TOKENS_MICROUSD,
            model_provider: DEFAULT_MODEL_PROVIDER.into(),
            model: DEFAULT_MODEL.into(),
            model_dimensions: DEFAULT_MODEL_DIMENSIONS,
            batch_size: 32,
            ollama_base_url: "http://127.0.0.1:11434".into(),
        }
    }
}

impl EmbeddingWorkerConfig {
    /// Load the deployment configuration without silently weakening a limit on
    /// malformed input. The NixOS unit may provide these as environment
    /// variables; direct construction remains useful for tests and RPC plans.
    pub fn from_env() -> Result<Self> {
        let defaults = Self::default();
        Ok(Self {
            max_events_per_run: env_u64("SINEX_EMBEDDING_MAX_EVENTS", defaults.max_events_per_run)?,
            max_materials_per_run: env_u64(
                "SINEX_EMBEDDING_MAX_MATERIALS",
                defaults.max_materials_per_run,
            )?,
            allowed_models: env_csv("SINEX_EMBEDDING_ALLOWED_MODELS", defaults.allowed_models),
            allowed_event_types: env_csv(
                "SINEX_EMBEDDING_ALLOWED_EVENT_TYPES",
                defaults.allowed_event_types,
            ),
            allowed_source_families: env_csv(
                "SINEX_EMBEDDING_ALLOWED_SOURCE_FAMILIES",
                defaults.allowed_source_families,
            ),
            allowed_material_source_prefixes: env_csv(
                "SINEX_EMBEDDING_ALLOWED_MATERIAL_PREFIXES",
                defaults.allowed_material_source_prefixes,
            ),
            max_estimated_tokens: env_u64(
                "SINEX_EMBEDDING_MAX_ESTIMATED_TOKENS",
                defaults.max_estimated_tokens,
            )?,
            max_estimated_cost_microusd: env_u64(
                "SINEX_EMBEDDING_MAX_ESTIMATED_COST_MICROUSD",
                defaults.max_estimated_cost_microusd,
            )?,
            estimated_cost_per_1k_tokens_microusd: env_u64(
                "SINEX_EMBEDDING_ESTIMATED_COST_PER_1K_TOKENS_MICROUSD",
                defaults.estimated_cost_per_1k_tokens_microusd,
            )?,
            model_provider: env_string("SINEX_EMBEDDING_MODEL_PROVIDER", defaults.model_provider),
            model: env_string("SINEX_EMBEDDING_MODEL", defaults.model),
            model_dimensions: env_u64(
                "SINEX_EMBEDDING_MODEL_DIMENSIONS",
                defaults.model_dimensions as u64,
            )?
            .try_into()
            .map_err(|_| SinexError::validation("SINEX_EMBEDDING_MODEL_DIMENSIONS is too large"))?,
            batch_size: env_u64("SINEX_EMBEDDING_BATCH_SIZE", defaults.batch_size)?,
            ollama_base_url: env_string(
                "SINEX_EMBEDDING_OLLAMA_BASE_URL",
                defaults.ollama_base_url,
            ),
        })
    }

    fn model_key(&self) -> String {
        format!("{}/{}", self.model_provider, self.model)
    }
}

fn env_string(name: &str, default: String) -> String {
    std::env::var(name).unwrap_or(default)
}

fn env_csv(name: &str, default: Vec<String>) -> Vec<String> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_owned)
                .collect()
        })
        .filter(|values: &Vec<String>| !values.is_empty())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value.parse::<u64>().map_err(|error| {
                SinexError::validation(format!("{name} must be an unsigned integer"))
                    .with_std_error(&error)
            })
        })
        .transpose()
        .map(|value| value.unwrap_or(default))
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EmbeddingRunEstimate {
    pub scanned_events: u64,
    pub scanned_materials: u64,
    pub eligible_events: u64,
    pub quarantined_events: u64,
    pub skipped_events: u64,
    pub estimated_tokens: u64,
    pub estimated_cost_microusd: u64,
    pub selected_event_types: BTreeMap<String, u64>,
    pub quarantined_by_reason: BTreeMap<String, u64>,
    pub model: String,
    pub model_allowed: bool,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct EmbeddingRunReport {
    pub estimate: EmbeddingRunEstimate,
    pub embedded_events: u64,
}

struct EmbeddingPlan {
    estimate: EmbeddingRunEstimate,
    eligible: Vec<EmbeddingTarget>,
}

/// Production embedding worker. It is intentionally separate from the
/// receipt-only transducer below: the worker owns the DB backfill, the
/// provider call, cache/store persistence, and the preflight boundary.
pub struct EmbeddingWorker<P> {
    provider: P,
    config: EmbeddingWorkerConfig,
    privacy: PrivacyEngine,
}

impl<P: EmbeddingProvider> EmbeddingWorker<P> {
    pub fn new(provider: P, config: EmbeddingWorkerConfig) -> Result<Self> {
        if config.max_events_per_run == 0
            || config.max_materials_per_run == 0
            || config.max_estimated_tokens == 0
            || config.batch_size == 0
        {
            return Err(SinexError::validation(
                "embedding worker hard limits and batch size must be positive",
            ));
        }
        let privacy = PrivacyEngine::new(PrivacyConfig::default()).map_err(|error| {
            SinexError::validation("embedding privacy policy failed to compile")
                .with_std_error(&error)
        })?;
        Ok(Self {
            provider,
            config,
            privacy,
        })
    }

    #[must_use]
    pub fn config(&self) -> &EmbeddingWorkerConfig {
        &self.config
    }

    pub async fn estimate(&self, pool: &sqlx::PgPool) -> Result<EmbeddingRunEstimate> {
        self.validate_model_allowlist()?;
        let model_id = pool
            .embeddings()
            .get_active_model(self.provider.provider(), self.provider.model())
            .await?
            .map(|model| model.id)
            .unwrap_or_else(Uuid::nil);
        let targets = self.load_targets(pool, model_id).await?;
        Ok(self.plan(targets)?.estimate)
    }

    pub async fn run_once(&self, pool: &sqlx::PgPool) -> Result<EmbeddingRunReport> {
        self.validate_model_allowlist()?;
        let model_id = pool
            .embeddings()
            .ensure_model(
                self.provider.provider(),
                self.provider.model(),
                self.provider.dimensions(),
            )
            .await?;
        let targets = self.load_targets(pool, model_id).await?;
        let plan = self.plan(targets)?;
        self.execute_plan(pool, model_id, plan).await
    }

    async fn load_targets(
        &self,
        pool: &sqlx::PgPool,
        model_id: Uuid,
    ) -> Result<Vec<EmbeddingTarget>> {
        let limit = self
            .config
            .max_events_per_run
            .saturating_add(1)
            .try_into()
            .map_err(|_| SinexError::validation("embedding event limit is too large"))?;
        pool.embeddings()
            .events_without_embeddings(
                model_id,
                &self
                    .config
                    .allowed_event_types
                    .iter()
                    .map(String::as_str)
                    .collect::<Vec<_>>(),
                limit,
            )
            .await
    }

    fn validate_model_allowlist(&self) -> Result<()> {
        if !self
            .config
            .allowed_models
            .iter()
            .any(|model| model == &self.config.model_key())
        {
            return Err(SinexError::validation(
                "embedding model is not on the configured allowlist",
            )
            .with_context("model", self.config.model_key()));
        }
        Ok(())
    }

    fn plan(&self, targets: Vec<EmbeddingTarget>) -> Result<EmbeddingPlan> {
        let mut material_ids = BTreeSet::new();
        for target in &targets {
            if let Some(material_id) = target.source_material_id {
                material_ids.insert(material_id);
            }
        }
        if targets.len() as u64 > self.config.max_events_per_run {
            return Err(
                SinexError::validation("embedding run exceeds max events ceiling")
                    .with_context("observed", targets.len())
                    .with_context("ceiling", self.config.max_events_per_run),
            );
        }
        if material_ids.len() as u64 > self.config.max_materials_per_run {
            return Err(
                SinexError::validation("embedding run exceeds max materials ceiling")
                    .with_context("observed", material_ids.len())
                    .with_context("ceiling", self.config.max_materials_per_run),
            );
        }

        let mut eligible = Vec::new();
        let mut selected_event_types = BTreeMap::new();
        let mut quarantined_by_reason = BTreeMap::new();
        let mut skipped_events = 0_u64;
        for target in targets {
            let reason = if is_agent_session_target(&target) {
                Some("agent_session")
            } else if source_role(&target.source) == SourceRole::Reflection {
                Some("reflection_lane")
            } else if private_material(&target) {
                Some("private_material")
            } else if lifecycle_quarantined(&target) {
                Some("quarantine_until_reviewed")
            } else if self
                .privacy
                .should_suppress(&target.text_for_embedding, ProcessingContext::Document)
            {
                Some("privacy_suppressed")
            } else if !self
                .config
                .allowed_event_types
                .iter()
                .any(|value| value == &target.event_type)
            {
                None
            } else if !self.source_family_allowed(&target) {
                None
            } else {
                eligible.push(target);
                continue;
            };

            if let Some(reason) = reason {
                *quarantined_by_reason.entry(reason.to_string()).or_default() += 1;
            } else {
                skipped_events += 1;
            }
        }

        let estimated_tokens = eligible
            .iter()
            .map(|target| estimate_tokens(&target.text_for_embedding))
            .sum::<u64>();
        let estimated_cost_microusd = estimated_tokens
            .saturating_mul(self.config.estimated_cost_per_1k_tokens_microusd)
            .div_ceil(1_000);
        if estimated_tokens > self.config.max_estimated_tokens {
            return Err(
                SinexError::validation("embedding run exceeds estimated token ceiling")
                    .with_context("estimated_tokens", estimated_tokens)
                    .with_context("ceiling", self.config.max_estimated_tokens),
            );
        }
        if estimated_cost_microusd > self.config.max_estimated_cost_microusd {
            return Err(
                SinexError::validation("embedding run exceeds estimated cost ceiling")
                    .with_context("estimated_cost_microusd", estimated_cost_microusd)
                    .with_context("ceiling", self.config.max_estimated_cost_microusd),
            );
        }
        for target in &eligible {
            *selected_event_types
                .entry(target.event_type.clone())
                .or_default() += 1;
        }

        Ok(EmbeddingPlan {
            estimate: EmbeddingRunEstimate {
                scanned_events: eligible.len() as u64
                    + skipped_events
                    + quarantined_by_reason.values().sum::<u64>(),
                scanned_materials: material_ids.len() as u64,
                eligible_events: eligible.len() as u64,
                quarantined_events: quarantined_by_reason.values().sum(),
                skipped_events,
                estimated_tokens,
                estimated_cost_microusd,
                selected_event_types,
                quarantined_by_reason,
                model: self.config.model_key(),
                model_allowed: true,
            },
            eligible,
        })
    }

    fn source_family_allowed(&self, target: &EmbeddingTarget) -> bool {
        self.config.allowed_source_families.iter().any(|family| {
            target.source == *family || target.source.starts_with(&format!("{family}."))
        }) || target
            .material_source_identifier
            .as_deref()
            .is_some_and(|source| {
                self.config
                    .allowed_material_source_prefixes
                    .iter()
                    .any(|prefix| source == prefix || source.starts_with(&format!("{prefix}.")))
            })
    }

    async fn execute_plan(
        &self,
        pool: &sqlx::PgPool,
        model_id: Uuid,
        plan: EmbeddingPlan,
    ) -> Result<EmbeddingRunReport> {
        let mut embedded_events = 0_u64;
        for batch in plan.eligible.chunks(self.config.batch_size as usize) {
            let texts: Vec<String> = batch
                .iter()
                .map(|target| target.text_for_embedding.clone())
                .collect();
            let vectors = self.provider.embed_batch(&texts).await?;
            if vectors.len() != batch.len() {
                return Err(SinexError::processing(
                    "embedding provider returned the wrong batch size",
                )
                .with_context("expected", batch.len())
                .with_context("actual", vectors.len()));
            }
            let rows = batch
                .iter()
                .zip(vectors)
                .map(|(target, embedding)| {
                    if embedding.len() != self.provider.dimensions() as usize
                        || embedding.iter().any(|value| !value.is_finite())
                    {
                        return Err(SinexError::processing(
                            "embedding provider returned an invalid vector",
                        )
                        .with_context("model", self.config.model_key())
                        .with_context("expected_dimensions", self.provider.dimensions())
                        .with_context("actual_dimensions", embedding.len()));
                    }
                    Ok(EventEmbeddingRow {
                        event_id: target.event_id,
                        model_id,
                        embedded_text: target.text_for_embedding.clone(),
                        embedding,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            embedded_events += pool.embeddings().insert_event_embeddings(&rows).await?;
        }
        Ok(EmbeddingRunReport {
            estimate: plan.estimate,
            embedded_events,
        })
    }
}

impl EmbeddingWorker<OllamaEmbeddingProvider> {
    pub fn from_env() -> Result<Self> {
        let config = EmbeddingWorkerConfig::from_env()?;
        let provider = OllamaEmbeddingProvider::new(
            config.ollama_base_url.clone(),
            config.model.clone(),
            config.model_dimensions,
        );
        Self::new(provider, config)
    }
}

fn estimate_tokens(text: &str) -> u64 {
    ((text.len() as u64).saturating_add(3) / 4).max(1)
}

fn is_agent_session_target(target: &EmbeddingTarget) -> bool {
    let source = target.source.to_ascii_lowercase();
    let material = target
        .material_source_identifier
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    source == "integration.polylogue"
        || source.starts_with("polylogue.")
        || source == "claude"
        || source == "chatgpt"
        || source == "codex"
        || source.starts_with("ai-session")
        || material.contains("polylogue")
        || material.contains("ai-session")
}

fn private_material(target: &EmbeddingTarget) -> bool {
    matches!(
        target.material_privacy_class.as_deref(),
        Some("personal" | "secret" | "redacted")
    )
}

fn lifecycle_quarantined(target: &EmbeddingTarget) -> bool {
    let Some(metadata) = target.material_metadata.as_ref() else {
        return false;
    };
    [
        metadata
            .get("material_lifecycle")
            .and_then(JsonValue::as_str),
        metadata.get("lifecycle").and_then(JsonValue::as_str),
        metadata
            .get("source_material_contract")
            .and_then(|value| value.get("lifecycle"))
            .and_then(JsonValue::as_str),
    ]
    .into_iter()
    .flatten()
    .any(|value| value == "quarantine_until_reviewed" || value == "quarantine")
}

/// Derivation control-plane declaration for `embedding-producer` (sinex-0vx.1/0vx.3).
///
/// `report_artifact`: the current output is an effect-key/replay-policy
/// receipt for a chunk embedding, not the embedding vector itself — never a
/// default-eligible input to further canonical derivation. Support defaults
/// to the doctrine-mandated unknown/low baseline (`ClaimSupportTemplate::UNKNOWN`)
/// rather than a fabricated `model_inferred` grade until the real vector is
/// recorded (see blueprint `04-interpretation-plane-blueprint.report.md`
/// per-automaton classification table).
pub const EMBEDDING_PRODUCER_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "embedding-producer.document.embedded",
        owner: "embedding-producer",
        product_class: DerivedProductClass::ReportArtifact,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("embedding-producer"),
        output_event_type: Some("document.embedded"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::NeverInput,
        default_support: ClaimSupportTemplate::UNKNOWN,
        verification_command: "xtask test -p sinexd -E 'test(embedding_producer)'",
    }];

#[derive(Default)]
pub struct EmbeddingProducer;

impl Transducer for EmbeddingProducer {
    type State = ();
    type Input = JsonValue;
    type Output = JsonValue;

    fn name(&self) -> &'static str {
        "embedding-producer"
    }
    fn input_event_type(&self) -> &'static str {
        "document.chunked"
    }
    fn output_event_type(&self) -> &'static str {
        "document.embedded"
    }
    fn output_event_source(&self) -> &'static str {
        "embedding-producer"
    }
    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::Any
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] =
        EMBEDDING_PRODUCER_OUTPUT_DECLARATIONS;

    async fn process(
        &mut self,
        _state: &mut Self::State,
        input: Self::Input,
        ctx: &AutomatonContext,
    ) -> std::result::Result<Option<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let chunk_id = input
            .get("chunk_id")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let chunk_hash = input
            .get("chunk_hash")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let document_id = input
            .get("document_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let input_hash = hash_model_input(&input.to_string());
        let request = ModelEffectRequest {
            provider: "local".into(),
            model: "embedding".into(),
            prompt_hash: chunk_hash.into(),
            schema_hash: None,
            input_hash,
        };

        let ts_orig = ctx.require_ts_orig()?;
        let source_id = ctx.trigger_uuid();
        let declaration = &EMBEDDING_PRODUCER_OUTPUT_DECLARATIONS[0];
        Ok(Some(
            DerivedOutput::transduced(
                serde_json::json!({
                    "chunk_id": chunk_id,
                    "document_id": document_id,
                    "chunk_hash": chunk_hash,
                    "effect_key": request.composite_key(),
                    "replay_policy": "reuse_recorded",
                }),
                ts_orig,
                source_id,
            )
            .with_declaration_id(declaration.declaration_id)
            .with_product_class(declaration.product_class)
            // Zero evidence counts: this is a receipt, not the embedding vector
            // itself (see the declaration's doc — ClaimSupportTemplate::UNKNOWN
            // until sinex-5v6 lands real model effects).
            .with_claim_support(declaration.default_support.instantiate(0, 0, 0, 0))
            .with_semantics_version(declaration.semantics_version)
            .with_derived_equivalence_key(declaration, chunk_id),
        ))
    }
}

pub type EmbeddingProducerRuntime = TransducerAdapter<EmbeddingProducer>;

#[cfg(test)]
#[path = "embedding_producer_test.rs"]
mod tests;
