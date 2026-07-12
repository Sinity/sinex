//! Health aggregator — [`ScopeReconciler`] implementation.
//!
//! Model classification: **`ScopeReconciler`** — groups health events by component
//! (the scope key), maintains per-component state, and emits reports when
//! conditions are met (status transitions, periodic intervals). During replay,
//! invalidating a component scope recomputes all health reports for that component.

use crate::runtime::automaton::{AutomatonContext, DerivedOutput, ScopeReconcilerAdapter};
use crate::runtime::{AutomatonLogicError, InputProvenanceFilter, ScopeReconciler};
use serde::{Deserialize, Serialize};
use sinex_primitives::derivation::{
    ClaimSupportTemplate, ClaimTemporalQuality, DerivationOutputDeclaration,
    DerivationWriteSurface, DerivedProductClass, InputEligibility, SourceCoverage, SupportLevel,
};
use sinex_primitives::domain::{HealthStatus, SyntheticTemporalPolicy};
use sinex_primitives::events::{
    EventPayload,
    payloads::{
        HealthAggregatedAlertPayload, HealthAggregatedComponentReportPayload,
        HealthAggregatedReportPayload, HealthAggregatedReportType,
        HealthAggregatedSystemStatusPayload, HealthAlertSeverity, HealthAlertType,
        HealthComponentSnapshot,
    },
};
use sinex_primitives::temporal::{Duration, Timestamp};
use sinex_primitives::{JsonValue, Uuid};
use std::collections::HashMap;
use std::str::FromStr;
use tracing::warn;

/// Configuration for the health aggregator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthAggregatorConfig {
    /// Component-specific check intervals (in seconds)
    #[serde(default = "default_component_intervals")]
    pub component_check_intervals: HashMap<String, u64>,

    /// Aggregation window for health statistics (in seconds)
    #[serde(default = "default_aggregation_window_secs")]
    pub aggregation_window_seconds: u64,

    /// Threshold for marking component as unhealthy (in minutes)
    #[serde(default = "default_unhealthy_threshold")]
    pub unhealthy_threshold_minutes: u64,

    /// Enable system-wide health status emission
    #[serde(default = "default_true")]
    pub enable_system_health_status: bool,

    /// Enable per-component health reports
    #[serde(default = "default_true")]
    pub enable_component_health_reports: bool,
}

fn default_component_intervals() -> HashMap<String, u64> {
    let mut intervals = HashMap::new();
    intervals.insert("default".to_string(), 60);
    intervals
}

fn default_aggregation_window_secs() -> u64 {
    300
}

fn default_unhealthy_threshold() -> u64 {
    5
}

fn default_true() -> bool {
    true
}

impl Default for HealthAggregatorConfig {
    fn default() -> Self {
        Self {
            component_check_intervals: default_component_intervals(),
            aggregation_window_seconds: default_aggregation_window_secs(),
            unhealthy_threshold_minutes: default_unhealthy_threshold(),
            enable_system_health_status: default_true(),
            enable_component_health_reports: default_true(),
        }
    }
}

impl HealthAggregatorConfig {
    /// Load configuration from environment variables.
    #[must_use]
    pub fn from_env() -> Self {
        let mut config = Self::default();
        apply_env_override(
            "SINEX_HEALTH_AGGREGATOR_AGGREGATION_WINDOW_SECONDS",
            &mut config.aggregation_window_seconds,
        );
        apply_env_override(
            "SINEX_HEALTH_AGGREGATOR_UNHEALTHY_THRESHOLD_MINUTES",
            &mut config.unhealthy_threshold_minutes,
        );
        apply_env_override(
            "SINEX_HEALTH_AGGREGATOR_ENABLE_SYSTEM_HEALTH_STATUS",
            &mut config.enable_system_health_status,
        );
        apply_env_override(
            "SINEX_HEALTH_AGGREGATOR_ENABLE_COMPONENT_HEALTH_REPORTS",
            &mut config.enable_component_health_reports,
        );

        match std::env::var("SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS") {
            Ok(value) => match serde_json::from_str::<HashMap<String, u64>>(&value) {
                Ok(intervals) => match validate_component_intervals(intervals) {
                    Ok(validated) => config.component_check_intervals = validated,
                    Err(error) => {
                        warn!(
                            env = "SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS",
                            value = %value,
                            %error,
                            "Invalid component check interval override; using default"
                        );
                    }
                },
                Err(error) => {
                    warn!(
                        env = "SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS",
                        value = %value,
                        %error,
                        "Invalid component check interval override; using default"
                    );
                }
            },
            Err(std::env::VarError::NotPresent) => {}
            Err(std::env::VarError::NotUnicode(_)) => {
                warn!(
                    env = "SINEX_HEALTH_AGGREGATOR_COMPONENT_CHECK_INTERVALS",
                    "Component check interval override is not valid UTF-8; using default"
                );
            }
        }

        config
    }
}

fn apply_env_override<T>(key: &str, target: &mut T)
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    if let Some(parsed) = sinex_primitives::env::parse_optional::<T>(key, "health automaton config")
    {
        *target = parsed;
    }
}

fn validate_component_intervals(
    intervals: HashMap<String, u64>,
) -> Result<HashMap<String, u64>, String> {
    for (component, interval) in &intervals {
        if component.trim().is_empty() {
            return Err("component interval override contains an empty component name".to_string());
        }
        if *interval == 0 {
            return Err(format!(
                "component interval override for '{component}' must be positive"
            ));
        }
    }
    Ok(intervals)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HealthState {
    pub component_health: HashMap<String, ComponentHealth>,
    pub last_window_emission: Option<Timestamp>,
    pub config: HealthAggregatorConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub component_name: String,
    pub current_status: HealthStatus,
    pub status_since: Timestamp,
    pub last_seen: Timestamp,
    pub last_check_emission: Option<Timestamp>,
    pub transition_count: u64,
    pub events: Vec<HealthEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthEvent {
    pub timestamp: Timestamp,
    pub previous_status: HealthStatus,
    pub current_status: HealthStatus,
    pub event_id: String,
}

// HealthStatus is imported from sinex_primitives::domain; no local alias needed.

/// Derivation control-plane declaration for `health` (sinex-0vx.1/0vx.3).
///
/// `analysis_claim`: an aggregated evaluation over per-component health
/// signals, not default-eligible as canonical derivation input (a health
/// report about the system is not itself a fact about user activity).
pub const HEALTH_OUTPUT_DECLARATIONS: &[DerivationOutputDeclaration] =
    &[DerivationOutputDeclaration {
        declaration_id: "health.health.aggregated_report",
        owner: "health",
        product_class: DerivedProductClass::AnalysisClaim,
        write_surface: DerivationWriteSurface::DerivedOutput,
        output_source: Some("health-aggregator"),
        output_event_type: Some("health.aggregated_report"),
        projection_kind: None,
        artifact_kind: None,
        proposal_kind: None,
        semantics_version: "1.0.0",
        input_eligibility: InputEligibility::ExplicitOnly,
        default_support: ClaimSupportTemplate::new(
            SupportLevel::Convergent,
            SourceCoverage::Partial,
            ClaimTemporalQuality::DeclaredEffective,
        ),
        verification_command: "xtask test -p sinexd -E 'test(health)'",
    }];

#[derive(Default)]
pub struct HealthAggregator {
    pub config: HealthAggregatorConfig,
}

impl ScopeReconciler for HealthAggregator {
    type State = HealthState;
    type Input = JsonValue;
    type Output = HealthAggregatedReportPayload;

    fn name(&self) -> &'static str {
        "health-aggregator"
    }
    fn input_event_type(&self) -> &'static str {
        "health.status"
    }
    fn output_event_type(&self) -> &'static str {
        HealthAggregatedReportPayload::EVENT_TYPE.as_static_str()
    }

    fn output_event_source(&self) -> &'static str {
        HealthAggregatedReportPayload::SOURCE.as_static_str()
    }

    fn input_provenance_filter(&self) -> InputProvenanceFilter {
        InputProvenanceFilter::MaterialOnly
    }

    const OUTPUT_DECLARATIONS: &'static [DerivationOutputDeclaration] = HEALTH_OUTPUT_DECLARATIONS;

    fn scope_keys(&self, input: &Self::Input, context: &AutomatonContext) -> Vec<String> {
        // Keep malformed payloads isolated on the live path so they do not all collide into a
        // shared "unknown" scope before reconcile rejects them into DLQ.
        let component = input
            .get("component")
            .and_then(|v| v.as_str())
            .filter(|component| !component.trim().is_empty())
            .map_or_else(
                || format!("__invalid_component__:{}", context.trigger_uuid()),
                str::to_owned,
            );
        vec![component]
    }

    async fn reconcile(
        &mut self,
        state: &mut Self::State,
        scope_key: &str,
        input: Self::Input,
        context: &AutomatonContext,
    ) -> Result<Vec<DerivedOutput<Self::Output>>, AutomatonLogicError> {
        let now = context.require_ts_orig()?;
        let component = parse_component_name(&input)?.to_string();
        if component != scope_key {
            return Err(AutomatonLogicError::InputParsing(format!(
                "health status scope key '{scope_key}' does not match payload component '{component}'"
            )));
        }

        // Ensure state has config
        if state.config.aggregation_window_seconds == 0 {
            state.config = self.config.clone();
        }

        let previous_status = parse_health_status_field(&input, "previous_status")?;
        let current_status = parse_health_status_field(&input, "current_status")?;

        // Get or create component health tracking
        let mut immediate_alert = None;
        let mut component_report = None;
        {
            let component_health = state
                .component_health
                .entry(component.clone())
                .or_insert_with(|| ComponentHealth {
                    component_name: component.clone(),
                    current_status,
                    status_since: now,
                    last_seen: now,
                    last_check_emission: None,
                    transition_count: 0,
                    events: Vec::new(),
                });

            // Track status transition
            let status_changed = component_health.current_status != current_status;
            if status_changed {
                component_health.current_status = current_status;
                component_health.status_since = now;
                component_health.transition_count += 1;
            }

            component_health.last_seen = now;

            // Add event to sliding window
            component_health.events.push(HealthEvent {
                timestamp: now,
                previous_status,
                current_status,
                event_id: context.trigger_uuid().to_string(),
            });

            // Prune events outside aggregation window
            let window_start =
                now - Duration::seconds(state.config.aggregation_window_seconds as i64);
            component_health
                .events
                .retain(|e| e.timestamp >= window_start);

            let window_event_ids =
                Self::collect_event_ids(&component_health.events, &component, "component report")?;

            // Immediate alert for component failure
            if status_changed && matches!(current_status, HealthStatus::Unhealthy) {
                let declaration = &HEALTH_OUTPUT_DECLARATIONS[0];
                let evidence_event_count = window_event_ids.len() as u32;
                immediate_alert = Some(
                    DerivedOutput::reconciled(
                        self.create_alert(
                            &component,
                            current_status,
                            now,
                            "Component entered failed state",
                        ),
                        now,
                        window_event_ids.clone(),
                        component.clone(),
                    )
                    .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
                    .with_equivalence_key(format!("alert:{component}:{}", now.format_rfc3339()))
                    .with_declaration_id(declaration.declaration_id)
                    .with_product_class(declaration.product_class)
                    .with_claim_support(declaration.default_support.instantiate(
                        evidence_event_count,
                        0,
                        1,
                        0,
                    )),
                );
            }

            // Component-specific check interval
            let check_interval = state
                .config
                .component_check_intervals
                .get(&component)
                .or_else(|| state.config.component_check_intervals.get("default"))
                .copied()
                .unwrap_or(60);

            let should_emit_component = component_health.last_check_emission.is_none_or(|last| {
                let elapsed = now - last;
                elapsed >= Duration::seconds(check_interval as i64)
            });

            if should_emit_component && state.config.enable_component_health_reports {
                let declaration = &HEALTH_OUTPUT_DECLARATIONS[0];
                let evidence_event_count = window_event_ids.len() as u32;
                component_report = Some(
                    DerivedOutput::reconciled(
                        self.create_component_report(component_health, now),
                        now,
                        window_event_ids,
                        component.clone(),
                    )
                    .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
                    .with_declaration_id(declaration.declaration_id)
                    .with_product_class(declaration.product_class)
                    .with_claim_support(declaration.default_support.instantiate(
                        evidence_event_count,
                        0,
                        1,
                        0,
                    )),
                );
                component_health.last_check_emission = Some(now);
            }
        }

        // Check for periodic system-wide report after the current event has updated state.
        let mut periodic_reports = Vec::new();

        let should_emit_window = state.last_window_emission.is_none_or(|last| {
            let elapsed = now - last;
            elapsed >= Duration::seconds(state.config.aggregation_window_seconds as i64)
        });

        if should_emit_window && state.config.enable_system_health_status {
            let mut all_event_ids = Vec::new();
            for component_health in state.component_health.values() {
                all_event_ids.extend(Self::collect_event_ids(
                    &component_health.events,
                    &component_health.component_name,
                    "system status",
                )?);
            }
            let declaration = &HEALTH_OUTPUT_DECLARATIONS[0];
            let evidence_event_count = all_event_ids.len() as u32;
            periodic_reports.push(
                DerivedOutput::reconciled(
                    self.create_system_status(state, now),
                    now,
                    all_event_ids,
                    component.clone(),
                )
                .with_temporal_policy(SyntheticTemporalPolicy::DeclaredEffective)
                .with_declaration_id(declaration.declaration_id)
                .with_product_class(declaration.product_class)
                .with_claim_support(declaration.default_support.instantiate(
                    evidence_event_count,
                    0,
                    1,
                    0,
                )),
            );
            state.last_window_emission = Some(now);
        }

        if let Some(report) = component_report {
            periodic_reports.push(report);
        }

        let mut outputs =
            Vec::with_capacity(periodic_reports.len() + usize::from(immediate_alert.is_some()));
        if let Some(alert) = immediate_alert {
            outputs.push(alert);
        }
        outputs.extend(periodic_reports);
        Ok(outputs)
    }
}

impl HealthAggregator {
    fn collect_event_ids(
        events: &[HealthEvent],
        component: &str,
        report_kind: &str,
    ) -> Result<Vec<Uuid>, AutomatonLogicError> {
        events
            .iter()
            .map(|event| {
                Uuid::parse_str(&event.event_id).map_err(|error| {
                    AutomatonLogicError::Processing(format!(
                        "health aggregator {report_kind} for component '{component}' contains invalid event_id '{}': {error}",
                        event.event_id
                    ))
                })
            })
            .collect()
    }

    fn create_alert(
        &self,
        component: &str,
        status: HealthStatus,
        timestamp: Timestamp,
        reason: &str,
    ) -> HealthAggregatedReportPayload {
        HealthAggregatedReportPayload::Alert(HealthAggregatedAlertPayload {
            alert_type: HealthAlertType::ComponentStatusChange,
            component: component.to_string(),
            status,
            timestamp,
            reason: reason.to_string(),
            severity: if matches!(status, HealthStatus::Unhealthy) {
                HealthAlertSeverity::Critical
            } else {
                HealthAlertSeverity::Warning
            },
        })
    }

    fn create_system_status(
        &self,
        state: &HealthState,
        timestamp: Timestamp,
    ) -> HealthAggregatedReportPayload {
        let total_components = state.component_health.len();
        let healthy = state
            .component_health
            .values()
            .filter(|c| matches!(c.current_status, HealthStatus::Healthy))
            .count();
        let degraded = state
            .component_health
            .values()
            .filter(|c| matches!(c.current_status, HealthStatus::Degraded))
            .count();
        let failed = state
            .component_health
            .values()
            .filter(|c| matches!(c.current_status, HealthStatus::Unhealthy))
            .count();
        let unknown = total_components.saturating_sub(healthy + degraded + failed);

        let overall_status = if failed > 0 {
            HealthStatus::Unhealthy
        } else if degraded > 0 {
            HealthStatus::Degraded
        } else if healthy == total_components && total_components > 0 {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        };

        HealthAggregatedReportPayload::SystemStatus(HealthAggregatedSystemStatusPayload {
            report_type: HealthAggregatedReportType::SystemHealthStatus,
            timestamp,
            overall_status,
            total_components,
            healthy_count: healthy,
            degraded_count: degraded,
            failed_count: failed,
            unknown_count: unknown,
            components: state
                .component_health
                .iter()
                .map(|(name, health)| HealthComponentSnapshot {
                    name: name.clone(),
                    status: health.current_status,
                    status_since: health.status_since,
                    last_seen: health.last_seen,
                })
                .collect(),
        })
    }

    fn create_component_report(
        &self,
        component_health: &ComponentHealth,
        timestamp: Timestamp,
    ) -> HealthAggregatedReportPayload {
        let window_start =
            timestamp - Duration::seconds(self.config.aggregation_window_seconds as i64);

        let events_in_window = component_health
            .events
            .iter()
            .filter(|e| e.timestamp >= window_start)
            .count();

        let transitions_in_window = component_health
            .events
            .iter()
            .filter(|e| e.timestamp >= window_start && e.previous_status != e.current_status)
            .count();

        HealthAggregatedReportPayload::ComponentReport(HealthAggregatedComponentReportPayload {
            report_type: HealthAggregatedReportType::ComponentHealthReport,
            timestamp,
            component: component_health.component_name.clone(),
            current_status: component_health.current_status,
            status_since: component_health.status_since,
            last_seen: component_health.last_seen,
            total_transitions: component_health.transition_count,
            events_in_window,
            transitions_in_window,
            window_seconds: self.config.aggregation_window_seconds,
        })
    }
}

/// RuntimeModule type alias registered via `AutomatonSpec` in `automata::registry`.
pub type HealthAggregatorRuntime = ScopeReconcilerAdapter<HealthAggregator>;

fn parse_component_name(input: &JsonValue) -> Result<&str, AutomatonLogicError> {
    let component = input.get("component").ok_or_else(|| {
        AutomatonLogicError::InputParsing(
            "health status payload is missing required field 'component'".to_string(),
        )
    })?;
    let component = component.as_str().ok_or_else(|| {
        AutomatonLogicError::InputParsing(
            "health status field 'component' must be a string".to_string(),
        )
    })?;
    if component.trim().is_empty() {
        return Err(AutomatonLogicError::InputParsing(
            "health status field 'component' must not be empty".to_string(),
        ));
    }
    Ok(component)
}

fn parse_health_status_field(
    input: &JsonValue,
    field: &str,
) -> Result<HealthStatus, AutomatonLogicError> {
    let value = input.get(field).ok_or_else(|| {
        AutomatonLogicError::InputParsing(format!(
            "health status payload is missing required field '{field}'"
        ))
    })?;
    let status = value.as_str().ok_or_else(|| {
        AutomatonLogicError::InputParsing(format!("health status field '{field}' must be a string"))
    })?;
    HealthStatus::from_str(status).map_err(|_| {
        AutomatonLogicError::InputParsing(format!(
            "health status field '{field}' has invalid value '{status}'"
        ))
    })
}

// --- Source descriptor (issue #690 / #734) ---

use sinex_primitives::privacy::ProcessingContext;
use sinex_primitives::source_contracts::{
    AccessScope, CheckpointFamily as ContractCheckpointFamily, Horizon as ContractHorizon,
    OccurrenceIdentity as ContractOccurrenceIdentity, PrivacyTier as ContractPrivacyTier,
    ResourceProfile, RetentionPolicy as ContractRetentionPolicy, RunnerPack,
    RuntimeShape as ContractRuntimeShape, SourceContract, SourceRuntimeBinding, SubjectRef,
};
use sinex_primitives::{register_source_contract, register_source_runtime_binding};

// Health is a ScopeReconciler over component scopes. State per-component is
// reconciled and reported as `health.aggregated_report`.
register_source_contract! {
    SourceContract {
        id: "health",
        namespace: "derived",
        event_types: &[
            ("health-aggregator", "health.aggregated_report"),
        ],
        // Health metrics describe component liveness, not user content.
        privacy_tier: ContractPrivacyTier::Public,
        horizons: &[ContractHorizon::Continuous],
        retention: ContractRetentionPolicy::Forever,
        occurrence_identity: ContractOccurrenceIdentity::Uuid5From(
            "(source, component_scope, parent_event_ids)",
        ),
        access_scope: AccessScope::Internal,
    }
}

register_source_runtime_binding! {
    SourceRuntimeBinding::builder(
        SubjectRef::from_static("source:health"),
        "health",
        "derived",
    )
    .implementation("sinexd")
    .adapter("AutomatonRuntime")
    .output_event_type("health.aggregated_report")
    .privacy_context(ProcessingContext::Metadata)
    .resource_profile(ResourceProfile::EventStreamConsumer)
    .source_id("health")
    .runner_pack(RunnerPack::InProcess)
    .checkpoint_family(ContractCheckpointFamily::AppendStream)
    .runtime_shape(ContractRuntimeShape::Continuous)
    .build_impact(sinex_primitives::source_contracts::SourceBuildImpact::ZERO)
    .build()
}
