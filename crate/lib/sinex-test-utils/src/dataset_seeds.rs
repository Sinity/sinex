use crate::timing_utils::WaitHelpers;
use crate::{EventOverrides, PipelineHarness, PipelineScope, TestContext, TestResult};
use chrono::{DateTime, Duration as ChronoDuration, TimeZone, Utc};
use color_eyre::eyre::{eyre, WrapErr};
use futures::future::try_join_all;
use serde_json::{json, Value};
use sinex_core::{types::Ulid, Event};
use std::collections::HashMap;

const DEFAULT_SEED_TIMEOUT_SECS: u64 = 12;

#[derive(Clone, Debug)]
pub struct SeedClock {
    base: DateTime<Utc>,
}

impl SeedClock {
    pub fn new(base: DateTime<Utc>) -> Self {
        Self { base }
    }

    pub fn fixed() -> Self {
        let base = Utc
            .with_ymd_and_hms(2025, 1, 2, 12, 0, 0)
            .single()
            .unwrap_or_else(Utc::now);
        Self { base }
    }

    pub fn base(&self) -> DateTime<Utc> {
        self.base
    }
}

impl Default for SeedClock {
    fn default() -> Self {
        Self::fixed()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum TimestampSpec {
    At(DateTime<Utc>),
    Before(ChronoDuration),
    After(ChronoDuration),
}

impl TimestampSpec {
    fn resolve(&self, clock: &SeedClock) -> DateTime<Utc> {
        match self {
            TimestampSpec::At(ts) => ts.clone(),
            TimestampSpec::Before(offset) => clock.base - *offset,
            TimestampSpec::After(offset) => clock.base + *offset,
        }
    }
}

#[derive(Clone, Debug)]
pub struct EventSpec {
    pub source: String,
    pub event_type: String,
    pub payload: Value,
    pub timestamp: TimestampSpec,
    pub overrides: EventOverrides,
}

impl EventSpec {
    pub fn new<S: Into<String>, T: Into<String>>(source: S, event_type: T, payload: Value) -> Self {
        Self {
            source: source.into(),
            event_type: event_type.into(),
            payload,
            timestamp: TimestampSpec::Before(ChronoDuration::zero()),
            overrides: EventOverrides::default(),
        }
    }

    pub fn at(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = TimestampSpec::At(timestamp);
        self
    }

    pub fn before(mut self, offset: ChronoDuration) -> Self {
        self.timestamp = TimestampSpec::Before(offset);
        self
    }

    pub fn after(mut self, offset: ChronoDuration) -> Self {
        self.timestamp = TimestampSpec::After(offset);
        self
    }

    pub fn with_overrides(mut self, overrides: EventOverrides) -> Self {
        self.overrides = overrides;
        self
    }

    fn resolved_timestamp(&self, clock: &SeedClock) -> DateTime<Utc> {
        self.timestamp.resolve(clock)
    }

    fn overrides_with_timestamp(&self, clock: &SeedClock) -> EventOverrides {
        let mut overrides = self.overrides.clone();
        if overrides.ts_orig.is_none() {
            overrides.ts_orig = Some(self.resolved_timestamp(clock).to_rfc3339());
        }
        overrides
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DatasetVariant {
    SemanticMin,
    Perf,
}

#[derive(Clone, Debug)]
pub struct QueryDataset {
    pub event_ids: Vec<Ulid>,
    pub reference_time: DateTime<Utc>,
    pub expected_total: usize,
    pub expected_sources: HashMap<String, usize>,
    pub expected_event_types: HashMap<String, usize>,
}

#[derive(Clone, Debug)]
pub struct AnalyticsDataset {
    pub event_ids: Vec<Ulid>,
    pub reference_time: DateTime<Utc>,
    pub expected_total: i64,
    pub expected_source_counts: HashMap<String, i64>,
    pub expected_event_type_counts: HashMap<String, i64>,
    pub expected_command_counts: HashMap<String, i64>,
    pub expected_command_total: i64,
}

#[derive(Clone, Debug)]
pub struct ServiceIntegrationDataset {
    pub event_ids: Vec<Ulid>,
    pub reference_time: DateTime<Utc>,
    pub expected_total: usize,
    pub expected_sources: HashMap<String, usize>,
    pub expected_event_types: HashMap<String, usize>,
}

pub async fn seed_events_via_pipeline(
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
    specs: &[EventSpec],
) -> TestResult<Vec<Ulid>> {
    ensure_fixed_seed(clock)?;
    let clock = clock.clone();
    let futures = specs.iter().cloned().map(|spec| {
        let clock = clock.clone();
        async move {
            let overrides = spec.overrides_with_timestamp(&clock);
            let event_id = pipeline
                .publish_event_with_overrides(
                    &spec.source,
                    &spec.event_type,
                    spec.payload,
                    overrides,
                )
                .await?;
            Ok::<Ulid, color_eyre::eyre::Report>(*event_id.as_ulid())
        }
    });

    try_join_all(futures).await.map_err(Into::into)
}

pub async fn seed_events_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
    specs: &[EventSpec],
) -> TestResult<Vec<Ulid>> {
    ensure_fixed_seed(clock)?;
    let clock = clock.clone();
    let futures = specs.iter().cloned().map(|spec| {
        let clock = clock.clone();
        async move {
            let overrides = spec.overrides_with_timestamp(&clock);
            let event_id = scope
                .publish_with_overrides(&spec.source, &spec.event_type, spec.payload, overrides)
                .await?;
            Ok::<Ulid, color_eyre::eyre::Report>(*event_id.as_ulid())
        }
    });

    try_join_all(futures).await.map_err(Into::into)
}

/// Publish pre-generated fixture events (from fixture_generator) through a PipelineScope.
pub async fn seed_fixture_events_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
    events: &[Event<Value>],
) -> TestResult<Vec<Ulid>> {
    let specs: Vec<EventSpec> = events
        .iter()
        .map(|event| {
            let mut spec = EventSpec::new(
                event.source.as_str(),
                event.event_type.as_str(),
                event.payload.clone(),
            );
            if let Some(ts) = event.ts_orig {
                spec = spec.at(ts);
            }
            spec
        })
        .collect();
    seed_events_via_scope(scope, clock, &specs).await
}

/// Publish pre-generated fixture events (from fixture_generator) through a PipelineHarness.
pub async fn seed_fixture_events_via_pipeline(
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
    events: &[Event<Value>],
) -> TestResult<Vec<Ulid>> {
    let specs: Vec<EventSpec> = events
        .iter()
        .map(|event| {
            let mut spec = EventSpec::new(
                event.source.as_str(),
                event.event_type.as_str(),
                event.payload.clone(),
            );
            if let Some(ts) = event.ts_orig {
                spec = spec.at(ts);
            }
            spec
        })
        .collect();
    seed_events_via_pipeline(pipeline, clock, &specs).await
}

fn ensure_fixed_seed(clock: &SeedClock) -> TestResult<()> {
    let expected = SeedClock::fixed().base();
    if clock.base() != expected {
        return Err(eyre!(
            "SeedClock must use the fixed baseline (SeedClock::fixed) for pipeline seeding; use explicit event overrides for custom timestamps."
        ));
    }
    Ok(())
}

pub async fn seed_query_dataset_semantic_min_via_pipeline(
    ctx: &TestContext,
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
) -> TestResult<QueryDataset> {
    seed_query_dataset_via_pipeline(ctx, pipeline, clock, DatasetVariant::SemanticMin).await
}

pub async fn seed_query_dataset_semantic_min_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
) -> TestResult<QueryDataset> {
    seed_query_dataset_via_scope(scope, clock, DatasetVariant::SemanticMin).await
}

pub async fn seed_query_dataset_perf_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
) -> TestResult<QueryDataset> {
    seed_query_dataset_via_scope(scope, clock, DatasetVariant::Perf).await
}

pub async fn seed_query_dataset_perf_via_pipeline(
    ctx: &TestContext,
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
) -> TestResult<QueryDataset> {
    seed_query_dataset_via_pipeline(ctx, pipeline, clock, DatasetVariant::Perf).await
}

pub async fn seed_analytics_dataset_semantic_min_via_pipeline(
    ctx: &TestContext,
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
) -> TestResult<AnalyticsDataset> {
    seed_analytics_dataset_via_pipeline(ctx, pipeline, clock, analytics_dataset_specs()).await
}

pub async fn seed_analytics_dataset_semantic_min_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
) -> TestResult<AnalyticsDataset> {
    seed_analytics_dataset_via_scope(scope, clock, analytics_dataset_specs()).await
}

pub async fn seed_service_integration_dataset_semantic_min_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
) -> TestResult<ServiceIntegrationDataset> {
    seed_service_integration_dataset_via_scope(scope, clock).await
}

pub async fn seed_service_integration_dataset_semantic_min_via_pipeline(
    ctx: &TestContext,
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
) -> TestResult<ServiceIntegrationDataset> {
    seed_service_integration_dataset_via_pipeline(ctx, pipeline, clock).await
}

pub async fn seed_analytics_dataset_perf_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
    event_count: usize,
) -> TestResult<AnalyticsDataset> {
    seed_analytics_dataset_via_scope(scope, clock, analytics_perf_specs(event_count)).await
}

pub async fn seed_analytics_dataset_perf_via_pipeline(
    ctx: &TestContext,
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
    event_count: usize,
) -> TestResult<AnalyticsDataset> {
    seed_analytics_dataset_via_pipeline(ctx, pipeline, clock, analytics_perf_specs(event_count))
        .await
}

async fn seed_query_dataset_via_pipeline(
    ctx: &TestContext,
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
    variant: DatasetVariant,
) -> TestResult<QueryDataset> {
    ensure_empty_slot(ctx, "query").await?;
    let specs = query_dataset_specs(variant);
    let event_ids = seed_events_via_pipeline(pipeline, clock, &specs).await?;
    let expected_total = specs.len();
    WaitHelpers::wait_for_event_count(&ctx.pool, expected_total, DEFAULT_SEED_TIMEOUT_SECS).await?;
    let (expected_sources, expected_event_types) = collect_counts(&specs);
    Ok(QueryDataset {
        event_ids,
        reference_time: clock.base(),
        expected_total,
        expected_sources,
        expected_event_types,
    })
}

async fn seed_query_dataset_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
    variant: DatasetVariant,
) -> TestResult<QueryDataset> {
    ensure_empty_slot(scope.ctx(), "query").await?;
    let specs = query_dataset_specs(variant);
    let event_ids = seed_events_via_scope(scope, clock, &specs).await?;
    let expected_total = specs.len();
    scope.wait_for_event_count(expected_total).await?;
    let (expected_sources, expected_event_types) = collect_counts(&specs);
    Ok(QueryDataset {
        event_ids,
        reference_time: clock.base(),
        expected_total,
        expected_sources,
        expected_event_types,
    })
}

async fn seed_analytics_dataset_via_pipeline(
    ctx: &TestContext,
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
    specs: Vec<EventSpec>,
) -> TestResult<AnalyticsDataset> {
    ensure_empty_slot(ctx, "analytics").await?;
    let event_ids = seed_events_via_pipeline(pipeline, clock, &specs).await?;
    let expected_total = specs.len() as i64;
    WaitHelpers::wait_for_event_count(
        &ctx.pool,
        expected_total as usize,
        DEFAULT_SEED_TIMEOUT_SECS,
    )
    .await?;
    let (source_counts, event_type_counts) = collect_counts(&specs);
    let expected_source_counts = to_i64_map(&source_counts);
    let expected_event_type_counts = to_i64_map(&event_type_counts);
    let expected_command_counts = collect_command_counts(&specs);
    let expected_command_total: i64 = expected_command_counts.values().sum();
    Ok(AnalyticsDataset {
        event_ids,
        reference_time: clock.base(),
        expected_total,
        expected_source_counts,
        expected_event_type_counts,
        expected_command_counts,
        expected_command_total,
    })
}

async fn seed_service_integration_dataset_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
) -> TestResult<ServiceIntegrationDataset> {
    ensure_empty_slot(scope.ctx(), "service integration").await?;
    let specs = service_integration_dataset_specs();
    let event_ids = seed_events_via_scope(scope, clock, &specs).await?;
    let expected_total = specs.len();
    scope.wait_for_event_count(expected_total).await?;
    let (expected_sources, expected_event_types) = collect_counts(&specs);
    Ok(ServiceIntegrationDataset {
        event_ids,
        reference_time: clock.base(),
        expected_total,
        expected_sources,
        expected_event_types,
    })
}

async fn seed_service_integration_dataset_via_pipeline(
    ctx: &TestContext,
    pipeline: &PipelineHarness<'_>,
    clock: &SeedClock,
) -> TestResult<ServiceIntegrationDataset> {
    ensure_empty_slot(ctx, "service integration").await?;
    let specs = service_integration_dataset_specs();
    let event_ids = seed_events_via_pipeline(pipeline, clock, &specs).await?;
    let expected_total = specs.len();
    WaitHelpers::wait_for_event_count(&ctx.pool, expected_total, DEFAULT_SEED_TIMEOUT_SECS).await?;
    let (expected_sources, expected_event_types) = collect_counts(&specs);
    Ok(ServiceIntegrationDataset {
        event_ids,
        reference_time: clock.base(),
        expected_total,
        expected_sources,
        expected_event_types,
    })
}

async fn seed_analytics_dataset_via_scope(
    scope: &PipelineScope<'_>,
    clock: &SeedClock,
    specs: Vec<EventSpec>,
) -> TestResult<AnalyticsDataset> {
    ensure_empty_slot(scope.ctx(), "analytics").await?;
    let event_ids = seed_events_via_scope(scope, clock, &specs).await?;
    let expected_total = specs.len() as i64;
    scope.wait_for_event_count(expected_total as usize).await?;
    let (source_counts, event_type_counts) = collect_counts(&specs);
    let expected_source_counts = to_i64_map(&source_counts);
    let expected_event_type_counts = to_i64_map(&event_type_counts);
    let expected_command_counts = collect_command_counts(&specs);
    let expected_command_total: i64 = expected_command_counts.values().sum();
    Ok(AnalyticsDataset {
        event_ids,
        reference_time: clock.base(),
        expected_total,
        expected_source_counts,
        expected_event_type_counts,
        expected_command_counts,
        expected_command_total,
    })
}
async fn ensure_empty_slot(ctx: &TestContext, dataset: &str) -> TestResult<()> {
    ctx.ensure_clean().await.wrap_err_with(|| {
        format!(
            "dataset {dataset} requires a clean slot\nslot={}\nnamespace={}",
            ctx.database_name(),
            ctx.pipeline_namespace().prefix()
        )
    })
}

fn collect_counts(specs: &[EventSpec]) -> (HashMap<String, usize>, HashMap<String, usize>) {
    let mut sources = HashMap::new();
    let mut event_types = HashMap::new();
    for spec in specs {
        *sources.entry(spec.source.clone()).or_insert(0) += 1;
        *event_types.entry(spec.event_type.clone()).or_insert(0) += 1;
    }
    (sources, event_types)
}

fn to_i64_map(input: &HashMap<String, usize>) -> HashMap<String, i64> {
    input
        .iter()
        .map(|(key, value)| (key.clone(), *value as i64))
        .collect()
}

fn collect_command_counts(specs: &[EventSpec]) -> HashMap<String, i64> {
    let mut counts = HashMap::new();
    for spec in specs {
        if spec.event_type != "command.executed" {
            continue;
        }
        if let Some(command) = spec.payload.get("command").and_then(Value::as_str) {
            *counts.entry(command.to_string()).or_insert(0) += 1;
        }
    }
    counts
}

fn query_dataset_specs(variant: DatasetVariant) -> Vec<EventSpec> {
    let mut specs = vec![
        EventSpec::new(
            "fs",
            "file.created",
            json!({
                "path": "/home/user/projects/rust/main.rs",
                "size": 2048,
                "content": "fn main() { println!(\"Hello, world!\"); }"
            }),
        )
        .before(ChronoDuration::minutes(10)),
        EventSpec::new(
            "fs",
            "file.modified",
            json!({
                "path": "/home/user/projects/rust/lib.rs",
                "size": 4096,
                "changes": "Added new function parse_query"
            }),
        )
        .before(ChronoDuration::minutes(5)),
        EventSpec::new(
            "shell.bash",
            "command.executed",
            json!({
                "command": "cargo nextest run --lib query",
                "exit_code": 0,
                "directory": "/home/user/projects/rust",
                "duration_ms": 1500
            }),
        )
        .before(ChronoDuration::minutes(15)),
        EventSpec::new(
            "shell.zsh",
            "command.executed",
            json!({
                "command": "grep -r 'query_service' src/",
                "exit_code": 0,
                "directory": "/home/user/projects",
                "duration_ms": 250
            }),
        )
        .before(ChronoDuration::hours(1)),
        EventSpec::new(
            "app.vscode",
            "file.opened",
            json!({
                "file": "/home/user/projects/rust/query_service.rs",
                "language": "rust",
                "workspace": "rust-project"
            }),
        )
        .before(ChronoDuration::minutes(30)),
        EventSpec::new(
            "fs",
            "file.deleted",
            json!({
                "path": "/tmp/old_file.txt",
                "reason": "cleanup"
            }),
        )
        .before(ChronoDuration::days(2)),
        EventSpec::new(
            "fs",
            "file.created",
            json!({
                "path": "/home/user/projects/rust/more.rs",
                "size": 1024,
                "content": "mod more;"
            }),
        ),
        EventSpec::new(
            "app.vscode",
            "file.saved",
            json!({
                "file": "/home/user/projects/rust/query_service.rs",
                "language": "rust"
            }),
        )
        .before(ChronoDuration::minutes(2)),
    ];

    if matches!(variant, DatasetVariant::Perf) {
        for minute_offset in [12, 9, 6, 3] {
            specs.push(
                EventSpec::new(
                    "fs",
                    "file.modified",
                    json!({
                        "path": format!("/home/user/projects/rust/batch_{minute_offset}.rs"),
                        "size": 1024 + minute_offset * 10,
                        "changes": "Refactor pipeline queries"
                    }),
                )
                .before(ChronoDuration::minutes(minute_offset as i64)),
            );
        }

        for i in 0..8 {
            specs.push(
                EventSpec::new(
                    "shell.bash",
                    "command.executed",
                    json!({
                        "command": format!("cargo test --package analytics --case batch_{i}"),
                        "exit_code": 0,
                        "directory": format!("/home/user/projects/rust/run_{i}"),
                        "duration_ms": 1200 + i * 10
                    }),
                )
                .before(ChronoDuration::minutes(20 + i as i64)),
            );
            specs.push(EventSpec::new(
                "fs",
                "file.created",
                json!({
                    "path": format!("/tmp/pipeline_seed_{i}.txt"),
                    "size": 512 + i * 17,
                    "content": format!("seed-{}", i)
                }),
            ));
        }

        for i in 0..4 {
            specs.push(
                EventSpec::new(
                    "app.vscode",
                    "file.opened",
                    json!({
                        "file": format!("/home/user/projects/rust/refactor_{i}.rs"),
                        "language": "rust",
                        "workspace": "rust-project",
                        "column": 40 + i * 3
                    }),
                )
                .before(ChronoDuration::minutes(45 + i as i64)),
            );
        }
    }

    specs
}

fn analytics_dataset_specs() -> Vec<EventSpec> {
    let mut specs = Vec::new();

    for i in 0..5 {
        specs.push(
            EventSpec::new(
                "fs",
                "file.created",
                json!({
                    "path": format!("/test/file_{}.txt", i),
                    "size": 1024 * (i + 1)
                }),
            )
            .before(ChronoDuration::minutes(20 * i as i64)),
        );
    }

    let commands = [
        ("git status", 8),
        ("cargo build", 5),
        ("ls -la", 3),
        ("cd /home", 2),
        ("vim file.rs", 1),
    ];
    for (command, count) in commands {
        for i in 0..count {
            specs.push(
                EventSpec::new(
                    "shell.kitty",
                    "command.executed",
                    json!({
                        "command": command,
                        "exit_code": 0,
                        "duration_ms": 100 + i * 10
                    }),
                )
                .before(ChronoDuration::minutes(5 * i as i64)),
            );
        }
    }

    for i in 0..3 {
        specs.push(
            EventSpec::new(
                "wm.hyprland",
                "window.opened",
                json!({
                    "title": format!("Window {}", i),
                    "class": "test-app",
                    "workspace": i + 1
                }),
            )
            .before(ChronoDuration::minutes(10 * i as i64)),
        );
    }

    specs.push(
        EventSpec::new(
            "clipboard",
            "copied",
            json!({
                "content": "test clipboard content",
                "application": "firefox"
            }),
        )
        .before(ChronoDuration::hours(3)),
    );

    specs.push(
        EventSpec::new(
            "system",
            "boot.completed",
            json!({
                "uptime_seconds": 0,
                "kernel_version": "6.1.0"
            }),
        )
        .before(ChronoDuration::days(2)),
    );

    specs
}

fn service_integration_dataset_specs() -> Vec<EventSpec> {
    vec![
        EventSpec::new(
            "fs-watcher",
            "file.created",
            json!({
                "path": "/home/user/documents/project.md",
                "size": 2048,
                "content": "Project documentation with key insights"
            }),
        )
        .before(ChronoDuration::minutes(12)),
        EventSpec::new(
            "terminal",
            "command.executed",
            json!({
                "command": "git commit -m 'Add documentation'",
                "exit_code": 0,
                "directory": "/home/user/documents"
            }),
        )
        .before(ChronoDuration::minutes(8)),
        EventSpec::new(
            "desktop",
            "window.focused",
            json!({
                "title": "VSCode - project.md",
                "application": "code",
                "workspace": "main"
            }),
        )
        .before(ChronoDuration::minutes(4)),
    ]
}

fn analytics_perf_specs(event_count: usize) -> Vec<EventSpec> {
    let mut specs = Vec::with_capacity(event_count);
    for i in 0..event_count {
        specs.push(
            EventSpec::new(
                format!("perf_source_{}", i % 5),
                format!("perf_type_{}", i % 3),
                json!({"sequence": i, "performance_test": true}),
            )
            .before(ChronoDuration::minutes((i % 60) as i64)),
        );
    }
    specs
}
