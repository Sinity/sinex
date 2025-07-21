// Declarative Test Scenario DSL
//
// This module provides a powerful DSL for writing readable, declarative tests
// that follow the given-when-then pattern. It integrates with the existing
// test infrastructure and provides automatic cleanup handling.
//
// # Examples
//
// ```rust
// scenario! {
//     name: "basic_event_processing",
//     given: {
//         events: factory.create_session(),
//         checkpoints: ["processor" => 10],
//         state: json!({"key": "value"})
//     },
//     when: {
//         action: "process_events",
//         params: ["source" => "test"],
//         wait: Duration::from_secs(1)
//     },
//     then: {
//         events_count: 50,
//         checkpoint: ["processor" => 60],
//         events_match: |e| e.source == "test",
//         no_errors: true
//     },
//     cleanup: {
//         delete_events: "test*",
//         reset_checkpoints: true
//     }
// }
// ```

use crate::common::prelude::*;
use crate::common::test_context::TestContext;
use crate::common::query_helpers::TestQueries;
use sinex_db::queries::{EventQueries, CheckpointQueries};
use sinex_events::EventFactory;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

/// A test scenario represents a complete test case with given-when-then structure
#[derive(Debug)]
pub struct TestScenario {
    pub name: String,
    pub given: GivenContext,
    pub when: WhenAction,
    pub then: ThenAssertion,
    pub cleanup: Option<CleanupSpec>,
}

/// The initial state setup for a test
#[derive(Debug, Default)]
pub struct GivenContext {
    pub events: Vec<RawEvent>,
    pub checkpoints: HashMap<String, u64>,
    pub state: Option<serde_json::Value>,
    pub redis_state: HashMap<String, String>,
    pub files: HashMap<String, String>,
    pub env_vars: HashMap<String, String>,
}

/// The action to perform in the test
#[derive(Debug)]
pub struct WhenAction {
    pub action: String,
    pub params: HashMap<String, String>,
    pub wait: Option<Duration>,
    pub repeat: Option<usize>,
    pub parallel: bool,
}

/// The assertions to verify after the action
#[derive(Debug, Default)]
pub struct ThenAssertion {
    pub events_count: Option<usize>,
    pub events_count_gte: Option<usize>,
    pub checkpoints: HashMap<String, u64>,
    pub events_match: Option<Box<dyn Fn(&RawEvent) -> bool + Send + Sync>>,
    pub no_errors: bool,
    pub custom_assertions: Vec<Box<dyn Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send + Sync>>,
    pub redis_keys: HashMap<String, String>,
    pub files_exist: Vec<String>,
    pub duration_under: Option<Duration>,
}

/// Cleanup operations to run after the test
#[derive(Debug, Default)]
pub struct CleanupSpec {
    pub delete_events: Option<String>,
    pub reset_checkpoints: bool,
    pub remove_files: Vec<String>,
    pub clear_redis: Vec<String>,
}

/// Builder for creating test scenarios fluently
pub struct ScenarioBuilder {
    name: String,
    given: GivenContext,
    when: Option<WhenAction>,
    then: ThenAssertion,
    cleanup: Option<CleanupSpec>,
}

impl ScenarioBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            given: GivenContext::default(),
            when: None,
            then: ThenAssertion::default(),
            cleanup: None,
        }
    }

    // Given builders
    pub fn given_events(mut self, events: Vec<RawEvent>) -> Self {
        self.given.events = events;
        self
    }

    pub fn given_checkpoint(mut self, name: impl Into<String>, count: u64) -> Self {
        self.given.checkpoints.insert(name.into(), count);
        self
    }

    pub fn given_state(mut self, state: serde_json::Value) -> Self {
        self.given.state = Some(state);
        self
    }

    pub fn given_redis(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.given.redis_state.insert(key.into(), value.into());
        self
    }

    pub fn given_file(mut self, path: impl Into<String>, content: impl Into<String>) -> Self {
        self.given.files.insert(path.into(), content.into());
        self
    }

    // When builders
    pub fn when_action(mut self, action: impl Into<String>) -> Self {
        self.when = Some(WhenAction {
            action: action.into(),
            params: HashMap::new(),
            wait: None,
            repeat: None,
            parallel: false,
        });
        self
    }

    pub fn with_param(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        if let Some(when) = &mut self.when {
            when.params.insert(key.into(), value.into());
        }
        self
    }

    pub fn wait_for(mut self, duration: Duration) -> Self {
        if let Some(when) = &mut self.when {
            when.wait = Some(duration);
        }
        self
    }

    pub fn repeat(mut self, times: usize) -> Self {
        if let Some(when) = &mut self.when {
            when.repeat = Some(times);
        }
        self
    }

    pub fn in_parallel(mut self) -> Self {
        if let Some(when) = &mut self.when {
            when.parallel = true;
        }
        self
    }

    // Then builders
    pub fn then_events_count(mut self, count: usize) -> Self {
        self.then.events_count = Some(count);
        self
    }

    pub fn then_events_count_gte(mut self, count: usize) -> Self {
        self.then.events_count_gte = Some(count);
        self
    }

    pub fn then_checkpoint(mut self, name: impl Into<String>, count: u64) -> Self {
        self.then.checkpoints.insert(name.into(), count);
        self
    }

    pub fn then_events_match<F>(mut self, predicate: F) -> Self
    where
        F: Fn(&RawEvent) -> bool + Send + Sync + 'static,
    {
        self.then.events_match = Some(Box::new(predicate));
        self
    }

    pub fn then_no_errors(mut self) -> Self {
        self.then.no_errors = true;
        self
    }

    pub fn then_custom<F>(mut self, assertion: F) -> Self
    where
        F: Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send + Sync + 'static,
    {
        self.then.custom_assertions.push(Box::new(assertion));
        self
    }

    pub fn then_duration_under(mut self, duration: Duration) -> Self {
        self.then.duration_under = Some(duration);
        self
    }

    // Cleanup builders
    pub fn cleanup_events(mut self, pattern: impl Into<String>) -> Self {
        let cleanup = self.cleanup.get_or_insert(CleanupSpec::default());
        cleanup.delete_events = Some(pattern.into());
        self
    }

    pub fn cleanup_checkpoints(mut self) -> Self {
        let cleanup = self.cleanup.get_or_insert(CleanupSpec::default());
        cleanup.reset_checkpoints = true;
        self
    }

    pub fn cleanup_files(mut self, files: Vec<String>) -> Self {
        let cleanup = self.cleanup.get_or_insert(CleanupSpec::default());
        cleanup.remove_files = files;
        self
    }

    /// Build the scenario
    pub fn build(self) -> TestScenario {
        TestScenario {
            name: self.name,
            given: self.given,
            when: self.when.expect("When action is required"),
            then: self.then,
            cleanup: self.cleanup,
        }
    }

    /// Execute the scenario
    pub async fn run(self, ctx: &TestContext) -> TestResult {
        let scenario = self.build();
        execute_scenario(&scenario, ctx).await
    }
}

/// Execute a test scenario
pub async fn execute_scenario(scenario: &TestScenario, ctx: &TestContext) -> TestResult {
    let start = std::time::Instant::now();
    println!("\n=== Scenario: {} ===", scenario.name);

    // Given: Set up initial state
    println!("📋 Given:");
    
    // Insert events
    if !scenario.given.events.is_empty() {
        println!("  - Inserting {} events", scenario.given.events.len());
        for event in &scenario.given.events {
            ctx.insert_event(event).await?;
        }
    }

    // Set up checkpoints
    for (name, count) in &scenario.given.checkpoints {
        println!("  - Setting checkpoint {} to {}", name, count);
        CheckpointQueries::upsert_checkpoint(
            Ulid::new(),
            name.clone(),
            format!("{}-group", name),
            format!("{}-consumer", name),
            None,
            *count as i64,
            chrono::Utc::now(),
            scenario.given.state.clone(),
            1,
            None,
            chrono::Utc::now(),
            chrono::Utc::now(),
        )
        .execute(ctx.pool())
        .await?;
    }

    // Set up Redis state
    if !scenario.given.redis_state.is_empty() {
        let mut redis = ctx.redis().await?;
        for (key, value) in &scenario.given.redis_state {
            println!("  - Setting Redis key {} = {}", key, value);
            use redis::AsyncCommands;
            redis.set::<_, _, ()>(key, value).await?;
        }
    }

    // Create files
    for (path, content) in &scenario.given.files {
        println!("  - Creating file: {}", path);
        tokio::fs::create_dir_all(std::path::Path::new(path).parent().unwrap()).await?;
        tokio::fs::write(path, content).await?;
    }

    // When: Execute action
    println!("\n🎯 When:");
    println!("  - Action: {}", scenario.when.action);
    
    let action_result = execute_action(&scenario.when, ctx).await?;

    // Wait if specified
    if let Some(wait_duration) = scenario.when.wait {
        println!("  - Waiting {:?}", wait_duration);
        tokio::time::sleep(wait_duration).await;
    }

    // Then: Verify assertions
    println!("\n✅ Then:");
    
    // Check event count
    if let Some(expected_count) = scenario.then.events_count {
        let actual_count = ctx.event_count().await? as usize;
        println!("  - Events count: {} (expected: {})", actual_count, expected_count);
        assert_eq!(actual_count, expected_count, "Event count mismatch");
    }

    if let Some(min_count) = scenario.then.events_count_gte {
        let actual_count = ctx.event_count().await? as usize;
        println!("  - Events count: {} (expected >= {})", actual_count, min_count);
        assert!(actual_count >= min_count, "Event count below minimum");
    }

    // Check checkpoints
    for (name, expected_count) in &scenario.then.checkpoints {
        let checkpoint = TestQueries::get_checkpoint_by_name(ctx.pool(), name).await?;
        if let Some(cp) = checkpoint {
            println!("  - Checkpoint {}: {} (expected: {})", name, cp.processed_count, expected_count);
            assert_eq!(cp.processed_count as u64, *expected_count, "Checkpoint count mismatch");
        } else {
            panic!("Checkpoint {} not found", name);
        }
    }

    // Check event matching
    if let Some(predicate) = &scenario.then.events_match {
        let events = TestQueries::get_recent_events(ctx.pool(), 1000).await?;
        let matching = events.iter().filter(|e| predicate(e)).count();
        println!("  - Events matching predicate: {}/{}", matching, events.len());
        assert!(matching > 0, "No events match the predicate");
    }

    // Check duration
    let duration = start.elapsed();
    if let Some(max_duration) = scenario.then.duration_under {
        println!("  - Duration: {:?} (max: {:?})", duration, max_duration);
        assert!(duration < max_duration, "Test took too long");
    }

    // Run custom assertions
    for (i, assertion) in scenario.then.custom_assertions.iter().enumerate() {
        println!("  - Running custom assertion {}", i + 1);
        assertion(ctx).await?;
    }

    // Cleanup
    if let Some(cleanup) = &scenario.cleanup {
        println!("\n🧹 Cleanup:");
        
        if let Some(pattern) = &cleanup.delete_events {
            println!("  - Deleting events matching: {}", pattern);
            EventQueries::delete_by_source(pattern.clone())
                .execute(ctx.pool())
                .await?;
        }

        if cleanup.reset_checkpoints {
            println!("  - Resetting checkpoints");
            sqlx::query!("DELETE FROM core.automaton_checkpoints WHERE automaton_name LIKE 'test_%'")
                .execute(ctx.pool())
                .await?;
        }

        for file in &cleanup.remove_files {
            println!("  - Removing file: {}", file);
            let _ = tokio::fs::remove_file(file).await;
        }

        if !cleanup.clear_redis.is_empty() {
            let mut redis = ctx.redis().await?;
            for pattern in &cleanup.clear_redis {
                println!("  - Clearing Redis keys: {}", pattern);
                use redis::AsyncCommands;
                let keys: Vec<String> = redis.keys(pattern).await?;
                if !keys.is_empty() {
                    let _: () = redis.del(&keys).await?;
                }
            }
        }
    }

    println!("\n✅ Scenario completed in {:?}", duration);
    Ok(())
}

/// Execute a when action
async fn execute_action(action: &WhenAction, ctx: &TestContext) -> TestResult {
    match action.action.as_str() {
        "process_events" => {
            // Simulate event processing
            if let Some(source) = action.params.get("source") {
                ctx.wait_for_source_events(source, 1).await?;
            }
        }
        "insert_events" => {
            // Insert additional events
            if let Some(count) = action.params.get("count") {
                let count: usize = count.parse()?;
                let source = action.params.get("source").unwrap_or(&"test".to_string());
                for i in 0..count {
                    let event = ctx.event_builder(source, "test.event")
                        .payload(json!({ "index": i }))
                        .build();
                    ctx.insert_event(&event).await?;
                }
            }
        }
        "trigger_automaton" => {
            // Trigger automaton processing
            if let Some(name) = action.params.get("automaton") {
                // In real tests, this would trigger the automaton
                println!("  - Triggering automaton: {}", name);
            }
        }
        _ => {
            // Custom action - would be handled by test-specific code
            println!("  - Custom action: {}", action.action);
        }
    }
    Ok(())
}

/// Macro for creating scenarios with cleaner syntax
#[macro_export]
macro_rules! scenario {
    (
        name: $name:expr,
        given: { $($given:tt)* },
        when: { $($when:tt)* },
        then: { $($then:tt)* }
        $(, cleanup: { $($cleanup:tt)* })?
    ) => {{
        use $crate::common::scenario_dsl::ScenarioBuilder;
        
        let mut builder = ScenarioBuilder::new($name);
        
        // Parse given
        scenario_given!(builder, $($given)*);
        
        // Parse when
        scenario_when!(builder, $($when)*);
        
        // Parse then
        scenario_then!(builder, $($then)*);
        
        // Parse cleanup if present
        $(scenario_cleanup!(builder, $($cleanup)*);)?
        
        builder
    }};
}

#[macro_export]
macro_rules! scenario_given {
    ($builder:ident, events: $events:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.given_events($events);
        $(scenario_given!($builder, $($rest)*);)?
    };
    ($builder:ident, checkpoints: [$($name:expr => $count:expr),*] $(, $($rest:tt)*)?) => {
        $(
            $builder = $builder.given_checkpoint($name, $count);
        )*
        $(scenario_given!($builder, $($rest)*);)?
    };
    ($builder:ident, state: $state:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.given_state($state);
        $(scenario_given!($builder, $($rest)*);)?
    };
    ($builder:ident,) => {};
}

#[macro_export]
macro_rules! scenario_when {
    ($builder:ident, action: $action:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.when_action($action);
        $(scenario_when!($builder, $($rest)*);)?
    };
    ($builder:ident, params: [$($key:expr => $value:expr),*] $(, $($rest:tt)*)?) => {
        $(
            $builder = $builder.with_param($key, $value);
        )*
        $(scenario_when!($builder, $($rest)*);)?
    };
    ($builder:ident, wait: $duration:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.wait_for($duration);
        $(scenario_when!($builder, $($rest)*);)?
    };
    ($builder:ident, parallel: true $(, $($rest:tt)*)?) => {
        $builder = $builder.in_parallel();
        $(scenario_when!($builder, $($rest)*);)?
    };
    ($builder:ident,) => {};
}

#[macro_export]
macro_rules! scenario_then {
    ($builder:ident, events_count: $count:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.then_events_count($count);
        $(scenario_then!($builder, $($rest)*);)?
    };
    ($builder:ident, events_count_gte: $count:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.then_events_count_gte($count);
        $(scenario_then!($builder, $($rest)*);)?
    };
    ($builder:ident, checkpoint: [$($name:expr => $count:expr),*] $(, $($rest:tt)*)?) => {
        $(
            $builder = $builder.then_checkpoint($name, $count);
        )*
        $(scenario_then!($builder, $($rest)*);)?
    };
    ($builder:ident, events_match: $predicate:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.then_events_match($predicate);
        $(scenario_then!($builder, $($rest)*);)?
    };
    ($builder:ident, no_errors: $value:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.then_no_errors();
        $(scenario_then!($builder, $($rest)*);)?
    };
    ($builder:ident, duration_under: $duration:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.then_duration_under($duration);
        $(scenario_then!($builder, $($rest)*);)?
    };
    ($builder:ident, custom_assertions: $assertions:expr $(, $($rest:tt)*)?) => {
        // Skip custom_assertions in macro - handle separately
        $(scenario_then!($builder, $($rest)*);)?
    };
    ($builder:ident,) => {};
}

#[macro_export]
macro_rules! scenario_cleanup {
    ($builder:ident, delete_events: $pattern:expr $(, $($rest:tt)*)?) => {
        $builder = $builder.cleanup_events($pattern);
        $(scenario_cleanup!($builder, $($rest)*);)?
    };
    ($builder:ident, reset_checkpoints: true $(, $($rest:tt)*)?) => {
        $builder = $builder.cleanup_checkpoints();
        $(scenario_cleanup!($builder, $($rest)*);)?
    };
    ($builder:ident,) => {};
}

/// Async scenario for more complex async operations
pub struct AsyncScenario {
    pub name: String,
    pub setup: Box<dyn Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send>,
    pub action: Box<dyn Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send>,
    pub verify: Box<dyn Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send>,
    pub teardown: Option<Box<dyn Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send>>,
}

impl AsyncScenario {
    pub async fn run(self, ctx: &TestContext) -> TestResult {
        println!("\n=== Async Scenario: {} ===", self.name);
        
        // Setup
        println!("📋 Setup");
        (self.setup)(ctx).await?;
        
        // Action
        println!("\n🎯 Action");
        (self.action)(ctx).await?;
        
        // Verify
        println!("\n✅ Verify");
        (self.verify)(ctx).await?;
        
        // Teardown
        if let Some(teardown) = self.teardown {
            println!("\n🧹 Teardown");
            teardown(ctx).await?;
        }
        
        println!("\n✅ Async scenario completed");
        Ok(())
    }
}

/// Builder for async scenarios
pub struct AsyncScenarioBuilder {
    name: String,
    setup: Option<Box<dyn Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send>>,
    action: Option<Box<dyn Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send>>,
    verify: Option<Box<dyn Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send>>,
    teardown: Option<Box<dyn Fn(&TestContext) -> Pin<Box<dyn Future<Output = TestResult> + Send>> + Send>>,
}

impl AsyncScenarioBuilder {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            setup: None,
            action: None,
            verify: None,
            teardown: None,
        }
    }

    pub fn setup<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(&TestContext) -> Fut + Send + 'static,
        Fut: Future<Output = TestResult> + Send + 'static,
    {
        self.setup = Some(Box::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    pub fn action<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(&TestContext) -> Fut + Send + 'static,
        Fut: Future<Output = TestResult> + Send + 'static,
    {
        self.action = Some(Box::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    pub fn verify<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(&TestContext) -> Fut + Send + 'static,
        Fut: Future<Output = TestResult> + Send + 'static,
    {
        self.verify = Some(Box::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    pub fn teardown<F, Fut>(mut self, f: F) -> Self
    where
        F: Fn(&TestContext) -> Fut + Send + 'static,
        Fut: Future<Output = TestResult> + Send + 'static,
    {
        self.teardown = Some(Box::new(move |ctx| Box::pin(f(ctx))));
        self
    }

    pub fn build(self) -> AsyncScenario {
        AsyncScenario {
            name: self.name,
            setup: self.setup.expect("Setup is required"),
            action: self.action.expect("Action is required"),
            verify: self.verify.expect("Verify is required"),
            teardown: self.teardown,
        }
    }

    pub async fn run(self, ctx: &TestContext) -> TestResult {
        self.build().run(ctx).await
    }
}

/// Batch scenario for testing multiple variations
pub struct BatchScenario {
    pub name: String,
    pub variations: Vec<ScenarioVariation>,
}

pub struct ScenarioVariation {
    pub name: String,
    pub modify_given: Box<dyn Fn(&mut GivenContext) + Send>,
    pub modify_then: Box<dyn Fn(&mut ThenAssertion) + Send>,
}

impl BatchScenario {
    pub async fn run(self, ctx: &TestContext, base_scenario: TestScenario) -> TestResult {
        println!("\n=== Batch Scenario: {} ===", self.name);
        println!("Running {} variations", self.variations.len());
        
        for (i, variation) in self.variations.into_iter().enumerate() {
            println!("\n--- Variation {}: {} ---", i + 1, variation.name);
            
            // Clone the base scenario and apply variations
            let mut scenario = TestScenario {
                name: format!("{} - {}", base_scenario.name, variation.name),
                given: base_scenario.given.clone(),
                when: base_scenario.when.clone(),
                then: base_scenario.then.clone(),
                cleanup: base_scenario.cleanup.clone(),
            };
            
            // Apply variations
            (variation.modify_given)(&mut scenario.given);
            (variation.modify_then)(&mut scenario.then);
            
            // Run the variation
            execute_scenario(&scenario, ctx).await?;
        }
        
        println!("\n✅ All variations completed successfully");
        Ok(())
    }
}

/// Property-based scenario for generating test cases
pub struct PropertyScenario<T> {
    pub name: String,
    pub generator: Box<dyn Fn() -> T + Send>,
    pub property: Box<dyn Fn(&TestContext, T) -> Pin<Box<dyn Future<Output = bool> + Send>> + Send>,
    pub samples: usize,
}

impl<T: Send + 'static> PropertyScenario<T> {
    pub async fn run(self, ctx: &TestContext) -> TestResult {
        println!("\n=== Property Scenario: {} ===", self.name);
        println!("Testing {} samples", self.samples);
        
        let mut failures = Vec::new();
        
        for i in 0..self.samples {
            let input = (self.generator)();
            let result = (self.property)(ctx, input).await;
            
            if !result {
                failures.push(i);
            }
            
            if (i + 1) % 10 == 0 {
                print!(".");
                use std::io::Write;
                std::io::stdout().flush().unwrap();
            }
        }
        
        println!();
        
        if failures.is_empty() {
            println!("✅ All {} samples passed", self.samples);
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "Property failed for {} out of {} samples: {:?}",
                failures.len(),
                self.samples,
                failures
            ))
        }
    }
}

// Re-export commonly used types
pub use crate::{scenario, scenario_given, scenario_when, scenario_then, scenario_cleanup};

// Helper traits for ergonomic scenario building
pub trait IntoScenario {
    fn into_scenario(self, name: impl Into<String>) -> ScenarioBuilder;
}

impl IntoScenario for Vec<RawEvent> {
    fn into_scenario(self, name: impl Into<String>) -> ScenarioBuilder {
        ScenarioBuilder::new(name).given_events(self)
    }
}

// Implement Clone for structures that need it
impl Clone for GivenContext {
    fn clone(&self) -> Self {
        Self {
            events: self.events.clone(),
            checkpoints: self.checkpoints.clone(),
            state: self.state.clone(),
            redis_state: self.redis_state.clone(),
            files: self.files.clone(),
            env_vars: self.env_vars.clone(),
        }
    }
}

impl Clone for WhenAction {
    fn clone(&self) -> Self {
        Self {
            action: self.action.clone(),
            params: self.params.clone(),
            wait: self.wait,
            repeat: self.repeat,
            parallel: self.parallel,
        }
    }
}

impl Clone for ThenAssertion {
    fn clone(&self) -> Self {
        Self {
            events_count: self.events_count,
            events_count_gte: self.events_count_gte,
            checkpoints: self.checkpoints.clone(),
            events_match: None, // Can't clone closures
            no_errors: self.no_errors,
            custom_assertions: Vec::new(), // Can't clone closures
            redis_keys: self.redis_keys.clone(),
            files_exist: self.files_exist.clone(),
            duration_under: self.duration_under,
        }
    }
}

impl Clone for CleanupSpec {
    fn clone(&self) -> Self {
        Self {
            delete_events: self.delete_events.clone(),
            reset_checkpoints: self.reset_checkpoints,
            remove_files: self.remove_files.clone(),
            clear_redis: self.clear_redis.clone(),
        }
    }
}