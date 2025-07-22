use crate::common::prelude::*;
use crate::common::property_helpers::*;
use proptest::test_runner::TestRunner;
use proptest::prelude::*;
use proptest::strategy::ValueTree;
use sinex_db::RawEvent;
use std::collections::{HashMap, HashSet};
use chrono::{DateTime, Utc};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn automaton_event_transformation_consistency(
        events in arbitrary_event_batch()
    ) {
        // Property: Automaton transformations should be deterministic
        let automaton = TestAutomaton::new("test-automaton");
        
        // Process events twice
        let first_run: Vec<_> = events.iter()
            .filter_map(|e| automaton.process_event(e.clone()).ok())
            .flatten()
            .collect();
            
        let second_run: Vec<_> = events.iter()
            .filter_map(|e| automaton.process_event(e.clone()).ok())
            .flatten()
            .collect();
        
        // Results should be identical
        assert_eq!(first_run.len(), second_run.len(), 
                   "Same events should produce same number of outputs");
        
        for (out1, out2) in first_run.iter().zip(second_run.iter()) {
            assert_eq!(out1.event_type, out2.event_type, 
                       "Output event types should match");
            assert_eq!(out1.source, out2.source,
                       "Output sources should match");
        }
    }

    #[test]
    fn automaton_state_accumulation(
        event_sequences in proptest::collection::vec(arbitrary_event_batch(), 1..5)
    ) {
        // Property: Automaton state should accumulate correctly across batches
        let automaton = TestAutomaton::new("accumulator-automaton");
        let mut total_processed = 0;
        let mut seen_event_types = HashSet::new();
        
        for batch in event_sequences {
            for event in batch {
                seen_event_types.insert(event.event_type.clone());
                
                if let Ok(outputs) = automaton.process_event(event) {
                    total_processed += 1;
                    
                    // Verify outputs reference accumulated state
                    for output in outputs {
                        if let Some(state_info) = output.payload.get("accumulated_types") {
                            if let Some(types) = state_info.as_array() {
                                assert!(types.len() <= seen_event_types.len(),
                                        "Accumulated types should not exceed seen types");
                            }
                        }
                    }
                }
            }
        }
        
        assert_eq!(automaton.get_processed_count(), total_processed,
                   "Processed count should match actual processing");
    }

    #[test]
    fn automaton_filtering_rules(
        events in arbitrary_event_batch(),
        filter_probability in 0.0f64..1.0
    ) {
        // Property: Filtering rules should be consistently applied
        let automaton = FilteringAutomaton::new(filter_probability);
        
        let outputs: Vec<_> = events.iter()
            .filter_map(|e| automaton.process_event(e.clone()).ok())
            .flatten()
            .collect();
        
        // Check filtering ratio is approximately correct
        let output_ratio = outputs.len() as f64 / events.len().max(1) as f64;
        
        // Allow for statistical variance
        let expected_ratio = 1.0 - filter_probability;
        let tolerance = 0.2; // 20% tolerance for randomness
        
        if events.len() > 10 {
            assert!((output_ratio - expected_ratio).abs() < tolerance,
                    "Filtering ratio {} should be close to expected {}",
                    output_ratio, expected_ratio);
        }
        
        // All outputs should have filter metadata
        for output in outputs {
            assert!(output.payload.get("filter_applied").is_some(),
                    "Filtered events should have metadata");
        }
    }

    #[test]
    fn automaton_aggregation_correctness(
        event_groups in proptest::collection::hash_map(
            event_types(),
            proptest::collection::vec(arbitrary_event(), 1..20),
            1..5
        )
    ) {
        // Property: Aggregation automata should correctly summarize event groups
        let automaton = AggregationAutomaton::new();
        
        // Process all events
        let mut type_counts = HashMap::new();
        for (event_type, events) in &event_groups {
            type_counts.insert(event_type.clone(), events.len());
            
            for event in events {
                let _ = automaton.process_event(event.clone());
            }
        }
        
        // Get aggregation summary
        let summary = automaton.get_summary();
        
        // Verify counts match
        for (event_type, expected_count) in type_counts {
            let actual_count = summary.get(&event_type).copied().unwrap_or(0);
            assert_eq!(actual_count, expected_count,
                       "Aggregated count for {} should match", event_type);
        }
    }

    #[test]
    fn automaton_time_window_processing(
        events in time_ordered_batch(),
        window_size_secs in 60u64..3600u64
    ) {
        // Property: Time window automata should correctly group events
        let window_duration = chrono::Duration::seconds(window_size_secs as i64);
        let automaton = TimeWindowAutomaton::new(window_duration);
        
        // Process events and collect windows
        let mut windows = Vec::new();
        for event in events {
            if let Ok(outputs) = automaton.process_event(event.clone()) {
                for output in outputs {
                    if output.event_type.contains("window.complete") {
                        windows.push(output);
                    }
                }
            }
        }
        
        // Verify window properties
        for window in &windows {
            if let Some(window_data) = window.payload.get("window") {
                let start = window_data.get("start")
                    .and_then(|s| s.as_str())
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
                let end = window_data.get("end")
                    .and_then(|s| s.as_str())
                    .and_then(|s| DateTime::parse_from_rfc3339(s).ok());
                    
                if let (Some(start), Some(end)) = (start, end) {
                    let actual_duration = end.signed_duration_since(start);
                    assert!(actual_duration <= window_duration,
                            "Window duration should not exceed configured size");
                }
                
                // Events in window should have correct count
                if let Some(count) = window_data.get("event_count").and_then(|c| c.as_u64()) {
                    assert!(count > 0, "Window should contain at least one event");
                }
            }
        }
    }

    #[test]
    fn automaton_correlation_detection(
        related_events in related_events_batch()
    ) {
        // Property: Correlation automata should detect related events
        let automaton = CorrelationAutomaton::new();
        
        // Track which events are part of the related sequence
        let related_paths: HashSet<_> = related_events.iter()
            .filter_map(|e| e.payload.get("path").and_then(|p| p.as_str()))
            .map(|s| s.to_string())
            .collect();
        
        // Process events
        let mut correlations = Vec::new();
        for event in related_events {
            if let Ok(outputs) = automaton.process_event(event) {
                for output in outputs {
                    if output.event_type.contains("correlation.detected") {
                        correlations.push(output);
                    }
                }
            }
        }
        
        // Should detect correlations for related events
        assert!(!correlations.is_empty(), "Should detect correlations in related events");
        
        // Verify correlations reference the correct events
        for correlation in correlations {
            if let Some(corr_data) = correlation.payload.get("correlation") {
                if let Some(events) = corr_data.get("events").and_then(|e| e.as_array()) {
                    // All correlated events should be from our related set
                    for event_ref in events {
                        if let Some(path) = event_ref.get("path").and_then(|p| p.as_str()) {
                            assert!(related_paths.contains(path),
                                    "Correlation should only include related events");
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn automaton_error_handling(
        events in arbitrary_event_batch(),
        error_injection_rate in 0.0f64..0.3f64
    ) {
        // Property: Automata should handle errors gracefully
        let automaton = FaultyAutomaton::new(error_injection_rate);
        
        let mut success_count = 0;
        let mut error_count = 0;
        
        for event in events {
            match automaton.process_event(event) {
                Ok(_) => success_count += 1,
                Err(e) => {
                    error_count += 1;
                    // Errors should be meaningful
                    assert!(!e.to_string().is_empty(), "Error message should not be empty");
                }
            }
        }
        
        // Some events should succeed even with error injection
        if error_injection_rate < 0.9 {
            assert!(success_count > 0, "Some events should process successfully");
        }
        
        // Error rate should approximately match injection rate
        let total = success_count + error_count;
        if total > 10 {
            let actual_error_rate = error_count as f64 / total as f64;
            assert!((actual_error_rate - error_injection_rate).abs() < 0.3,
                    "Error rate should match injection rate approximately");
        }
    }

    #[test]
    fn automaton_output_chaining(
        initial_events in arbitrary_event_batch()
    ) {
        // Property: Automaton outputs should be valid inputs for other automata
        let automaton1 = TestAutomaton::new("stage1");
        let automaton2 = TestAutomaton::new("stage2");
        
        // First stage processing
        let stage1_outputs: Vec<_> = initial_events.iter()
            .filter_map(|e| automaton1.process_event(e.clone()).ok())
            .flatten()
            .collect();
        
        // Second stage processing
        let stage2_outputs: Vec<_> = stage1_outputs.iter()
            .filter_map(|e| automaton2.process_event(e.clone()).ok())
            .flatten()
            .collect();
        
        // All outputs should be valid events
        for output in &stage2_outputs {
            assert!(!output.id.is_nil(), "Output should have valid ID");
            assert!(!output.event_type.is_empty(), "Output should have event type");
            assert!(!output.source.is_empty(), "Output should have source");
            // Every event should have at least one timestamp
            // (TestEventBuilder likely ensures this)
        }
        
        // Chained processing should preserve lineage
        for output in stage2_outputs {
            if let Some(lineage) = output.payload.get("lineage") {
                if let Some(stages) = lineage.as_array() {
                    assert!(stages.len() >= 2, "Should track processing stages");
                }
            }
        }
    }

    #[test]
    fn automaton_performance_characteristics(
        event_batch_sizes in proptest::collection::vec(1usize..100, 1..10)
    ) {
        // Property: Processing time should scale reasonably with batch size
        let automaton = TestAutomaton::new("performance-test");
        let mut timings = Vec::new();
        
        for batch_size in event_batch_sizes {
            let events: Vec<_> = (0..batch_size)
                .map(|_| arbitrary_event().new_tree(&mut TestRunner::default()).unwrap().current())
                .collect();
            
            let start = std::time::Instant::now();
            for event in events {
                let _ = automaton.process_event(event);
            }
            let elapsed = start.elapsed();
            
            timings.push((batch_size, elapsed));
        }
        
        // Verify reasonable scaling
        if timings.len() > 2 {
            // Sort by batch size
            timings.sort_by_key(|(size, _)| *size);
            
            // Larger batches should generally take more time
            for window in timings.windows(2) {
                let (size1, time1) = window[0];
                let (size2, time2) = window[1];
                
                if size2 > size1 * 2 {
                    // If batch size doubles, time should not more than quadruple
                    assert!(time2.as_millis() < time1.as_millis() * 5,
                            "Processing time should scale sub-quadratically");
                }
            }
        }
    }
}

// Mock automaton implementations for testing
mod mock_automatons {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    
    pub struct TestAutomaton {
        name: String,
        processed_count: AtomicUsize,
    }
    
    impl TestAutomaton {
        pub fn new(name: &str) -> Self {
            Self {
                name: name.to_string(),
                processed_count: AtomicUsize::new(0),
            }
        }
        
        pub fn process_event(&self, mut event: RawEvent) -> AnyhowResult<Vec<RawEvent>> {
            self.processed_count.fetch_add(1, Ordering::SeqCst);
            
            // Simple transformation
            event.event_type = format!("{}.processed", event.event_type);
            event.source = format!("{}.{}", event.source, self.name);
            
            // Add processing metadata
            let mut payload = event.payload.as_object().cloned().unwrap_or_default();
            payload.insert("automaton".to_string(), json!(self.name));
            payload.insert("processed_at".to_string(), json!(Utc::now().to_rfc3339()));
            
            // Track lineage
            let lineage = payload.get("lineage")
                .and_then(|l| l.as_array())
                .cloned()
                .unwrap_or_default();
            let mut new_lineage = lineage;
            new_lineage.push(json!(self.name));
            payload.insert("lineage".to_string(), json!(new_lineage));
            
            event.payload = json!(payload);
            
            Ok(vec![event])
        }
        
        pub fn get_processed_count(&self) -> usize {
            self.processed_count.load(Ordering::SeqCst)
        }
    }
    
    pub struct FilteringAutomaton {
        filter_probability: f64,
    }
    
    impl FilteringAutomaton {
        pub fn new(filter_probability: f64) -> Self {
            Self { filter_probability }
        }
        
        pub fn process_event(&self, mut event: RawEvent) -> AnyhowResult<Vec<RawEvent>> {
            let random: f64 = rand::random();
            
            if random < self.filter_probability {
                // Filter out
                return Ok(vec![]);
            }
            
            // Pass through with metadata
            let mut payload = event.payload.as_object().cloned().unwrap_or_default();
            payload.insert("filter_applied".to_string(), json!(true));
            payload.insert("filter_probability".to_string(), json!(self.filter_probability));
            event.payload = json!(payload);
            
            Ok(vec![event])
        }
    }
    
    pub struct AggregationAutomaton {
        type_counts: Mutex<HashMap<String, usize>>,
    }
    
    impl AggregationAutomaton {
        pub fn new() -> Self {
            Self {
                type_counts: Mutex::new(HashMap::new()),
            }
        }
        
        pub fn process_event(&self, event: RawEvent) -> AnyhowResult<Vec<RawEvent>> {
            let mut counts = self.type_counts.lock().unwrap();
            *counts.entry(event.event_type.clone()).or_insert(0) += 1;
            Ok(vec![])
        }
        
        pub fn get_summary(&self) -> HashMap<String, usize> {
            self.type_counts.lock().unwrap().clone()
        }
    }
    
    pub struct TimeWindowAutomaton {
        window_duration: chrono::Duration,
        current_window: Mutex<Option<WindowState>>,
    }
    
    struct WindowState {
        start: DateTime<Utc>,
        events: Vec<RawEvent>,
    }
    
    impl TimeWindowAutomaton {
        pub fn new(window_duration: chrono::Duration) -> Self {
            Self {
                window_duration,
                current_window: Mutex::new(None),
            }
        }
        
        pub fn process_event(&self, event: RawEvent) -> AnyhowResult<Vec<RawEvent>> {
            let mut window = self.current_window.lock().unwrap();
            let event_time = event.ts_orig.unwrap_or_else(Utc::now);
            
            let mut outputs = vec![];
            
            match &mut *window {
                None => {
                    // Start new window
                    *window = Some(WindowState {
                        start: event_time,
                        events: vec![event],
                    });
                }
                Some(state) => {
                    if event_time >= state.start + self.window_duration {
                        // Complete current window
                        let window_event = TestEventBuilder::new("time-window-automaton", "window.complete")
                            .with_payload(json!({
                                "window": {
                                    "start": state.start.to_rfc3339(),
                                    "end": (state.start + self.window_duration).to_rfc3339(),
                                    "event_count": state.events.len(),
                                    "event_types": state.events.iter()
                                        .map(|e| &e.event_type)
                                        .collect::<HashSet<_>>(),
                                }
                            }))
                            .with_timestamp(event_time)
                            .build();
                        outputs.push(window_event);
                        
                        // Start new window
                        *window = Some(WindowState {
                            start: event_time,
                            events: vec![event],
                        });
                    } else {
                        // Add to current window
                        state.events.push(event);
                    }
                }
            }
            
            Ok(outputs)
        }
    }
    
    pub struct CorrelationAutomaton {
        recent_events: Mutex<Vec<RawEvent>>,
        correlation_window: usize,
    }
    
    impl CorrelationAutomaton {
        pub fn new() -> Self {
            Self {
                recent_events: Mutex::new(Vec::new()),
                correlation_window: 10,
            }
        }
        
        pub fn process_event(&self, event: RawEvent) -> AnyhowResult<Vec<RawEvent>> {
            let mut recent = self.recent_events.lock().unwrap();
            recent.push(event.clone());
            
            // Keep only recent events
            if recent.len() > self.correlation_window {
                recent.remove(0);
            }
            
            let mut outputs = vec![];
            
            // Look for correlations (simplified: same path)
            if let Some(path) = event.payload.get("path").and_then(|p| p.as_str()) {
                let correlated: Vec<_> = recent.iter()
                    .filter(|e| {
                        e.payload.get("path")
                            .and_then(|p| p.as_str())
                            .map(|p| p == path)
                            .unwrap_or(false)
                    })
                    .collect();
                
                if correlated.len() > 1 {
                    let correlation_event = TestEventBuilder::new("correlation-automaton", "correlation.detected")
                        .with_payload(json!({
                            "correlation": {
                                "type": "path-based",
                                "path": path,
                                "event_count": correlated.len(),
                                "events": correlated.iter().map(|e| json!({
                                    "id": e.id.to_string(),
                                    "type": &e.event_type,
                                    "path": path,
                                })).collect::<Vec<_>>(),
                            }
                        }))
                        .build();
                    outputs.push(correlation_event);
                }
            }
            
            Ok(outputs)
        }
    }
    
    pub struct FaultyAutomaton {
        error_rate: f64,
    }
    
    impl FaultyAutomaton {
        pub fn new(error_rate: f64) -> Self {
            Self { error_rate }
        }
        
        pub fn process_event(&self, event: RawEvent) -> AnyhowResult<Vec<RawEvent>> {
            let random: f64 = rand::random();
            
            if random < self.error_rate {
                return Err(anyhow::anyhow!("Simulated processing error"));
            }
            
            Ok(vec![event])
        }
    }
}

use mock_automatons::*;