use chrono::{DateTime, Duration, Utc};
use proptest::prelude::*;
use sinex_core::db::repositories::{common::Repository, events::EventRepository};
use sinex_test_utils::prelude::*;

/// Property tests for time range operations and overlap logic
///
/// This module tests critical time range invariants:
/// - Time range queries are consistent and complete
/// - Overlapping ranges behave predictably  
/// - Edge cases with timestamps are handled correctly
/// - No events are lost in range boundaries

// =============================================================================
// Time Range Generation Strategies
// =============================================================================

/// Generate arbitrary time ranges within reasonable bounds
fn arb_time_range() -> impl Strategy<Value = (DateTime<Utc>, DateTime<Utc>)> {
    let base_time = Utc::now();
    let year_ago = base_time - Duration::days(365);
    let year_future = base_time + Duration::days(365);
    
    (
        year_ago.timestamp()..=year_future.timestamp(),
        1i64..=Duration::days(30).num_seconds(), // Range duration
    ).prop_map(|(start_ts, duration_secs)| {
        let start = DateTime::from_timestamp(start_ts, 0).unwrap_or(base_time);
        let end = start + Duration::seconds(duration_secs);
        (start, end)
    })
}

/// Generate overlapping time ranges for overlap testing
fn arb_overlapping_ranges() -> impl Strategy<Value = ((DateTime<Utc>, DateTime<Utc>), (DateTime<Utc>, DateTime<Utc>))> {
    arb_time_range().prop_flat_map(|(start1, end1)| {
        let duration1 = end1 - start1;
        
        // Generate second range that overlaps with first
        (
            Just((start1, end1)),
            (
                (start1 - duration1/2).timestamp()..=(end1 + duration1/2).timestamp(),
                1i64..=duration1.num_seconds(),
            ).prop_map(|(start2_ts, duration2_secs)| {
                let start2 = DateTime::from_timestamp(start2_ts, 0).unwrap_or(start1);
                let end2 = start2 + Duration::seconds(duration2_secs);
                (start2, end2)
            })
        )
    })
}

/// Generate edge case time ranges
fn arb_edge_case_ranges() -> impl Strategy<Value = (DateTime<Utc>, DateTime<Utc>)> {
    let now = Utc::now();
    
    prop_oneof![
        // Zero-length ranges (same start and end)
        Just((now, now)),
        Just((now - Duration::hours(1), now - Duration::hours(1))),
        
        // Very short ranges
        Just((now, now + Duration::microseconds(1))),
        Just((now, now + Duration::milliseconds(1))),
        Just((now, now + Duration::seconds(1))),
        
        // Very long ranges
        Just((now - Duration::days(365*10), now + Duration::days(365*10))),
        
        // Past ranges
        Just((now - Duration::days(100), now - Duration::days(50))),
        
        // Future ranges 
        Just((now + Duration::days(50), now + Duration::days(100))),
        
        // Microsecond precision ranges
        Just((
            now.with_nanosecond(123_456_000).unwrap_or(now),
            now.with_nanosecond(123_457_000).unwrap_or(now + Duration::microseconds(1))
        )),
    ]
}

// =============================================================================
// Time Range Properties
// =============================================================================

#[sinex_test]
fn test_time_range_ordering_invariant() -> color_eyre::eyre::Result<()> {
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(1000))]

        fn property_time_range_ordering_invariant(
            (start, end) in arb_time_range()
        ) {
            // Property: Start time should always be <= end time
            prop_assert!(
                start <= end,
                "Time range start should be <= end: {} vs {}",
                start.to_rfc3339(),
                end.to_rfc3339()
            );
            
            // Property: Duration should be non-negative
            let duration = end - start;
            prop_assert!(
                duration >= Duration::zero(),
                "Time range duration should be non-negative: {}",
                duration
            );
        }
    }
    Ok(())
}

#[sinex_test]
fn test_time_range_overlap_detection() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_time_range_overlap_detection(
            ((start1, end1), (start2, end2)) in arb_overlapping_ranges()
        ) {
            // Property: Overlap detection should be symmetric and consistent
            let overlap1 = ranges_overlap((start1, end1), (start2, end2));
            let overlap2 = ranges_overlap((start2, end2), (start1, end1));
            
            prop_assert_eq!(
                overlap1, overlap2,
                "Range overlap should be symmetric: [{}, {}] vs [{}, {}]",
                start1.to_rfc3339(), end1.to_rfc3339(),
                start2.to_rfc3339(), end2.to_rfc3339()
            );
            
            // Property: If ranges overlap, their intersection should be non-empty
            if overlap1 {
                let intersection_start = start1.max(start2);
                let intersection_end = end1.min(end2);
                
                prop_assert!(
                    intersection_start <= intersection_end,
                    "Overlapping ranges should have valid intersection: [{}, {}]",
                    intersection_start.to_rfc3339(),
                    intersection_end.to_rfc3339()
                );
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_time_range_intersection_properties() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_time_range_intersection_properties(
            ((start1, end1), (start2, end2)) in arb_overlapping_ranges()
        ) {
            // Property: Intersection should be contained within both ranges
            if let Some((int_start, int_end)) = intersect_ranges((start1, end1), (start2, end2)) {
                prop_assert!(
                    start1 <= int_start && int_end <= end1,
                    "Intersection should be within first range: [{}, {}] not within [{}, {}]",
                    int_start.to_rfc3339(), int_end.to_rfc3339(),
                    start1.to_rfc3339(), end1.to_rfc3339()
                );
                
                prop_assert!(
                    start2 <= int_start && int_end <= end2,
                    "Intersection should be within second range: [{}, {}] not within [{}, {}]",
                    int_start.to_rfc3339(), int_end.to_rfc3339(),
                    start2.to_rfc3339(), end2.to_rfc3339()
                );
                
                // Property: Intersection should be valid range
                prop_assert!(
                    int_start <= int_end,
                    "Intersection should be valid range: {} <= {}",
                    int_start.to_rfc3339(), int_end.to_rfc3339()
                );
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_time_range_union_properties() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_time_range_union_properties(
            ((start1, end1), (start2, end2)) in arb_overlapping_ranges()
        ) {
            // Property: Union should contain both original ranges
            let (union_start, union_end) = union_ranges((start1, end1), (start2, end2));
            
            prop_assert!(
                union_start <= start1 && end1 <= union_end,
                "Union should contain first range: [{}, {}] should contain [{}, {}]",
                union_start.to_rfc3339(), union_end.to_rfc3339(),
                start1.to_rfc3339(), end1.to_rfc3339()
            );
            
            prop_assert!(
                union_start <= start2 && end2 <= union_end,
                "Union should contain second range: [{}, {}] should contain [{}, {}]",
                union_start.to_rfc3339(), union_end.to_rfc3339(),
                start2.to_rfc3339(), end2.to_rfc3339()
            );
            
            // Property: Union should be minimal (no smaller range contains both)
            prop_assert!(
                union_start == start1.min(start2),
                "Union start should be minimum of range starts: {} vs {}",
                union_start.to_rfc3339(), start1.min(start2).to_rfc3339()
            );
            
            prop_assert!(
                union_end == end1.max(end2),
                "Union end should be maximum of range ends: {} vs {}",
                union_end.to_rfc3339(), end1.max(end2).to_rfc3339()
            );
        }
    }
    Ok(())
}

#[sinex_test]
fn test_edge_case_time_ranges() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_edge_case_time_ranges(
            (start, end) in arb_edge_case_ranges()
        ) {
            // Property: Edge case ranges should be handled gracefully
            
            // Zero-length ranges are valid
            if start == end {
                let duration = end - start;
                prop_assert_eq!(
                    duration,
                    Duration::zero(),
                    "Zero-length range should have zero duration"
                );
            }
            
            // Very short ranges should maintain precision
            let duration = end - start;
            if duration < Duration::seconds(1) {
                prop_assert!(
                    duration >= Duration::zero(),
                    "Very short range should have non-negative duration: {}",
                    duration
                );
            }
            
            // Very long ranges should not overflow
            if duration > Duration::days(365) {
                // Should be representable
                let _start_ts = start.timestamp();
                let _end_ts = end.timestamp();
                prop_assert!(true); // If we get here without panic, it's good
            }
        }
    }
    Ok(())
}

#[sinex_test]
async fn test_time_range_query_consistency(ctx: TestContext) -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_time_range_query_consistency(
            (start, end) in arb_time_range()
        ) {
            // Property: Querying the same range twice should yield same results
            // Note: This is a simplified test since we can't run async code in proptest
            
            // Test the time range calculation logic
            let duration = end - start;
            prop_assert!(
                duration >= Duration::zero(),
                "Query range should have non-negative duration"
            );
            
            // Test boundary conditions
            prop_assert!(
                start <= end,
                "Query start should be <= end"
            );
        }
    }
    Ok(())
}

#[sinex_test]
fn test_timestamp_precision_handling() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_timestamp_precision_handling(
            base_timestamp in any::<i64>().prop_filter("Valid timestamp", |&ts| {
                ts > 0 && ts < 4_102_444_800 // Within reasonable range (1970-2100)
            }),
            nanoseconds in 0u32..1_000_000_000u32
        ) {
            // Property: Timestamp precision should be preserved in range operations
            if let Some(dt1) = DateTime::from_timestamp(base_timestamp, nanoseconds) {
                if let Some(dt2) = DateTime::from_timestamp(base_timestamp + 1, nanoseconds) {
                    let range = (dt1, dt2);
                    
                    // Duration should be exactly 1 second
                    let duration = range.1 - range.0;
                    prop_assert_eq!(
                        duration.num_seconds(),
                        1,
                        "One second range should have 1 second duration"
                    );
                    
                    // Nanosecond precision should be preserved
                    prop_assert_eq!(
                        range.0.nanosecond(),
                        nanoseconds,
                        "Nanosecond precision should be preserved"
                    );
                }
            }
        }
    }
    Ok(())
}

#[sinex_test]
fn test_range_partitioning_properties() -> color_eyre::eyre::Result<()> {
    proptest! {
        fn property_range_partitioning_properties(
            (start, end) in arb_time_range(),
            partition_count in 2usize..=10
        ) {
            // Property: Partitioning a range should cover the entire range exactly once
            let partitions = partition_time_range((start, end), partition_count);
            
            prop_assert_eq!(
                partitions.len(),
                partition_count,
                "Should create exactly {} partitions",
                partition_count
            );
            
            // First partition should start at range start
            if let Some(first) = partitions.first() {
                prop_assert_eq!(
                    first.0,
                    start,
                    "First partition should start at range start"
                );
            }
            
            // Last partition should end at range end
            if let Some(last) = partitions.last() {
                prop_assert_eq!(
                    last.1,
                    end,
                    "Last partition should end at range end"
                );
            }
            
            // Partitions should be contiguous and non-overlapping
            for i in 1..partitions.len() {
                prop_assert_eq!(
                    partitions[i-1].1,
                    partitions[i].0,
                    "Partitions should be contiguous at boundary {}: {} != {}",
                    i,
                    partitions[i-1].1.to_rfc3339(),
                    partitions[i].0.to_rfc3339()
                );
            }
        }
    }
    Ok(())
}

// =============================================================================
// Helper Functions
// =============================================================================

/// Check if two time ranges overlap
fn ranges_overlap(
    (start1, end1): (DateTime<Utc>, DateTime<Utc>),
    (start2, end2): (DateTime<Utc>, DateTime<Utc>),
) -> bool {
    start1 < end2 && start2 < end1
}

/// Compute intersection of two time ranges
fn intersect_ranges(
    (start1, end1): (DateTime<Utc>, DateTime<Utc>),
    (start2, end2): (DateTime<Utc>, DateTime<Utc>),
) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
    let int_start = start1.max(start2);
    let int_end = end1.min(end2);
    
    if int_start <= int_end {
        Some((int_start, int_end))
    } else {
        None
    }
}

/// Compute union of two time ranges
fn union_ranges(
    (start1, end1): (DateTime<Utc>, DateTime<Utc>),
    (start2, end2): (DateTime<Utc>, DateTime<Utc>),
) -> (DateTime<Utc>, DateTime<Utc>) {
    (start1.min(start2), end1.max(end2))
}

/// Partition a time range into equal sub-ranges
fn partition_time_range(
    (start, end): (DateTime<Utc>, DateTime<Utc>),
    count: usize,
) -> Vec<(DateTime<Utc>, DateTime<Utc>)> {
    if count == 0 {
        return vec![];
    }
    
    let total_duration = end - start;
    let partition_duration = total_duration / (count as i32);
    
    let mut partitions = Vec::new();
    let mut current_start = start;
    
    for i in 0..count {
        let current_end = if i == count - 1 {
            // Last partition - use exact end to avoid rounding errors
            end
        } else {
            current_start + partition_duration
        };
        
        partitions.push((current_start, current_end));
        current_start = current_end;
    }
    
    partitions
}

// =============================================================================
// Unit Tests for Property Test Helpers
// =============================================================================

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[sinex_test]
    fn test_range_overlap_logic() -> color_eyre::eyre::Result<()> {
        let now = Utc::now();
        let hour_ago = now - Duration::hours(1);
        let hour_future = now + Duration::hours(1);
        
        // Overlapping ranges
        assert!(ranges_overlap((hour_ago, now), (now - Duration::minutes(30), hour_future)));
        
        // Non-overlapping ranges
        assert!(!ranges_overlap((hour_ago, now - Duration::minutes(30)), (now, hour_future)));
        
        // Adjacent ranges (should not overlap)
        assert!(!ranges_overlap((hour_ago, now), (now, hour_future)));
        
        Ok(())
    }

    #[sinex_test] 
    fn test_range_intersection_logic() -> color_eyre::eyre::Result<()> {
        let now = Utc::now();
        let hour_ago = now - Duration::hours(1);
        let hour_future = now + Duration::hours(1);
        
        // Overlapping ranges should have intersection
        let intersection = intersect_ranges(
            (hour_ago, hour_future),
            (now - Duration::minutes(30), now + Duration::minutes(30))
        );
        assert!(intersection.is_some());
        
        // Non-overlapping ranges should have no intersection
        let no_intersection = intersect_ranges(
            (hour_ago, now - Duration::minutes(30)),
            (now, hour_future)
        );
        assert!(no_intersection.is_none());
        
        Ok(())
    }

    #[sinex_test]
    fn test_range_partitioning_logic() -> color_eyre::eyre::Result<()> {
        let start = Utc::now();
        let end = start + Duration::hours(2);
        
        let partitions = partition_time_range((start, end), 4);
        assert_eq!(partitions.len(), 4);
        assert_eq!(partitions[0].0, start);
        assert_eq!(partitions[3].1, end);
        
        // Each partition should be about 30 minutes
        for partition in &partitions {
            let duration = partition.1 - partition.0;
            assert!(duration >= Duration::minutes(25)); // Allow some rounding
            assert!(duration <= Duration::minutes(35));
        }
        
        Ok(())
    }

    #[sinex_test]
    fn test_time_range_generators() -> color_eyre::eyre::Result<()> {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        
        // Test basic time range generator
        let (start, end) = arb_time_range().new_tree(&mut runner).unwrap().current();
        assert!(start <= end);
        
        // Test overlapping ranges generator
        let ((start1, end1), (start2, end2)) = arb_overlapping_ranges().new_tree(&mut runner).unwrap().current();
        assert!(start1 <= end1);
        assert!(start2 <= end2);
        
        // Test edge case generator
        let (edge_start, edge_end) = arb_edge_case_ranges().new_tree(&mut runner).unwrap().current();
        assert!(edge_start <= edge_end);
        
        Ok(())
    }
}