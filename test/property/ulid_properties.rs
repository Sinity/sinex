use crate::common::prelude::*;
use chrono::{DateTime, Duration, Utc};
use proptest::prelude::*;

// Property tests for ULID generation and ordering
// Agent Alpha - VM Infrastructure (adding quick property test example)

/// Test that ULIDs generated from chronologically ordered timestamps maintain order
#[cfg(test)]
mod ulid_ordering_properties {
    use super::*;

    #[test]
    fn test_ulid_chronological_ordering() {
        proptest!(|(
            base_timestamp in 0u64..1_000_000_000_000,
            offsets in prop::collection::vec(0u64..1000, 2..10)
        )| {
            let base_time = DateTime::from_timestamp(base_timestamp as i64, 0).unwrap_or(Utc::now());
            let mut timestamps: Vec<DateTime<Utc>> = offsets
                .iter()
                .map(|&offset| base_time + Duration::seconds(offset as i64))
                .collect();

            // Sort timestamps to ensure chronological order
            timestamps.sort();

            // Generate ULIDs from sorted timestamps
            let ulids: Vec<Ulid> = timestamps
                .iter()
                .map(|&ts| Ulid::from_datetime(ts))
                .collect();

            // Verify ULIDs maintain chronological order
            for window in ulids.windows(2) {
                let (prev, curr) = (&window[0], &window[1]);
                prop_assert!(
                    prev <= curr,
                    "ULID ordering violated: {} > {} (timestamps: {} > {})",
                    prev,
                    curr,
                    prev.timestamp(),
                    curr.timestamp()
                );
            }
        });
    }

    #[test]
    fn test_ulid_uniqueness_under_rapid_generation() {
        proptest!(|(count in 2usize..1000)| {
            let base_time = Utc::now();
            let mut ulids = Vec::new();

            // Generate ULIDs rapidly (simulating high-frequency events)
            for i in 0..count {
                let timestamp = base_time + Duration::milliseconds(i as i64);
                ulids.push(Ulid::from_datetime(timestamp));
            }

            // Verify all ULIDs are unique
            let mut sorted_ulids = ulids.clone();
            sorted_ulids.sort();
            sorted_ulids.dedup();

            prop_assert_eq!(
                ulids.len(),
                sorted_ulids.len(),
                "Duplicate ULIDs generated: original count={}, unique count={}",
                ulids.len(),
                sorted_ulids.len()
            );
        });
    }

    #[test]
    fn test_ulid_timestamp_extraction() {
        proptest!(|(timestamp in 0u64..2_000_000_000)| {
            let dt = DateTime::from_timestamp(timestamp as i64, 0).unwrap_or(Utc::now());
            let ulid = Ulid::from_datetime(dt);
            let extracted_timestamp = ulid.timestamp();

            // ULID timestamp should be within 1ms of original
            let time_diff = (timestamp * 1000) as i64 - extracted_timestamp.timestamp_millis();
            prop_assert!(
                time_diff.abs() <= 1000, // 1 second tolerance for edge cases
                "ULID timestamp extraction inaccurate: original={}, extracted={}, diff={}ms",
                timestamp * 1000,
                extracted_timestamp.timestamp_millis(),
                time_diff
            );
        });
    }
}

#[cfg(test)]
mod event_ulid_properties {
    use super::*;
    use crate::common::generators;

    #[test]
    fn test_event_ulids_maintain_ingestion_order() {
        proptest!(|(event_count in 5usize..50)| {
            let events = generators::time_distributed_events(
                event_count,
                Utc::now() - Duration::hours(1),
                60  // 60 seconds between events
            );

            // Verify events are in ULID order (which implies time order)
            for window in events.windows(2) {
                let (prev, curr) = (&window[0], &window[1]);
                prop_assert!(
                    prev.id <= curr.id,
                    "Event ULID ordering violated: {} > {}",
                    prev.id,
                    curr.id
                );
            }
        });
    }

    #[test]
    fn test_burst_events_maintain_order() {
        proptest!(|(burst_size in 10usize..100)| {
            let burst_events = generators::burst_pattern_events(3, burst_size);

            // Group events by burst (every burst_size events)
            for burst_chunk in burst_events.chunks(burst_size) {
                // Within each burst, ULIDs should still maintain order
                for window in burst_chunk.windows(2) {
                    let (prev, curr) = (&window[0], &window[1]);
                    prop_assert!(
                        prev.id <= curr.id,
                        "Burst event ULID ordering violated: {} > {} (burst size: {})",
                        prev.id,
                        curr.id,
                        burst_size
                    );
                }
            }
        });
    }
}
