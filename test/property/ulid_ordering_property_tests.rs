use proptest::prelude::*;
use proptest::strategy::ValueTree;
use sinex_ulid::Ulid;
use sinex_db::{create_test_pool, run_migrations, queries::insert_raw_event};
use std::collections::HashSet;
use chrono::{Utc, DateTime, Duration as ChronoDuration};
use serde_json::json;
use std::str::FromStr;

/// Generate a strategy for creating lists of ULIDs with controlled time gaps
fn arb_ulid_sequence(min_size: usize, max_size: usize) -> impl Strategy<Value = Vec<Ulid>> {
    (min_size..=max_size).prop_flat_map(|size| {
        // Start with a base time and create ULIDs with small incremental delays
        prop::collection::vec(any::<u64>().prop_map(|delay_ms| delay_ms % 1000), size)
            .prop_map(move |delays| {
                let mut ulids = Vec::new();
                let base_time = Utc::now() - ChronoDuration::hours(1); // Start an hour ago
                let mut current_time = base_time;
                
                for delay_ms in delays {
                    current_time = current_time + ChronoDuration::milliseconds(delay_ms as i64 + 1);
                    ulids.push(Ulid::from_datetime(current_time));
                }
                ulids
            })
    })
}

/// Generate ULIDs from specific time ranges
fn arb_ulid_from_time_range(
    start: DateTime<Utc>, 
    end: DateTime<Utc>
) -> impl Strategy<Value = Ulid> {
    let start_ms = start.timestamp_millis();
    let end_ms = end.timestamp_millis();
    
    (start_ms..=end_ms).prop_map(|ts_ms| {
        let datetime = DateTime::from_timestamp_millis(ts_ms).unwrap_or(Utc::now());
        Ulid::from_datetime(datetime)
    })
}

proptest! {
    #[test]
    fn test_ulid_ordering_property_in_memory(
        ulids in arb_ulid_sequence(2, 20)
    ) {
        // Property: ULIDs generated with increasing timestamps should be ordered
        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();
        
        // The original sequence should already be sorted since we used increasing times
        prop_assert_eq!(ulids.clone(), sorted_ulids, 
            "ULIDs with increasing timestamps should already be in sorted order");
        
        // Property: Each ULID should be greater than the previous one
        for i in 1..ulids.len() {
            prop_assert!(ulids[i] > ulids[i-1], 
                "ULID at index {} ({}) should be greater than previous ({}) for monotonic sequence", 
                i, ulids[i], ulids[i-1]);
        }
        
        // Property: All ULIDs should be unique
        let unique_set: HashSet<_> = ulids.iter().collect();
        prop_assert_eq!(unique_set.len(), ulids.len(), 
            "All ULIDs in sequence should be unique");
    }
}

proptest! {
    #[test]
    fn test_ulid_database_ordering_property(
        ulid_count in 3..15usize,
        time_gap_seconds in 1..10u64,
    ) {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let database_url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
            
            let pool = create_test_pool(&database_url).await.expect("Failed to create pool");
            run_migrations(&pool).await.expect("Failed to run migrations");
            
            // Insert events with time delays and collect their generated ULIDs
            let mut generated_ulids = Vec::new();
            
            for i in 0..ulid_count {
                // Add small delay between insertions to ensure ULID ordering
                if i > 0 {
                    tokio::time::sleep(tokio::time::Duration::from_millis(time_gap_seconds * 100)).await;
                }
                
                let event = insert_raw_event(
                    &pool,
                    "property.ulid_ordering",
                    "ordering_test",
                    "localhost",
                    json!({"sequence": i}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await.expect("DB insert failed");
                
                generated_ulids.push(event.id);
            }
            
            // Property: Generated ULIDs should be in increasing order due to time separation
            for i in 1..generated_ulids.len() {
                prop_assert!(generated_ulids[i] > generated_ulids[i-1], 
                    "Generated ULID at index {} should be greater than previous due to time separation", i);
            }
            
            // Property: Database ordering should match generation order
            let db_ordered_ids: Vec<String> = sqlx::query_scalar(
                "SELECT id::text FROM raw.events 
                 WHERE source = 'property.ulid_ordering' 
                 ORDER BY id"
            )
            .fetch_all(&pool)
            .await
            .expect("Query failed");
            
            let expected_order: Vec<String> = generated_ulids.iter().map(|u| u.to_string()).collect();
            prop_assert_eq!(db_ordered_ids.clone(), expected_order, 
                "Database ordering by ULID should match generation order");
            
            // Property: Ordering by id should match ordering by ts_ingest
            let ts_ordered_ids: Vec<String> = sqlx::query_scalar(
                "SELECT id::text FROM raw.events 
                 WHERE source = 'property.ulid_ordering' 
                 ORDER BY ts_ingest"
            )
            .fetch_all(&pool)
            .await
            .expect("Query failed");
            
            prop_assert_eq!(db_ordered_ids, ts_ordered_ids, 
                "Ordering by ULID should match ordering by extracted timestamp");
            
            // Cleanup
            sqlx::query("DELETE FROM raw.events WHERE source = 'property.ulid_ordering'")
                .execute(&pool)
                .await
                .expect("Cleanup failed");
            
            Ok(())
        })?
    }
}

proptest! {
    #[test]
    fn test_ulid_range_query_property(
        batch1_size in 2..8usize,
        batch2_size in 2..8usize,
        gap_minutes in 1..30i64,
    ) {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let database_url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
            
            let pool = create_test_pool(&database_url).await.expect("Failed to create pool");
            run_migrations(&pool).await.expect("Failed to run migrations");
            
            let source_name = format!("property.range_test_{}", Ulid::new());
            
            // Create first batch of events with time gap
            let mut batch1_ulids = Vec::new();
            
            for i in 0..batch1_size {
                let event = insert_raw_event(
                    &pool,
                    &source_name,
                    "batch1_event",
                    "localhost",
                    json!({"batch": 1, "sequence": i}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await.expect("DB insert failed");
                
                batch1_ulids.push(event.id);
                
                // Small delay between events
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            
            // Create gap between batches
            tokio::time::sleep(tokio::time::Duration::from_secs(gap_minutes as u64)).await;
            
            // Get the timestamp of the last batch1 event for cutoff calculation
            let last_batch1_ulid = batch1_ulids.last().unwrap();
            let cutoff_time = last_batch1_ulid.timestamp() + ChronoDuration::milliseconds(500);
            let cutoff_ulid = Ulid::from_datetime(cutoff_time);
            
            // Create second batch of events
            let mut batch2_ulids = Vec::new();
            
            for i in 0..batch2_size {
                let event = insert_raw_event(
                    &pool,
                    &source_name,
                    "batch2_event",
                    "localhost", 
                    json!({"batch": 2, "sequence": i}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await.expect("DB insert failed");
                
                batch2_ulids.push(event.id);
                
                // Small delay between events
                tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            }
            
            // Property: Range queries should partition events correctly
            let count_before: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM raw.events 
                 WHERE source = $1 AND id < $2::ulid"
            )
            .bind(&source_name)
            .bind(cutoff_ulid.to_string())
            .fetch_one(&pool)
            .await
            .expect("Query failed");
            
            let count_after: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM raw.events 
                 WHERE source = $1 AND id >= $2::ulid"
            )
            .bind(&source_name) 
            .bind(cutoff_ulid.to_string())
            .fetch_one(&pool)
            .await
            .expect("Query failed");
            
            // Property: All batch1 ULIDs should be before cutoff
            for ulid in &batch1_ulids {
                prop_assert!(ulid < &cutoff_ulid, 
                    "Batch1 ULID {} should be before cutoff {}", ulid, cutoff_ulid);
            }
            
            // Property: All batch2 ULIDs should be after cutoff
            for ulid in &batch2_ulids {
                prop_assert!(ulid >= &cutoff_ulid, 
                    "Batch2 ULID {} should be >= cutoff {}", ulid, cutoff_ulid);
            }
            
            // Property: Range query counts should match batch sizes
            prop_assert_eq!(count_before as usize, batch1_size, 
                "Count before cutoff should match batch1 size");
            prop_assert_eq!(count_after as usize, batch2_size, 
                "Count after cutoff should match batch2 size");
            
            // Property: Total should equal sum of parts
            let total_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM raw.events WHERE source = $1"
            )
            .bind(&source_name)
            .fetch_one(&pool)
            .await
            .expect("Query failed");
            
            prop_assert_eq!(count_before + count_after, total_count, 
                "Range query counts should sum to total");
            prop_assert_eq!(total_count as usize, batch1_size + batch2_size, 
                "Total count should equal sum of batch sizes");
            
            // Cleanup
            sqlx::query("DELETE FROM raw.events WHERE source = $1")
                .bind(&source_name)
                .execute(&pool)
                .await
                .expect("Cleanup failed");
            
            Ok(())
        })?
    }
}

proptest! {
    #[test]
    fn test_ulid_timestamp_extraction_property(
        time_offset_hours in -24..24i64,
        time_offset_minutes in 0..60i64,
        time_offset_seconds in 0..60i64,
    ) {
        // Property: ULID timestamp extraction should be consistent and accurate
        let base_time = Utc::now();
        let target_time = base_time 
            + ChronoDuration::hours(time_offset_hours)
            + ChronoDuration::minutes(time_offset_minutes)
            + ChronoDuration::seconds(time_offset_seconds);
        
        let ulid = Ulid::from_datetime(target_time);
        let extracted_time = ulid.timestamp();
        
        // Property: Extracted timestamp should match input timestamp (within precision)
        let time_diff = extracted_time.signed_duration_since(target_time);
        prop_assert!(time_diff.num_milliseconds().abs() <= 1, 
            "Extracted timestamp should match input within 1ms: input={:?}, extracted={:?}, diff={}ms", 
            target_time, extracted_time, time_diff.num_milliseconds());
        
        // Property: ULID string representation should be consistent
        let ulid_str = ulid.to_string();
        let parsed_ulid = Ulid::from_str(&ulid_str).expect("Should parse ULID string");
        prop_assert_eq!(ulid, parsed_ulid, "ULID should round-trip through string representation");
        
        let parsed_time = parsed_ulid.timestamp();
        prop_assert_eq!(extracted_time, parsed_time, 
            "Timestamp should be consistent after string round-trip");
        
        // Property: ULID should be valid length and format
        prop_assert_eq!(ulid_str.len(), 26, "ULID string should be 26 characters");
        prop_assert!(ulid_str.chars().all(|c| "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(c)), 
            "ULID should only contain valid Crockford base32 characters");
    }
}

proptest! {
    #[test]
    fn test_ulid_monotonic_property_with_rapid_generation(
        generation_count in 5..50usize,
        delay_microseconds in 0..1000u64,
    ) {
        // Property: Rapidly generated ULIDs should maintain ordering even with small delays
        let mut ulids = Vec::new();
        let mut timestamps = Vec::new();
        
        for i in 0..generation_count {
            if delay_microseconds > 0 {
                std::thread::sleep(std::time::Duration::from_micros(delay_microseconds));
            }
            
            let ulid = Ulid::new();
            let timestamp = ulid.timestamp();
            
            ulids.push(ulid);
            timestamps.push(timestamp);
            
            // Property: Each ULID should be unique
            for j in 0..i {
                prop_assert!(ulid != ulids[j], 
                    "ULID at index {} should be unique (different from index {})", i, j);
            }
        }
        
        // Property: ULIDs should be in increasing order
        for i in 1..ulids.len() {
            prop_assert!(ulids[i] >= ulids[i-1], 
                "ULID at index {} should be >= previous ULID for monotonic sequence", i);
        }
        
        // Property: Timestamps should be non-decreasing (allowing equal for same millisecond)
        for i in 1..timestamps.len() {
            prop_assert!(timestamps[i] >= timestamps[i-1], 
                "Timestamp at index {} should be >= previous timestamp", i);
        }
        
        // Property: All ULIDs should be unique
        let unique_ulids: HashSet<_> = ulids.iter().collect();
        prop_assert_eq!(unique_ulids.len(), ulids.len(), 
            "All rapidly generated ULIDs should be unique");
        
        // Property: Sorted order should match generation order
        let mut sorted_ulids = ulids.clone();
        sorted_ulids.sort();
        prop_assert_eq!(ulids, sorted_ulids, 
            "ULIDs should already be in sorted order due to monotonic generation");
    }
}

proptest! {
    #[test] 
    fn test_ulid_foreign_key_consistency_property(
        num_relationships in 1..10usize,
    ) {
        let rt = tokio::runtime::Runtime::new().expect("Failed to create runtime");
        rt.block_on(async {
            let database_url = std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgresql:///sinex_dev?host=/run/postgresql".to_string());
            
            let pool = create_test_pool(&database_url).await.expect("Failed to create pool");
            run_migrations(&pool).await.expect("Failed to run migrations");
            
            let agent_name = format!("property_fk_test_{}", Ulid::new());
            
            // Create test agent
            sqlx::query(
                "INSERT INTO sinex_schemas.agent_manifests (agent_name, version, description) 
                 VALUES ($1, $2, $3)"
            )
            .bind(&agent_name)
            .bind("1.0.0")
            .bind("Property test agent")
            .execute(&pool)
            .await
            .expect("Agent creation failed");
            
            let mut event_ulids = Vec::new();
            let mut queue_ulids = Vec::new();
            
            // Create relationships
            for i in 0..num_relationships {
                // Insert event with ULID
                let event_ulid = Ulid::new();
                let event = insert_raw_event(
                    &pool,
                    "property.fk_test",
                    "foreign_key_test",
                    "localhost",
                    json!({"relationship": i}),
                    None,
                    Some("1.0.0"),
                    None,
                ).await.expect("Event insert failed");
                
                prop_assert_eq!(event.id, event_ulid, 
                    "Inserted event should have matching ULID");
                event_ulids.push(event_ulid);
                
                // Insert work queue item referencing the event
                let queue_ulid = Ulid::new();
                sqlx::query(
                    "INSERT INTO sinex_schemas.work_queue 
                     (queue_id, raw_event_id, target_agent_name, max_attempts) 
                     VALUES ($1::ulid, $2::ulid, $3, 3)"
                )
                .bind(queue_ulid.to_string())
                .bind(event_ulid.to_string())
                .bind(&agent_name)
                .execute(&pool)
                .await
                .expect("Queue insert failed");
                
                queue_ulids.push(queue_ulid);
            }
            
            // Property: All foreign key relationships should be queryable
            for i in 0..num_relationships {
                let found_event_id: String = sqlx::query_scalar(
                    "SELECT e.id::text 
                     FROM raw.events e 
                     JOIN sinex_schemas.work_queue q ON e.id = q.raw_event_id 
                     WHERE q.queue_id = $1::ulid"
                )
                .bind(queue_ulids[i].to_string())
                .fetch_one(&pool)
                .await
                .expect("FK query failed");
                
                prop_assert_eq!(found_event_id, event_ulids[i].to_string(), 
                    "Foreign key relationship {} should be consistent", i);
            }
            
            // Property: Reverse lookup should also work
            for i in 0..num_relationships {
                let found_queue_id: String = sqlx::query_scalar(
                    "SELECT q.queue_id::text 
                     FROM sinex_schemas.work_queue q 
                     WHERE q.raw_event_id = $1::ulid"
                )
                .bind(event_ulids[i].to_string())
                .fetch_one(&pool)
                .await
                .expect("Reverse FK query failed");
                
                prop_assert_eq!(found_queue_id, queue_ulids[i].to_string(), 
                    "Reverse foreign key lookup {} should be consistent", i);
            }
            
            // Property: Join count should match relationship count
            let join_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) 
                 FROM raw.events e 
                 JOIN sinex_schemas.work_queue q ON e.id = q.raw_event_id 
                 WHERE e.source = 'property.fk_test'"
            )
            .fetch_one(&pool)
            .await
            .expect("Join count query failed");
            
            prop_assert_eq!(join_count as usize, num_relationships, 
                "Join count should match number of created relationships");
            
            // Cleanup
            sqlx::query("DELETE FROM sinex_schemas.work_queue WHERE target_agent_name = $1")
                .bind(&agent_name)
                .execute(&pool)
                .await
                .expect("Queue cleanup failed");
                
            sqlx::query("DELETE FROM raw.events WHERE source = 'property.fk_test'")
                .execute(&pool)
                .await
                .expect("Event cleanup failed");
                
            sqlx::query("DELETE FROM sinex_schemas.agent_manifests WHERE agent_name = $1")
                .bind(&agent_name)
                .execute(&pool)
                .await
                .expect("Agent cleanup failed");
            
            Ok(())
        })?
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    
    #[test]
    fn test_ulid_sequence_generator() {
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let sequence = arb_ulid_sequence(3, 5)
            .new_tree(&mut runner)
            .unwrap()
            .current();
        
        assert!(sequence.len() >= 3 && sequence.len() <= 5);
        
        // Should be in increasing order
        for i in 1..sequence.len() {
            assert!(sequence[i] > sequence[i-1]);
        }
    }
    
    #[test]
    fn test_time_range_ulid_generator() {
        let start = Utc::now() - ChronoDuration::hours(1);
        let end = Utc::now();
        
        let mut runner = proptest::test_runner::TestRunner::deterministic();
        let ulid = arb_ulid_from_time_range(start, end)
            .new_tree(&mut runner)
            .unwrap()
            .current();
        
        let timestamp = ulid.timestamp();
        assert!(timestamp >= start && timestamp <= end);
    }
}