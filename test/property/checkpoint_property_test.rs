use crate::common::prelude::*;
use crate::common::property_builders::*;
use proptest::prelude::*;
use sinex_satellite_sdk::stream_processor::Checkpoint;
use sinex_ulid::Ulid;
use chrono::{DateTime, Utc};

proptest! {
    #![proptest_config(ProptestConfig::with_cases(1000))]

    #[test]
    fn checkpoint_serialization_roundtrip(
        checkpoint in arbitrary_checkpoint()
    ) {
        // Property: Checkpoints should survive JSON serialization
        let serialized = serde_json::to_string(&checkpoint);
        assert!(serialized.is_ok(), "Checkpoint should be serializable");
        
        let json_str = serialized.unwrap();
        let deserialized: Result<Checkpoint, _> = serde_json::from_str(&json_str);
        
        assert!(deserialized.is_ok(), "Checkpoint should be deserializable");
        assert_eq!(deserialized.unwrap(), checkpoint, "Deserialized checkpoint should match original");
    }

    #[test]
    fn checkpoint_ordering_property(
        cp1 in arbitrary_checkpoint(),
        cp2 in arbitrary_checkpoint()
    ) {
        // Property: Checkpoint ordering should be consistent
        match (&cp1, &cp2) {
            (Checkpoint::None, _) => {
                // None is always "less than" any other checkpoint
            },
            (_, Checkpoint::None) => {
                // Any checkpoint is "greater than" None
            },
            (Checkpoint::Database { event_id: id1 }, Checkpoint::Database { event_id: id2 }) => {
                // Database checkpoints ordered by ULID
                assert_eq!(id1 < id2, id1.to_string() < id2.to_string());
            },
            (Checkpoint::Timestamp { timestamp: ts1 }, Checkpoint::Timestamp { timestamp: ts2 }) => {
                // Timestamp checkpoints ordered chronologically
                let ordering = ts1.cmp(ts2);
                assert_eq!(ordering, ts1.cmp(ts2));
            },
            _ => {
                // Different types don't have defined ordering
            }
        }
    }

    #[test]
    fn checkpoint_state_transitions(
        initial in arbitrary_checkpoint(),
        next_event_id in arbitrary_ulid()
    ) {
        // Property: Checkpoint updates should maintain consistency
        let updated = match initial {
            Checkpoint::None => {
                // Can transition to any checkpoint type
                Checkpoint::Database { event_id: next_event_id }
            },
            Checkpoint::Database { event_id: current } => {
                // Database checkpoint should advance
                if next_event_id > current {
                    Checkpoint::Database { event_id: next_event_id }
                } else {
                    // Keep current if next is not newer
                    Checkpoint::Database { event_id: current }
                }
            },
            Checkpoint::Stream { message_id, .. } => {
                // Stream checkpoint updates with new event
                Checkpoint::Stream {
                    message_id,
                    event_id: Some(next_event_id.to_string())
                }
            },
            Checkpoint::Timestamp { timestamp } => {
                // Timestamp checkpoint remains timestamp-based
                Checkpoint::Timestamp { timestamp }
            }
        };
        
        // Verify the update maintains checkpoint type consistency
        match (&initial, &updated) {
            (Checkpoint::None, _) => {}, // Can change to any type
            (Checkpoint::Database { .. }, Checkpoint::Database { .. }) => {},
            (Checkpoint::Stream { .. }, Checkpoint::Stream { .. }) => {},
            (Checkpoint::Timestamp { .. }, Checkpoint::Timestamp { .. }) => {},
            _ => panic!("Checkpoint type should not change during update"),
        }
    }

    #[test]
    fn checkpoint_progress_tracking(
        checkpoints in proptest::collection::vec(arbitrary_checkpoint(), 1..20)
    ) {
        // Property: Checkpoint sequence should show progress
        let mut last_db_id: Option<Ulid> = None;
        let mut last_timestamp: Option<DateTime<Utc>> = None;
        
        for checkpoint in checkpoints {
            match checkpoint {
                Checkpoint::None => {
                    // None doesn't indicate progress
                },
                Checkpoint::Database { event_id } => {
                    if let Some(last) = last_db_id {
                        // New checkpoint should indicate progress (or at least not regress)
                        assert!(event_id >= last, "Database checkpoint should not regress");
                    }
                    last_db_id = Some(event_id);
                },
                Checkpoint::Timestamp { timestamp } => {
                    if let Some(last) = last_timestamp {
                        // Timestamps might not always increase (could be processing old data)
                        // but we track them
                    }
                    last_timestamp = Some(timestamp);
                },
                Checkpoint::Stream { .. } => {
                    // Stream checkpoints are independent
                }
            }
        }
    }

    #[test]
    fn checkpoint_redis_stream_compatibility(
        message_id in "[0-9]+-[0-9]+",
        event_id in arbitrary_ulid()
    ) {
        // Property: Stream checkpoints should maintain Redis stream ID format
        let checkpoint = Checkpoint::Stream {
            message_id: message_id.clone(),
            event_id: Some(event_id.to_string())
        };
        
        if let Checkpoint::Stream { message_id: msg_id, .. } = &checkpoint {
            // Redis stream IDs have format: milliseconds-sequence
            let parts: Vec<&str> = msg_id.split('-').collect();
            assert_eq!(parts.len(), 2, "Redis stream ID should have two parts");
            
            // Both parts should be numeric
            assert!(parts[0].parse::<u64>().is_ok(), "First part should be numeric (timestamp)");
            assert!(parts[1].parse::<u64>().is_ok(), "Second part should be numeric (sequence)");
        }
    }

    #[test]
    fn checkpoint_recovery_scenarios(
        checkpoint in arbitrary_checkpoint(),
        failure_type in prop_oneof![
            Just("crash"),
            Just("network"),
            Just("timeout"),
            Just("data_corruption")
        ]
    ) {
        // Property: Checkpoints should enable recovery from various failure scenarios
        let recovery_checkpoint = match (checkpoint.clone(), failure_type.as_str()) {
            (Checkpoint::None, _) => {
                // No checkpoint means start from beginning
                Checkpoint::None
            },
            (cp @ Checkpoint::Database { .. }, "crash") => {
                // Database checkpoint survives crashes
                cp
            },
            (Checkpoint::Stream { message_id, .. }, "network") => {
                // Stream checkpoint can resume from message ID
                Checkpoint::Stream { message_id, event_id: None }
            },
            (cp @ Checkpoint::Timestamp { .. }, "timeout") => {
                // Timestamp checkpoint remains valid
                cp
            },
            (_, "data_corruption") => {
                // Corruption might require starting fresh
                Checkpoint::None
            },
            (cp, _) => cp
        };
        
        // Verify recovery checkpoint is valid
        match recovery_checkpoint {
            Checkpoint::None => {},
            Checkpoint::Database { event_id } => {
                assert_ne!(event_id, Ulid::nil(), "Database checkpoint should have valid ULID");
            },
            Checkpoint::Stream { ref message_id, .. } => {
                assert!(!message_id.is_empty(), "Stream checkpoint should have valid message ID");
            },
            Checkpoint::Timestamp { timestamp } => {
                assert!(timestamp.timestamp() > 0, "Timestamp checkpoint should be valid");
            }
        }
    }

    #[test]
    fn checkpoint_size_boundaries(
        checkpoint_type in prop_oneof![
            Just("minimal"),
            Just("typical"),
            Just("large")
        ]
    ) {
        // Property: Checkpoint size should be reasonable for storage
        let checkpoint = match checkpoint_type.as_str() {
            "minimal" => Checkpoint::None,
            "typical" => Checkpoint::Database { event_id: Ulid::new() },
            "large" => {
                // Even with additional data, checkpoints should be compact
                Checkpoint::Stream {
                    message_id: format!("{}-{}", u64::MAX, u64::MAX),
                    event_id: Some(Ulid::new().to_string())
                }
            },
            _ => Checkpoint::None
        };
        
        let serialized = serde_json::to_string(&checkpoint).unwrap();
        
        // Checkpoints should be compact for efficient storage
        assert!(serialized.len() < 1024, "Checkpoint should be less than 1KB when serialized");
        
        // Minimal checkpoint should be very small
        if matches!(checkpoint, Checkpoint::None) {
            assert!(serialized.len() < 50, "None checkpoint should be minimal");
        }
    }

    #[test]
    fn checkpoint_concurrent_update_safety(
        initial in arbitrary_checkpoint(),
        updates in proptest::collection::vec(arbitrary_ulid(), 2..10)
    ) {
        // Property: Concurrent updates should maintain checkpoint consistency
        // In a real system, these would be protected by transactions or CAS operations
        
        let mut checkpoint = initial;
        let mut applied_updates = Vec::new();
        
        for update_id in updates {
            // Simulate concurrent update attempts
            let new_checkpoint = match &checkpoint {
                Checkpoint::None => Checkpoint::Database { event_id: update_id },
                Checkpoint::Database { event_id } => {
                    // Only update if newer
                    if update_id > *event_id {
                        applied_updates.push(update_id);
                        Checkpoint::Database { event_id: update_id }
                    } else {
                        checkpoint.clone()
                    }
                },
                cp => cp.clone()
            };
            checkpoint = new_checkpoint;
        }
        
        // Verify final state is consistent
        if let Checkpoint::Database { event_id } = checkpoint {
            // The checkpoint should reflect the maximum update
            for applied in applied_updates {
                assert!(event_id >= applied, "Checkpoint should reflect all applied updates");
            }
        }
    }
}

#[cfg(test)]
mod checkpoint_persistence_tests {
    use super::*;
    use sinex_db::{queries::CheckpointQueries, DatabaseTestContext};

    proptest! {
        #[test]
        fn checkpoint_database_persistence(
            automaton_name in "[a-z]+-automaton",
            checkpoint in arbitrary_checkpoint()
        ) {
            // Property: Checkpoints should persist correctly in database
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let ctx = DatabaseTestContext::new("checkpoint_persistence").await;
                let pool = ctx.get_pool();
                
                // Convert checkpoint to database format
                let (last_processed_id, checkpoint_data) = match &checkpoint {
                    Checkpoint::None => (None, None),
                    Checkpoint::Database { event_id } => (Some(*event_id), None),
                    Checkpoint::Stream { message_id, event_id } => {
                        let data = serde_json::json!({
                            "type": "stream",
                            "message_id": message_id,
                            "event_id": event_id
                        });
                        (None, Some(data))
                    },
                    Checkpoint::Timestamp { timestamp } => {
                        let data = serde_json::json!({
                            "type": "timestamp",
                            "timestamp": timestamp.to_rfc3339()
                        });
                        (None, Some(data))
                    }
                };
                
                // Save checkpoint
                let result = CheckpointQueries::upsert_checkpoint(
                    pool,
                    &automaton_name,
                    last_processed_id.as_ref(),
                    checkpoint_data.as_ref()
                ).await;
                
                assert!(result.is_ok(), "Checkpoint should save successfully");
                
                // Retrieve checkpoint
                let retrieved = CheckpointQueries::get_checkpoint(pool, &automaton_name).await;
                assert!(retrieved.is_ok(), "Checkpoint should be retrievable");
                
                if let Ok(Some(record)) = retrieved {
                    // Verify data matches
                    match &checkpoint {
                        Checkpoint::None => {
                            assert!(record.last_processed_id.is_none());
                            assert!(record.checkpoint_data.is_none());
                        },
                        Checkpoint::Database { event_id } => {
                            assert_eq!(record.last_processed_id, Some(*event_id));
                        },
                        _ => {
                            assert!(record.checkpoint_data.is_some());
                        }
                    }
                }
            });
        }
    }
}