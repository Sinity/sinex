use sinex_db::models::{QueueStatus, AgentStatus};

// Note: Basic serde serialization/deserialization is guaranteed by derive macros
// and doesn't need explicit testing. Keeping only tests that verify business logic.

#[test]
fn test_queue_status_unknown_default() {
    // Test that unknown status strings default to Pending (business logic)
    assert_eq!(QueueStatus::from("unknown"), QueueStatus::Pending);
    assert_eq!(QueueStatus::from("invalid"), QueueStatus::Pending);
    assert_eq!(QueueStatus::from(""), QueueStatus::Pending);
}

#[test] 
fn test_agent_status_unknown_handling() {
    // Test that unknown status strings default to Unknown (business logic)
    assert_eq!(AgentStatus::from("invalid"), AgentStatus::Unknown);
    assert_eq!(AgentStatus::from(""), AgentStatus::Unknown);
    assert_eq!(AgentStatus::from("not_a_real_status"), AgentStatus::Unknown);
}