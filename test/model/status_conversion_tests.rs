use sinex_db::models::{QueueStatus, AgentStatus, AgentHeartbeat};

#[test]
fn test_queue_status_conversion() {
    assert_eq!(QueueStatus::from("pending"), QueueStatus::Pending);
    assert_eq!(QueueStatus::from("processing"), QueueStatus::Processing);
    assert_eq!(QueueStatus::from("failed_retryable"), QueueStatus::FailedRetryable);
    assert_eq!(QueueStatus::from("unknown"), QueueStatus::Pending); // Default
    
    assert_eq!(QueueStatus::Pending.as_str(), "pending");
    assert_eq!(QueueStatus::Processing.as_str(), "processing");
    assert_eq!(QueueStatus::FailedRetryable.as_str(), "failed_retryable");
}

#[test]
fn test_agent_status_conversion() {
    assert_eq!(AgentStatus::from("running"), AgentStatus::Running);
    assert_eq!(AgentStatus::from("stopped"), AgentStatus::Stopped);
    assert_eq!(AgentStatus::from("error_state"), AgentStatus::ErrorState);
    assert_eq!(AgentStatus::from("disabled_by_user"), AgentStatus::DisabledByUser);
    assert_eq!(AgentStatus::from("pending_registration"), AgentStatus::PendingRegistration);
    assert_eq!(AgentStatus::from("degraded"), AgentStatus::Degraded);
    assert_eq!(AgentStatus::from("whatever"), AgentStatus::Unknown);
    
    assert_eq!(AgentStatus::Running.as_str(), "running");
    assert_eq!(AgentStatus::ErrorState.as_str(), "error_state");
}

#[test]
fn test_queue_status_serde() {
    let status = QueueStatus::Processing;
    let json = serde_json::to_string(&status).unwrap();
    assert_eq!(json, "\"processing\"");
    
    let deserialized: QueueStatus = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, QueueStatus::Processing);
}

#[test]
fn test_agent_status_serde() {
    let status = AgentStatus::ErrorState;
    let json = serde_json::to_string(&status).unwrap();
    assert_eq!(json, "\"error_state\"");
    
    let deserialized: AgentStatus = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, AgentStatus::ErrorState);
}

#[test]
fn test_agent_heartbeat_serialization() {
    let heartbeat = AgentHeartbeat {
        agent_name: "TestAgent".to_string(),
        status: "running".to_string(),
        uptime_seconds: 3600,
        events_processed_session: 100,
        dlq_size: 0,
        version: "0.1.0".to_string(),
    };
    
    let json = serde_json::to_string(&heartbeat).unwrap();
    let deserialized: AgentHeartbeat = serde_json::from_str(&json).unwrap();
    
    assert_eq!(deserialized.agent_name, heartbeat.agent_name);
    assert_eq!(deserialized.status, heartbeat.status);
    assert_eq!(deserialized.uptime_seconds, heartbeat.uptime_seconds);
    assert_eq!(deserialized.events_processed_session, heartbeat.events_processed_session);
    assert_eq!(deserialized.dlq_size, heartbeat.dlq_size);
    assert_eq!(deserialized.version, heartbeat.version);
}