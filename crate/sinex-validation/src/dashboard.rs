use std::sync::{Arc, Mutex};
use std::collections::VecDeque;
use std::time::{SystemTime, Duration};
use serde::{Serialize, Deserialize};
use crate::monitoring::SecurityEvent;

/// Security monitoring dashboard for real-time threat visibility
pub struct SecurityDashboard {
    /// Recent security events
    events: Arc<Mutex<VecDeque<SecurityEventRecord>>>,
    /// Configuration
    config: DashboardConfig,
}

#[derive(Debug, Clone)]
pub struct DashboardConfig {
    /// Maximum number of events to keep in memory
    pub max_events: usize,
    /// How long to keep events (in seconds)
    pub event_retention: u64,
    /// Enable real-time alerting
    pub enable_alerts: bool,
    /// Alert thresholds
    pub alert_thresholds: AlertThresholds,
}

#[derive(Debug, Clone)]
pub struct AlertThresholds {
    /// Alert if more than N null byte attempts in a minute
    pub null_byte_per_minute: u32,
    /// Alert if more than N path traversal attempts in a minute
    pub path_traversal_per_minute: u32,
    /// Alert if more than N command injection attempts in a minute
    pub command_injection_per_minute: u32,
    /// Alert if more than N JSON attacks in a minute
    pub json_attacks_per_minute: u32,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            max_events: 10000,
            event_retention: 3600, // 1 hour
            enable_alerts: true,
            alert_thresholds: AlertThresholds::default(),
        }
    }
}

impl Default for AlertThresholds {
    fn default() -> Self {
        Self {
            null_byte_per_minute: 10,
            path_traversal_per_minute: 10,
            command_injection_per_minute: 5,
            json_attacks_per_minute: 20,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityEventRecord {
    pub timestamp: SystemTime,
    pub event_type: String,
    pub severity: Severity,
    pub details: String,
    pub source_ip: Option<String>,
    pub user_id: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Severity {
    Critical,
    High,
    Medium,
    Low,
}

impl SecurityDashboard {
    pub fn new(config: DashboardConfig) -> Self {
        Self {
            events: Arc::new(Mutex::new(VecDeque::with_capacity(config.max_events))),
            config,
        }
    }
    
    pub fn default() -> Self {
        Self::new(DashboardConfig::default())
    }
    
    /// Record a security event
    pub fn record_event(&self, event: SecurityEvent) {
        let record = self.event_to_record(event);
        
        let mut events = self.events.lock().unwrap();
        
        // Add new event
        events.push_back(record.clone());
        
        // Remove old events if over capacity
        while events.len() > self.config.max_events {
            events.pop_front();
        }
        
        // Check for alerts
        if self.config.enable_alerts {
            drop(events); // Release lock before checking alerts
            self.check_alerts();
        }
    }
    
    fn event_to_record(&self, event: SecurityEvent) -> SecurityEventRecord {
        let (event_type, severity, details) = match event {
            SecurityEvent::NullByteRejected { path } => (
                "null_byte_injection".to_string(),
                Severity::Critical,
                format!("Null byte injection blocked in path: {}", path),
            ),
            SecurityEvent::PathTraversal { path } => (
                "path_traversal".to_string(),
                Severity::Critical,
                format!("Path traversal attempt blocked: {}", path),
            ),
            SecurityEvent::CommandInjectionAttempt { command, arg } => (
                "command_injection".to_string(),
                Severity::Critical,
                format!("Command injection blocked: {} with arg: {}", command, arg),
            ),
            SecurityEvent::JsonTooLarge { size } => (
                "json_size_limit".to_string(),
                Severity::Medium,
                format!("Oversized JSON rejected: {} bytes", size),
            ),
            SecurityEvent::CircularReference { path } => (
                "circular_reference".to_string(),
                Severity::High,
                format!("Circular JSON reference detected: {}", path),
            ),
            SecurityEvent::HashCollisionAttempt { prefix, count } => (
                "hash_collision_dos".to_string(),
                Severity::High,
                format!("Hash collision DoS detected: {} keys with prefix '{}'", count, prefix),
            ),
            SecurityEvent::BillionLaughsAttempt { depth, array_size } => (
                "billion_laughs".to_string(),
                Severity::High,
                format!("Billion laughs attack blocked at depth {} with array size {}", depth, array_size),
            ),
            SecurityEvent::UnicodeNormalizationBypass { input } => (
                "unicode_bypass".to_string(),
                Severity::High,
                format!("Unicode normalization bypass attempt: {}", input),
            ),
            _ => (
                "unknown".to_string(),
                Severity::Low,
                "Unknown security event".to_string(),
            ),
        };
        
        SecurityEventRecord {
            timestamp: SystemTime::now(),
            event_type,
            severity,
            details,
            source_ip: None, // TODO: Add request context
            user_id: None,   // TODO: Add user context
        }
    }
    
    /// Get recent events
    pub fn get_recent_events(&self, limit: usize) -> Vec<SecurityEventRecord> {
        let events = self.events.lock().unwrap();
        events.iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }
    
    /// Get events by severity
    pub fn get_events_by_severity(&self, severity: Severity) -> Vec<SecurityEventRecord> {
        let events = self.events.lock().unwrap();
        events.iter()
            .filter(|e| e.severity == severity)
            .cloned()
            .collect()
    }
    
    /// Get event statistics for a time window
    pub fn get_stats(&self, window: Duration) -> DashboardStats {
        let now = SystemTime::now();
        let cutoff = now - window;
        
        let events = self.events.lock().unwrap();
        let recent_events: Vec<_> = events.iter()
            .filter(|e| e.timestamp > cutoff)
            .collect();
        
        let mut stats = DashboardStats::default();
        
        for event in recent_events {
            stats.total_events += 1;
            
            match event.severity {
                Severity::Critical => stats.critical_events += 1,
                Severity::High => stats.high_events += 1,
                Severity::Medium => stats.medium_events += 1,
                Severity::Low => stats.low_events += 1,
            }
            
            match event.event_type.as_str() {
                "null_byte_injection" => stats.null_byte_attempts += 1,
                "path_traversal" => stats.path_traversal_attempts += 1,
                "command_injection" => stats.command_injection_attempts += 1,
                "json_size_limit" | "circular_reference" | "hash_collision_dos" | "billion_laughs" => {
                    stats.json_attacks += 1;
                }
                _ => {}
            }
        }
        
        stats
    }
    
    /// Check if any alert thresholds are exceeded
    fn check_alerts(&self) {
        let stats = self.get_stats(Duration::from_secs(60)); // 1 minute window
        let thresholds = &self.config.alert_thresholds;
        
        if stats.null_byte_attempts > thresholds.null_byte_per_minute {
            self.trigger_alert(AlertType::NullByteFlood, stats.null_byte_attempts);
        }
        
        if stats.path_traversal_attempts > thresholds.path_traversal_per_minute {
            self.trigger_alert(AlertType::PathTraversalFlood, stats.path_traversal_attempts);
        }
        
        if stats.command_injection_attempts > thresholds.command_injection_per_minute {
            self.trigger_alert(AlertType::CommandInjectionFlood, stats.command_injection_attempts);
        }
        
        if stats.json_attacks > thresholds.json_attacks_per_minute {
            self.trigger_alert(AlertType::JsonAttackFlood, stats.json_attacks);
        }
    }
    
    fn trigger_alert(&self, alert_type: AlertType, count: u32) {
        // TODO: Implement actual alerting (email, webhook, etc.)
        tracing::error!(
            alert_type = ?alert_type,
            count = count,
            "SECURITY ALERT: Threshold exceeded"
        );
    }
    
    /// Export events for analysis
    pub fn export_events(&self, format: ExportFormat) -> Result<String, String> {
        let events = self.events.lock().unwrap();
        let all_events: Vec<_> = events.iter().cloned().collect();
        
        match format {
            ExportFormat::Json => {
                serde_json::to_string_pretty(&all_events)
                    .map_err(|e| format!("JSON serialization error: {}", e))
            }
            ExportFormat::Csv => {
                self.export_csv(&all_events)
            }
        }
    }
    
    fn export_csv(&self, events: &[SecurityEventRecord]) -> Result<String, String> {
        let mut csv = String::from("timestamp,event_type,severity,details,source_ip,user_id\n");
        
        for event in events {
            csv.push_str(&format!(
                "{:?},{},{:?},{},{},{}\n",
                event.timestamp.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
                event.event_type,
                event.severity,
                event.details.replace(',', ";"),
                event.source_ip.as_deref().unwrap_or(""),
                event.user_id.as_deref().unwrap_or("")
            ));
        }
        
        Ok(csv)
    }
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct DashboardStats {
    pub total_events: u32,
    pub critical_events: u32,
    pub high_events: u32,
    pub medium_events: u32,
    pub low_events: u32,
    pub null_byte_attempts: u32,
    pub path_traversal_attempts: u32,
    pub command_injection_attempts: u32,
    pub json_attacks: u32,
}

#[derive(Debug)]
pub enum AlertType {
    NullByteFlood,
    PathTraversalFlood,
    CommandInjectionFlood,
    JsonAttackFlood,
}

#[derive(Debug, Clone, Copy)]
pub enum ExportFormat {
    Json,
    Csv,
}

/// Global dashboard instance
lazy_static::lazy_static! {
    pub static ref DASHBOARD: SecurityDashboard = SecurityDashboard::default();
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_event_recording() {
        let dashboard = SecurityDashboard::default();
        
        dashboard.record_event(SecurityEvent::NullByteRejected {
            path: "/etc/passwd\0.txt".to_string(),
        });
        
        let events = dashboard.get_recent_events(10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type, "null_byte_injection");
        assert_eq!(events[0].severity, Severity::Critical);
    }
    
    #[test]
    fn test_stats_calculation() {
        let dashboard = SecurityDashboard::default();
        
        // Record various events
        for _ in 0..5 {
            dashboard.record_event(SecurityEvent::NullByteRejected {
                path: "test".to_string(),
            });
        }
        
        for _ in 0..3 {
            dashboard.record_event(SecurityEvent::JsonTooLarge { size: 1000000 });
        }
        
        let stats = dashboard.get_stats(Duration::from_secs(60));
        assert_eq!(stats.total_events, 8);
        assert_eq!(stats.critical_events, 5);
        assert_eq!(stats.medium_events, 3);
        assert_eq!(stats.null_byte_attempts, 5);
        assert_eq!(stats.json_attacks, 3);
    }
}