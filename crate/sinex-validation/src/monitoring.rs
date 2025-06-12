use tracing::{error, warn};

#[derive(Debug, Clone)]
pub enum SecurityEvent {
    NullByteRejected { path: String },
    PathTraversal { path: String },
    SuspiciousPath { path: String },
    CommandInjectionAttempt { command: String, arg: String },
    JsonTooLarge { size: usize },
    JsonTooDeep { depth: usize },
    JsonTooManyKeys { count: usize },
    HashCollisionAttempt { prefix: String, count: usize },
    BillionLaughsAttempt { depth: usize, array_size: usize },
    CircularReference { path: String },
    UnicodeNormalizationBypass { input: String },
}

pub fn log_security_event(event: SecurityEvent) {
    // Record in dashboard
    if let Ok(_dashboard) = std::panic::catch_unwind(|| {
        crate::dashboard::DASHBOARD.record_event(event.clone());
    }) {
        // Dashboard recording succeeded
    }
    
    // Log the event
    match event {
        SecurityEvent::NullByteRejected { path } => {
            error!(
                category = "security",
                event_type = "null_byte_injection",
                path = %path,
                "SECURITY: Null byte injection attempt blocked"
            );
        }
        SecurityEvent::PathTraversal { path } => {
            error!(
                category = "security", 
                event_type = "path_traversal",
                path = %path,
                "SECURITY: Path traversal attempt blocked"
            );
        }
        SecurityEvent::SuspiciousPath { path } => {
            warn!(
                category = "security",
                event_type = "suspicious_path", 
                path = %path,
                "SECURITY: Suspicious path pattern detected"
            );
        }
        SecurityEvent::CommandInjectionAttempt { command, arg } => {
            error!(
                category = "security",
                event_type = "command_injection",
                command = %command,
                arg = %arg,
                "SECURITY: Command injection attempt blocked"
            );
        }
        SecurityEvent::JsonTooLarge { size } => {
            warn!(
                category = "security",
                event_type = "json_size_limit",
                size = size,
                "SECURITY: Oversized JSON rejected"
            );
        }
        SecurityEvent::JsonTooDeep { depth } => {
            warn!(
                category = "security",
                event_type = "json_depth_limit",
                depth = depth,
                "SECURITY: Deeply nested JSON rejected"
            );
        }
        SecurityEvent::JsonTooManyKeys { count } => {
            warn!(
                category = "security",
                event_type = "json_key_limit",
                count = count,
                "SECURITY: JSON with too many keys rejected"
            );
        }
        SecurityEvent::HashCollisionAttempt { prefix, count } => {
            error!(
                category = "security",
                event_type = "hash_collision_dos",
                prefix = %prefix,
                count = count,
                "SECURITY: Potential hash collision DoS attempt detected"
            );
        }
        SecurityEvent::BillionLaughsAttempt { depth, array_size } => {
            error!(
                category = "security",
                event_type = "billion_laughs",
                depth = depth,
                array_size = array_size,
                "SECURITY: Potential billion laughs attack blocked"
            );
        }
        SecurityEvent::CircularReference { path } => {
            error!(
                category = "security",
                event_type = "circular_reference",
                path = %path,
                "SECURITY: Circular JSON reference detected"
            );
        }
        SecurityEvent::UnicodeNormalizationBypass { input } => {
            error!(
                category = "security",
                event_type = "unicode_bypass",
                input = %input,
                "SECURITY: Unicode normalization bypass attempt"
            );
        }
    }
}

pub struct SecurityMetrics {
    null_byte_attempts: std::sync::atomic::AtomicU64,
    path_traversal_attempts: std::sync::atomic::AtomicU64,
    command_injection_attempts: std::sync::atomic::AtomicU64,
    json_attacks: std::sync::atomic::AtomicU64,
}

lazy_static::lazy_static! {
    pub static ref METRICS: SecurityMetrics = SecurityMetrics {
        null_byte_attempts: std::sync::atomic::AtomicU64::new(0),
        path_traversal_attempts: std::sync::atomic::AtomicU64::new(0),
        command_injection_attempts: std::sync::atomic::AtomicU64::new(0),
        json_attacks: std::sync::atomic::AtomicU64::new(0),
    };
}

impl SecurityMetrics {
    pub fn increment_null_byte_attempts(&self) {
        self.null_byte_attempts.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
    
    pub fn get_stats(&self) -> SecurityStats {
        SecurityStats {
            null_byte_attempts: self.null_byte_attempts.load(std::sync::atomic::Ordering::Relaxed),
            path_traversal_attempts: self.path_traversal_attempts.load(std::sync::atomic::Ordering::Relaxed),
            command_injection_attempts: self.command_injection_attempts.load(std::sync::atomic::Ordering::Relaxed),
            json_attacks: self.json_attacks.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

#[derive(Debug)]
pub struct SecurityStats {
    pub null_byte_attempts: u64,
    pub path_traversal_attempts: u64, 
    pub command_injection_attempts: u64,
    pub json_attacks: u64,
}