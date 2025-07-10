//! Concrete Identifier Implementations
//!
//! Pre-defined identifier types commonly used throughout the Sinex system.

use crate::validation::validators;
use crate::{TemporalIdentifier, HierarchicalIdentifier, NamespacedIdentifier, GeneratedIdentifier};

// ===== Core System Identifiers =====

crate::define_ulid_identifier!(EventId);

crate::define_uuid_identifier!(ServiceInstanceId);

crate::define_identifier!(HostId, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators(".-")
));

crate::define_identifier!(UserId, validators::combine_and(
    validators::not_empty,
    validators::length_between(1, 64)
));

crate::define_ulid_identifier!(SessionId);

crate::define_ulid_identifier!(RequestId);

// ===== Service and Component Identifiers =====

crate::define_identifier!(ServiceName, validators::combine_and(
    validators::not_empty,
    validators::matches_regex(r"^[a-z][a-z0-9-]*$")
), display = "service:{}" );

crate::define_identifier!(ComponentName, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("._-")
));

crate::define_identifier!(AgentName, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("_-")
));

crate::define_ulid_identifier!(TaskId);

crate::define_ulid_identifier!(JobId);

// ===== Event Source Identifiers =====

crate::define_identifier!(SourceName, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators(".")
), display = "source:{}" );

crate::define_identifier!(EventType, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("._")
), display = "event:{}" );

crate::define_ulid_identifier!(SchemaId);

crate::define_identifier!(SchemaVersion, validators::combine_and(
    validators::not_empty,
    validators::matches_regex(r"^[0-9]+\.[0-9]+\.[0-9]+$")
));

// ===== File System Identifiers =====

crate::define_identifier!(FilePath, validators::combine_and(
    validators::not_empty,
    validators::path_format
));

crate::define_identifier!(DirectoryPath, validators::combine_and(
    validators::not_empty,
    validators::path_format
));

crate::define_identifier!(BlobHash, validators::combine_and(
    validators::not_empty,
    validators::length_between(40, 128) // Support various hash algorithms
));

crate::define_identifier!(FileExtension, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators(".")
));

// ===== Database Identifiers =====

crate::define_identifier!(TableName, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("_")
));

crate::define_identifier!(ColumnName, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("_")
));

crate::define_identifier!(IndexName, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("_")
));

// ===== Network Identifiers =====

crate::define_identifier!(Hostname, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators(".-")
));

crate::define_identifier!(Port, |s: &str| {
    match s.parse::<u16>() {
        Ok(port) if port > 0 => Ok(()),
        Ok(_) => Err("port must be greater than 0".to_string()),
        Err(_) => Err("port must be a valid number".to_string()),
    }
});

crate::define_identifier!(Url, validators::url_format);

crate::define_identifier!(EmailAddress, validators::email_format);

// ===== Configuration Identifiers =====

crate::define_identifier!(ConfigKey, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("._-")
));

crate::define_identifier!(EnvVar, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("_")
));

// ===== Security Identifiers =====

crate::define_identifier!(ApiKey, validators::combine_and(
    validators::not_empty,
    validators::length_between(16, 256)
));

crate::define_identifier!(Token, validators::combine_and(
    validators::not_empty,
    validators::length_between(8, 512)
));

crate::define_identifier!(Permission, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("._:")
));

crate::define_identifier!(Role, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("_-")
));

// ===== Workspace and Organization =====

crate::define_identifier!(WorkspaceId, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("_-")
));

crate::define_identifier!(OrganizationId, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("_-")
));

crate::define_identifier!(ProjectId, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators("_-")
));

// ===== Hierarchical Identifiers =====

crate::define_identifier!(Namespace, validators::combine_and(
    validators::not_empty,
    validators::alphanumeric_with_separators(".")
));

impl HierarchicalIdentifier for Namespace {
    fn parent(&self) -> Option<Self> {
        let components = self.components();
        if components.len() > 1 {
            let parent_path = components[..components.len() - 1].join(".");
            Self::new(parent_path).ok()
        } else {
            None
        }
    }
    
    fn child(&self, name: &str) -> Result<Self, crate::IdentifierError> {
        let child_path = format!("{}.{}", self.as_str(), name);
        Self::new(child_path).map_err(|e| crate::IdentifierError::InvalidHierarchy {
            reason: format!("Failed to create child namespace: {}", e)
        })
    }
    
    fn components(&self) -> Vec<&str> {
        self.as_str().split('.').collect()
    }
}

// ===== Temporal Identifiers =====

impl TemporalIdentifier for EventId {
    fn timestamp(&self) -> Result<chrono::DateTime<chrono::Utc>, crate::IdentifierError> {
        self.timestamp()
    }
}

impl TemporalIdentifier for SessionId {
    fn timestamp(&self) -> Result<chrono::DateTime<chrono::Utc>, crate::IdentifierError> {
        self.timestamp()
    }
}

impl TemporalIdentifier for RequestId {
    fn timestamp(&self) -> Result<chrono::DateTime<chrono::Utc>, crate::IdentifierError> {
        self.timestamp()
    }
}

impl TemporalIdentifier for TaskId {
    fn timestamp(&self) -> Result<chrono::DateTime<chrono::Utc>, crate::IdentifierError> {
        self.timestamp()
    }
}

impl TemporalIdentifier for JobId {
    fn timestamp(&self) -> Result<chrono::DateTime<chrono::Utc>, crate::IdentifierError> {
        self.timestamp()
    }
}

impl TemporalIdentifier for SchemaId {
    fn timestamp(&self) -> Result<chrono::DateTime<chrono::Utc>, crate::IdentifierError> {
        self.timestamp()
    }
}

// ===== Namespaced Identifiers =====

impl NamespacedIdentifier for SourceName {
    fn namespace(&self) -> Option<&str> {
        if let Some(dot_pos) = self.as_str().find('.') {
            Some(&self.as_str()[..dot_pos])
        } else {
            None
        }
    }
    
    fn local_part(&self) -> &str {
        if let Some(dot_pos) = self.as_str().find('.') {
            &self.as_str()[dot_pos + 1..]
        } else {
            self.as_str()
        }
    }
    
    fn in_namespace(namespace: &str, local: &str) -> Result<Self, crate::IdentifierError> {
        let full_name = format!("{}.{}", namespace, local);
        Self::new(full_name).map_err(|e| crate::IdentifierError::InvalidNamespace {
            reason: format!("Failed to create namespaced source: {}", e)
        })
    }
}

impl NamespacedIdentifier for EventType {
    fn namespace(&self) -> Option<&str> {
        if let Some(dot_pos) = self.as_str().find('.') {
            Some(&self.as_str()[..dot_pos])
        } else {
            None
        }
    }
    
    fn local_part(&self) -> &str {
        if let Some(dot_pos) = self.as_str().find('.') {
            &self.as_str()[dot_pos + 1..]
        } else {
            self.as_str()
        }
    }
    
    fn in_namespace(namespace: &str, local: &str) -> Result<Self, crate::IdentifierError> {
        let full_name = format!("{}.{}", namespace, local);
        Self::new(full_name).map_err(|e| crate::IdentifierError::InvalidNamespace {
            reason: format!("Failed to create namespaced event type: {}", e)
        })
    }
}

// ===== Convenience Functions =====

pub fn event_id_now() -> EventId {
    EventId::generate()
}

pub fn session_id_now() -> SessionId {
    SessionId::generate()
}

pub fn request_id_now() -> RequestId {
    RequestId::generate()
}

pub fn task_id_now() -> TaskId {
    TaskId::generate()
}

pub fn parse_file_path(path: &str) -> Result<FilePath, crate::IdentifierError> {
    FilePath::new(path)
}

pub fn source_name(namespace: &str, local: &str) -> Result<SourceName, crate::IdentifierError> {
    SourceName::in_namespace(namespace, local)
}

pub fn event_type(namespace: &str, local: &str) -> Result<EventType, crate::IdentifierError> {
    EventType::in_namespace(namespace, local)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{TemporalIdentifier, NamespacedIdentifier, HierarchicalIdentifier};
    
    #[test]
    fn test_event_id_generation() {
        let id1 = EventId::generate();
        let id2 = EventId::generate();
        
        assert_ne!(id1, id2);
        assert_eq!(id1.len(), 26); // ULID length
        assert!(id1.timestamp().is_ok());
    }
    
    #[test]
    fn test_service_name_validation() {
        assert!(ServiceName::new("sinex-collector").is_ok());
        assert!(ServiceName::new("my-service").is_ok());
        
        assert!(ServiceName::new("Invalid-Service").is_err()); // Capital letters
        assert!(ServiceName::new("service_name").is_err()); // Underscores
        assert!(ServiceName::new("").is_err()); // Empty
    }
    
    #[test]
    fn test_namespaced_source_name() {
        let source = SourceName::new("shell.kitty").unwrap();
        
        assert_eq!(source.namespace(), Some("shell"));
        assert_eq!(source.local_part(), "kitty");
        assert!(source.is_in_namespace("shell"));
        
        let namespaced = SourceName::in_namespace("wm", "hyprland").unwrap();
        assert_eq!(namespaced.as_str(), "wm.hyprland");
    }
    
    #[test]
    fn test_hierarchical_namespace() {
        let ns = Namespace::new("sinex.events.filesystem").unwrap();
        
        assert_eq!(ns.depth(), 3);
        assert!(!ns.is_root());
        
        let parent = ns.parent().unwrap();
        assert_eq!(parent.as_str(), "sinex.events");
        
        let child = ns.child("operations").unwrap();
        assert_eq!(child.as_str(), "sinex.events.filesystem.operations");
    }
    
    #[test]
    fn test_temporal_ordering() {
        let id1 = EventId::generate();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = EventId::generate();
        
        assert!(id1.created_before(&id2).unwrap());
        assert!(id2.created_after(&id1).unwrap());
    }
    
    #[test]
    fn test_port_validation() {
        assert!(Port::new("8080").is_ok());
        assert!(Port::new("443").is_ok());
        
        assert!(Port::new("0").is_err()); // Port 0 not allowed
        assert!(Port::new("65536").is_err()); // Too large
        assert!(Port::new("abc").is_err()); // Not a number
    }
    
    #[test]
    fn test_email_validation() {
        assert!(EmailAddress::new("user@example.com").is_ok());
        assert!(EmailAddress::new("test.email@domain.org").is_ok());
        
        assert!(EmailAddress::new("invalid-email").is_err());
        assert!(EmailAddress::new("@domain.com").is_err());
        assert!(EmailAddress::new("user@").is_err());
    }
    
    #[test]
    fn test_file_path_validation() {
        assert!(FilePath::new("/home/user/file.txt").is_ok());
        assert!(FilePath::new("relative/path.txt").is_ok());
        
        assert!(FilePath::new("../../../etc/passwd").is_err()); // Path traversal
        assert!(FilePath::new("file\0name").is_err()); // Null byte
        assert!(FilePath::new("").is_err()); // Empty
    }
}