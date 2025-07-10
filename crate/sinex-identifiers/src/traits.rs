//! Identifier Traits
//!
//! Core traits for type-safe identifiers with different capabilities.

use crate::validation::{IdentifierError, IdentifierResult};

/// Base trait for all identifiers
pub trait Identifier: 
    std::fmt::Debug + 
    std::fmt::Display + 
    Clone + 
    PartialEq + 
    Eq + 
    std::hash::Hash +
    Send + 
    Sync + 
    'static 
{
    /// Get the identifier as a string slice
    fn as_str(&self) -> &str;
    
    /// Convert the identifier to an owned string
    fn into_string(self) -> String;
    
    /// Get the length of the identifier
    fn len(&self) -> usize {
        self.as_str().len()
    }
    
    /// Check if the identifier is empty
    fn is_empty(&self) -> bool {
        self.as_str().is_empty()
    }
    
    /// Get the identifier type name for debugging
    fn type_name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }
}

/// Trait for identifiers that can be validated
pub trait ValidatedIdentifier: Identifier {
    /// Validate a string value for this identifier type
    fn validate(value: &str) -> IdentifierResult<()>;
    
    /// Create a new validated identifier
    fn new_validated(value: impl Into<String>) -> IdentifierResult<Self>
    where 
        Self: Sized,
    {
        let value = value.into();
        Self::validate(&value)?;
        // This is a bit of a hack since we can't implement new() in the trait
        // Implementors should override this method
        Err(IdentifierError::Other("new_validated not implemented".to_string()))
    }
}

/// Trait for identifiers that can be automatically generated
pub trait GeneratedIdentifier: Identifier {
    /// Generate a new identifier
    fn generate() -> Self;
    
    /// Generate multiple identifiers
    fn generate_batch(count: usize) -> Vec<Self>
    where
        Self: Sized,
    {
        (0..count).map(|_| Self::generate()).collect()
    }
}

/// Trait for identifiers that have temporal ordering (like ULIDs)
pub trait TemporalIdentifier: Identifier {
    /// Get the timestamp embedded in this identifier
    fn timestamp(&self) -> Result<crate::chrono::DateTime<crate::chrono::Utc>, IdentifierError>;
    
    /// Check if this identifier was created before another
    fn created_before(&self, other: &Self) -> Result<bool, IdentifierError> {
        Ok(self.timestamp()? < other.timestamp()?)
    }
    
    /// Check if this identifier was created after another
    fn created_after(&self, other: &Self) -> Result<bool, IdentifierError> {
        Ok(self.timestamp()? > other.timestamp()?)
    }
    
    /// Get the age of this identifier
    fn age(&self) -> Result<crate::chrono::Duration, IdentifierError> {
        Ok(crate::chrono::Utc::now() - self.timestamp()?)
    }
}

/// Trait for identifiers that can be used in hierarchical structures
pub trait HierarchicalIdentifier: Identifier {
    /// Get the parent identifier if this is a child
    fn parent(&self) -> Option<Self>
    where
        Self: Sized;
    
    /// Create a child identifier under this parent
    fn child(&self, name: &str) -> Result<Self, IdentifierError>
    where
        Self: Sized;
    
    /// Get all path components
    fn components(&self) -> Vec<&str>;
    
    /// Get the depth in the hierarchy (0 for root)
    fn depth(&self) -> usize {
        self.components().len()
    }
    
    /// Check if this is a root identifier
    fn is_root(&self) -> bool {
        self.depth() == 1
    }
    
    /// Check if this identifier is an ancestor of another
    fn is_ancestor_of(&self, other: &Self) -> bool {
        other.as_str().starts_with(self.as_str())
    }
    
    /// Check if this identifier is a descendant of another
    fn is_descendant_of(&self, other: &Self) -> bool {
        other.is_ancestor_of(self)
    }
}

/// Trait for identifiers that can be namespaced
pub trait NamespacedIdentifier: Identifier {
    /// Get the namespace part of the identifier
    fn namespace(&self) -> Option<&str>;
    
    /// Get the local part of the identifier (without namespace)
    fn local_part(&self) -> &str;
    
    /// Create a new identifier in a specific namespace
    fn in_namespace(namespace: &str, local: &str) -> Result<Self, IdentifierError>
    where
        Self: Sized;
    
    /// Check if this identifier is in a specific namespace
    fn is_in_namespace(&self, namespace: &str) -> bool {
        self.namespace() == Some(namespace)
    }
}

/// Trait for identifiers that can be scoped to specific contexts
pub trait ScopedIdentifier: Identifier {
    /// The scope type for this identifier
    type Scope: std::fmt::Debug + Clone + PartialEq + Eq;
    
    /// Get the scope of this identifier
    fn scope(&self) -> &Self::Scope;
    
    /// Check if this identifier is in a specific scope
    fn is_in_scope(&self, scope: &Self::Scope) -> bool {
        self.scope() == scope
    }
    
    /// Create a new identifier in a specific scope
    fn in_scope(scope: Self::Scope, local: &str) -> Result<Self, IdentifierError>
    where
        Self: Sized;
}

/// Collection of identifier utilities
pub mod utils {
    use super::*;
    
    /// Check if two identifiers are of the same type
    pub fn same_type<T1: Identifier, T2: Identifier>(_id1: &T1, _id2: &T2) -> bool {
        std::any::TypeId::of::<T1>() == std::any::TypeId::of::<T2>()
    }
    
    /// Convert an identifier to a different type if they have the same string representation
    pub fn convert_identifier<From: Identifier, To: ValidatedIdentifier>(
        from: From
    ) -> IdentifierResult<To> {
        To::new_validated(from.into_string())
    }
    
    /// Batch convert multiple identifiers
    pub fn convert_batch<From: Identifier, To: ValidatedIdentifier>(
        from_ids: Vec<From>
    ) -> IdentifierResult<Vec<To>> {
        from_ids.into_iter()
            .map(convert_identifier)
            .collect()
    }
    
    /// Sort identifiers by their string representation
    pub fn sort_identifiers<T: Identifier>(mut ids: Vec<T>) -> Vec<T> {
        ids.sort_by(|a, b| a.as_str().cmp(b.as_str()));
        ids
    }
    
    /// Sort temporal identifiers by timestamp
    pub fn sort_by_timestamp<T: TemporalIdentifier>(mut ids: Vec<T>) -> Result<Vec<T>, IdentifierError> {
        ids.sort_by(|a, b| {
            match (a.timestamp(), b.timestamp()) {
                (Ok(ta), Ok(tb)) => ta.cmp(&tb),
                _ => std::cmp::Ordering::Equal,
            }
        });
        Ok(ids)
    }
    
    /// Group identifiers by a key function
    pub fn group_by<T: Identifier, K: std::hash::Hash + Eq, F: Fn(&T) -> K>(
        ids: Vec<T>,
        key_fn: F,
    ) -> std::collections::HashMap<K, Vec<T>> {
        let mut groups = std::collections::HashMap::new();
        for id in ids {
            let key = key_fn(&id);
            groups.entry(key).or_insert_with(Vec::new).push(id);
        }
        groups
    }
}