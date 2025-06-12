pub mod path;
pub mod json;
pub mod unicode;
pub mod command;
pub mod monitoring;
pub mod secure_json;
pub mod json_ref;
pub mod validator;
pub mod dashboard;

pub use path::PathValidator;
pub use json::{JsonValidator, JsonLimits};
pub use unicode::UnicodeNormalizer;
pub use command::SafeCommand;
pub use validator::{Validator, ValidatorConfig};

#[derive(Debug, thiserror::Error)]
pub enum ValidationError {
    #[error("Path contains null bytes")]
    NullBytesInPath,
    
    #[error("Path traversal attempt detected")]
    PathTraversal,
    
    #[error("Path contains invalid characters: {0}")]
    InvalidPathCharacters(String),
    
    #[error("JSON exceeds size limit: {size} > {limit}")]
    JsonTooLarge { size: usize, limit: usize },
    
    #[error("JSON exceeds depth limit: {depth} > {limit}")]
    JsonTooDeep { depth: usize, limit: usize },
    
    #[error("JSON contains too many keys: {count} > {limit}")]
    JsonTooManyKeys { count: usize, limit: usize },
    
    #[error("Unicode normalization error: {0}")]
    UnicodeError(String),
    
    #[error("Command injection attempt detected")]
    CommandInjection,
    
    #[error("Validation error: {0}")]
    Other(String),
}