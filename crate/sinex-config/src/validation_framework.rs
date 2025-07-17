//! Advanced configuration validation framework
//!
//! This module provides comprehensive validation capabilities for Sinex configurations,
//! including cross-field validation, dependency checking, and environment-aware validation.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

// ============================================================================
// Core Validation Framework
// ============================================================================

/// Advanced configuration validator with support for complex validation scenarios
#[derive(Debug)]
pub struct ConfigurationValidator {
    validation_rules: Vec<ValidationRule>,
    cross_field_rules: Vec<CrossFieldRule>,
    environment_rules: Vec<EnvironmentRule>,
    custom_validators: Vec<Box<dyn CustomValidator>>,
}

/// Individual validation rule for a specific field
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRule {
    pub field_path: String,
    pub rule_type: RuleType,
    pub parameters: HashMap<String, ValidationValue>,
    pub error_message: String,
    pub severity: Severity,
}

/// Cross-field validation rule (dependencies between fields)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossFieldRule {
    pub rule_id: String,
    pub description: String,
    pub dependent_fields: Vec<String>,
    pub condition: CrossFieldCondition,
    pub error_message: String,
    pub severity: Severity,
}

/// Environment-aware validation rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentRule {
    pub rule_id: String,
    pub environment_pattern: String, // regex for environment names
    pub field_overrides: HashMap<String, ValidationValue>,
    pub additional_rules: Vec<ValidationRule>,
}

/// Types of validation rules
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RuleType {
    Required,
    NotEmpty,
    Range {
        min: Option<i64>,
        max: Option<i64>,
    },
    Regex {
        pattern: String,
    },
    Enum {
        allowed_values: Vec<String>,
    },
    Url {
        schemes: Vec<String>,
    },
    Path {
        must_exist: bool,
        must_be_readable: bool,
    },
    Custom {
        validator_name: String,
    },
}

/// Cross-field validation conditions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CrossFieldCondition {
    RequiredIf {
        field: String,
        equals: ValidationValue,
    },
    MutuallyExclusive {
        fields: Vec<String>,
    },
    AtLeastOne {
        fields: Vec<String>,
    },
    DependsOn {
        field: String,
        condition: Box<CrossFieldCondition>,
    },
    Custom {
        condition_name: String,
        parameters: HashMap<String, ValidationValue>,
    },
}

/// Validation value types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValidationValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Array(Vec<ValidationValue>),
    Object(HashMap<String, ValidationValue>),
    Null,
}

/// Validation severity levels
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum Severity {
    Info,
    Warning,
    Error,
    Critical,
}

impl fmt::Display for Severity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Severity::Info => write!(f, "INFO"),
            Severity::Warning => write!(f, "WARN"),
            Severity::Error => write!(f, "ERROR"),
            Severity::Critical => write!(f, "CRITICAL"),
        }
    }
}

/// Custom validator trait for complex validation logic
pub trait CustomValidator: fmt::Debug + Send + Sync {
    fn name(&self) -> &str;
    fn validate(&self, value: &ValidationValue, context: &ValidationContext) -> ValidationResult;
}

/// Validation context providing access to full configuration and environment
#[derive(Debug, Clone)]
pub struct ValidationContext {
    pub config: HashMap<String, ValidationValue>,
    pub environment: HashMap<String, String>,
    pub metadata: HashMap<String, ValidationValue>,
}

/// Result of validation with detailed information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub valid: bool,
    pub issues: Vec<ValidationIssue>,
}

/// Individual validation issue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub field_path: String,
    pub rule_id: String,
    pub severity: Severity,
    pub message: String,
    pub suggested_fix: Option<String>,
    pub documentation_link: Option<String>,
}

impl Default for ConfigurationValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl ConfigurationValidator {
    /// Create a new configuration validator
    pub fn new() -> Self {
        Self {
            validation_rules: Vec::new(),
            cross_field_rules: Vec::new(),
            environment_rules: Vec::new(),
            custom_validators: Vec::new(),
        }
    }

    /// Add a field validation rule
    pub fn add_rule(mut self, rule: ValidationRule) -> Self {
        self.validation_rules.push(rule);
        self
    }

    /// Add a cross-field validation rule
    pub fn add_cross_field_rule(mut self, rule: CrossFieldRule) -> Self {
        self.cross_field_rules.push(rule);
        self
    }

    /// Add an environment-specific rule
    pub fn add_environment_rule(mut self, rule: EnvironmentRule) -> Self {
        self.environment_rules.push(rule);
        self
    }

    /// Add a custom validator
    pub fn add_custom_validator(mut self, validator: Box<dyn CustomValidator>) -> Self {
        self.custom_validators.push(validator);
        self
    }

    /// Perform comprehensive validation
    pub fn validate(
        &self,
        config: &HashMap<String, ValidationValue>,
        environment: Option<&str>,
    ) -> ValidationResult {
        let context = ValidationContext {
            config: config.clone(),
            environment: std::env::vars().collect(),
            metadata: HashMap::new(),
        };

        let mut result = ValidationResult {
            valid: true,
            issues: Vec::new(),
        };

        // Apply field-level validation rules
        self.apply_field_rules(config, &context, &mut result);

        // Apply cross-field validation rules
        self.apply_cross_field_rules(config, &context, &mut result);

        // Apply environment-specific rules
        if let Some(env) = environment {
            self.apply_environment_rules(config, env, &context, &mut result);
        }

        // Apply custom validators
        self.apply_custom_validators(config, &context, &mut result);

        // Set overall validity based on error/critical issues
        result.valid = !result
            .issues
            .iter()
            .any(|issue| matches!(issue.severity, Severity::Error | Severity::Critical));

        result
    }

    fn apply_field_rules(
        &self,
        config: &HashMap<String, ValidationValue>,
        context: &ValidationContext,
        result: &mut ValidationResult,
    ) {
        for rule in &self.validation_rules {
            let value = self.get_field_value(config, &rule.field_path);
            let validation_result = self.validate_field_rule(rule, value, context);

            if !validation_result.valid {
                result.issues.extend(validation_result.issues);
            }
        }
    }

    fn apply_cross_field_rules(
        &self,
        config: &HashMap<String, ValidationValue>,
        context: &ValidationContext,
        result: &mut ValidationResult,
    ) {
        for rule in &self.cross_field_rules {
            let validation_result = self.validate_cross_field_rule(rule, config, context);

            if !validation_result.valid {
                result.issues.extend(validation_result.issues);
            }
        }
    }

    fn apply_environment_rules(
        &self,
        config: &HashMap<String, ValidationValue>,
        environment: &str,
        context: &ValidationContext,
        result: &mut ValidationResult,
    ) {
        for rule in &self.environment_rules {
            if self.matches_environment_pattern(&rule.environment_pattern, environment) {
                // Apply additional rules for this environment
                for additional_rule in &rule.additional_rules {
                    let value = self.get_field_value(config, &additional_rule.field_path);
                    let validation_result =
                        self.validate_field_rule(additional_rule, value, context);

                    if !validation_result.valid {
                        result.issues.extend(validation_result.issues);
                    }
                }
            }
        }
    }

    fn apply_custom_validators(
        &self,
        config: &HashMap<String, ValidationValue>,
        context: &ValidationContext,
        result: &mut ValidationResult,
    ) {
        for validator in &self.custom_validators {
            // Apply custom validator to entire config
            let validation_result =
                validator.validate(&ValidationValue::Object(config.clone()), context);

            if !validation_result.valid {
                result.issues.extend(validation_result.issues);
            }
        }
    }

    fn validate_field_rule(
        &self,
        rule: &ValidationRule,
        value: Option<&ValidationValue>,
        _context: &ValidationContext,
    ) -> ValidationResult {
        let mut result = ValidationResult {
            valid: true,
            issues: Vec::new(),
        };

        match &rule.rule_type {
            RuleType::Required => {
                if value.is_none() {
                    result.valid = false;
                    result.issues.push(ValidationIssue {
                        field_path: rule.field_path.clone(),
                        rule_id: "required".to_string(),
                        severity: rule.severity.clone(),
                        message: rule.error_message.clone(),
                        suggested_fix: Some(format!("Provide a value for {}", rule.field_path)),
                        documentation_link: None,
                    });
                }
            }
            RuleType::NotEmpty => {
                if let Some(ValidationValue::String(s)) = value {
                    if s.is_empty() {
                        result.valid = false;
                        result.issues.push(ValidationIssue {
                            field_path: rule.field_path.clone(),
                            rule_id: "not_empty".to_string(),
                            severity: rule.severity.clone(),
                            message: rule.error_message.clone(),
                            suggested_fix: Some(format!(
                                "Provide a non-empty value for {}",
                                rule.field_path
                            )),
                            documentation_link: None,
                        });
                    }
                }
            }
            RuleType::Range { min, max } => {
                if let Some(ValidationValue::Integer(i)) = value {
                    if let Some(min_val) = min {
                        if *i < *min_val {
                            result.valid = false;
                            result.issues.push(ValidationIssue {
                                field_path: rule.field_path.clone(),
                                rule_id: "range_min".to_string(),
                                severity: rule.severity.clone(),
                                message: format!(
                                    "{} (value: {}, minimum: {})",
                                    rule.error_message, i, min_val
                                ),
                                suggested_fix: Some(format!(
                                    "Set {} to at least {}",
                                    rule.field_path, min_val
                                )),
                                documentation_link: None,
                            });
                        }
                    }
                    if let Some(max_val) = max {
                        if *i > *max_val {
                            result.valid = false;
                            result.issues.push(ValidationIssue {
                                field_path: rule.field_path.clone(),
                                rule_id: "range_max".to_string(),
                                severity: rule.severity.clone(),
                                message: format!(
                                    "{} (value: {}, maximum: {})",
                                    rule.error_message, i, max_val
                                ),
                                suggested_fix: Some(format!(
                                    "Set {} to at most {}",
                                    rule.field_path, max_val
                                )),
                                documentation_link: None,
                            });
                        }
                    }
                }
            }
            RuleType::Enum { allowed_values } => {
                if let Some(ValidationValue::String(s)) = value {
                    if !allowed_values.contains(s) {
                        result.valid = false;
                        result.issues.push(ValidationIssue {
                            field_path: rule.field_path.clone(),
                            rule_id: "enum".to_string(),
                            severity: rule.severity.clone(),
                            message: format!(
                                "{} (value: '{}', allowed: [{}])",
                                rule.error_message,
                                s,
                                allowed_values.join(", ")
                            ),
                            suggested_fix: Some(format!(
                                "Set {} to one of: {}",
                                rule.field_path,
                                allowed_values.join(", ")
                            )),
                            documentation_link: None,
                        });
                    }
                }
            }
            RuleType::Url { schemes } => {
                if let Some(ValidationValue::String(s)) = value {
                    if !self.is_valid_url(s, schemes) {
                        result.valid = false;
                        result.issues.push(ValidationIssue {
                            field_path: rule.field_path.clone(),
                            rule_id: "url".to_string(),
                            severity: rule.severity.clone(),
                            message: format!(
                                "{} (value: '{}', expected schemes: [{}])",
                                rule.error_message,
                                s,
                                schemes.join(", ")
                            ),
                            suggested_fix: Some(format!(
                                "Ensure {} starts with one of: {}",
                                rule.field_path,
                                schemes.join(", ")
                            )),
                            documentation_link: None,
                        });
                    }
                }
            }
            RuleType::Path {
                must_exist,
                must_be_readable,
            } => {
                if let Some(ValidationValue::String(s)) = value {
                    let path = Path::new(s);

                    if *must_exist && !path.exists() {
                        result.valid = false;
                        result.issues.push(ValidationIssue {
                            field_path: rule.field_path.clone(),
                            rule_id: "path_exists".to_string(),
                            severity: rule.severity.clone(),
                            message: format!("Path does not exist: {}", s),
                            suggested_fix: Some(format!(
                                "Create the path {} or update the configuration",
                                s
                            )),
                            documentation_link: None,
                        });
                    }

                    if *must_be_readable && path.exists() {
                        if std::fs::metadata(path).is_err() {
                            result.valid = false;
                            result.issues.push(ValidationIssue {
                                field_path: rule.field_path.clone(),
                                rule_id: "path_readable".to_string(),
                                severity: rule.severity.clone(),
                                message: format!("Path is not readable: {}", s),
                                suggested_fix: Some(format!("Check permissions for path {}", s)),
                                documentation_link: None,
                            });
                        }
                    }
                }
            }
            RuleType::Custom { validator_name: _ } => {
                // Custom validation would be handled by the custom validators
                // This is a placeholder for field-specific custom rules
            }
            RuleType::Regex { pattern } => {
                if let Some(ValidationValue::String(s)) = value {
                    if let Ok(regex) = regex::Regex::new(pattern) {
                        if !regex.is_match(s) {
                            result.valid = false;
                            result.issues.push(ValidationIssue {
                                field_path: rule.field_path.clone(),
                                rule_id: "regex".to_string(),
                                severity: rule.severity.clone(),
                                message: format!(
                                    "{} (value: '{}', pattern: '{}')",
                                    rule.error_message, s, pattern
                                ),
                                suggested_fix: Some(format!(
                                    "Ensure {} matches the pattern: {}",
                                    rule.field_path, pattern
                                )),
                                documentation_link: None,
                            });
                        }
                    }
                }
            }
        }

        result
    }

    #[allow(clippy::only_used_in_recursion)]
    fn validate_cross_field_rule(
        &self,
        rule: &CrossFieldRule,
        config: &HashMap<String, ValidationValue>,
        context: &ValidationContext,
    ) -> ValidationResult {
        let mut result = ValidationResult {
            valid: true,
            issues: Vec::new(),
        };

        match &rule.condition {
            CrossFieldCondition::RequiredIf { field, equals } => {
                let field_value = self.get_field_value(config, field);
                if let Some(value) = field_value {
                    if value == equals {
                        // Check that all dependent fields are present
                        for dependent_field in &rule.dependent_fields {
                            if self.get_field_value(config, dependent_field).is_none() {
                                result.valid = false;
                                result.issues.push(ValidationIssue {
                                    field_path: dependent_field.clone(),
                                    rule_id: rule.rule_id.clone(),
                                    severity: rule.severity.clone(),
                                    message: format!(
                                        "{} (required because {} = {:?})",
                                        rule.error_message, field, equals
                                    ),
                                    suggested_fix: Some(format!(
                                        "Provide a value for {} or change {}",
                                        dependent_field, field
                                    )),
                                    documentation_link: None,
                                });
                            }
                        }
                    }
                }
            }
            CrossFieldCondition::MutuallyExclusive { fields } => {
                let present_fields: Vec<&String> = fields
                    .iter()
                    .filter(|field| self.get_field_value(config, field).is_some())
                    .collect();

                if present_fields.len() > 1 {
                    result.valid = false;
                    let present_fields_str: Vec<String> =
                        present_fields.iter().map(|s| (*s).clone()).collect();
                    result.issues.push(ValidationIssue {
                        field_path: present_fields_str.join(", "),
                        rule_id: rule.rule_id.clone(),
                        severity: rule.severity.clone(),
                        message: format!(
                            "{} (conflicting fields: {})",
                            rule.error_message,
                            present_fields_str.join(", ")
                        ),
                        suggested_fix: Some(format!("Choose only one of: {}", fields.join(", "))),
                        documentation_link: None,
                    });
                }
            }
            CrossFieldCondition::AtLeastOne { fields } => {
                let present_count = fields
                    .iter()
                    .filter(|field| self.get_field_value(config, field).is_some())
                    .count();

                if present_count == 0 {
                    result.valid = false;
                    result.issues.push(ValidationIssue {
                        field_path: fields.join(", "),
                        rule_id: rule.rule_id.clone(),
                        severity: rule.severity.clone(),
                        message: rule.error_message.clone(),
                        suggested_fix: Some(format!(
                            "Provide at least one of: {}",
                            fields.join(", ")
                        )),
                        documentation_link: None,
                    });
                }
            }
            CrossFieldCondition::DependsOn { field, condition } => {
                if self.get_field_value(config, field).is_some() {
                    // Recursively validate the dependent condition
                    let dependent_rule = CrossFieldRule {
                        rule_id: format!("{}_dependent", rule.rule_id),
                        description: format!("Dependent rule for {}", rule.rule_id),
                        dependent_fields: rule.dependent_fields.clone(),
                        condition: (**condition).clone(),
                        error_message: rule.error_message.clone(),
                        severity: rule.severity.clone(),
                    };

                    let dependent_result =
                        self.validate_cross_field_rule(&dependent_rule, config, context);
                    if !dependent_result.valid {
                        result.issues.extend(dependent_result.issues);
                        result.valid = false;
                    }
                }
            }
            CrossFieldCondition::Custom {
                condition_name: _,
                parameters: _,
            } => {
                // Custom cross-field validation would be implemented here
                // This is a placeholder for extensible cross-field rules
            }
        }

        result
    }

    fn get_field_value<'a>(
        &self,
        config: &'a HashMap<String, ValidationValue>,
        field_path: &str,
    ) -> Option<&'a ValidationValue> {
        // Handle nested field paths (e.g., "database.pool_size")
        let parts: Vec<&str> = field_path.split('.').collect();
        let mut current = config.get(parts[0])?;

        for part in parts.iter().skip(1) {
            if let ValidationValue::Object(obj) = current {
                current = obj.get(*part)?;
            } else {
                return None;
            }
        }

        Some(current)
    }

    fn is_valid_url(&self, url: &str, schemes: &[String]) -> bool {
        schemes
            .iter()
            .any(|scheme| url.starts_with(&format!("{}://", scheme)))
    }

    fn matches_environment_pattern(&self, pattern: &str, environment: &str) -> bool {
        if let Ok(regex) = regex::Regex::new(pattern) {
            regex.is_match(environment)
        } else {
            // Fallback to simple string matching
            pattern == environment || pattern == "*"
        }
    }
}

// ============================================================================
// Built-in Custom Validators
// ============================================================================

/// Database connection validator
#[derive(Debug)]
pub struct DatabaseConnectionValidator;

impl CustomValidator for DatabaseConnectionValidator {
    fn name(&self) -> &str {
        "database_connection"
    }

    fn validate(&self, _value: &ValidationValue, context: &ValidationContext) -> ValidationResult {
        let mut result = ValidationResult {
            valid: true,
            issues: Vec::new(),
        };

        // Extract database URL from config
        if let Some(ValidationValue::String(db_url)) = context.config.get("database_url") {
            // In a real implementation, we would test the actual database connection
            // For now, just validate the URL format
            if !db_url.starts_with("postgresql://") && !db_url.starts_with("postgres://") {
                result.valid = false;
                result.issues.push(ValidationIssue {
                    field_path: "database_url".to_string(),
                    rule_id: "database_connection".to_string(),
                    severity: Severity::Error,
                    message: "Invalid database URL format".to_string(),
                    suggested_fix: Some("Use a PostgreSQL connection string (postgresql://...)".to_string()),
                    documentation_link: Some("https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING".to_string()),
                });
            }
        }

        result
    }
}

/// Resource constraints validator
#[derive(Debug)]
pub struct ResourceConstraintsValidator;

impl CustomValidator for ResourceConstraintsValidator {
    fn name(&self) -> &str {
        "resource_constraints"
    }

    fn validate(&self, _value: &ValidationValue, context: &ValidationContext) -> ValidationResult {
        let mut result = ValidationResult {
            valid: true,
            issues: Vec::new(),
        };

        // Check for resource constraint violations
        if let Some(ValidationValue::Integer(pool_size)) = context.config.get("database_pool_size")
        {
            if let Some(ValidationValue::Integer(batch_size)) = context.config.get("batch_size") {
                // Warn if the combination might use too many resources
                let estimated_connections = pool_size * 2; // Rough estimate
                let estimated_memory_mb = (batch_size * 100) / (1024 * 1024); // Rough estimate

                if estimated_connections > 100 {
                    result.issues.push(ValidationIssue {
                        field_path: "database_pool_size".to_string(),
                        rule_id: "resource_constraints".to_string(),
                        severity: Severity::Warning,
                        message: format!(
                            "High database pool size ({}) may consume many connections",
                            pool_size
                        ),
                        suggested_fix: Some(
                            "Consider reducing database_pool_size for lower resource usage"
                                .to_string(),
                        ),
                        documentation_link: None,
                    });
                }

                if estimated_memory_mb > 512 {
                    result.issues.push(ValidationIssue {
                        field_path: "batch_size".to_string(),
                        rule_id: "resource_constraints".to_string(),
                        severity: Severity::Warning,
                        message: format!(
                            "Large batch size ({}) may consume significant memory",
                            batch_size
                        ),
                        suggested_fix: Some(
                            "Consider reducing batch_size for lower memory usage".to_string(),
                        ),
                        documentation_link: None,
                    });
                }
            }
        }

        result
    }
}

// ============================================================================
// Configuration Builder with Validation
// ============================================================================

/// Builder for creating validated configurations
#[derive(Debug)]
pub struct ValidatedConfigBuilder {
    validator: ConfigurationValidator,
    config: HashMap<String, ValidationValue>,
    environment: Option<String>,
}

impl Default for ValidatedConfigBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidatedConfigBuilder {
    /// Create a new validated config builder
    pub fn new() -> Self {
        Self {
            validator: ConfigurationValidator::new(),
            config: HashMap::new(),
            environment: None,
        }
    }

    /// Set the target environment
    pub fn environment(mut self, env: &str) -> Self {
        self.environment = Some(env.to_string());
        self
    }

    /// Add a validation rule
    pub fn add_rule(mut self, rule: ValidationRule) -> Self {
        self.validator = self.validator.add_rule(rule);
        self
    }

    /// Set a configuration value
    pub fn set<T: Into<ValidationValue>>(mut self, key: &str, value: T) -> Self {
        self.config.insert(key.to_string(), value.into());
        self
    }

    /// Build and validate the configuration
    pub fn build(self) -> Result<HashMap<String, ValidationValue>, ValidationResult> {
        let validation_result = self
            .validator
            .validate(&self.config, self.environment.as_deref());

        if validation_result.valid {
            Ok(self.config)
        } else {
            Err(validation_result)
        }
    }
}

// ============================================================================
// Convenience Functions
// ============================================================================

/// Create a standard Sinex configuration validator
pub fn create_sinex_validator() -> ConfigurationValidator {
    ConfigurationValidator::new()
        // Service name validation
        .add_rule(ValidationRule {
            field_path: "service_name".to_string(),
            rule_type: RuleType::Required,
            parameters: HashMap::new(),
            error_message: "Service name is required".to_string(),
            severity: Severity::Error,
        })
        .add_rule(ValidationRule {
            field_path: "service_name".to_string(),
            rule_type: RuleType::NotEmpty,
            parameters: HashMap::new(),
            error_message: "Service name cannot be empty".to_string(),
            severity: Severity::Error,
        })
        // Log level validation
        .add_rule(ValidationRule {
            field_path: "log_level".to_string(),
            rule_type: RuleType::Enum {
                allowed_values: vec![
                    "trace".to_string(),
                    "debug".to_string(),
                    "info".to_string(),
                    "warn".to_string(),
                    "error".to_string(),
                ],
            },
            parameters: HashMap::new(),
            error_message: "Invalid log level".to_string(),
            severity: Severity::Error,
        })
        // Database URL validation
        .add_rule(ValidationRule {
            field_path: "database_url".to_string(),
            rule_type: RuleType::Url {
                schemes: vec!["postgresql".to_string(), "postgres".to_string()],
            },
            parameters: HashMap::new(),
            error_message: "Database URL must be a PostgreSQL connection string".to_string(),
            severity: Severity::Error,
        })
        // Redis URL validation
        .add_rule(ValidationRule {
            field_path: "redis_url".to_string(),
            rule_type: RuleType::Url {
                schemes: vec!["redis".to_string(), "rediss".to_string()],
            },
            parameters: HashMap::new(),
            error_message: "Redis URL must be a valid Redis connection string".to_string(),
            severity: Severity::Error,
        })
        // Pool size validation
        .add_rule(ValidationRule {
            field_path: "database_pool_size".to_string(),
            rule_type: RuleType::Range {
                min: Some(1),
                max: Some(1000),
            },
            parameters: HashMap::new(),
            error_message: "Database pool size must be between 1 and 1000".to_string(),
            severity: Severity::Error,
        })
        // Cross-field rules
        .add_cross_field_rule(CrossFieldRule {
            rule_id: "automaton_requires_redis".to_string(),
            description: "Automaton configurations require Redis URL".to_string(),
            dependent_fields: vec!["redis_url".to_string()],
            condition: CrossFieldCondition::RequiredIf {
                field: "consumer_group".to_string(),
                equals: ValidationValue::String("automaton".to_string()),
            },
            error_message: "Redis URL is required for automaton configurations".to_string(),
            severity: Severity::Error,
        })
        // Custom validators
        .add_custom_validator(Box::new(DatabaseConnectionValidator))
        .add_custom_validator(Box::new(ResourceConstraintsValidator))
}

// ============================================================================
// Value Conversions
// ============================================================================

impl From<String> for ValidationValue {
    fn from(value: String) -> Self {
        ValidationValue::String(value)
    }
}

impl From<&str> for ValidationValue {
    fn from(value: &str) -> Self {
        ValidationValue::String(value.to_string())
    }
}

impl From<i64> for ValidationValue {
    fn from(value: i64) -> Self {
        ValidationValue::Integer(value)
    }
}

impl From<i32> for ValidationValue {
    fn from(value: i32) -> Self {
        ValidationValue::Integer(value as i64)
    }
}

impl From<bool> for ValidationValue {
    fn from(value: bool) -> Self {
        ValidationValue::Boolean(value)
    }
}

impl From<f64> for ValidationValue {
    fn from(value: f64) -> Self {
        ValidationValue::Float(value)
    }
}
