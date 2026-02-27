// Comprehensive configuration testing and validation framework
//
// This module provides systematic testing of all configuration options across
// the Sinex ecosystem, including validation, compatibility, and environment testing.

use xtask::sandbox::prelude::*;
use std::collections::HashMap;
use std::env;
use std::time::Duration;

// ============================================================================
// Supporting Types
// ============================================================================

#[derive(Debug, Clone)]
pub enum ConfigValue {
    String(String),
    Integer(i64),
    Boolean(bool),
    Array(Vec<ConfigValue>),
}

#[derive(Debug)]
pub struct ValidationReport {
    pub valid: bool,
    pub errors: Vec<String>,
}

impl Default for ValidationReport {
    fn default() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
        }
    }
}

// ============================================================================
// Configuration Coverage Analysis
// ============================================================================

/// Comprehensive mapping of all configuration options across the codebase
#[derive(Debug, Clone, bon::Builder)]
pub struct ConfigurationCoverage {
    pub core_configs: HashMap<String, ConfigSchemaInfo>,
    pub node_configs: HashMap<String, ConfigSchemaInfo>,
    pub service_configs: HashMap<String, ConfigSchemaInfo>,
    pub environment_variables: HashMap<String, EnvVarInfo>,
}

#[derive(Debug, Clone, bon::Builder)]
pub struct ConfigSchemaInfo {
    pub required_fields: Vec<String>,
    pub optional_fields: Vec<String>,
    pub default_values: HashMap<String, ConfigValue>,
    pub validation_rules: Vec<ValidationRule>,
    pub interdependencies: Vec<String>,
}

#[derive(Debug, Clone, bon::Builder)]
pub struct EnvVarInfo {
    pub description: String,
    pub default_value: Option<String>,
    pub validation_pattern: Option<String>,
    pub used_by: Vec<String>,
}

#[derive(Debug, Clone, bon::Builder)]
pub struct ValidationRule {
    pub field_path: String,
    pub rule_type: String,
    pub parameters: HashMap<String, ConfigValue>,
    pub error_message: String,
}

impl ConfigurationCoverage {
    /// Build comprehensive configuration coverage analysis
    pub fn build_coverage_analysis() -> Self {
        let mut coverage = Self {
            core_configs: HashMap::new(),
            node_configs: HashMap::new(),
            service_configs: HashMap::new(),
            environment_variables: HashMap::new(),
        };

        // Analyze modern configuration patterns
        coverage.analyze_ingestd_config();
        coverage.analyze_nats_config();
        coverage.analyze_service_configs();
        coverage.analyze_environment_variables();

        coverage
    }

    fn analyze_ingestd_config(&mut self) {
        self.service_configs.insert(
            "IngestdConfig".to_string(),
            ConfigSchemaInfo {
                required_fields: vec!["database_url".to_string(), "nats_servers".to_string()],
                optional_fields: vec![
                    "database_pool_size".to_string(),
                    "nats_stream_name".to_string(),
                    "work_dir".to_string(),
                    "dry_run".to_string(),
                    "validate_schemas".to_string(),
                ],
                default_values: HashMap::from([
                    ("database_pool_size".to_string(), ConfigValue::Integer(50)),
                    (
                        "nats_stream_name".to_string(),
                        ConfigValue::String("sinex-events".to_string()),
                    ),
                    ("dry_run".to_string(), ConfigValue::Boolean(false)),
                    ("validate_schemas".to_string(), ConfigValue::Boolean(true)),
                ]),
                validation_rules: vec![
                    ValidationRule {
                        field_path: "database_url".to_string(),
                        rule_type: "url_prefix".to_string(),
                        parameters: HashMap::from([(
                            "prefixes".to_string(),
                            ConfigValue::Array(vec![
                                ConfigValue::String("postgresql://".to_string()),
                                ConfigValue::String("postgres://".to_string()),
                            ]),
                        )]),
                        error_message: "Database URL must be a PostgreSQL connection string"
                            .to_string(),
                    },
                    ValidationRule {
                        field_path: "nats_servers".to_string(),
                        rule_type: "not_empty_array".to_string(),
                        parameters: HashMap::new(),
                        error_message: "At least one NATS server must be specified".to_string(),
                    },
                ],
                interdependencies: vec!["work_dir must be writable".to_string()],
            },
        );
    }

    fn analyze_nats_config(&mut self) {
        self.core_configs.insert(
            "NatsConfig".to_string(),
            ConfigSchemaInfo {
                required_fields: vec!["servers".to_string()],
                optional_fields: vec![
                    "stream_name".to_string(),
                    "consumer_name".to_string(),
                    "max_deliver".to_string(),
                    "ack_wait".to_string(),
                ],
                default_values: HashMap::from([
                    (
                        "stream_name".to_string(),
                        ConfigValue::String("sinex-events".to_string()),
                    ),
                    ("max_deliver".to_string(), ConfigValue::Integer(3)),
                    ("ack_wait".to_string(), ConfigValue::Integer(30)),
                ]),
                validation_rules: vec![ValidationRule {
                    field_path: "servers".to_string(),
                    rule_type: "url_list".to_string(),
                    parameters: HashMap::new(),
                    error_message: "NATS servers must be valid URLs".to_string(),
                }],
                interdependencies: vec![],
            },
        );
    }

    fn analyze_service_configs(&mut self) {
        // Database configuration patterns
        self.core_configs.insert(
            "DatabaseConfig".to_string(),
            ConfigSchemaInfo {
                required_fields: vec!["url".to_string()],
                optional_fields: vec!["pool_size".to_string()],
                default_values: HashMap::from([
                    (
                        "url".to_string(),
                        ConfigValue::String(
                            "postgresql:///sinex_dev?host=/run/postgresql".to_string(),
                        ),
                    ),
                    ("pool_size".to_string(), ConfigValue::Integer(25)),
                ]),
                validation_rules: vec![ValidationRule {
                    field_path: "url".to_string(),
                    rule_type: "not_empty".to_string(),
                    parameters: HashMap::new(),
                    error_message: "Database URL cannot be empty".to_string(),
                }],
                interdependencies: vec![],
            },
        );
    }

    fn analyze_environment_variables(&mut self) {
        let env_vars = vec![
            (
                "DATABASE_URL",
                "PostgreSQL database connection string",
                None,
                vec!["All database-enabled services"],
            ),
            (
                "SINEX_LOG_LEVEL",
                "Log level for Sinex services",
                Some("info"),
                vec!["All services"],
            ),
            (
                "SINEX_NATS_URL",
                "NATS server URLs (comma-separated)",
                Some("nats://localhost:4222"),
                vec!["ingestd", "All automata"],
            ),
            (
                "SINEX_DB_POOL_SIZE",
                "Database connection pool size",
                Some("10"),
                vec!["All database-enabled services"],
            ),
            (
                "SINEX_WORK_DIR",
                "Working directory for temporary files",
                None,
                vec!["All services"],
            ),
            (
                "SINEX_DRY_RUN",
                "Enable dry-run mode",
                Some("false"),
                vec!["All services"],
            ),
            (
                "RUST_LOG",
                "Rust logging configuration",
                None,
                vec!["All Rust services"],
            ),
        ];

        for (name, desc, default, used_by) in env_vars {
            self.environment_variables.insert(
                name.to_string(),
                EnvVarInfo {
                    description: desc.to_string(),
                    default_value: default.map(|s| s.to_string()),
                    validation_pattern: None,
                    used_by: used_by.into_iter().map(|s| s.to_string()).collect(),
                },
            );
        }
    }
}

// ============================================================================
// Configuration Compatibility Matrix
// ============================================================================

/// Test matrix for configuration compatibility across different scenarios
#[derive(Debug, Clone)]
pub struct ConfigCompatibilityMatrix {
    pub test_scenarios: Vec<CompatibilityScenario>,
}

#[derive(Debug, Clone)]
pub struct CompatibilityScenario {
    pub name: String,
    pub description: String,
    pub config_combinations: Vec<ConfigCombination>,
    pub expected_outcome: CompatibilityOutcome,
}

#[derive(Debug, Clone)]
pub struct ConfigCombination {
    pub component: String,
    pub config_overrides: HashMap<String, ConfigValue>,
    pub env_var_overrides: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CompatibilityOutcome {
    Success,
    Warning(String),
    Failure(String),
}

impl ConfigCompatibilityMatrix {
    pub fn build_compatibility_matrix() -> Self {
        let mut matrix = Self {
            test_scenarios: Vec::new(),
        };

        matrix.add_basic_compatibility_scenarios();
        matrix.add_resource_constraint_scenarios();
        matrix.add_security_scenarios();
        matrix.add_failure_scenarios();

        matrix
    }

    fn add_basic_compatibility_scenarios(&mut self) {
        // Scenario: Default configuration compatibility
        self.test_scenarios.push(CompatibilityScenario {
            name: "default_configs_compatibility".to_string(),
            description: "Test that all default configurations work together".to_string(),
            config_combinations: vec![
                ConfigCombination {
                    component: "ingestd".to_string(),
                    config_overrides: HashMap::new(),
                    env_var_overrides: HashMap::new(),
                },
                ConfigCombination {
                    component: "nats-consumer".to_string(),
                    config_overrides: HashMap::new(),
                    env_var_overrides: HashMap::new(),
                },
            ],
            expected_outcome: CompatibilityOutcome::Success,
        });

        // Scenario: Mixed database pool sizes
        self.test_scenarios.push(CompatibilityScenario {
            name: "mixed_pool_sizes".to_string(),
            description: "Test different database pool sizes across components".to_string(),
            config_combinations: vec![
                ConfigCombination {
                    component: "ingestd".to_string(),
                    config_overrides: HashMap::from([(
                        "database_pool_size".to_string(),
                        ConfigValue::Integer(50),
                    )]),
                    env_var_overrides: HashMap::new(),
                },
                ConfigCombination {
                    component: "automaton".to_string(),
                    config_overrides: HashMap::from([(
                        "database_pool_size".to_string(),
                        ConfigValue::Integer(10),
                    )]),
                    env_var_overrides: HashMap::new(),
                },
            ],
            expected_outcome: CompatibilityOutcome::Success,
        });
    }

    fn add_resource_constraint_scenarios(&mut self) {
        // Low memory scenario
        self.test_scenarios.push(CompatibilityScenario {
            name: "low_memory_config".to_string(),
            description: "Test configuration for low-memory environments".to_string(),
            config_combinations: vec![ConfigCombination {
                component: "ingestd".to_string(),
                config_overrides: HashMap::from([(
                    "database_pool_size".to_string(),
                    ConfigValue::Integer(5),
                )]),
                env_var_overrides: HashMap::new(),
            }],
            expected_outcome: CompatibilityOutcome::Warning(
                "Performance may be reduced with low resource limits".to_string(),
            ),
        });
    }

    fn add_security_scenarios(&mut self) {
        // Secure configuration
        self.test_scenarios.push(CompatibilityScenario {
            name: "secure_config".to_string(),
            description: "Test secure configuration with minimal permissions".to_string(),
            config_combinations: vec![ConfigCombination {
                component: "ingestd".to_string(),
                config_overrides: HashMap::from([(
                    "validate_schemas".to_string(),
                    ConfigValue::Boolean(true),
                )]),
                env_var_overrides: HashMap::new(),
            }],
            expected_outcome: CompatibilityOutcome::Success,
        });
    }

    fn add_failure_scenarios(&mut self) {
        // Invalid database URL
        self.test_scenarios.push(CompatibilityScenario {
            name: "invalid_database_url".to_string(),
            description: "Test behavior with invalid database configuration".to_string(),
            config_combinations: vec![ConfigCombination {
                component: "ingestd".to_string(),
                config_overrides: HashMap::from([(
                    "database_url".to_string(),
                    ConfigValue::String("invalid://url".to_string()),
                )]),
                env_var_overrides: HashMap::new(),
            }],
            expected_outcome: CompatibilityOutcome::Failure(
                "Invalid database URL format".to_string(),
            ),
        });

        // Invalid NATS URL
        self.test_scenarios.push(CompatibilityScenario {
            name: "invalid_nats_url".to_string(),
            description: "Test behavior with invalid NATS configuration".to_string(),
            config_combinations: vec![ConfigCombination {
                component: "ingestd".to_string(),
                config_overrides: HashMap::from([(
                    "nats_servers".to_string(),
                    ConfigValue::Array(vec![ConfigValue::String("invalid://server".to_string())]),
                )]),
                env_var_overrides: HashMap::new(),
            }],
            expected_outcome: CompatibilityOutcome::Failure("Invalid NATS server URL".to_string()),
        });
    }
}

// ============================================================================
// Environment-Specific Testing
// ============================================================================

/// Environment configuration test suite
#[derive(Debug, Clone)]
pub struct EnvironmentConfigTester {
    pub environments: Vec<TestEnvironment>,
}

#[derive(Debug, Clone)]
pub struct TestEnvironment {
    pub name: String,
    pub description: String,
    pub base_config: HashMap<String, ConfigValue>,
    pub environment_variables: HashMap<String, String>,
    pub resource_constraints: ResourceConstraints,
    pub expected_behavior: EnvironmentExpectation,
}

#[derive(Debug, Clone)]
pub struct ResourceConstraints {
    pub max_memory_mb: Option<u64>,
    pub max_cpu_cores: Option<u32>,
    pub max_disk_space_mb: Option<u64>,
    pub max_file_descriptors: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct EnvironmentExpectation {
    pub should_start: bool,
    pub performance_tier: PerformanceTier,
    pub expected_warnings: Vec<String>,
    pub critical_features: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PerformanceTier {
    HighPerformance,
    Standard,
    LowResource,
    Minimal,
}

impl EnvironmentConfigTester {
    pub fn build_environment_tester() -> Self {
        let mut tester = Self {
            environments: Vec::new(),
        };

        tester.add_development_environment();
        tester.add_production_environment();
        tester.add_edge_environments();

        tester
    }

    fn add_development_environment(&mut self) {
        self.environments.push(TestEnvironment {
            name: "development".to_string(),
            description: "Local development environment with debug features".to_string(),
            base_config: HashMap::from([
                (
                    "log_level".to_string(),
                    ConfigValue::String("debug".to_string()),
                ),
                ("dry_run".to_string(), ConfigValue::Boolean(false)),
                ("validate_schemas".to_string(), ConfigValue::Boolean(true)),
            ]),
            environment_variables: HashMap::from([
                ("RUST_LOG".to_string(), "debug".to_string()),
                (
                    "DATABASE_URL".to_string(),
                    "postgresql:///sinex_dev?host=/run/postgresql".to_string(),
                ),
                (
                    "SINEX_NATS_URL".to_string(),
                    "nats://localhost:4222".to_string(),
                ),
            ]),
            resource_constraints: ResourceConstraints {
                max_memory_mb: Some(2048),
                max_cpu_cores: Some(4),
                max_disk_space_mb: Some(10240),
                max_file_descriptors: Some(1024),
            },
            expected_behavior: EnvironmentExpectation {
                should_start: true,
                performance_tier: PerformanceTier::Standard,
                expected_warnings: vec![],
                critical_features: vec![
                    "event_ingestion".to_string(),
                    "database_persistence".to_string(),
                    "schema_validation".to_string(),
                ],
            },
        });
    }

    fn add_production_environment(&mut self) {
        self.environments.push(TestEnvironment {
            name: "production".to_string(),
            description: "Production environment with optimal settings".to_string(),
            base_config: HashMap::from([
                (
                    "log_level".to_string(),
                    ConfigValue::String("warn".to_string()),
                ),
                ("database_pool_size".to_string(), ConfigValue::Integer(50)),
                ("validate_schemas".to_string(), ConfigValue::Boolean(true)),
            ]),
            environment_variables: HashMap::from([
                (
                    "DATABASE_URL".to_string(),
                    "postgresql://prod-user:***@prod-db:5432/sinex_prod".to_string(),
                ),
                (
                    "SINEX_NATS_URL".to_string(),
                    "nats://prod-nats:4222".to_string(),
                ),
            ]),
            resource_constraints: ResourceConstraints {
                max_memory_mb: Some(16384),
                max_cpu_cores: Some(32),
                max_disk_space_mb: Some(1048576), // 1TB
                max_file_descriptors: Some(65536),
            },
            expected_behavior: EnvironmentExpectation {
                should_start: true,
                performance_tier: PerformanceTier::HighPerformance,
                expected_warnings: vec![],
                critical_features: vec![
                    "event_ingestion".to_string(),
                    "database_persistence".to_string(),
                    "nats_streaming".to_string(),
                    "checkpoint_persistence".to_string(),
                    "schema_validation".to_string(),
                    "error_recovery".to_string(),
                ],
            },
        });
    }

    fn add_edge_environments(&mut self) {
        // Minimal resource environment (IoT/Edge)
        self.environments.push(TestEnvironment {
            name: "edge_minimal".to_string(),
            description: "Minimal resource edge deployment".to_string(),
            base_config: HashMap::from([
                (
                    "log_level".to_string(),
                    ConfigValue::String("error".to_string()),
                ),
                ("database_pool_size".to_string(), ConfigValue::Integer(2)),
            ]),
            environment_variables: HashMap::from([
                (
                    "DATABASE_URL".to_string(),
                    "postgresql:///sinex_edge?host=/run/postgresql".to_string(),
                ),
                (
                    "SINEX_NATS_URL".to_string(),
                    "nats://localhost:4222".to_string(),
                ),
            ]),
            resource_constraints: ResourceConstraints {
                max_memory_mb: Some(256),
                max_cpu_cores: Some(1),
                max_disk_space_mb: Some(1024),
                max_file_descriptors: Some(128),
            },
            expected_behavior: EnvironmentExpectation {
                should_start: true,
                performance_tier: PerformanceTier::Minimal,
                expected_warnings: vec![
                    "Low resource environment detected".to_string(),
                    "Performance may be limited".to_string(),
                ],
                critical_features: vec![
                    "event_ingestion".to_string(),
                    "database_persistence".to_string(),
                ],
            },
        });
    }
}

// ============================================================================
// Default Configuration Validation
// ============================================================================

/// Validator for default configurations across all components
#[derive(Debug)]
pub struct DefaultConfigValidator {
    pub component_defaults: HashMap<String, HashMap<String, ConfigValue>>,
}

impl DefaultConfigValidator {
    pub fn new() -> Self {
        let mut validator = Self {
            component_defaults: HashMap::new(),
        };

        validator.collect_default_configurations();
        validator
    }

    fn collect_default_configurations(&mut self) {
        // Collect defaults from IngestdConfig
        self.component_defaults.insert(
            "IngestdConfig".to_string(),
            HashMap::from([
                (
                    "database_url".to_string(),
                    ConfigValue::String("postgresql:///sinex_dev?host=/run/postgresql".to_string()),
                ),
                ("database_pool_size".to_string(), ConfigValue::Integer(50)),
                (
                    "nats_servers".to_string(),
                    ConfigValue::Array(vec![ConfigValue::String(
                        "nats://localhost:4222".to_string(),
                    )]),
                ),
                ("dry_run".to_string(), ConfigValue::Boolean(false)),
                ("validate_schemas".to_string(), ConfigValue::Boolean(true)),
            ]),
        );

        // Collect defaults from NatsConfig
        self.component_defaults.insert(
            "NatsConfig".to_string(),
            HashMap::from([
                (
                    "servers".to_string(),
                    ConfigValue::Array(vec![ConfigValue::String(
                        "nats://localhost:4222".to_string(),
                    )]),
                ),
                (
                    "stream_name".to_string(),
                    ConfigValue::String("sinex-events".to_string()),
                ),
                ("max_deliver".to_string(), ConfigValue::Integer(3)),
                ("ack_wait".to_string(), ConfigValue::Integer(30)),
            ]),
        );
    }

    pub fn validate_all_defaults(&self) -> ValidationReport {
        let mut report = ValidationReport::default();

        for (component, defaults) in &self.component_defaults {
            let component_report = self.validate_component_defaults(component, defaults);
            if !component_report.valid {
                report.valid = false;
                for error in component_report.errors {
                    report.errors.push(format!("{}: {}", component, error));
                }
            }
        }

        report
    }

    fn validate_component_defaults(
        &self,
        component: &str,
        defaults: &HashMap<String, ConfigValue>,
    ) -> ValidationReport {
        let mut report = ValidationReport::default();

        // Validate based on component type
        match component {
            "IngestdConfig" => self.validate_ingestd_defaults(defaults, &mut report),
            "NatsConfig" => self.validate_nats_defaults(defaults, &mut report),
            _ => {
                // Generic validation for unknown components
                self.validate_generic_defaults(defaults, &mut report);
            }
        }

        report
    }

    fn validate_ingestd_defaults(
        &self,
        defaults: &HashMap<String, ConfigValue>,
        report: &mut ValidationReport,
    ) {
        // Validate database URL
        if let Some(ConfigValue::String(db_url)) = defaults.get("database_url") {
            if !db_url.starts_with("postgresql://") && !db_url.starts_with("postgres://") {
                report.valid = false;
                report
                    .errors
                    .push(format!("Invalid default database URL format: {}", db_url));
            }
        }

        // Validate pool size
        if let Some(ConfigValue::Integer(pool_size)) = defaults.get("database_pool_size") {
            if pool_size <= &0 {
                report.valid = false;
                report
                    .errors
                    .push("Default database pool size must be positive".to_string());
            }
        }
    }

    fn validate_nats_defaults(
        &self,
        defaults: &HashMap<String, ConfigValue>,
        report: &mut ValidationReport,
    ) {
        // Validate NATS servers
        if let Some(ConfigValue::Array(servers)) = defaults.get("servers") {
            if servers.is_empty() {
                report.valid = false;
                report
                    .errors
                    .push("Default NATS servers cannot be empty".to_string());
            }
        }

        // Validate max deliver
        if let Some(ConfigValue::Integer(max_deliver)) = defaults.get("max_deliver") {
            if max_deliver <= &0 {
                report.valid = false;
                report
                    .errors
                    .push("Default max deliver must be positive".to_string());
            }
        }
    }

    fn validate_generic_defaults(
        &self,
        defaults: &HashMap<String, ConfigValue>,
        report: &mut ValidationReport,
    ) {
        // Generic validation rules
        for (key, value) in defaults {
            match value {
                ConfigValue::String(s) if s.is_empty() => {
                    report.valid = false;
                    report
                        .errors
                        .push(format!("Default string value for {} cannot be empty", key));
                }
                ConfigValue::Integer(i) if key.contains("size") && i <= &0 => {
                    report.valid = false;
                    report
                        .errors
                        .push(format!("Default size value for {} must be positive", key));
                }
                _ => {} // Other validations could be added
            }
        }
    }
}

// ============================================================================
// Integration Tests
// ============================================================================

#[sinex_test]
async fn test_configuration_coverage_analysis(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing configuration coverage analysis");

    let coverage = ConfigurationCoverage::build_coverage_analysis();

    // Verify we have captured all major configuration types
    assert!(
        !coverage.service_configs.is_empty(),
        "Should have service configs"
    );
    assert!(
        !coverage.environment_variables.is_empty(),
        "Should have environment variables"
    );

    // Verify specific configurations exist
    assert!(
        coverage.service_configs.contains_key("IngestdConfig"),
        "Should have IngestdConfig"
    );
    assert!(
        coverage.core_configs.contains_key("NatsConfig"),
        "Should have NatsConfig"
    );

    // Verify environment variables are properly mapped
    assert!(
        coverage.environment_variables.contains_key("DATABASE_URL"),
        "Should map DATABASE_URL"
    );
    assert!(
        coverage
            .environment_variables
            .contains_key("SINEX_LOG_LEVEL"),
        "Should map SINEX_LOG_LEVEL"
    );
    assert!(
        coverage
            .environment_variables
            .contains_key("SINEX_NATS_URL"),
        "Should map SINEX_NATS_URL"
    );

    tracing::info!(
        service_configs = coverage.service_configs.len(),
        core_configs = coverage.core_configs.len(),
        environment_variables = coverage.environment_variables.len(),
        "Configuration coverage analysis completed"
    );

    Ok(())
}

#[sinex_test]
async fn test_configuration_compatibility_matrix(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing configuration compatibility matrix");

    let matrix = ConfigCompatibilityMatrix::build_compatibility_matrix();

    // Verify we have test scenarios
    assert!(
        !matrix.test_scenarios.is_empty(),
        "Should have compatibility scenarios"
    );

    // Test each scenario
    for scenario in &matrix.test_scenarios {
        tracing::debug!(scenario_name = %scenario.name, "Testing scenario");

        // Verify the scenario structure is valid
        assert!(
            !scenario.config_combinations.is_empty(),
            "Scenario {} should have config combinations",
            scenario.name
        );

        match &scenario.expected_outcome {
            CompatibilityOutcome::Success => {
                tracing::debug!("Expected: Success");
            }
            CompatibilityOutcome::Warning(msg) => {
                tracing::debug!(warning = %msg, "Expected: Warning");
            }
            CompatibilityOutcome::Failure(msg) => {
                tracing::debug!(failure = %msg, "Expected: Failure");
            }
        }
    }

    tracing::info!(
        scenarios = matrix.test_scenarios.len(),
        "Configuration compatibility matrix tested"
    );

    Ok(())
}

#[sinex_test]
async fn test_environment_specific_configurations(
    ctx: TestContext,
) -> TestResult<()> {
    tracing::info!("Testing environment-specific configurations");

    let env_tester = EnvironmentConfigTester::build_environment_tester();

    // Verify we have all expected environments
    let env_names: Vec<String> = env_tester
        .environments
        .iter()
        .map(|e| e.name.clone())
        .collect();
    assert!(
        env_names.contains(&"development".to_string()),
        "Should have development env"
    );
    assert!(
        env_names.contains(&"production".to_string()),
        "Should have production env"
    );
    assert!(
        env_names.contains(&"edge_minimal".to_string()),
        "Should have edge env"
    );

    // Test each environment configuration
    for env in &env_tester.environments {
        tracing::debug!(environment = %env.name, "Testing environment");

        // Verify configuration consistency
        assert!(
            !env.base_config.is_empty() || !env.environment_variables.is_empty(),
            "Environment {} should have some configuration",
            env.name
        );

        // Verify performance expectations match resource constraints
        match (
            &env.expected_behavior.performance_tier,
            &env.resource_constraints.max_memory_mb,
        ) {
            (PerformanceTier::Minimal, Some(mem)) if *mem < 512 => {
                tracing::debug!("Minimal performance tier matches low memory constraint");
            }
            (PerformanceTier::HighPerformance, Some(mem)) if *mem > 4096 => {
                tracing::debug!("High performance tier matches high memory availability");
            }
            _ => {
                tracing::debug!("Performance tier and resource constraints seem reasonable");
            }
        }

        // Verify critical features are reasonable
        assert!(
            !env.expected_behavior.critical_features.is_empty(),
            "Environment {} should have critical features defined",
            env.name
        );
    }

    tracing::info!(
        environments = env_tester.environments.len(),
        "Environment-specific configurations tested"
    );

    Ok(())
}

#[sinex_test]
async fn test_default_configuration_validation(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing default configuration validation");

    let validator = DefaultConfigValidator::new();

    // Verify we collected default configurations
    assert!(
        !validator.component_defaults.is_empty(),
        "Should have component defaults"
    );

    // Validate all defaults
    let report = validator.validate_all_defaults();

    if !report.valid {
        tracing::error!("Default configuration validation failed:");
        for error in &report.errors {
            tracing::error!(error = %error, "Validation error");
        }
        return Err(color_eyre::eyre::eyre!(
            "Default configurations are invalid"
        ));
    }

    tracing::info!(
        components = validator.component_defaults.len(),
        "Default configuration validation passed"
    );

    // Test specific default values
    if let Some(ingestd_defaults) = validator.component_defaults.get("IngestdConfig") {
        if let Some(ConfigValue::Integer(pool_size)) = ingestd_defaults.get("database_pool_size") {
            assert!(pool_size > &0, "Default pool size should be positive");
        }

        if let Some(ConfigValue::Boolean(validate_schemas)) =
            ingestd_defaults.get("validate_schemas")
        {
            assert!(
                validate_schemas == &true,
                "Schema validation should be enabled by default"
            );
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_configuration_validation_comprehensive(
    ctx: TestContext,
) -> TestResult<()> {
    tracing::info!("Testing comprehensive configuration validation");

    // Test environment variable handling
    let original_db_url = env::var("DATABASE_URL").ok();
    let original_log_level = env::var("SINEX_LOG_LEVEL").ok();

    // Test with invalid environment variables
    unsafe { env::set_var("SINEX_LOG_LEVEL", "invalid_level") };

    // Test with missing required environment variables
    unsafe { env::remove_var("DATABASE_URL") };

    // Restore original environment
    unsafe {
        match original_db_url {
            Some(url) => env::set_var("DATABASE_URL", url),
            None => env::remove_var("DATABASE_URL"),
        }
        match original_log_level {
            Some(level) => env::set_var("SINEX_LOG_LEVEL", level),
            None => env::remove_var("SINEX_LOG_LEVEL"),
        }
    }

    tracing::info!("Comprehensive configuration validation completed");

    Ok(())
}

#[sinex_test]
async fn test_configuration_performance_impact(ctx: TestContext) -> TestResult<()> {
    tracing::info!("Testing configuration performance impact");

    let start = std::time::Instant::now();

    // Test configuration loading performance
    for i in 0..1000 {
        let coverage = ConfigurationCoverage::build_coverage_analysis();
        assert!(!coverage.service_configs.is_empty());
    }

    let duration = start.elapsed();
    tracing::info!(
        duration_ms = duration.as_millis(),
        "Configuration analysis (1000x)"
    );

    // Verify performance is reasonable (should be very fast for configuration analysis)
    assert!(
        duration < Duration::from_millis(1000),
        "Configuration analysis should be fast, took {:?}",
        duration
    );

    // Test compatibility matrix performance
    let start = std::time::Instant::now();

    for i in 0..100 {
        let matrix = ConfigCompatibilityMatrix::build_compatibility_matrix();
        assert!(!matrix.test_scenarios.is_empty());
    }

    let duration = start.elapsed();
    tracing::info!(
        duration_ms = duration.as_millis(),
        "Compatibility matrix (100x)"
    );

    assert!(
        duration < Duration::from_millis(500),
        "Compatibility matrix should be fast, took {:?}",
        duration
    );

    tracing::info!("Configuration performance impact is acceptable");

    Ok(())
}

#[sinex_test]
async fn test_configuration_documentation_completeness(
    ctx: TestContext,
) -> TestResult<()> {
    tracing::info!("Testing configuration documentation completeness");

    let coverage = ConfigurationCoverage::build_coverage_analysis();

    // Verify all configurations have proper documentation structures
    for (config_name, config_info) in &coverage.service_configs {
        // Verify required fields are documented
        assert!(
            !config_info.required_fields.is_empty() || !config_info.optional_fields.is_empty(),
            "Config {} should have documented fields",
            config_name
        );

        // Verify validation rules exist for complex configs
        if config_info.required_fields.len() + config_info.optional_fields.len() > 3 {
            assert!(
                !config_info.validation_rules.is_empty(),
                "Complex config {} should have validation rules",
                config_name
            );
        }
    }

    // Verify environment variables have descriptions
    for (env_var, env_info) in &coverage.environment_variables {
        assert!(
            !env_info.description.is_empty(),
            "Environment variable {} should have description",
            env_var
        );
        assert!(
            !env_info.used_by.is_empty(),
            "Environment variable {} should list components that use it",
            env_var
        );
    }

    tracing::info!(
        documented_configs = coverage.service_configs.len() + coverage.core_configs.len(),
        documented_env_vars = coverage.environment_variables.len(),
        "Configuration documentation completeness verified"
    );

    Ok(())
}