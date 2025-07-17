# Configuration Testing and Validation Framework

This document describes the comprehensive configuration testing and validation framework implemented for the Sinex project. This framework ensures configuration reliability, compatibility, and maintainability across all components.

## Overview

The configuration testing framework provides:

1. **Configuration Coverage Analysis** - Maps all configuration options across all crates
2. **Configuration Validation Framework** - Advanced validation with cross-field dependencies
3. **Compatibility Testing Matrix** - Tests configuration combinations across components
4. **Environment-Specific Testing** - Validates configurations for different deployment environments
5. **Default Configuration Validation** - Ensures sensible defaults across all components
6. **Migration and Versioning Support** - Tests configuration changes and upgrades
7. **Enhanced Error Handling** - User-friendly error messages and suggestions

## Architecture

### Core Components

#### 1. Configuration Coverage Analysis (`ConfigurationCoverage`)

Located in: `test/integration/configuration_comprehensive_test.rs`

Provides systematic mapping of all configuration options:

```rust
pub struct ConfigurationCoverage {
    pub core_configs: HashMap<String, ConfigSchemaInfo>,
    pub satellite_configs: HashMap<String, ConfigSchemaInfo>,
    pub service_configs: HashMap<String, ConfigSchemaInfo>,
    pub environment_variables: HashMap<String, EnvVarInfo>,
}
```

**Features:**
- Maps all configuration types (SatelliteConfig, IngestdConfig, service-specific configs)
- Tracks required vs optional fields
- Documents default values and validation rules
- Maps environment variable usage across components

#### 2. Advanced Validation Framework (`ConfigurationValidator`)

Located in: `crate/sinex-config/src/validation_framework.rs`

Provides comprehensive validation capabilities:

```rust
pub struct ConfigurationValidator {
    validation_rules: Vec<ValidationRule>,
    cross_field_rules: Vec<CrossFieldRule>,
    environment_rules: Vec<EnvironmentRule>,
    custom_validators: Vec<Box<dyn CustomValidator>>,
}
```

**Validation Types:**
- **Field-level validation**: Required, NotEmpty, Range, Regex, Enum, URL, Path
- **Cross-field validation**: RequiredIf, MutuallyExclusive, AtLeastOne, DependsOn
- **Environment-aware validation**: Different rules for dev/staging/prod
- **Custom validation**: Extensible validation logic for complex scenarios

#### 3. Compatibility Testing Matrix (`ConfigCompatibilityMatrix`)

Located in: `test/integration/configuration_comprehensive_test.rs`

Tests configuration combinations across components:

```rust
pub struct CompatibilityTestScenario {
    pub name: String,
    pub description: String,
    pub config_combinations: Vec<ConfigCombination>,
    pub expected_outcome: CompatibilityOutcome,
}
```

**Test Scenarios:**
- Default configuration compatibility
- Resource constraint scenarios
- Security configurations
- Performance configurations
- Failure scenarios

#### 4. Configuration Compatibility Tester (`ConfigCompatibilityTester`)

Located in: `test/common/config_compatibility_tester.rs`

Provides end-to-end compatibility testing:

```rust
pub struct ConfigCompatibilityTester {
    test_scenarios: Vec<CompatibilityTestScenario>,
    temp_dir: TempDir,
}
```

**Capabilities:**
- Environment simulation (dev/staging/prod/edge)
- Resource constraint testing
- Dependency availability testing
- Performance expectation validation
- Issue detection and recommendations

## Configuration Types Covered

### 1. Satellite Configurations

#### SatelliteConfig (Base)
- **Required Fields**: `service_name`
- **Optional Fields**: `log_level`, `ingest_socket_path`, `redis_url`, `database_url`, `database_pool_size`, `work_dir`, `dry_run`, `replay`
- **Default Values**: `log_level="info"`, `ingest_socket_path="/run/sinex/ingest.sock"`, etc.
- **Validation Rules**: Non-empty service name, valid log levels, URL format validation

#### EventSourceConfig
- **Required Fields**: `base` (SatelliteConfig)
- **Optional Fields**: `batch_size`, `batch_timeout_secs`, `source_config`
- **Default Values**: `batch_size=100`, `batch_timeout_secs=5`
- **Validation Rules**: Positive batch sizes, accessible socket paths

#### AutomatonConfig
- **Required Fields**: `base`, `consumer_group`, `consumer_name`, `topics`
- **Optional Fields**: `processing_batch_size`, `checkpoint_interval_secs`, `automaton_config`
- **Dependencies**: Requires Redis URL and database URL for checkpoint persistence

### 2. Service Configurations

#### IngestdConfig
- **Required Fields**: `database_url`, `redis_url`, `socket_path`
- **Optional Fields**: `database_pool_size`, `batch_size`, `batch_timeout_secs`, `dry_run`, etc.
- **Validation**: Connection testing, directory creation, URL format validation

#### Component-Specific Configurations
- **FilesystemConfig**: Watch paths, ignore patterns, recursion settings
- **TerminalConfig**: Shell types, history sources, scrollback sources
- **DesktopConfig**: Window manager, clipboard monitoring

### 3. Environment Variables

Comprehensive mapping of all environment variables:

- `DATABASE_URL` - PostgreSQL connection string (used by all database-enabled services)
- `SINEX_LOG_LEVEL` - Log level override (used by all satellites)
- `SINEX_INGEST_SOCKET` - Socket path override (used by ingestd and ingestors)
- `SINEX_REDIS_URL` - Redis connection override (used by ingestd and automata)
- `SINEX_DB_POOL_SIZE` - Pool size override (used by all database services)
- `SINEX_WORK_DIR` - Working directory override (used by all satellites)
- `SINEX_DRY_RUN` - Dry-run mode toggle (used by all satellites)
- `SINEX_CONFIG` - Configuration file path (used by all services)
- `RUST_LOG` - Rust logging configuration (used by all Rust services)

## Test Scenarios

### 1. Environment-Specific Testing

#### Development Environment
- **Configuration**: Debug logging, schema validation enabled, smaller resource limits
- **Resources**: 2GB memory, 4 CPU cores, 10GB disk
- **Dependencies**: Local PostgreSQL and Redis
- **Expectations**: Fast startup, debug features available, dry-run mode supported

#### Staging Environment
- **Configuration**: Info logging, production-like settings, moderate resources
- **Resources**: 4GB memory, 8 CPU cores, 50GB disk
- **Dependencies**: Dedicated staging database and Redis
- **Expectations**: Production-like performance, all features working

#### Production Environment
- **Configuration**: Warning logging, optimized batch sizes, high resource limits
- **Resources**: 16GB memory, 32 CPU cores, 1TB disk
- **Dependencies**: High-availability database and Redis clusters
- **Expectations**: High throughput, low latency, full feature set

#### Edge Computing Environment
- **Configuration**: Error-only logging, minimal resource usage, small batches
- **Resources**: 256MB memory, 1 CPU core, 1GB disk
- **Dependencies**: Local database only
- **Expectations**: Minimal functionality, low resource consumption

### 2. Compatibility Testing Scenarios

#### Default Configuration Compatibility
Tests that all default configurations work together without conflicts.

#### Mixed Pool Sizes
Tests different database pool sizes across components to ensure they don't interfere.

#### Resource Constraint Testing
Tests behavior under low memory, CPU, or disk space conditions.

#### Security Configuration Testing
Tests secure configurations with minimal permissions and validation enabled.

#### High-Throughput Configuration
Tests configurations optimized for maximum event processing throughput.

#### Failure Scenarios
Tests behavior with invalid configurations, unreachable dependencies, and conflicting settings.

### 3. Validation Testing

#### Field-Level Validation
- Required field presence
- Empty string validation
- Numeric range validation
- URL format validation
- File path existence and permissions
- Regular expression pattern matching

#### Cross-Field Validation
- Conditional requirements (Redis URL required for automata)
- Mutually exclusive options
- At-least-one requirements
- Complex dependencies

#### Custom Validation
- Database connection testing
- Resource constraint analysis
- Performance impact assessment

## Usage Guide

### Running Configuration Tests

#### Complete Test Suite
```bash
# Run all configuration tests
cargo test --test configuration_comprehensive_test

# Run specific test categories
cargo test test_configuration_coverage_analysis
cargo test test_configuration_compatibility_matrix
cargo test test_environment_specific_configurations
cargo test test_default_configuration_validation
cargo test test_configuration_error_handling
```

#### Compatibility Testing
```rust
use sinex_test_common::config_compatibility_tester::ConfigCompatibilityTester;

#[tokio::test]
async fn test_my_configuration_scenario() {
    let tester = ConfigCompatibilityTester::new().await?;
    let results = tester.run_all_tests().await?;
    
    for result in &results {
        if !result.overall_success {
            println!("❌ Failed: {}", result.scenario_name);
            for issue in &result.issues_found {
                println!("  - {}: {}", issue.severity, issue.description);
            }
        }
    }
}
```

### Using the Validation Framework

#### Basic Configuration Validation
```rust
use sinex_config::{create_sinex_validator, ValidationValue};

let validator = create_sinex_validator();
let config = HashMap::from([
    ("service_name".to_string(), ValidationValue::String("my-service".to_string())),
    ("log_level".to_string(), ValidationValue::String("info".to_string())),
    ("database_url".to_string(), ValidationValue::String("postgresql://localhost/sinex".to_string())),
]);

let result = validator.validate(&config, Some("development"));
if !result.valid {
    for issue in &result.issues {
        println!("⚠️ {}: {}", issue.severity, issue.message);
        if let Some(fix) = &issue.suggested_fix {
            println!("   💡 Suggestion: {}", fix);
        }
    }
}
```

#### Creating Custom Validation Rules
```rust
use sinex_config::{ConfigurationValidator, ValidationRule, RuleType, Severity};

let validator = ConfigurationValidator::new()
    .add_rule(ValidationRule {
        field_path: "custom_field".to_string(),
        rule_type: RuleType::Range { min: Some(1), max: Some(100) },
        parameters: HashMap::new(),
        error_message: "Custom field must be between 1 and 100".to_string(),
        severity: Severity::Error,
    });
```

#### Environment-Aware Validation
```rust
use sinex_config::{EnvironmentRule, ValidationRule, RuleType};

let env_rule = EnvironmentRule {
    rule_id: "production_requirements".to_string(),
    environment_pattern: "prod.*".to_string(),
    field_overrides: HashMap::new(),
    additional_rules: vec![
        ValidationRule {
            field_path: "log_level".to_string(),
            rule_type: RuleType::Enum {
                allowed_values: vec!["warn".to_string(), "error".to_string()],
            },
            parameters: HashMap::new(),
            error_message: "Production environments should use warn or error log levels".to_string(),
            severity: Severity::Warning,
        },
    ],
};
```

### Configuration Builder Pattern

#### Building Validated Configurations
```rust
use sinex_config::{ValidatedConfigBuilder, ValidationRule, RuleType, Severity};

let config = ValidatedConfigBuilder::new()
    .environment("development")
    .add_rule(ValidationRule {
        field_path: "service_name".to_string(),
        rule_type: RuleType::Required,
        parameters: HashMap::new(),
        error_message: "Service name is required".to_string(),
        severity: Severity::Error,
    })
    .set("service_name", "my-service")
    .set("log_level", "debug")
    .build()?;
```

## Best Practices

### 1. Configuration Design

- **Use sensible defaults**: Every optional field should have a reasonable default
- **Validate early**: Fail fast with clear error messages
- **Document everything**: Every configuration option should be documented
- **Consider environments**: Different environments may need different validation rules
- **Test combinations**: Test how configurations interact across components

### 2. Validation Rules

- **Be specific**: Error messages should clearly indicate what's wrong and how to fix it
- **Provide suggestions**: Include suggested fixes in validation messages
- **Use appropriate severity**: Not every validation issue needs to be an error
- **Test edge cases**: Validate boundary conditions and unusual inputs
- **Keep rules simple**: Complex validation logic should be in custom validators

### 3. Testing Strategy

- **Test all environments**: Development, staging, production, and edge cases
- **Test failure scenarios**: Invalid configurations should fail gracefully
- **Test performance impact**: Validation shouldn't significantly slow down startup
- **Test backwards compatibility**: Old configurations should still work
- **Test documentation**: Ensure documentation matches actual behavior

### 4. Error Handling

- **Clear messages**: Users should understand what went wrong
- **Actionable suggestions**: Tell users how to fix the problem
- **Context information**: Show which component and configuration section has the issue
- **Documentation links**: Link to relevant documentation when possible
- **Recovery guidance**: Suggest fallback options when appropriate

## Integration with Sinex Architecture

### Satellite SDK Integration

The validation framework integrates with the Satellite SDK:

```rust
use sinex_satellite_sdk::config::{SatelliteConfig, EventSourceConfig, AutomatonConfig};
use sinex_config::create_sinex_validator;

// Automatic validation during configuration loading
let config = SatelliteConfig::load_from_env("my-service");
// Validation is performed automatically in the load methods

// Manual validation for custom configurations
let validator = create_sinex_validator();
let custom_config = /* ... */;
let validation_result = validator.validate(&custom_config, Some("production"));
```

### Test Integration

The framework integrates with the existing test infrastructure:

```rust
use sinex_test_common::prelude::*;
use sinex_test_common::config_compatibility_tester::ConfigCompatibilityTester;

#[sinex_test]
async fn test_configuration_in_context(ctx: TestContext) -> TestResult {
    let tester = ConfigCompatibilityTester::new().await?;
    let scenario = tester.get_scenario("development_environment").unwrap();
    let result = tester.run_scenario(scenario).await?;
    
    assert!(result.overall_success, "Configuration test failed: {:?}", result.issues_found);
    Ok(())
}
```

## Extending the Framework

### Adding New Configuration Types

1. **Update Configuration Coverage**: Add new configuration schemas to `ConfigurationCoverage::build_coverage_analysis()`
2. **Add Validation Rules**: Create validation rules in the `create_sinex_validator()` function
3. **Create Test Scenarios**: Add new compatibility test scenarios to `ConfigCompatibilityMatrix`
4. **Document Usage**: Update this documentation with the new configuration options

### Adding Custom Validators

```rust
use sinex_config::{CustomValidator, ValidationValue, ValidationContext, ValidationResult};

#[derive(Debug)]
pub struct MyCustomValidator;

impl CustomValidator for MyCustomValidator {
    fn name(&self) -> &str {
        "my_custom_validator"
    }

    fn validate(&self, value: &ValidationValue, context: &ValidationContext) -> ValidationResult {
        // Custom validation logic here
        ValidationResult {
            valid: true,
            issues: Vec::new(),
        }
    }
}

// Use the custom validator
let validator = ConfigurationValidator::new()
    .add_custom_validator(Box::new(MyCustomValidator));
```

### Adding New Test Scenarios

```rust
// In ConfigCompatibilityTester::initialize_test_scenarios()
async fn add_my_custom_scenario(&mut self) -> Result<()> {
    let scenario = CompatibilityTestScenario {
        name: "my_custom_scenario".to_string(),
        description: "Test my specific configuration requirements".to_string(),
        components: vec![/* component configurations */],
        environment_setup: EnvironmentSetup {/* environment setup */},
        expected_outcome: ExpectedOutcome {/* expected results */},
        validation_steps: vec![/* validation steps */],
    };
    
    self.test_scenarios.push(scenario);
    Ok(())
}
```

## Future Enhancements

### Planned Features

1. **Configuration Schema Generation**: Automatically generate JSON schemas from configuration types
2. **Runtime Configuration Validation**: Validate configuration changes at runtime
3. **Configuration Migration Tools**: Automated migration between configuration versions
4. **Performance Benchmarking**: Measure and optimize validation performance
5. **Configuration Drift Detection**: Detect when running configurations differ from expected
6. **Integration Testing**: Deeper integration with actual service startup and operation
7. **Configuration Visualization**: Generate diagrams showing configuration relationships

### Potential Improvements

1. **Parallel Validation**: Run validation rules in parallel for large configurations
2. **Caching**: Cache validation results for frequently-used configurations
3. **Incremental Validation**: Only re-validate changed configuration sections
4. **Configuration Templates**: Provide templates for common configuration patterns
5. **Monitoring Integration**: Report configuration issues to monitoring systems

## Conclusion

The Sinex Configuration Testing and Validation Framework provides comprehensive coverage of configuration reliability, from individual field validation to complex cross-component compatibility testing. By following the patterns and best practices outlined in this document, developers can ensure that configuration changes are safe, well-tested, and properly documented.

The framework is designed to be extensible and maintainable, allowing for easy addition of new configuration types, validation rules, and test scenarios as the Sinex project continues to evolve.