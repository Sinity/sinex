use tracing::{info, Level};
use tracing_subscriber::{
    fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
};

use crate::config::LoggingConfig;
use crate::error::Result;

/// Initialize the logging system based on configuration
#[allow(dead_code)]
pub fn init_logging(config: &LoggingConfig) -> Result<()> {
    let _level = parse_log_level(&config.level)?;
    
    // Create the env filter
    let env_filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(&config.level))
        .unwrap_or_else(|_| EnvFilter::new("info"));

    // Create the formatter based on the format setting
    let registry = Registry::default().with(env_filter);

    match config.format.as_str() {
        "json" => {
            let fmt_layer = fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(true)
                .with_target(true)
                .with_thread_ids(true)
                .with_thread_names(true);

            registry.with(fmt_layer).init();
        }
        "pretty" | _ => {
            let fmt_layer = fmt::layer()
                .pretty()
                .with_target(true)
                .with_thread_ids(false)
                .with_thread_names(false);

            registry.with(fmt_layer).init();
        }
    }

    info!(
        "Logging initialized with level '{}' and format '{}'",
        config.level, config.format
    );

    Ok(())
}

/// Parse log level string into tracing Level
#[allow(dead_code)]
fn parse_log_level(level: &str) -> Result<Level> {
    match level.to_lowercase().as_str() {
        "trace" => Ok(Level::TRACE),
        "debug" => Ok(Level::DEBUG),
        "info" => Ok(Level::INFO),
        "warn" | "warning" => Ok(Level::WARN),
        "error" => Ok(Level::ERROR),
        _ => {
            eprintln!(
                "Invalid log level '{}'. Using 'info' as default. Valid levels: trace, debug, info, warn, error",
                level
            );
            Ok(Level::INFO)
        }
    }
}

/// Create a structured log context for the application startup
#[allow(dead_code)]
pub fn log_startup_info(config: &crate::config::Config) {
    info!(
        app_name = config.app.name,
        app_version = config.app.version,
        database_url = mask_url(&config.database.url),
        log_level = config.logging.level,
        max_connections = config.database.max_connections,
        "Application starting"
    );
}

/// Create a structured log context for shutdown
#[allow(dead_code)]
pub fn log_shutdown_info(reason: &str) {
    info!(
        reason = reason,
        "Application shutting down"
    );
}

/// Mask sensitive information in URLs for logging
#[allow(dead_code)]
fn mask_url(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        let mut masked = parsed.clone();
        if masked.password().is_some() {
            let _ = masked.set_password(Some("***"));
        }
        masked.to_string()
    } else {
        url.to_string()
    }
}

/// Log an error with context
#[allow(dead_code)]
pub fn log_error_with_context(error: &dyn std::error::Error, context: &str) {
    let mut source_chain = Vec::new();
    let mut current_error: &dyn std::error::Error = error;
    
    while let Some(source) = current_error.source() {
        source_chain.push(source.to_string());
        current_error = source;
    }

    tracing::error!(
        error = %error,
        context = context,
        error_chain = ?source_chain,
        "Error occurred"
    );
}

/// Macro for logging function entry/exit in debug mode
#[macro_export]
macro_rules! trace_fn {
    ($fn_name:expr) => {
        tracing::debug!("Entering function: {}", $fn_name);
    };
    ($fn_name:expr, $($field:tt)*) => {
        tracing::debug!($($field)*, "Entering function: {}", $fn_name);
    };
}

/// Macro for logging function exit in debug mode
#[macro_export]
macro_rules! trace_fn_exit {
    ($fn_name:expr) => {
        tracing::debug!("Exiting function: {}", $fn_name);
    };
    ($fn_name:expr, $($field:tt)*) => {
        tracing::debug!($($field)*, "Exiting function: {}", $fn_name);
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LoggingConfig;

    #[test]
    fn test_parse_log_level() {
        assert!(matches!(parse_log_level("debug").unwrap(), Level::DEBUG));
        assert!(matches!(parse_log_level("INFO").unwrap(), Level::INFO));
        assert!(matches!(parse_log_level("warn").unwrap(), Level::WARN));
        assert!(matches!(parse_log_level("error").unwrap(), Level::ERROR));
        assert!(matches!(parse_log_level("trace").unwrap(), Level::TRACE));
    }

    #[test]
    fn test_mask_url() {
        assert_eq!(
            mask_url("postgresql://user:password@localhost/db"),
            "postgresql://user:***@localhost/db"
        );
        assert_eq!(
            mask_url("postgresql://localhost/db"),
            "postgresql://localhost/db"
        );
    }

    #[test]
    fn test_logging_config_defaults() {
        let config = LoggingConfig::default();
        assert_eq!(config.level, "info");
        assert_eq!(config.format, "pretty");
        assert!(!config.include_location);
    }
}