# Database configuration module
{ lib, config, ... }:

with lib;

let
  cfg = config.services.sinex;
in
{
  options.services.sinex.database = {
    host = mkOption {
      type = types.str;
      default = "localhost";
      description = "PostgreSQL host";
    };

    port = mkOption {
      type = types.port;
      default = 5432;
      description = "PostgreSQL port";
    };

    name = mkOption {
      type = types.str;
      default = "sinex";
      description = "Database name";
    };

    user = mkOption {
      type = types.str;
      default = cfg.database.name;  # Derive from database name
      defaultText = literalExpression "cfg.database.name";
      description = "Database user (defaults to database name)";
    };

    passwordFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "Path to file containing database password";
    };

    autoSetup = mkOption {
      type = types.bool;
      default = true;
      description = "Automatically setup database user and permissions";
    };

    # Connection pool with sensible defaults
    connectionPool = {
      maxConnections = mkOption {
        type = types.int;
        default = 20;
        description = "Maximum database connections";
      };

      minConnections = mkOption {
        type = types.int;
        default = 5;
        description = "Minimum database connections";
      };

      connectionTimeout = mkOption {
        type = types.int;
        default = 30;
        description = "Connection timeout in seconds";
      };

      idleTimeout = mkOption {
        type = types.int;
        default = 600;
        description = "Idle connection timeout in seconds";
      };
    };

    # Health check configuration
    healthCheck = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable database health checks";
      };

      interval = mkOption {
        type = types.int;
        default = 30;
        description = "Health check interval in seconds";
      };

      timeout = mkOption {
        type = types.int;
        default = 5;
        description = "Health check timeout in seconds";
      };
    };
  };

  config = mkIf cfg.enable {
    # Auto-apply sensible database performance defaults when autoSetup is enabled
    services.postgresql = mkIf cfg.database.autoSetup {
      settings = {
        # Connection and timeout settings
        statement_timeout = mkDefault "60s";
        lock_timeout = mkDefault "30s"; 
        idle_in_transaction_session_timeout = mkDefault "300s";
        
        # Performance settings
        shared_buffers = mkDefault "256MB";
        effective_cache_size = mkDefault "1GB";
        maintenance_work_mem = mkDefault "256MB";
        checkpoint_completion_target = mkDefault "0.9";
        
        # Prepared statements
        max_prepared_transactions = mkDefault 256;
        
        # Logging for monitoring
        log_statement = mkDefault "mod";
        log_duration = mkDefault true;
        log_slow_statements = mkDefault true;
        log_min_duration_statement = mkDefault "1000ms";
        
        # Connection limits
        max_connections = mkDefault cfg.database.connectionPool.maxConnections;
      };
    };
  };
}