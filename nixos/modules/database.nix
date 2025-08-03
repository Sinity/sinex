# Database configuration module
{ lib, config, pkgs, ... }:

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
        description = ''
          Maximum database connections per service.
          
          Calculation guidelines:
          - Base: 5-10 connections for low-traffic satellites
          - Scale: 20-30 for high-traffic services (ingestd, gateway)
          - Total PostgreSQL max_connections should be:
            (number_of_services * avg_maxConnections) + 50 overhead
          
          Example with 10 satellites:
          - 8 satellites @ 10 connections = 80
          - 2 high-traffic @ 30 connections = 60
          - Overhead for admin/monitoring = 50
          - Total max_connections = 190 (round to 200)
        '';
      };

      minConnections = mkOption {
        type = types.int;
        default = 5;
        description = ''
          Minimum database connections to maintain.
          
          Trade-offs:
          - Higher: Faster response for bursts, more idle resources
          - Lower: Better resource efficiency, slower burst response
          - Recommended: 25% of maxConnections
        '';
      };

      connectionTimeout = mkOption {
        type = types.int;
        default = 30;
        description = ''
          Connection timeout in seconds.
          
          When to adjust:
          - Increase (60s): High-latency networks, overloaded DB
          - Decrease (10s): Fast failure detection needed
          - Default (30s): Good for local/LAN PostgreSQL
        '';
      };

      idleTimeout = mkOption {
        type = types.int;
        default = 600;
        description = ''
          Idle connection timeout in seconds.
          
          Balancing act:
          - Shorter (300s): Aggressive resource reclaim, more reconnects
          - Longer (1800s): Fewer reconnects, more idle resources
          - Default (600s/10min): Good balance for most workloads
          
          Set to 0 to disable idle timeouts (not recommended).
        '';
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

    # Migration configuration
    migration = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable automatic database migrations";
      };

      package = mkOption {
        type = types.package;
        default = cfg.package; # Use the main sinex package which includes migration binary
        defaultText = literalExpression "cfg.package";
        description = "Package containing the migration binary (sinex-db-migration)";
      };

      binary = mkOption {
        type = types.str;
        default = "sinex-db-migration";
        description = "Name of the migration binary";
      };

      timeout = mkOption {
        type = types.int;
        default = 300;
        description = "Migration timeout in seconds";
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
        log_min_duration_statement = mkDefault "1000ms";
        
        # Connection limits
        max_connections = mkDefault cfg.database.connectionPool.maxConnections;
      };
    };
  };
}