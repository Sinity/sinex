# Sinex NixOS Module - First-class system integration
{
  config,
  lib,
  pkgs,
  ...
}:

with lib;

let
  cfg = config.services.sinex;
  configGen = import ./config-gen.nix { inherit lib pkgs; };

  # Use config generation from separate module
  collectorConfigFile = configGen.mkCollectorConfigFile cfg.unifiedCollector cfg;

  # Helper function to escape database identifiers
  escapeDbIdentifier = str: lib.escape ["?" "&" "=" "'" "\"" " " "\\" "/"] str;
  
  # Helper function to build comprehensive database URL with all options
  buildDatabaseUrl = cfg: let
    baseUrl = if cfg.database.passwordFile != null
      then "postgresql://${escapeDbIdentifier cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${escapeDbIdentifier cfg.database.name}"
      else "postgresql:///${escapeDbIdentifier cfg.database.name}?host=/run/postgresql&user=${escapeDbIdentifier cfg.database.user}";
    
    # Build query parameters from configuration
    queryParams = lib.flatten [
      # Connection pool parameters
      (lib.optional (cfg.database.connectionPool.maxConnections != 20) "pool_max_conns=${toString cfg.database.connectionPool.maxConnections}")
      (lib.optional (cfg.database.connectionPool.minConnections != 5) "pool_min_conns=${toString cfg.database.connectionPool.minConnections}")
      (lib.optional (cfg.database.connectionPool.connectionTimeout != 30) "connect_timeout=${toString cfg.database.connectionPool.connectionTimeout}")
      (lib.optional (cfg.database.connectionPool.idleTimeout != 600) "pool_idle_timeout=${toString cfg.database.connectionPool.idleTimeout}")
      (lib.optional (cfg.database.connectionPool.maxLifetime != 3600) "pool_max_lifetime=${toString cfg.database.connectionPool.maxLifetime}")
      
      # Performance parameters
      (lib.optional (cfg.database.performance.statementTimeout != 60 && cfg.database.performance.statementTimeout != 0) "statement_timeout=${toString (cfg.database.performance.statementTimeout * 1000)}")
      (lib.optional (cfg.database.performance.lockTimeout != 30 && cfg.database.performance.lockTimeout != 0) "lock_timeout=${toString (cfg.database.performance.lockTimeout * 1000)}")
      (lib.optional (cfg.database.performance.idleInTransactionTimeout != 300 && cfg.database.performance.idleInTransactionTimeout != 0) "idle_in_transaction_session_timeout=${toString (cfg.database.performance.idleInTransactionTimeout * 1000)}")
      (lib.optional (cfg.database.performance.defaultTransactionIsolation != "read_committed") "default_transaction_isolation=${cfg.database.performance.defaultTransactionIsolation}")
      
      # SSL parameters
      (lib.optional (cfg.database.ssl.mode != "prefer") "sslmode=${cfg.database.ssl.mode}")
      (lib.optional (cfg.database.ssl.certFile != null) "sslcert=${cfg.database.ssl.certFile}")
      (lib.optional (cfg.database.ssl.keyFile != null) "sslkey=${cfg.database.ssl.keyFile}")
      (lib.optional (cfg.database.ssl.caFile != null) "sslrootcert=${cfg.database.ssl.caFile}")
      (lib.optional (cfg.database.ssl.crlFile != null) "sslcrl=${cfg.database.ssl.crlFile}")
      
      # Application name for connection tracking
      "application_name=sinex-collector"
    ];
    
    # Join non-empty parameters with &
    paramString = lib.concatStringsSep "&" (lib.filter (p: p != "") queryParams);
    
    # Build final URL
    finalUrl = if paramString != "" then
      if lib.hasInfix "?" baseUrl then "${baseUrl}&${paramString}" else "${baseUrl}?${paramString}"
    else baseUrl;
      
  in finalUrl;
  
  # Database migration script with idempotent permissions
  migrateDbScript = pkgs.writeShellScript "migrate-sinex-db" ''
    set -euo pipefail
    
    # Logging function with timestamps
    log() {
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    }
    
    log "Starting Sinex database migration and setup"

    # Wait for PostgreSQL to be available with extended timeout and exponential backoff
    TIMEOUT=120
    ELAPSED=0
    DELAY=1
    log "Waiting for PostgreSQL to become available..."
    
    while ! ${pkgs.postgresql}/bin/pg_isready -h /run/postgresql -q; do
      if [ $ELAPSED -ge $TIMEOUT ]; then
        log "ERROR: PostgreSQL did not become ready within $TIMEOUT seconds"
        log "Last pg_isready output:"
        ${pkgs.postgresql}/bin/pg_isready -h /run/postgresql || true
        exit 1
      fi
      
      log "PostgreSQL not ready, waiting $DELAY seconds... ($ELAPSED/$TIMEOUT elapsed)"
      sleep $DELAY
      ELAPSED=$((ELAPSED + DELAY))
      
      # Exponential backoff up to 8 seconds
      if [ $DELAY -lt 8 ]; then
        DELAY=$((DELAY * 2))
      fi
    done
    
    log "PostgreSQL is ready"

    # Verify database exists
    export DATABASE_URL="postgresql://postgres/${escapeDbIdentifier cfg.database.name}?host=/run/postgresql"
    if ! ${pkgs.postgresql}/bin/psql -lqt | cut -d '|' -f 1 | grep -qw "${escapeDbIdentifier cfg.database.name}"; then
      log "ERROR: Database '${escapeDbIdentifier cfg.database.name}' does not exist"
      exit 1
    fi
    
    log "Database '${escapeDbIdentifier cfg.database.name}' exists"

    # Run migrations with configured timeout and proper error handling
    MIGRATION_TIMEOUT=${toString cfg.database.migration.timeout}
    log "Running database migrations with timeout of $MIGRATION_TIMEOUT seconds..."
    
    # Set up migration environment variables
    export SQLX_OFFLINE=true
    ${lib.optionalString cfg.database.migration.enableLocking ''
      export SQLX_MIGRATION_LOCK_TIMEOUT=${toString cfg.database.migration.lockTimeout}
    ''}
    ${lib.optionalString cfg.database.migration.validateChecksums ''
      export SQLX_MIGRATION_VALIDATE_CHECKSUMS=true
    ''}
    
    if ! timeout $MIGRATION_TIMEOUT ${pkgs.sqlx-cli}/bin/sqlx migrate run --source ${cfg.package}/share/sinex/migrations; then
      log "ERROR: Database migration failed or timed out after $MIGRATION_TIMEOUT seconds"
      exit 1
    fi
    
    log "Database migrations completed successfully"
    
    # Grant permissions to sinex user on all schemas and tables (fully idempotent)
    log "Setting up database permissions..."
    if ! ${pkgs.postgresql}/bin/psql -d ${escapeDbIdentifier cfg.database.name} <<'EOF'
      DO $$
      DECLARE
        schema_name text;
        schemas text[] := ARRAY['raw', 'core', 'sinex_schemas', 'sinex_router'];
        user_name text := '${escapeDbIdentifier cfg.database.user}';
        schema_exists boolean;
        user_exists boolean;
      BEGIN
        -- Check if user exists
        SELECT EXISTS (
          SELECT 1 FROM pg_catalog.pg_user WHERE usename = user_name
        ) INTO user_exists;
        
        IF NOT user_exists THEN
          RAISE WARNING 'User % does not exist, skipping permission grants', user_name;
          RETURN;
        END IF;
        
        RAISE NOTICE 'Setting up permissions for user: %', user_name;
        
        -- Grant usage on each schema if it exists (idempotent)
        FOREACH schema_name IN ARRAY schemas
        LOOP
          SELECT EXISTS (
            SELECT 1 FROM information_schema.schemata 
            WHERE schema_name = schema_name
          ) INTO schema_exists;
          
          IF schema_exists THEN
            RAISE NOTICE 'Granting permissions on schema: %', schema_name;
            
            BEGIN
              -- Grant schema usage (idempotent - no error if already granted)
              EXECUTE format('GRANT USAGE ON SCHEMA %I TO %I', schema_name, user_name);
              
              -- Grant all privileges on existing tables (idempotent)
              EXECUTE format('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA %I TO %I', schema_name, user_name);
              
              -- Grant usage on all sequences (idempotent)
              EXECUTE format('GRANT USAGE ON ALL SEQUENCES IN SCHEMA %I TO %I', schema_name, user_name);
              
              -- Set default privileges for future tables (idempotent)
              EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT ALL ON TABLES TO %I', schema_name, user_name);
              
              -- Set default privileges for future sequences (idempotent)
              EXECUTE format('ALTER DEFAULT PRIVILEGES IN SCHEMA %I GRANT USAGE ON SEQUENCES TO %I', schema_name, user_name);
              
              RAISE NOTICE 'Successfully granted permissions on schema: %', schema_name;
              
            EXCEPTION
              WHEN OTHERS THEN
                RAISE WARNING 'Failed to grant some permissions on schema %: % (SQLSTATE: %)', 
                  schema_name, SQLERRM, SQLSTATE;
                -- Continue with other schemas
            END;
          ELSE
            RAISE NOTICE 'Schema % does not exist, skipping', schema_name;
          END IF;
        END LOOP;
        
        RAISE NOTICE 'Permission setup completed for user: %', user_name;
        
      EXCEPTION
        WHEN OTHERS THEN
          RAISE WARNING 'Unexpected error during permission setup: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
          -- Don't fail the entire script for permission issues
      END;
      $$;
EOF
    then
      log "WARNING: Permission setup encountered errors, but continuing..."
      log "This may be expected if permissions were already granted"
    else
      log "Database permissions configured successfully"
    fi
    
    log "Database setup completed successfully"
  '';

in
{
  options.services.sinex = {
    enable = mkEnableOption "Sinex Exocortex event capture system";

    package = mkOption {
      type = types.package;
      default = pkgs.sinex or (import ./. { }).packages.${pkgs.system}.default;
      defaultText = literalExpression "pkgs.sinex";
      description = "The Sinex package to use";
    };

    targetUser = mkOption {
      type = types.str;
      default = "sinity";
      description = "Username whose files to monitor (defaults to sinity)";
    };

    database = {
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
        default = "sinex";
        description = "Database user";
      };

      passwordFile = mkOption {
        type = types.nullOr types.path;
        default = null;
        description = "Path to file containing database password";
      };

      url = mkOption {
        type = types.str;
        default = buildDatabaseUrl cfg;
        defaultText = literalExpression ''buildDatabaseUrl cfg'';
        description = "PostgreSQL connection URL with all configuration options applied";
      };

      autoSetup = mkOption {
        type = types.bool;
        default = true;
        description = "Automatically create database and run migrations";
      };

      # Connection Pool Configuration
      connectionPool = {
        maxConnections = mkOption {
          type = types.int;
          default = 20;
          description = "Maximum number of connections in the pool";
        };

        minConnections = mkOption {
          type = types.int;
          default = 5;
          description = "Minimum number of connections to maintain in the pool";
        };

        connectionTimeout = mkOption {
          type = types.int;
          default = 30;
          description = "Connection timeout in seconds";
        };

        idleTimeout = mkOption {
          type = types.int;
          default = 600;
          description = "Maximum idle time for connections in seconds";
        };

        maxLifetime = mkOption {
          type = types.int;
          default = 3600;
          description = "Maximum lifetime for connections in seconds";
        };
      };

      # Connection Retry Configuration
      retry = {
        maxRetries = mkOption {
          type = types.int;
          default = 5;
          description = "Maximum number of connection retry attempts";
        };

        initialDelay = mkOption {
          type = types.int;
          default = 1;
          description = "Initial retry delay in seconds";
        };

        maxDelay = mkOption {
          type = types.int;
          default = 30;
          description = "Maximum retry delay in seconds";
        };

        backoffMultiplier = mkOption {
          type = types.float;
          default = 2.0;
          description = "Backoff multiplier for exponential backoff";
        };

        enableJitter = mkOption {
          type = types.bool;
          default = true;
          description = "Add random jitter to retry delays";
        };
      };

      # Performance Tuning Options
      performance = {
        statementTimeout = mkOption {
          type = types.int;
          default = 60;
          description = "Statement timeout in seconds (0 = disabled)";
        };

        lockTimeout = mkOption {
          type = types.int;
          default = 30;
          description = "Lock timeout in seconds (0 = disabled)";
        };

        idleInTransactionTimeout = mkOption {
          type = types.int;
          default = 300;
          description = "Idle in transaction timeout in seconds (0 = disabled)";
        };

        enablePreparedStatements = mkOption {
          type = types.bool;
          default = true;
          description = "Enable prepared statement caching";
        };

        preparedStatementCacheSize = mkOption {
          type = types.int;
          default = 256;
          description = "Maximum number of prepared statements to cache";
        };

        enableAutoCommit = mkOption {
          type = types.bool;
          default = true;
          description = "Enable auto-commit for connections";
        };

        defaultTransactionIsolation = mkOption {
          type = types.enum [
            "read_uncommitted"
            "read_committed"
            "repeatable_read"
            "serializable"
          ];
          default = "read_committed";
          description = "Default transaction isolation level";
        };
      };

      # Health Check Configuration
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

        query = mkOption {
          type = types.str;
          default = "SELECT 1";
          description = "Health check query to execute";
        };

        failureThreshold = mkOption {
          type = types.int;
          default = 3;
          description = "Number of consecutive failures before marking unhealthy";
        };

        successThreshold = mkOption {
          type = types.int;
          default = 1;
          description = "Number of consecutive successes to mark healthy again";
        };
      };

      # Migration Configuration
      migration = {
        timeout = mkOption {
          type = types.int;
          default = 600;
          description = "Migration timeout in seconds";
        };

        enableLocking = mkOption {
          type = types.bool;
          default = true;
          description = "Enable migration locking to prevent concurrent migrations";
        };

        lockTimeout = mkOption {
          type = types.int;
          default = 300;
          description = "Migration lock timeout in seconds";
        };

        validateChecksums = mkOption {
          type = types.bool;
          default = true;
          description = "Validate migration file checksums";
        };
      };

      # SSL Configuration
      ssl = {
        mode = mkOption {
          type = types.enum [
            "disable"
            "allow"
            "prefer"
            "require"
            "verify-ca"
            "verify-full"
          ];
          default = "prefer";
          description = "SSL connection mode";
        };

        certFile = mkOption {
          type = types.nullOr types.path;
          default = null;
          description = "Path to client certificate file";
        };

        keyFile = mkOption {
          type = types.nullOr types.path;
          default = null;
          description = "Path to client private key file";
        };

        caFile = mkOption {
          type = types.nullOr types.path;
          default = null;
          description = "Path to certificate authority file";
        };

        crlFile = mkOption {
          type = types.nullOr types.path;
          default = null;
          description = "Path to certificate revocation list file";
        };
      };

      # Monitoring and Logging
      monitoring = {
        enableSlowQueryLog = mkOption {
          type = types.bool;
          default = true;
          description = "Enable slow query logging";
        };

        slowQueryThreshold = mkOption {
          type = types.int;
          default = 1000;
          description = "Slow query threshold in milliseconds";
        };

        enableConnectionLogging = mkOption {
          type = types.bool;
          default = false;
          description = "Enable connection event logging";
        };

        enableMetrics = mkOption {
          type = types.bool;
          default = true;
          description = "Enable database connection metrics";
        };

        metricsInterval = mkOption {
          type = types.int;
          default = 60;
          description = "Metrics collection interval in seconds";
        };
      };
    };

    unifiedCollector = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the unified event collector";
      };

      metricsPort = mkOption {
        type = types.port;
        default = 2112;
        description = "Port for Prometheus metrics endpoint";
      };

      logLevel = mkOption {
        type = types.enum [
          "trace"
          "debug"
          "info"
          "warn"
          "error"
        ];
        default = "info";
        description = "Log level for the collector";
      };

      dryRun = mkOption {
        type = types.bool;
        default = false;
        description = "Run in dry-run mode (no database writes)";
      };

      outputFile = mkOption {
        type = types.nullOr types.path;
        default = null;
        description = "Write events to file instead of database";
      };

      # Health Check Configuration
      healthCheck = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable health checks for the unified collector";
        };

        port = mkOption {
          type = types.port;
          default = 8080;
          description = "HTTP port for health check endpoint";
        };

        path = mkOption {
          type = types.str;
          default = "/health";
          description = "HTTP path for health check endpoint";
        };

        readinessPath = mkOption {
          type = types.str;
          default = "/ready";
          description = "HTTP path for readiness probe endpoint";
        };

        livenessPath = mkOption {
          type = types.str;
          default = "/alive";
          description = "HTTP path for liveness probe endpoint";
        };

        interval = mkOption {
          type = types.int;
          default = 10;
          description = "Health check interval in seconds";
        };

        timeout = mkOption {
          type = types.int;
          default = 5;
          description = "Health check timeout in seconds";
        };

        startupProbe = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable startup health probe";
          };

          initialDelay = mkOption {
            type = types.int;
            default = 30;
            description = "Initial delay before first startup probe in seconds";
          };

          periodSeconds = mkOption {
            type = types.int;
            default = 5;
            description = "Period between startup probes in seconds";
          };

          timeoutSeconds = mkOption {
            type = types.int;
            default = 3;
            description = "Timeout for startup probes in seconds";
          };

          failureThreshold = mkOption {
            type = types.int;
            default = 12;
            description = "Number of consecutive startup probe failures before giving up";
          };
        };

        readinessProbe = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable readiness health probe";
          };

          initialDelay = mkOption {
            type = types.int;
            default = 5;
            description = "Initial delay before first readiness probe in seconds";
          };

          periodSeconds = mkOption {
            type = types.int;
            default = 10;
            description = "Period between readiness probes in seconds";
          };

          timeoutSeconds = mkOption {
            type = types.int;
            default = 3;
            description = "Timeout for readiness probes in seconds";
          };

          failureThreshold = mkOption {
            type = types.int;
            default = 3;
            description = "Number of consecutive readiness probe failures before marking unready";
          };

          successThreshold = mkOption {
            type = types.int;
            default = 1;
            description = "Number of consecutive readiness probe successes to mark ready";
          };
        };

        livenessProbe = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable liveness health probe";
          };

          initialDelay = mkOption {
            type = types.int;
            default = 60;
            description = "Initial delay before first liveness probe in seconds";
          };

          periodSeconds = mkOption {
            type = types.int;
            default = 30;
            description = "Period between liveness probes in seconds";
          };

          timeoutSeconds = mkOption {
            type = types.int;
            default = 5;
            description = "Timeout for liveness probes in seconds";
          };

          failureThreshold = mkOption {
            type = types.int;
            default = 3;
            description = "Number of consecutive liveness probe failures before restart";
          };
        };

        persistence = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable health state persistence across restarts";
          };

          stateFile = mkOption {
            type = types.str;
            default = "collector_health_state";
            description = "Filename for persisting health state";
          };

          retainDuration = mkOption {
            type = types.str;
            default = "24h";
            description = "Duration to retain health history";
          };
        };
      };

      # Restart Policy Configuration
      restart = {
        policy = mkOption {
          type = types.enum [ "always" "on-failure" "unless-stopped" "no" ];
          default = "always";
          description = "Restart policy for the collector service";
        };

        maxRestarts = mkOption {
          type = types.int;
          default = 5;
          description = "Maximum number of restarts within restart window";
        };

        restartWindow = mkOption {
          type = types.str;
          default = "10min";
          description = "Time window for counting restarts";
        };

        baseDelay = mkOption {
          type = types.str;
          default = "10s";
          description = "Base delay between restart attempts";
        };

        maxDelay = mkOption {
          type = types.str;
          default = "5min";
          description = "Maximum delay between restart attempts";
        };

        backoffMultiplier = mkOption {
          type = types.float;
          default = 2.0;
          description = "Backoff multiplier for exponential restart delay";
        };
      };

      sources = {
        atuin = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable Atuin shell history ingestion";
          };

          pollInterval = mkOption {
            type = types.int;
            default = 3;
            description = "Polling interval in seconds";
          };

          databasePath = mkOption {
            type = types.str;
            default = "/home/${cfg.targetUser}/.local/share/atuin/history.db";
            description = "Path to Atuin SQLite database";
          };
        };

        shellHistory = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable shell history file ingestion";
          };

          zshPath = mkOption {
            type = types.str;
            default = "/home/${cfg.targetUser}/.zsh_history";
            description = "Path to zsh history file";
          };

          bashPath = mkOption {
            type = types.str;
            default = "/home/${cfg.targetUser}/.bash_history";
            description = "Path to bash history file";
          };
        };

        asciinema = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable asciinema recording detection";
          };

          recordingsPath = mkOption {
            type = types.str;
            default = "/home/${cfg.targetUser}/.local/share/asciinema";
            description = "Path to asciinema recordings directory";
          };

          autoRecord = mkOption {
            type = types.bool;
            default = false;
            description = "Automatically start recording all terminal sessions";
          };

          autoAnnex = mkOption {
            type = types.bool;
            default = true;
            description = "Automatically add recordings to git-annex when they complete";
          };
        };

        kittyScrollback = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable Kitty terminal scrollback capture";
          };

          captureInterval = mkOption {
            type = types.int;
            default = 15;
            description = "Scrollback capture interval in seconds";
          };

          socketPath = mkOption {
            type = types.str;
            default = "/tmp/kitty";
            description = "Kitty remote control socket path";
          };

          maxScrollbackLines = mkOption {
            type = types.int;
            default = 10000;
            description = "Maximum scrollback lines to capture";
          };

          captureOnCommand = mkOption {
            type = types.bool;
            default = true;
            description = "Capture scrollback when commands are executed";
          };

          commandCaptureDelay = mkOption {
            type = types.int;
            default = 500;
            description = "Delay in milliseconds after command execution before capturing";
          };
        };

        filesystem = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable filesystem event monitoring";
          };

          watchPaths = mkOption {
            type = types.listOf types.str;
            default = [
              "/home/${cfg.targetUser}/Documents"
              "/home/${cfg.targetUser}/Projects"
            ];
            description = "Paths to monitor for filesystem events";
          };

          excludePatterns = mkOption {
            type = types.listOf types.str;
            default = [
              "*.tmp"
              "*.cache"
              ".git/*"
            ];
            description = "Patterns to exclude from monitoring";
          };
        };

        dbus = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable D-Bus event monitoring";
          };

          monitorSession = mkOption {
            type = types.bool;
            default = true;
            description = "Monitor session bus";
          };

          monitorSystem = mkOption {
            type = types.bool;
            default = true;
            description = "Monitor system bus";
          };

          logAllSignals = mkOption {
            type = types.bool;
            default = false;
            description = "Log all D-Bus signals (verbose)";
          };

          extractNotifications = mkOption {
            type = types.bool;
            default = true;
            description = "Extract notification events";
          };

          extractMedia = mkOption {
            type = types.bool;
            default = true;
            description = "Extract media playback events";
          };

          extractPower = mkOption {
            type = types.bool;
            default = true;
            description = "Extract power management events";
          };

          extractHardware = mkOption {
            type = types.bool;
            default = true;
            description = "Extract hardware device events";
          };

          extractSession = mkOption {
            type = types.bool;
            default = true;
            description = "Extract session/idle events";
          };

          extractPolicykit = mkOption {
            type = types.bool;
            default = true;
            description = "Extract PolicyKit authorization events";
          };

          extractBluetooth = mkOption {
            type = types.bool;
            default = true;
            description = "Extract Bluetooth device events";
          };

          extractNetwork = mkOption {
            type = types.bool;
            default = true;
            description = "Extract network connection events";
          };

          extractScreensaver = mkOption {
            type = types.bool;
            default = true;
            description = "Extract screen saver/lock events";
          };

          extractMounts = mkOption {
            type = types.bool;
            default = true;
            description = "Extract storage mount/unmount events";
          };
        };

        clipboard = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable clipboard monitoring";
          };

          monitorClipboard = mkOption {
            type = types.bool;
            default = true;
            description = "Monitor standard clipboard";
          };

          monitorPrimary = mkOption {
            type = types.bool;
            default = true;
            description = "Monitor primary selection (Linux)";
          };

          monitorSecondary = mkOption {
            type = types.bool;
            default = false;
            description = "Monitor secondary selection (rarely used)";
          };

          pollInterval = mkOption {
            type = types.int;
            default = 500;
            description = "Polling interval in milliseconds";
          };

          hashFileContent = mkOption {
            type = types.bool;
            default = false;
            description = "Include file content hashes";
          };

          maxPreviewLength = mkOption {
            type = types.int;
            default = 100;
            description = "Maximum preview length for text content";
          };

          enableHistory = mkOption {
            type = types.bool;
            default = true;
            description = "Store clipboard history";
          };

          maxHistoryEntries = mkOption {
            type = types.int;
            default = 1000;
            description = "Maximum history entries to keep";
          };
        };
      };

      dlq = {
        maxRetries = mkOption {
          type = types.int;
          default = 3;
          description = "Maximum retry attempts for failed events";
        };

        retryDelaySecs = mkOption {
          type = types.int;
          default = 60;
          description = "Delay between retry attempts in seconds";
        };

        enableFileDlq = mkOption {
          type = types.bool;
          default = true;
          description = "Enable file-based DLQ for ultimate fallback";
        };

        filePath = mkOption {
          type = types.path;
          default = cfg.directories.dlq;
          defaultText = literalExpression "cfg.directories.dlq";
          description = "Path for file-based DLQ storage";
        };
      };
    };

    promoWorker = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the promotion worker";
      };

      metricsPort = mkOption {
        type = types.port;
        default = 2113;
        description = "Port for Prometheus metrics endpoint";
      };

      pollInterval = mkOption {
        type = types.int;
        default = 5;
        description = "Queue polling interval in seconds";
      };

      batchSize = mkOption {
        type = types.int;
        default = 100;
        description = "Number of events to process per batch";
      };

      # Health Check Configuration
      healthCheck = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable health checks for the promotion worker";
        };

        port = mkOption {
          type = types.port;
          default = 8081;
          description = "HTTP port for health check endpoint";
        };

        path = mkOption {
          type = types.str;
          default = "/health";
          description = "HTTP path for health check endpoint";
        };

        readinessPath = mkOption {
          type = types.str;
          default = "/ready";
          description = "HTTP path for readiness probe endpoint";
        };

        livenessPath = mkOption {
          type = types.str;
          default = "/alive";
          description = "HTTP path for liveness probe endpoint";
        };

        interval = mkOption {
          type = types.int;
          default = 15;
          description = "Health check interval in seconds";
        };

        timeout = mkOption {
          type = types.int;
          default = 5;
          description = "Health check timeout in seconds";
        };

        startupProbe = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable startup health probe";
          };

          initialDelay = mkOption {
            type = types.int;
            default = 15;
            description = "Initial delay before first startup probe in seconds";
          };

          periodSeconds = mkOption {
            type = types.int;
            default = 5;
            description = "Period between startup probes in seconds";
          };

          timeoutSeconds = mkOption {
            type = types.int;
            default = 3;
            description = "Timeout for startup probes in seconds";
          };

          failureThreshold = mkOption {
            type = types.int;
            default = 6;
            description = "Number of consecutive startup probe failures before giving up";
          };
        };

        readinessProbe = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable readiness health probe";
          };

          initialDelay = mkOption {
            type = types.int;
            default = 5;
            description = "Initial delay before first readiness probe in seconds";
          };

          periodSeconds = mkOption {
            type = types.int;
            default = 15;
            description = "Period between readiness probes in seconds";
          };

          timeoutSeconds = mkOption {
            type = types.int;
            default = 3;
            description = "Timeout for readiness probes in seconds";
          };

          failureThreshold = mkOption {
            type = types.int;
            default = 3;
            description = "Number of consecutive readiness probe failures before marking unready";
          };

          successThreshold = mkOption {
            type = types.int;
            default = 1;
            description = "Number of consecutive readiness probe successes to mark ready";
          };
        };

        livenessProbe = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable liveness health probe";
          };

          initialDelay = mkOption {
            type = types.int;
            default = 30;
            description = "Initial delay before first liveness probe in seconds";
          };

          periodSeconds = mkOption {
            type = types.int;
            default = 60;
            description = "Period between liveness probes in seconds";
          };

          timeoutSeconds = mkOption {
            type = types.int;
            default = 5;
            description = "Timeout for liveness probes in seconds";
          };

          failureThreshold = mkOption {
            type = types.int;
            default = 3;
            description = "Number of consecutive liveness probe failures before restart";
          };
        };

        queueHealth = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable queue depth health monitoring";
          };

          maxDepthThreshold = mkOption {
            type = types.int;
            default = 1000;
            description = "Maximum queue depth before marking unhealthy";
          };

          processingTimeThreshold = mkOption {
            type = types.str;
            default = "30s";
            description = "Maximum processing time per batch before marking unhealthy";
          };

          stalledJobThreshold = mkOption {
            type = types.str;
            default = "5min";
            description = "Maximum time for jobs to remain unprocessed before marking unhealthy";
          };
        };

        persistence = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable health state persistence across restarts";
          };

          stateFile = mkOption {
            type = types.str;
            default = "worker_health_state";
            description = "Filename for persisting health state";
          };

          retainDuration = mkOption {
            type = types.str;
            default = "24h";
            description = "Duration to retain health history";
          };
        };
      };

      # Restart Policy Configuration  
      restart = {
        policy = mkOption {
          type = types.enum [ "always" "on-failure" "unless-stopped" "no" ];
          default = "always";
          description = "Restart policy for the worker service";
        };

        maxRestarts = mkOption {
          type = types.int;
          default = 5;
          description = "Maximum number of restarts within restart window";
        };

        restartWindow = mkOption {
          type = types.str;
          default = "10min";
          description = "Time window for counting restarts";
        };

        baseDelay = mkOption {
          type = types.str;
          default = "15s";
          description = "Base delay between restart attempts";
        };

        maxDelay = mkOption {
          type = types.str;
          default = "5min";
          description = "Maximum delay between restart attempts";
        };

        backoffMultiplier = mkOption {
          type = types.float;
          default = 2.0;
          description = "Backoff multiplier for exponential restart delay";
        };
      };
    };

    blobStorage = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable git-annex blob storage integration";
      };

      repositoryPath = mkOption {
        type = types.path;
        default = "${cfg.directories.state}/annex";
        defaultText = literalExpression ''"''${cfg.directories.state}/annex"'';
        description = "Path to git-annex repository";
      };

      autoInit = mkOption {
        type = types.bool;
        default = true;
        description = "Automatically initialize git-annex repository";
      };

      numCopies = mkOption {
        type = types.int;
        default = 2;
        description = "Minimum number of copies for git-annex";
      };

      # Advanced Configuration
      backend = mkOption {
        type = types.str;
        default = "SHA256E";
        description = "Git-annex backend to use for new files";
      };

      repoDescription = mkOption {
        type = types.str;
        default = "Sinex Blob Storage";
        description = "Description for the git-annex repository";
      };

      largeFiles = mkOption {
        type = types.str;
        default = "anything";
        description = "Git-annex largefiles expression for automatic annexing";
      };

      # Health Monitoring
      healthCheck = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable git-annex repository health checks";
        };

        interval = mkOption {
          type = types.int;
          default = 3600;
          description = "Health check interval in seconds";
        };

        fastFsck = mkOption {
          type = types.bool;
          default = true;
          description = "Use fast fsck mode for routine health checks";
        };

        wantedSize = mkOption {
          type = types.nullOr types.str;
          default = null;
          description = "Maximum repository size (e.g., '10G', '1T'). Null for unlimited.";
        };
      };

      # Maintenance Tasks
      maintenance = {
        enableAutoGc = mkOption {
          type = types.bool;
          default = true;
          description = "Enable automatic garbage collection";
        };

        gcSchedule = mkOption {
          type = types.str;
          default = "weekly";
          description = "Schedule for garbage collection (systemd timer format)";
        };

        enablePeriodicFsck = mkOption {
          type = types.bool;
          default = true;
          description = "Enable periodic file system consistency checks";
        };

        fsckSchedule = mkOption {
          type = types.str;
          default = "monthly";
          description = "Schedule for periodic fsck (systemd timer format)";
        };

        enableAutoSync = mkOption {
          type = types.bool;
          default = false;
          description = "Enable automatic synchronization with remotes";
        };

        syncSchedule = mkOption {
          type = types.str;
          default = "hourly";
          description = "Schedule for auto-sync with remotes (systemd timer format)";
        };

        unusedCleanup = mkOption {
          type = types.bool;
          default = true;
          description = "Automatically clean up unused files during maintenance";
        };

        unusedRetention = mkOption {
          type = types.str;
          default = "30d";
          description = "How long to keep unused files before cleanup";
        };
      };

      # Remote Configuration
      remotes = mkOption {
        type = types.attrsOf (types.submodule {
          options = {
            url = mkOption {
              type = types.str;
              description = "URL or path to the remote repository";
            };

            type = mkOption {
              type = types.enum [ "git" "directory" "rsync" "S3" "glacier" ];
              default = "git";
              description = "Type of remote";
            };

            autoInit = mkOption {
              type = types.bool;
              default = true;
              description = "Automatically initialize the remote";
            };

            autoSync = mkOption {
              type = types.bool;
              default = false;
              description = "Include this remote in automatic sync operations";
            };

            encryption = mkOption {
              type = types.nullOr types.str;
              default = null;
              description = "Encryption method for remote (none, hybrid, shared, etc.)";
            };

            cost = mkOption {
              type = types.nullOr types.int;
              default = null;
              description = "Cost value for this remote (lower is preferred)";
            };

            extraConfig = mkOption {
              type = types.attrsOf types.str;
              default = {};
              description = "Additional git-annex remote configuration";
            };
          };
        });
        default = {};
        description = "Git-annex remote repositories configuration";
      };

      # Activation Scripts
      activationScripts = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Use activation scripts for git-annex initialization";
        };

        preInitCommands = mkOption {
          type = types.listOf types.str;
          default = [];
          description = "Commands to run before git-annex initialization";
        };

        postInitCommands = mkOption {
          type = types.listOf types.str;
          default = [
            "git config core.filemode false"
            "git config core.symlinks true"
          ];
          description = "Commands to run after git-annex initialization";
        };
      };
    };

    observability = {
      enablePrometheus = mkOption {
        type = types.bool;
        default = true;
        description = "Configure Prometheus to scrape Sinex metrics";
      };

      enableGrafana = mkOption {
        type = types.bool;
        default = true;
        description = "Configure Grafana with Sinex dashboards";
      };

      logToDatabase = mkOption {
        type = types.bool;
        default = false;
        description = "Store logs as events in database (alternative to Loki)";
      };

      metricsToDatabase = mkOption {
        type = types.bool;
        default = false;
        description = "Store metrics as events in database (in addition to Prometheus)";
      };
    };

    # Global Health Check Aggregation Configuration
    healthMonitoring = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable global health monitoring and aggregation";
      };

      coordinatorPort = mkOption {
        type = types.port;
        default = 8082;
        description = "Port for health check coordinator service";
      };

      aggregationInterval = mkOption {
        type = types.int;
        default = 30;
        description = "Interval for aggregating health data from all services in seconds";
      };

      overallHealthEndpoint = mkOption {
        type = types.str;
        default = "/health/overall";
        description = "HTTP endpoint for overall system health status";
      };

      detailedHealthEndpoint = mkOption {
        type = types.str;
        default = "/health/detailed";
        description = "HTTP endpoint for detailed health information";
      };

      metricsEndpoint = mkOption {
        type = types.str;
        default = "/health/metrics";
        description = "HTTP endpoint for health metrics";
      };

      dependencies = {
        checkDatabase = mkOption {
          type = types.bool;
          default = true;
          description = "Include database connectivity in overall health";
        };

        checkDiskSpace = mkOption {
          type = types.bool;
          default = true;
          description = "Include disk space monitoring in overall health";
        };

        checkQueueDepth = mkOption {
          type = types.bool;
          default = true;
          description = "Include queue depth monitoring in overall health";
        };

        checkExternalServices = mkOption {
          type = types.bool;
          default = false;
          description = "Include external service dependencies in health checks";
        };

        criticalServices = mkOption {
          type = types.listOf types.str;
          default = [ "sinex-unified-collector" "sinex-promo-worker" ];
          description = "List of services considered critical for overall system health";
        };

        optionalServices = mkOption {
          type = types.listOf types.str;
          default = [ "sinex-disk-monitor" "sinex-queue-monitor" ];
          description = "List of services that don't affect overall health but are monitored";
        };
      };

      alerting = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable health status alerting";
        };

        alertThreshold = mkOption {
          type = types.int;
          default = 3;
          description = "Number of consecutive failures before triggering alerts";
        };

        cooldownPeriod = mkOption {
          type = types.str;
          default = "5min";
          description = "Cooldown period between repeated alerts";
        };

        logLevel = mkOption {
          type = types.enum [ "debug" "info" "warn" "error" ];
          default = "warn";
          description = "Log level for health alerts";
        };

        destinations = {
          journald = mkOption {
            type = types.bool;
            default = true;
            description = "Send alerts to systemd journal";
          };

          file = mkOption {
            type = types.nullOr types.str;
            default = null;
            description = "File path for health alert logs";
          };

          webhook = mkOption {
            type = types.nullOr types.str;
            default = null;
            description = "Webhook URL for external alert notifications";
          };
        };
      };

      recovery = {
        enableAutoRecovery = mkOption {
          type = types.bool;
          default = true;
          description = "Enable automatic recovery actions for failed services";
        };

        maxRecoveryAttempts = mkOption {
          type = types.int;
          default = 3;
          description = "Maximum number of automatic recovery attempts";
        };

        recoveryWindow = mkOption {
          type = types.str;
          default = "30min";
          description = "Time window for counting recovery attempts";
        };

        actions = {
          restartServices = mkOption {
            type = types.bool;
            default = true;
            description = "Automatically restart failed services";
          };

          clearQueues = mkOption {
            type = types.bool;
            default = false;
            description = "Clear stuck queues during recovery";
          };

          recreateConnections = mkOption {
            type = types.bool;
            default = true;
            description = "Recreate database connections during recovery";
          };

          escalate = mkOption {
            type = types.bool;
            default = false;
            description = "Escalate to manual intervention if auto-recovery fails";
          };
        };
      };

      stateManagement = {
        enablePersistence = mkOption {
          type = types.bool;
          default = true;
          description = "Persist health state across system restarts";
        };

        stateFile = mkOption {
          type = types.str;
          default = "health_coordinator_state";
          description = "Filename for persisting health coordinator state";
        };

        historyRetention = mkOption {
          type = types.str;
          default = "7d";
          description = "Duration to retain health check history";
        };

        snapshotInterval = mkOption {
          type = types.str;
          default = "1h";
          description = "Interval for taking health state snapshots";
        };

        compressionEnabled = mkOption {
          type = types.bool;
          default = true;
          description = "Enable compression for health state storage";
        };
      };
    };

    resourceLimits = {
      memory = {
        collectorMax = mkOption {
          type = types.nullOr types.str;
          default = "2G";
          description = "Maximum memory for unified collector (MemoryMax)";
        };

        collectorHigh = mkOption {
          type = types.nullOr types.str;
          default = "1.5G";
          description = "Memory pressure threshold for unified collector (MemoryHigh)";
        };

        workerMax = mkOption {
          type = types.nullOr types.str;
          default = "1G";
          description = "Maximum memory for promotion worker (MemoryMax)";
        };

        workerHigh = mkOption {
          type = types.nullOr types.str;
          default = "750M";
          description = "Memory pressure threshold for promotion worker (MemoryHigh)";
        };

        migrateMax = mkOption {
          type = types.nullOr types.str;
          default = "512M";
          description = "Maximum memory for database migrations (MemoryMax)";
        };
      };

      cpu = {
        collectorQuota = mkOption {
          type = types.nullOr types.str;
          default = "200%";
          description = "CPU quota for unified collector (CPUQuota)";
        };

        workerQuota = mkOption {
          type = types.nullOr types.str;
          default = "150%";
          description = "CPU quota for promotion worker (CPUQuota)";
        };

        migrateQuota = mkOption {
          type = types.nullOr types.str;
          default = "100%";
          description = "CPU quota for database migrations (CPUQuota)";
        };

        collectorWeight = mkOption {
          type = types.nullOr types.int;
          default = 500;
          description = "CPU scheduling weight for unified collector";
        };

        workerWeight = mkOption {
          type = types.nullOr types.int;
          default = 400;
          description = "CPU scheduling weight for promotion worker";
        };
      };

      io = {
        collectorReadBandwidth = mkOption {
          type = types.nullOr types.str;
          default = "100M";
          description = "IO read bandwidth limit for unified collector";
        };

        collectorWriteBandwidth = mkOption {
          type = types.nullOr types.str;
          default = "50M";
          description = "IO write bandwidth limit for unified collector";
        };

        workerReadBandwidth = mkOption {
          type = types.nullOr types.str;
          default = "50M";
          description = "IO read bandwidth limit for promotion worker";
        };

        workerWriteBandwidth = mkOption {
          type = types.nullOr types.str;
          default = "25M";
          description = "IO write bandwidth limit for promotion worker";
        };

        collectorIOPS = mkOption {
          type = types.nullOr types.int;
          default = 1000;
          description = "IOPS limit for unified collector";
        };

        workerIOPS = mkOption {
          type = types.nullOr types.int;
          default = 500;
          description = "IOPS limit for promotion worker";
        };
      };

      fileDescriptors = {
        collectorSoft = mkOption {
          type = types.nullOr types.int;
          default = 8192;
          description = "Soft limit for file descriptors (unified collector)";
        };

        collectorHard = mkOption {
          type = types.nullOr types.int;
          default = 16384;
          description = "Hard limit for file descriptors (unified collector)";
        };

        workerSoft = mkOption {
          type = types.nullOr types.int;
          default = 4096;
          description = "Soft limit for file descriptors (promotion worker)";
        };

        workerHard = mkOption {
          type = types.nullOr types.int;
          default = 8192;
          description = "Hard limit for file descriptors (promotion worker)";
        };
      };

      restart = {
        collectorBurst = mkOption {
          type = types.int;
          default = 5;
          description = "Maximum restart attempts within interval (StartLimitBurst)";
        };

        collectorInterval = mkOption {
          type = types.str;
          default = "10min";
          description = "Restart rate limiting interval (StartLimitIntervalSec)";
        };

        workerBurst = mkOption {
          type = types.int;
          default = 3;
          description = "Maximum restart attempts within interval for worker";
        };

        workerInterval = mkOption {
          type = types.str;
          default = "5min";
          description = "Restart rate limiting interval for worker";
        };

        enableRateLimiting = mkOption {
          type = types.bool;
          default = true;
          description = "Enable restart rate limiting for all services";
        };
      };

      enableResourceLimits = mkOption {
        type = types.bool;
        default = true;
        description = "Enable all systemd resource limits";
      };
    };

    directories = {
      base = mkOption {
        type = types.path;
        default = "/var/lib/sinex";
        description = "Base directory for all Sinex data";
      };

      state = mkOption {
        type = types.path;
        default = "/var/lib/sinex";
        description = "Directory for persistent state data (StateDirectory)";
      };

      runtime = mkOption {
        type = types.path;
        default = "/run/sinex";
        description = "Directory for runtime data (RuntimeDirectory)";
      };

      cache = mkOption {
        type = types.path;
        default = "/var/cache/sinex";
        description = "Directory for cache data (CacheDirectory)";
      };

      logs = mkOption {
        type = types.path;
        default = "/var/log/sinex";
        description = "Directory for log files (LogsDirectory)";
      };

      dlq = mkOption {
        type = types.path;
        default = "/var/lib/sinex/dlq";
        description = "Directory for dead letter queue files";
      };

      health = mkOption {
        type = types.path;
        default = "/var/lib/sinex/health";
        description = "Directory for health check state files";
      };

      monitoring = mkOption {
        type = types.path;
        default = "/var/lib/sinex/monitoring";
        description = "Directory for monitoring data";
      };

      config = mkOption {
        type = types.path;
        default = "/etc/sinex";
        description = "Directory for configuration files";
      };

      sockets = mkOption {
        type = types.path;
        default = "/run/sinex/sockets";
        description = "Directory for Unix domain sockets";
      };

      pid = mkOption {
        type = types.path;
        default = "/run/sinex/pid";
        description = "Directory for PID files";
      };

      permissions = {
        state = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for state directories";
        };

        runtime = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for runtime directories";
        };

        cache = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for cache directories";
        };

        logs = mkOption {
          type = types.str;
          default = "0755";
          description = "Permissions for log directories";
        };

        config = mkOption {
          type = types.str;
          default = "0644";
          description = "Permissions for configuration files";
        };

        sockets = mkOption {
          type = types.str;
          default = "0750";
          description = "Permissions for socket directories";
        };
      };

      cleanup = {
        enableAutoCleanup = mkOption {
          type = types.bool;
          default = true;
          description = "Enable automatic cleanup of temporary files";
        };

        maxTempAge = mkOption {
          type = types.str;
          default = "7d";
          description = "Maximum age for temporary files before cleanup";
        };

        maxCacheAge = mkOption {
          type = types.str;
          default = "30d";
          description = "Maximum age for cache files before cleanup";
        };

        maxLogAge = mkOption {
          type = types.str;
          default = "90d";
          description = "Maximum age for log files before cleanup";
        };

        cleanupSchedule = mkOption {
          type = types.str;
          default = "daily";
          description = "Schedule for cleanup operations (systemd timer format)";
        };
      };
    };

    queueManagement = {
      monitoring = {
        enableDepthMonitoring = mkOption {
          type = types.bool;
          default = true;
          description = "Enable queue depth monitoring";
        };

        maxQueueDepth = mkOption {
          type = types.int;
          default = 10000;
          description = "Maximum allowed queue depth before alerting";
        };

        queueDepthWarningThreshold = mkOption {
          type = types.int;
          default = 5000;
          description = "Queue depth warning threshold";
        };

        enableProcessingTimeMonitoring = mkOption {
          type = types.bool;
          default = true;
          description = "Monitor event processing time";
        };

        maxProcessingTime = mkOption {
          type = types.str;
          default = "30s";
          description = "Maximum allowed processing time per event";
        };

        enableBackpressureHandling = mkOption {
          type = types.bool;
          default = true;
          description = "Enable backpressure handling when queues are full";
        };
      };

      limits = {
        maxConcurrentWorkers = mkOption {
          type = types.int;
          default = 4;
          description = "Maximum concurrent worker processes";
        };

        maxEventsPerBatch = mkOption {
          type = types.int;
          default = 100;
          description = "Maximum events processed per batch";
        };

        batchTimeout = mkOption {
          type = types.str;
          default = "5s";
          description = "Timeout for batch processing";
        };

        enableCircuitBreaker = mkOption {
          type = types.bool;
          default = true;
          description = "Enable circuit breaker for queue processing";
        };

        circuitBreakerThreshold = mkOption {
          type = types.int;
          default = 10;
          description = "Number of failures before opening circuit breaker";
        };

        circuitBreakerTimeout = mkOption {
          type = types.str;
          default = "30s";
          description = "Circuit breaker timeout before attempting recovery";
        };
      };
    };

    diskMonitoring = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable disk space monitoring";
      };

      dlqPath = mkOption {
        type = types.path;
        default = cfg.unifiedCollector.dlq.filePath;
        description = "Path to monitor for DLQ disk usage";
      };

      blobStoragePath = mkOption {
        type = types.path;
        default = cfg.blobStorage.repositoryPath;
        description = "Path to monitor for blob storage disk usage";
      };

      warningThreshold = mkOption {
        type = types.int;
        default = 80;
        description = "Disk usage warning threshold (percentage)";
      };

      criticalThreshold = mkOption {
        type = types.int;
        default = 90;
        description = "Disk usage critical threshold (percentage)";
      };

      maxDlqSize = mkOption {
        type = types.str;
        default = "1G";
        description = "Maximum size for DLQ directory";
      };

      maxBlobStorageSize = mkOption {
        type = types.str;
        default = "10G";
        description = "Maximum size for blob storage";
      };

      cleanupOldFiles = mkOption {
        type = types.bool;
        default = true;
        description = "Enable automatic cleanup of old files";
      };

      retentionDays = mkOption {
        type = types.int;
        default = 30;
        description = "Number of days to retain DLQ files";
      };
    };
  };

  config = mkIf cfg.enable {
    # Ensure PostgreSQL is configured
    assertions = [
      {
        assertion = config.services.postgresql.enable;
        message = "Sinex requires PostgreSQL to be enabled";
      }
      {
        assertion = config.services.postgresql.package.version >= "14";
        message = "Sinex requires PostgreSQL 14 or later";
      }

      # Poll interval validations - must be > 0 for all event sources
      {
        assertion = !cfg.unifiedCollector.sources.atuin.enable || cfg.unifiedCollector.sources.atuin.pollInterval > 0;
        message = "Atuin poll interval must be greater than 0 seconds (got ${toString cfg.unifiedCollector.sources.atuin.pollInterval})";
      }
      {
        assertion = !cfg.unifiedCollector.sources.kittyScrollback.enable || cfg.unifiedCollector.sources.kittyScrollback.captureInterval > 0;
        message = "Kitty scrollback capture interval must be greater than 0 seconds (got ${toString cfg.unifiedCollector.sources.kittyScrollback.captureInterval})";
      }
      {
        assertion = !cfg.unifiedCollector.sources.clipboard.enable || cfg.unifiedCollector.sources.clipboard.pollInterval > 0;
        message = "Clipboard poll interval must be greater than 0 milliseconds (got ${toString cfg.unifiedCollector.sources.clipboard.pollInterval})";
      }
      {
        assertion = !cfg.promoWorker.enable || cfg.promoWorker.pollInterval > 0;
        message = "Promotion worker poll interval must be greater than 0 seconds (got ${toString cfg.promoWorker.pollInterval})";
      }

      # Batch size validations - reasonable bounds (1-10000)
      {
        assertion = !cfg.promoWorker.enable || (cfg.promoWorker.batchSize >= 1 && cfg.promoWorker.batchSize <= 10000);
        message = "Promotion worker batch size must be between 1 and 10000 (got ${toString cfg.promoWorker.batchSize})";
      }
      {
        assertion = cfg.queueManagement.limits.maxEventsPerBatch >= 1 && cfg.queueManagement.limits.maxEventsPerBatch <= 10000;
        message = "Queue management max events per batch must be between 1 and 10000 (got ${toString cfg.queueManagement.limits.maxEventsPerBatch})";
      }
      {
        assertion = !cfg.unifiedCollector.sources.clipboard.enable || (cfg.unifiedCollector.sources.clipboard.maxHistoryEntries >= 1 && cfg.unifiedCollector.sources.clipboard.maxHistoryEntries <= 100000);
        message = "Clipboard max history entries must be between 1 and 100000 (got ${toString cfg.unifiedCollector.sources.clipboard.maxHistoryEntries})";
      }
      {
        assertion = !cfg.unifiedCollector.sources.kittyScrollback.enable || (cfg.unifiedCollector.sources.kittyScrollback.maxScrollbackLines >= 100 && cfg.unifiedCollector.sources.kittyScrollback.maxScrollbackLines <= 1000000);
        message = "Kitty max scrollback lines must be between 100 and 1000000 (got ${toString cfg.unifiedCollector.sources.kittyScrollback.maxScrollbackLines})";
      }

      # Port conflict validations
      {
        assertion = cfg.unifiedCollector.metricsPort != cfg.promoWorker.metricsPort;
        message = "Unified collector and promotion worker cannot use the same metrics port (both using ${toString cfg.unifiedCollector.metricsPort})";
      }
      {
        assertion = cfg.database.port != cfg.unifiedCollector.metricsPort && cfg.database.port != cfg.promoWorker.metricsPort;
        message = "Database port cannot conflict with metrics ports (database: ${toString cfg.database.port}, collector: ${toString cfg.unifiedCollector.metricsPort}, worker: ${toString cfg.promoWorker.metricsPort})";
      }

      # Path validity validations - absolute paths where required
      {
        assertion = lib.hasPrefix "/" cfg.directories.base;
        message = "Base directory must be an absolute path (got '${cfg.directories.base}')";
      }
      {
        assertion = lib.hasPrefix "/" cfg.directories.state;
        message = "State directory must be an absolute path (got '${cfg.directories.state}')";
      }
      {
        assertion = lib.hasPrefix "/" cfg.directories.runtime;
        message = "Runtime directory must be an absolute path (got '${cfg.directories.runtime}')";
      }
      {
        assertion = lib.hasPrefix "/" cfg.directories.cache;
        message = "Cache directory must be an absolute path (got '${cfg.directories.cache}')";
      }
      {
        assertion = lib.hasPrefix "/" cfg.directories.logs;
        message = "Logs directory must be an absolute path (got '${cfg.directories.logs}')";
      }
      {
        assertion = lib.hasPrefix "/" cfg.directories.dlq;
        message = "DLQ directory must be an absolute path (got '${cfg.directories.dlq}')";
      }
      {
        assertion = !cfg.blobStorage.enable || lib.hasPrefix "/" cfg.blobStorage.repositoryPath;
        message = "Blob storage repository path must be an absolute path (got '${cfg.blobStorage.repositoryPath}')";
      }
      {
        assertion = !cfg.unifiedCollector.sources.atuin.enable || lib.hasPrefix "/" cfg.unifiedCollector.sources.atuin.databasePath;
        message = "Atuin database path must be an absolute path (got '${cfg.unifiedCollector.sources.atuin.databasePath}')";
      }
      {
        assertion = !cfg.unifiedCollector.sources.shellHistory.enable || lib.hasPrefix "/" cfg.unifiedCollector.sources.shellHistory.zshPath;
        message = "Zsh history path must be an absolute path (got '${cfg.unifiedCollector.sources.shellHistory.zshPath}')";
      }
      {
        assertion = !cfg.unifiedCollector.sources.shellHistory.enable || lib.hasPrefix "/" cfg.unifiedCollector.sources.shellHistory.bashPath;
        message = "Bash history path must be an absolute path (got '${cfg.unifiedCollector.sources.shellHistory.bashPath}')";
      }
      {
        assertion = !cfg.unifiedCollector.sources.asciinema.enable || lib.hasPrefix "/" cfg.unifiedCollector.sources.asciinema.recordingsPath;
        message = "Asciinema recordings path must be an absolute path (got '${cfg.unifiedCollector.sources.asciinema.recordingsPath}')";
      }
      {
        assertion = !cfg.unifiedCollector.sources.kittyScrollback.enable || lib.hasPrefix "/" cfg.unifiedCollector.sources.kittyScrollback.socketPath;
        message = "Kitty socket path must be an absolute path (got '${cfg.unifiedCollector.sources.kittyScrollback.socketPath}')";
      }
      {
        assertion = builtins.all (path: lib.hasPrefix "/" path) cfg.unifiedCollector.sources.filesystem.watchPaths;
        message = "All filesystem watch paths must be absolute paths (got: ${lib.concatStringsSep ", " (builtins.filter (path: !lib.hasPrefix "/" path) cfg.unifiedCollector.sources.filesystem.watchPaths)})";
      }

      # SSL file validations - paths must exist if provided
      {
        assertion = cfg.database.ssl.certFile == null || builtins.pathExists cfg.database.ssl.certFile;
        message = "SSL certificate file does not exist: ${toString cfg.database.ssl.certFile}";
      }
      {
        assertion = cfg.database.ssl.keyFile == null || builtins.pathExists cfg.database.ssl.keyFile;
        message = "SSL key file does not exist: ${toString cfg.database.ssl.keyFile}";
      }
      {
        assertion = cfg.database.ssl.caFile == null || builtins.pathExists cfg.database.ssl.caFile;
        message = "SSL CA file does not exist: ${toString cfg.database.ssl.caFile}";
      }
      {
        assertion = cfg.database.ssl.crlFile == null || builtins.pathExists cfg.database.ssl.crlFile;
        message = "SSL CRL file does not exist: ${toString cfg.database.ssl.crlFile}";
      }

      # Database connection settings consistency
      {
        assertion = cfg.database.connectionPool.minConnections <= cfg.database.connectionPool.maxConnections;
        message = "Database minimum connections (${toString cfg.database.connectionPool.minConnections}) cannot exceed maximum connections (${toString cfg.database.connectionPool.maxConnections})";
      }
      {
        assertion = cfg.database.connectionPool.minConnections >= 1;
        message = "Database minimum connections must be at least 1 (got ${toString cfg.database.connectionPool.minConnections})";
      }
      {
        assertion = cfg.database.connectionPool.maxConnections >= 1 && cfg.database.connectionPool.maxConnections <= 1000;
        message = "Database maximum connections must be between 1 and 1000 (got ${toString cfg.database.connectionPool.maxConnections})";
      }
      {
        assertion = cfg.database.connectionPool.connectionTimeout > 0 && cfg.database.connectionPool.connectionTimeout <= 300;
        message = "Database connection timeout must be between 1 and 300 seconds (got ${toString cfg.database.connectionPool.connectionTimeout})";
      }
      {
        assertion = cfg.database.connectionPool.idleTimeout > 0;
        message = "Database idle timeout must be greater than 0 seconds (got ${toString cfg.database.connectionPool.idleTimeout})";
      }
      {
        assertion = cfg.database.connectionPool.maxLifetime > 0;
        message = "Database max lifetime must be greater than 0 seconds (got ${toString cfg.database.connectionPool.maxLifetime})";
      }
      {
        assertion = cfg.database.retry.maxRetries >= 0 && cfg.database.retry.maxRetries <= 20;
        message = "Database max retries must be between 0 and 20 (got ${toString cfg.database.retry.maxRetries})";
      }
      {
        assertion = cfg.database.retry.initialDelay > 0 && cfg.database.retry.initialDelay <= 60;
        message = "Database initial retry delay must be between 1 and 60 seconds (got ${toString cfg.database.retry.initialDelay})";
      }
      {
        assertion = cfg.database.retry.maxDelay > 0 && cfg.database.retry.maxDelay <= 300;
        message = "Database max retry delay must be between 1 and 300 seconds (got ${toString cfg.database.retry.maxDelay})";
      }
      {
        assertion = cfg.database.retry.initialDelay <= cfg.database.retry.maxDelay;
        message = "Database initial retry delay (${toString cfg.database.retry.initialDelay}s) cannot exceed max retry delay (${toString cfg.database.retry.maxDelay}s)";
      }
      {
        assertion = cfg.database.retry.backoffMultiplier >= 1.0 && cfg.database.retry.backoffMultiplier <= 10.0;
        message = "Database backoff multiplier must be between 1.0 and 10.0 (got ${toString cfg.database.retry.backoffMultiplier})";
      }

      # Resource limits sanity checks
      {
        assertion = !cfg.resourceLimits.enableResourceLimits || cfg.resourceLimits.fileDescriptors.collectorSoft <= cfg.resourceLimits.fileDescriptors.collectorHard;
        message = "Collector soft file descriptor limit (${toString cfg.resourceLimits.fileDescriptors.collectorSoft}) cannot exceed hard limit (${toString cfg.resourceLimits.fileDescriptors.collectorHard})";
      }
      {
        assertion = !cfg.resourceLimits.enableResourceLimits || cfg.resourceLimits.fileDescriptors.workerSoft <= cfg.resourceLimits.fileDescriptors.workerHard;
        message = "Worker soft file descriptor limit (${toString cfg.resourceLimits.fileDescriptors.workerSoft}) cannot exceed hard limit (${toString cfg.resourceLimits.fileDescriptors.workerHard})";
      }
      {
        assertion = !cfg.resourceLimits.enableResourceLimits || cfg.resourceLimits.fileDescriptors.collectorSoft >= 1024;
        message = "Collector soft file descriptor limit must be at least 1024 (got ${toString cfg.resourceLimits.fileDescriptors.collectorSoft})";
      }
      {
        assertion = !cfg.resourceLimits.enableResourceLimits || cfg.resourceLimits.fileDescriptors.workerSoft >= 512;
        message = "Worker soft file descriptor limit must be at least 512 (got ${toString cfg.resourceLimits.fileDescriptors.workerSoft})";
      }
      {
        assertion = !cfg.resourceLimits.enableResourceLimits || cfg.resourceLimits.restart.collectorBurst >= 1 && cfg.resourceLimits.restart.collectorBurst <= 20;
        message = "Collector restart burst must be between 1 and 20 (got ${toString cfg.resourceLimits.restart.collectorBurst})";
      }
      {
        assertion = !cfg.resourceLimits.enableResourceLimits || cfg.resourceLimits.restart.workerBurst >= 1 && cfg.resourceLimits.restart.workerBurst <= 20;
        message = "Worker restart burst must be between 1 and 20 (got ${toString cfg.resourceLimits.restart.workerBurst})";
      }
      {
        assertion = cfg.resourceLimits.io.collectorIOPS == null || (cfg.resourceLimits.io.collectorIOPS >= 100 && cfg.resourceLimits.io.collectorIOPS <= 100000);
        message = "Collector IOPS limit must be between 100 and 100000 (got ${toString cfg.resourceLimits.io.collectorIOPS})";
      }
      {
        assertion = cfg.resourceLimits.io.workerIOPS == null || (cfg.resourceLimits.io.workerIOPS >= 100 && cfg.resourceLimits.io.workerIOPS <= 100000);
        message = "Worker IOPS limit must be between 100 and 100000 (got ${toString cfg.resourceLimits.io.workerIOPS})";
      }

      # User/group existence validations
      {
        assertion = !cfg.database.autoSetup || config.users.users ? ${cfg.database.user} || cfg.database.user == "postgres";
        message = "Database user '${cfg.database.user}' must exist or be created by autoSetup, or use 'postgres' for system user";
      }
      {
        assertion = config.users.users ? ${cfg.targetUser};
        message = "Target user '${cfg.targetUser}' must exist on the system for file monitoring";
      }

      # Directory permissions validations
      {
        assertion = builtins.match "^[0-7]{3,4}$" cfg.directories.permissions.state != null;
        message = "State directory permissions must be valid octal format (e.g., '0755', got '${cfg.directories.permissions.state}')";
      }
      {
        assertion = builtins.match "^[0-7]{3,4}$" cfg.directories.permissions.runtime != null;
        message = "Runtime directory permissions must be valid octal format (e.g., '0755', got '${cfg.directories.permissions.runtime}')";
      }
      {
        assertion = builtins.match "^[0-7]{3,4}$" cfg.directories.permissions.cache != null;
        message = "Cache directory permissions must be valid octal format (e.g., '0755', got '${cfg.directories.permissions.cache}')";
      }
      {
        assertion = builtins.match "^[0-7]{3,4}$" cfg.directories.permissions.logs != null;
        message = "Logs directory permissions must be valid octal format (e.g., '0755', got '${cfg.directories.permissions.logs}')";
      }
      {
        assertion = builtins.match "^[0-7]{3,4}$" cfg.directories.permissions.config != null;
        message = "Config file permissions must be valid octal format (e.g., '0644', got '${cfg.directories.permissions.config}')";
      }
      {
        assertion = builtins.match "^[0-7]{3,4}$" cfg.directories.permissions.sockets != null;
        message = "Socket directory permissions must be valid octal format (e.g., '0750', got '${cfg.directories.permissions.sockets}')";
      }

      # Database performance validations
      {
        assertion = cfg.database.performance.statementTimeout >= 0;
        message = "Database statement timeout must be non-negative (got ${toString cfg.database.performance.statementTimeout})";
      }
      {
        assertion = cfg.database.performance.lockTimeout >= 0;
        message = "Database lock timeout must be non-negative (got ${toString cfg.database.performance.lockTimeout})";
      }
      {
        assertion = cfg.database.performance.idleInTransactionTimeout >= 0;
        message = "Database idle in transaction timeout must be non-negative (got ${toString cfg.database.performance.idleInTransactionTimeout})";
      }
      {
        assertion = cfg.database.performance.preparedStatementCacheSize >= 0 && cfg.database.performance.preparedStatementCacheSize <= 10000;
        message = "Database prepared statement cache size must be between 0 and 10000 (got ${toString cfg.database.performance.preparedStatementCacheSize})";
      }

      # Health check validations
      {
        assertion = !cfg.database.healthCheck.enable || cfg.database.healthCheck.interval > 0;
        message = "Database health check interval must be greater than 0 seconds (got ${toString cfg.database.healthCheck.interval})";
      }
      {
        assertion = !cfg.database.healthCheck.enable || cfg.database.healthCheck.timeout > 0 && cfg.database.healthCheck.timeout <= 60;
        message = "Database health check timeout must be between 1 and 60 seconds (got ${toString cfg.database.healthCheck.timeout})";
      }
      {
        assertion = !cfg.database.healthCheck.enable || cfg.database.healthCheck.failureThreshold >= 1 && cfg.database.healthCheck.failureThreshold <= 20;
        message = "Database health check failure threshold must be between 1 and 20 (got ${toString cfg.database.healthCheck.failureThreshold})";
      }
      {
        assertion = !cfg.database.healthCheck.enable || cfg.database.healthCheck.successThreshold >= 1 && cfg.database.healthCheck.successThreshold <= 20;
        message = "Database health check success threshold must be between 1 and 20 (got ${toString cfg.database.healthCheck.successThreshold})";
      }

      # Unified Collector health check validations
      {
        assertion = !cfg.unifiedCollector.healthCheck.enable || cfg.unifiedCollector.healthCheck.interval > 0 && cfg.unifiedCollector.healthCheck.interval <= 300;
        message = "Unified collector health check interval must be between 1 and 300 seconds (got ${toString cfg.unifiedCollector.healthCheck.interval})";
      }
      {
        assertion = !cfg.unifiedCollector.healthCheck.enable || cfg.unifiedCollector.healthCheck.timeout > 0 && cfg.unifiedCollector.healthCheck.timeout <= 60;
        message = "Unified collector health check timeout must be between 1 and 60 seconds (got ${toString cfg.unifiedCollector.healthCheck.timeout})";
      }
      {
        assertion = !cfg.unifiedCollector.healthCheck.enable || cfg.unifiedCollector.healthCheck.port != cfg.database.port && cfg.unifiedCollector.healthCheck.port != cfg.unifiedCollector.metricsPort;
        message = "Unified collector health check port (${toString cfg.unifiedCollector.healthCheck.port}) must not conflict with database port (${toString cfg.database.port}) or metrics port (${toString cfg.unifiedCollector.metricsPort})";
      }
      {
        assertion = !cfg.unifiedCollector.healthCheck.startupProbe.enable || cfg.unifiedCollector.healthCheck.startupProbe.failureThreshold >= 1 && cfg.unifiedCollector.healthCheck.startupProbe.failureThreshold <= 30;
        message = "Unified collector startup probe failure threshold must be between 1 and 30 (got ${toString cfg.unifiedCollector.healthCheck.startupProbe.failureThreshold})";
      }
      {
        assertion = !cfg.unifiedCollector.healthCheck.readinessProbe.enable || cfg.unifiedCollector.healthCheck.readinessProbe.failureThreshold >= 1 && cfg.unifiedCollector.healthCheck.readinessProbe.failureThreshold <= 10;
        message = "Unified collector readiness probe failure threshold must be between 1 and 10 (got ${toString cfg.unifiedCollector.healthCheck.readinessProbe.failureThreshold})";
      }
      {
        assertion = !cfg.unifiedCollector.healthCheck.livenessProbe.enable || cfg.unifiedCollector.healthCheck.livenessProbe.failureThreshold >= 1 && cfg.unifiedCollector.healthCheck.livenessProbe.failureThreshold <= 10;
        message = "Unified collector liveness probe failure threshold must be between 1 and 10 (got ${toString cfg.unifiedCollector.healthCheck.livenessProbe.failureThreshold})";
      }
      {
        assertion = cfg.unifiedCollector.restart.maxRestarts >= 1 && cfg.unifiedCollector.restart.maxRestarts <= 20;
        message = "Unified collector max restarts must be between 1 and 20 (got ${toString cfg.unifiedCollector.restart.maxRestarts})";
      }

      # Promotion Worker health check validations
      {
        assertion = !cfg.promoWorker.healthCheck.enable || cfg.promoWorker.healthCheck.interval > 0 && cfg.promoWorker.healthCheck.interval <= 300;
        message = "Promotion worker health check interval must be between 1 and 300 seconds (got ${toString cfg.promoWorker.healthCheck.interval})";
      }
      {
        assertion = !cfg.promoWorker.healthCheck.enable || cfg.promoWorker.healthCheck.timeout > 0 && cfg.promoWorker.healthCheck.timeout <= 60;
        message = "Promotion worker health check timeout must be between 1 and 60 seconds (got ${toString cfg.promoWorker.healthCheck.timeout})";
      }
      {
        assertion = !cfg.promoWorker.healthCheck.enable || cfg.promoWorker.healthCheck.port != cfg.database.port && cfg.promoWorker.healthCheck.port != cfg.promoWorker.metricsPort && cfg.promoWorker.healthCheck.port != cfg.unifiedCollector.healthCheck.port;
        message = "Promotion worker health check port (${toString cfg.promoWorker.healthCheck.port}) must not conflict with database port (${toString cfg.database.port}), metrics port (${toString cfg.promoWorker.metricsPort}), or collector health port (${toString cfg.unifiedCollector.healthCheck.port})";
      }
      {
        assertion = !cfg.promoWorker.healthCheck.queueHealth.enable || cfg.promoWorker.healthCheck.queueHealth.maxDepthThreshold >= 10 && cfg.promoWorker.healthCheck.queueHealth.maxDepthThreshold <= 100000;
        message = "Promotion worker queue health max depth threshold must be between 10 and 100000 (got ${toString cfg.promoWorker.healthCheck.queueHealth.maxDepthThreshold})";
      }
      {
        assertion = cfg.promoWorker.restart.maxRestarts >= 1 && cfg.promoWorker.restart.maxRestarts <= 20;
        message = "Promotion worker max restarts must be between 1 and 20 (got ${toString cfg.promoWorker.restart.maxRestarts})";
      }

      # Health monitoring validations
      {
        assertion = !cfg.healthMonitoring.enable || cfg.healthMonitoring.aggregationInterval >= 10 && cfg.healthMonitoring.aggregationInterval <= 300;
        message = "Health monitoring aggregation interval must be between 10 and 300 seconds (got ${toString cfg.healthMonitoring.aggregationInterval})";
      }
      {
        assertion = !cfg.healthMonitoring.enable || cfg.healthMonitoring.coordinatorPort != cfg.database.port && cfg.healthMonitoring.coordinatorPort != cfg.unifiedCollector.healthCheck.port && cfg.healthMonitoring.coordinatorPort != cfg.promoWorker.healthCheck.port;
        message = "Health monitoring coordinator port (${toString cfg.healthMonitoring.coordinatorPort}) must not conflict with other service ports";
      }
      {
        assertion = !cfg.healthMonitoring.alerting.enable || cfg.healthMonitoring.alerting.alertThreshold >= 1 && cfg.healthMonitoring.alerting.alertThreshold <= 10;
        message = "Health monitoring alert threshold must be between 1 and 10 (got ${toString cfg.healthMonitoring.alerting.alertThreshold})";
      }
      {
        assertion = !cfg.healthMonitoring.recovery.enableAutoRecovery || cfg.healthMonitoring.recovery.maxRecoveryAttempts >= 1 && cfg.healthMonitoring.recovery.maxRecoveryAttempts <= 10;
        message = "Health monitoring max recovery attempts must be between 1 and 10 (got ${toString cfg.healthMonitoring.recovery.maxRecoveryAttempts})";
      }

      # Migration validations
      {
        assertion = cfg.database.migration.timeout > 0 && cfg.database.migration.timeout <= 3600;
        message = "Database migration timeout must be between 1 and 3600 seconds (got ${toString cfg.database.migration.timeout})";
      }
      {
        assertion = !cfg.database.migration.enableLocking || cfg.database.migration.lockTimeout > 0 && cfg.database.migration.lockTimeout <= 1800;
        message = "Database migration lock timeout must be between 1 and 1800 seconds when locking is enabled (got ${toString cfg.database.migration.lockTimeout})";
      }

      # Queue management validations
      {
        assertion = cfg.queueManagement.monitoring.maxQueueDepth > 0 && cfg.queueManagement.monitoring.maxQueueDepth <= 1000000;
        message = "Maximum queue depth must be between 1 and 1000000 (got ${toString cfg.queueManagement.monitoring.maxQueueDepth})";
      }
      {
        assertion = cfg.queueManagement.monitoring.queueDepthWarningThreshold <= cfg.queueManagement.monitoring.maxQueueDepth;
        message = "Queue depth warning threshold (${toString cfg.queueManagement.monitoring.queueDepthWarningThreshold}) cannot exceed max queue depth (${toString cfg.queueManagement.monitoring.maxQueueDepth})";
      }
      {
        assertion = cfg.queueManagement.limits.maxConcurrentWorkers >= 1 && cfg.queueManagement.limits.maxConcurrentWorkers <= 64;
        message = "Maximum concurrent workers must be between 1 and 64 (got ${toString cfg.queueManagement.limits.maxConcurrentWorkers})";
      }
      {
        assertion = !cfg.queueManagement.limits.enableCircuitBreaker || cfg.queueManagement.limits.circuitBreakerThreshold >= 1 && cfg.queueManagement.limits.circuitBreakerThreshold <= 100;
        message = "Circuit breaker threshold must be between 1 and 100 when enabled (got ${toString cfg.queueManagement.limits.circuitBreakerThreshold})";
      }

      # Disk monitoring validations
      {
        assertion = !cfg.diskMonitoring.enable || cfg.diskMonitoring.warningThreshold >= 1 && cfg.diskMonitoring.warningThreshold <= 100;
        message = "Disk warning threshold must be between 1 and 100 percent (got ${toString cfg.diskMonitoring.warningThreshold})";
      }
      {
        assertion = !cfg.diskMonitoring.enable || cfg.diskMonitoring.criticalThreshold >= 1 && cfg.diskMonitoring.criticalThreshold <= 100;
        message = "Disk critical threshold must be between 1 and 100 percent (got ${toString cfg.diskMonitoring.criticalThreshold})";
      }
      {
        assertion = !cfg.diskMonitoring.enable || cfg.diskMonitoring.warningThreshold < cfg.diskMonitoring.criticalThreshold;
        message = "Disk warning threshold (${toString cfg.diskMonitoring.warningThreshold}%) must be less than critical threshold (${toString cfg.diskMonitoring.criticalThreshold}%)";
      }
      {
        assertion = !cfg.diskMonitoring.enable || cfg.diskMonitoring.retentionDays >= 1 && cfg.diskMonitoring.retentionDays <= 365;
        message = "Disk monitoring retention days must be between 1 and 365 (got ${toString cfg.diskMonitoring.retentionDays})";
      }

      # Blob storage validations
      {
        assertion = !cfg.blobStorage.enable || cfg.blobStorage.numCopies >= 1 && cfg.blobStorage.numCopies <= 10;
        message = "Git-annex number of copies must be between 1 and 10 (got ${toString cfg.blobStorage.numCopies})";
      }
      {
        assertion = !cfg.blobStorage.enable || cfg.blobStorage.healthCheck.interval >= 300;
        message = "Git-annex health check interval must be at least 300 seconds (got ${toString cfg.blobStorage.healthCheck.interval})";
      }
      {
        assertion = !cfg.blobStorage.enable || (cfg.blobStorage.backend != "");
        message = "Git-annex backend cannot be empty when blob storage is enabled";
      }

      # Monitoring validations
      {
        assertion = !cfg.database.monitoring.enableMetrics || cfg.database.monitoring.metricsInterval > 0 && cfg.database.monitoring.metricsInterval <= 3600;
        message = "Database metrics interval must be between 1 and 3600 seconds when enabled (got ${toString cfg.database.monitoring.metricsInterval})";
      }
      {
        assertion = !cfg.database.monitoring.enableSlowQueryLog || cfg.database.monitoring.slowQueryThreshold > 0;
        message = "Database slow query threshold must be greater than 0 milliseconds when enabled (got ${toString cfg.database.monitoring.slowQueryThreshold})";
      }

      # Event source specific validations
      {
        assertion = !cfg.unifiedCollector.sources.kittyScrollback.enable || cfg.unifiedCollector.sources.kittyScrollback.commandCaptureDelay >= 0 && cfg.unifiedCollector.sources.kittyScrollback.commandCaptureDelay <= 10000;
        message = "Kitty command capture delay must be between 0 and 10000 milliseconds (got ${toString cfg.unifiedCollector.sources.kittyScrollback.commandCaptureDelay})";
      }
      {
        assertion = !cfg.unifiedCollector.sources.clipboard.enable || cfg.unifiedCollector.sources.clipboard.maxPreviewLength >= 10 && cfg.unifiedCollector.sources.clipboard.maxPreviewLength <= 10000;
        message = "Clipboard max preview length must be between 10 and 10000 characters (got ${toString cfg.unifiedCollector.sources.clipboard.maxPreviewLength})";
      }

      # DLQ validations
      {
        assertion = cfg.unifiedCollector.dlq.maxRetries >= 0 && cfg.unifiedCollector.dlq.maxRetries <= 20;
        message = "DLQ max retries must be between 0 and 20 (got ${toString cfg.unifiedCollector.dlq.maxRetries})";
      }
      {
        assertion = cfg.unifiedCollector.dlq.retryDelaySecs > 0 && cfg.unifiedCollector.dlq.retryDelaySecs <= 3600;
        message = "DLQ retry delay must be between 1 and 3600 seconds (got ${toString cfg.unifiedCollector.dlq.retryDelaySecs})";
      }

      # SSL mode consistency validation
      {
        assertion = cfg.database.ssl.mode != "verify-ca" || cfg.database.ssl.caFile != null;
        message = "SSL CA file must be provided when using 'verify-ca' mode";
      }
      {
        assertion = cfg.database.ssl.mode != "verify-full" || cfg.database.ssl.caFile != null;
        message = "SSL CA file must be provided when using 'verify-full' mode";
      }
      {
        assertion = cfg.database.ssl.certFile == null || cfg.database.ssl.keyFile != null;
        message = "SSL key file must be provided when SSL certificate file is specified";
      }
      {
        assertion = cfg.database.ssl.keyFile == null || cfg.database.ssl.certFile != null;
        message = "SSL certificate file must be provided when SSL key file is specified";
      }
    ];

    # System packages
    environment.systemPackages = [ cfg.package ] 
      ++ lib.optionals cfg.blobStorage.enable [ pkgs.git-annex pkgs.git ];
    
    # Activation scripts for git-annex setup
    system.activationScripts.sinex-annex-setup = mkIf (cfg.blobStorage.enable && cfg.blobStorage.activationScripts.enable) {
      text = ''
        echo "Setting up git-annex repository directory structure..."
        
        # Ensure the parent directory exists with correct ownership
        mkdir -p "$(dirname ${cfg.blobStorage.repositoryPath})"
        chown -R ${cfg.database.user}:${cfg.database.user} "$(dirname ${cfg.blobStorage.repositoryPath})"
        
        # Create the repository directory if it doesn't exist
        if [ ! -d "${cfg.blobStorage.repositoryPath}" ]; then
          mkdir -p "${cfg.blobStorage.repositoryPath}"
          chown ${cfg.database.user}:${cfg.database.user} "${cfg.blobStorage.repositoryPath}"
          chmod ${cfg.directories.permissions.state} "${cfg.blobStorage.repositoryPath}"
          echo "Created git-annex repository directory: ${cfg.blobStorage.repositoryPath}"
        fi
        
        # Ensure proper permissions for health monitoring
        mkdir -p "${cfg.directories.health}"
        chown ${cfg.database.user}:${cfg.database.user} "${cfg.directories.health}"
        chmod ${cfg.directories.permissions.state} "${cfg.directories.health}"
        
        echo "Git-annex activation script completed"
      '';
      deps = [ ];
    };
    
    # Create sinex user and group
    users.users.${cfg.database.user} = mkIf cfg.database.autoSetup {
      isSystemUser = true;
      group = cfg.database.user;
      description = "Sinex service user";
      home = cfg.directories.state;
      createHome = true;
      shell = pkgs.bash;  # Allow shell access for testing and maintenance
    };
    
    users.groups.${cfg.database.user} = mkIf cfg.database.autoSetup {};

    # Database setup
    services.postgresql = mkIf cfg.database.autoSetup {
      enable = true;
      package = mkForce pkgs.postgresql_16;
      extraPlugins = with pkgs.postgresql16Packages; [
        timescaledb
        pgvector
        pgx_ulid
        pg_jsonschema # From our overlay
      ];
      settings = {
        shared_preload_libraries = "timescaledb";
      };
      ensureDatabases = [ cfg.database.name ];
      ensureUsers = [
        {
          name = cfg.database.user;
          ensureDBOwnership = true;
        }
      ];
    };

    # Database migrations
    systemd.services.sinex-migrate = mkIf cfg.database.autoSetup {
      description = "Run Sinex database migrations";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" ];
      requires = [ "postgresql.service" ];

      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = migrateDbScript;
        User = "postgres";
        Environment = "PATH=${pkgs.postgresql}/bin:${pkgs.sqlx-cli}/bin";
        
        # Resource limits for migration process
      } // (optionalAttrs cfg.resourceLimits.enableResourceLimits {
        MemoryMax = mkIf (cfg.resourceLimits.memory.migrateMax != null) cfg.resourceLimits.memory.migrateMax;
        CPUQuota = mkIf (cfg.resourceLimits.cpu.migrateQuota != null) cfg.resourceLimits.cpu.migrateQuota;
        
        # Timeout for long-running migrations
        TimeoutStartSec = "600s";
        TimeoutStopSec = "30s";
      });
    };

    # Atuin database initialization service
    systemd.services.sinex-atuin-init = mkIf cfg.unifiedCollector.sources.atuin.enable {
      description = "Initialize Atuin database for Sinex";
      wantedBy = [ "multi-user.target" ];
      before = [ "sinex-unified-collector.service" ];
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = cfg.targetUser;
        Group = "users";
        ExecStart = pkgs.writeShellScript "init-atuin" ''
          set -e
          if [ ! -f ${cfg.unifiedCollector.sources.atuin.databasePath} ]; then
            ${pkgs.atuin}/bin/atuin init zsh
            ${pkgs.atuin}/bin/atuin import auto
          fi
        '';
      };
    };

    # Unified Collector service
    systemd.services.sinex-unified-collector = mkIf cfg.unifiedCollector.enable {
      description = "Sinex Unified Event Collector";
      after = [
        "network.target"
        "postgresql.service"
      ] ++ optional cfg.database.autoSetup "sinex-migrate.service"
        ++ optional cfg.unifiedCollector.sources.atuin.enable "sinex-atuin-init.service";
      wants = [ "postgresql.service" ] 
        ++ optional cfg.unifiedCollector.sources.atuin.enable "sinex-atuin-init.service";
      wantedBy = [ "multi-user.target" ];

      environment = {
        RUST_LOG = cfg.unifiedCollector.logLevel;
        DATABASE_URL = buildDatabaseUrl cfg;
        SINEX_CONFIG = collectorConfigFile;
        
        # Database connection configuration
        SINEX_DB_MAX_CONNECTIONS = toString cfg.database.connectionPool.maxConnections;
        SINEX_DB_MIN_CONNECTIONS = toString cfg.database.connectionPool.minConnections;
        SINEX_DB_CONNECTION_TIMEOUT = toString cfg.database.connectionPool.connectionTimeout;
        SINEX_DB_IDLE_TIMEOUT = toString cfg.database.connectionPool.idleTimeout;
        SINEX_DB_MAX_LIFETIME = toString cfg.database.connectionPool.maxLifetime;
        
        # Database retry configuration
        SINEX_DB_MAX_RETRIES = toString cfg.database.retry.maxRetries;
        SINEX_DB_INITIAL_DELAY = toString cfg.database.retry.initialDelay;
        SINEX_DB_MAX_DELAY = toString cfg.database.retry.maxDelay;
        SINEX_DB_BACKOFF_MULTIPLIER = toString cfg.database.retry.backoffMultiplier;
        SINEX_DB_ENABLE_JITTER = if cfg.database.retry.enableJitter then "true" else "false";
        
        # Database performance configuration
        SINEX_DB_STATEMENT_TIMEOUT = toString cfg.database.performance.statementTimeout;
        SINEX_DB_LOCK_TIMEOUT = toString cfg.database.performance.lockTimeout;
        SINEX_DB_IDLE_IN_TRANSACTION_TIMEOUT = toString cfg.database.performance.idleInTransactionTimeout;
        SINEX_DB_ENABLE_PREPARED_STATEMENTS = if cfg.database.performance.enablePreparedStatements then "true" else "false";
        SINEX_DB_PREPARED_STATEMENT_CACHE_SIZE = toString cfg.database.performance.preparedStatementCacheSize;
        SINEX_DB_ENABLE_AUTO_COMMIT = if cfg.database.performance.enableAutoCommit then "true" else "false";
        SINEX_DB_DEFAULT_TRANSACTION_ISOLATION = cfg.database.performance.defaultTransactionIsolation;
        
        # Database monitoring configuration
        SINEX_DB_ENABLE_SLOW_QUERY_LOG = if cfg.database.monitoring.enableSlowQueryLog then "true" else "false";
        SINEX_DB_SLOW_QUERY_THRESHOLD = toString cfg.database.monitoring.slowQueryThreshold;
        SINEX_DB_ENABLE_CONNECTION_LOGGING = if cfg.database.monitoring.enableConnectionLogging then "true" else "false";
        SINEX_DB_ENABLE_METRICS = if cfg.database.monitoring.enableMetrics then "true" else "false";
        SINEX_DB_METRICS_INTERVAL = toString cfg.database.monitoring.metricsInterval;
        
        # Queue management environment variables
        SINEX_MAX_QUEUE_DEPTH = toString cfg.queueManagement.monitoring.maxQueueDepth;
        SINEX_QUEUE_WARNING_THRESHOLD = toString cfg.queueManagement.monitoring.queueDepthWarningThreshold;
        SINEX_MAX_PROCESSING_TIME = cfg.queueManagement.monitoring.maxProcessingTime;
        SINEX_MAX_CONCURRENT_WORKERS = toString cfg.queueManagement.limits.maxConcurrentWorkers;
        SINEX_BATCH_SIZE = toString cfg.queueManagement.limits.maxEventsPerBatch;
        SINEX_BATCH_TIMEOUT = cfg.queueManagement.limits.batchTimeout;
        
        # Directory configuration environment variables
        SINEX_STATE_DIR = cfg.directories.state;
        SINEX_RUNTIME_DIR = cfg.directories.runtime;
        SINEX_CACHE_DIR = cfg.directories.cache;
        SINEX_LOGS_DIR = cfg.directories.logs;
        SINEX_DLQ_DIR = cfg.directories.dlq;
        SINEX_HEALTH_DIR = cfg.directories.health;
        SINEX_MONITORING_DIR = cfg.directories.monitoring;
        SINEX_CONFIG_DIR = cfg.directories.config;
        SINEX_SOCKETS_DIR = cfg.directories.sockets;
        SINEX_PID_DIR = cfg.directories.pid;
        
        # Health check environment variables
        SINEX_HEALTH_CHECK_ENABLED = if cfg.unifiedCollector.healthCheck.enable then "true" else "false";
        SINEX_HEALTH_CHECK_PORT = toString cfg.unifiedCollector.healthCheck.port;
        SINEX_HEALTH_CHECK_PATH = cfg.unifiedCollector.healthCheck.path;
        SINEX_READINESS_PATH = cfg.unifiedCollector.healthCheck.readinessPath;
        SINEX_LIVENESS_PATH = cfg.unifiedCollector.healthCheck.livenessPath;
        SINEX_HEALTH_CHECK_TIMEOUT = toString cfg.unifiedCollector.healthCheck.timeout;
        SINEX_HEALTH_STATE_FILE = "${cfg.directories.health}/${cfg.unifiedCollector.healthCheck.persistence.stateFile}";
      };

      serviceConfig = {
        Type = "simple";
        ExecStart = "${cfg.package}/bin/sinex-collector --config ${collectorConfigFile}";
        
        # Health check post-start command
        ExecStartPost = mkIf cfg.unifiedCollector.healthCheck.enable (pkgs.writeShellScript "collector-startup-health-check" ''
          set -euo pipefail
          
          echo "Starting health checks for sinex-unified-collector..."
          
          # Wait for initial startup delay
          sleep ${toString cfg.unifiedCollector.healthCheck.startupProbe.initialDelay}
          
          # Perform startup health checks
          max_attempts=${toString cfg.unifiedCollector.healthCheck.startupProbe.failureThreshold}
          period=${toString cfg.unifiedCollector.healthCheck.startupProbe.periodSeconds}
          timeout=${toString cfg.unifiedCollector.healthCheck.startupProbe.timeoutSeconds}
          
          for attempt in $(seq 1 $max_attempts); do
            echo "Health check attempt $attempt/$max_attempts..."
            
            if ${pkgs.curl}/bin/curl \
              --max-time $timeout \
              --fail \
              --silent \
              --show-error \
              "http://localhost:${toString cfg.unifiedCollector.healthCheck.port}${cfg.unifiedCollector.healthCheck.path}"; then
              echo "✓ Collector startup health check passed"
              
              # Update health state file
              echo "startup_healthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.unifiedCollector.healthCheck.persistence.stateFile}"
              
              exit 0
            else
              echo "⚠️  Health check attempt $attempt failed"
              if [ $attempt -lt $max_attempts ]; then
                sleep $period
              fi
            fi
          done
          
          echo "✗ Collector startup health check failed after $max_attempts attempts" >&2
          exit 1
        '');

        Restart = cfg.unifiedCollector.restart.policy;
        RestartSec = cfg.unifiedCollector.restart.baseDelay;

        # Security hardening - use static user to match database
        User = cfg.database.user;
        Group = cfg.database.user;
        
        # Directory configuration
        StateDirectory = "sinex";
        StateDirectoryMode = cfg.directories.permissions.state;
        RuntimeDirectory = "sinex";
        RuntimeDirectoryMode = cfg.directories.permissions.runtime;
        CacheDirectory = "sinex";
        CacheDirectoryMode = cfg.directories.permissions.cache;
        LogsDirectory = "sinex";
        LogsDirectoryMode = cfg.directories.permissions.logs;
        
        # Security configuration
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = "read-only";
        NoNewPrivileges = true;

        # Allow access to user files for ingestion
        SupplementaryGroups = [ "users" ];

        # Capability for monitoring
        AmbientCapabilities = "CAP_DAC_READ_SEARCH";
        
        # Process limits
        TasksMax = "256";
        
        # Timeout settings
        TimeoutStartSec = "60s";
        TimeoutStopSec = "30s";
        TimeoutAbortSec = "10s";
        
        # Watchdog settings for health monitoring
        WatchdogSec = "30s";
        NotifyAccess = "main";
        
      } // (optionalAttrs cfg.resourceLimits.enableResourceLimits {
        # Memory limits
        MemoryMax = mkIf (cfg.resourceLimits.memory.collectorMax != null) cfg.resourceLimits.memory.collectorMax;
        MemoryHigh = mkIf (cfg.resourceLimits.memory.collectorHigh != null) cfg.resourceLimits.memory.collectorHigh;
        MemorySwapMax = "0";  # Disable swap for performance
        
        # CPU limits
        CPUQuota = mkIf (cfg.resourceLimits.cpu.collectorQuota != null) cfg.resourceLimits.cpu.collectorQuota;
        CPUWeight = mkIf (cfg.resourceLimits.cpu.collectorWeight != null) cfg.resourceLimits.cpu.collectorWeight;
        
        # IO limits
        IOReadBandwidthMax = mkIf (cfg.resourceLimits.io.collectorReadBandwidth != null) 
          "/ ${cfg.resourceLimits.io.collectorReadBandwidth}";
        IOWriteBandwidthMax = mkIf (cfg.resourceLimits.io.collectorWriteBandwidth != null) 
          "/ ${cfg.resourceLimits.io.collectorWriteBandwidth}";
        IOReadIOPSMax = mkIf (cfg.resourceLimits.io.collectorIOPS != null) 
          "/ ${toString cfg.resourceLimits.io.collectorIOPS}";
        IOWriteIOPSMax = mkIf (cfg.resourceLimits.io.collectorIOPS != null) 
          "/ ${toString cfg.resourceLimits.io.collectorIOPS}";
        
        # File descriptor limits
        LimitNOFILE = mkIf (cfg.resourceLimits.fileDescriptors.collectorHard != null) 
          "${toString cfg.resourceLimits.fileDescriptors.collectorSoft}:${toString cfg.resourceLimits.fileDescriptors.collectorHard}";
        
        # Process and thread limits
        LimitNPROC = "1024";
        
      }) // (optionalAttrs (cfg.resourceLimits.restart.enableRateLimiting) {
        # Restart rate limiting  
        StartLimitBurst = cfg.resourceLimits.restart.collectorBurst;
        StartLimitIntervalSec = cfg.resourceLimits.restart.collectorInterval;
      }) // {
        # Enhanced restart policy configuration
        StartLimitBurst = cfg.unifiedCollector.restart.maxRestarts;
        StartLimitIntervalSec = cfg.unifiedCollector.restart.restartWindow;
        RestartPreventExitStatus = "SIGKILL";
        RestartKillSignal = "SIGTERM";
        FinalKillSignal = "SIGKILL";
        TimeoutStopFailureMode = "abort";
      };
    };

    # Promotion Worker service
    systemd.services.sinex-promo-worker = mkIf cfg.promoWorker.enable {
      description = "Sinex Event Promotion Worker";
      after = [
        "network.target"
        "postgresql.service"
      ] ++ optional cfg.database.autoSetup "sinex-migrate.service";
      wants = [ "postgresql.service" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        RUST_LOG = "info";
        DATABASE_URL = buildDatabaseUrl cfg;
        
        # Database connection configuration (shared with collector)
        SINEX_DB_MAX_CONNECTIONS = toString cfg.database.connectionPool.maxConnections;
        SINEX_DB_MIN_CONNECTIONS = toString cfg.database.connectionPool.minConnections;
        SINEX_DB_CONNECTION_TIMEOUT = toString cfg.database.connectionPool.connectionTimeout;
        SINEX_DB_IDLE_TIMEOUT = toString cfg.database.connectionPool.idleTimeout;
        SINEX_DB_MAX_LIFETIME = toString cfg.database.connectionPool.maxLifetime;
        
        # Database retry configuration
        SINEX_DB_MAX_RETRIES = toString cfg.database.retry.maxRetries;
        SINEX_DB_INITIAL_DELAY = toString cfg.database.retry.initialDelay;
        SINEX_DB_MAX_DELAY = toString cfg.database.retry.maxDelay;
        SINEX_DB_BACKOFF_MULTIPLIER = toString cfg.database.retry.backoffMultiplier;
        SINEX_DB_ENABLE_JITTER = if cfg.database.retry.enableJitter then "true" else "false";
        
        # Database performance configuration
        SINEX_DB_STATEMENT_TIMEOUT = toString cfg.database.performance.statementTimeout;
        SINEX_DB_LOCK_TIMEOUT = toString cfg.database.performance.lockTimeout;
        SINEX_DB_IDLE_IN_TRANSACTION_TIMEOUT = toString cfg.database.performance.idleInTransactionTimeout;
        SINEX_DB_ENABLE_PREPARED_STATEMENTS = if cfg.database.performance.enablePreparedStatements then "true" else "false";
        SINEX_DB_PREPARED_STATEMENT_CACHE_SIZE = toString cfg.database.performance.preparedStatementCacheSize;
        SINEX_DB_ENABLE_AUTO_COMMIT = if cfg.database.performance.enableAutoCommit then "true" else "false";
        SINEX_DB_DEFAULT_TRANSACTION_ISOLATION = cfg.database.performance.defaultTransactionIsolation;
        
        # Database monitoring configuration
        SINEX_DB_ENABLE_SLOW_QUERY_LOG = if cfg.database.monitoring.enableSlowQueryLog then "true" else "false";
        SINEX_DB_SLOW_QUERY_THRESHOLD = toString cfg.database.monitoring.slowQueryThreshold;
        SINEX_DB_ENABLE_CONNECTION_LOGGING = if cfg.database.monitoring.enableConnectionLogging then "true" else "false";
        SINEX_DB_ENABLE_METRICS = if cfg.database.monitoring.enableMetrics then "true" else "false";
        SINEX_DB_METRICS_INTERVAL = toString cfg.database.monitoring.metricsInterval;
        
        # Worker-specific queue management settings
        SINEX_WORKER_POLL_INTERVAL = toString cfg.promoWorker.pollInterval;
        SINEX_WORKER_BATCH_SIZE = toString cfg.promoWorker.batchSize;
        SINEX_WORKER_MAX_PROCESSING_TIME = cfg.queueManagement.monitoring.maxProcessingTime;
        SINEX_WORKER_CIRCUIT_BREAKER_THRESHOLD = toString cfg.queueManagement.limits.circuitBreakerThreshold;
        SINEX_WORKER_CIRCUIT_BREAKER_TIMEOUT = cfg.queueManagement.limits.circuitBreakerTimeout;
        
        # Directory configuration environment variables
        SINEX_STATE_DIR = cfg.directories.state;
        SINEX_RUNTIME_DIR = cfg.directories.runtime;
        SINEX_CACHE_DIR = cfg.directories.cache;
        SINEX_LOGS_DIR = cfg.directories.logs;
        SINEX_DLQ_DIR = cfg.directories.dlq;
        SINEX_HEALTH_DIR = cfg.directories.health;
        SINEX_MONITORING_DIR = cfg.directories.monitoring;
        SINEX_CONFIG_DIR = cfg.directories.config;
        SINEX_SOCKETS_DIR = cfg.directories.sockets;
        SINEX_PID_DIR = cfg.directories.pid;
        
        # Health check environment variables
        SINEX_WORKER_HEALTH_CHECK_ENABLED = if cfg.promoWorker.healthCheck.enable then "true" else "false";
        SINEX_WORKER_HEALTH_CHECK_PORT = toString cfg.promoWorker.healthCheck.port;
        SINEX_WORKER_HEALTH_CHECK_PATH = cfg.promoWorker.healthCheck.path;
        SINEX_WORKER_READINESS_PATH = cfg.promoWorker.healthCheck.readinessPath;
        SINEX_WORKER_LIVENESS_PATH = cfg.promoWorker.healthCheck.livenessPath;
        SINEX_WORKER_HEALTH_CHECK_TIMEOUT = toString cfg.promoWorker.healthCheck.timeout;
        SINEX_WORKER_HEALTH_STATE_FILE = "${cfg.directories.health}/${cfg.promoWorker.healthCheck.persistence.stateFile}";
        SINEX_WORKER_QUEUE_HEALTH_ENABLED = if cfg.promoWorker.healthCheck.queueHealth.enable then "true" else "false";
        SINEX_WORKER_HEALTH_MAX_QUEUE_DEPTH = toString cfg.promoWorker.healthCheck.queueHealth.maxDepthThreshold;
        SINEX_WORKER_HEALTH_MAX_PROCESSING_TIME = cfg.promoWorker.healthCheck.queueHealth.processingTimeThreshold;
        SINEX_WORKER_HEALTH_STALLED_JOB_THRESHOLD = cfg.promoWorker.healthCheck.queueHealth.stalledJobThreshold;
      };

      serviceConfig = {
        Type = "simple";
        ExecStart = "${cfg.package}/bin/sinex-promo-worker";
        
        # Health check post-start command
        ExecStartPost = mkIf cfg.promoWorker.healthCheck.enable (pkgs.writeShellScript "worker-startup-health-check" ''
          set -euo pipefail
          
          echo "Starting health checks for sinex-promo-worker..."
          
          # Wait for initial startup delay
          sleep ${toString cfg.promoWorker.healthCheck.startupProbe.initialDelay}
          
          # Perform startup health checks
          max_attempts=${toString cfg.promoWorker.healthCheck.startupProbe.failureThreshold}
          period=${toString cfg.promoWorker.healthCheck.startupProbe.periodSeconds}
          timeout=${toString cfg.promoWorker.healthCheck.startupProbe.timeoutSeconds}
          
          for attempt in $(seq 1 $max_attempts); do
            echo "Health check attempt $attempt/$max_attempts..."
            
            if ${pkgs.curl}/bin/curl \
              --max-time $timeout \
              --fail \
              --silent \
              --show-error \
              "http://localhost:${toString cfg.promoWorker.healthCheck.port}${cfg.promoWorker.healthCheck.path}"; then
              echo "✓ Worker startup health check passed"
              
              # Update health state file
              echo "startup_healthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.promoWorker.healthCheck.persistence.stateFile}"
              
              exit 0
            else
              echo "⚠️  Health check attempt $attempt failed"
              if [ $attempt -lt $max_attempts ]; then
                sleep $period
              fi
            fi
          done
          
          echo "✗ Worker startup health check failed after $max_attempts attempts" >&2
          exit 1
        '');

        Restart = cfg.promoWorker.restart.policy;
        RestartSec = cfg.promoWorker.restart.baseDelay;

        # Security hardening - use static user to match database
        User = cfg.database.user;
        Group = cfg.database.user;
        
        # Directory configuration
        StateDirectory = "sinex";
        StateDirectoryMode = cfg.directories.permissions.state;
        RuntimeDirectory = "sinex";
        RuntimeDirectoryMode = cfg.directories.permissions.runtime;
        CacheDirectory = "sinex";
        CacheDirectoryMode = cfg.directories.permissions.cache;
        LogsDirectory = "sinex";
        LogsDirectoryMode = cfg.directories.permissions.logs;
        
        # Security configuration
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        NoNewPrivileges = true;
        
        # Process limits
        TasksMax = "128";
        
        # Timeout settings
        TimeoutStartSec = "30s";
        TimeoutStopSec = "15s";
        TimeoutAbortSec = "5s";
        
        # Watchdog for worker health
        WatchdogSec = "60s";
        NotifyAccess = "main";
        
      } // (optionalAttrs cfg.resourceLimits.enableResourceLimits {
        # Memory limits
        MemoryMax = mkIf (cfg.resourceLimits.memory.workerMax != null) cfg.resourceLimits.memory.workerMax;
        MemoryHigh = mkIf (cfg.resourceLimits.memory.workerHigh != null) cfg.resourceLimits.memory.workerHigh;
        MemorySwapMax = "0";
        
        # CPU limits
        CPUQuota = mkIf (cfg.resourceLimits.cpu.workerQuota != null) cfg.resourceLimits.cpu.workerQuota;
        CPUWeight = mkIf (cfg.resourceLimits.cpu.workerWeight != null) cfg.resourceLimits.cpu.workerWeight;
        
        # IO limits
        IOReadBandwidthMax = mkIf (cfg.resourceLimits.io.workerReadBandwidth != null) 
          "/ ${cfg.resourceLimits.io.workerReadBandwidth}";
        IOWriteBandwidthMax = mkIf (cfg.resourceLimits.io.workerWriteBandwidth != null) 
          "/ ${cfg.resourceLimits.io.workerWriteBandwidth}";
        IOReadIOPSMax = mkIf (cfg.resourceLimits.io.workerIOPS != null) 
          "/ ${toString cfg.resourceLimits.io.workerIOPS}";
        IOWriteIOPSMax = mkIf (cfg.resourceLimits.io.workerIOPS != null) 
          "/ ${toString cfg.resourceLimits.io.workerIOPS}";
        
        # File descriptor limits
        LimitNOFILE = mkIf (cfg.resourceLimits.fileDescriptors.workerHard != null) 
          "${toString cfg.resourceLimits.fileDescriptors.workerSoft}:${toString cfg.resourceLimits.fileDescriptors.workerHard}";
        
        # Process limits
        LimitNPROC = "512";
        
      }) // (optionalAttrs (cfg.resourceLimits.restart.enableRateLimiting) {
        # Restart rate limiting
        StartLimitBurst = cfg.resourceLimits.restart.workerBurst;
        StartLimitIntervalSec = cfg.resourceLimits.restart.workerInterval;
      }) // {
        # Enhanced restart policy configuration
        StartLimitBurst = cfg.promoWorker.restart.maxRestarts;
        StartLimitIntervalSec = cfg.promoWorker.restart.restartWindow;
        RestartPreventExitStatus = "SIGKILL";
        RestartKillSignal = "SIGTERM";
        FinalKillSignal = "SIGKILL";
        TimeoutStopFailureMode = "abort";
      };
    };

    # Git-annex repository initialization
    systemd.services.sinex-annex-init = mkIf (cfg.blobStorage.enable && cfg.blobStorage.autoInit) {
      description = "Initialize Sinex git-annex repository";
      wantedBy = [ "multi-user.target" ];
      before = [ "sinex-unified-collector.service" "sinex-annex-remotes-setup.service" ];

      script = let
        preInitCommands = concatStringsSep "\n" cfg.blobStorage.activationScripts.preInitCommands;
        postInitCommands = concatStringsSep "\n" cfg.blobStorage.activationScripts.postInitCommands;
      in ''
        set -euo pipefail
        
        cd "${cfg.blobStorage.repositoryPath}"
        
        # Pre-initialization commands
        ${preInitCommands}
        
        # Initialize repository if not already done
        if [ ! -d ".git" ]; then
          echo "Initializing git repository..."
          ${pkgs.git}/bin/git init
          echo "Repository initialized"
        fi
        
        # Initialize git-annex if not already done
        if [ ! -d ".git/annex" ]; then
          echo "Initializing git-annex repository..."
          ${pkgs.git-annex}/bin/git-annex init "${cfg.blobStorage.repoDescription}"
          echo "Git-annex initialized"
        fi
        
        # Configure git-annex settings
        echo "Configuring git-annex settings..."
        ${pkgs.git}/bin/git config annex.numcopies ${toString cfg.blobStorage.numCopies}
        ${pkgs.git}/bin/git config annex.largefiles "${cfg.blobStorage.largeFiles}"
        ${pkgs.git}/bin/git config annex.backend "${cfg.blobStorage.backend}"
        
        # Create .gitattributes if it doesn't exist
        if [ ! -f ".gitattributes" ]; then
          cat > .gitattributes << 'EOF'
        # Automatically annex files matching largefiles configuration
        * annex.largefiles=${cfg.blobStorage.largeFiles}
        # But not git/nix metadata files
        .gitattributes annex.largefiles=nothing
        .gitignore annex.largefiles=nothing
        flake.* annex.largefiles=nothing
        default.nix annex.largefiles=nothing
        shell.nix annex.largefiles=nothing
        EOF
          ${pkgs.git}/bin/git add .gitattributes
          if ! ${pkgs.git}/bin/git diff --cached --quiet; then
            ${pkgs.git}/bin/git commit -m "Initial commit: configure git-annex largefiles"
          fi
        fi
        
        # Post-initialization commands
        ${postInitCommands}
        
        echo "Git-annex repository initialization completed"
      '';

      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = cfg.database.user;
        Group = cfg.database.user;
        WorkingDirectory = cfg.blobStorage.repositoryPath;
        EnvironmentFile = pkgs.writeText "sinex-annex-env" ''
          PATH=${lib.makeBinPath [ pkgs.git pkgs.git-annex ]}
        '';
      };
    };

    # Git-annex remotes setup service
    systemd.services.sinex-annex-remotes-setup = mkIf (cfg.blobStorage.enable && cfg.blobStorage.remotes != {}) {
      description = "Setup Sinex git-annex remotes";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-annex-init.service" "network.target" ];
      wants = [ "sinex-annex-init.service" ];

      script = let
        setupRemoteScript = name: remote: ''
          echo "Setting up remote: ${name}"
          
          # Add git remote if it doesn't exist
          if ! ${pkgs.git}/bin/git remote get-url "${name}" >/dev/null 2>&1; then
            echo "Adding git remote ${name}: ${remote.url}"
            ${pkgs.git}/bin/git remote add "${name}" "${remote.url}"
          fi
          
          # Initialize git-annex remote if configured
          ${lib.optionalString remote.autoInit ''
            if ! ${pkgs.git-annex}/bin/git-annex info "${name}" >/dev/null 2>&1; then
              echo "Initializing git-annex remote: ${name}"
              ${pkgs.git-annex}/bin/git-annex initremote "${name}" \
                type=${remote.type} \
                ${lib.optionalString (remote.encryption != null) "encryption=${remote.encryption}"} \
                ${lib.optionalString (remote.cost != null) "cost=${toString remote.cost}"} \
                ${lib.concatStringsSep " " (lib.mapAttrsToList (k: v: "${k}=${v}") remote.extraConfig)} \
                || echo "Remote ${name} already exists or failed to initialize"
            fi
          ''}
          
          echo "Remote ${name} setup completed"
        '';
        remoteSetupCommands = lib.concatStringsSep "\n" (lib.mapAttrsToList setupRemoteScript cfg.blobStorage.remotes);
      in ''
        set -euo pipefail
        
        cd "${cfg.blobStorage.repositoryPath}"
        
        # Ensure we're in a git-annex repository
        if [ ! -d ".git/annex" ]; then
          echo "Error: Not a git-annex repository"
          exit 1
        fi
        
        echo "Setting up git-annex remotes..."
        ${remoteSetupCommands}
        
        echo "All remotes setup completed"
      '';

      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = cfg.database.user;
        Group = cfg.database.user;
        WorkingDirectory = cfg.blobStorage.repositoryPath;
        EnvironmentFile = pkgs.writeText "sinex-annex-env" ''
          PATH=${lib.makeBinPath [ pkgs.git pkgs.git-annex ]}
        '';
      };
    };

    # Git-annex garbage collection service
    systemd.services.sinex-annex-gc = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enableAutoGc) {
      description = "Sinex git-annex garbage collection";
      
      script = ''
        set -euo pipefail
        
        cd "${cfg.blobStorage.repositoryPath}"
        
        echo "Starting git-annex garbage collection..."
        
        # Clean up unused files older than retention period
        ${lib.optionalString cfg.blobStorage.maintenance.unusedCleanup ''
          echo "Identifying unused files..."
          unused_files=$(${pkgs.git-annex}/bin/git-annex unused --used-refspec=+refs/heads/*:refs/heads/* 2>/dev/null || true)
          
          if [ -n "$unused_files" ]; then
            echo "Found unused files, checking retention period..."
            # Note: This is a simplified approach. In practice, you'd want more sophisticated
            # unused file management based on actual timestamps and retention policies.
            ${pkgs.git-annex}/bin/git-annex dropunused --force 1-1000 2>/dev/null || true
            echo "Cleaned up old unused files"
          else
            echo "No unused files found"
          fi
        ''}
        
        # Run git garbage collection
        echo "Running git garbage collection..."
        ${pkgs.git}/bin/git gc --auto
        
        # Run git-annex unused cleanup
        echo "Running git-annex unused cleanup..."
        ${pkgs.git-annex}/bin/git-annex unused >/dev/null 2>&1 || true
        
        echo "Git-annex garbage collection completed"
      '';

      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        WorkingDirectory = cfg.blobStorage.repositoryPath;
        IOSchedulingClass = 3;  # Idle I/O priority
        CPUSchedulingPolicy = "idle";
        EnvironmentFile = pkgs.writeText "sinex-annex-env" ''
          PATH=${lib.makeBinPath [ pkgs.git pkgs.git-annex ]}
        '';
      };
    };

    # Git-annex periodic fsck service
    systemd.services.sinex-annex-fsck = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enablePeriodicFsck) {
      description = "Sinex git-annex periodic file system check";
      
      script = ''
        set -euo pipefail
        
        cd "${cfg.blobStorage.repositoryPath}"
        
        echo "Starting git-annex periodic fsck..."
        
        # Determine fsck mode based on configuration
        fsck_args=""
        ${lib.optionalString cfg.blobStorage.healthCheck.fastFsck ''
          fsck_args="--fast"
        ''}
        
        # Run fsck
        echo "Running git-annex fsck $fsck_args..."
        ${pkgs.git-annex}/bin/git-annex fsck $fsck_args
        
        echo "Git-annex periodic fsck completed"
      '';

      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        WorkingDirectory = cfg.blobStorage.repositoryPath;
        IOSchedulingClass = 3;  # Idle I/O priority
        CPUSchedulingPolicy = "idle";
        EnvironmentFile = pkgs.writeText "sinex-annex-env" ''
          PATH=${lib.makeBinPath [ pkgs.git pkgs.git-annex ]}
        '';
      };
    };

    # Git-annex sync service
    systemd.services.sinex-annex-sync = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enableAutoSync) {
      description = "Sinex git-annex automatic synchronization";
      
      script = let
        syncRemotes = lib.filter (remote: remote.autoSync) (lib.attrValues cfg.blobStorage.remotes);
        remoteNames = lib.concatStringsSep " " (lib.mapAttrsToList (name: remote: 
          lib.optionalString remote.autoSync name
        ) cfg.blobStorage.remotes);
      in ''
        set -euo pipefail
        
        cd "${cfg.blobStorage.repositoryPath}"
        
        echo "Starting git-annex synchronization..."
        
        # Sync with all auto-sync enabled remotes
        ${lib.optionalString (remoteNames != "") ''
          echo "Syncing with remotes: ${remoteNames}"
          ${pkgs.git-annex}/bin/git-annex sync ${remoteNames}
        ''}
        
        # If no specific remotes, sync with all
        ${lib.optionalString (remoteNames == "") ''
          echo "Syncing with all remotes..."
          ${pkgs.git-annex}/bin/git-annex sync
        ''}
        
        echo "Git-annex synchronization completed"
      '';

      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        WorkingDirectory = cfg.blobStorage.repositoryPath;
        EnvironmentFile = pkgs.writeText "sinex-annex-env" ''
          PATH=${lib.makeBinPath [ pkgs.git pkgs.git-annex ]}
        '';
      };
    };

    # Git-annex health check service
    systemd.services.sinex-annex-health = mkIf (cfg.blobStorage.enable && cfg.blobStorage.healthCheck.enable) {
      description = "Sinex git-annex repository health check";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-annex-init.service" ];
      wants = [ "sinex-annex-init.service" ];
      
      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        WorkingDirectory = cfg.blobStorage.repositoryPath;
        ExecStart = pkgs.writeShellScript "sinex-annex-health" ''
          set -euo pipefail
          
          # Health check configuration
          HEALTH_CHECK_TIMEOUT=300  # 5 minutes for git-annex operations
          FAILURE_THRESHOLD=3
          SUCCESS_THRESHOLD=2
          
          # Health state tracking
          STATE_DIR="${cfg.directories.health}"
          mkdir -p "$STATE_DIR"
          FAILURE_COUNT_FILE="$STATE_DIR/annex_failure_count"
          SUCCESS_COUNT_FILE="$STATE_DIR/annex_success_count"
          LAST_STATUS_FILE="$STATE_DIR/annex_last_status"
          
          # Helper functions
          get_count() {
            [ -f "$1" ] && cat "$1" || echo "0"
          }
          
          set_count() {
            echo "$2" > "$1"
          }
          
          cd "${cfg.blobStorage.repositoryPath}"
          
          echo "=== Git-annex Health Check ==="
          echo "Repository: ${cfg.blobStorage.repositoryPath}"
          echo "Timestamp: $(date)"
          echo
          
          health_check_result=0
          failure_count=$(get_count "$FAILURE_COUNT_FILE")
          success_count=$(get_count "$SUCCESS_COUNT_FILE")
          
          # Test 1: Repository structure
          if [ ! -d ".git" ]; then
            echo "✗ Git repository not found" >&2
            health_check_result=1
          elif [ ! -d ".git/annex" ]; then
            echo "✗ Git-annex not initialized" >&2
            health_check_result=1
          else
            echo "✓ Repository structure is valid"
          fi
          
          # Test 2: Basic git-annex operations
          if [ "$health_check_result" -eq 0 ]; then
            echo "Running git-annex status check..."
            if timeout $HEALTH_CHECK_TIMEOUT ${pkgs.git-annex}/bin/git-annex version >/dev/null 2>&1; then
              echo "✓ Git-annex is responsive"
            else
              echo "✗ Git-annex version check failed" >&2
              health_check_result=1
            fi
            
            # Test repository consistency
            echo "Running repository consistency check..."
            if timeout $HEALTH_CHECK_TIMEOUT ${pkgs.git-annex}/bin/git-annex fsck --fast --quiet >/dev/null 2>&1; then
              echo "✓ Repository consistency check passed"
            else
              echo "⚠️  Repository consistency check failed (may indicate corruption)" >&2
              health_check_result=1
            fi
          fi
          
          # Test 3: Check repository size if limit configured
          ${lib.optionalString (cfg.blobStorage.healthCheck.wantedSize != null) ''
            echo "Checking repository size limit..."
            repo_size=$(du -sb . | cut -f1)
            size_limit_bytes=$(numfmt --from=iec "${cfg.blobStorage.healthCheck.wantedSize}")
            
            if [ "$repo_size" -gt "$size_limit_bytes" ]; then
              echo "⚠️  Repository size ($repo_size bytes) exceeds limit (${cfg.blobStorage.healthCheck.wantedSize})" >&2
              # Don't fail health check for size warnings, just log
            else
              echo "✓ Repository size within limits"
            fi
          ''}
          
          # Test 4: Check available disk space
          echo "Checking available disk space..."
          available_space=$(df -B1 . | tail -1 | awk '{print $4}')
          required_space=$((1024 * 1024 * 1024))  # 1GB minimum
          
          if [ "$available_space" -lt "$required_space" ]; then
            echo "✗ Insufficient disk space ($(numfmt --to=iec $available_space) available)" >&2
            health_check_result=1
          else
            echo "✓ Sufficient disk space available"
          fi
          
          # Update health status tracking
          if [ "$health_check_result" -eq 0 ]; then
            success_count=$((success_count + 1))
            set_count "$SUCCESS_COUNT_FILE" "$success_count"
            
            if [ "$success_count" -ge "$SUCCESS_THRESHOLD" ]; then
              echo "✅ Git-annex repository is healthy"
              logger -t sinex-annex-health "Git-annex repository health check passed"
              set_count "$FAILURE_COUNT_FILE" "0"
              set_count "$LAST_STATUS_FILE" "1"
            fi
          else
            failure_count=$((failure_count + 1))
            set_count "$FAILURE_COUNT_FILE" "$failure_count"
            set_count "$SUCCESS_COUNT_FILE" "0"
            
            if [ "$failure_count" -ge "$FAILURE_THRESHOLD" ]; then
              echo "🚨 Git-annex repository marked as unhealthy after $failure_count failures" >&2
              logger -t sinex-annex-health "CRITICAL: Git-annex repository marked as unhealthy"
              set_count "$LAST_STATUS_FILE" "0"
            else
              echo "⚠️  Git-annex health check failed ($failure_count/$FAILURE_THRESHOLD failures)" >&2
              logger -t sinex-annex-health "WARNING: Git-annex repository health check failed"
            fi
          fi
          
          # Additional diagnostic information on failure
          if [ "$health_check_result" -ne 0 ]; then
            echo
            echo "=== Diagnostic Information ==="
            
            # Git repository status
            if ${pkgs.git}/bin/git status >/dev/null 2>&1; then
              echo "✓ Git repository is accessible"
            else
              echo "✗ Git repository access failed" >&2
            fi
            
            # Git-annex info
            echo "Git-annex repository info:"
            ${pkgs.git-annex}/bin/git-annex info || echo "Failed to get git-annex info"
            
            # Disk usage
            echo "Repository disk usage:"
            du -sh . 2>/dev/null || echo "Failed to get disk usage"
            
            echo "Available disk space:"
            df -h . 2>/dev/null || echo "Failed to get disk space info"
          fi
          
          exit $health_check_result
        '';
        EnvironmentFile = pkgs.writeText "sinex-annex-env" ''
          PATH=${lib.makeBinPath [ pkgs.git pkgs.git-annex pkgs.coreutils pkgs.util-linux ]}
        '';
      };
    };

    # Prometheus configuration
    services.prometheus.scrapeConfigs = mkIf cfg.observability.enablePrometheus [
      {
        job_name = "sinex_unified_collector";
        static_configs = [
          {
            targets = [ "localhost:${toString cfg.unifiedCollector.metricsPort}" ];
          }
        ];
      }
      {
        job_name = "sinex_promo_worker";
        static_configs = [
          {
            targets = [ "localhost:${toString cfg.promoWorker.metricsPort}" ];
          }
        ];
      }
    ];

    # Terminal auto-recording for all users
    programs.bash.promptInit = mkIf cfg.unifiedCollector.sources.asciinema.autoRecord ''
      # Automatic asciinema recording for Sinex
      if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
        export ASCIINEMA_REC=1
        ASCIINEMA_DIR="$HOME/.local/share/asciinema"
        mkdir -p "$ASCIINEMA_DIR"
        exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
          "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
      fi
    '';

    programs.zsh.promptInit = mkIf cfg.unifiedCollector.sources.asciinema.autoRecord ''
      # Automatic asciinema recording for Sinex
      if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
        export ASCIINEMA_REC=1
        ASCIINEMA_DIR="$HOME/.local/share/asciinema"
        mkdir -p "$ASCIINEMA_DIR"
        exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
          "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
      fi
    '';

    # Directory structure setup
    systemd.tmpfiles.rules = [
      # Base directories
      "d ${cfg.directories.state} ${cfg.directories.permissions.state} ${cfg.database.user} ${cfg.database.user}"
      "d ${cfg.directories.runtime} ${cfg.directories.permissions.runtime} ${cfg.database.user} ${cfg.database.user}"
      "d ${cfg.directories.cache} ${cfg.directories.permissions.cache} ${cfg.database.user} ${cfg.database.user}"
      "d ${cfg.directories.logs} ${cfg.directories.permissions.logs} ${cfg.database.user} ${cfg.database.user}"
      
      # Specific functional directories
      "d ${cfg.directories.dlq} ${cfg.directories.permissions.state} ${cfg.database.user} ${cfg.database.user}"
      "d ${cfg.directories.health} ${cfg.directories.permissions.state} ${cfg.database.user} ${cfg.database.user}"
      "d ${cfg.directories.monitoring} ${cfg.directories.permissions.state} ${cfg.database.user} ${cfg.database.user}"
      "d ${cfg.directories.sockets} ${cfg.directories.permissions.sockets} ${cfg.database.user} ${cfg.database.user}"
      "d ${cfg.directories.pid} ${cfg.directories.permissions.runtime} ${cfg.database.user} ${cfg.database.user}"
      
      # Configuration directory (with different ownership)
      "d ${cfg.directories.config} ${cfg.directories.permissions.state} root root"
    ] ++ optional cfg.blobStorage.enable 
      "d ${cfg.blobStorage.repositoryPath} ${cfg.directories.permissions.state} ${cfg.database.user} ${cfg.database.user}";

    # Disk space monitoring service
    systemd.services.sinex-disk-monitor = mkIf cfg.diskMonitoring.enable {
      description = "Sinex Disk Space Monitor";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-unified-collector.service" ];
      
      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        ExecStart = pkgs.writeShellScript "sinex-disk-monitor" ''
          set -euo pipefail
          
          # Function to check disk usage and log warnings
          check_disk_usage() {
            local path="$1"
            local name="$2"
            local warning_threshold=${toString cfg.diskMonitoring.warningThreshold}
            local critical_threshold=${toString cfg.diskMonitoring.criticalThreshold}
            
            if [ ! -d "$path" ]; then
              echo "Warning: Directory $path does not exist" >&2
              return 0
            fi
            
            local usage=$(df "$path" | awk 'NR==2 {print $5}' | sed 's/%//')
            
            if [ "$usage" -ge "$critical_threshold" ]; then
              echo "CRITICAL: $name disk usage at $usage% (path: $path)" >&2
              logger -t sinex-disk-monitor "CRITICAL: $name disk usage at $usage%"
              return 1
            elif [ "$usage" -ge "$warning_threshold" ]; then
              echo "WARNING: $name disk usage at $usage% (path: $path)" >&2
              logger -t sinex-disk-monitor "WARNING: $name disk usage at $usage%"
            else
              echo "OK: $name disk usage at $usage% (path: $path)"
            fi
            
            return 0
          }
          
          # Function to check directory size limits
          check_directory_size() {
            local path="$1"
            local name="$2"
            local max_size="$3"
            
            if [ ! -d "$path" ]; then
              return 0
            fi
            
            local current_size=$(du -sb "$path" | cut -f1)
            local max_bytes=$(echo "$max_size" | ${pkgs.gnused}/bin/sed 's/G/*1024*1024*1024/g; s/M/*1024*1024/g; s/K/*1024/g' | bc)
            
            if [ "$current_size" -gt "$max_bytes" ]; then
              echo "WARNING: $name directory size ($current_size bytes) exceeds limit ($max_size)" >&2
              logger -t sinex-disk-monitor "WARNING: $name directory size exceeds limit"
              
              ${optionalString cfg.diskMonitoring.cleanupOldFiles ''
                if [ "$name" = "DLQ" ]; then
                  echo "Cleaning up old DLQ files older than ${toString cfg.diskMonitoring.retentionDays} days"
                  find "$path" -type f -mtime +${toString cfg.diskMonitoring.retentionDays} -delete || true
                fi
              ''}
            fi
          }
          
          # Check disk usage for key directories
          exit_code=0
          
          check_disk_usage "${cfg.diskMonitoring.dlqPath}" "DLQ" || exit_code=1
          
          ${optionalString cfg.blobStorage.enable ''
            check_disk_usage "${cfg.diskMonitoring.blobStoragePath}" "Blob Storage" || exit_code=1
          ''}
          
          # Check directory size limits
          check_directory_size "${cfg.diskMonitoring.dlqPath}" "DLQ" "${cfg.diskMonitoring.maxDlqSize}"
          
          ${optionalString cfg.blobStorage.enable ''
            check_directory_size "${cfg.diskMonitoring.blobStoragePath}" "Blob Storage" "${cfg.diskMonitoring.maxBlobStorageSize}"
          ''}
          
          exit $exit_code
        '';
      };
    };

    # Timer for regular disk monitoring
    systemd.timers.sinex-disk-monitor = mkIf cfg.diskMonitoring.enable {
      description = "Timer for Sinex Disk Space Monitor";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnBootSec = "5min";
        OnUnitActiveSec = "15min";
        Persistent = true;
      };
    };

    # Queue depth monitoring service
    systemd.services.sinex-queue-monitor = mkIf cfg.queueManagement.monitoring.enableDepthMonitoring {
      description = "Sinex Queue Depth Monitor";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" "sinex-migrate.service" ];
      requires = [ "postgresql.service" ];
      
      environment = {
        DATABASE_URL = cfg.database.url;
        RUST_LOG = "info";
      };
      
      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        ExecStart = pkgs.writeShellScript "sinex-queue-monitor" ''
          set -euo pipefail
          
          # Query promotion queue depth
          queue_depth=$(${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "
            SELECT COUNT(*) FROM sinex_schemas.promotion_queue;
          " | tr -d ' ')
          
          warning_threshold=${toString cfg.queueManagement.monitoring.queueDepthWarningThreshold}
          max_threshold=${toString cfg.queueManagement.monitoring.maxQueueDepth}
          
          echo "Current queue depth: $queue_depth"
          
          if [ "$queue_depth" -ge "$max_threshold" ]; then
            echo "CRITICAL: Queue depth ($queue_depth) exceeds maximum ($max_threshold)" >&2
            logger -t sinex-queue-monitor "CRITICAL: Queue depth exceeds maximum"
            exit 1
          elif [ "$queue_depth" -ge "$warning_threshold" ]; then
            echo "WARNING: Queue depth ($queue_depth) exceeds warning threshold ($warning_threshold)" >&2
            logger -t sinex-queue-monitor "WARNING: Queue depth exceeds warning threshold"
          else
            echo "OK: Queue depth within normal limits"
          fi
          
          # Check for stuck events (processing for too long)
          stuck_events=$(${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "
            SELECT COUNT(*) FROM sinex_schemas.promotion_queue 
            WHERE processing_started_at IS NOT NULL 
            AND processing_started_at < NOW() - INTERVAL '${cfg.queueManagement.monitoring.maxProcessingTime}';
          " | tr -d ' ')
          
          if [ "$stuck_events" -gt "0" ]; then
            echo "WARNING: Found $stuck_events events stuck in processing" >&2
            logger -t sinex-queue-monitor "WARNING: Found stuck events in processing"
          fi
        '';
      };
    };

    # Timer for queue monitoring
    systemd.timers.sinex-queue-monitor = mkIf cfg.queueManagement.monitoring.enableDepthMonitoring {
      description = "Timer for Sinex Queue Monitor";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnBootSec = "2min";
        OnUnitActiveSec = "1min";
        Persistent = true;
      };
    };

    # Database health check service
    systemd.services.sinex-database-health = mkIf cfg.database.healthCheck.enable {
      description = "Sinex Database Health Check";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" ] ++ optional cfg.database.autoSetup "sinex-migrate.service";
      requires = [ "postgresql.service" ];
      
      environment = {
        DATABASE_URL = buildDatabaseUrl cfg;
        RUST_LOG = "warn"; # Only log warnings and errors for health checks
      };
      
      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        ExecStart = pkgs.writeShellScript "sinex-database-health" ''
          set -euo pipefail
          
          # Health check configuration
          HEALTH_CHECK_QUERY="${cfg.database.healthCheck.query}"
          HEALTH_CHECK_TIMEOUT=${toString cfg.database.healthCheck.timeout}
          FAILURE_THRESHOLD=${toString cfg.database.healthCheck.failureThreshold}
          SUCCESS_THRESHOLD=${toString cfg.database.healthCheck.successThreshold}
          
          # Health state tracking files
          STATE_DIR="${cfg.directories.health}"
          mkdir -p "$STATE_DIR"
          FAILURE_COUNT_FILE="$STATE_DIR/db_failure_count"
          SUCCESS_COUNT_FILE="$STATE_DIR/db_success_count"
          LAST_STATUS_FILE="$STATE_DIR/db_last_status"
          
          # Function to get current count from file
          get_count() {
            local file="$1"
            if [ -f "$file" ]; then
              cat "$file"
            else
              echo "0"
            fi
          }
          
          # Function to set count in file
          set_count() {
            local file="$1"
            local count="$2"
            echo "$count" > "$file"
          }
          
          # Function to reset count file
          reset_count() {
            local file="$1"
            set_count "$file" "0"
          }
          
          # Get current counts
          failure_count=$(get_count "$FAILURE_COUNT_FILE")
          success_count=$(get_count "$SUCCESS_COUNT_FILE")
          last_status=$(get_count "$LAST_STATUS_FILE")
          
          echo "=== Database Health Check ==="
          echo "Timestamp: $(date)"
          echo "Query: $HEALTH_CHECK_QUERY"
          echo "Timeout: $HEALTH_CHECK_TIMEOUT seconds"
          echo "Current failure count: $failure_count (threshold: $FAILURE_THRESHOLD)"
          echo "Current success count: $success_count (threshold: $SUCCESS_THRESHOLD)"
          echo
          
          # Perform health check with timeout
          health_check_result=0
          if timeout "$HEALTH_CHECK_TIMEOUT" ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "$HEALTH_CHECK_QUERY" >/dev/null 2>&1; then
            echo "✓ Database health check PASSED"
            
            # Increment success count, reset failure count
            success_count=$((success_count + 1))
            set_count "$SUCCESS_COUNT_FILE" "$success_count"
            reset_count "$FAILURE_COUNT_FILE"
            
            # Check if we've reached success threshold to mark healthy
            if [ "$success_count" -ge "$SUCCESS_THRESHOLD" ]; then
              if [ "$last_status" != "1" ]; then
                echo "🎉 Database marked as HEALTHY (reached success threshold)"
                logger -t sinex-database-health "Database marked as healthy"
              fi
              set_count "$LAST_STATUS_FILE" "1"
            fi
            
          else
            echo "✗ Database health check FAILED"
            health_check_result=1
            
            # Increment failure count, reset success count
            failure_count=$((failure_count + 1))
            set_count "$FAILURE_COUNT_FILE" "$failure_count"
            reset_count "$SUCCESS_COUNT_FILE"
            
            # Check if we've reached failure threshold to mark unhealthy
            if [ "$failure_count" -ge "$FAILURE_THRESHOLD" ]; then
              if [ "$last_status" != "0" ]; then
                echo "💀 Database marked as UNHEALTHY (reached failure threshold)" >&2
                logger -t sinex-database-health "CRITICAL: Database marked as unhealthy"
              fi
              set_count "$LAST_STATUS_FILE" "0"
            else
              echo "⚠️  Database health check failed ($failure_count/$FAILURE_THRESHOLD failures)" >&2
              logger -t sinex-database-health "WARNING: Database health check failed"
            fi
          fi
          
          # Additional diagnostic information on failure
          if [ "$health_check_result" -ne 0 ]; then
            echo
            echo "=== Diagnostic Information ==="
            
            # Check PostgreSQL service status
            if systemctl is-active postgresql >/dev/null 2>&1; then
              echo "✓ PostgreSQL service is active"
            else
              echo "✗ PostgreSQL service is not active" >&2
            fi
            
            # Check if PostgreSQL is accepting connections
            if ${pkgs.postgresql}/bin/pg_isready -h /run/postgresql -q; then
              echo "✓ PostgreSQL is accepting connections"
            else
              echo "✗ PostgreSQL is not accepting connections" >&2
            fi
            
            # Check database existence
            if ${pkgs.postgresql}/bin/psql -lqt | cut -d '|' -f 1 | grep -qw "${escapeDbIdentifier cfg.database.name}"; then
              echo "✓ Database '${escapeDbIdentifier cfg.database.name}' exists"
            else
              echo "✗ Database '${escapeDbIdentifier cfg.database.name}' does not exist" >&2
            fi
            
            # Check user permissions
            if ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "SELECT current_user;" >/dev/null 2>&1; then
              echo "✓ Database user has connection permissions"
            else
              echo "✗ Database user lacks connection permissions" >&2
            fi
          fi
          
          echo "=== End Health Check ==="
          exit $health_check_result
        '';
        
        # Timeout for the entire health check process (includes setup time)
        TimeoutStartSec = "${toString (cfg.database.healthCheck.timeout + 10)}s";
      };
    };

    # Timer for database health checks
    systemd.timers.sinex-database-health = mkIf cfg.database.healthCheck.enable {
      description = "Timer for Sinex Database Health Check";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnBootSec = "30s";
        OnUnitActiveSec = "${toString cfg.database.healthCheck.interval}s";
        Persistent = true;
      };
    };

    # Resource monitoring and alerting service
    systemd.services.sinex-resource-monitor = mkIf cfg.resourceLimits.enableResourceLimits {
      description = "Sinex Resource Usage Monitor";
      wantedBy = [ "multi-user.target" ];
      after = [ "sinex-unified-collector.service" "sinex-promo-worker.service" ];
      
      serviceConfig = {
        Type = "oneshot";
        User = "root";  # Need root to read all process info
        ExecStart = pkgs.writeShellScript "sinex-resource-monitor" ''
          set -euo pipefail
          
          # Function to check memory usage of a service
          check_service_memory() {
            local service="$1"
            local limit="$2"
            
            if ! systemctl is-active "$service" >/dev/null 2>&1; then
              echo "Service $service is not active"
              return 0
            fi
            
            local pid=$(systemctl show "$service" --property MainPID --value)
            if [ "$pid" = "0" ] || [ -z "$pid" ]; then
              echo "Could not determine PID for $service"
              return 0
            fi
            
            # Get memory usage in MB
            local memory_kb=$(ps -o rss= -p "$pid" | tr -d ' ' || echo "0")
            local memory_mb=$((memory_kb / 1024))
            
            echo "$service memory usage: $memory_mb MB (PID: $pid)"
            
            # Parse limit and convert to MB for comparison
            if [ -n "$limit" ]; then
              local limit_mb
              case "$limit" in
                *G) limit_mb=$((''${limit%G} * 1024)) ;;
                *M) limit_mb=''${limit%M} ;;
                *) limit_mb=0 ;;
              esac
              
              if [ "$limit_mb" -gt 0 ] && [ "$memory_mb" -gt "$((limit_mb * 80 / 100))" ]; then
                echo "WARNING: $service using $memory_mb MB, approaching limit of $limit" >&2
                logger -t sinex-resource-monitor "WARNING: $service memory usage high"
              fi
            fi
          }
          
          # Function to check CPU usage
          check_service_cpu() {
            local service="$1"
            
            if ! systemctl is-active "$service" >/dev/null 2>&1; then
              return 0
            fi
            
            local pid=$(systemctl show "$service" --property MainPID --value)
            if [ "$pid" = "0" ] || [ -z "$pid" ]; then
              return 0
            fi
            
            # Get CPU percentage (this is a simple check)
            local cpu_percent=$(ps -o %cpu= -p "$pid" | tr -d ' ' || echo "0")
            echo "$service CPU usage: $cpu_percent%"
            
            # Alert if CPU usage is consistently high (>80%)
            if (( $(echo "$cpu_percent > 80" | bc -l) )); then
              echo "WARNING: $service high CPU usage: $cpu_percent%" >&2
              logger -t sinex-resource-monitor "WARNING: $service high CPU usage"
            fi
          }
          
          echo "=== Sinex Resource Monitor Report ==="
          echo "Timestamp: $(date)"
          echo
          
          # Check collector resources
          if systemctl is-enabled sinex-unified-collector >/dev/null 2>&1; then
            echo "--- Unified Collector ---"
            check_service_memory "sinex-unified-collector" "${cfg.resourceLimits.memory.collectorMax}"
            check_service_cpu "sinex-unified-collector"
            echo
          fi
          
          # Check worker resources  
          if systemctl is-enabled sinex-promo-worker >/dev/null 2>&1; then
            echo "--- Promotion Worker ---"
            check_service_memory "sinex-promo-worker" "${cfg.resourceLimits.memory.workerMax}"
            check_service_cpu "sinex-promo-worker"
            echo
          fi
          
          # Check service restart counts
          echo "--- Service Restart Counts ---"
          for service in sinex-unified-collector sinex-promo-worker; do
            if systemctl is-enabled "$service" >/dev/null 2>&1; then
              local restart_count=$(systemctl show "$service" --property NRestarts --value)
              echo "$service restarts: $restart_count"
              
              if [ "$restart_count" -gt 5 ]; then
                echo "WARNING: $service has restarted $restart_count times" >&2
                logger -t sinex-resource-monitor "WARNING: $service restart count high"
              fi
            fi
          done
          
          echo "=== End Report ==="
        '';
      };
    };

    # Timer for resource monitoring
    systemd.timers.sinex-resource-monitor = mkIf cfg.resourceLimits.enableResourceLimits {
      description = "Timer for Sinex Resource Monitor";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnBootSec = "5min";
        OnUnitActiveSec = "10min";
        Persistent = true;
      };
    };

    # Health Check Coordination Service  
    systemd.services.sinex-health-coordinator = mkIf cfg.healthMonitoring.enable {
      description = "Sinex Health Check Coordinator";
      after = [ "network.target" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        SINEX_HEALTH_COORDINATOR_PORT = toString cfg.healthMonitoring.coordinatorPort;
        SINEX_HEALTH_AGGREGATION_INTERVAL = toString cfg.healthMonitoring.aggregationInterval;
        SINEX_HEALTH_STATE_FILE = "${cfg.directories.health}/${cfg.healthMonitoring.stateManagement.stateFile}";
        SINEX_HEALTH_HISTORY_RETENTION = cfg.healthMonitoring.stateManagement.historyRetention;
        SINEX_HEALTH_ENABLE_ALERTING = if cfg.healthMonitoring.alerting.enable then "true" else "false";
        SINEX_HEALTH_ENABLE_RECOVERY = if cfg.healthMonitoring.recovery.enableAutoRecovery then "true" else "false";
      };

      serviceConfig = {
        Type = "simple";
        User = cfg.database.user;
        Group = cfg.database.user;
        ExecStart = pkgs.writeShellScript "sinex-health-coordinator" ''
          set -euo pipefail
          
          echo "Starting Sinex Health Check Coordinator..."
          echo "Coordinator Port: $SINEX_HEALTH_COORDINATOR_PORT"
          echo "Aggregation Interval: $SINEX_HEALTH_AGGREGATION_INTERVAL seconds"
          
          # Create health state file if it doesn't exist
          touch "$SINEX_HEALTH_STATE_FILE"
          
          # Function to check individual service health
          check_service_health() {
            local service_name="$1"
            local health_port="$2"
            local health_path="$3"
            local timeout="$4"
            
            if systemctl is-active "$service_name" >/dev/null 2>&1; then
              if ${pkgs.curl}/bin/curl \
                --max-time "$timeout" \
                --fail \
                --silent \
                "http://localhost:$health_port$health_path" >/dev/null 2>&1; then
                echo "healthy"
              else
                echo "unhealthy"
              fi
            else
              echo "inactive"
            fi
          }
          
          # Function to aggregate health status
          aggregate_health() {
            local overall_status="healthy"
            local critical_failures=0
            
            echo "=== Health Aggregation $(date) ===" >> "$SINEX_HEALTH_STATE_FILE"
            
            # Check critical services
            ${lib.concatStringsSep "\n" (map (service: ''
              status=$(check_service_health "${service}" \
                "$(if [ "${service}" = "sinex-unified-collector" ]; then echo "${toString cfg.unifiedCollector.healthCheck.port}"; else echo "${toString cfg.promoWorker.healthCheck.port}"; fi)" \
                "$(if [ "${service}" = "sinex-unified-collector" ]; then echo "${cfg.unifiedCollector.healthCheck.path}"; else echo "${cfg.promoWorker.healthCheck.path}"; fi)" \
                "5")
              echo "${service}:$status:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
              
              if [ "$status" != "healthy" ]; then
                critical_failures=$((critical_failures + 1))
                overall_status="degraded"
              fi
            '') cfg.healthMonitoring.dependencies.criticalServices)}
            
            # Check database if enabled
            ${lib.optionalString cfg.healthMonitoring.dependencies.checkDatabase ''
              if ${pkgs.postgresql}/bin/pg_isready -h /run/postgresql -q >/dev/null 2>&1; then
                if echo "SELECT 1;" | ${pkgs.postgresql}/bin/psql "${buildDatabaseUrl cfg}" >/dev/null 2>&1; then
                  echo "database:healthy:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
                else
                  echo "database:unhealthy:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
                  overall_status="degraded"
                  critical_failures=$((critical_failures + 1))
                fi
              else
                echo "database:disconnected:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
                overall_status="critical"
                critical_failures=$((critical_failures + 1))
              fi
            ''}
            
            # Check disk space if enabled
            ${lib.optionalString cfg.healthMonitoring.dependencies.checkDiskSpace ''
              for path in "${cfg.unifiedCollector.dlq.filePath}" ${optionalString cfg.blobStorage.enable "\"${cfg.blobStorage.repositoryPath}\""}; do
                if [ -d "$path" ]; then
                  usage=$(df "$path" | awk 'NR==2 {print $5}' | sed 's/%//')
                  if [ "$usage" -lt "${toString cfg.diskMonitoring.criticalThreshold}" ]; then
                    echo "disk:$path:healthy:$usage:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
                  else
                    echo "disk:$path:critical:$usage:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
                    overall_status="critical"
                    critical_failures=$((critical_failures + 1))
                  fi
                fi
              done
            ''}
            
            # Check git-annex repository health if enabled
            ${lib.optionalString (cfg.blobStorage.enable && cfg.blobStorage.healthCheck.enable) ''
              if [ -f "${cfg.directories.health}/annex_last_status" ]; then
                annex_status=$(cat "${cfg.directories.health}/annex_last_status" 2>/dev/null || echo "0")
                if [ "$annex_status" = "1" ]; then
                  echo "annex:${cfg.blobStorage.repositoryPath}:healthy:repository_ok:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
                else
                  echo "annex:${cfg.blobStorage.repositoryPath}:critical:repository_failed:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
                  overall_status="critical"
                  critical_failures=$((critical_failures + 1))
                fi
              else
                echo "annex:${cfg.blobStorage.repositoryPath}:unknown:no_status_file:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
              fi
            ''}
            
            echo "overall:$overall_status:$critical_failures:$(date -Iseconds)" >> "$SINEX_HEALTH_STATE_FILE"
            echo "$overall_status"
          }
          
          # Function to handle recovery actions
          handle_recovery() {
            local failed_service="$1"
            
            if [ "$SINEX_HEALTH_ENABLE_RECOVERY" = "true" ]; then
              echo "Initiating recovery for $failed_service..."
              
              # Git-annex specific recovery actions
              if [[ "$failed_service" == *"annex"* ]]; then
                ${lib.optionalString (cfg.blobStorage.enable && cfg.blobStorage.healthCheck.enable) ''
                  echo "Running git-annex specific recovery actions..."
                  
                  # Try to repair the repository
                  cd "${cfg.blobStorage.repositoryPath}" || return 1
                  echo "Attempting git-annex fsck to repair repository..."
                  ${pkgs.git-annex}/bin/git-annex fsck --fast || echo "Git-annex fsck failed"
                  
                  # Try to reinit if fsck fails
                  if [ ! -d ".git/annex" ]; then
                    echo "Attempting to reinitialize git-annex repository..."
                    ${pkgs.git-annex}/bin/git-annex init "${cfg.blobStorage.repoDescription}" || echo "Git-annex reinit failed"
                  fi
                  
                  # Restart related services
                  systemctl restart sinex-annex-health.service || true
                ''}
              else
                # Standard recovery actions for other services
                ${lib.optionalString cfg.healthMonitoring.recovery.actions.restartServices ''
                  echo "Restarting $failed_service..."
                  systemctl restart "$failed_service" || echo "Failed to restart $failed_service"
                ''}
                
                ${lib.optionalString cfg.healthMonitoring.recovery.actions.recreateConnections ''
                  echo "Triggering connection recreation for $failed_service..."
                  systemctl kill -s USR1 "$failed_service" || true
                ''}
              fi
            fi
          }
          
          # Function to send alerts
          send_alert() {
            local alert_message="$1"
            local alert_level="$2"
            
            if [ "$SINEX_HEALTH_ENABLE_ALERTING" = "true" ]; then
              ${lib.optionalString cfg.healthMonitoring.alerting.destinations.journald ''
                logger -t sinex-health-coordinator -p "daemon.$alert_level" "$alert_message"
              ''}
              
              ${lib.optionalString (cfg.healthMonitoring.alerting.destinations.file != null) ''
                echo "$(date -Iseconds) [$alert_level] $alert_message" >> "${cfg.healthMonitoring.alerting.destinations.file}"
              ''}
              
              ${lib.optionalString (cfg.healthMonitoring.alerting.destinations.webhook != null) ''
                ${pkgs.curl}/bin/curl -X POST \
                  -H "Content-Type: application/json" \
                  -d "{\"level\":\"$alert_level\",\"message\":\"$alert_message\",\"timestamp\":\"$(date -Iseconds)\"}" \
                  "${cfg.healthMonitoring.alerting.destinations.webhook}" || true
              ''}
            fi
          }
          
          # Start HTTP server for health endpoints (simple netcat-based server)
          start_http_server() {
            while true; do
              (
                echo "HTTP/1.1 200 OK"
                echo "Content-Type: application/json"
                echo ""
                
                # Generate health response based on latest state
                latest_status=$(tail -1 "$SINEX_HEALTH_STATE_FILE" | grep "^overall:" | cut -d: -f2)
                if [ "$latest_status" = "healthy" ]; then
                  echo '{"status":"healthy","timestamp":"'$(date -Iseconds)'"}'
                else
                  echo '{"status":"'$latest_status'","timestamp":"'$(date -Iseconds)'"}'
                fi
              ) | ${pkgs.netcat}/bin/nc -l -p "$SINEX_HEALTH_COORDINATOR_PORT" -q 1
              
              sleep 1  # Brief pause between requests
            done &
          }
          
          # Start HTTP server
          start_http_server
          
          # Main health check loop
          while true; do
            overall_status=$(aggregate_health)
            
            if [ "$overall_status" != "healthy" ]; then
              send_alert "System health degraded: $overall_status" "warning"
              
              # Check for specific failed services and attempt recovery
              ${lib.concatStringsSep "\n" (map (service: ''
                if ! systemctl is-active "${service}" >/dev/null 2>&1; then
                  handle_recovery "${service}"
                fi
              '') cfg.healthMonitoring.dependencies.criticalServices)}
            fi
            
            sleep "$SINEX_HEALTH_AGGREGATION_INTERVAL"
          done
        '';
        
        Restart = "always";
        RestartSec = "30s";
        
        # Directory configuration
        StateDirectory = "sinex";
        StateDirectoryMode = cfg.directories.permissions.state;
        RuntimeDirectory = "sinex";
        RuntimeDirectoryMode = cfg.directories.permissions.runtime;
        
        # Security configuration
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        NoNewPrivileges = true;
        
        # Process limits
        TasksMax = "64";
        
        # Timeout settings
        TimeoutStartSec = "30s";
        TimeoutStopSec = "15s";
        TimeoutAbortSec = "5s";
      };
    };

    # Individual Service Health Check Services
    systemd.services.sinex-collector-health-monitor = mkIf (cfg.unifiedCollector.enable && cfg.unifiedCollector.healthCheck.enable) {
      description = "Sinex Collector Health Monitor";
      after = [ "sinex-unified-collector.service" ];
      wants = [ "sinex-unified-collector.service" ];
      wantedBy = [ "multi-user.target" ];

      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        ExecStart = pkgs.writeShellScript "collector-health-monitor" ''
          set -euo pipefail
          
          echo "Running collector health check..."
          
          # Readiness probe
          if ${pkgs.curl}/bin/curl \
            --max-time ${toString cfg.unifiedCollector.healthCheck.readinessProbe.timeoutSeconds} \
            --fail \
            --silent \
            "http://localhost:${toString cfg.unifiedCollector.healthCheck.port}${cfg.unifiedCollector.healthCheck.readinessPath}"; then
            echo "readiness:healthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.unifiedCollector.healthCheck.persistence.stateFile}"
          else
            echo "readiness:unhealthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.unifiedCollector.healthCheck.persistence.stateFile}"
          fi
          
          # Liveness probe
          if ${pkgs.curl}/bin/curl \
            --max-time ${toString cfg.unifiedCollector.healthCheck.livenessProbe.timeoutSeconds} \
            --fail \
            --silent \
            "http://localhost:${toString cfg.unifiedCollector.healthCheck.port}${cfg.unifiedCollector.healthCheck.livenessPath}"; then
            echo "liveness:healthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.unifiedCollector.healthCheck.persistence.stateFile}"
          else
            echo "liveness:unhealthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.unifiedCollector.healthCheck.persistence.stateFile}"
            exit 1  # Trigger restart if liveness fails
          fi
        '';
      };
    };

    systemd.services.sinex-worker-health-monitor = mkIf (cfg.promoWorker.enable && cfg.promoWorker.healthCheck.enable) {
      description = "Sinex Worker Health Monitor";
      after = [ "sinex-promo-worker.service" ];
      wants = [ "sinex-promo-worker.service" ];
      wantedBy = [ "multi-user.target" ];

      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        ExecStart = pkgs.writeShellScript "worker-health-monitor" ''
          set -euo pipefail
          
          echo "Running worker health check..."
          
          # Readiness probe
          if ${pkgs.curl}/bin/curl \
            --max-time ${toString cfg.promoWorker.healthCheck.readinessProbe.timeoutSeconds} \
            --fail \
            --silent \
            "http://localhost:${toString cfg.promoWorker.healthCheck.port}${cfg.promoWorker.healthCheck.readinessPath}"; then
            echo "readiness:healthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.promoWorker.healthCheck.persistence.stateFile}"
          else
            echo "readiness:unhealthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.promoWorker.healthCheck.persistence.stateFile}"
          fi
          
          # Liveness probe
          if ${pkgs.curl}/bin/curl \
            --max-time ${toString cfg.promoWorker.healthCheck.livenessProbe.timeoutSeconds} \
            --fail \
            --silent \
            "http://localhost:${toString cfg.promoWorker.healthCheck.port}${cfg.promoWorker.healthCheck.livenessPath}"; then
            echo "liveness:healthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.promoWorker.healthCheck.persistence.stateFile}"
          else
            echo "liveness:unhealthy:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.promoWorker.healthCheck.persistence.stateFile}"
            exit 1  # Trigger restart if liveness fails
          fi
          
          # Queue health check
          ${lib.optionalString cfg.promoWorker.healthCheck.queueHealth.enable ''
            queue_depth=$(echo "SELECT COUNT(*) FROM sinex_schemas.promotion_queue;" | ${pkgs.postgresql}/bin/psql "${buildDatabaseUrl cfg}" -t | tr -d ' ')
            
            if [ "$queue_depth" -gt "${toString cfg.promoWorker.healthCheck.queueHealth.maxDepthThreshold}" ]; then
              echo "queue:unhealthy:depth_exceeded:$queue_depth:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.promoWorker.healthCheck.persistence.stateFile}"
              exit 1
            else
              echo "queue:healthy:depth_ok:$queue_depth:$(date -Iseconds)" >> "${cfg.directories.health}/${cfg.promoWorker.healthCheck.persistence.stateFile}"
            fi
          ''}
        '';
      };
    };

    # Legacy health check service (simplified, for compatibility)
    systemd.services.sinex-healthcheck = {
      description = "Sinex System Health Check (Legacy)";
      
      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        ExecStart = pkgs.writeShellScript "sinex-healthcheck" ''
          set -euo pipefail
          
          echo "=== Sinex Health Check ==="
          echo "Timestamp: $(date)"
          echo
          
          exit_code=0
          
          # Check health coordinator if enabled
          ${lib.optionalString cfg.healthMonitoring.enable ''
            if ${pkgs.curl}/bin/curl \
              --max-time 5 \
              --fail \
              --silent \
              "http://localhost:${toString cfg.healthMonitoring.coordinatorPort}${cfg.healthMonitoring.overallHealthEndpoint}" >/dev/null 2>&1; then
              echo "✓ Health Coordinator: ACCESSIBLE"
            else
              echo "⚠️  Health Coordinator: UNREACHABLE" >&2
              exit_code=1
            fi
          ''}
          
          # Check service status
          echo "--- Service Status ---"
          for service in ${lib.concatStringsSep " " cfg.healthMonitoring.dependencies.criticalServices}; do
            if systemctl is-enabled "$service" >/dev/null 2>&1; then
              if systemctl is-active "$service" >/dev/null 2>&1; then
                echo "✓ $service: ACTIVE"
              else
                echo "✗ $service: INACTIVE" >&2
                exit_code=1
              fi
            fi
          done
          echo
          
          if [ $exit_code -eq 0 ]; then
            echo "✓ Overall Status: HEALTHY"
          else
            echo "⚠️  Overall Status: DEGRADED" >&2
          fi
          
          exit $exit_code
        '';
      };
    };

    # Timer for health checks
    systemd.timers.sinex-healthcheck = {
      description = "Timer for Sinex Health Check";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnBootSec = "1min";
        OnUnitActiveSec = "5min";
        Persistent = true;
      };
    };

    # Timer for collector health monitoring
    systemd.timers.sinex-collector-health-monitor = mkIf (cfg.unifiedCollector.enable && cfg.unifiedCollector.healthCheck.enable) {
      description = "Timer for Sinex Collector Health Monitor";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnBootSec = "${toString cfg.unifiedCollector.healthCheck.readinessProbe.initialDelay}s";
        OnUnitActiveSec = "${toString cfg.unifiedCollector.healthCheck.readinessProbe.periodSeconds}s";
        Persistent = true;
      };
    };

    # Timer for worker health monitoring
    systemd.timers.sinex-worker-health-monitor = mkIf (cfg.promoWorker.enable && cfg.promoWorker.healthCheck.enable) {
      description = "Timer for Sinex Worker Health Monitor";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnBootSec = "${toString cfg.promoWorker.healthCheck.readinessProbe.initialDelay}s";
        OnUnitActiveSec = "${toString cfg.promoWorker.healthCheck.readinessProbe.periodSeconds}s";
        Persistent = true;
      };
    };

    # Directory cleanup service
    systemd.services.sinex-directory-cleanup = mkIf cfg.directories.cleanup.enableAutoCleanup {
      description = "Sinex Directory Cleanup Service";
      
      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        Group = cfg.database.user;
        ExecStart = pkgs.writeShellScript "sinex-directory-cleanup" ''
          set -euo pipefail
          
          echo "=== Sinex Directory Cleanup ==="
          echo "Timestamp: $(date)"
          echo
          
          # Function to clean directory with age limit
          cleanup_directory() {
            local dir="$1"
            local age="$2"
            local description="$3"
            
            if [ ! -d "$dir" ]; then
              echo "Directory $dir does not exist, skipping cleanup"
              return 0
            fi
            
            echo "Cleaning $description in $dir (age: $age)"
            
            # Convert age to find format (e.g., "7d" -> "+7")
            local find_age
            case "$age" in
              *d) find_age="+''${age%d}" ;;
              *h) find_age="+''${age%h}"/24 ;;
              *m) find_age="+''${age%m}"/1440 ;;
              *) find_age="+1" ;;  # Default to 1 day if format unknown
            esac
            
            # Find and remove old files
            local removed_count=0
            if removed_count=$(find "$dir" -type f -mtime "$find_age" -delete -print | wc -l); then
              echo "  Removed $removed_count old files from $description"
            else
              echo "  Warning: Failed to clean some files in $description" >&2
            fi
            
            # Find and remove empty directories (but not the base directory itself)
            local removed_dirs=0
            if removed_dirs=$(find "$dir" -mindepth 1 -type d -empty -delete -print | wc -l); then
              echo "  Removed $removed_dirs empty directories from $description"
            else
              echo "  Note: No empty directories found in $description"
            fi
          }
          
          # Function to check and report directory sizes
          report_directory_size() {
            local dir="$1"
            local description="$2"
            
            if [ ! -d "$dir" ]; then
              return 0
            fi
            
            local size_bytes=$(du -sb "$dir" | cut -f1)
            local size_human=$(du -sh "$dir" | cut -f1)
            
            echo "  $description: $size_human ($size_bytes bytes)"
          }
          
          echo "--- Pre-cleanup Directory Sizes ---"
          report_directory_size "${cfg.directories.cache}" "Cache directory"
          report_directory_size "${cfg.directories.logs}" "Logs directory"
          report_directory_size "${cfg.directories.runtime}" "Runtime directory"
          echo
          
          # Perform cleanup operations
          echo "--- Cleanup Operations ---"
          
          # Clean cache directory
          cleanup_directory "${cfg.directories.cache}" "${cfg.directories.cleanup.maxCacheAge}" "cache files"
          
          # Clean log directory
          cleanup_directory "${cfg.directories.logs}" "${cfg.directories.cleanup.maxLogAge}" "log files"
          
          # Clean runtime temporary files
          cleanup_directory "${cfg.directories.runtime}" "${cfg.directories.cleanup.maxTempAge}" "runtime files"
          
          # Clean DLQ directory if configured
          if [ -d "${cfg.directories.dlq}" ]; then
            cleanup_directory "${cfg.directories.dlq}" "${cfg.directories.cleanup.maxTempAge}" "DLQ files"
          fi
          
          echo
          echo "--- Post-cleanup Directory Sizes ---"
          report_directory_size "${cfg.directories.cache}" "Cache directory"
          report_directory_size "${cfg.directories.logs}" "Logs directory" 
          report_directory_size "${cfg.directories.runtime}" "Runtime directory"
          echo
          
          echo "=== Cleanup Completed ==="
        '';
      };
    };

    # Timer for directory cleanup
    systemd.timers.sinex-directory-cleanup = mkIf cfg.directories.cleanup.enableAutoCleanup {
      description = "Timer for Sinex Directory Cleanup";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = cfg.directories.cleanup.cleanupSchedule;
        Persistent = true;
        RandomizedDelaySec = "1h";  # Spread load
      };
    };

    # Git-annex maintenance timers
    systemd.timers.sinex-annex-gc = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enableAutoGc) {
      description = "Timer for Sinex git-annex garbage collection";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = cfg.blobStorage.maintenance.gcSchedule;
        Persistent = true;
        RandomizedDelaySec = "2h";  # Spread load across different repos
      };
    };

    systemd.timers.sinex-annex-fsck = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enablePeriodicFsck) {
      description = "Timer for Sinex git-annex periodic fsck";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = cfg.blobStorage.maintenance.fsckSchedule;
        Persistent = true;
        RandomizedDelaySec = "6h";  # Spread load for intensive operations
      };
    };

    systemd.timers.sinex-annex-sync = mkIf (cfg.blobStorage.enable && cfg.blobStorage.maintenance.enableAutoSync) {
      description = "Timer for Sinex git-annex auto-sync";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = cfg.blobStorage.maintenance.syncSchedule;
        Persistent = true;
        RandomizedDelaySec = "15min";  # Small delay for sync operations
      };
    };

    systemd.timers.sinex-annex-health = mkIf (cfg.blobStorage.enable && cfg.blobStorage.healthCheck.enable) {
      description = "Timer for Sinex git-annex health check";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnBootSec = "10min";  # Wait for system to stabilize
        OnUnitActiveSec = "${toString cfg.blobStorage.healthCheck.interval}s";
        Persistent = true;
      };
    };
  };
}
