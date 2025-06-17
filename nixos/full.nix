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
          default = "/var/lib/sinex/dlq";
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
    };

    blobStorage = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable git-annex blob storage integration";
      };

      repositoryPath = mkOption {
        type = types.path;
        default = "/var/lib/sinex/annex";
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
    ];

    # System packages
    environment.systemPackages = [ cfg.package ];
    
    # Create sinex user and group
    users.users.${cfg.database.user} = mkIf cfg.database.autoSetup {
      isSystemUser = true;
      group = cfg.database.user;
      description = "Sinex service user";
      home = "/var/lib/sinex";
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
      };

      serviceConfig = {
        Type = "simple";
        ExecStart = "${cfg.package}/bin/sinex-collector --config ${collectorConfigFile}";
        Restart = "always";
        RestartSec = "10s";

        # Security hardening - use static user to match database
        User = cfg.database.user;
        Group = cfg.database.user;
        StateDirectory = "sinex";
        RuntimeDirectory = "sinex";
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
      });
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
      };

      serviceConfig = {
        Type = "simple";
        ExecStart = "${cfg.package}/bin/sinex-promo-worker";
        Restart = "always";
        RestartSec = "10s";

        # Security hardening - use static user to match database
        User = cfg.database.user;
        Group = cfg.database.user;
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
      });
    };

    # Git-annex initialization
    systemd.services.sinex-annex-init = mkIf (cfg.blobStorage.enable && cfg.blobStorage.autoInit) {
      description = "Initialize Sinex git-annex repository";
      wantedBy = [ "multi-user.target" ];
      before = [ "sinex-unified-collector.service" ];

      script = ''
        if [ ! -d "${cfg.blobStorage.repositoryPath}/.git" ]; then
          mkdir -p "$(dirname ${cfg.blobStorage.repositoryPath})"
          cd "$(dirname ${cfg.blobStorage.repositoryPath})"
          git init "$(basename ${cfg.blobStorage.repositoryPath})"
          cd "$(basename ${cfg.blobStorage.repositoryPath})"
          ${pkgs.git-annex}/bin/git-annex init "Sinex Blob Storage"
          git config annex.numcopies ${toString cfg.blobStorage.numCopies}
          git config annex.largefiles "anything"
          git config annex.backend "SHA256E"
        fi
      '';

      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = "root";
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

    # DLQ directory and monitoring setup
    systemd.tmpfiles.rules = [
      "d ${cfg.unifiedCollector.dlq.filePath} 0755 sinex sinex"
      "d /var/lib/sinex/monitoring 0755 sinex sinex"
      "d /var/lib/sinex/health 0755 sinex sinex"
      "d /var/log/sinex 0755 sinex sinex"
    ] ++ optional cfg.blobStorage.enable 
      "d ${cfg.blobStorage.repositoryPath} 0755 sinex sinex";

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
          STATE_DIR="/var/lib/sinex/health"
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

    # Healthcheck service that aggregates all monitoring
    systemd.services.sinex-healthcheck = {
      description = "Sinex System Health Check";
      
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
          
          # Check service status
          echo "--- Service Status ---"
          for service in sinex-unified-collector sinex-promo-worker; do
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
          
          # Check database connectivity
          echo "--- Database Connectivity ---"
          if ${pkgs.postgresql}/bin/pg_isready -h /run/postgresql -q; then
            echo "✓ PostgreSQL: CONNECTED"
            
            # Test database query
            if echo "SELECT 1;" | ${pkgs.postgresql}/bin/psql "${buildDatabaseUrl cfg}" >/dev/null 2>&1; then
              echo "✓ Sinex Database: ACCESSIBLE"
              
              # Check database health status if health checks are enabled
              ${lib.optionalString cfg.database.healthCheck.enable ''
                if [ -f "/var/lib/sinex/health/db_last_status" ]; then
                  db_health_status=$(cat /var/lib/sinex/health/db_last_status)
                  if [ "$db_health_status" = "1" ]; then
                    echo "✓ Database Health: HEALTHY"
                  else
                    echo "⚠️  Database Health: UNHEALTHY" >&2
                    exit_code=1
                  fi
                  
                  # Show failure/success counts
                  if [ -f "/var/lib/sinex/health/db_failure_count" ]; then
                    failure_count=$(cat /var/lib/sinex/health/db_failure_count)
                    success_count=$(cat /var/lib/sinex/health/db_success_count 2>/dev/null || echo "0")
                    echo "  Health Stats: $success_count successes, $failure_count failures"
                  fi
                else
                  echo "⚠️  Database Health: STATUS UNKNOWN (no health data)" >&2
                fi
              ''}
            else
              echo "✗ Sinex Database: QUERY FAILED" >&2
              exit_code=1
            fi
          else
            echo "✗ PostgreSQL: DISCONNECTED" >&2
            exit_code=1
          fi
          echo
          
          # Check disk space for critical paths
          echo "--- Disk Space ---"
          for path in "${cfg.unifiedCollector.dlq.filePath}" ${optionalString cfg.blobStorage.enable "\"${cfg.blobStorage.repositoryPath}\""}; do
            if [ -d "$path" ]; then
              local usage=$(df "$path" | awk 'NR==2 {print $5}' | sed 's/%//')
              if [ "$usage" -lt "${toString cfg.diskMonitoring.criticalThreshold}" ]; then
                echo "✓ $path: $usage% used"
              else
                echo "✗ $path: $usage% used (CRITICAL)" >&2
                exit_code=1
              fi
            fi
          done
          echo
          
          if [ $exit_code -eq 0 ]; then
            echo "🎉 Overall Status: HEALTHY"
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
  };
}
