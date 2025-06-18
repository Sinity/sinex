# Sinex NixOS Module - Modularized Structure
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Import utility modules
  healthChecks = import ./health-checks.nix { inherit lib; };
  
  # Import configuration generation utilities
  configGen = import ../config-gen.nix { inherit lib pkgs; };
  
  # Generate TOML configuration file using config-gen utilities with validation
  collectorConfigFile = configGen.mkCollectorConfigFile cfg.unifiedCollector cfg;
  
in
{
  imports = [
    ./database.nix
    ./event-sources.nix
    ./blob-storage.nix
    ./monitoring.nix
  ];

  options.services.sinex = {
    enable = mkEnableOption "Sinex Exocortex event capture system";

    package = mkOption {
      type = types.package;
      default = pkgs.sinex or (import ../. { }).packages.${pkgs.system}.default;
      defaultText = literalExpression "pkgs.sinex";
      description = "Sinex package to use";
    };

    # Simplified target user configuration
    targetUser = mkOption {
      type = types.str;
      description = "Username whose files to monitor for events";
      example = "myuser";
    };

    # Simplified directories - monitoring.nix compatibility
    directories = {
      state = mkOption {
        type = types.path;
        default = "/var/lib/sinex";
        description = "Directory for persistent state data";
      };

      logs = mkOption {
        type = types.path;
        default = "/var/log/sinex";
        description = "Directory for log files";
      };
    };

    # Unified collector configuration
    unifiedCollector = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the unified event collector";
      };

      # Metrics are now emitted as events, not via HTTP endpoint

      logLevel = mkOption {
        type = types.enum [ "trace" "debug" "info" "warn" "error" ];
        default = "info";
        description = "Log level for the collector";
      };

      dryRun = mkOption {
        type = types.bool;
        default = false;
        description = "Enable dry-run mode (events logged but not stored in database)";
      };

      # Use modularized health checks
      healthCheck = healthChecks.mkHealthCheckOptions {
        defaultPort = 8080;
        serviceName = "unified collector";
        enableProbes = true;
      };

      # Use modularized restart options
      restart = healthChecks.mkRestartOptions;

      # DLQ configuration
      dlq = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable Dead Letter Queue for failed events";
        };

        failureStoragePath = mkOption {
          type = types.str;
          default = "/var/lib/sinex/failures";
          description = ''
            Directory for DLQ files and critical failure logs when database is down.
            Contains both failed event files and critical meta-failure logs.
          '';
        };

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

        cleanup = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable automatic DLQ file cleanup";
          };

          maxAge = mkOption {
            type = types.str;
            default = "7d";
            description = "Maximum age of DLQ files before cleanup";
          };

          maxFiles = mkOption {
            type = types.int;
            default = 10000;
            description = "Maximum number of DLQ files before cleanup";
          };
        };
      };
    };

    # Promotion worker configuration
    promoWorker = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the promotion worker";
      };

      # Metrics are now emitted as events, not via HTTP endpoint

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

      # Use modularized health checks
      healthCheck = healthChecks.mkHealthCheckOptions {
        defaultPort = 8081;
        serviceName = "promotion worker";
        enableProbes = true;
      };
    };

    # Presets for real use cases
    preset = mkOption {
      type = types.enum [ "lite" "normal" "max" ];
      default = "normal";
      description = ''
        Preset configuration for different capture levels:
        - lite: Lightweight capture (core events, minimal resources)
        - normal: Standard comprehensive capture (good default)
        - max: Maximum data capture (everything, high frequency)
      '';
    };

    # Update configuration
    update = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable coordinated update process";
      };

      gracePeriod = mkOption {
        type = types.int;
        default = 30;
        description = "Grace period in seconds for services to complete work before update";
      };

      healthCheckTimeout = mkOption {
        type = types.int;
        default = 60;
        description = "Maximum time to wait for health checks after update";
      };

      rollbackOnFailure = mkOption {
        type = types.bool;
        default = true;
        description = "Automatically rollback if health checks fail";
      };

      preserveData = mkOption {
        type = types.bool;
        default = true;
        description = "Preserve DLQ and failure data during updates";
      };
    };

  };

  config = mkIf cfg.enable {
    # Environment packages
    environment.systemPackages = with pkgs; [ asciinema ];

    # Apply preset configurations based on capture intensity
    services.sinex = mkMerge [
      # Lite preset - lightweight capture with minimal resources
      (mkIf (cfg.preset == "lite") {
        unifiedCollector = {
          logLevel = "warn";  # Minimal logging
          sources = {
            # Core events only
            atuin.pollInterval = 10;  # Slower polling
            clipboard.pollInterval = 2000;  # Very slow clipboard
            kittyScrollback.enable = false;  # Skip terminal capture
            filesystem.watchPaths = [ "~/Documents" ];  # Limited scope
            # Minimal D-Bus monitoring
            dbus = {
              extractNotifications = true;
              extractMedia = false;
              extractPower = true;
              extractScreensaver = false;
              extractPolicykit = false;
              extractNetwork = false;
              extractMounts = false;
              extractBluetooth = false;
              extractHardware = false;
              extractSession = false;
            };
            asciinema.enable = false;  # Skip recording
          };
        };
        database.connectionPool.maxConnections = 10;
        promoWorker.pollInterval = 10;
        blobStorage = {
          healthCheck = {
            interval = 7200;  # 2 hours
            wantedSize = "20G";
          };
          maintenance = {
            gcSchedule = "monthly";
            fsckSchedule = "quarterly";
          };
        };
        unifiedCollector.dlq.cleanup = {
          maxAge = "3d";
          maxFiles = 1000;
        };
      })

      # Normal preset - standard comprehensive capture
      (mkIf (cfg.preset == "normal") {
        unifiedCollector = {
          logLevel = "info";
          sources = {
            atuin.pollInterval = 5;
            clipboard.pollInterval = 1000;
            kittyScrollback.captureInterval = 30;
            filesystem.watchPaths = [ "~/Documents" "~/Projects" "~/Downloads" ];
            # Comprehensive D-Bus monitoring
            dbus = {
              extractNotifications = true;
              extractMedia = true;
              extractPower = true;
              extractScreensaver = true;
              extractPolicykit = true;
              extractNetwork = true;
              extractMounts = true;
              extractBluetooth = true;
              extractHardware = true;
              extractSession = true;
            };
          };
        };
        database.connectionPool.maxConnections = 30;
        promoWorker = {
          pollInterval = 3;
          batchSize = 300;
        };
        blobStorage = {
          healthCheck = {
            interval = 1800;  # 30 minutes
            wantedSize = "100G";
          };
          maintenance = {
            gcSchedule = "weekly";
            fsckSchedule = "monthly";
          };
        };
        unifiedCollector.dlq.cleanup = {
          maxAge = "14d";
          maxFiles = 25000;
        };
        monitoring = {
          alerting.enable = true;
          observabilityStack.enable = true;
          dashboards.grafana.enable = true;
        };
      })

      # Max preset - maximum data capture at high frequency
      (mkIf (cfg.preset == "max") {
        unifiedCollector = {
          logLevel = "debug";  # Detailed logging
          sources = {
            # Everything at maximum frequency
            atuin.pollInterval = 1;
            clipboard.pollInterval = 100;
            kittyScrollback.captureInterval = 5;
            filesystem.watchPaths = [ "~/" ];  # Monitor entire home directory
            dbus.extractAll = true;  # All D-Bus events
            asciinema.autoRecord = true;  # Auto-record all sessions
          };
          healthCheck = {
            interval = 5;
            timeout = 2;
          };
          dlq.cleanup = {
            maxAge = "90d";  # Very long retention
            maxFiles = 100000;
          };
        };
        database = {
          connectionPool.maxConnections = 100;
          healthCheck.interval = 10;
        };
        promoWorker = {
          pollInterval = 1;
          batchSize = 1000;
        };
        blobStorage = {
          healthCheck = {
            interval = 900;  # 15 minutes
            wantedSize = "1T";  # Large storage
          };
          maintenance = {
            gcSchedule = "daily";
            fsckSchedule = "weekly";
          };
        };
        monitoring = {
          logging.level = "debug";
          prometheus.enable = true;
          alerting.enable = true;
          observabilityStack.enable = true;
          dashboards.grafana.enable = true;
        };
      })
    ];

    # System integration (simplified from original)
    systemd.services = {
      sinex-unified-collector = {
        description = "Sinex Unified Event Collector";
        wantedBy = [ "multi-user.target" ];
        after = [ "postgresql.service" "network-online.target" ];
        wants = [ "network-online.target" ];
        requires = [ "postgresql.service" ];
        
        serviceConfig = {
          Type = "notify";  # Changed to notify for proper startup coordination
          User = cfg.database.user;
          Group = cfg.database.user;
          
          # Pre-start validation and migration
          ExecStartPre = mkIf (cfg.database.autoSetup && cfg.database.migration.enable) (
            pkgs.writeShellScript "sinex-collector-pre-start" ''
              set -euo pipefail
              
              echo "Preparing Sinex collector startup..."
              
              # Setup database URL
              export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
              
              # Wait for PostgreSQL
              echo "Waiting for PostgreSQL..."
              for i in {1..30}; do
                if ${pkgs.postgresql}/bin/pg_isready -h ${cfg.database.host} -p ${toString cfg.database.port} -U ${cfg.database.user} -d ${cfg.database.name}; then
                  break
                fi
                sleep 1
              done
              
              # Ensure extensions exist
              echo "Ensuring database extensions..."
              ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";" || true
              ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" || true
              ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;" || true
              ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pgx_ulid;" || true
              
              # Check current schema version
              CURRENT_VERSION=$(${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "SELECT version FROM _sqlx_migrations ORDER BY version DESC LIMIT 1;" 2>/dev/null || echo "0")
              echo "Current schema version: $CURRENT_VERSION"
              
              # Run migrations
              if [ -d "${cfg.database.migration.directory}" ]; then
                echo "Running database migrations..."
                if ! ${cfg.database.migration.package}/bin/sqlx migrate run --source "${cfg.database.migration.directory}"; then
                  echo "ERROR: Database migration failed!" >&2
                  exit 1
                fi
                
                # Verify new version
                NEW_VERSION=$(${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "SELECT version FROM _sqlx_migrations ORDER BY version DESC LIMIT 1;" 2>/dev/null || echo "0")
                echo "New schema version: $NEW_VERSION"
              fi
              
              # Test database connectivity with actual query
              echo "Testing database connectivity..."
              if ! ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "SELECT 1 FROM pg_tables WHERE schemaname = 'raw' LIMIT 1;" >/dev/null 2>&1; then
                echo "ERROR: Database schema validation failed!" >&2
                exit 1
              fi
              
              echo "Pre-start validation completed successfully"
            ''
          );
          
          # Post-start health check
          ExecStartPost = pkgs.writeShellScript "sinex-collector-post-start" ''
            set -euo pipefail
            
            echo "Validating Sinex collector startup..."
            
            # Wait for service to be ready (systemd notify)
            for i in {1..30}; do
              if systemctl show -p SubState --value sinex-unified-collector | grep -q "running"; then
                echo "✓ Service is running"
                break
              fi
              if [ $i -eq 30 ]; then
                echo "ERROR: Service failed to reach running state" >&2
                exit 1
              fi
              sleep 1
            done
            
            # Check database connectivity from the service
            export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            
            # Verify we can insert a test event
            TEST_ID=$(${pkgs.util-linux}/bin/uuidgen)
            if ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "
              INSERT INTO raw.events (id, source, event_type, ts_ingest, ts_orig, host, payload) 
              VALUES ('$TEST_ID', 'sinex.health', 'startup.test', NOW(), NOW(), '$(hostname)', '{\"test\": true}'::jsonb);
              DELETE FROM raw.events WHERE id = '$TEST_ID';
            " >/dev/null 2>&1; then
              echo "✓ Database write test passed"
            else
              echo "ERROR: Database write test failed!" >&2
              exit 1
            fi
            
            # Wait for heartbeat to appear
            echo "Waiting for heartbeat..."
            for i in {1..10}; do
              if ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "
                SELECT COUNT(*) FROM component_heartbeats 
                WHERE component_name = 'unified-collector' 
                AND timestamp > NOW() - INTERVAL '1 minute'
              " | grep -q "[1-9]"; then
                echo "✓ Heartbeat detected"
                break
              fi
              if [ $i -eq 10 ]; then
                echo "WARNING: No heartbeat detected (non-fatal)" >&2
              fi
              sleep 1
            done
            
            echo "Collector startup validation completed successfully"
          '';
          
          # Restart policy with rate limiting
          Restart = cfg.unifiedCollector.restart.policy;
          RestartSec = cfg.unifiedCollector.restart.baseDelay;
          StartLimitIntervalSec = 300;  # 5 minutes
          StartLimitBurst = 3;
          
          # Graceful shutdown
          KillMode = "mixed";
          KillSignal = "SIGTERM";
          TimeoutStopSec = 30;  # Give time for graceful shutdown
          
          # Resource limits
          MemoryMax = "1G";
          CPUQuota = "200%";
          TasksMax = 1000;
          IOWeight = 100;
          
          # Security hardening
          PrivateTmp = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          NoNewPrivileges = true;
          RestrictSUIDSGID = true;
          RemoveIPC = true;
          ProtectKernelTunables = true;
          ProtectControlGroups = true;
          RestrictRealtime = true;
          LockPersonality = true;
          SystemCallFilter = [ "@system-service" "~@privileged" ];
          
          # Allow writes to DLQ and logs
          ReadWritePaths = lib.optionals cfg.unifiedCollector.dlq.enable [
            cfg.unifiedCollector.dlq.failureStoragePath
          ] ++ [
            cfg.directories.state
            cfg.directories.logs
          ];
          
          ExecStart = "${cfg.package}/bin/sinex-collector --config ${collectorConfigFile}";
          
          # Environment variables (use agenix for DATABASE_URL if needed)
          Environment = [
            "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            "RUST_LOG=${cfg.unifiedCollector.logLevel}"
          ] ++ lib.optionals cfg.unifiedCollector.dlq.enable [
            "SINEX_DLQ_BASE=${cfg.unifiedCollector.dlq.failureStoragePath}"
            "SINEX_LOG_BASE=${cfg.unifiedCollector.dlq.failureStoragePath}"
          ];
        };
      };

      sinex-promo-worker = mkIf cfg.promoWorker.enable {
        description = "Sinex Promotion Worker";
        wantedBy = [ "multi-user.target" ];
        after = [ "postgresql.service" "sinex-unified-collector.service" ];
        requires = [ "postgresql.service" ];
        
        serviceConfig = {
          Type = "notify";
          User = cfg.database.user;
          Group = cfg.database.user;
          
          # Pre-start validation
          ExecStartPre = pkgs.writeShellScript "sinex-worker-pre-start" ''
            set -euo pipefail
            
            echo "Preparing Sinex worker startup..."
            
            # Setup database URL
            export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            
            # Wait for PostgreSQL and verify schema
            echo "Verifying database schema..."
            if ! ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "SELECT 1 FROM pg_tables WHERE schemaname = 'sinex_schemas' AND tablename = 'promotion_queue' LIMIT 1;" >/dev/null 2>&1; then
              echo "ERROR: Promotion queue table not found!" >&2
              exit 1
            fi
            
            echo "Pre-start validation completed successfully"
          '';
          
          # Restart policy with rate limiting
          Restart = "on-failure";
          RestartSec = "5s";
          StartLimitIntervalSec = "60s";
          StartLimitBurst = 3;
          
          # Resource limits
          MemoryMax = "512M";
          CPUQuota = "100%";
          TasksMax = 500;
          IOWeight = 100;
          
          # Security hardening
          PrivateTmp = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          NoNewPrivileges = true;
          RestrictSUIDSGID = true;
          RemoveIPC = true;
          ProtectKernelTunables = true;
          ProtectControlGroups = true;
          RestrictRealtime = true;
          LockPersonality = true;
          SystemCallFilter = [ "@system-service" "~@privileged" ];
          
          # Database access only, no file writes needed for promo worker
          ReadWritePaths = [ ];
          
          ExecStart = "${cfg.package}/bin/sinex-promo-worker --agent-name=default-worker";
          
          # Environment variables (use agenix for DATABASE_URL if needed)  
          Environment = [
            "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            "RUST_LOG=${cfg.unifiedCollector.logLevel}"
            "POLL_INTERVAL=${toString cfg.promoWorker.pollInterval}"
            "BATCH_SIZE=${toString cfg.promoWorker.batchSize}"
          ];
        };
      };

      # DLQ cleanup service
      sinex-dlq-cleanup = mkIf (cfg.unifiedCollector.dlq.enable && cfg.unifiedCollector.dlq.cleanup.enable) {
        description = "Sinex DLQ Cleanup";
        serviceConfig = {
          Type = "oneshot";
          User = cfg.database.user;
          Group = cfg.database.user;
          
          # Resource limits for oneshot service
          MemoryMax = "256M";
          TasksMax = 50;
          IOWeight = 50;
          
          # Security hardening
          PrivateTmp = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          NoNewPrivileges = true;
          RestrictSUIDSGID = true;
          RemoveIPC = true;
          ProtectKernelTunables = true;
          ProtectControlGroups = true;
          RestrictRealtime = true;
          LockPersonality = true;
          SystemCallFilter = [ "@system-service" "~@privileged" ];
          
          # Only allow writes to DLQ directory
          ReadWritePaths = [ cfg.unifiedCollector.dlq.failureStoragePath ];
          
          ExecStart = pkgs.writeShellScript "sinex-dlq-cleanup" ''
            set -euo pipefail
            
            DLQ_PATH="${cfg.unifiedCollector.dlq.failureStoragePath}"
            MAX_AGE="${cfg.unifiedCollector.dlq.cleanup.maxAge}"
            MAX_FILES="${toString cfg.unifiedCollector.dlq.cleanup.maxFiles}"
            
            echo "$(date): Starting DLQ cleanup..."
            
            # Clean by age
            if [ -d "$DLQ_PATH" ]; then
              find "$DLQ_PATH" -name "*.json" -type f -mtime +''${MAX_AGE%d} -delete 2>/dev/null || true
              echo "Cleaned DLQ files older than $MAX_AGE"
              
              # Clean by count (keep newest files)
              FILE_COUNT=$(find "$DLQ_PATH" -name "*.json" -type f | wc -l)
              if [ "$FILE_COUNT" -gt "$MAX_FILES" ]; then
                EXCESS=$((FILE_COUNT - MAX_FILES))
                find "$DLQ_PATH" -name "*.json" -type f -printf '%T@ %p\n' | sort -n | head -n "$EXCESS" | cut -d' ' -f2- | xargs -r rm
                echo "Cleaned $EXCESS excess DLQ files (kept $MAX_FILES newest)"
              fi
            fi
            
            echo "$(date): DLQ cleanup completed"
          '';
        };
      };
    };

    # DLQ cleanup timer
    systemd.timers.sinex-dlq-cleanup = mkIf (cfg.unifiedCollector.dlq.enable && cfg.unifiedCollector.dlq.cleanup.enable) {
      description = "Sinex DLQ Cleanup Timer";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = "daily";
        Persistent = true;
        RandomizedDelaySec = "1h";
      };
    };

    # Coordinated update service
    systemd.services.sinex-update = mkIf cfg.update.enable {
      description = "Sinex Coordinated Update";
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        
        ExecStart = pkgs.writeShellScript "sinex-update" ''
          set -euo pipefail
          
          echo "$(date): Starting Sinex coordinated update..."
          
          # Function to check service health
          check_health() {
            local service=$1
            
            # Check if service is active
            if ! systemctl is-active "$service" >/dev/null 2>&1; then
              return 1
            fi
            
            # Check for recent heartbeats (if database is available)
            if systemctl is-active postgresql >/dev/null 2>&1; then
              export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
              
              local component_name=""
              case "$service" in
                sinex-unified-collector) component_name="unified-collector" ;;
                sinex-promo-worker) component_name="default-worker" ;;
              esac
              
              if [ -n "$component_name" ]; then
                local heartbeat_count=$(${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "
                  SELECT COUNT(*) FROM component_heartbeats 
                  WHERE component_name = '$component_name' 
                  AND timestamp > NOW() - INTERVAL '2 minutes'
                  AND status != 'failed'
                " 2>/dev/null || echo "0")
                
                if [ "$heartbeat_count" -eq 0 ]; then
                  echo "WARNING: No recent healthy heartbeats for $component_name"
                  return 1
                fi
              fi
            fi
            
            return 0
          }
          
          # Save current state
          echo "Saving current state..."
          COLLECTOR_WAS_ACTIVE=false
          WORKER_WAS_ACTIVE=false
          
          if systemctl is-active sinex-unified-collector >/dev/null 2>&1; then
            COLLECTOR_WAS_ACTIVE=true
          fi
          
          if systemctl is-active sinex-promo-worker >/dev/null 2>&1; then
            WORKER_WAS_ACTIVE=true
          fi
          
          # Preserve data if requested
          if [ "${toString cfg.update.preserveData}" = "1" ] && [ -d "${cfg.unifiedCollector.dlq.failureStoragePath}" ]; then
            echo "Preserving DLQ data..."
            BACKUP_DIR="${cfg.unifiedCollector.dlq.failureStoragePath}.backup-$(date +%Y%m%d-%H%M%S)"
            cp -a "${cfg.unifiedCollector.dlq.failureStoragePath}" "$BACKUP_DIR" || true
          fi
          
          # Graceful shutdown
          echo "Initiating graceful shutdown..."
          
          # Stop worker first (processes events)
          if [ "$WORKER_WAS_ACTIVE" = "true" ]; then
            echo "Stopping promotion worker..."
            systemctl stop sinex-promo-worker
          fi
          
          # Give worker time to finish processing
          sleep 5
          
          # Stop collector
          if [ "$COLLECTOR_WAS_ACTIVE" = "true" ]; then
            echo "Stopping collector with ${cfg.update.gracePeriod}s grace period..."
            systemctl stop sinex-unified-collector
            
            # Wait for graceful shutdown
            sleep ${toString cfg.update.gracePeriod}
          fi
          
          # Perform updates (migrations were already run in ExecStartPre)
          echo "Updates applied via service ExecStartPre hooks"
          
          # Restart services in order
          echo "Starting services..."
          
          if [ "$COLLECTOR_WAS_ACTIVE" = "true" ]; then
            echo "Starting collector..."
            if ! systemctl start sinex-unified-collector; then
              echo "ERROR: Failed to start collector!" >&2
              exit 1
            fi
            
            # Wait for collector to be ready
            sleep 5
          fi
          
          if [ "$WORKER_WAS_ACTIVE" = "true" ]; then
            echo "Starting promotion worker..."
            if ! systemctl start sinex-promo-worker; then
              echo "ERROR: Failed to start worker!" >&2
              # Stop collector if worker fails
              [ "$COLLECTOR_WAS_ACTIVE" = "true" ] && systemctl stop sinex-unified-collector
              exit 1
            fi
          fi
          
          # Health check with timeout
          echo "Performing health checks (timeout: ${toString cfg.update.healthCheckTimeout}s)..."
          
          HEALTH_CHECK_PASSED=true
          START_TIME=$(date +%s)
          
          while true; do
            CURRENT_TIME=$(date +%s)
            ELAPSED=$((CURRENT_TIME - START_TIME))
            
            if [ $ELAPSED -gt ${toString cfg.update.healthCheckTimeout} ]; then
              echo "ERROR: Health check timeout exceeded!" >&2
              HEALTH_CHECK_PASSED=false
              break
            fi
            
            ALL_HEALTHY=true
            
            if [ "$COLLECTOR_WAS_ACTIVE" = "true" ] && ! check_health sinex-unified-collector; then
              ALL_HEALTHY=false
            fi
            
            if [ "$WORKER_WAS_ACTIVE" = "true" ] && ! check_health sinex-promo-worker; then
              ALL_HEALTHY=false
            fi
            
            if [ "$ALL_HEALTHY" = "true" ]; then
              echo "✓ All services healthy"
              break
            fi
            
            echo "Waiting for services to become healthy... ($ELAPSED/${toString cfg.update.healthCheckTimeout}s)"
            sleep 5
          done
          
          # Handle rollback if needed
          if [ "$HEALTH_CHECK_PASSED" = "false" ] && [ "${toString cfg.update.rollbackOnFailure}" = "1" ]; then
            echo "Initiating rollback due to health check failure..."
            
            # Stop failed services
            [ "$WORKER_WAS_ACTIVE" = "true" ] && systemctl stop sinex-promo-worker || true
            [ "$COLLECTOR_WAS_ACTIVE" = "true" ] && systemctl stop sinex-unified-collector || true
            
            # Restore data if preserved
            if [ -n "''${BACKUP_DIR:-}" ] && [ -d "$BACKUP_DIR" ]; then
              echo "Restoring DLQ data..."
              rm -rf "${cfg.unifiedCollector.dlq.failureStoragePath}"
              mv "$BACKUP_DIR" "${cfg.unifiedCollector.dlq.failureStoragePath}"
            fi
            
            echo "ERROR: Update failed and was rolled back" >&2
            exit 1
          fi
          
          # Cleanup backup if successful
          if [ -n "''${BACKUP_DIR:-}" ] && [ -d "$BACKUP_DIR" ]; then
            echo "Cleaning up backup..."
            rm -rf "$BACKUP_DIR"
          fi
          
          echo "$(date): Sinex update completed successfully"
        '';
      };
    };


    # User and group creation
    users.users.${cfg.database.user} = {
      isSystemUser = true;
      group = cfg.database.user;
      description = "Sinex system user";
      home = "/var/lib/${cfg.database.user}";
      createHome = true;
    };

    users.groups.${cfg.database.user} = {};

    # Directory setup and configuration
    systemd.tmpfiles.rules = [
      # Basic directories for monitoring.nix compatibility  
      "d ${cfg.directories.state} 0755 ${cfg.database.user} ${cfg.database.user} -"
      "d ${cfg.directories.logs} 0755 ${cfg.database.user} ${cfg.database.user} -"
      # Configuration directory
      "d /etc/sinex 0755 root root -"
    ] ++ lib.optionals cfg.unifiedCollector.dlq.enable [
      # DLQ failure storage directory
      "d ${cfg.unifiedCollector.dlq.failureStoragePath} 0755 ${cfg.database.user} ${cfg.database.user} -"
    ];
    
    # Place generated configuration file in standard location
    environment.etc."sinex/collector.toml".source = collectorConfigFile;

    # Database setup (if enabled)
    services.postgresql = mkIf cfg.database.autoSetup {
      enable = true;
      ensureDatabases = [ cfg.database.name ];
      ensureUsers = [
        {
          name = cfg.database.user;
          ensureDBOwnership = true;
        }
      ];
    };

    
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

    # Assertions for configuration validation
    assertions = [
      {
        assertion = cfg.enable -> cfg.targetUser != "";
        message = "services.sinex.targetUser must be set when Sinex is enabled";
      }
      {
        assertion = cfg.monitoring.observabilityStack.enable -> cfg.database.autoSetup || config.services.postgresql.enable;
        message = "PostgreSQL must be enabled for Sinex observability stack";
      }
      {
        assertion = cfg.monitoring.dashboards.grafana.enable -> cfg.monitoring.observabilityStack.enable;
        message = "Grafana dashboards require the observability stack to be enabled";
      }
    ];
  };
}