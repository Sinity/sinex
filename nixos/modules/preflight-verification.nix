# Sinex Pre-Flight Verification Module
# Implements the Pre-Flight Verification Model for safe, zero-downtime deployments
{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  
  # Pre-flight verification configuration
  preflightCfg = cfg.preflightVerification;
  
in
{
  options.services.sinex.preflightVerification = {
    enable = mkOption {
      type = types.bool;
      default = true;
      description = ''
        Enable pre-flight verification for all Sinex service deployments.
        This ensures that new versions are thoroughly tested before activation.
      '';
    };
    
    timeout = mkOption {
      type = types.int;
      default = 120;
      description = "Timeout for complete verification process in seconds";
    };
    
    skipPhases = mkOption {
      type = types.listOf (types.enum [ 
        "database" "extensions" "migrations" "resources" 
        "configuration" "services" "integration" 
      ]);
      default = [];
      description = "Verification phases to skip (use with caution)";
    };
    
    failureAction = mkOption {
      type = types.enum [ "abort" "warn" "ignore" ];
      default = "abort";
      description = ''
        Action to take when pre-flight verification fails:
        - abort: Stop deployment (recommended)
        - warn: Continue with warnings
        - ignore: Continue anyway (dangerous)
      '';
    };
    
    recordResults = mkOption {
      type = types.bool;
      default = true;
      description = "Record verification results in database for monitoring";
    };
    
    notifications = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = "Enable notifications for verification results";
      };
      
      onFailure = mkOption {
        type = types.bool;
        default = true;
        description = "Send notifications on verification failure";
      };
      
      onSuccess = mkOption {
        type = types.bool;
        default = false;
        description = "Send notifications on verification success";
      };
    };
  };

  config = mkIf (cfg.enable && preflightCfg.enable) {
    
    # Pre-flight verification service
    systemd.services.sinex-preflight = {
      description = "Sinex Pre-Flight Verification";
      
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        TimeoutStartSec = preflightCfg.timeout;
        
        # Security hardening
        DynamicUser = true;
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
        
        # Allow reads from configuration and state directories
        ReadOnlyPaths = [
          "/etc/sinex"
          cfg.directories.state
          cfg.directories.logs
        ];
        
        ExecStart = pkgs.writeShellScript "sinex-preflight-verification" ''
          set -euo pipefail
          
          echo "$(date): Starting Sinex Pre-Flight Verification"
          
          # Environment setup
          export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
          export RUST_LOG="sinex_preflight=info"
          
          # Build verification command
          VERIFY_CMD="${cfg.package}/bin/sinex-preflight verify"
          VERIFY_CMD="$VERIFY_CMD --timeout ${toString preflightCfg.timeout}"
          VERIFY_CMD="$VERIFY_CMD --output json"
          
          # Add skip phases if specified
          ${concatMapStringsSep "\n" (phase: ''
            VERIFY_CMD="$VERIFY_CMD --skip ${phase}"
          '') preflightCfg.skipPhases}
          
          # Run verification
          echo "Executing: $VERIFY_CMD"
          
          if VERIFICATION_RESULT=$($VERIFY_CMD); then
            echo "✓ Pre-flight verification PASSED"
            echo "$VERIFICATION_RESULT" | ${pkgs.jq}/bin/jq .
            
            ${optionalString preflightCfg.notifications.enable ''
              # Send success notification if enabled
              ${optionalString preflightCfg.notifications.onSuccess ''
                echo "Sending success notification..."
                systemctl --user start sinex-notification-success.service 2>/dev/null || true
              ''}
            ''}
            
            exit 0
          else
            VERIFICATION_EXIT_CODE=$?
            echo "✗ Pre-flight verification FAILED (exit code: $VERIFICATION_EXIT_CODE)"
            
            # Still try to parse and display results
            if echo "$VERIFICATION_RESULT" | ${pkgs.jq}/bin/jq . >/dev/null 2>&1; then
              echo "$VERIFICATION_RESULT" | ${pkgs.jq}/bin/jq .
            else
              echo "Raw verification output:"
              echo "$VERIFICATION_RESULT"
            fi
            
            ${optionalString preflightCfg.notifications.enable ''
              # Send failure notification
              ${optionalString preflightCfg.notifications.onFailure ''
                echo "Sending failure notification..."
                systemctl --user start sinex-notification-failure.service 2>/dev/null || true
              ''}
            ''}
            
            # Handle failure based on configuration
            case "${preflightCfg.failureAction}" in
              "abort")
                echo "ERROR: Aborting deployment due to verification failure"
                exit $VERIFICATION_EXIT_CODE
                ;;
              "warn")
                echo "WARNING: Continuing despite verification failure"
                exit 0
                ;;
              "ignore")
                echo "INFO: Ignoring verification failure (dangerous)"
                exit 0
                ;;
            esac
          fi
        '';
        
        Environment = [
          "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
          "RUST_LOG=sinex_preflight=info"
        ];
      };
    };
    
    # Enhanced Sinex service definitions with pre-flight verification
    systemd.services.sinex-unified-collector = mkOverride 500 {
      description = "Sinex Unified Event Collector (with Pre-Flight Verification)";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" "network-online.target" "sinex-preflight.service" ];
      wants = [ "network-online.target" ];
      requires = [ "postgresql.service" "sinex-preflight.service" ];
      
      serviceConfig = {
        Type = "notify";
        User = cfg.database.user;
        Group = cfg.database.user;
        
        # Enhanced pre-start with verification dependency
        ExecStartPre = [
          # First ensure pre-flight verification has passed
          "${pkgs.writeShellScript "verify-preflight-status" ''
            set -euo pipefail
            
            echo "Checking pre-flight verification status..."
            
            if ! systemctl is-active sinex-preflight.service >/dev/null 2>&1; then
              echo "ERROR: Pre-flight verification has not run or failed"
              echo "Run: systemctl start sinex-preflight.service"
              exit 1
            fi
            
            # Check that verification completed recently (within last hour)
            if [ -f /var/lib/systemd/system/sinex-preflight.service ]; then
              LAST_SUCCESS=$(stat -c %Y /var/lib/systemd/system/sinex-preflight.service 2>/dev/null || echo 0)
              CURRENT_TIME=$(date +%s)
              TIME_DIFF=$((CURRENT_TIME - LAST_SUCCESS))
              
              if [ $TIME_DIFF -gt 3600 ]; then
                echo "WARNING: Pre-flight verification is older than 1 hour"
                echo "Consider running: systemctl start sinex-preflight.service"
              fi
            fi
            
            echo "✓ Pre-flight verification requirements met"
          ''}"
          
          # Then run existing pre-start validation
        ] ++ (lib.optionals (cfg.database.autoSetup && cfg.database.migration.enable) [
          "${pkgs.writeShellScript "sinex-collector-pre-start" ''
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
            
            # Ensure extensions exist (these were pre-verified by preflight)
            echo "Ensuring database extensions..."
            ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";" || true
            ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS timescaledb;" || true
            ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pg_jsonschema;" || true
            ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "CREATE EXTENSION IF NOT EXISTS pgx_ulid;" || true
            
            # Check current schema version
            CURRENT_VERSION=$(${pkgs.postgresql}/bin/psql "$DATABASE_URL" -t -c "SELECT version FROM _sqlx_migrations ORDER BY version DESC LIMIT 1;" 2>/dev/null || echo "0")
            echo "Current schema version: $CURRENT_VERSION"
            
            # Run migrations (these were pre-verified by preflight)
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
          ''}"
        ]);
        
        # Enhanced post-start with verification integration
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
          
          ${optionalString preflightCfg.recordResults ''
            # Record successful deployment
            ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "
              INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen)
              VALUES ('sinex-deployment', '$(hostname)-$(date +%s)', 'success', 
                     '{\"event\": \"collector_started\", \"verification\": \"passed\"}', NOW())
              ON CONFLICT (component_name, instance_id) 
              DO UPDATE SET status = EXCLUDED.status, metadata = EXCLUDED.metadata, last_seen = EXCLUDED.last_seen
            " || true
          ''}
        '';
        
        # Copy existing service configuration
        Restart = cfg.unifiedCollector.restart.policy;
        RestartSec = cfg.unifiedCollector.restart.baseDelay;
        StartLimitIntervalSec = 300;
        StartLimitBurst = 3;
        
        # Resource and security settings
        KillMode = "mixed";
        KillSignal = "SIGTERM";
        TimeoutStopSec = 30;
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
        
        ReadWritePaths = lib.optionals cfg.unifiedCollector.dlq.enable [
          cfg.unifiedCollector.dlq.failureStoragePath
        ] ++ [
          cfg.directories.state
          cfg.directories.logs
        ];
        
        ExecStart = "${cfg.package}/bin/sinex-collector --config ${import ../config-gen.nix { inherit lib pkgs; } .mkCollectorConfigFile cfg.unifiedCollector cfg}";
        
        Environment = [
          "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
          "RUST_LOG=${cfg.unifiedCollector.logLevel}"
        ] ++ lib.optionals cfg.unifiedCollector.dlq.enable [
          "SINEX_DLQ_BASE=${cfg.unifiedCollector.dlq.failureStoragePath}"
          "SINEX_LOG_BASE=${cfg.unifiedCollector.dlq.failureStoragePath}"
        ];
      };
    };
    
    # Enhanced promotion worker with pre-flight verification
    systemd.services.sinex-promo-worker = mkIf cfg.promoWorker.enable (mkOverride 500 {
      description = "Sinex Promotion Worker (with Pre-Flight Verification)";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" "sinex-unified-collector.service" "sinex-preflight.service" ];
      requires = [ "postgresql.service" "sinex-preflight.service" ];
      
      serviceConfig = {
        Type = "notify";
        User = cfg.database.user;
        Group = cfg.database.user;
        
        ExecStartPre = [
          # Verify pre-flight status
          "${pkgs.writeShellScript "verify-worker-preflight" ''
            set -euo pipefail
            
            echo "Checking pre-flight verification for worker..."
            
            if ! systemctl is-active sinex-preflight.service >/dev/null 2>&1; then
              echo "ERROR: Pre-flight verification required for worker startup"
              exit 1
            fi
            
            echo "✓ Pre-flight verification confirmed for worker"
          ''}"
          
          # Existing pre-start validation
          "${pkgs.writeShellScript "sinex-worker-pre-start" ''
            set -euo pipefail
            
            echo "Preparing Sinex worker startup..."
            
            export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            
            echo "Verifying database schema..."
            if ! ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "SELECT 1 FROM pg_tables WHERE schemaname = 'sinex_schemas' AND tablename = 'promotion_queue' LIMIT 1;" >/dev/null 2>&1; then
              echo "ERROR: Promotion queue table not found!" >&2
              exit 1
            fi
            
            echo "Pre-start validation completed successfully"
          ''}"
        ];
        
        # Copy existing worker configuration
        Restart = "on-failure";
        RestartSec = "5s";
        StartLimitIntervalSec = "60s";
        StartLimitBurst = 3;
        
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
        
        ReadWritePaths = [ ];
        
        ExecStart = "${cfg.package}/bin/sinex-promo-worker --agent-name=default-worker";
        
        Environment = [
          "DATABASE_URL=postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
          "RUST_LOG=${cfg.unifiedCollector.logLevel}"
          "POLL_INTERVAL=${toString cfg.promoWorker.pollInterval}"
          "BATCH_SIZE=${toString cfg.promoWorker.batchSize}"
        ];
      };
    });
    
    # Enhanced update service with pre-flight verification
    systemd.services.sinex-update = mkIf cfg.update.enable (mkOverride 500 {
      description = "Sinex Coordinated Update (with Pre-Flight Verification)";
      
      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        
        ExecStart = pkgs.writeShellScript "sinex-update-with-preflight" ''
          set -euo pipefail
          
          echo "$(date): Starting Sinex coordinated update with pre-flight verification..."
          
          # Phase 1: Pre-flight verification
          echo "Phase 1: Running pre-flight verification..."
          
          if ! systemctl start sinex-preflight.service; then
            echo "ERROR: Pre-flight verification failed!" >&2
            exit 1
          fi
          
          echo "✓ Pre-flight verification passed"
          
          # Phase 2: Execute existing update logic with enhanced checks
          echo "Phase 2: Proceeding with service updates..."
          
          # Enhanced health check function
          check_health() {
            local service=$1
            
            # Check if service is active
            if ! systemctl is-active "$service" >/dev/null 2>&1; then
              return 1
            fi
            
            # Check for recent heartbeats
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
          
          if [ "$WORKER_WAS_ACTIVE" = "true" ]; then
            echo "Stopping promotion worker..."
            systemctl stop sinex-promo-worker
          fi
          
          sleep 5
          
          if [ "$COLLECTOR_WAS_ACTIVE" = "true" ]; then
            echo "Stopping collector with ${toString cfg.update.gracePeriod}s grace period..."
            systemctl stop sinex-unified-collector
            sleep ${toString cfg.update.gracePeriod}
          fi
          
          # Restart services in order (pre-flight verification already passed)
          echo "Starting services..."
          
          if [ "$COLLECTOR_WAS_ACTIVE" = "true" ]; then
            echo "Starting collector..."
            if ! systemctl start sinex-unified-collector; then
              echo "ERROR: Failed to start collector!" >&2
              exit 1
            fi
            sleep 5
          fi
          
          if [ "$WORKER_WAS_ACTIVE" = "true" ]; then
            echo "Starting promotion worker..."
            if ! systemctl start sinex-promo-worker; then
              echo "ERROR: Failed to start worker!" >&2
              [ "$COLLECTOR_WAS_ACTIVE" = "true" ] && systemctl stop sinex-unified-collector
              exit 1
            fi
          fi
          
          # Enhanced health check with timeout
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
          
          # Handle rollback with enhanced verification
          if [ "$HEALTH_CHECK_PASSED" = "false" ] && [ "${toString cfg.update.rollbackOnFailure}" = "true" ]; then
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
            
            ${optionalString preflightCfg.recordResults ''
              # Record rollback event
              export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
              ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "
                INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen)
                VALUES ('sinex-deployment', '$(hostname)-$(date +%s)', 'rollback', 
                       '{\"event\": \"update_rollback\", \"reason\": \"health_check_failure\"}', NOW())
              " || true
            ''}
            
            echo "ERROR: Update failed and was rolled back" >&2
            exit 1
          fi
          
          # Cleanup backup if successful
          if [ -n "''${BACKUP_DIR:-}" ] && [ -d "$BACKUP_DIR" ]; then
            echo "Cleaning up backup..."
            rm -rf "$BACKUP_DIR"
          fi
          
          ${optionalString preflightCfg.recordResults ''
            # Record successful update
            export DATABASE_URL="postgresql://${cfg.database.user}@${cfg.database.host}:${toString cfg.database.port}/${cfg.database.name}"
            ${pkgs.postgresql}/bin/psql "$DATABASE_URL" -c "
              INSERT INTO component_heartbeats (component_name, instance_id, status, metadata, last_seen)
              VALUES ('sinex-deployment', '$(hostname)-$(date +%s)', 'success', 
                     '{\"event\": \"update_completed\", \"verification\": \"passed\"}', NOW())
            " || true
          ''}
          
          echo "$(date): Sinex update completed successfully with pre-flight verification"
        '';
      };
    });
    
    # Optional notification services
    systemd.services.sinex-notification-success = mkIf preflightCfg.notifications.enable {
      description = "Sinex Pre-Flight Success Notification";
      serviceConfig = {
        Type = "oneshot";
        ExecStart = pkgs.writeShellScript "notify-success" ''
          # Placeholder for success notification
          # Could integrate with systemd-notify, email, webhook, etc.
          logger "Sinex pre-flight verification succeeded"
        '';
      };
    };
    
    systemd.services.sinex-notification-failure = mkIf preflightCfg.notifications.enable {
      description = "Sinex Pre-Flight Failure Notification";
      serviceConfig = {
        Type = "oneshot";
        ExecStart = pkgs.writeShellScript "notify-failure" ''
          # Placeholder for failure notification
          # Could integrate with systemd-notify, email, webhook, etc.
          logger "Sinex pre-flight verification FAILED - deployment may be unsafe"
        '';
      };
    };
    
    # Assertions for pre-flight verification
    assertions = [
      {
        assertion = preflightCfg.enable -> (cfg.database.autoSetup || config.services.postgresql.enable);
        message = "Pre-flight verification requires PostgreSQL to be enabled";
      }
      {
        assertion = preflightCfg.enable -> cfg.enable;
        message = "Pre-flight verification requires Sinex to be enabled";
      }
      {
        assertion = preflightCfg.recordResults -> (cfg.database.autoSetup || config.services.postgresql.enable);
        message = "Recording verification results requires database access";
      }
    ];
  };
}