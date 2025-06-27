# Simplified Monitoring Module with Rich Defaults
# Replaces the over-engineered monitoring.nix with a preset-based approach
{
  lib,
  config,
  pkgs,
  ...
}:

with lib;

let
  cfg = config.services.sinex;

  # Observability presets with rich defaults
  observabilityPresets = {
    minimal = {
      prometheus = false;
      grafana = false;
      alerts = false;
      logging = "warn";
      retention = "7d";
      dashboards = [];
    };
    
    standard = {
      prometheus = true;
      grafana = true;
      alerts = true;
      logging = "info";
      retention = "30d";
      dashboards = [ "overview" "events" "system" ];
    };
    
    comprehensive = {
      prometheus = true;
      grafana = true;
      alerts = true;
      logging = "debug";
      retention = "90d";
      dashboards = [ "overview" "events" "system" "pipeline" "performance" "analytics" "troubleshooting" ];
    };
  };

  # Get preset configuration
  preset = observabilityPresets.${cfg.observability.level};

in
{
  options.services.sinex.observability = {
    level = mkOption {
      type = types.enum [ "minimal" "standard" "comprehensive" ];
      default = "standard";
      description = ''
        Observability level with rich defaults:
        - minimal: Basic health checks only, minimal logging, 7d retention
        - standard: Grafana + essential metrics + structured logging + key alerts, 30d retention  
        - comprehensive: Full stack + debug logging + all alerts + extended dashboards, 90d retention
      '';
    };

    retention = mkOption {
      type = types.str;
      default = preset.retention;
      description = "Data retention period (overrides preset default)";
    };

    customDashboards = mkOption {
      type = types.listOf types.str;
      default = [];
      description = "Additional dashboards beyond preset defaults";
    };

    # Advanced overrides (rarely needed)
    advanced = {
      prometheusPort = mkOption {
        type = types.port;
        default = 9090;
        description = "Prometheus port (only change if conflicts)";
      };

      grafanaPort = mkOption {
        type = types.port;
        default = 3000;
        description = "Grafana port (only change if conflicts)";
      };

      listenAddress = mkOption {
        type = types.str;
        default = "127.0.0.1";
        description = "Listen address (localhost only for security)";
      };

      alertThresholds = mkOption {
        type = types.attrs;
        default = {
          memory = 0.9;      # 90% memory usage
          cpu = 0.8;         # 80% CPU usage
          disk = 0.85;       # 85% disk usage
          errorRate = 0.05;  # 5% error rate
        };
        description = "Alert thresholds (only override if defaults don't work)";
      };
    };
  };

  config = mkIf cfg.enable {
    # Prometheus configuration (only if enabled in preset)
    services.prometheus = mkIf preset.prometheus {
      enable = true;
      listenAddress = cfg.observability.advanced.listenAddress;
      port = cfg.observability.advanced.prometheusPort;
      retentionTime = cfg.observability.retention;

      # Smart scrape configuration - auto-discovers Sinex services
      scrapeConfigs = [
        {
          job_name = "sinex-metrics";
          static_configs = [
            {
              targets = [ 
                "${cfg.observability.advanced.listenAddress}:2112"  # Standard metrics port
              ];
            }
          ];
          scrape_interval = "15s";
          metrics_path = "/metrics";
        }
        {
          job_name = "node_exporter";
          static_configs = [ 
            { targets = [ "${cfg.observability.advanced.listenAddress}:9100" ]; } 
          ];
        }
        {
          job_name = "postgres_exporter";
          static_configs = [ 
            { targets = [ "${cfg.observability.advanced.listenAddress}:9187" ]; } 
          ];
        }
      ];

      exporters = {
        node = {
          enable = true;
          listenAddress = cfg.observability.advanced.listenAddress;
          port = 9100;
          enabledCollectors = [ "systemd" "processes" "filesystem" "meminfo" "loadavg" ];
        };

        postgres = {
          enable = true;
          listenAddress = cfg.observability.advanced.listenAddress;
          port = 9187;
          runAsLocalSuperUser = true;
        };
      };

      # Smart alerting rules based on preset
      rules = mkIf preset.alerts [
        (pkgs.writeText "sinex-alerts.yaml" (builtins.toJSON {
          groups = [
            {
              name = "sinex.critical";
              rules = [
                {
                  alert = "SinexServiceDown";
                  expr = "up{job=\"sinex-metrics\"} == 0";
                  for = "2m";
                  labels.severity = "critical";
                  annotations = {
                    summary = "Sinex service is down";
                    description = "{{ $labels.instance }} has been down for more than 2 minutes";
                  };
                }
                {
                  alert = "SinexHighMemoryUsage";
                  expr = "node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes < ${toString (1 - cfg.observability.advanced.alertThresholds.memory)}";
                  for = "5m";
                  labels.severity = "warning";
                  annotations = {
                    summary = "High memory usage detected";
                    description = "Memory usage is above ${toString (cfg.observability.advanced.alertThresholds.memory * 100)}%";
                  };
                }
                {
                  alert = "SinexHighCPUUsage";
                  expr = "100 - (avg by(instance) (rate(node_cpu_seconds_total{mode=\"idle\"}[5m])) * 100) > ${toString (cfg.observability.advanced.alertThresholds.cpu * 100)}";
                  for = "10m";
                  labels.severity = "warning";
                  annotations = {
                    summary = "High CPU usage detected";
                    description = "CPU usage is above ${toString (cfg.observability.advanced.alertThresholds.cpu * 100)}% for 10 minutes";
                  };
                }
                {
                  alert = "SinexDiskSpaceLow";
                  expr = "node_filesystem_avail_bytes{mountpoint=\"/\"} / node_filesystem_size_bytes{mountpoint=\"/\"} < ${toString (1 - cfg.observability.advanced.alertThresholds.disk)}";
                  for = "5m";
                  labels.severity = "critical";
                  annotations = {
                    summary = "Disk space critically low";
                    description = "Less than ${toString ((1 - cfg.observability.advanced.alertThresholds.disk) * 100)}% disk space remaining";
                  };
                }
              ];
            }
          ];
        }))
      ];
    };

    # Grafana configuration (only if enabled in preset)
    services.grafana = mkIf preset.grafana {
      enable = true;
      settings = {
        server = {
          http_addr = cfg.observability.advanced.listenAddress;
          http_port = cfg.observability.advanced.grafanaPort;
          domain = "localhost";
        };
        
        # Rich defaults for immediate productivity
        "auth.anonymous" = {
          enabled = true;
          org_name = "Sinex Exocortex";
          org_role = "Admin";  # Local-only access, admin for convenience
        };
        
        users = {
          allow_sign_up = false;
          auto_assign_org = true;
          auto_assign_org_role = "Viewer";
          default_theme = "dark";
        };
        
        ui.default_theme = "dark";
        database.wal = true;  # Better performance
        
        # Enable modern features
        feature_toggles.enable = "ngalert";
        
        # Smart session management  
        session = {
          cookie_secure = false;  # localhost only
          cookie_samesite = "lax";
          session_life_time = 86400;  # 24 hours
        };
      };

      provision = {
        enable = true;
        
        # Auto-configure data sources
        datasources.settings.datasources = [
          {
            name = "Sinex-PostgreSQL";
            type = "postgres";
            access = "proxy";
            url = "postgresql:///${cfg.database.name}?host=/run/postgresql";
            isDefault = true;
            jsonData = {
              sslmode = "disable";
              postgresVersion = 1600;
              timescaledb = true;  # Enable TimescaleDB features
            };
          }
          {
            name = "Sinex-Prometheus";
            type = "prometheus";
            access = "proxy";
            url = "http://${cfg.observability.advanced.listenAddress}:${toString cfg.observability.advanced.prometheusPort}";
            isDefault = false;
            jsonData = {
              httpMethod = "POST";
              prometheusType = "Prometheus";
            };
          }
        ];

        # Auto-provision dashboards based on preset + custom
        dashboards.settings.providers = [
          {
            name = "Sinex Dashboards";
            orgId = 1;
            folder = "Sinex";
            type = "file";
            disableDeletion = false;
            updateIntervalSeconds = 30;
            allowUiUpdates = true;
            options.path = "/var/lib/grafana/dashboards";
          }
        ];
      };
    };

    # Smart logging configuration
    services.journald.extraConfig = mkIf (preset.logging != "warn") ''
      SystemMaxUse=1G
      SystemMaxFileSize=100M
      SystemMaxFiles=10
      MaxRetentionSec=${
        if preset.logging == "debug" then "7d"
        else if preset.logging == "info" then "30d"  
        else "3d"
      }
    '';

    # Automatic dashboard provisioning based on preset
    systemd.tmpfiles.rules = mkIf preset.grafana (
      let
        enabledDashboards = preset.dashboards ++ cfg.observability.customDashboards;
        dashboardMappings = {
          overview = "sinex-overview.json";
          events = "sinex-event-analysis.json";
          system = "system-health.json";
          pipeline = "event-pipeline.json";
          performance = "worker-performance.json";
          analytics = "metrics-continuous-aggregates.json";
          troubleshooting = "troubleshooting.json";
        };
      in
      [ "d /var/lib/grafana/dashboards 0755 grafana grafana" ] ++
      (map (dashboard: 
        "L+ /var/lib/grafana/dashboards/${dashboardMappings.${dashboard}} - - - - ${../grafana-dashboards}/${dashboardMappings.${dashboard}}"
      ) enabledDashboards)
    );

    # Convenient monitoring scripts (installed based on preset)
    environment.systemPackages = mkIf preset.prometheus [
      (pkgs.writeShellScriptBin "sinex-status" ''
        echo "🔍 Sinex Observability Status"
        echo "Level: ${cfg.observability.level}"
        echo "Retention: ${cfg.observability.retention}"
        echo ""
        
        ${optionalString preset.prometheus ''
          echo "📊 Prometheus: http://${cfg.observability.advanced.listenAddress}:${toString cfg.observability.advanced.prometheusPort}"
          curl -s "http://${cfg.observability.advanced.listenAddress}:${toString cfg.observability.advanced.prometheusPort}/-/healthy" >/dev/null && echo "✅ Prometheus healthy" || echo "❌ Prometheus down"
        ''}
        
        ${optionalString preset.grafana ''
          echo "📈 Grafana: http://${cfg.observability.advanced.listenAddress}:${toString cfg.observability.advanced.grafanaPort}"
          curl -s "http://${cfg.observability.advanced.listenAddress}:${toString cfg.observability.advanced.grafanaPort}/api/health" >/dev/null && echo "✅ Grafana healthy" || echo "❌ Grafana down"
        ''}
        
        echo ""
        echo "🏥 Services Status:"
        systemctl is-active sinex-unified-collector && echo "✅ Unified Collector" || echo "❌ Unified Collector"
        ${optionalString cfg.promoWorker.enable ''systemctl is-active sinex-promo-worker && echo "✅ Promo Worker" || echo "❌ Promo Worker"''}
        
        echo ""
        echo "💾 Resource Usage:"
        free -h | grep Mem | awk '{printf "Memory: %s/%s (%.0f%%)\n", $3, $2, $3/$2*100}'
        df -h / | tail -1 | awk '{printf "Disk: %s used (%s)\n", $5, $4}'
        uptime | awk -F'load average:' '{printf "Load: %s\n", $2}'
      '')
      
      (pkgs.writeShellScriptBin "sinex-logs" ''
        echo "📋 Sinex Service Logs (${preset.logging} level)"
        echo "Choose service to follow:"
        echo "1) All Sinex services"
        echo "2) Unified Collector only"
        echo "3) Promo Worker only"
        ${optionalString preset.prometheus "echo \"4) Prometheus\""}
        ${optionalString preset.grafana "echo \"5) Grafana\""}
        read -p "Choice: " choice
        
        case $choice in
          1) journalctl -u 'sinex-*' -f ;;
          2) journalctl -u sinex-unified-collector -f ;;
          3) journalctl -u sinex-promo-worker -f ;;
          ${optionalString preset.prometheus "4) journalctl -u prometheus -f ;;"}
          ${optionalString preset.grafana "5) journalctl -u grafana -f ;;"}
          *) echo "Invalid choice" ;;
        esac
      '')
    ];

    # Smart firewall configuration (only open needed ports)
    networking.firewall.interfaces.lo.allowedTCPPorts = 
      (optionals preset.prometheus [ cfg.observability.advanced.prometheusPort 9100 9187 ]) ++
      (optionals preset.grafana [ cfg.observability.advanced.grafanaPort ]);

    # Health checks tuned to observability level
    systemd.services.sinex-health-check = {
      description = "Sinex Health Monitor";
      serviceConfig = {
        Type = "oneshot";
        User = cfg.database.user;
        ExecStart = pkgs.writeShellScript "sinex-health-check" ''
          set -euo pipefail
          
          # Check core services
          systemctl is-active sinex-unified-collector >/dev/null || { echo "CRITICAL: Unified collector down"; exit 1; }
          
          # Check database
          ${pkgs.postgresql}/bin/psql "${cfg.database.name}" -c "SELECT 1;" >/dev/null || { echo "CRITICAL: Database unreachable"; exit 1; }
          
          # Resource checks based on thresholds
          MEMORY_PERCENT=$(free | awk 'NR==2{printf "%.2f", $3*100/$2}')
          if (( $(echo "$MEMORY_PERCENT > ${toString (cfg.observability.advanced.alertThresholds.memory * 100)}" | bc -l) )); then
            echo "WARNING: Memory usage at $MEMORY_PERCENT%"
          fi
          
          DISK_PERCENT=$(df / | awk 'NR==2{print $5}' | sed 's/%//')
          if [ "$DISK_PERCENT" -gt "${toString (cfg.observability.advanced.alertThresholds.disk * 100)}" ]; then
            echo "WARNING: Disk usage at $DISK_PERCENT%"
          fi
          
          echo "$(date): Health check passed"
        '';
      };
    };

    systemd.timers.sinex-health-check = {
      description = "Sinex Health Check Timer";
      wantedBy = [ "timers.target" ];
      timerConfig = {
        OnCalendar = "*:0/5";  # Every 5 minutes
        Persistent = true;
        RandomizedDelaySec = 30;
      };
    };

    # Assertions for sanity checks
    assertions = [
      {
        assertion = cfg.observability.level != "comprehensive" || config.services.postgresql.enable;
        message = "Comprehensive observability requires PostgreSQL to be enabled";
      }
      {
        assertion = cfg.observability.advanced.prometheusPort != cfg.observability.advanced.grafanaPort;
        message = "Prometheus and Grafana cannot use the same port";
      }
    ];
  };
}