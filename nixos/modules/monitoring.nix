{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  obs = cfg.observability;
  monitoringCfg = obs.monitoring;
  loggingCfg = obs.logging;
  alertsCfg = obs.alerts;

  logDir = obs.logDir;
  prometheusCfg = monitoringCfg.prometheus;

  enablePrometheus = cfg.enable && obs.enable && monitoringCfg.enable && prometheusCfg.enable;
  enableGrafana = cfg.enable && obs.enable && monitoringCfg.enable && monitoringCfg.grafana.enable;
  enableLogging = cfg.enable && obs.enable && loggingCfg.structured;

  natsEnabled = cfg.nats.enable || cfg.nats.autoSetup;
  natsExporterPkg = pkgs.prometheus-nats-exporter or null;
  # NATS monitoring port serves a JSON API, not Prometheus format.
  # prometheus-nats-exporter bridges the gap: it scrapes the NATS monitoring
  # HTTP API and re-exposes the data in Prometheus format on port 7777.
  enableNatsExporter = natsEnabled && enablePrometheus && monitoringCfg.exporters.nats && natsExporterPkg != null;
  natsExporterPort = 7777;
  natsMonitoringUrl = "http://${cfg.nats.monitoringHost}:${toString cfg.nats.monitoringPort}";

in
{
  config = mkMerge [
    # Sinex services log to stderr; systemd captures that into the journal.
    # Log retention is therefore managed by journald, not logrotate.
    # The logDir path is kept for compatibility (other tooling may write there),
    # but log rotation is wired to journald SystemMaxUse / SystemMaxFiles / MaxRetentionSec.
    (mkIf enableLogging {
      services.journald.extraConfig = lib.mkDefault ''
        SystemMaxUse=${loggingCfg.retention.size}
        SystemMaxFiles=${toString loggingCfg.retention.files}
        MaxRetentionSec=${loggingCfg.retention.age}
      '';
    })

    (mkIf enableNatsExporter {
      systemd.services.sinex-nats-prometheus-exporter = {
        description = "NATS Prometheus exporter for Sinex";
        wantedBy = [ "multi-user.target" ];
        after = [ "nats.service" ];
        wants = [ "nats.service" ];
        serviceConfig = {
          Type = "simple";
          ExecStart = "${natsExporterPkg}/bin/prometheus-nats-exporter"
            + " -port ${toString natsExporterPort}"
            + " -varz ${natsMonitoringUrl}"
            + " -jsz all"
            + " -connz"
            + " -routez";
          Restart = "on-failure";
          RestartSec = 5;
          DynamicUser = true;
          NoNewPrivileges = true;
          ProtectSystem = "strict";
          ProtectHome = true;
          PrivateTmp = true;
        };
      };
    })

    (mkIf enablePrometheus {
      services.prometheus =
        let
          builtinScrapeConfigs =
            (optional monitoringCfg.exporters.node {
              job_name = "node";
              static_configs = [{ targets = [ "localhost:9100" ]; }];
            })
            ++ (optional monitoringCfg.exporters.postgres {
              job_name = "postgres";
              static_configs = [{ targets = [ "localhost:9187" ]; }];
            })
            # NATS metrics are served by prometheus-nats-exporter (when enabled),
            # which translates the NATS JSON monitoring API to Prometheus format.
            # Direct scraping of the NATS monitoring port (8222) is intentionally
            # omitted: that port serves JSON, not Prometheus metrics.
            ++ (optional enableNatsExporter {
              job_name = "nats";
              static_configs = [{ targets = [ "localhost:${toString natsExporterPort}" ]; }];
            });
          # Note: sinex-gateway does not expose a Prometheus /metrics endpoint.
          # Gateway metrics are emitted as Sinex events via NATS self-observation
          # and stored in core.events. Query them via sinexctl or the analytics API.
        in
        {
          enable = true;
          listenAddress = prometheusCfg.listen;
          port = prometheusCfg.port;
          retentionTime = prometheusCfg.retention;
          ruleFiles = if alertsCfg.enable then alertsCfg.rulesFiles else [];
          exporters = {
            node.enable = monitoringCfg.exporters.node;
            postgres.enable = monitoringCfg.exporters.postgres;
          };
          scrapeConfigs = builtinScrapeConfigs ++ prometheusCfg.extraScrapeConfigs;
        };
    })

    (mkIf enableGrafana {
      services.grafana = {
        enable = true;
        settings.server.http_port = monitoringCfg.grafana.port;
        provision = mkIf enablePrometheus {
          enable = true;
          datasources.settings.datasources = [
            {
              name = "Prometheus";
              type = "prometheus";
              url = "http://${prometheusCfg.listen}:${toString prometheusCfg.port}";
              isDefault = true;
              access = "proxy";
            }
          ];
        };
      };
    })
  ];
}
