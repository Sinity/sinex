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

in
{
  config = mkMerge [
    # Sinex services log to stderr; systemd captures that into the journal.
    # Log retention is therefore managed by journald, not logrotate.
    # The logDir path is kept for compatibility (other tooling may write there),
    # but log rotation is wired to journald SystemMaxUse / MaxFileSec instead.
    (mkIf enableLogging {
      services.journald.extraConfig = lib.mkDefault ''
        SystemMaxUse=${loggingCfg.retention.size}
        MaxFileSec=${loggingCfg.retention.age}
      '';
    })

    (mkIf enablePrometheus {
      services.prometheus =
        let
          natsEnabled = cfg.nats.enable || cfg.nats.autoSetup;
          builtinScrapeConfigs =
            (optional monitoringCfg.exporters.node {
              job_name = "node";
              static_configs = [{ targets = [ "localhost:9100" ]; }];
            })
            ++ (optional monitoringCfg.exporters.postgres {
              job_name = "postgres";
              static_configs = [{ targets = [ "localhost:9187" ]; }];
            })
            # NATS exposes a Prometheus-compatible /metrics endpoint on its monitoring port.
            ++ (optional natsEnabled {
              job_name = "nats";
              static_configs = [{
                targets = [ "${cfg.nats.monitoringHost}:${toString cfg.nats.monitoringPort}" ];
              }];
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
      };
    })
  ];
}
