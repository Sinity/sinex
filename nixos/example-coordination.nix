
# Sinex hot-standby coordination example
#
# Demonstrates running multiple satellite instances with leadership hand-off for
# zero-downtime upgrades.  Use this as a starting point for production clusters.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    targetUser = "sinex-prod"; # replace with the operator account to monitor

    serviceManagement.serviceGroups = {
      core = true;
      maintenance = true;  # keep DLQ/git-annex timers active in production
      monitoring = true;   # expose Prometheus + Grafana on loopback
    };

    database = {
      autoSetup = true;
      name = "sinex_prod";
      user = "sinex";
      listenAddress = "127.0.0.1";
    };

    satellite = {
      enable = true;
      coordination = {
        enable = true;
        heartbeatInterval = 30;
        leadershipTimeout = 120;
        handoffTimeout = 60;
      };

      database.url = "postgresql:///sinex_prod?host=/run/postgresql";
      logLevel = "info";

      coreServices.enable = true;

      eventSources = {
        filesystem = {
          enable = true;
          instances = 3;   # one leader + two standbys
          watchPaths = [
            "/home/sinex-prod"
            "/var/lib/sinex"
          ];
        };
        terminal = {
          enable = true;
          instances = 2;
        };
        desktop = {
          enable = true;
          instances = 2;
        };
        system = {
          enable = true;
          instances = 2;
        };
      };

      automata = {
        canonicalCommandSynthesizer.enable = true;
        healthAggregator.enable = true;
      };
    };

    shell = {
      asciinema = {
        autoRecord = false;
        recordingsPath = "/var/lib/sinex/.local/share/asciinema";
      };
      kitty = {
        enable = true;
        autoConfigure = true;
        userConfigPath = "~/.config/kitty/kitty.conf";
      };
    };

    monitoring = {
      observabilityStack = {
        enable = true;
        listenAddress = "127.0.0.1";
        prometheusPort = 9002;
        grafanaPort = 9003;
      };
      dashboards.grafana.enable = true;
    };
  };

  # Optional: monitor the coordination tables for debugging
  systemd.services.sinex-coordination-monitor = {
    description = "Monitor Sinex coordination state";
    wantedBy = [ "multi-user.target" ];
    after = [ "postgresql.service" ];
    serviceConfig = {
      Type = "simple";
      User = "sinex";
      Restart = "on-failure";
      RestartSec = "30s";
    };
    script = ''
      set -e
      while true; do
        ${pkgs.postgresql}/bin/psql "postgresql:///sinex_prod?host=/run/postgresql" -c           "SELECT service_name, COUNT(*) AS instances, MAX(last_heartbeat) AS latest
             FROM core.satellite_instances
            WHERE last_heartbeat > NOW() - INTERVAL '2 minutes'
            GROUP BY service_name;"
        sleep 60
      done
    '';
  };

  # Ensure the monitored operator account exists
  users.users."sinex-prod" = {
    isNormalUser = true;
    extraGroups = [ "wheel" ];
  };

  systemd.tmpfiles.rules = [
    "d /var/lib/sinex 0755 sinex sinex -"
    "d /var/log/sinex 0755 sinex sinex -"
  ];
}
