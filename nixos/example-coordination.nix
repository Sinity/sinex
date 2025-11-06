
# Sinex hot-standby coordination example
#
# Demonstrates running multiple satellite instances with leadership hand-off for
# zero-downtime upgrades.  Use this as a starting point for production clusters.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    users.target = "sinex-prod"; # replace with the operator account to monitor

    database = {
      autoSetup = true;
      host = "127.0.0.1";
      name = "sinex_prod";
      user = "sinex";
      passwordFile = config.sinex.secrets.paths."sinex-local-db";
    };

    lifecycle.maintenance.enable = true;

    core.enable = true;

    satellites = {
      enable = true;
      defaults.logLevel = "info";

      coordination = {
        enable = true;
        heartbeatSec = 30;
        leadershipTimeoutSec = 120;
        handoffTimeoutSec = 60;
      };

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

      automata = {
        enable = true;
        canonicalizer.enable = true;
        healthAggregator.enable = true;
      };
    };

    observability = {
      enable = true;
      monitoring = {
        enable = true;
        prometheus = {
          listen = "127.0.0.1";
          port = 9002;
        };
        grafana = {
          enable = true;
          port = 9003;
        };
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
        configFile = "~/.config/kitty/kitty.conf";
      };
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
}
