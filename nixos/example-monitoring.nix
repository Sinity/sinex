
# Sinex observability example
#
# Enables the monitoring stack (Prometheus/Grafana) and maintenance timers
# alongside the satellite deployment. Suitable for staging environments where
# insight into resource usage and DLQ behaviour is required.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    users.target = "observer";

    database = {
      autoSetup = true;
      host = "127.0.0.1";
      name = "sinex_obs";
      user = "sinex";
      passwordFile = config.sinex.secrets.paths."sinex-local-db";
    };

    nats.environment = "prod";

    lifecycle.maintenance.enable = true;

    core.enable = true;

    satellites = {
      enable = true;
      filesystem.watchPaths = [ "/home/observer" ];
      terminal.enable = true;
      desktop.enable = true;
      system.enable = true;
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
          port = 9090;
          retention = "7d";
        };
        grafana = {
          enable = true;
          port = 3000;
        };
      };
    };

    shell = {
      asciinema.autoRecord = false;
      kitty.enable = true;
    };
  };

  users.users.observer = {
    isNormalUser = true;
    extraGroups = [ "wheel" ];
  };

  networking.firewall.interfaces.lo.allowedTCPPorts = [ 9090 3000 ];
}
