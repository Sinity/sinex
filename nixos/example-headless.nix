# Sinex headless server example
#
# Intended for machines without desktop/terminal capture requirements.
# Enables filesystem and system satellites along with maintenance timers.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    users.target = "serveruser";

    database = {
      autoSetup = true;
      host = "127.0.0.1";
      name = "sinex_server";
      user = "sinex";
      passwordFile = config.sinex.secrets.paths."sinex-local-db";
    };

    nats.environment = "prod";

    lifecycle.maintenance.enable = true;

    core.enable = true;

    satellites = {
      enable = true;
      defaults.logLevel = "info";

      filesystem = {
        enable = true;
        instances = 1;
        watchPaths = [ "/var/lib/sinex/sources" "/srv/data" ];
        resources = {
          memoryMax = "256M";
          cpuQuota = "60%";
        };
      };

      system = {
        enable = true;
        instances = 1;
        resources = {
          memoryMax = "384M";
          cpuQuota = "60%";
        };
      };

      terminal.enable = false;
      desktop.enable = false;

      automata = {
        enable = true;
        canonicalizer.enable = true;
        healthAggregator.enable = true;
      };
    };

    observability.enable = false;
    shell.kitty.enable = false;
  };

  users.users.serveruser = {
    isNormalUser = true;
    createHome = true;
    extraGroups = [ "wheel" ];
  };
}
