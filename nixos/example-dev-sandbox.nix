{
  config,
  lib,
  pkgs,
  ...
}:

# Comprehensive developer sandbox configuration for Sinex.
#
# This example turns on every major subsystem (satellites, maintenance, monitoring,
# coordination) on a single host so engineers can explore behaviour locally. It
# also provisions helper tooling and a sample data generator.
{
  imports = [ ./modules ];

  networking.hostName = "sinex-devbox";

  services.sinex = {
    enable = true;
    targetUser = "developer";

    serviceManagement.serviceGroups = {
      core = true;
      maintenance = true;
      monitoring = true;
    };

    database = {
      autoSetup = true;
      name = "sinex_dev";
      user = "sinex";
      listenAddress = "127.0.0.1";
    };

    satellite = {
      enable = true;
      logLevel = "debug";

      database.url = "postgresql:///sinex_dev?host=/run/postgresql";

      coordination = {
        enable = true;
        heartbeatInterval = 20;
        leadershipTimeout = 60;
        handoffTimeout = 30;
      };

      coreServices.enable = true;

      ingestd = {
        batchSize = 500;
        batchTimeout = 3;
      };

      eventSources = {
        filesystem = {
          enable = true;
          instances = 2;
          memoryLimit = "384M";
        };
        terminal = {
          enable = true;
          instances = 2;
          memoryLimit = "256M";
        };
        desktop = {
          enable = true;
          instances = 1;
          memoryLimit = "256M";
        };
        system = {
          enable = true;
          instances = 1;
          memoryLimit = "384M";
        };
      };

      automata = {
        canonicalCommandSynthesizer.enable = true;
        healthAggregator.enable = true;
      };
    };

    monitoring = {
      observabilityStack = {
        enable = true;
        listenAddress = "127.0.0.1";
        prometheusPort = 9090;
        grafanaPort = 3000;
        retentionTime = "3d";
      };
      dashboards.grafana.enable = true;
    };
  };

  users.users.developer = {
    isNormalUser = true;
    createHome = true;
    extraGroups = [ "wheel" ];
  };

  environment.systemPackages = with pkgs; [
    sinexCli
    jq
    httpie
    stress
    btop
  ];

  networking.firewall.interfaces.lo.allowedTCPPorts = [ 9090 3000 ];

  # Generate sample events periodically for demo purposes.
  systemd.services.sinex-sample-data = {
    description = "Inject sample Sinex events";
    wantedBy = [ "multi-user.target" ];
    after = [ "sinex-ingestd.service" ];
    serviceConfig = {
      Type = "simple";
      User = "developer";
      Restart = "always";
      RestartSec = 60;
    };
    script = ''
      set -euo pipefail
      mkdir -p "$HOME/demo"
      echo "sample-event-$(date +%s)" >> "$HOME/demo/notes.txt"
    '';
  };

  systemd.tmpfiles.rules = [
    "d /var/lib/sinex 0755 sinex sinex -"
    "d /var/log/sinex 0755 sinex sinex -"
    "d /home/developer/demo 0755 developer developer -"
  ];
}
