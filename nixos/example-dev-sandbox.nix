{
  config,
  lib,
  pkgs,
  ...
}:

# Comprehensive developer sandbox configuration for Sinex.
#
# This example turns on every major subsystem (nodes, maintenance, monitoring,
# coordination) on a single host so engineers can explore behaviour locally. It
# also provisions helper tooling and a sample data generator.
{
  imports = [ ./modules ];

  networking.hostName = "sinex-devbox";

  services.sinex = {
    enable = true;
    users.target = "developer";
    secrets.gatewayAdminTokenFile = "/etc/sinex/gateway-admin-token";

    database = {
      autoSetup = true;
      host = "127.0.0.1";
      name = "sinex_dev";
      user = "sinex";
      passwordFile = config.sinex.secrets.paths."sinex-local-db";
    };

    nats.environment = "dev";

    lifecycle.maintenance.enable = true;

    core = {
      enable = true;
      ingestd = {
        batch = {
          size = 500;
          timeoutSec = 3;
        };
      };
    };

    nodes = {
      enable = true;
      defaults.logLevel = "debug";

      coordination = {
        enable = true;
        heartbeatSec = 20;
        leadershipTimeoutSec = 60;
        handoffTimeoutSec = 30;
      };

      filesystem = {
        enable = true;
        instances = 2;
        watchPaths = [ "/home/developer" "/var/lib/sinex" ];
        resources = {
          memoryMax = "384M";
          cpuQuota = "60%";
        };
      };

      terminal = {
        enable = true;
        instances = 2;
        resources = {
          memoryMax = "256M";
          cpuQuota = "60%";
        };
      };

      desktop = {
        enable = true;
        instances = 1;
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
          retention = "3d";
        };
        grafana = {
          enable = true;
          port = 3000;
        };
      };
    };

    shell = {
      asciinema = {
        autoRecord = false;
        recordingsPath = "/home/developer/.local/share/asciinema";
      };
      kitty = {
        enable = true;
        autoConfigure = true;
        configFile = "~/.config/kitty/kitty.conf";
      };
    };
  };

  environment.etc."sinex/gateway-admin-token".text = "example-dev-sandbox-admin:admin";

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
    "d /home/developer/demo 0755 developer developer -"
  ];
}
