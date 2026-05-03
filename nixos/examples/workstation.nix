
# Minimal Sinex configuration example
#
# Defines a single-node deployment with the node architecture and
# filesystem/terminal/browser capture enabled. Update the REQUIRED fields for your host.

{ config, lib, pkgs, ... }:

{
  imports = [ ../modules ];

  services.sinex = {
    enable = true;
    users.target = "myuser"; # REQUIRED: replace with the user to observe

    # Optional: select packages explicitly (module defaults work out of the box)
    # package = pkgs.sinex;
    # cliPackage = pkgs.sinexctl;

    database = {
      autoSetup = true;
      host = "127.0.0.1";
      port = 5432;
      name = "sinex_prod";
      user = "sinex";
    };

    nats = {
      environment = "prod"; # REQUIRED for production; use "dev" for local testing only
      # Workstations prefer a fast, terminal-style shutdown over the JetStream
      # graceful drain that hosted deployments rely on.
      killPolicy = {
        signal = "SIGTERM";
        timeoutStopSec = "10s";
      };
    };

    # Workstation runtime policy: keep the runtime out of automatic activation
    # restarts and bound the restart loop so a misbehaving capture node can't
    # spin forever under interactive load.
    runtime = {
      target = {
        attachToMultiUser = false;
        manualStartOnly = true;
      };
      restartOnSwitch = false;
      restartPolicy = {
        intervalSec = 600;
        burst = 3;
        backoffSec = 30;
      };
    };

    # Surface bootstrap failures instead of looping silently on a workstation.
    bootstrap.restartPolicy = "no";

    core = {
      enable = true;
      gateway.autoGenerateTls = true;
    };

    nodes = {
      enable = true;
      coordination.enable = false;
      defaults.logLevel = "info";

      filesystem = {
        enable = true;
        instances = 1;
      };
      terminal = {
        enable = true;
        instances = 1;
      };
      browser = {
        enable = true;
        instances = 1;
      };
      desktop.enable = false;
      system.enable = false;

      automata = {
        enable = true;
        canonicalizer.enable = true;
        healthAggregator.enable = true;
        analyticsAutomaton.enable = true;
        sessionDetector.enable = true;
      };
    };

    observability = {
      enable = false;
      monitoring.enable = false;
    };

    shell = {
      asciinema.autoRecord = false;
      kitty.enable = true;
    };
  };

  # Ensure the monitored user exists (adjust to match services.sinex.users.target above)
  users.users.myuser = {
    isNormalUser = true;
    createHome = true;
    extraGroups = [ "wheel" ];
  };

  environment.etc."sinex/gateway-admin-token".text = "workstation-admin:admin";
}
