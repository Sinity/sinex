# Sinex remote satellite example
#
# Configures a node to run only satellite collectors and forward data to remote
# ingestd/NATS and PostgreSQL endpoints. Suitable for edge devices feeding a
# central Sinex cluster.

{ config, lib, pkgs, ... }:

{
  imports = [ ./modules ];

  services.sinex = {
    enable = true;
    targetUser = "agent";

    serviceManagement.serviceGroups = {
      core = true;
      maintenance = false;
      monitoring = false;
    };

    database = {
      autoSetup = false;
      host = "db.example.net";
      port = 5432;
      name = "sinex";
      user = "sinex_agent";
    };

    satellite = {
      enable = true;
      coordination.enable = false;
      coreServices.enable = false; # ingestd/gateway run on central cluster
      database.url = "postgresql://sinex_agent@db.example.net:5432/sinex";
      nats.servers = "tls://core.example.net:4222";
      logLevel = "info";

      # Inject shared environment into every satellite unit (e.g. TLS paths)
      environment = [
        "SINEX_NATS_CA_CERT=/etc/sinex/nats/ca.pem"
        "SINEX_NATS_CLIENT_CERT=/etc/sinex/nats/client.pem"
        "SINEX_NATS_CLIENT_KEY=/etc/sinex/nats/client.key"
      ];

      # Load credentials via environment file owned by root (see below)
      environmentFiles = [ "/etc/sinex/remote-satellite.env" ];

      eventSources = {
        filesystem = {
          enable = true;
          instances = 1;
          watchPaths = [ "/var/lib/sinex/watch" ];
        };
        terminal = {
          enable = true;
          instances = 1;
        };
        desktop.enable = false;
        system.enable = false;
      };

      automata = {
        canonicalCommandSynthesizer.enable = false;
        healthAggregator.enable = false;
      };
    };

    shell.kitty.enable = false;
  };

  # Disable local services that would conflict with remote endpoints.
  services.nats.enable = lib.mkForce false;
  services.postgresql.enable = lib.mkForce false;

  users.users.agent = {
    isNormalUser = true;
    createHome = true;
  };

  # Placeholder secret material — replace with your own deployment mechanism (e.g. agenix, sops-nix)
  environment.etc = {
    "sinex/nats/ca.pem" = {
      text = "# insert NATS CA certificate\n";
      mode = "0400";
    };
    "sinex/nats/client.pem" = {
      text = "# insert client certificate\n";
      mode = "0400";
    };
    "sinex/nats/client.key" = {
      text = "# insert client key\n";
      mode = "0400";
    };
    "sinex/remote-satellite.env" = {
      text = ''
        # Exported into every satellite unit via EnvironmentFile=
        # DATABASE_PASSWORD=***YOUR_DATABASE_PASSWORD_HERE*** # IMPORTANT: Replace with a securely managed secret (e.g., via agenix or sops-nix)
        # SINEX_NATS_TOKEN=***YOUR_NATS_TOKEN_HERE*** # IMPORTANT: Replace with a securely managed secret (e.g., via agenix or sops-nix)
      '';
      mode = "0400";
    };
  };

  environment.systemPackages = with pkgs; [ sinexCli jq ];
}
