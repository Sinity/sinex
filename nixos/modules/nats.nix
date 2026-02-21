{ config, lib, pkgs, modulesPath, ... }:

with lib;

let
  cfg = config.services.sinex.nats;
  stateRoot = config.services.sinex.stateRoot;
  natsUser = "nats";
  dataDir = cfg.dataDir or (stateRoot + "/nats");
  storeDir = cfg.storeDir or (dataDir + "/jetstream");
  natsCli = pkgs.natscli or null; # natscli provides the `nats` CLI
  envName = lib.toLower cfg.environment;
  envUpper = lib.toUpper envName;
  prefixStreamName = name:
    if lib.hasPrefix "${envUpper}_" name then name else "${envUpper}_" + name;
  prefixSubject = subject:
    if lib.hasPrefix "${envName}." subject then subject else "${envName}." + subject;
  namespacedStreams = map (stream: stream // {
    name = prefixStreamName stream.name;
    subjects = map prefixSubject stream.subjects;
  }) cfg.bootstrapStreams.streams;
in
{
  # Ensure the upstream NATS service options are present even if not pulled in elsewhere.
  imports = [
    (modulesPath + "/services/networking/nats.nix")
  ];

  options.services.sinex.nats = with types; {
    enable = mkEnableOption "Manage a local NATS server with JetStream for Sinex";

    autoSetup = mkOption {
      type = bool;
      default = false;
      description = "Automatically provision NATS/JetStream alongside Sinex.";
    };

    environment = mkOption {
      type = str;
      default = "dev";
      example = "prod";
      description = ''
        Deployment environment name. Prefixes all NATS stream names and subjects
        (e.g., "prod" → PROD_SINEX_RAW_EVENTS, "prod.events.raw.>").

        This value is propagated as SINEX_ENVIRONMENT to all Sinex services so
        that publish/subscribe subjects match the bootstrapped streams.

        WARNING: Changing this after initial deployment renames all streams.
        Existing data in old streams will not be migrated automatically.
        For production deployments, set this explicitly; the default "dev" is
        only appropriate for local development environments.
      '';
    };

    host = mkOption {
      type = str;
      default = "127.0.0.1";
      description = ''
        Listen address for NATS clients. Defaults to loopback (127.0.0.1) to
        prevent accidental network exposure. Set to "0.0.0.0" only for
        multi-machine deployments with proper firewall rules.
      '';
    };

    port = mkOption {
      type = port;
      default = 4222;
      description = "NATS client port.";
    };

    monitoringPort = mkOption {
      type = port;
      default = 8222;
      description = "NATS monitoring/HTTP port.";
    };

    monitoringHost = mkOption {
      type = str;
      default = "127.0.0.1";
      description = ''
        Bind address for the NATS HTTP monitoring endpoint. Defaults to loopback.
        Set to "0.0.0.0" only if monitoring must be accessible from the network.
      '';
    };

    dataDir = mkOption {
      type = path;
      default = stateRoot + "/nats";
      defaultText = literalExpression "config.services.sinex.stateRoot + \"/nats\"";
      description = "Base data directory for NATS (accounts, leafnodes, JetStream).";
    };

    storeDir = mkOption {
      type = path;
      default = stateRoot + "/nats/jetstream";
      defaultText = literalExpression "config.services.sinex.stateRoot + \"/nats/jetstream\"";
      description = "JetStream storage directory.";
    };

    jetstreamMaxMemory = mkOption {
      type = nullOr str;
      default = null;
      description = "Optional JetStream memory cap (e.g., \"1GB\").";
    };

    jetstreamMaxStore = mkOption {
      type = nullOr str;
      default = null;
      description = "Optional JetStream file store cap (e.g., \"20GB\").";
    };

    extraSettings = mkOption {
      type = attrsOf (oneOf [ int str bool ]);
      default = {};
      description = "Additional raw NATS settings merged into the generated config.";
    };

    bootstrapStreams = {
      enable = mkOption {
        type = bool;
        default = true;
        description = "Automatically bootstrap standard Sinex JetStream streams via nats CLI.";
      };

      streams = mkOption {
        type = listOf attrs;
        default = [
          {
            name = "SINEX_RAW_EVENTS";
            subjects = [ "events.raw.>" ];
            maxAge = "168h"; # 7d
          }
          {
            name = "SOURCE_MATERIAL_BEGIN";
            subjects = [ "source_material.begin" ];
            maxAge = "168h";
          }
          {
            name = "SOURCE_MATERIAL_SLICES";
            subjects = [ "source_material.slices.>" ];
            maxAge = "168h";
          }
          {
            name = "SOURCE_MATERIAL_END";
            subjects = [ "source_material.end" ];
            maxAge = "168h";
          }
          {
            name = "SINEX_RAW_EVENTS_CONFIRMATIONS";
            subjects = [ "events.confirmations.>" ];
            maxAge = "720h"; # 30d
            maxMsgsPerSubject = 1;
          }
        ];
        description = "Stream definitions to bootstrap when bootstrapStreams.enable is true.";
      };
    };
  };

  config = mkIf (cfg.enable || cfg.autoSetup) {
    assertions = [
      {
        assertion = !(cfg.bootstrapStreams.enable && natsCli == null);
        message = "services.sinex.nats.bootstrapStreams requires pkgs.natscli to be available.";
      }
    ];

    warnings = optional (cfg.environment == "dev") ''
      services.sinex.nats.environment is set to "dev". This prefixes all NATS stream names
      and subjects with DEV_ / dev., which is only appropriate for local development.
      For production deployments set services.sinex.nats.environment = "prod" (or your
      environment name) before first boot — changing it afterwards renames all streams
      and existing data will not be migrated automatically.
    '';

    users.groups.${natsUser} = { };
    users.users.${natsUser} = {
      isSystemUser = true;
      group = natsUser;
      description = mkForce "NATS daemon user";
      home = mkForce storeDir;
      createHome = true;
    };

    systemd.tmpfiles.rules = mkAfter [
      "d ${dataDir} 0755 ${natsUser} ${natsUser} -"
      "d ${storeDir} 0755 ${natsUser} ${natsUser} -"
    ];

    services.nats = {
      enable = true;
      user = natsUser;
      group = natsUser;
      jetstream = true;
      port = cfg.port;
      dataDir = storeDir;
      settings =
        {
          server_name = mkDefault "${config.networking.hostName}-sinex";
          host = cfg.host;
          http = "${cfg.monitoringHost}:${toString cfg.monitoringPort}";
          jetstream = {
            store_dir = storeDir;
          } // optionalAttrs (cfg.jetstreamMaxMemory != null) { max_mem = cfg.jetstreamMaxMemory; }
            // optionalAttrs (cfg.jetstreamMaxStore != null) { max_file = cfg.jetstreamMaxStore; };
        }
        // cfg.extraSettings;
    };

    systemd.services.sinex-nats-bootstrap = mkIf (cfg.bootstrapStreams.enable && natsCli != null) {
      description = "Sinex NATS JetStream bootstrap";
      wants = [ "nats.service" ];
      after = [ "nats.service" ];
      wantedBy = [ "multi-user.target" ];
      serviceConfig = {
        Type = "oneshot";
        User = natsUser;
        Group = natsUser;
        Restart = "on-failure";
        RestartSec = 5;
        TimeoutStartSec = 60;
        Environment = [
          "NATS_URL=nats://${cfg.host}:${toString cfg.port}"
        ];
        ExecStart = let
          mkStreamCommand = stream:
            let
              subjArgs = concatStringsSep " " (map (s: "--subjects ${escapeShellArg s}") stream.subjects);
              maxMsgsPerSubjectArg = optionalString (stream ? maxMsgsPerSubject) "--max-msgs-per-subject ${toString stream.maxMsgsPerSubject}";
            in ''
              if ! ${natsCli}/bin/nats --server "$NATS_URL" stream info ${stream.name} >/dev/null 2>&1; then
                if ! output=$(${natsCli}/bin/nats --server "$NATS_URL" stream add ${stream.name} \
                  --defaults \
                  ${subjArgs} \
                  --storage file \
                  --retention limits \
                  --max-age ${stream.maxAge} \
                  --replicas 1 \
                  ${maxMsgsPerSubjectArg} 2>&1); then
                  if echo "$output" | grep -q "subjects overlap with an existing stream"; then
                    echo "Stream ${stream.name} already provisioned elsewhere; skipping bootstrap for it"
                  else
                    echo "$output"
                    exit 1
                  fi
                fi
              fi
            '';
          script = concatStringsSep "\n" (map mkStreamCommand namespacedStreams);
        in
          pkgs.writeShellScript "sinex-nats-bootstrap" ''
            set -euo pipefail
            ${script}
          '';
      };
    };
  };
}
