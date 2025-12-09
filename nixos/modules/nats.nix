{ config, lib, pkgs, modulesPath, ... }:

with lib;

let
  cfg = config.services.sinex.nats;
  stateRoot = config.services.sinex.stateRoot;
  natsUser = "nats";
  dataDir = cfg.dataDir or (stateRoot + "/nats");
  storeDir = cfg.storeDir or (dataDir + "/jetstream");
  natsCli = pkgs.natscli or null; # natscli provides the `nats` CLI
  envName = lib.toLower (config.environment.variables.SINEX_ENVIRONMENT or "dev");
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

    host = mkOption {
      type = str;
      default = "0.0.0.0";
      description = "Listen address for NATS.";
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
            name = "EVENTS_CONFIRMATIONS";
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
          server_name = mkForce "sinex";
          host = cfg.host;
          http = cfg.monitoringPort;
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
                ${natsCli}/bin/nats --server "$NATS_URL" stream add ${stream.name} \
                  ${subjArgs} \
                  --storage file \
                  --retention limits \
                  --max-age ${stream.maxAge} \
                  --replicas 1 \
                  ${maxMsgsPerSubjectArg}
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
