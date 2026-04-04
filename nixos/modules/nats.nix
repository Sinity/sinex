{ config, lib, pkgs, modulesPath, ... }:

with lib;

let
  systemdHardening = import ./lib/systemd-hardening.nix { inherit lib; };
  inherit (systemdHardening) mkHelperServiceConfig;
  cfg = config.services.sinex.nats;
  sinexCfg = config.services.sinex;
  stateRoot = config.services.sinex.stateRoot;
  natsUser = "nats";
  dataDir = cfg.dataDir or (stateRoot + "/nats");
  storeDir = cfg.storeDir or (dataDir + "/jetstream");
  natsCli = pkgs.natscli or null; # natscli provides the `nats` CLI
  secretPaths = config.sinex.secrets.paths or {};
  envName = lib.toLower cfg.environment;
  envUpper = lib.toUpper envName;
  prefixStreamName = name:
    if lib.hasPrefix "${envUpper}_" name then name else "${envUpper}_" + name;
  prefixSubject = subject:
    if lib.hasPrefix "${envName}." subject then subject else "${envName}." + subject;
  resolveSecretPath = explicit: names:
    if explicit != null then explicit else
    let
      match = findFirst (name: builtins.hasAttr name secretPaths) null names;
    in
    if match == null then null else builtins.getAttr match secretPaths;
  isLoopbackHost = host: elem host [ "127.0.0.1" "::1" "localhost" ];
  effectiveServerCertFile = resolveSecretPath cfg.tls.certFile [
    "sinex-nats-server-cert"
    "nats-server-cert"
  ];
  effectiveServerKeyFile = resolveSecretPath cfg.tls.keyFile [
    "sinex-nats-server-key"
    "nats-server-key"
  ];
  effectiveClientCaFile = resolveSecretPath cfg.tls.caCertFile [
    "sinex-nats-client-ca"
    "nats-client-ca"
  ];
  effectiveSharedClientCaFile = resolveSecretPath sinexCfg.nodes.nats.tls.caCertFile [
    "sinex-nats-ca"
    "nats-ca"
  ];
  effectiveSharedClientCertFile = resolveSecretPath sinexCfg.nodes.nats.tls.clientCertFile [
    "sinex-nats-client-cert"
    "nats-client-cert"
  ];
  effectiveSharedClientKeyFile = resolveSecretPath sinexCfg.nodes.nats.tls.clientKeyFile [
    "sinex-nats-client-key"
    "nats-client-key"
  ];
  effectiveSharedClientTokenFile = resolveSecretPath sinexCfg.nodes.nats.auth.tokenFile [
    "sinex-nats-token"
    "nats-token"
  ];
  effectiveSharedClientCredsFile = resolveSecretPath sinexCfg.nodes.nats.auth.credsFile [
    "sinex-nats-client-creds"
    "nats-client-creds"
  ];
  effectiveSharedClientNkeySeedFile = resolveSecretPath sinexCfg.nodes.nats.auth.nkeySeedFile [
    "sinex-nats-client-nkey"
    "nats-client-nkey"
  ];
  serverTlsEnabled = cfg.tls.enable || (cfg.extraSettings ? tls);
  serverAuthorizationEnabled =
    cfg.authorization.sharedClient.enable || (cfg.extraSettings ? authorization);
  sharedClientBaseSubjects = map prefixSubject [
    "events.raw.>"
    "events.confirmations.>"
    "events.dlq.>"
    "source_material.begin"
    "source_material.slices.>"
    "source_material.end"
    "system.schemas.active"
    "sinex.control.>"
    "sinex.coordination.>"
    "sinex.telemetry.>"
  ];
  sharedClientInternalSubjects = [
    "$JS.API.>"
    "$KV.>"
    "_INBOX.>"
  ];
  sharedClientPublishAllow =
    sharedClientBaseSubjects
    ++ sharedClientInternalSubjects
    ++ cfg.authorization.sharedClient.extraPublishAllow;
  sharedClientSubscribeAllow =
    sharedClientBaseSubjects
    ++ sharedClientInternalSubjects
    ++ cfg.authorization.sharedClient.extraSubscribeAllow;
  mkPermissionMap = allow: deny:
    { inherit allow; }
    // optionalAttrs (deny != []) { inherit deny; };
  bootstrapEnv = [
    "NATS_URL=${if serverTlsEnabled then "tls" else "nats"}://${cfg.host}:${toString cfg.port}"
  ]
    ++ optional (effectiveSharedClientTokenFile != null) "NATS_TOKEN_FILE=${toString effectiveSharedClientTokenFile}"
    ++ optional (effectiveSharedClientCredsFile != null) "NATS_CREDS=${toString effectiveSharedClientCredsFile}"
    ++ optional (effectiveSharedClientNkeySeedFile != null) "NATS_NKEY=${toString effectiveSharedClientNkeySeedFile}"
    ++ optional (effectiveSharedClientCaFile != null) "NATS_CA=${toString effectiveSharedClientCaFile}"
    ++ optional (effectiveSharedClientCertFile != null) "NATS_CERT=${toString effectiveSharedClientCertFile}"
    ++ optional (effectiveSharedClientKeyFile != null) "NATS_KEY=${toString effectiveSharedClientKeyFile}";
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

    tls = mkOption {
      type = submodule {
        options = {
          enable = mkEnableOption "TLS for the managed local NATS server";
          certFile = mkOption {
            type = nullOr path;
            default = null;
            description = ''
              Server certificate for the managed NATS listener.
              If unset, the module falls back to agenix secrets named
              <literal>sinex-nats-server-cert</literal> or <literal>nats-server-cert</literal>.
            '';
          };
          keyFile = mkOption {
            type = nullOr path;
            default = null;
            description = ''
              Server private key for the managed NATS listener.
              If unset, the module falls back to agenix secrets named
              <literal>sinex-nats-server-key</literal> or <literal>nats-server-key</literal>.
            '';
          };
          caCertFile = mkOption {
            type = nullOr path;
            default = null;
            description = ''
              Client CA bundle used when verifying client certificates.
              If unset, the module falls back to agenix secrets named
              <literal>sinex-nats-client-ca</literal> or <literal>nats-client-ca</literal>.
            '';
          };
          verifyClients = mkOption {
            type = bool;
            default = false;
            description = "Require client TLS certificates for the managed NATS listener.";
          };
          verifyAndMap = mkOption {
            type = bool;
            default = false;
            description = ''
              Require client TLS certificates and map the presented certificate to users in
              the server authorization config.
            '';
          };
        };
      };
      default = {};
      description = "Typed TLS configuration for the managed local NATS server.";
    };

    authorization = mkOption {
      type = submodule {
        options = {
          sharedClient = mkOption {
            type = submodule {
              options = {
                enable = mkEnableOption ''
                  subject-level authorization for the current shared Sinex client identity
                '';
                nkey = mkOption {
                  type = nullOr str;
                  default = null;
                  description = ''
                    Public NKey assigned to the shared Sinex client user on the managed NATS
                    server. The matching seed should be provided via
                    <literal>services.sinex.nodes.nats.auth.nkeySeedFile</literal> or an agenix
                    secret named <literal>sinex-nats-client-nkey</literal> /
                    <literal>nats-client-nkey</literal>.
                  '';
                };
                extraPublishAllow = mkOption {
                  type = listOf str;
                  default = [];
                  description = "Additional publish subjects allowed for the shared Sinex client.";
                };
                extraPublishDeny = mkOption {
                  type = listOf str;
                  default = [];
                  description = "Explicit publish subject denies for the shared Sinex client.";
                };
                extraSubscribeAllow = mkOption {
                  type = listOf str;
                  default = [];
                  description = "Additional subscribe subjects allowed for the shared Sinex client.";
                };
                extraSubscribeDeny = mkOption {
                  type = listOf str;
                  default = [];
                  description = "Explicit subscribe subject denies for the shared Sinex client.";
                };
              };
            };
            default = {};
            description = ''
              Server-side authorization entry for the current shared Sinex runtime identity.
              This fences the deployment to Sinex subjects and JetStream/KV internals without
              pretending per-service credentials already exist.
            '';
          };
        };
      };
      default = {};
      description = "Typed authorization configuration for the managed local NATS server.";
    };

    extraSettings = mkOption {
      type = with types; lazyAttrsOf anything;
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
            maxAge = "2160h"; # 90d
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
            maxAge = "168h"; # 7d
            maxMsgsPerSubject = 1;
          }
          {
            name = "SINEX_RAW_EVENTS_DLQ";
            subjects = [ "events.dlq.>" ];
            maxAge = "720h"; # 30d
            dupeWindow = "1h";
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
      {
        assertion = (!cfg.tls.enable) || (effectiveServerCertFile != null && effectiveServerKeyFile != null);
        message = "Managed NATS TLS requires services.sinex.nats.tls.certFile/keyFile or agenix secrets named sinex-nats-server-cert and sinex-nats-server-key.";
      }
      {
        assertion = (!(cfg.tls.verifyClients || cfg.tls.verifyAndMap)) || effectiveClientCaFile != null;
        message = "Managed NATS client-certificate verification requires services.sinex.nats.tls.caCertFile or an agenix secret named sinex-nats-client-ca.";
      }
      {
        assertion = !(cfg.tls.verifyClients && cfg.tls.verifyAndMap);
        message = "Choose either services.sinex.nats.tls.verifyClients or verifyAndMap, not both.";
      }
      {
        assertion = (!cfg.authorization.sharedClient.enable) || cfg.authorization.sharedClient.nkey != null;
        message = "Managed NATS shared-client authorization requires services.sinex.nats.authorization.sharedClient.nkey.";
      }
      {
        assertion = (!cfg.authorization.sharedClient.enable) || effectiveSharedClientNkeySeedFile != null;
        message = "Managed NATS shared-client authorization requires services.sinex.nodes.nats.auth.nkeySeedFile or an agenix secret named sinex-nats-client-nkey.";
      }
      {
        assertion = isLoopbackHost cfg.host || (serverTlsEnabled && serverAuthorizationEnabled);
        message = "Managed NATS binds beyond loopback; enable services.sinex.nats.tls and services.sinex.nats.authorization (or provide equivalent extraSettings) before exposing it.";
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
        // optionalAttrs cfg.tls.enable {
          tls =
            {
              cert_file = toString effectiveServerCertFile;
              key_file = toString effectiveServerKeyFile;
            }
            // optionalAttrs (effectiveClientCaFile != null) { ca_file = toString effectiveClientCaFile; }
            // optionalAttrs cfg.tls.verifyClients { verify = true; }
            // optionalAttrs cfg.tls.verifyAndMap { verify_and_map = true; };
        }
        // optionalAttrs cfg.authorization.sharedClient.enable {
          authorization = {
            users = [
              {
                nkey = cfg.authorization.sharedClient.nkey;
                permissions = {
                  publish = mkPermissionMap
                    sharedClientPublishAllow
                    cfg.authorization.sharedClient.extraPublishDeny;
                  subscribe = mkPermissionMap
                    sharedClientSubscribeAllow
                    cfg.authorization.sharedClient.extraSubscribeDeny;
                };
              }
            ];
          };
        }
        // cfg.extraSettings;
    };

    systemd.services.sinex-nats-bootstrap = mkIf (cfg.bootstrapStreams.enable && natsCli != null) {
      description = "Sinex NATS JetStream bootstrap";
      requires = [ "nats.service" ];
      wants = [ "nats.service" ];
      after = [ "nats.service" ];
      wantedBy = [ "multi-user.target" ];
      serviceConfig = {
        Environment = bootstrapEnv;
        ExecStart = let
          mkStreamCommand = stream:
            let
              streamName = escapeShellArg stream.name;
              subjectArgLines = concatStringsSep "\n" (
                map (subject: "  stream_args+=(--subjects ${escapeShellArg subject})") stream.subjects
              );
              optionalArgLines = concatStringsSep "\n" (filter (line: line != "") [
                (optionalString (stream ? maxMsgsPerSubject) "  stream_args+=(--max-msgs-per-subject ${escapeShellArg (toString stream.maxMsgsPerSubject)})")
                (optionalString (stream ? dupeWindow) "  stream_args+=(--dupe-window ${escapeShellArg stream.dupeWindow})")
              ]);
            in ''
              stream_args=()
${subjectArgLines}
              stream_args+=(--retention limits)
              stream_args+=(--max-age ${escapeShellArg stream.maxAge})
${optionalArgLines}
              if ${natsCli}/bin/nats --server "$NATS_URL" "''${auth_args[@]}" "''${tls_args[@]}" stream info ${streamName} >/dev/null 2>&1; then
                if ! output=$(${natsCli}/bin/nats --server "$NATS_URL" "''${auth_args[@]}" "''${tls_args[@]}" stream edit ${streamName} \
                  --dry-run "''${stream_args[@]}" \
                  2>&1); then
                  echo "Stream ${stream.name} drifts from declarative bootstrap config; refusing to overwrite it automatically" >&2
                  echo "$output" >&2
                  exit 1
                fi
              else
                if ! output=$(${natsCli}/bin/nats --server "$NATS_URL" "''${auth_args[@]}" "''${tls_args[@]}" stream add ${streamName} \
                  --defaults "''${stream_args[@]}" \
                  --storage file --replicas 1 \
                  2>&1); then
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
            auth_args=()
            tls_args=()
            if [[ -n "''${NATS_TOKEN_FILE:-}" ]]; then
              auth_args+=(--token "$(<"$NATS_TOKEN_FILE")")
            elif [[ -n "''${NATS_CREDS:-}" ]]; then
              auth_args+=(--creds "$NATS_CREDS")
            elif [[ -n "''${NATS_NKEY:-}" ]]; then
              auth_args+=(--nkey "$NATS_NKEY")
            fi
            if [[ -n "''${NATS_CA:-}" ]]; then
              tls_args+=(--tlsca "$NATS_CA")
            fi
            if [[ -n "''${NATS_CERT:-}" ]]; then
              tls_args+=(--tlscert "$NATS_CERT")
            fi
            if [[ -n "''${NATS_KEY:-}" ]]; then
              tls_args+=(--tlskey "$NATS_KEY")
            fi
            ${script}
          '';
      } // mkHelperServiceConfig {
        user = natsUser;
        group = natsUser;
        extra = {
          Restart = "on-failure";
          RestartSec = 5;
          TimeoutStartSec = 60;
        };
      };
    };
  };
}
