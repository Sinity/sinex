{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  systemdHardening = import ./lib/systemd-hardening.nix { inherit lib; };
  databaseRuntime = import ./lib/database-runtime.nix { inherit lib pkgs; };
  secretResolution = import ./lib/secret-resolution.nix { inherit lib; };
  inherit (systemdHardening) mkHelperServiceConfig;
  inherit (databaseRuntime)
    mkDatabasePasswordExec
    renderDatabaseUrl
    ;
  inherit (secretResolution) resolveNamedSecretPath;

  defaultSinexPackage =
    if pkgs ? sinex then
      pkgs.sinex
    else
      throw ''
        services.sinex.package is unset and no sinex package was found in pkgs.
        Provide one explicitly or overlay pkgs.sinex.
      '';

  defaultCliPackage =
    if pkgs ? sinexctl then pkgs.sinexctl else null;

  defaultKittySnippet = ''
    # Enable shell integration boundaries for session capture
    shell_integration enabled

    # Allow remote control for event collection (unix socket only)
    allow_remote_control socket-only

    # Socket path used by Sinex listeners
    listen_on unix:/tmp/kitty-$USER

    # Preserve editor cursor and titles while still emitting events
    shell_integration no-cursor
    shell_integration no-title
  '';

in
{
  imports = [
    ./secrets.nix
    ./database.nix
    ./nats.nix
    ./blob-storage.nix
    ./monitoring.nix
    ./preflight-verification.nix
    ./kitty-shell-integration.nix
    ./node-services.nix
  ];

  options.services.sinex = with types; let
    positive = ints.positive;
    unsigned = ints.unsigned;
    batchModule = { defaultSize, defaultTimeout }:
      submodule {
        options = {
          size = mkOption {
            type = positive;
            default = defaultSize;
            description = "Processing batch size.";
          };
          timeoutSec = mkOption {
            type = positive;
            default = defaultTimeout;
            description = "Maximum seconds to wait before flushing a partial batch.";
          };
        };
      };
    resourceModule = { defaultMemory, defaultCpu, defaultShutdownSec ? 90, defaultOpenFiles ? null }:
      submodule {
        options = {
          memoryMax = mkOption {
            type = str;
            default = defaultMemory;
            description = "systemd MemoryMax limit.";
          };
          cpuQuota = mkOption {
            type = str;
            default = defaultCpu;
            description = "systemd CPUQuota limit.";
          };
          shutdownTimeoutSec = mkOption {
            type = positive;
            default = defaultShutdownSec;
            description = "systemd TimeoutStopSec in seconds.";
          };
          openFilesLimit = mkOption {
            type = nullOr positive;
            default = defaultOpenFiles;
            description = "systemd LimitNOFILE soft/hard limit.";
          };
        };
      };
    envModule = attrsOf str;
    strList = listOf str;
    pathList = listOf path;
    bindReadOnlyPathModule = submodule {
      options = {
        source = mkOption {
          type = str;
          description = "Source path exposed into the service namespace.";
        };
        destination = mkOption {
          type = str;
          description = "Destination path inside the service namespace.";
        };
      };
    };
    terminalHistorySourceModule = submodule {
      options = {
        path = mkOption {
          type = str;
          description = "Absolute path to a shell history source.";
        };
        shell = mkOption {
          type = str;
          description = "Shell identifier (`bash`, `zsh`, `fish`, etc.).";
        };
      };
    };
    browserSqliteSourceModule = submodule {
      options = {
        path = mkOption {
          type = str;
          description = "Absolute path to a browser SQLite history source.";
        };
        browser = mkOption {
          type = str;
          description = "Browser identifier emitted on page.visited events.";
        };
        format = mkOption {
          type = enum [ "QutebrowserNative" "ChromiumHistory" ];
          description = "Typed browser history SQLite format.";
        };
      };
    };
  in
  {
    enable = mkEnableOption "Sinex Exocortex event capture system";

    package = mkOption {
      type = package;
      default = defaultSinexPackage;
      defaultText = literalExpression "pkgs.sinex";
      description = "Sinex package that provides all binaries.";
    };

    cliPackage = mkOption {
      type = nullOr package;
      default = defaultCliPackage;
      defaultText = literalExpression "pkgs.sinexctl or null";
      description = "Optional CLI package that will be placed on PATH when present.";
    };

    stateRoot = mkOption {
      type = path;
      default = "/var/lib/sinex";
      description = "Root directory for Sinex state and derived paths.";
    };

    logLevel = mkOption {
      type = enum [ "trace" "debug" "info" "warn" "error" ];
      default = "info";
      description = "Global log level propagated to Sinex services.";
    };

    users = mkOption {
      type = submodule {
        options = {
          target = mkOption {
            type = nullOr str;
            default = null;
            description = "Interactive user whose environment is captured (optional).";
          };

          nodes = mkOption {
            type = str;
            default = "sinex";
            description = "System account used to run Sinex services.";
          };
        };
      };
      default = { };
      description = "User and identity configuration.";
    };

    database = mkOption {
      type = submodule {
        options = {
          enable = mkOption {
            type = bool;
            default = true;
            description = "Manage the PostgreSQL cluster for Sinex.";
          };

          autoSetup = mkOption {
            type = bool;
            default = false;
            defaultText = literalExpression "true when services.sinex.enable = true";
            description = ''
              Automatically provision PostgreSQL (install, configure, create databases and
              users, enable extensions). When false, you must provision PostgreSQL yourself
              and point services.sinex.database.host/port/name/user at an existing instance.

              Implicitly set to true (via mkDefault) when services.sinex.enable = true.
              Set explicitly to false to opt out of automatic provisioning.
            '';
          };

          host = mkOption {
            type = str;
            default = "127.0.0.1";
            description = "PostgreSQL host that Sinex services connect to.";
          };

          port = mkOption {
            type = port;
            default = 5432;
            description = "PostgreSQL port.";
          };

          name = mkOption {
            type = str;
            default = "sinex";
            description = ''
              Database name used by Sinex.

              When <option>services.sinex.enable</option> is true, the module defaults this to
              <literal>sinex_&lt;environment&gt;</literal> so the runtime database tracks
              <option>services.sinex.nats.environment</option>.
            '';
          };

          extraDatabases = mkOption {
            type = listOf str;
            default = [ ];
            description = ''
              Additional PostgreSQL databases to provision alongside the primary one.
              Useful when you want both `sinex` and `sinex_dev` (or other sandboxes)
              managed by the module.
            '';
          };

          user = mkOption {
            type = str;
            default = "sinex";
            description = "Database role used by Sinex services.";
          };

          passwordFile = mkOption {
            type = nullOr path;
            default = null;
            description = ''
              Optional PostgreSQL password file.
              When unset, the module falls back to the conventional secret sources
              <literal>sinex-local-db</literal> / <literal>sinex-remote-db</literal>
              and the declarative files <literal>/etc/sinex/db-password</literal> /
              <literal>/etc/sinex/remote-db-password</literal>.
              Local loopback deployments using <literal>localAuth = "trust"</literal>
              usually do not need this at all.
            '';
          };

          localAuth = mkOption {
            type = enum [ "trust" "scram-sha-256" "md5" ];
            default = "trust";
            description = ''
              Authentication method for loopback TCP connections (127.0.0.1/::1).
              Use "trust" for local-only deployments where the OS provides access control.
              Use "scram-sha-256" to require password authentication even on loopback;
              requires a database password source (services.sinex.database.passwordFile or
              an agenix secret named sinex-local-db / sinex-remote-db).
            '';
          };

          package = mkOption {
            type = package;
            default = pkgs.postgresql_18;
            defaultText = literalExpression "pkgs.postgresql_18";
            description = "PostgreSQL package to deploy.";
          };

          extensionCompatibilityPackages = mkOption {
            type = listOf package;
            default = [ ];
            description = ''
              Extra PostgreSQL extension packages to add to the deployed
              PostgreSQL package without making them the canonical extension
              provider. This is intended for one-generation compatibility
              bridges when an existing database catalog still references an
              older versioned extension shared object during an extension update.
            '';
          };

          connectionPool = mkOption {
            type = submodule {
              options = {
                maxConnections = mkOption {
                  type = positive;
                  default = 40;
                  description = "Maximum connections per Sinex process.";
                };
                minConnections = mkOption {
                  type = positive;
                  default = 10;
                  description = "Minimum number of pooled connections.";
                };
                connectionTimeout = mkOption {
                  type = positive;
                  default = 30;
                  description = "Connection acquisition timeout in seconds.";
                };
                idleTimeout = mkOption {
                  type = positive;
                  default = 600;
                  description = "Idle connection timeout in seconds.";
                };
              };
            };
            default = { };
            description = "Connection pool tuning for Sinex services.";
          };

        };
      };
      default = { };
      description = "PostgreSQL provisioning and connection configuration.";
    };

    storage = mkOption {
      type = submodule {
        options = {
          dlq = mkOption {
            type = submodule {
              options = {
                enable = mkOption {
                  type = bool;
                  default = true;
                  description = "Enable the Dead Letter Queue.";
                };
                path = mkOption {
                  type = path;
                  default = cfg.stateRoot + "/failures";
                  defaultText = literalExpression "config.services.sinex.stateRoot + \"/failures\"";
                  description = "Directory used to store DLQ payloads.";
                };
                cleanup = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption {
                        type = bool;
                        default = true;
                        description = "Enable scheduled DLQ cleanup.";
                      };
                      maxAge = mkOption {
                        type = str;
                        default = "30d";
                        description = "Delete DLQ entries older than this duration.";
                      };
                      maxFiles = mkOption {
                        type = positive;
                        default = 10000;
                        description = "Delete DLQ entries when file count exceeds this number.";
                      };
                      schedule = mkOption {
                        type = str;
                        default = "daily";
                        description = "systemd.timer OnCalendar expression for cleanup.";
                      };
                    };
                  };
                  default = { };
                  description = "DLQ maintenance configuration.";
                };
              };
            };
            default = { };
            description = "Dead Letter Queue settings.";
          };

          blob = mkOption {
            type = submodule {
              options = {
	                enable = mkOption {
	                  type = bool;
	                  default = true;
	                  description = "Enable content-store backed blob storage.";
	                };
                repositoryPath = mkOption {
                  type = path;
                  default = cfg.stateRoot + "/blob-repository";
                  defaultText = literalExpression "config.services.sinex.stateRoot + \"/blob-repository\"";
	                  description = "Path to the content-store root.";
                };
                autoInit = mkOption {
                  type = bool;
                  default = true;
                  description = "Automatically initialize the repository if missing.";
                };
                numCopies = mkOption {
                  type = positive;
                  default = 2;
                  description = "Default git-annex numcopies value.";
                };
                backend = mkOption {
                  type = str;
                  default = "SHA256E";
                  description = "git-annex backend used for new blobs.";
                };
                maintenance = mkOption {
                  type = submodule {
                    options = {
                      gc = mkOption {
                        type = submodule {
                          options = {
                            enable = mkOption {
                              type = bool;
                              default = true;
                              description = "Enable git-annex garbage collection timer.";
                            };
                            schedule = mkOption {
                              type = str;
                              default = "weekly";
                              description = "OnCalendar schedule for git-annex GC.";
                            };
                          };
                        };
                        default = { };
                        description = "git-annex garbage collection.";
                      };
                      fsck = mkOption {
                        type = submodule {
                          options = {
                            enable = mkOption {
                              type = bool;
                              default = true;
                              description = "Enable git-annex fsck timer.";
                            };
                            schedule = mkOption {
                              type = str;
                              default = "monthly";
                              description = "OnCalendar schedule for git-annex fsck.";
                            };
                          };
                        };
                        default = { };
                        description = "git-annex fsck configuration.";
                      };
                    };
                  };
                  default = { };
                  description = "Blob maintenance tasks.";
                };
                health = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption {
                        type = bool;
                        default = false;
                        description = "Enable periodic blob repository health checks.";
                      };
                      intervalSec = mkOption {
                        type = positive;
                        default = 3600;
                        description = "Interval in seconds between health checks.";
                      };
                      warnAtBytes = mkOption {
                        type = nullOr unsigned;
                        default = null;
                        description = "Emit warnings when repository exceeds this size (bytes).";
                      };
                      warnAtPercent = mkOption {
                        type = float;
                        default = 0.8;
                        description = "Emit warnings when usage exceeds this fraction of warnAtBytes.";
                      };
                    };
                  };
                  default = { };
                  description = "Blob repository health monitoring.";
                };
              };
            };
            default = { };
            description = "Blob storage configuration.";
          };
        };
      };
      default = { };
      description = "Storage configuration.";
    };

    core = mkOption {
      type = submodule {
        options = {
          enable = mkOption {
            type = bool;
            default = true;
            description = "Enable core Sinex services (ingestd and gateway).";
          };

          ingestd = mkOption {
            type = submodule {
              options = {
                enable = mkOption {
                  type = bool;
                  default = true;
                  description = "Enable the ingestion daemon.";
                };
                spoolDir = mkOption {
                  type = path;
                  default = cfg.stateRoot + "/spool/ingestd";
                  defaultText = literalExpression "config.services.sinex.stateRoot + \"/spool/ingestd\"";
                  description = "Spool directory for ingestd.";
                };
                logLevel = mkOption {
                  type = str;
                  default = cfg.logLevel;
                  defaultText = literalExpression "config.services.sinex.logLevel";
                  description = "Log level for ingestd.";
                };
                batch = mkOption {
                  type = batchModule { defaultSize = 50; defaultTimeout = 2; };
                  default = { };
                  description = "Batch settings for ingestd. Defaults tuned for desktop workloads (low latency). Increase size/timeout for high-throughput server deployments.";
                };
                consumerMaxAckPending = mkOption {
                  type = positive;
                  default = 100;
                  description = "JetStream max_ack_pending for the main ingestd consumer.";
                };
                materialSlicesMaxAckPending = mkOption {
                  type = positive;
                  default = 1000;
                  description = "JetStream max_ack_pending for the material slices consumer.";
                };
                resources = mkOption {
                  type = resourceModule {
                    defaultMemory = "1G";
                    defaultCpu = "100%";
                    defaultOpenFiles = 524288;
                  };
                  default = { };
                  description = "Resource limits for ingestd.";
                };
                gitopsEnabled = mkOption {
                  type = bool;
                  default = false;
                  description = ''
                    Enable GitOps schema sync service.
                    When enabled, ingestd periodically fetches configured Git repositories
                    and registers discovered JSON schema files in the database.
                  '';
                };
                skipSchemaSync = mkOption {
                  type = bool;
                  default = false;
                  description = ''
                    Skip schema synchronization on startup.
                    Useful for environments where schemas are managed externally.
                  '';
                };
                strictValidation = mkOption {
                  type = bool;
                  default = false;
                  description = ''
                    Reject events without registered schemas (strict mode).
                    When false (default), unrecognized event types pass through without schema validation.
                  '';
                };
                validateSchemas = mkOption {
                  type = bool;
                  default = true;
                  description = "Enable JSON schema validation for ingested events.";
                };
                schemaReloadIntervalSecs = mkOption {
                  type = positive;
                  default = 300;
                  description = ''
                    Interval in seconds between schema reloads from the database.
                    Lower values make schema updates take effect faster at the cost of more DB queries.
                  '';
                };
                statsLogIntervalSecs = mkOption {
                  type = positive;
                  default = 60;
                  description = "Interval in seconds between processing statistics log entries.";
                };
                extraArgs = mkOption {
                  type = strList;
                  default = [ ];
                  description = "Additional command-line arguments for ingestd.";
                };
              };
            };
            default = { };
            description = "Ingestion daemon configuration.";
          };

          gateway = mkOption {
            type = submodule {
              options = {
                enable = mkOption {
                  type = bool;
                  default = true;
                  description = "Enable the RPC gateway.";
                };
                logLevel = mkOption {
                  type = str;
                  default = cfg.logLevel;
                  defaultText = literalExpression "config.services.sinex.logLevel";
                  description = "Log level for the gateway.";
                };
                resources = mkOption {
                  type = resourceModule { defaultMemory = "512M"; defaultCpu = "75%"; };
                  default = { };
                  description = "Resource limits for the gateway.";
                };
                listenAddress = mkOption {
                  type = str;
                  default = "127.0.0.1:9999";
                  description = "TCP listen address for the RPC gateway (host:port).";
                };
                requireClientTLS = mkOption {
                  type = bool;
                  default = false;
                  description = "Force mTLS even on loopback; when enabled, clients must present certificates.";
                };
                limits = mkOption {
                  type = submodule {
                    options = {
                      maxConcurrency = mkOption {
                        type = positive;
                        default = 100;
                        description = "Max concurrent RPC requests enforced by the gateway.";
                      };
                      requestTimeoutSec = mkOption {
                        type = positive;
                        default = 30;
                        description = "RPC request timeout in seconds.";
                      };
                      maxBodyBytes = mkOption {
                        type = positive;
                        default = 2 * 1024 * 1024;
                        description = "Maximum JSON-RPC payload size in bytes.";
                      };
                      maxBlobBytes = mkOption {
                        type = positive;
                        default = 5 * 1024 * 1024;
                        description = "Maximum decoded blob upload size in bytes.";
                      };

                      rateLimit = mkOption {
                        type = submodule {
                          options = {
                            enable = mkOption {
                              type = bool;
                              default = true;
                              description = "Enable per-token rate limiting on the gateway.";
                            };
                            requestsPerSec = mkOption {
                              type = positive;
                              default = 100;
                              description = "Local rate limiter: sustained requests per second allowed per token.";
                            };
                            burst = mkOption {
                              type = positive;
                              default = 50;
                              description = "Local rate limiter: burst capacity above the sustained rate per token.";
                            };
                            idleTimeoutSec = mkOption {
                              type = positive;
                              default = 3600;
                              description = "Local rate limiter: evict idle token entries after this many seconds.";
                            };
                            distributedPerMinute = mkOption {
                              type = positive;
                              default = 6000;
                              description = ''
                                Distributed rate limiter: max requests per minute per token,
                                enforced across all gateway instances via NATS KV.
                                Default 6000 = 100 req/s sustained.
                              '';
                            };
                            distributedWindowSec = mkOption {
                              type = positive;
                              default = 60;
                              description = "Distributed rate limiter: sliding window duration in seconds.";
                            };
                          };
                        };
                        default = { };
                        description = "Per-token rate limiting. Two complementary limiters operate in tandem: a local token-bucket (fast, in-process) and a distributed NATS KV limiter (consistent across gateway replicas).";
                      };
                    };
                  };
                  default = { };
                  description = "RPC resource guard configuration for the gateway.";
                };
                tlsCertFile = mkOption {
                  type = nullOr path;
                  default = if cfg.core.gateway.autoGenerateTls then cfg.stateRoot + "/tls/server.pem" else null;
                  description = ''
                    Path to the gateway TLS certificate. Required unless autoGenerateTls is enabled.
                    Exported as <literal>SINEX_GATEWAY_TLS_CERT</literal>.
                  '';
                };
                tlsKeyFile = mkOption {
                  type = nullOr path;
                  default = if cfg.core.gateway.autoGenerateTls then cfg.stateRoot + "/tls/server-key.pem" else null;
                  description = ''
                    Path to the gateway TLS private key. Required unless autoGenerateTls is enabled.
                    Exported as <literal>SINEX_GATEWAY_TLS_KEY</literal>.
                  '';
                };
                tlsClientCAFile = mkOption {
                  type = nullOr path;
                  default = null;
                  description = ''
                    Client CA bundle for gateway mTLS. Required for non-loopback binds
                    and whenever requireClientTLS is enabled. Exported as
                    <literal>SINEX_GATEWAY_TLS_CLIENT_CA</literal>.
                  '';
                };
                autoGenerateTls = mkOption {
                  type = bool;
                  default = false;
                  description = ''
                    Automatically generate an rcgen-backed local PKI for the gateway on first boot.
                    Stores credentials at
                    <literal>''${stateRoot}/tls/{server.pem,server-key.pem,ca.pem,client.pem,client-key.pem}</literal>
                    and sets <option>tlsCertFile</option>/<option>tlsKeyFile</option> accordingly.
                    The generated CA becomes the gateway trust anchor for deployment-readiness checks.
                    Suitable for single-host deployments. For production clusters, provide real certs.
                  '';
                };
                corsOrigins = mkOption {
                  type = nullOr str;
                  default = null;
                  description = ''
                    Comma-separated list of allowed CORS origins for the gateway HTTP interface.
                    Set to "*" to allow all origins (not recommended for production).
                    Null disables CORS headers entirely.
                  '';
                };

                nativeMessagingMaxSizeBytes = mkOption {
                  type = positive;
                  default = 1048576; # 1 MiB — matches Chrome/Firefox native messaging spec
                  description = "Maximum single message size in bytes for the native messaging protocol.";
                };

                extraArgs = mkOption {
                  type = strList;
                  default = [ ];
                  description = "Additional command-line arguments for the gateway.";
                };
              };
            };
            default = { };
            description = "Gateway configuration.";
          };
        };
      };
      default = { };
      description = "Core service configuration.";
    };

    nodes = mkOption {
      type = submodule {
        options = {
          enable = mkOption {
            type = bool;
            default = true;
            description = "Enable node services.";
          };

          nats = mkOption {
            type = submodule {
              options = {
                servers = mkOption {
                  type = strList;
                  default = [ "nats://127.0.0.1:4222" ];
                  description = ''
                    List of NATS server URLs shared by core services and nodes.
                    Rendered as <literal>SINEX_NATS_URL</literal> for managed services.
                  '';
                };
                monitoringPort = mkOption {
                  type = port;
                  default = 8222;
                  description = ''
                    NATS monitoring port.
                    Rendered as <literal>SINEX_NATS_MONITORING_PORT</literal> for managed services.
                  '';
                };
                tls = mkOption {
                  type = submodule {
                    options = {
                      requireTls = mkOption {
                        type = bool;
                        default = false;
                        description = ''
                          Enforce TLS for NATS connections. When enabled, services export
                          <literal>SINEX_NATS_REQUIRE_TLS=1</literal> and startup validation
                          rejects non-<literal>tls://</literal> / <literal>wss://</literal> URLs.
                        '';
                      };
                      caCertFile = mkOption {
                        type = nullOr path;
                        default = null;
                        description = ''
                          CA bundle used to verify the NATS server certificate.
                          Exported as <literal>SINEX_NATS_CA_CERT</literal>.
                          If unset, managed deployments fall back to agenix secrets named
                          <literal>sinex-nats-ca</literal> or <literal>nats-ca</literal>.
                        '';
                      };
                      clientCertFile = mkOption {
                        type = nullOr path;
                        default = null;
                        description = ''
                          Client certificate for NATS mutual TLS.
                          Exported as <literal>SINEX_NATS_CLIENT_CERT</literal>.
                          If unset, managed deployments fall back to agenix secrets named
                          <literal>sinex-nats-client-cert</literal> or
                          <literal>nats-client-cert</literal>.
                        '';
                      };
                      clientKeyFile = mkOption {
                        type = nullOr path;
                        default = null;
                        description = ''
                          Client private key for NATS mutual TLS.
                          Exported as <literal>SINEX_NATS_CLIENT_KEY</literal>.
                          If unset, managed deployments fall back to agenix secrets named
                          <literal>sinex-nats-client-key</literal> or
                          <literal>nats-client-key</literal>.
                        '';
                      };
                    };
                  };
                  default = { };
                  description = "Typed TLS configuration for the shared NATS client connection; exported automatically to core services and nodes.";
                };
                auth = mkOption {
                  type = submodule {
                    options = {
                      tokenFile = mkOption {
                        type = nullOr path;
                        default = null;
                        description = ''
                          Path to a file containing the shared NATS auth token.
                          Prefer this for simple file-backed secret deployment.
                          Exported as <literal>SINEX_NATS_TOKEN_FILE</literal>.
                          If unset, managed deployments fall back to agenix secrets named
                          <literal>sinex-nats-token</literal> or <literal>nats-token</literal>.
                        '';
                      };
                      credsFile = mkOption {
                        type = nullOr path;
                        default = null;
                        description = ''
                          Path to a NATS credentials file (`.creds`, JWT + seed).
                          Use this when the NATS deployment expects credentials-file auth.
                          Exported as <literal>SINEX_NATS_CREDS_FILE</literal>.
                          If unset, managed deployments fall back to agenix secrets named
                          <literal>sinex-nats-client-creds</literal> or
                          <literal>nats-client-creds</literal>.
                        '';
                      };
                      nkeySeedFile = mkOption {
                        type = nullOr path;
                        default = null;
                        description = ''
                          Path to a file containing the NATS NKey seed.
                          Use this only when the deployment expects direct NKey auth.
                          Exported as <literal>SINEX_NATS_NKEY_SEED_FILE</literal>.
                          If unset, managed deployments fall back to agenix secrets named
                          <literal>sinex-nats-client-nkey</literal> or
                          <literal>nats-client-nkey</literal>.
                        '';
                      };
                    };
                  };
                  default = { };
                  description = ''
                    Typed shared NATS authentication configuration exported automatically to
                    core services and nodes. Configure at most one auth mode.
                  '';
                };
              };
            };
            default = { };
            description = "Shared NATS client configuration used by core services and nodes.";
          };

          defaults = mkOption {
            type = submodule {
              options = {
                instances = mkOption {
                  type = positive;
                  default = 1;
                  description = "Default number of instances per node.";
                };
                logLevel = mkOption {
                  type = str;
                  default = cfg.logLevel;
                  defaultText = literalExpression "config.services.sinex.logLevel";
                  description = "Default log level for nodes.";
                };
                batch = mkOption {
                  type = batchModule { defaultSize = 100; defaultTimeout = 2; };
                  default = { };
                  description = "Default batching configuration for nodes.";
                };
                resources = mkOption {
                  type = resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; };
                  default = { };
                  description = "Default resource limits.";
                };
                env = mkOption {
                  type = envModule;
                  default = { };
                  description = "Environment variables applied to every node.";
                };
              };
            };
            default = { };
            description = "Node defaults.";
          };

          filesystem = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable filesystem node. Watches large directory trees; needs more memory than other nodes."; };
                watchPaths = mkOption {
                  type = strList;
                  default = [ ];
                  description = ''
                    Absolute paths for the filesystem node to watch.
                    When empty and <option>services.sinex.users.target</option> is set,
                    defaults to the target user's home directory.
                  '';
                };
                maxWatches = mkOption {
                  type = positive;
                  default = 524288;
                  description = ''
                    Filesystem watch-budget threshold passed to the node config.
                    When the recursive tree exceeds this value, the node now tries a
                    filtered native watch plan before failing honestly instead of
                    switching to recursive poll mode.
                  '';
                };
                ignoredDirectoryNames = mkOption {
                  type = strList;
                  default = [
                    ".btrfs"
                    ".cache"
                    ".direnv"
                    ".git"
                    ".hg"
                    ".jj"
                    ".sinex"
                    ".svn"
                    ".Trash-1000"
                    "__pycache__"
                    "node_modules"
                    "target"
                  ];
                  description = ''
                    Directory names excluded from recursive filesystem watch planning
                    and historical scans. This trims heavy local tooling trees that
                    otherwise consume watch budget without adding useful user signal.
                  '';
                };
                pollIntervalSec = mkOption {
                  type = positive;
                  default = 5;
                  description = ''
                    Legacy compatibility field for older filesystem node configs.
                    Automatic recursive poll fallback is no longer used.
                  '';
                };
                instances = mkOption { type = nullOr positive; default = null; description = "Instance override (null ⇒ inherit defaults)."; };
                batch = mkOption {
                  type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; });
                  default = null;
                  description = "Batch override (null ⇒ inherit defaults).";
                };
                resources = mkOption {
                  type = nullOr (resourceModule { defaultMemory = "2G"; defaultCpu = "50%"; });
                  default = { };
                  description = "Filesystem node resource limits. Defaults to 2G memory (higher than other nodes due to inotify watch and file-cache overhead).";
                };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
                extraArgs = mkOption { type = strList; default = [ ]; description = "Extra CLI args."; };
              };
            };
            default = { };
            description = "Filesystem node.";
          };

          terminal = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable terminal node."; };
                instances = mkOption { type = nullOr positive; default = null; description = "Instance override."; };
                batch = mkOption { type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; }); default = null; description = "Batch override."; };
                resources = mkOption { type = nullOr (resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; }); default = null; description = "Resource override."; };
                historySources = mkOption {
                  type = listOf terminalHistorySourceModule;
                  default = [ ];
                  description = ''
                    Structured history sources passed to the terminal node through
                    <literal>--node-config</literal>. When empty, the node falls back to its
                    built-in defaults (the service user's home directory), which is usually not
                    what workstation deployments want.
                  '';
                };
                access = mkOption {
                  type = submodule {
                    options = {
                      bindReadOnlyPaths = mkOption {
                        type = listOf bindReadOnlyPathModule;
                        default = [ ];
                        description = ''
                          Optional <literal>BindReadOnlyPaths</literal> entries for exposing
                          target-user history files into the service namespace.
                        '';
                      };
                    };
                  };
                  default = { };
                  description = "Terminal node host-access configuration.";
                };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
                extraArgs = mkOption { type = strList; default = [ ]; description = "Extra CLI args."; };
              };
            };
            default = { };
            description = "Terminal node.";
          };

          browser = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable browser history node."; };
                instances = mkOption { type = nullOr positive; default = null; description = "Instance override."; };
                batch = mkOption { type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; }); default = null; description = "Batch override."; };
                resources = mkOption {
                  type = nullOr (resourceModule { defaultMemory = "384M"; defaultCpu = "50%"; });
                  default = null;
                  description = "Resource override.";
                };
                dumpSources = mkOption {
                  type = strList;
                  default = [ ];
                  description = ''
                    Absolute paths to browser export roots (`json`, `jsonl`, `ndjson`, `csv`)
                    scanned by the browser history node.
                  '';
                };
                sqliteSources = mkOption {
                  type = listOf browserSqliteSourceModule;
                  default = [ ];
                  description = ''
                    Typed browser SQLite sources passed to the browser history node through
                    <literal>--node-config</literal>.
                  '';
                };
                pollIntervalSec = mkOption {
                  type = positive;
                  default = 30;
                  description = "Polling interval for browser dump roots and SQLite sources.";
                };
                access = mkOption {
                  type = submodule {
                    options = {
                      bindReadOnlyPaths = mkOption {
                        type = listOf bindReadOnlyPathModule;
                        default = [ ];
                        description = ''
                          Optional <literal>BindReadOnlyPaths</literal> entries for exposing
                          browser history files into the service namespace.
                        '';
                      };
                    };
                  };
                  default = { };
                  description = "Browser node host-access configuration.";
                };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
                extraArgs = mkOption { type = strList; default = [ ]; description = "Extra CLI args."; };
              };
            };
            default = { };
            description = "Browser history node.";
          };

          desktop = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable desktop node."; };
                instances = mkOption { type = nullOr positive; default = null; description = "Instance override."; };
                batch = mkOption { type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; }); default = null; description = "Batch override."; };
                resources = mkOption { type = nullOr (resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; }); default = null; description = "Resource override."; };
                session = mkOption {
                  type = submodule {
                    options = {
                      runtimeDir = mkOption {
                        type = nullOr str;
                        default = null;
                        description = ''
                          Runtime directory presented to the desktop node. When set, the module
                          exports both <literal>SINEX_HYPRLAND_RUNTIME_DIR</literal> and
                          <literal>XDG_RUNTIME_DIR</literal>.
                        '';
                      };
                      waylandDisplay = mkOption {
                        type = nullOr str;
                        default = null;
                        description = "Explicit `WAYLAND_DISPLAY` value for clipboard access.";
                      };
                      hyprlandInstanceSignature = mkOption {
                        type = nullOr str;
                        default = null;
                        description = "Explicit Hyprland instance signature for socket discovery.";
                      };
                      hyprlandEventSocket = mkOption {
                        type = nullOr str;
                        default = null;
                        description = "Explicit path to the Hyprland event socket (.socket2.sock).";
                      };
                      hyprlandCommandSocket = mkOption {
                        type = nullOr str;
                        default = null;
                        description = "Explicit path to the Hyprland command socket (.socket.sock).";
                      };
                    };
                  };
                  default = { };
                  description = "Desktop node session/runtime wiring.";
                };
                access = mkOption {
                  type = submodule {
                    options = {
                      bindReadOnlyPaths = mkOption {
                        type = listOf bindReadOnlyPathModule;
                        default = [ ];
                        description = ''
                          Optional <literal>BindReadOnlyPaths</literal> entries for exposing
                          user-runtime sockets (Hyprland, Wayland) into the service namespace.
                        '';
                      };
                    };
                  };
                  default = { };
                  description = "Desktop node host-access configuration.";
                };
                history = mkOption {
                  type = submodule {
                    options = {
                      activitywatchDbPath = mkOption {
                        type = nullOr path;
                        default = null;
                        description = ''
                          Optional ActivityWatch SQLite database path used for desktop historical
                          import. Exported as <literal>SINEX_ACTIVITYWATCH_DB_PATH</literal>.
                        '';
                      };
                    };
                  };
                  default = { };
                  description = "Desktop historical-import configuration.";
                };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
                extraArgs = mkOption { type = strList; default = [ ]; description = "Extra CLI args."; };
                clipboard = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption {
                        type = bool;
                        default = true;
                        description = "Enable clipboard integration.";
                      };
                    };
                  };
                  default = { };
                  description = "Desktop clipboard integration.";
                };
              };
            };
            default = { };
            description = "Desktop node.";
          };

          system = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable system node."; };
                instances = mkOption { type = nullOr positive; default = 1; description = "Instance override (default 1)."; };
                batch = mkOption {
                  type = nullOr (batchModule { defaultSize = 200; defaultTimeout = 10; });
                  default = { size = 200; timeoutSec = 10; };
                  description = "Batch override (defaults to a slower cadence).";
                };
                resources = mkOption { type = nullOr (resourceModule { defaultMemory = "512M"; defaultCpu = "50%"; }); default = null; description = "Resource override."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
                extraArgs = mkOption { type = strList; default = [ ]; description = "Extra CLI args."; };
              };
            };
            default = { };
            description = "System node.";
          };

          document = mkOption {
            type = submodule {
              options = {
                enable = mkOption {
                  type = bool;
                  default = true;
                  description = ''
                    Enable managed document snapshot ingestion. This node runs as a
                    scheduled scan service rather than a long-running daemon.
                  '';
                };
                allowedRoots = mkOption {
                  type = pathList;
                  default = [ ];
                  description = ''
                    Root directories scanned by the managed document-ingestion service.
                    When left empty and <option>services.sinex.users.target</option> is
                    set, the module derives a default root of <literal>$HOME/Documents</literal>.
                  '';
                };
                supportedMimeTypes = mkOption {
                  type = strList;
                  default = [
                    "text/plain"
                    "text/markdown"
                    "application/pdf"
                    "application/json"
                    "text/html"
                    "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
                  ];
                  description = "MIME types accepted by the document ingestor.";
                };
                maxDocumentSize = mkOption {
                  type = unsigned;
                  default = 25 * 1024 * 1024;
                  description = "Maximum document size in bytes.";
                };
                runOnBoot = mkOption {
                  type = bool;
                  default = true;
                  description = "Run a full document snapshot once during boot.";
                };
                schedule = mkOption {
                  type = nullOr str;
                  default = "hourly";
                  description = ''
                    Optional <literal>systemd.timer</literal> OnCalendar schedule for
                    recurring document scans. Set to <literal>null</literal> to disable
                    the timer while keeping the boot scan.
                  '';
                };
                persistentTimer = mkOption {
                  type = bool;
                  default = true;
                  description = "Set <literal>Persistent=true</literal> on the document scan timer.";
                };
                resources = mkOption {
                  type = nullOr (resourceModule {
                    defaultMemory = "512M";
                    defaultCpu = "100%";
                    defaultShutdownSec = 600;
                  });
                  default = null;
                  description = "Resource limits for managed document snapshot scans.";
                };
                env = mkOption {
                  type = envModule;
                  default = { };
                  description = "Extra environment variables.";
                };
                extraArgs = mkOption {
                  type = strList;
                  default = [ ];
                  description = "Extra CLI args.";
                };
              };
            };
            default = { };
            description = "Managed document snapshot ingestion.";
          };

          automata = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable automata services."; };

                canonicalizer = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption { type = bool; default = true; description = "Enable canonical command synthesizer."; };
                      profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                      env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
                    };
                  };
                  default = { };
                  description = "Canonical command synthesizer automaton.";
                };

                healthAggregator = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption { type = bool; default = true; description = "Enable health aggregator automaton."; };
                      profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                      env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
                    };
                  };
                  default = { };
                  description = "Health aggregator automaton.";
                };

                analyticsAutomaton = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption { type = bool; default = true; description = "Enable analytics automaton."; };
                      profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                      env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
                    };
                  };
                  default = { };
                  description = "Analytics automaton. Emits bounded `activity.window.summary` rollups from trusted activity signals.";
                };

                sessionDetector = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption { type = bool; default = true; description = "Enable session detector automaton. Groups events by temporal proximity into session boundaries."; };
                      profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                      env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
                    };
                  };
                  default = { };
                  description = "Session detector automaton. Rolls bounded `activity.window.summary` inputs into `activity.session.boundary` outputs.";
                };

                profiles = mkOption {
                  type = attrsOf (submodule {
                    options = {
                      batch = mkOption {
                        type = batchModule { defaultSize = 100; defaultTimeout = 5; };
                        default = { };
                        description = "Batch parameters for this automata profile.";
                      };
                      resources = mkOption {
                        type = resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; };
                        default = { };
                        description = "Resource limits for this automata profile.";
                      };
                    };
                  });
                  default = {
                    light = {
                      batch = { size = 50; timeoutSec = 2; };
                      resources = { memoryMax = "128M"; cpuQuota = "25%"; };
                    };
                    standard = {
                      batch = { size = 100; timeoutSec = 5; };
                      resources = { memoryMax = "256M"; cpuQuota = "50%"; };
                    };
                    heavy = {
                      batch = { size = 500; timeoutSec = 5; };
                      resources = { memoryMax = "512M"; cpuQuota = "100%"; };
                    };
                  };
                  description = "Named automata performance profiles.";
                };
              };
            };
            default = { };
            description = "Automata configuration.";
          };

          coordination = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = false; description = "Enable node coordination."; };
                heartbeatSec = mkOption { type = positive; default = 5; description = "Heartbeat interval in seconds."; };
                leadershipTimeoutSec = mkOption { type = positive; default = 30; description = "Leadership timeout in seconds."; };
                handoffTimeoutSec = mkOption { type = positive; default = 10; description = "Handoff timeout in seconds."; };
              };
            };
            default = { };
            description = "Coordination settings.";
          };

          generatedUnits = mkOption {
            type = listOf str;
            default = [ ];
            internal = true;
            description = "Systemd units generated for node services.";
          };
        };
      };
      default = { };
      description = "Node ecosystem configuration.";
    };

    observability = mkOption {
      type = submodule {
        options = {
          enable = mkOption { type = bool; default = true; description = "Enable observability features."; };
          logDir = mkOption {
            type = path;
            default = cfg.stateRoot + "/logs";
            defaultText = literalExpression "config.services.sinex.stateRoot + \"/logs\"";
            description = "Directory used for log files.";
          };

          monitoring = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable Prometheus/Grafana stack."; };

                prometheus = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption { type = bool; default = true; description = "Enable Prometheus."; };
                      listen = mkOption { type = str; default = "127.0.0.1"; description = "Prometheus bind address."; };
                      port = mkOption { type = port; default = 9090; description = "Prometheus port."; };
                      retention = mkOption { type = str; default = "30d"; description = "Prometheus retention window."; };
                      extraScrapeConfigs = mkOption { type = listOf attrs; default = [ ]; description = "Additional scrape configs merged in."; };
                    };
                  };
                  default = { };
                  description = "Prometheus configuration.";
                };

                grafana = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption { type = bool; default = false; description = "Enable Grafana."; };
                      port = mkOption { type = port; default = 3000; description = "Grafana port."; };
                      secretKey = mkOption {
                        type = nullOr str;
                        default = null;
                        description = ''
                          Optional literal Grafana secret key.
                          When unset, the module first looks for the conventional secret sources
                          <literal>sinex-grafana-secret-key</literal> or
                          <literal>grafana-secret-key</literal> (including
                          <literal>/etc/sinex/grafana-secret-key</literal> when declared via
                          <literal>environment.etc</literal>), and otherwise derives a stable
                          host-local key automatically.
                        '';
                      };
                      secretKeyFile = mkOption {
                        type = nullOr path;
                        default = null;
                        description = ''
                          Optional path to a Grafana secret key file.
                          This is only needed when you want the module to read the key from a
                          specific file instead of using the agenix convention or the derived
                          declarative default.
                        '';
                      };
                    };
                  };
                  default = { };
                  description = "Grafana configuration.";
                };

                exporters = mkOption {
                  type = submodule {
                    options = {
                      node = mkOption { type = bool; default = true; description = "Enable node exporter."; };
                      postgres = mkOption { type = bool; default = true; description = "Enable postgres exporter."; };
                      nats = mkOption {
                        type = bool;
                        default = true;
                        description = ''
                          Enable the NATS Prometheus exporter (prometheus-nats-exporter).
                          Requires pkgs.prometheus-nats-exporter to be available.
                          Scrapes the NATS HTTP monitoring endpoint and re-exposes metrics
                          in Prometheus format on port 7777.
                        '';
                      };
                    };
                  };
                  default = { };
                  description = "Exporter configuration.";
                };
              };
            };
            default = { };
            description = "Monitoring stack.";
          };

          logging = mkOption {
            type = submodule {
              options = {
                structured = mkOption { type = bool; default = true; description = "Enable structured JSON logging."; };
                retention = mkOption {
                  type = submodule {
                    options = {
                      files = mkOption { type = positive; default = 10; description = "Max rotated files."; };
                      size = mkOption { type = str; default = "100M"; description = "Max size per log file."; };
                      age = mkOption { type = str; default = "30d"; description = "Maximum log age."; };
                    };
                  };
                  default = { };
                  description = "Log retention policy.";
                };
              };
            };
            default = { };
            description = "Logging configuration.";
          };

          alerts = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = false; description = "Enable Prometheus alert rules."; };
                rulesFiles = mkOption { type = pathList; default = [ ]; description = "Alert rule files to include."; };
              };
            };
            default = { };
            description = "Alerting configuration.";
          };
        };
      };
      default = { };
      description = "Observability configuration.";
    };

    lifecycle = mkOption {
      type = submodule {
        options = {
          preflight = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable preflight verification gates."; };
                timeoutSec = mkOption { type = positive; default = 120; description = "Preflight timeout in seconds."; };
                schemaApplyTimeoutSec = mkOption {
                  type = positive;
                  default = 600;
                  description = "Schema-apply timeout in seconds.";
                };
                skip = mkOption {
                  type = listOf (enum [
                    "database"
                    "extensions"
                    "migrations"
                    "resources"
                    "configuration"
                    "services"
                    "integration"
                  ]);
                  default = [ ];
                  description = "Phases to skip during preflight verification.";
                };
                failureAction = mkOption {
                  type = enum [ "abort" "warn" "ignore" ];
                  default = "abort";
                  description = "Action when preflight fails.";
                };
              };
            };
            default = { };
            description = "Preflight verification configuration.";
          };

          updates = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable coordinated updates."; };
                gracePeriodSec = mkOption { type = positive; default = 30; description = "Grace period before restarting units."; };
                healthCheckTimeoutSec = mkOption { type = positive; default = 60; description = "Time to wait for units to become healthy."; };
                rollbackOnFailure = mkOption { type = bool; default = true; description = "Rollback units if update fails."; };
                preserveData = mkOption { type = bool; default = true; description = "Preserve DLQ data during update."; };
                units = mkOption { type = strList; default = [ ]; description = "Explicit list of units to manage (empty derives)."; };
              };
            };
            default = { };
            description = "Coordinated update configuration.";
          };

          maintenance = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable maintenance timers."; };
                tasks = mkOption {
                  type = submodule {
                    options = {
                      dlq = mkOption { type = bool; default = true; description = "Run DLQ cleanup timer."; };
                      blobGc = mkOption { type = bool; default = true; description = "Run blob garbage collection."; };
                      blobFsck = mkOption { type = bool; default = true; description = "Run blob fsck timer."; };
                      custom = mkOption { type = strList; default = [ ]; description = "Additional maintenance units to start."; };
                    };
                  };
                  default = { };
                  description = "Maintenance task selection.";
                };
              };
            };
            default = { };
            description = "Maintenance configuration.";
          };
        };
      };
      default = { };
      description = "Lifecycle management configuration.";
    };

    shell = mkOption {
      type = submodule {
        options = {
          asciinema = mkOption {
            type = submodule {
              options = {
                autoRecord = mkOption { type = bool; default = false; description = "Automatically record shell sessions with asciinema."; };
                recordingsPath = mkOption {
                  type = str;
                  default = cfg.stateRoot + "/asciinema";
                  defaultText = literalExpression "config.services.sinex.stateRoot + \"/asciinema\"";
                  description = "Path where recordings are stored.";
                };
              };
            };
            default = { };
            description = "Shell recording configuration.";
          };

          kitty = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable Kitty integration."; };
                autoConfigure = mkOption { type = bool; default = true; description = "Automatically patch kitty.conf."; };
                configFile = mkOption { type = str; default = "~/.config/kitty/kitty.conf"; description = "Path to kitty configuration file."; };
                snippet = mkOption {
                  type = lines;
                  default = defaultKittySnippet;
                  defaultText = literalExpression "defaultKittySnippet";
                  description = "Configuration snippet to inject.";
                };
              };
            };
            default = { };
            description = "Kitty terminal integration.";
          };
        };
      };
      default = { };
      description = "Developer ergonomics configuration.";
    };

    secrets = mkOption {
      type = submodule {
        options = {
          enableAgenix = mkOption {
            type = bool;
            default = true;
            description = "Enable agenix integration for secret management.";
          };

          secretsDirectory = mkOption {
            type = nullOr path;
            default = null;
            description = ''
              Path to the directory containing age-encrypted secret files (.age).
              When null, defaults to the <literal>nixos/secret/</literal> directory in the Sinex
              source tree (i.e., adjacent to the Sinex NixOS modules).
              Set this when importing the Sinex NixOS module from an external flake and
              storing secrets in a project-specific location.
            '';
          };

          gatewayAdminTokenFile = mkOption {
            type = nullOr str;
            default = null;
            description = ''
              Optional path to the gateway admin token file.
              When unset, the module first looks for the conventional secret sources
              <literal>sinex-gateway-admin-token</literal> (agenix) and
              <literal>/etc/sinex/gateway-admin-token</literal> (declarative environment.etc),
              and the gateway refuses to start only if none of those exist.
            '';
          };
        };
      };
      default = { };
      description = "Secret management integration.";
    };
  };

  config =
    let
      stateRoot = cfg.stateRoot;
      runtimeDir = "${stateRoot}/run";
      spoolBase = "${stateRoot}/spool";
      nodesSpool = "${spoolBase}/nodes";
      ingestSpool = cfg.core.ingestd.spoolDir;
      logDir = cfg.observability.logDir;
      dlqDir = cfg.storage.dlq.path;
      blobDir = cfg.storage.blob.repositoryPath;
      sinexUser = cfg.users.nodes;
      targetUser = cfg.users.target;
      targetHome =
        if targetUser == null then null
        else lib.attrByPath [ "users" "users" targetUser "home" ] "/home/${targetUser}" config;
      targetGroup =
        if targetUser == null then null
        else lib.attrByPath [ "users" "users" targetUser "group" ] "users" config;
      targetUid =
        if targetUser == null then null
        else lib.attrByPath [ "users" "users" targetUser "uid" ] null config;
      effectiveDocumentRoots =
        if cfg.nodes.document.allowedRoots != [ ] then cfg.nodes.document.allowedRoots
        else if targetHome == null then [ ]
        else [ "${targetHome}/Documents" ];
      dbUser = cfg.database.user;
      dbCfg = cfg.database;
      databaseUrl = renderDatabaseUrl dbCfg;
      secretPaths = config.sinex.secrets.paths or { };
      resolveSecretPath = resolveNamedSecretPath secretPaths;
      gatewayAdminTokenFile =
        resolveSecretPath cfg.secrets.gatewayAdminTokenFile [
          "sinex-gateway-admin-token"
        ];
      effectiveDatabasePasswordFile = resolveSecretPath cfg.database.passwordFile [
        "sinex-local-db"
        "sinex-remote-db"
      ];
      natsTlsCfg = cfg.nodes.nats.tls;
      natsAuthCfg = cfg.nodes.nats.auth;
      effectiveNatsCaCertFile = resolveSecretPath natsTlsCfg.caCertFile [
        "sinex-nats-ca"
        "nats-ca"
        "sinex-remote-nats-ca"
      ];
      effectiveNatsClientCertFile = resolveSecretPath natsTlsCfg.clientCertFile [
        "sinex-nats-client-cert"
        "nats-client-cert"
        "sinex-remote-nats-cert"
      ];
      effectiveNatsClientKeyFile = resolveSecretPath natsTlsCfg.clientKeyFile [
        "sinex-nats-client-key"
        "nats-client-key"
        "sinex-remote-nats-key"
      ];
      effectiveNatsTokenFile = resolveSecretPath natsAuthCfg.tokenFile [
        "sinex-nats-token"
        "nats-token"
      ];
      effectiveNatsCredsFile = resolveSecretPath natsAuthCfg.credsFile [
        "sinex-nats-client-creds"
        "nats-client-creds"
      ];
      effectiveNatsNkeySeedFile = resolveSecretPath natsAuthCfg.nkeySeedFile [
        "sinex-nats-client-nkey"
        "nats-client-nkey"
      ];
      gatewayTlsCertFile = cfg.core.gateway.tlsCertFile;
      gatewayTlsKeyFile = cfg.core.gateway.tlsKeyFile;
      gatewayTlsTrustAnchorFile =
        if cfg.core.gateway.autoGenerateTls then runtimeDir + "/gateway-ca.pem" else null;
      gatewayTlsClientCAFile = cfg.core.gateway.tlsClientCAFile;
      gatewayProbeListenAddress =
        if hasPrefix "0.0.0.0:" cfg.core.gateway.listenAddress then
          "127.0.0.1:${removePrefix "0.0.0.0:" cfg.core.gateway.listenAddress}"
        else if hasPrefix "[::]:" cfg.core.gateway.listenAddress then
          "[::1]:${removePrefix "[::]:" cfg.core.gateway.listenAddress}"
        else
          cfg.core.gateway.listenAddress;
      gatewayProbeBaseUrl =
        if cfg.core.enable && cfg.core.gateway.enable then
          "https://${gatewayProbeListenAddress}"
        else
          null;
      deploymentManagedUnits = lib.unique (
        (lib.optionals (cfg.enable && cfg.core.enable) [ "sinex-ingestd.service" ])
        ++ (lib.optionals (cfg.enable && cfg.core.enable && cfg.core.gateway.enable) [ "sinex-gateway.service" ])
        ++ lib.optionals cfg.enable (map (name: "${name}.service") (config.sinex._generatedUnits or [ ]))
      );
      resolveNodeInstances = nodeInstances:
        if nodeInstances == null then
          if cfg.enable then cfg.nodes.defaults.instances else 1
        else
          nodeInstances;
      mkDeploymentSurface = enabled: instances: {
        inherit enabled;
        instances = if enabled then resolveNodeInstances instances else null;
      };
      deploymentReadinessDescriptor = {
        version = 1;
        mode = if cfg.enable then "enabled" else "prepared";
        source = "nixos";
        managed_units = deploymentManagedUnits;
        target =
          if targetUser == null then null
          else {
            user = targetUser;
            uid = targetUid;
            home = targetHome;
          };
        database = {
          enabled = dbCfg.enable;
          host = dbCfg.host;
          port = dbCfg.port;
          name = dbCfg.name;
          user = dbCfg.user;
          local_auth = dbCfg.localAuth;
          password_required = dbCfg.localAuth != "trust";
        };
        gateway = {
          base_url = gatewayProbeBaseUrl;
          require_client_tls = cfg.core.gateway.requireClientTLS;
        };
        nats = {
          servers = cfg.nodes.nats.servers;
        };
        filesystem = mkDeploymentSurface (cfg.nodes.enable && cfg.nodes.filesystem.enable) cfg.nodes.filesystem.instances;
        terminal =
          (mkDeploymentSurface (cfg.nodes.enable && cfg.nodes.terminal.enable) cfg.nodes.terminal.instances)
          // {
            kitty_enabled = cfg.shell.kitty.enable;
            history_sources = map
              (source: {
                path = source.path;
                shell = source.shell;
              })
              cfg.nodes.terminal.historySources;
          };
        browser =
          (mkDeploymentSurface (cfg.nodes.enable && cfg.nodes.browser.enable) cfg.nodes.browser.instances)
          // {
            dump_sources = cfg.nodes.browser.dumpSources;
            sqlite_sources = map
              (source: {
                path = source.path;
                browser = source.browser;
                format = source.format;
              })
              cfg.nodes.browser.sqliteSources;
            polling_interval_secs = cfg.nodes.browser.pollIntervalSec;
          };
        desktop =
          (mkDeploymentSurface (cfg.nodes.enable && cfg.nodes.desktop.enable) cfg.nodes.desktop.instances)
          // {
            clipboard_enabled = cfg.nodes.desktop.clipboard.enable;
            activitywatch_db_path = cfg.nodes.desktop.history.activitywatchDbPath;
            runtime_dir = cfg.nodes.desktop.session.runtimeDir;
            wayland_display = cfg.nodes.desktop.session.waylandDisplay;
            hyprland_instance_signature = cfg.nodes.desktop.session.hyprlandInstanceSignature;
            hyprland_event_socket = cfg.nodes.desktop.session.hyprlandEventSocket;
            hyprland_command_socket = cfg.nodes.desktop.session.hyprlandCommandSocket;
          };
        system = mkDeploymentSurface (cfg.nodes.enable && cfg.nodes.system.enable) cfg.nodes.system.instances;
        document =
          (mkDeploymentSurface (cfg.nodes.enable && cfg.nodes.document.enable) null)
          // {
            allowed_roots = effectiveDocumentRoots;
            scan_service_unit =
              if cfg.nodes.enable && cfg.nodes.document.enable then
                "sinex-document-scan.service"
              else
                null;
            timer_unit =
              if cfg.nodes.enable && cfg.nodes.document.enable && cfg.nodes.document.schedule != null then
                "sinex-document-scan.timer"
              else
                null;
            schedule = cfg.nodes.document.schedule;
            run_on_boot = cfg.nodes.document.runOnBoot;
          };
        automata =
          (mkDeploymentSurface (cfg.nodes.enable && cfg.nodes.automata.enable) null)
          // {
            canonicalizer =
              cfg.nodes.enable
              && cfg.nodes.automata.enable
              && cfg.nodes.automata.canonicalizer.enable;
            health_aggregator =
              cfg.nodes.enable
              && cfg.nodes.automata.enable
              && cfg.nodes.automata.healthAggregator.enable;
            analytics_automaton =
              cfg.nodes.enable
              && cfg.nodes.automata.enable
              && cfg.nodes.automata.analyticsAutomaton.enable;
            session_detector =
              cfg.nodes.enable
              && cfg.nodes.automata.enable
              && cfg.nodes.automata.sessionDetector.enable;
          };
        expectations = {
          schema_apply = cfg.database.enable && cfg.database.autoSetup;
          nats_streams = cfg.enable && (cfg.core.enable || cfg.nodes.enable);
          gateway_ready = cfg.enable && cfg.core.enable && cfg.core.gateway.enable;
        };
        secrets = {
          database_password_file = effectiveDatabasePasswordFile;
          gateway_admin_token_file = gatewayAdminTokenFile;
          gateway_tls_cert_file = gatewayTlsCertFile;
          gateway_tls_key_file = gatewayTlsKeyFile;
          gateway_tls_trust_anchor_file = gatewayTlsTrustAnchorFile;
          gateway_tls_client_ca_file = gatewayTlsClientCAFile;
          nats_ca_cert_file = effectiveNatsCaCertFile;
          nats_client_cert_file = effectiveNatsClientCertFile;
          nats_client_key_file = effectiveNatsClientKeyFile;
          nats_token_file = effectiveNatsTokenFile;
          nats_creds_file = effectiveNatsCredsFile;
          nats_nkey_seed_file = effectiveNatsNkeySeedFile;
        };
      };
      deploymentReadinessDescriptorJson = builtins.toJSON deploymentReadinessDescriptor;
      deploymentReadinessDescriptorFile = pkgs.writeText "sinex-deployment-readiness.json" deploymentReadinessDescriptorJson;
      runtimeTargetDescriptor = {
        version = 1;
        name =
          if targetUser == null then
            "deployed-host"
          else
            "deployed-host:${targetUser}";
        kind = "deployed_host";
        source = "nixos";
        source_path = "/etc/sinex/runtime-target.json";
        database = {
          url = databaseUrl;
          host = dbCfg.host;
          port = dbCfg.port;
          name = dbCfg.name;
          user = dbCfg.user;
          password_file = effectiveDatabasePasswordFile;
          password_required = dbCfg.localAuth != "trust";
        };
        gateway = {
          base_url = gatewayProbeBaseUrl;
          token_file = gatewayAdminTokenFile;
          token_role = "admin";
          ca_cert_file = gatewayTlsTrustAnchorFile;
          client_cert_file = null;
          client_key_file = null;
          require_client_tls = cfg.core.gateway.requireClientTLS;
          insecure = false;
        };
        nats = {
          servers = cfg.nodes.nats.servers;
          environment = cfg.nats.environment;
          token_file = effectiveNatsTokenFile;
          creds_file = effectiveNatsCredsFile;
          nkey_seed_file = effectiveNatsNkeySeedFile;
          ca_cert_file = effectiveNatsCaCertFile;
          client_cert_file = effectiveNatsClientCertFile;
          client_key_file = effectiveNatsClientKeyFile;
        };
        state = {
          state_dir = stateRoot;
          cache_dir = null;
        };
        services = {
          managed_units = deploymentManagedUnits;
        };
        notes = [
          "Generated by the NixOS module. This is the runtime connection/status target; deployment-readiness remains the broader host proof descriptor."
        ];
      };
      runtimeTargetDescriptorJson = builtins.toJSON runtimeTargetDescriptor;
      runtimeTargetDescriptorFile = pkgs.writeText "sinex-runtime-target.json" runtimeTargetDescriptorJson;
      dlqCleanupScript = if cfg.cliPackage == null then null else
      pkgs.writeShellScript "sinex-dlq-cleanup" ''
        set -euo pipefail

        CLI_BIN="${cfg.cliPackage}/bin/sinexctl"
        if [ ! -x "$CLI_BIN" ]; then
          echo "sinexctl not found at $CLI_BIN" >&2
          exit 1
        fi

        "$CLI_BIN" dlq cleanup \
          --older-than ${cfg.storage.dlq.cleanup.maxAge} \
          --max-files ${toString cfg.storage.dlq.cleanup.maxFiles} \
          --confirm
      '';
      asciinemaDir = cfg.shell.asciinema.recordingsPath;
      asciiPath = toString asciinemaDir;

      directoryRules =
        [
          { path = stateRoot; mode = "0755"; }
          { path = runtimeDir; mode = "0755"; }
          { path = spoolBase; mode = "0755"; }
          { path = nodesSpool; mode = "0755"; }
          { path = ingestSpool; mode = "0755"; }
          { path = logDir; mode = "0755"; }
        ]
        ++ optionals (cfg.storage.dlq.enable) [{ path = dlqDir; mode = "0750"; }]
        ++ optionals (cfg.storage.blob.enable) [{ path = blobDir; mode = "0750"; }]
        ++ optionals (cfg.core.enable && cfg.core.gateway.autoGenerateTls) [{ path = "${stateRoot}/tls"; mode = "0750"; }]
        ++ optionals (cfg.shell.asciinema.autoRecord && targetUser != null && hasPrefix "/" asciiPath) [
          { path = asciiPath; mode = "0770"; user = targetUser; group = targetGroup; }
        ];
      tmpRule = rule:
        let
          owner = rule.user or sinexUser;
          group = rule.group or sinexUser;
        in
        "d ${rule.path} ${rule.mode} ${owner} ${group} -";
    in
    mkMerge [
      (mkIf cfg.enable {
        assertions = [
          {
            assertion = cfg.package != null;
            message = "services.sinex.package must be set when services.sinex.enable = true.";
          }
          {
            assertion = (!cfg.core.enable || !cfg.core.gateway.enable) || gatewayAdminTokenFile != null;
            message = ''
              Gateway requires an admin token file. Set services.sinex.secrets.gatewayAdminTokenFile,
              provide an agenix secret named sinex-gateway-admin-token, or define
              environment.etc."sinex/gateway-admin-token".
            '';
          }
          {
            assertion =
              (!cfg.core.enable || !cfg.core.gateway.enable)
              || (gatewayTlsCertFile != null && gatewayTlsKeyFile != null);
            message = "Gateway TCP/TLS requires tlsCertFile and tlsKeyFile when gateway is enabled.";
          }
          {
            # Non-loopback bindings must enforce mTLS; loopback-only listeners are trusted.
            assertion =
              (!cfg.core.enable || !cfg.core.gateway.enable)
              || (hasPrefix "127." cfg.core.gateway.listenAddress)
              || (hasPrefix "[::1]" cfg.core.gateway.listenAddress)
              || cfg.core.gateway.requireClientTLS;
            message = "Gateway binds to non-loopback address '${cfg.core.gateway.listenAddress}'; set services.sinex.core.gateway.requireClientTLS = true and configure tlsClientCAFile.";
          }
          {
            # mTLS requires a client CA bundle to verify the certificates presented by clients.
            assertion =
              (!cfg.core.enable || !cfg.core.gateway.enable)
              || (!cfg.core.gateway.requireClientTLS)
              || (gatewayTlsClientCAFile != null);
            message = "Gateway mTLS (requireClientTLS = true) requires tlsClientCAFile. Set services.sinex.core.gateway.tlsClientCAFile.";
          }
          {
            assertion = (effectiveNatsClientCertFile == null) == (effectiveNatsClientKeyFile == null);
            message = "NATS mutual TLS requires both services.sinex.nodes.nats.tls.clientCertFile/clientKeyFile or matching agenix secrets named sinex-nats-client-cert and sinex-nats-client-key.";
          }
          {
            assertion =
              length
                (filter (x: x != null) [
                  effectiveNatsTokenFile
                  effectiveNatsCredsFile
                  effectiveNatsNkeySeedFile
                ]) <= 1;
            message = "Configure at most one NATS auth mode under services.sinex.nodes.nats.auth: tokenFile, credsFile, or nkeySeedFile.";
          }
          {
            assertion =
              (!(cfg.nats.enable || cfg.nats.autoSetup))
              || (!cfg.nats.tls.verifyClients && !cfg.nats.tls.verifyAndMap)
              || (effectiveNatsClientCertFile != null && effectiveNatsClientKeyFile != null);
            message = "Managed NATS client-certificate verification requires services.sinex.nodes.nats.tls.clientCertFile/clientKeyFile or matching agenix secrets named sinex-nats-client-cert and sinex-nats-client-key.";
          }
          {
            assertion =
              (!cfg.nodes.enable || !cfg.nodes.document.enable)
              || effectiveDocumentRoots != [ ];
            message = ''
              Document ingestion is enabled but no allowed roots resolved. Set
              services.sinex.nodes.document.allowedRoots explicitly or configure
              services.sinex.users.target so the module can derive $HOME/Documents.
            '';
          }
          {
            assertion =
              (!cfg.nodes.enable || !cfg.nodes.document.enable)
              || cfg.nodes.document.runOnBoot
              || cfg.nodes.document.schedule != null;
            message = ''
              Document ingestion is enabled but neither runOnBoot nor schedule is set.
              Enable at least one so the managed document scan surface actually runs.
            '';
          }
        ];
        environment.systemPackages = mkAfter (
          [ pkgs.dbus pkgs.git pkgs.git-annex ]
          ++ optionals cfg.shell.asciinema.autoRecord [ pkgs.asciinema ]
        );
      })

      (mkIf (cfg.cliPackage != null) {
        environment.systemPackages = mkAfter [ cfg.cliPackage ];
      })

      (mkIf (cfg.enable || (cfg.database.enable && cfg.database.autoSetup) || cfg.storage.blob.enable) {
        users.groups.${dbUser} = { };
        users.users.${dbUser} = {
          isSystemUser = true;
          group = dbUser;
          description = "Sinex database account";
          home = stateRoot;
          createHome = true;
        };
      })

      (mkIf ((cfg.enable || cfg.storage.blob.enable || cfg.lifecycle.maintenance.enable) && cfg.users.nodes != dbUser) {
        users.groups.${sinexUser} = { };
        users.users.${sinexUser} = {
          isSystemUser = true;
          group = sinexUser;
          description = "Sinex service account";
          home = stateRoot;
          createHome = true;
        };
      })

      (mkIf (cfg.enable || cfg.storage.dlq.enable || cfg.storage.blob.enable) {
        systemd.tmpfiles.rules = mkAfter (map tmpRule directoryRules);
      })

      (mkIf (cfg.enable && cfg.shell.asciinema.autoRecord) {
        programs.bash.promptInit = ''
          # Automatic asciinema recording for Sinex
          if [[ -z "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
            export ASCIINEMA_REC=1
            ASCIINEMA_DIR="${cfg.shell.asciinema.recordingsPath}"
            if [[ "$ASCIINEMA_DIR" == "~/"* ]]; then
              ASCIINEMA_DIR="$HOME/''${ASCIINEMA_DIR#~/}"
            fi
            mkdir -p "$ASCIINEMA_DIR"
            exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
          fi
        '';
        programs.zsh.promptInit = ''
          # Automatic asciinema recording for Sinex
          if [[ -z "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
            export ASCIINEMA_REC=1
            ASCIINEMA_DIR="${cfg.shell.asciinema.recordingsPath}"
            if [[ "$ASCIINEMA_DIR" == "~/"* ]]; then
              ASCIINEMA_DIR="$HOME/''${ASCIINEMA_DIR#~/}"
            fi
            mkdir -p "$ASCIINEMA_DIR"
            exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
          fi
        '';
      })

      (mkIf cfg.enable {
        services.sinex.database.autoSetup = mkDefault true;
        services.sinex.database.name = mkDefault "sinex_${cfg.nats.environment}";
        services.sinex.nats.autoSetup = mkDefault true;
      })

      # When a target user is configured with no explicit watchPaths, default to
      # watching that user's home directory. Keeping these defaults live in
      # prepared mode makes the deployment descriptor honest before first enable.
      # NB: guard only on cfg.users.target — reading cfg.nodes.* while writing to
      # services.sinex.nodes.* creates an evaluation cycle.
      # mkDefault ensures explicit watchPaths override this fallback.
      (mkIf (cfg.users.target != null) {
        services.sinex.nodes.filesystem.watchPaths = mkDefault [
          targetHome
        ];
      })

      (mkIf (targetHome != null) {
        services.sinex.nodes.terminal.historySources = mkDefault [
          {
            path = "${targetHome}/.bash_history";
            shell = "bash";
          }
          {
            path = "${targetHome}/.zsh_history";
            shell = "zsh";
          }
          {
            path = "${targetHome}/.local/share/atuin/history.db";
            shell = "atuin";
          }
          {
            path = "${targetHome}/.local/share/fish/fish_history";
            shell = "fish";
          }
        ];
      })

      (mkIf (targetHome != null) {
        services.sinex.nodes.browser.dumpSources = mkDefault [ ];
        services.sinex.nodes.browser.sqliteSources = mkDefault [
          {
            path = "${targetHome}/.local/share/qutebrowser/history.sqlite";
            browser = "qutebrowser";
            format = "QutebrowserNative";
          }
          {
            path = "${targetHome}/.local/share/qutebrowser/webengine/History";
            browser = "qutebrowser";
            format = "ChromiumHistory";
          }
        ];
      })

      (mkIf (cfg.enable || targetUser != null) {
        environment.etc."sinex/deployment-readiness.json".source = deploymentReadinessDescriptorFile;
        environment.etc."sinex/runtime-target.json".source = runtimeTargetDescriptorFile;
      })

      (mkIf (targetUser != null) {
        services.sinex.nodes.filesystem.instances = mkDefault 1;
        services.sinex.nodes.terminal.instances = mkDefault 1;
        services.sinex.nodes.browser.instances = mkDefault 1;
        services.sinex.nodes.desktop.instances = mkDefault 1;
        services.sinex.nodes.system.instances = mkDefault 1;
      })

      (mkIf (targetUid != null) {
        services.sinex.nodes.desktop.session.runtimeDir =
          mkDefault "/run/user/${toString targetUid}";
      })

      (mkIf (targetHome != null) {
        services.sinex.nodes.desktop.history.activitywatchDbPath =
          mkDefault "${targetHome}/.local/share/activitywatch/aw-server-rust/sqlite.db";
      })

      (mkIf (cfg.storage.dlq.enable && cfg.lifecycle.maintenance.enable && cfg.lifecycle.maintenance.tasks.dlq && cfg.cliPackage != null) {
        systemd.services.sinex-dlq-cleanup = {
          description = "Sinex DLQ cleanup";
          serviceConfig = {
            Environment = [
              "DATABASE_URL=${databaseUrl}"
              "SINEX_DLQ_PATH=${dlqDir}"
            ];
            ExecStart = mkDatabasePasswordExec {
              name = "dlq-cleanup";
              command = dlqCleanupScript;
              passwordFile = if cfg.database.enable then effectiveDatabasePasswordFile else null;
            };
            # Retry within the same calendar window if cleanup fails
            # (e.g. gateway unavailable, transient I/O error).
            Restart = "on-failure";
            RestartSec = 300;
          } // mkHelperServiceConfig {
            user = sinexUser;
            group = sinexUser;
            readWritePaths = [ dlqDir ];
          };
        };

        systemd.timers.sinex-dlq-cleanup = {
          description = "Timer for Sinex DLQ cleanup";
          wantedBy = [ "timers.target" ];
          timerConfig = {
            OnCalendar = cfg.storage.dlq.cleanup.schedule;
            Persistent = true;
          };
        };
      })

      (mkIf (cfg.nats.enable || cfg.nats.autoSetup) {
        services.sinex.nodes.nats.servers = mkDefault [
          "${if cfg.nats.tls.enable then "tls" else "nats"}://${cfg.nats.host}:${toString cfg.nats.port}"
        ];
      })
    ];
}
