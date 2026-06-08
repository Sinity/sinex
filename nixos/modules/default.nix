{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  systemdHardening = import ./lib/systemd-hardening.nix { inherit lib; };
  databaseRuntime = import ./lib/database-runtime.nix { inherit lib pkgs; };
  secretResolution = import ./lib/secret-resolution.nix { inherit lib; };
  automataLib = import ./lib/automata.nix { inherit lib; };
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

  defaultAdminPackage =
    if pkgs ? xtask then pkgs.xtask else cfg.package;

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

  terminalSourceIdForShell = shell:
    let
      normalized = toLower shell;
    in
    if normalized == "atuin" then "terminal.atuin-history"
    else if normalized == "zsh" then "terminal.zsh-history"
    else if normalized == "fish" then "terminal.fish-history"
    else if normalized == "bash" then "terminal.bash-history"
    else "terminal.text-history";

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
    ./sources.nix
    ./source-bindings.nix
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
    resourceModule =
      { defaultMemory
      , defaultCpu
      , defaultShutdownSec ? 90
      , defaultOpenFiles ? null
      , defaultCpuWeight ? 10
      , defaultIoWeight ? 10
      , defaultIoSchedulingClass ? "idle"
      , defaultNice ? 10
      }:
      submodule {
        options = {
          memoryHigh = mkOption {
            type = str;
            default = defaultMemory;
            description = "systemd MemoryHigh soft memory pressure threshold.";
          };
          memoryMax = mkOption {
            type = nullOr str;
            default = null;
            example = defaultMemory;
            description = "systemd MemoryMax hard memory limit. Null leaves the unit uncapped.";
          };
          cpuQuota = mkOption {
            type = nullOr str;
            default = null;
            example = defaultCpu;
            description = "systemd CPUQuota limit. Null leaves CPU throughput uncapped.";
          };
          cpuWeight = mkOption {
            type = ints.between 1 10000;
            default = defaultCpuWeight;
            description = "systemd CPUWeight scheduling weight.";
          };
          ioWeight = mkOption {
            type = ints.between 1 10000;
            default = defaultIoWeight;
            description = "systemd IOWeight scheduling weight.";
          };
          ioSchedulingClass = mkOption {
            type = enum [ "idle" "best-effort" "realtime" ];
            default = defaultIoSchedulingClass;
            description = "systemd IOSchedulingClass.";
          };
          nice = mkOption {
            type = ints.between (-20) 19;
            default = defaultNice;
            description = "systemd Nice value.";
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
        sourceId = mkOption {
          type = nullOr str;
          default = null;
          description = ''
            Optional stable source identity. When null, the module derives
            one from <option>shell</option> and passes it through
            <literal>--source</literal>.
          '';
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
      description = "Sinex runtime package that provides service binaries.";
    };

    adminPackage = mkOption {
      type = package;
      default = defaultAdminPackage;
      defaultText = literalExpression "pkgs.xtask or config.services.sinex.package";
      description = "Package that provides managed deployment/admin helpers such as xtask.";
    };

    cliPackage = mkOption {
      type = nullOr package;
      default = defaultCliPackage;
      defaultText = literalExpression "pkgs.sinexctl or null";
      description = "Optional human/operator CLI package placed on PATH when present.";
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

          runtime = mkOption {
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

          setupWaitForPaths = mkOption {
            type = with types; listOf path;
            default = [ ];
            example = literalExpression ''[ "/run/agenix/sinex-local-db" ]'';
            description = ''
              Paths whose readability gates the start of postgresql-setup.service.
              Each entry becomes a ConditionPathIsReadable= entry on the unit.

              Use when a secret materializer (e.g. agenix, sops-nix) provides the
              database password file late in the boot sequence and
              postgresql-setup must wait for it.
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
                  default = 4;
                  description = "Maximum connections per Sinex process.";
                };
                minConnections = mkOption {
                  type = positive;
                  default = 1;
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

          walArchiveCommand = mkOption {
            type = nullOr str;
            default = null;
            example = "pg_basebackup -D /backup/sinex/$(date +%Y%m%d-%H%M) -Ft -z -P";
            description = ''
              Optional shell command for continuous WAL archiving and base backups.

              When set, PostgreSQL's `archive_command` is configured to invoke this
              command for each completed WAL segment, enabling point-in-time recovery.
              A typical setup uses `pg_basebackup` for periodic full backups and this
              setting for continuous WAL shipping.

              Example with WAL-G:
                `wal-g wal-push /var/lib/postgresql/%p`

              Example with pg_basebackup (periodic, not per-WAL):
                Set a periodic snapshot via a systemd timer instead; this option
                enables the archive side so the operator can wire in WAL archiving.

              Leave `null` (the default) when WAL archiving is not required.
              Sinex does not mandate WAL archiving — data events are replayable
              from source materials, so archival is a defense-in-depth measure.
            '';
          };

        };
      };
      default = { };
      description = "PostgreSQL provisioning and connection configuration.";
    };

    storage = mkOption {
      type = submodule {
        options = {
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
                legacyAnnexData = mkOption {
                  type = bool;
                  default = false;
                  description = ''
                    Enable legacy git-annex support. When false (default), only local
                    BLAKE3 CAS storage is used and git-annex is not initialized or called.
                    Set to true if you have existing git-annex blobs that need access or
                    migration.
                  '';
                };
                maxBlobSize = mkOption {
                  type = signed;
                  default = 104857600;
                  description = "Maximum allowed blob size in bytes (default 100 MB). Set to 0 to disable.";
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
                cas = mkOption {
                  type = submodule {
                    options = {
                      maintenance = mkOption {
                        type = submodule {
                          options = {
                            sweep = mkOption {
                              type = submodule {
                                options = {
                                  enable = mkOption {
                                    type = bool;
                                    default = true;
                                    description = ''
                                      Enable periodic `sinexctl blob sweep-orphans` timer
                                      against the local BLAKE3 CAS. Runs only when blob
                                      storage is enabled and legacy git-annex mode is
                                      disabled.
                                    '';
                                  };
                                  schedule = mkOption {
                                    type = str;
                                    default = "weekly";
                                    description = "OnCalendar schedule for the CAS orphan sweep.";
                                  };
                                  apply = mkOption {
                                    type = bool;
                                    default = true;
                                    description = ''
                                      Pass `--apply` (actually drop orphaned CAS keys).
                                      Set false for a dry-run-only timer (logs orphans
                                      without removing them).
                                    '';
                                  };
                                };
                              };
                              default = { };
                              description = ''
                                Local CAS orphan sweep configuration. Runs `sinexctl blob
                                sweep-orphans` periodically to reclaim content-store keys
                                that no longer have a matching `core.blobs` row.
                              '';
                            };
                            fsck = mkOption {
                              type = submodule {
                                options = {
                                  enable = mkOption {
                                    type = bool;
                                    default = true;
                                    description = ''
                                      Enable periodic `sinexctl blob fsck` timer against
                                      the local BLAKE3 CAS. Runs only when blob storage
                                      is enabled and legacy git-annex mode is disabled.
                                    '';
                                  };
                                  schedule = mkOption {
                                    type = str;
                                    default = "monthly";
                                    description = "OnCalendar schedule for the CAS fsck pass.";
                                  };
                                  apply = mkOption {
                                    type = bool;
                                    default = false;
                                    description = ''
                                      Pass `--apply` (actually remove orphaned CAS files
                                      found during fsck). Default false: dry-run only,
                                      logs the fsck report. Set true for full reclaim.
                                    '';
                                  };
                                };
                              };
                              default = { };
                              description = ''
                                Local CAS filesystem-integrity check configuration. Runs
                                `sinexctl blob fsck` to cross-reference CAS files against
                                `core.blobs` rows and report missing/corrupt entries.
                              '';
                            };
                          };
                        };
                        default = { };
                        description = ''
                          Local BLAKE3 CAS maintenance timers. Distinct from
                          `blob.maintenance.{gc,fsck}` which target the legacy
                          git-annex backend.
                        '';
                      };
                    };
                  };
                  default = { };
                  description = "Local BLAKE3 content-store configuration.";
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
            description = "Enable core Sinex services (event engine and API).";
          };

          event_engine = mkOption {
            type = submodule {
              options = {
                enable = mkOption {
                  type = bool;
                  default = true;
                  description = "Enable the ingestion daemon.";
                };
                spoolDir = mkOption {
                  type = path;
                  default = cfg.stateRoot + "/spool/event_engine";
                  defaultText = literalExpression "config.services.sinex.stateRoot + \"/spool/event_engine\"";
                  description = "Spool directory for event_engine.";
                };
                logLevel = mkOption {
                  type = str;
                  default = cfg.logLevel;
                  defaultText = literalExpression "config.services.sinex.logLevel";
                  description = "Log level for event_engine.";
                };
                batch = mkOption {
                  type = batchModule { defaultSize = 50; defaultTimeout = 2; };
                  default = { };
                  description = "Batch settings for event_engine. Defaults tuned for desktop workloads (low latency). Increase size/timeout for high-throughput server deployments.";
                };
                consumerMaxAckPending = mkOption {
                  type = positive;
                  default = 100;
                  description = "JetStream max_ack_pending for the main event_engine consumer.";
                };
                materialSlicesMaxAckPending = mkOption {
                  type = positive;
                  default = 1000;
                  description = "JetStream max_ack_pending for the material slices consumer.";
                };
                resources = mkOption {
                  type = resourceModule {
                    defaultMemory = "8G";
                    defaultCpu = "100%";
                    defaultOpenFiles = 524288;
                  };
                  default = { };
                  description = "Resource limits for event_engine.";
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
                telemetryIntervalSecs = mkOption {
                  type = positive;
                  default = 60;
                  description = "Interval in seconds between event_engine processing telemetry emissions.";
                };
                blobGcIntervalSecs = mkOption {
                  type = nullOr positive;
                  default = null;
                  description = ''
                    Interval in seconds between automatic blob garbage collection sweeps.
                    When null (default), automatic GC is disabled and orphans must be
                    reclaimed manually via `sinexctl blob sweep-orphans --apply`.
                    Minimum effective value is 60 seconds (validated by event_engine).
                  '';
                };
                extraArgs = mkOption {
                  type = strList;
                  default = [ ];
                  description = "Additional command-line arguments for event_engine.";
                };
              };
            };
            default = { };
            description = "Ingestion daemon configuration.";
          };

          api = mkOption {
            type = submodule {
              options = {
                enable = mkOption {
                  type = bool;
                  default = true;
                  description = "Enable the API.";
                };
                logLevel = mkOption {
                  type = str;
                  default = cfg.logLevel;
                  defaultText = literalExpression "config.services.sinex.logLevel";
                  description = "Log level for the API.";
                };
                resources = mkOption {
                  type = resourceModule { defaultMemory = "8G"; defaultCpu = "75%"; };
                  default = { };
                  description = "Resource limits for the API.";
                };
                listenAddress = mkOption {
                  type = str;
                  default = "127.0.0.1:9999";
                  description = "TCP listen address for the API (host:port).";
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
                        description = "Max concurrent RPC requests enforced by the API.";
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
                              description = "Enable per-token rate limiting on the API.";
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
                            distributedWindowSec = mkOption {
                              type = positive;
                              default = 60;
                              description = "Distributed rate limiter: sliding window duration in seconds.";
                            };
                          };
                        };
                        default = { };
                        description = "Per-token rate limiting. Two complementary limiters operate in tandem: a local token-bucket (fast, in-process) and a distributed NATS KV limiter (consistent across API replicas).";
                      };
                    };
                  };
                  default = { };
                  description = "RPC resource guard configuration for the API.";
                };
                tlsCertFile = mkOption {
                  type = nullOr path;
                  default = if cfg.core.api.autoGenerateTls then cfg.stateRoot + "/tls/server.pem" else null;
                  description = ''
                    Path to the API TLS certificate. Required unless autoGenerateTls is enabled.
                    Exported as <literal>SINEX_API_TLS_CERT</literal>.
                  '';
                };
                tlsKeyFile = mkOption {
                  type = nullOr path;
                  default = if cfg.core.api.autoGenerateTls then cfg.stateRoot + "/tls/server-key.pem" else null;
                  description = ''
                    Path to the API TLS private key. Required unless autoGenerateTls is enabled.
                    Exported as <literal>SINEX_API_TLS_KEY</literal>.
                  '';
                };
                tlsClientCAFile = mkOption {
                  type = nullOr path;
                  default = null;
                  description = ''
                    Client CA bundle for API mTLS. Required for non-loopback binds
                    and whenever requireClientTLS is enabled. Exported as
                    <literal>SINEX_API_TLS_CLIENT_CA</literal>.
                  '';
                };
                autoGenerateTls = mkOption {
                  type = bool;
                  default = false;
                  description = ''
                    Automatically generate an rcgen-backed local PKI for the API on first boot.
                    Stores credentials at
                    <literal>''${stateRoot}/tls/{server.pem,server-key.pem,ca.pem,client.pem,client-key.pem}</literal>
                    and sets <option>tlsCertFile</option>/<option>tlsKeyFile</option> accordingly.
                    The generated CA becomes the API trust anchor for deployment-readiness checks.
                    Suitable for single-host deployments. For production clusters, provide real certs.
                  '';
                };
                corsOrigins = mkOption {
                  type = nullOr str;
                  default = null;
                  description = ''
                    Comma-separated list of allowed CORS origins for the API HTTP interface.
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
                  description = "Additional command-line arguments for the API.";
                };
              };
            };
            default = { };
            description = "API configuration.";
          };
        };
      };
      default = { };
      description = "Core service configuration.";
    };

    runtime = mkOption {
      type = submodule {
        options = {
          enable = mkOption {
            type = bool;
            default = true;
            description = "Enable runtime services.";
          };

          nats = mkOption {
            type = submodule {
              options = {
                servers = mkOption {
                  type = strList;
                  default = [ "nats://127.0.0.1:4222" ];
                  description = ''
                    List of NATS server URLs shared by core services and runtime modules.
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
                  description = "Typed TLS configuration for the shared NATS client connection; exported automatically to core services and runtime modules.";
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
                    core services and runtime modules. Configure at most one auth mode.
                  '';
                };
              };
            };
            default = { };
            description = "Shared NATS client configuration used by core services and runtime modules.";
          };

          defaults = mkOption {
            type = submodule {
              options = {
                instances = mkOption {
                  type = positive;
                  default = 1;
                  description = "Default number of instances per runtime.";
                };
                logLevel = mkOption {
                  type = str;
                  default = cfg.logLevel;
                  defaultText = literalExpression "config.services.sinex.logLevel";
                  description = "Default log level for runtime modules.";
                };
                batch = mkOption {
                  type = batchModule { defaultSize = 100; defaultTimeout = 2; };
                  default = { };
                  description = "Default batching configuration for runtime modules.";
                };
                resources = mkOption {
                  type = resourceModule { defaultMemory = "8G"; defaultCpu = "50%"; };
                  default = { };
                  description = "Default resource limits.";
                };
                env = mkOption {
                  type = envModule;
                  default = { };
                  description = "Environment variables applied to every runtime module.";
                };
              };
            };
            default = { };
            description = "Source runtime defaults.";
          };

          coordination = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = false; description = "Enable runtime coordination."; };
                heartbeatSec = mkOption { type = positive; default = 5; description = "Heartbeat interval in seconds."; };
                leadershipTimeoutSec = mkOption { type = positive; default = 30; description = "Leadership timeout in seconds."; };
                handoffTimeoutSec = mkOption { type = positive; default = 10; description = "Handoff timeout in seconds."; };
              };
            };
            default = { };
            description = "Coordination settings.";
          };


          target = mkOption {
            type = submodule {
              options = {
                attachToMultiUser = mkOption {
                  type = bool;
                  default = true;
                  description = ''
                    When true (default), Sinex runtime services attach to
                    multi-user.target and start at boot. Set to false on
                    hosts that gate the runtime behind a deferred timer or
                    operator action; services attach to
                    sinex-runtime.target instead, which only starts when
                    explicitly pulled.
                  '';
                };
                manualStartOnly = mkOption {
                  type = bool;
                  default = false;
                  description = ''
                    When true, sinex-runtime.target carries
                    X-OnlyManualStart=true so it never starts implicitly.
                    Has no effect when attachToMultiUser = true.
                  '';
                };
                includeDatabase = mkOption {
                  type = bool;
                  default = false;
                  description = ''
                    When true, postgresql + postgresql-setup are pulled into
                    sinex-runtime.target's dependency graph and their
                    multi-user.target attachment is honored by
                    attachToMultiUser. Use this on hosts where Postgres exists
                    solely to serve Sinex and should not start at boot
                    independently of the Sinex runtime. The module does not
                    define postgresql itself; this only adjusts unit gating.
                  '';
                };
                extraAfter = mkOption {
                  type = with types; listOf str;
                  default = [ ];
                  example = [ "network-online.target" ];
                  description = ''
                    Additional units appended to sinex-runtime.target's After=
                    list. Use for ordering against host-specific resources
                    (e.g. network-online.target on hosts that defer the
                    runtime until networking is up).
                  '';
                };
              };
            };
            default = { };
            description = "How runtime services are wired into systemd targets.";
          };

          deferredStart = mkOption {
            type = submodule {
              options = {
                enable = mkOption {
                  type = bool;
                  default = false;
                  description = ''
                    When true, define sinex-runtime.timer with OnActiveSec=
                    set to delay. The timer pulls sinex-runtime.target after
                    boot when autoStart = true; when autoStart = false the
                    timer is defined but not installed into timers.target
                    (introspectable but inert).

                    Use this with attachToMultiUser = false on hosts that
                    bring the runtime up automatically but only after the
                    desktop is settled.
                  '';
                };
                autoStart = mkOption {
                  type = bool;
                  default = true;
                  description = ''
                    When true (default) and enable = true, install the
                    deferred-start timer into timers.target so it fires
                    automatically at boot. When false, the timer is defined
                    but inert; operators can start it manually with
                    systemctl start sinex-runtime.timer.
                  '';
                };
                delay = mkOption {
                  type = str;
                  default = "5min";
                  example = "2min";
                  description = ''
                    OnActiveSec= value for sinex-runtime.timer. Time after
                    boot/timer activation before sinex-runtime.target is
                    pulled.
                  '';
                };
                accuracy = mkOption {
                  type = str;
                  default = "15s";
                  description = "AccuracySec= for the deferred-start timer.";
                };
              };
            };
            default = { };
            description = ''
              Optional timer that automatically pulls sinex-runtime.target
              after a delay. Mutually compatible with
              attachToMultiUser = false; mutually exclusive with
              attachToMultiUser = true (which makes the timer redundant).
            '';
          };

          restartOnSwitch = mkOption {
            type = bool;
            default = true;
            description = ''
              When true (default), NixOS activation restarts changed Sinex
              services. When false, services keep running with the
              previously active code until next manual restart. Workstations
              set false because activation under load triggers I/O pressure
              while large in-memory state (NATS, Postgres) is recreated.
            '';
          };

          restartPolicy = mkOption {
            type = submodule {
              options = {
                mode = mkOption {
                  type = enum [ "no" "on-failure" "always" "on-success" "on-abnormal" "on-watchdog" "on-abort" ];
                  default = "on-failure";
                  description = "systemd Restart= for runtime services.";
                };
                backoffSec = mkOption {
                  type = positive;
                  default = 10;
                  description = "RestartSec=. Delay before retrying after a crash.";
                };
                intervalSec = mkOption {
                  type = unsigned;
                  default = 0;
                  description = ''
                    StartLimitIntervalSec=. 0 disables the rate limit
                    (legacy behaviour for hosted deployments where capture
                    must recover indefinitely).
                  '';
                };
                burst = mkOption {
                  type = unsigned;
                  default = 0;
                  description = ''
                    StartLimitBurst=. Maximum start attempts within
                    intervalSec; unit fails after this. 0 disables.
                  '';
                };
              };
            };
            default = { };
            description = ''
              Failure-recovery policy applied to long-running Sinex services.
              Default suits hosted deployments. Workstations bound the
              policy:

                services.sinex.runtime.restartPolicy = {
                  intervalSec = 600;
                  burst = 3;
                };
            '';
          };
                };
      };
      default = { };
      description = "Sinex runtime lifecycle and shared client configuration.";
    };

    sources = {
      enable = mkOption {
        type = bool;
        default = true;
        description = "Enable source drivers hosted by sinexd.";
      };

      filesystem = mkOption {
        type = submodule {
          options = {
            enable = mkOption { type = bool; default = true; description = "Enable filesystem source. Watches large directory trees; needs more memory than other runtime modules."; };
            watchPaths = mkOption {
              type = strList;
              default = [ ];
              description = ''
                Absolute paths for the filesystem source to watch.
                When empty and <option>services.sinex.users.target</option> is set,
                defaults to the target user's home directory.
              '';
            };
            maxWatches = mkOption {
              type = positive;
              default = 524288;
              description = ''
                Filesystem watch-budget threshold passed to the runtime config.
                When the recursive tree exceeds this value, the runtime now tries a
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
            ignoredFileSuffixes = mkOption {
              type = strList;
              default = [
                # SQLite write-ahead and shared-memory companions; rewritten
                # on every commit by the owning database, never useful as a
                # standalone capture.
                "-wal"
                "-shm"
                "-journal"
                ".wal"
                # pytest's testmondata WAL; same churn pattern as SQLite WAL.
                ".testmondata"
                ".testmondata-wal"
                # Editor swap / lock / temp files.
                ".swp"
                ".swx"
                ".swo"
                "~"
                ".tmp"
                ".part"
                ".crdownload"
              ];
              description = ''
                File-name suffixes excluded from fs source host records.
                Volatile files (SQLite -wal/-shm, pytest testmondata,
                editor swap/temp files) produce per-write churn with no
                standalone capture value and bloat the CAS — issue #1543
                saw 449 GB accumulate from substrate.duckdb.wal and
                .testmondata-wal alone. Matched as case-sensitive suffix
                on the file basename.
              '';
            };
            instances = mkOption { type = nullOr positive; default = null; description = "Instance override (null ⇒ inherit defaults)."; };
            batch = mkOption {
              type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; });
              default = null;
              description = "Batch override (null ⇒ inherit defaults).";
            };
            resources = mkOption {
              type = nullOr (resourceModule { defaultMemory = "8G"; defaultCpu = "50%"; });
              default = { };
              description = "Filesystem source runtime resource limits. Defaults to an 8G soft MemoryHigh watermark; hard caps remain opt-in.";
            };
            env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
            extraArgs = mkOption { type = strList; default = [ ]; description = "Extra CLI args."; };
          };
        };
        default = { };
        description = "Filesystem source runtime.";
      };

      terminal = mkOption {
        type = submodule {
          options = {
            enable = mkOption { type = bool; default = true; description = "Enable terminal source runtime."; };
            instances = mkOption { type = nullOr positive; default = null; description = "Instance override."; };
            batch = mkOption { type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; }); default = null; description = "Batch override."; };
            resources = mkOption { type = nullOr (resourceModule { defaultMemory = "8G"; defaultCpu = "50%"; }); default = null; description = "Resource override."; };
            historySources = mkOption {
              type = listOf terminalHistorySourceModule;
              default = [ ];
              description = ''
                Structured history sources passed to the terminal source runtime through
                <literal>--runtime-config</literal>. When empty, the runtime falls back to its
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
              description = "Terminal source runtime host-access configuration.";
            };
            env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
            extraArgs = mkOption { type = strList; default = [ ]; description = "Extra CLI args."; };
          };
        };
        default = { };
        description = "Terminal source runtime.";
      };

      browser = mkOption {
        type = submodule {
          options = {
            enable = mkOption { type = bool; default = true; description = "Enable browser history source runtime."; };
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
                scanned by the browser history source runtime.
              '';
            };
            sqliteSources = mkOption {
              type = listOf browserSqliteSourceModule;
              default = [ ];
              description = ''
                Typed browser SQLite sources passed to the browser history source runtime through
                <literal>--runtime-config</literal>.
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
              description = "Browser source runtime host-access configuration.";
            };
            env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
            extraArgs = mkOption { type = strList; default = [ ]; description = "Extra CLI args."; };
          };
        };
        default = { };
        description = "Browser history source runtime.";
      };

      desktop = mkOption {
        type = submodule {
          options = {
            enable = mkOption { type = bool; default = true; description = "Enable desktop source runtime."; };
            instances = mkOption { type = nullOr positive; default = null; description = "Instance override."; };
            batch = mkOption { type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; }); default = null; description = "Batch override."; };
            resources = mkOption { type = nullOr (resourceModule { defaultMemory = "8G"; defaultCpu = "50%"; }); default = null; description = "Resource override."; };
            session = mkOption {
              type = submodule {
                options = {
                  runtimeDir = mkOption {
                    type = nullOr str;
                    default = null;
                    description = ''
                      Runtime directory presented to the desktop source runtime. When set, the module
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
              description = "Desktop source runtime session wiring.";
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
              description = "Desktop source runtime host-access configuration.";
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
        description = "Desktop source runtime.";
      };

      system = mkOption {
        type = submodule {
          options = {
            enable = mkOption { type = bool; default = true; description = "Enable system source runtime."; };
            instances = mkOption { type = nullOr positive; default = 1; description = "Instance override (default 1)."; };
            batch = mkOption {
              type = nullOr (batchModule { defaultSize = 200; defaultTimeout = 10; });
              default = { size = 200; timeoutSec = 10; };
              description = "Batch override (defaults to a slower cadence).";
            };
            resources = mkOption { type = nullOr (resourceModule { defaultMemory = "8G"; defaultCpu = "50%"; }); default = null; description = "Resource override."; };
            env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
            extraArgs = mkOption { type = strList; default = [ ]; description = "Extra CLI args."; };
          };
        };
        default = { };
        description = "System source runtime.";
      };

      document = mkOption {
        type = submodule {
          options = {
            enable = mkOption {
              type = bool;
              default = true;
              description = ''
                Enable managed document snapshot ingestion. This source runtime runs as a
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
              description = "MIME types accepted by the document source.";
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
                defaultMemory = "8G";
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

          hourlySummarizer = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable hourly activity summarizer automaton."; };
                profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
              };
            };
            default = { };
            description = "Hourly summarizer automaton. Rolls bounded `activity.window.summary` inputs into UTC-hour `activity.summary.hourly` outputs.";
          };

          dailySummarizer = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable daily activity summarizer automaton."; };
                profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
              };
            };
            default = { };
            description = "Daily summarizer automaton. Rolls hourly `activity.summary.hourly` inputs into UTC-day `activity.summary.daily` outputs.";
          };

          documentParser = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable document parser automaton."; };
                profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
              };
            };
            default = { };
            description = "Document parser automaton. Consumes `document.ingested` and `command.canonical` events, emits `document.parsed` + `document.chunked` derived events.";
          };

          tagApplier = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable tag applier automaton."; };
                profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
              };
            };
            default = { };
            description = "Rule-based tag automaton. Applies source, file-type, and MIME tags to events — emits `knowledge.tag_applied`.";
          };

          instructionReconciler = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable instruction expectation reconciler automaton."; };
                profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
              };
            };
            default = { };
            description = "Instruction expectation reconciler. Compares desired-state instruction events with ordinary observations — emits `runtime.instruction/expectation.status`.";
          };

          entityExtractor = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable entity extractor automaton."; };
                profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
              };
            };
            default = { };
            description = "Entity extractor automaton (Stage 1). Scans events for URLs, file paths, commands, emails — emits `entity.extracted`.";
          };

          entityResolver = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable entity resolver automaton."; };
                profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
              };
            };
            default = { };
            description = "Entity resolver automaton. Consumes `entity.extracted` events, emits `entity.resolved` with UUIDv5 deterministic IDs.";
          };

          relationExtractor = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable relation extractor automaton."; };
                profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
              };
            };
            default = { };
            description = "Relation extractor automaton. Consumes `entity.resolved`, emits `entity.related` from co-occurrence within source events.";
          };

          entityEnricher = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable entity enricher automaton."; };
                profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                env = mkOption { type = envModule; default = { }; description = "Extra environment variables."; };
              };
            };
            default = { };
            description = "Entity enricher automaton. Consumes `entity.resolved`, emits `entity.enriched` with temporal stats and category refinement.";
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
                  type = resourceModule { defaultMemory = "8G"; defaultCpu = "50%"; };
                  default = { };
                  description = "Resource limits for this automata profile.";
                };
              };
            });
            default = {
              light = {
                batch = { size = 50; timeoutSec = 2; };
                # memoryMax is 1.5x memoryHigh — soft pressure throttles
                # the process; hard cap kills it before a runaway leak
                # can saturate the host. 2026-05-15 forensic observed
                # sinex-relation-extractor at 4.5G RSS leaking ~70MB/min
                # past the (memoryHigh-only) 4G threshold. Root cause
                # was the heartbeat collision in #1284; the absence of
                # a hard cap turned a per-runtime-module leak into a host risk.
                resources = { memoryHigh = "2G"; memoryMax = "3G"; };
              };
              standard = {
                batch = { size = 100; timeoutSec = 5; };
                resources = { memoryHigh = "4G"; memoryMax = "6G"; };
              };
              heavy = {
                batch = { size = 500; timeoutSec = 5; };
                resources = { memoryHigh = "8G"; memoryMax = "12G"; };
              };
            };
            description = "Named automata performance profiles.";
          };
        };
      };
      default = { };
      description = "Automata configuration.";
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


    bootstrap = mkOption {
      type = submodule {
        options = {
          restartPolicy = mkOption {
            type = enum [ "no" "on-failure" "always" "on-abnormal" "on-watchdog" "on-abort" ];
            default = "on-failure";
            description = ''
              Restart= applied to one-shot bootstrap units (NATS stream
              provisioning, schema-apply, blob repository init). Set to "no"
              on workstations so failed bootstraps are visible instead of
              looping.
            '';
          };
        };
      };
      default = { };
      description = "Bootstrap unit lifecycle policy.";
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

          apiAdminTokenFile = mkOption {
            type = nullOr str;
            default = null;
            description = ''
              Optional path to the API admin token file.
              When unset, the module first looks for the conventional secret sources
              <literal>sinex-api-admin-token</literal> (agenix) and
              <literal>/etc/sinex/api-admin-token</literal> (declarative environment.etc),
              and the API refuses to start only if none of those exist.
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
      runtimeSpool = "${spoolBase}/runtime modules";
      ingestSpool = cfg.core.event_engine.spoolDir;
      logDir = cfg.observability.logDir;
      blobDir = cfg.storage.blob.repositoryPath;
      sinexUser = cfg.users.runtime;
      sourcesEnabled = cfg.enable && cfg.runtime.enable && cfg.sources.enable;
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
        if cfg.sources.document.allowedRoots != [ ] then cfg.sources.document.allowedRoots
        else if targetHome == null then [ ]
        else [ "${targetHome}/Documents" ];
      dbUser = cfg.database.user;
      dbCfg = cfg.database;
      databaseUrl = renderDatabaseUrl dbCfg;
      secretPaths = config.sinex.secrets.paths or { };
      resolveSecretPath = resolveNamedSecretPath secretPaths;
      apiAdminTokenFile =
        resolveSecretPath cfg.secrets.apiAdminTokenFile [
          "sinex-api-admin-token"
        ];
      effectiveDatabasePasswordFile = resolveSecretPath cfg.database.passwordFile [
        "sinex-local-db"
        "sinex-remote-db"
      ];
      natsTlsCfg = cfg.runtime.nats.tls;
      natsAuthCfg = cfg.runtime.nats.auth;
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
      apiTlsCertFile = cfg.core.api.tlsCertFile;
      apiTlsKeyFile = cfg.core.api.tlsKeyFile;
      apiTlsTrustAnchorFile =
        if cfg.core.api.autoGenerateTls then runtimeDir + "/api-ca.pem" else null;
      apiTlsClientCAFile = cfg.core.api.tlsClientCAFile;
      apiProbeListenAddress =
        if hasPrefix "0.0.0.0:" cfg.core.api.listenAddress then
          "127.0.0.1:${removePrefix "0.0.0.0:" cfg.core.api.listenAddress}"
        else if hasPrefix "[::]:" cfg.core.api.listenAddress then
          "[::1]:${removePrefix "[::]:" cfg.core.api.listenAddress}"
        else
          cfg.core.api.listenAddress;
      apiProbeBaseUrl =
        if cfg.core.enable && cfg.core.api.enable then
          "https://${apiProbeListenAddress}"
        else
          null;
      deploymentManagedUnits = lib.unique (
        (lib.optionals (cfg.enable && cfg.core.enable) [ "sinexd.service" ])
        ++ lib.optionals cfg.enable (map (name: "${name}.service") (config.sinex._generatedUnits or [ ]))
      );
      resolveRuntimeInstances = nodeInstances:
        if nodeInstances == null then
          if cfg.enable then cfg.runtime.defaults.instances else 1
        else
          nodeInstances;
      mkDeploymentSurface = enabled: instances: {
        inherit enabled;
        instances = if enabled then resolveRuntimeInstances instances else null;
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
          base_url = apiProbeBaseUrl;
          require_client_tls = cfg.core.api.requireClientTLS;
        };
        nats = {
          servers = cfg.runtime.nats.servers;
        };
        filesystem = mkDeploymentSurface (sourcesEnabled && cfg.sources.filesystem.enable) cfg.sources.filesystem.instances;
        terminal =
          (mkDeploymentSurface (sourcesEnabled && cfg.sources.terminal.enable) cfg.sources.terminal.instances)
          // {
            kitty_enabled = cfg.shell.kitty.enable;
            history_sources = map
              (source: {
                path = source.path;
                shell = source.shell;
                source_id =
                  if source.sourceId != null then source.sourceId
                  else terminalSourceIdForShell source.shell;
                runner_pack = "terminal";
                runner_binary = "sinexd";
                service = "sinexd.service";
              })
              cfg.sources.terminal.historySources;
          };
        browser =
          (mkDeploymentSurface (sourcesEnabled && cfg.sources.browser.enable) cfg.sources.browser.instances)
          // {
            dump_sources = cfg.sources.browser.dumpSources;
            sqlite_sources = map
              (source: {
                path = source.path;
                browser = source.browser;
                format = source.format;
              })
              cfg.sources.browser.sqliteSources;
            polling_interval_secs = cfg.sources.browser.pollIntervalSec;
          };
        desktop =
          (mkDeploymentSurface (sourcesEnabled && cfg.sources.desktop.enable) cfg.sources.desktop.instances)
          // {
            clipboard_enabled = cfg.sources.desktop.clipboard.enable;
            activitywatch_db_path = cfg.sources.desktop.history.activitywatchDbPath;
            runtime_dir = cfg.sources.desktop.session.runtimeDir;
            wayland_display = cfg.sources.desktop.session.waylandDisplay;
            hyprland_instance_signature = cfg.sources.desktop.session.hyprlandInstanceSignature;
            hyprland_event_socket = cfg.sources.desktop.session.hyprlandEventSocket;
            hyprland_command_socket = cfg.sources.desktop.session.hyprlandCommandSocket;
          };
        system = mkDeploymentSurface (sourcesEnabled && cfg.sources.system.enable) cfg.sources.system.instances;
        document =
          (mkDeploymentSurface (sourcesEnabled && cfg.sources.document.enable) null)
          // {
            allowed_roots = effectiveDocumentRoots;
            scan_service_unit =
              if sourcesEnabled && cfg.sources.document.enable then
                "sinex-document-scan.service"
              else
                null;
            timer_unit =
              if sourcesEnabled && cfg.sources.document.enable && cfg.sources.document.schedule != null then
                "sinex-document-scan.timer"
              else
                null;
            schedule = cfg.sources.document.schedule;
            run_on_boot = cfg.sources.document.runOnBoot;
          };
        automata =
          (mkDeploymentSurface (cfg.runtime.enable && cfg.automata.enable) null)
          // listToAttrs (
            map
              (spec:
                nameValuePair spec.surfaceName (
                  cfg.runtime.enable
                  && cfg.automata.enable
                  && cfg.automata.${spec.optionName}.enable
                )
              )
              automataLib.specs
          );
        expectations = {
          schema_apply = cfg.database.enable && cfg.database.autoSetup;
          nats_streams = cfg.enable && (cfg.core.enable || cfg.runtime.enable);
          gateway_ready = cfg.enable && cfg.core.enable && cfg.core.api.enable;
        };
        secrets = {
          database_password_file = effectiveDatabasePasswordFile;
          api_admin_token_file = apiAdminTokenFile;
          gateway_tls_cert_file = apiTlsCertFile;
          gateway_tls_key_file = apiTlsKeyFile;
          gateway_tls_trust_anchor_file = apiTlsTrustAnchorFile;
          gateway_tls_client_ca_file = apiTlsClientCAFile;
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
          base_url = apiProbeBaseUrl;
          token_file = apiAdminTokenFile;
          token_role = "admin";
          ca_cert_file = apiTlsTrustAnchorFile;
          client_cert_file = null;
          client_key_file = null;
          require_client_tls = cfg.core.api.requireClientTLS;
          insecure = false;
        };
        nats = {
          servers = cfg.runtime.nats.servers;
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
      asciinemaDir = cfg.shell.asciinema.recordingsPath;
      asciiPath = toString asciinemaDir;

      directoryRules =
        [
          # stateRoot is owned by root so child entries with their own
          # ownership (service-account homes, postgres data dirs, NATS state)
          # can coexist without a single uid dominating the namespace.
          { path = stateRoot; mode = "0755"; user = "root"; group = "root"; }
          { path = runtimeDir; mode = "0755"; }
          { path = spoolBase; mode = "0755"; }
          { path = runtimeSpool; mode = "0755"; }
          { path = ingestSpool; mode = "0755"; }
          # event_engine writes its working directory under ${stateRoot}/event_engine/work
          # (see SINEX_EVENT_ENGINE_WORK_DIR). Pre-create so event_engine does not need
          # write access to stateRoot itself.
          { path = "${stateRoot}/event_engine"; mode = "0750"; }
          { path = "${stateRoot}/event_engine/work"; mode = "0750"; }
          { path = logDir; mode = "0755"; }
        ]
        ++ optionals (cfg.storage.blob.enable) [{ path = blobDir; mode = "0750"; }]
        ++ optionals (cfg.core.enable && cfg.core.api.autoGenerateTls) [{ path = "${stateRoot}/tls"; mode = "0750"; }]
        ++ optionals (cfg.shell.asciinema.autoRecord && targetUser != null && hasPrefix "/" asciiPath) [
          { path = asciiPath; mode = "0770"; user = targetUser; group = targetGroup; }
        ];
      tmpRule = rule:
        let
          owner = rule.user or sinexUser;
          group = rule.group or sinexUser;
        in
        "d ${rule.path} ${rule.mode} ${owner} ${group} -";

      # Auxiliary sinex-owned units that should be gated alongside the
      # long-running runtime services. Long-running services
      # (sinexd, hosted source bindings, automata) already wire their own wantedBy
      # from cfg.runtime.target.attachToMultiUser and publish their service
      # names via config.sinex._generatedUnits. The auxiliary list here
      # covers the one-shots, the standalone sinex-document-scan and its
      # timer, NATS, and the bootstrap helpers that the long-running services
      # depend on.
      coreAuxUnitNames =
        lib.optionals (cfg.enable && cfg.core.enable) [
          "sinexd"
        ];
      generatedRuntimeUnitNames =
        lib.optionals cfg.enable (config.sinex._generatedUnits or [ ]);
      bootstrapAuxUnitNames =
        lib.optionals (cfg.nats.enable || cfg.nats.autoSetup) [
          "nats"
        ]
        ++ lib.optionals (
          (cfg.nats.enable || cfg.nats.autoSetup)
          && cfg.nats.bootstrapStreams.enable
        ) [ "sinex-nats-bootstrap" ]
        ++ lib.optionals (cfg.core.enable && cfg.core.api.enable && cfg.core.api.autoGenerateTls) [
          "sinex-tls-init"
        ]
        ++ lib.optionals (cfg.storage.blob.enable && cfg.storage.blob.legacyAnnexData && cfg.storage.blob.autoInit) [
          "sinex-blob-init"
        ]
        ++ lib.optionals (cfg.enable && cfg.shell.kitty.enable && cfg.shell.kitty.autoConfigure && targetUser != null) [
          "sinex-kitty-setup"
        ]
        ++ lib.optionals (cfg.enable && cfg.lifecycle.preflight.enable) [
          "sinex-preflight"
        ]
        ++ lib.optionals (cfg.enable && sourcesEnabled && cfg.sources.document.enable) [
          "sinex-document-scan"
        ];
      # Auxiliary units = bootstrap + standalone oneshots + the long-running
      # core/automata/source host services. The runtime target wants the
      # whole graph so that pulling the target reliably brings the runtime
      # online (and stopping it tears the runtime down cleanly).
      runtimeAuxiliaryUnitNames = lib.unique (
        coreAuxUnitNames
        ++ generatedRuntimeUnitNames
        ++ bootstrapAuxUnitNames
      );
      runtimeAuxiliaryUnits = map (n: "${n}.service") runtimeAuxiliaryUnitNames;
      runtimeAuxiliaryTimerNames =
        lib.optionals (
          cfg.enable
          && sourcesEnabled
          && cfg.sources.document.enable
          && cfg.sources.document.schedule != null
        ) [ "sinex-document-scan" ];
      runtimeDatabaseUnits =
        lib.optionals cfg.runtime.target.includeDatabase [
          "postgresql.service"
        ]
        ++ lib.optionals (
          cfg.runtime.target.includeDatabase && cfg.database.enable && cfg.database.autoSetup
        ) [ "postgresql-setup.service" ];
    in
    mkMerge [
      (mkIf cfg.enable {
        assertions = [
          {
            assertion = cfg.package != null;
            message = "services.sinex.package must be set when services.sinex.enable = true.";
          }
          {
            assertion = (!cfg.core.enable || !cfg.core.api.enable) || apiAdminTokenFile != null;
            message = ''
              API requires an admin token file. Set services.sinex.secrets.apiAdminTokenFile,
              provide an agenix secret named sinex-api-admin-token, or define
              environment.etc."sinex/api-admin-token".
            '';
          }
          {
            assertion =
              (!cfg.core.enable || !cfg.core.api.enable)
              || (apiTlsCertFile != null && apiTlsKeyFile != null);
            message = "API TCP/TLS requires tlsCertFile and tlsKeyFile when API is enabled.";
          }
          {
            # Non-loopback bindings must enforce mTLS; loopback-only listeners are trusted.
            assertion =
              (!cfg.core.enable || !cfg.core.api.enable)
              || (hasPrefix "127." cfg.core.api.listenAddress)
              || (hasPrefix "[::1]" cfg.core.api.listenAddress)
              || cfg.core.api.requireClientTLS;
            message = "API binds to non-loopback address '${cfg.core.api.listenAddress}'; set services.sinex.core.api.requireClientTLS = true and configure tlsClientCAFile.";
          }
          {
            # mTLS requires a client CA bundle to verify the certificates presented by clients.
            assertion =
              (!cfg.core.enable || !cfg.core.api.enable)
              || (!cfg.core.api.requireClientTLS)
              || (apiTlsClientCAFile != null);
            message = "API mTLS (requireClientTLS = true) requires tlsClientCAFile. Set services.sinex.core.api.tlsClientCAFile.";
          }
          {
            assertion = (effectiveNatsClientCertFile == null) == (effectiveNatsClientKeyFile == null);
            message = "NATS mutual TLS requires both services.sinex.runtime.nats.tls.clientCertFile/clientKeyFile or matching agenix secrets named sinex-nats-client-cert and sinex-nats-client-key.";
          }
          {
            assertion =
              length
                (filter (x: x != null) [
                  effectiveNatsTokenFile
                  effectiveNatsCredsFile
                  effectiveNatsNkeySeedFile
                ]) <= 1;
            message = "Configure at most one NATS auth mode under services.sinex.runtime.nats.auth: tokenFile, credsFile, or nkeySeedFile.";
          }
          {
            assertion =
              (!(cfg.nats.enable || cfg.nats.autoSetup))
              || (!cfg.nats.tls.verifyClients && !cfg.nats.tls.verifyAndMap)
              || (effectiveNatsClientCertFile != null && effectiveNatsClientKeyFile != null);
            message = "Managed NATS client-certificate verification requires services.sinex.runtime.nats.tls.clientCertFile/clientKeyFile or matching agenix secrets named sinex-nats-client-cert and sinex-nats-client-key.";
          }
          {
            assertion =
              (!sourcesEnabled || !cfg.sources.document.enable)
              || effectiveDocumentRoots != [ ];
            message = ''
              Document ingestion is enabled but no allowed roots resolved. Set
              services.sinex.sources.document.allowedRoots explicitly or configure
              services.sinex.users.target so the module can derive $HOME/Documents.
            '';
          }
          {
            assertion =
              (!sourcesEnabled || !cfg.sources.document.enable)
              || cfg.sources.document.runOnBoot
              || cfg.sources.document.schedule != null;
            message = ''
              Document ingestion is enabled but neither runOnBoot nor schedule is set.
              Enable at least one so the managed document scan surface actually runs.
            '';
          }
        ];
        environment.systemPackages = mkAfter (
          [ pkgs.dbus pkgs.git ] ++ optionals cfg.storage.blob.legacyAnnexData [ pkgs.git-annex ]
          ++ optionals cfg.shell.asciinema.autoRecord [ pkgs.asciinema ]
        );

        systemd.targets.sinex-runtime = {
          description = "Sinex runtime services aggregate target";
          wantedBy = lib.optional cfg.runtime.target.attachToMultiUser "multi-user.target";
          # Pull every sinex-owned auxiliary unit (and optionally postgresql)
          # into the runtime target's dependency graph. When
          # attachToMultiUser=false, this is the only thing that brings them
          # online; when true, it makes the target a coherent stop boundary.
          wants = runtimeAuxiliaryUnits ++ runtimeDatabaseUnits;
          after = cfg.runtime.target.extraAfter;
          unitConfig = lib.optionalAttrs
            (cfg.runtime.target.manualStartOnly && !cfg.runtime.target.attachToMultiUser)
            { X-OnlyManualStart = true; };
        };
      })

      # When attachToMultiUser=false, strip the sinex-owned auxiliary one-shots,
      # timers, and (if includeDatabase) postgresql/postgresql-setup off
      # multi-user.target. The runtime target's wants graph above keeps them
      # reachable; this just prevents them from starting at boot independently.
      (mkIf (cfg.enable && !cfg.runtime.target.attachToMultiUser) {
        systemd.services = lib.genAttrs runtimeAuxiliaryUnitNames (_: {
          wantedBy = lib.mkForce [ ];
          unitConfig.PartOf = [ "sinex-runtime.target" ];
          restartIfChanged = cfg.runtime.restartOnSwitch;
        }) // lib.optionalAttrs cfg.runtime.target.includeDatabase {
          postgresql = {
            wantedBy = lib.mkForce [ ];
            unitConfig = {
              PartOf = lib.mkAfter [ "sinex-runtime.target" ];
            };
            restartIfChanged = cfg.runtime.restartOnSwitch;
          };
          postgresql-setup = {
            wantedBy = lib.mkForce [ ];
            unitConfig = {
              PartOf = [ "sinex-runtime.target" ];
            };
            restartIfChanged = cfg.runtime.restartOnSwitch;
          };
        };
        systemd.timers = lib.genAttrs runtimeAuxiliaryTimerNames (_: {
          wantedBy = lib.mkForce (lib.optionals cfg.runtime.target.attachToMultiUser [ "timers.target" ]);
          unitConfig.PartOf = [ "sinex-runtime.target" ];
        });
        # postgresql.target leaks into multi-user even with the runtime
        # service's wantedBy stripped; suppress it alongside.
        systemd.targets = lib.optionalAttrs cfg.runtime.target.includeDatabase {
          postgresql.wantedBy = lib.mkForce [ ];
        };
      })

      # Deferred-start timer: pulls sinex-runtime.target after a delay.
      # The timer is always defined when configured so its shape is
      # introspectable (tests, status probes); wantedBy is gated separately
      # so deployers can keep the timer defined but inert.
      (mkIf (cfg.enable && cfg.runtime.deferredStart.enable) {
        systemd.timers.sinex-runtime = {
          description = "Delay Sinex runtime startup until after boot";
          wantedBy = lib.optionals cfg.runtime.deferredStart.autoStart [ "timers.target" ];
          timerConfig = {
            OnActiveSec = cfg.runtime.deferredStart.delay;
            AccuracySec = cfg.runtime.deferredStart.accuracy;
            Unit = "sinex-runtime.target";
          };
        };
      })

      # Postgres-setup waits for declared secret materializer paths.
      (mkIf (cfg.database.enable && cfg.database.autoSetup && cfg.database.setupWaitForPaths != [ ]) {
        systemd.services.postgresql-setup.unitConfig.ConditionPathIsReadable =
          cfg.database.setupWaitForPaths;
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
          # Service accounts get their own home under ${stateRoot}/home so the
          # stateRoot itself can stay traversable (0755) for sibling tmpfiles
          # entries (postgres data dir, NATS jetstream, blob repo).
          home = "${stateRoot}/home/${dbUser}";
          homeMode = "0711";
          createHome = true;
        };
      })

      (mkIf ((cfg.enable || cfg.storage.blob.enable || cfg.lifecycle.maintenance.enable) && cfg.users.runtime != dbUser) {
        users.groups.${sinexUser} = { };
        users.users.${sinexUser} = {
          isSystemUser = true;
          group = sinexUser;
          description = "Sinex service account";
          home = "${stateRoot}/home/${sinexUser}";
          homeMode = "0711";
          createHome = true;
        };
      })

      (mkIf (cfg.enable || cfg.storage.blob.enable) {
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
      # NB: guard only on cfg.users.target — reading cfg.runtime.* while writing to
      # services.sinex.runtime.* creates an evaluation cycle.
      # mkDefault ensures explicit watchPaths override this fallback.
      (mkIf (cfg.users.target != null) {
        services.sinex.sources.filesystem.watchPaths = mkDefault [
          targetHome
        ];
      })

      (mkIf (targetHome != null) {
        services.sinex.sources.terminal.historySources = mkDefault [
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
        services.sinex.sources.browser.dumpSources = mkDefault [ ];
        services.sinex.sources.browser.sqliteSources = mkDefault [
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
        services.sinex.sources.filesystem.instances = mkDefault 1;
        services.sinex.sources.terminal.instances = mkDefault 1;
        services.sinex.sources.browser.instances = mkDefault 1;
        services.sinex.sources.desktop.instances = mkDefault 1;
        services.sinex.sources.system.instances = mkDefault 1;
      })

      (mkIf (targetUid != null) {
        services.sinex.sources.desktop.session.runtimeDir =
          mkDefault "/run/user/${toString targetUid}";
      })

      (mkIf (targetHome != null) {
        services.sinex.sources.desktop.history.activitywatchDbPath =
          mkDefault "${targetHome}/.local/share/activitywatch/aw-server-rust/sqlite.db";
      })

      (mkIf (cfg.nats.enable || cfg.nats.autoSetup) {
        services.sinex.runtime.nats.servers = mkDefault [
          "${if cfg.nats.tls.enable then "tls" else "nats"}://${cfg.nats.host}:${toString cfg.nats.port}"
        ];
      })
    ];
}
