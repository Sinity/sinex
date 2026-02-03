{ config, lib, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;

  defaultSinexPackage =
    if pkgs ? sinex then
      pkgs.sinex
    else
      throw ''
        services.sinex.package is unset and no sinex package was found in pkgs.
        Provide one explicitly or overlay pkgs.sinex.
      '';

  defaultCliPackage =
    if pkgs ? sinexCli then pkgs.sinexCli else null;

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
    resourceModule = { defaultMemory, defaultCpu, defaultShutdownSec ? 90 }:
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
        };
      };
    envModule = attrsOf str;
    strList = listOf str;
    pathList = listOf path;
  in {
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
      defaultText = literalExpression "pkgs.sinexCli or null";
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

          satellites = mkOption {
            type = str;
            default = "sinex";
            description = "System account used to run Sinex services.";
          };
        };
      };
      default = {};
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
            description = ''
              Automatically provision PostgreSQL when enabled. Defaults to true when
              services.sinex.enable = true, but stays disabled otherwise unless explicitly set.
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
            description = "Database name used by Sinex.";
          };

          extraDatabases = mkOption {
            type = listOf str;
            default = [];
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
            description = "Path to a file containing the database password.";
          };

          package = mkOption {
            type = package;
            default = pkgs.postgresql_16;
            defaultText = literalExpression "pkgs.postgresql_16";
            description = "PostgreSQL package to deploy.";
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
            default = {};
            description = "Connection pool tuning for Sinex services.";
          };

          migration = mkOption {
            type = submodule {
              options = {
                enable = mkOption {
                  type = bool;
                  default = true;
                  description = "Run database migrations automatically.";
                };
                binary = mkOption {
                  type = str;
                  default = "sinex-schema";
                  description = "Migration binary name.";
                };
                package = mkOption {
                  type = nullOr package;
                  default = null;
                  description = "Package that provides the migration binary (defaults to services.sinex.package).";
                };
                timeout = mkOption {
                  type = positive;
                  default = 300;
                  description = "Migration timeout in seconds.";
                };
              };
            };
            default = {};
            description = "Database migration configuration.";
          };
        };
      };
      default = {};
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
                  default = {};
                  description = "DLQ maintenance configuration.";
                };
              };
            };
            default = {};
            description = "Dead Letter Queue settings.";
          };

          blob = mkOption {
            type = submodule {
              options = {
                enable = mkOption {
                  type = bool;
                  default = true;
                  description = "Enable git-annex backed blob storage.";
                };
                repositoryPath = mkOption {
                  type = path;
                  default = cfg.stateRoot + "/blob-repository";
                  defaultText = literalExpression "config.services.sinex.stateRoot + \"/blob-repository\"";
                  description = "Path to the git-annex repository.";
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
                        default = {};
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
                        default = {};
                        description = "git-annex fsck configuration.";
                      };
                    };
                  };
                  default = {};
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
                  default = {};
                  description = "Blob repository health monitoring.";
                };
              };
            };
            default = {};
            description = "Blob storage configuration.";
          };
        };
      };
      default = {};
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
                  type = batchModule { defaultSize = 1000; defaultTimeout = 5; };
                  default = {};
                  description = "Batch settings for ingestd.";
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
                  type = resourceModule { defaultMemory = "1G"; defaultCpu = "100%"; };
                  default = {};
                  description = "Resource limits for ingestd.";
                };
                extraArgs = mkOption {
                  type = strList;
                  default = [];
                  description = "Additional command-line arguments for ingestd.";
                };
              };
            };
            default = {};
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
                  default = {};
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
                        default = 32;
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
                    };
                  };
                  default = {};
                  description = "RPC resource guard configuration for the gateway.";
                };
                tlsCertFile = mkOption {
                  type = nullOr path;
                  default = null;
                  description = "Path to TLS certificate for gateway TCP bindings.";
                };
                tlsKeyFile = mkOption {
                  type = nullOr path;
                  default = null;
                  description = "Path to TLS private key for gateway TCP bindings.";
                };
                tlsClientCAFile = mkOption {
                  type = nullOr path;
                  default = null;
                  description = "Client CA bundle for gateway mTLS. Required for non-loopback bindings.";
                };
                extraArgs = mkOption {
                  type = strList;
                  default = [];
                  description = "Additional command-line arguments for the gateway.";
                };
              };
            };
            default = {};
            description = "Gateway configuration.";
          };
        };
      };
      default = {};
      description = "Core service configuration.";
    };

    satellites = mkOption {
      type = submodule {
        options = {
          enable = mkOption {
            type = bool;
            default = true;
            description = "Enable satellite services.";
          };

          nats = mkOption {
            type = submodule {
              options = {
                servers = mkOption {
                  type = strList;
                  default = [ "nats://127.0.0.1:4222" ];
                  description = "List of NATS server URLs.";
                };
                monitoringPort = mkOption {
                  type = port;
                  default = 8222;
                  description = "NATS monitoring port.";
                };
              };
            };
            default = {};
            description = "NATS client configuration.";
          };

          defaults = mkOption {
            type = submodule {
              options = {
                instances = mkOption {
                  type = positive;
                  default = 2;
                  description = "Default number of instances per satellite.";
                };
                logLevel = mkOption {
                  type = str;
                  default = cfg.logLevel;
                  defaultText = literalExpression "config.services.sinex.logLevel";
                  description = "Default log level for satellites.";
                };
                batch = mkOption {
                  type = batchModule { defaultSize = 100; defaultTimeout = 5; };
                  default = {};
                  description = "Default batching configuration.";
                };
                resources = mkOption {
                  type = resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; };
                  default = {};
                  description = "Default resource limits.";
                };
                env = mkOption {
                  type = envModule;
                  default = {};
                  description = "Environment variables applied to every satellite.";
                };
              };
            };
            default = {};
            description = "Satellite defaults.";
          };

          filesystem = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable filesystem satellite."; };
                watchPaths = mkOption { type = strList; default = [ "~/" ]; description = "Paths to watch."; };
                instances = mkOption { type = nullOr positive; default = null; description = "Instance override (null ⇒ inherit defaults)."; };
                batch = mkOption {
                  type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; });
                  default = null;
                  description = "Batch override (null ⇒ inherit defaults).";
                };
                resources = mkOption {
                  type = nullOr (resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; });
                  default = null;
                  description = "Resource override (null ⇒ inherit defaults).";
                };
                env = mkOption { type = envModule; default = {}; description = "Extra environment variables."; };
                extraArgs = mkOption { type = strList; default = []; description = "Extra CLI args."; };
              };
            };
            default = {};
            description = "Filesystem satellite.";
          };

          terminal = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable terminal satellite."; };
                instances = mkOption { type = nullOr positive; default = null; description = "Instance override."; };
                batch = mkOption { type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; }); default = null; description = "Batch override."; };
                resources = mkOption { type = nullOr (resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; }); default = null; description = "Resource override."; };
                env = mkOption { type = envModule; default = {}; description = "Extra environment variables."; };
                extraArgs = mkOption { type = strList; default = []; description = "Extra CLI args."; };
              };
            };
            default = {};
            description = "Terminal satellite.";
          };

          desktop = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable desktop satellite."; };
                instances = mkOption { type = nullOr positive; default = null; description = "Instance override."; };
                batch = mkOption { type = nullOr (batchModule { defaultSize = 100; defaultTimeout = 5; }); default = null; description = "Batch override."; };
                resources = mkOption { type = nullOr (resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; }); default = null; description = "Resource override."; };
                env = mkOption { type = envModule; default = {}; description = "Extra environment variables."; };
                extraArgs = mkOption { type = strList; default = []; description = "Extra CLI args."; };
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
                  default = {};
                  description = "Desktop clipboard integration.";
                };
              };
            };
            default = {};
            description = "Desktop satellite.";
          };

          system = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable system satellite."; };
                instances = mkOption { type = nullOr positive; default = 1; description = "Instance override (default 1)."; };
                batch = mkOption {
                  type = nullOr (batchModule { defaultSize = 200; defaultTimeout = 10; });
                  default = { size = 200; timeoutSec = 10; };
                  description = "Batch override (defaults to a slower cadence).";
                };
                resources = mkOption { type = nullOr (resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; }); default = null; description = "Resource override."; };
                env = mkOption { type = envModule; default = {}; description = "Extra environment variables."; };
                extraArgs = mkOption { type = strList; default = []; description = "Extra CLI args."; };
              };
            };
            default = {};
            description = "System satellite.";
          };

          automata = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = true; description = "Enable automata services."; };

                canonicalizer = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption { type = bool; default = true; description = "Enable canonical command synthesizer."; };
                      subjects = mkOption { type = strList; default = [ "events.terminal.*" ]; description = "Subject filters to consume."; };
                      profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                      env = mkOption { type = envModule; default = {}; description = "Extra environment variables."; };
                    };
                  };
                  default = {};
                  description = "Canonical command synthesizer automaton.";
                };

                healthAggregator = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption { type = bool; default = true; description = "Enable health aggregator automaton."; };
                      subjects = mkOption { type = strList; default = [ "events.system.*" ]; description = "Subject filters to consume."; };
                      profile = mkOption { type = str; default = "standard"; description = "Performance profile key."; };
                      env = mkOption { type = envModule; default = {}; description = "Extra environment variables."; };
                    };
                  };
                  default = {};
                  description = "Health aggregator automaton.";
                };

                profiles = mkOption {
                  type = attrsOf (submodule {
                    options = {
                      batch = mkOption {
                        type = batchModule { defaultSize = 100; defaultTimeout = 5; };
                        default = {};
                        description = "Batch parameters for this automata profile.";
                      };
                      resources = mkOption {
                        type = resourceModule { defaultMemory = "256M"; defaultCpu = "50%"; };
                        default = {};
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
            default = {};
            description = "Automata configuration.";
          };

          coordination = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = false; description = "Enable satellite coordination."; };
                heartbeatSec = mkOption { type = positive; default = 5; description = "Heartbeat interval in seconds."; };
                leadershipTimeoutSec = mkOption { type = positive; default = 30; description = "Leadership timeout in seconds."; };
                handoffTimeoutSec = mkOption { type = positive; default = 10; description = "Handoff timeout in seconds."; };
              };
            };
            default = {};
            description = "Coordination settings.";
          };

          generatedUnits = mkOption {
            type = listOf str;
            default = [];
            internal = true;
            description = "Systemd units generated for node services.";
          };
        };
      };
      default = {};
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
                      extraScrapeConfigs = mkOption { type = listOf attrs; default = []; description = "Additional scrape configs merged in."; };
                    };
                  };
                  default = {};
                  description = "Prometheus configuration.";
                };

                grafana = mkOption {
                  type = submodule {
                    options = {
                      enable = mkOption { type = bool; default = true; description = "Enable Grafana."; };
                      port = mkOption { type = port; default = 3000; description = "Grafana port."; };
                    };
                  };
                  default = {};
                  description = "Grafana configuration.";
                };

                exporters = mkOption {
                  type = submodule {
                    options = {
                      node = mkOption { type = bool; default = true; description = "Enable node exporter."; };
                      postgres = mkOption { type = bool; default = true; description = "Enable postgres exporter."; };
                    };
                  };
                  default = {};
                  description = "Exporter configuration.";
                };
              };
            };
            default = {};
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
                  default = {};
                  description = "Log retention policy.";
                };
              };
            };
            default = {};
            description = "Logging configuration.";
          };

          alerts = mkOption {
            type = submodule {
              options = {
                enable = mkOption { type = bool; default = false; description = "Enable Prometheus alert rules."; };
                rulesFiles = mkOption { type = pathList; default = []; description = "Alert rule files to include."; };
              };
            };
            default = {};
            description = "Alerting configuration.";
          };
        };
      };
      default = {};
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
                  default = [];
                  description = "Phases to skip during preflight verification.";
                };
                failureAction = mkOption {
                  type = enum [ "abort" "warn" "ignore" ];
                  default = "abort";
                  description = "Action when preflight fails.";
                };
              };
            };
            default = {};
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
                units = mkOption { type = strList; default = []; description = "Explicit list of units to manage (empty derives)."; };
              };
            };
            default = {};
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
                      custom = mkOption { type = strList; default = []; description = "Additional maintenance units to start."; };
                    };
                  };
                  default = {};
                  description = "Maintenance task selection.";
                };
              };
            };
            default = {};
            description = "Maintenance configuration.";
          };
        };
      };
      default = {};
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
            default = {};
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
            default = {};
            description = "Kitty terminal integration.";
          };
        };
      };
      default = {};
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

          gatewayAdminTokenFile = mkOption {
            type = nullOr str;
            default = null;
            description = "Path to the gateway admin token file (typically under /run/agenix/...). If unset while the gateway is enabled, the unit will refuse to start.";
          };
        };
      };
      default = {};
      description = "Secret management integration.";
    };
  };

  config =
    let
      stateRoot = cfg.stateRoot;
      runtimeDir = "${stateRoot}/run";
      spoolBase = "${stateRoot}/spool";
      satellitesSpool = "${spoolBase}/satellites";
      ingestSpool = cfg.core.ingestd.spoolDir;
      logDir = cfg.observability.logDir;
      dlqDir = cfg.storage.dlq.path;
      blobDir = cfg.storage.blob.repositoryPath;
      sinexUser = cfg.users.satellites;
      targetUser = cfg.users.target;
      dbUser = cfg.database.user;
      dbCfg = cfg.database;
      databaseUrl = "postgresql://${dbCfg.user}@${dbCfg.host}:${toString dbCfg.port}/${dbCfg.name}";
      secretPaths = config.sinex.secrets.paths or {};
      gatewayAdminTokenFile =
        if cfg.secrets.gatewayAdminTokenFile != null then cfg.secrets.gatewayAdminTokenFile
        else if secretPaths ? sinex-gateway-admin-token then secretPaths.sinex-gateway-admin-token
        else null;
      gatewayTlsCertFile = cfg.core.gateway.tlsCertFile;
      gatewayTlsKeyFile = cfg.core.gateway.tlsKeyFile;
      gatewayTlsClientCAFile = cfg.core.gateway.tlsClientCAFile;
      dlqCleanupScript = if cfg.cliPackage == null then null else pkgs.writeShellScript "sinex-dlq-cleanup" ''
        set -euo pipefail

        CLI_BIN="${cfg.cliPackage}/bin/sinex-cli"
        if [ ! -x "$CLI_BIN" ]; then
          echo "sinex-cli not found at $CLI_BIN" >&2
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
          { path = satellitesSpool; mode = "0755"; }
          { path = ingestSpool; mode = "0755"; }
          { path = logDir; mode = "0755"; }
        ]
        ++ optionals (cfg.storage.dlq.enable) [ { path = dlqDir; mode = "0750"; } ]
        ++ optionals (cfg.storage.blob.enable) [ { path = blobDir; mode = "0750"; } ]
        ++ optionals (cfg.shell.asciinema.autoRecord && targetUser != null && hasPrefix "/" asciiPath) [
          { path = asciiPath; mode = "0770"; user = targetUser; group = targetUser; }
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
            message = "Gateway requires an admin token file. Set services.sinex.secrets.gatewayAdminTokenFile or provide an agenix secret named sinex-gateway-admin-token.";
          }
          {
            assertion =
              (!cfg.core.enable || !cfg.core.gateway.enable)
              || (gatewayTlsCertFile != null && gatewayTlsKeyFile != null);
            message = "Gateway TCP/TLS requires tlsCertFile and tlsKeyFile when gateway is enabled.";
          }
          {
            # mTLS is required for non-loopback bindings to prevent unauthorized access
            assertion =
              (!cfg.core.enable || !cfg.core.gateway.enable)
              || (cfg.core.gateway.listenAddress == "127.0.0.1:9999")
              || (!cfg.core.gateway.requireClientTLS)
              || (gatewayTlsClientCAFile != null);
            message = "Gateway mTLS on non-loopback address requires tlsClientCAFile. Set services.sinex.core.gateway.tlsClientCAFile.";
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
        users.groups.${dbUser} = {};
        users.users.${dbUser} = {
          isSystemUser = true;
          group = dbUser;
          description = "Sinex database account";
          home = stateRoot;
          createHome = true;
        };
      })

      (mkIf ((cfg.enable || cfg.storage.blob.enable || cfg.lifecycle.maintenance.enable) && cfg.users.satellites != dbUser) {
        users.groups.${sinexUser} = {};
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
        services.sinex.nats.autoSetup = mkDefault true;
      })

      (mkIf (cfg.storage.dlq.enable && cfg.lifecycle.maintenance.enable && cfg.lifecycle.maintenance.tasks.dlq && cfg.cliPackage != null) {
        systemd.services.sinex-dlq-cleanup = {
          description = "Sinex DLQ cleanup";
          serviceConfig = {
            Type = "oneshot";
            User = sinexUser;
            Group = sinexUser;
            Environment = [
              "DATABASE_URL=${databaseUrl}"
              "SINEX_DLQ_PATH=${dlqDir}"
            ];
            ExecStart = dlqCleanupScript;
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
        services.sinex.satellites.nats.servers = mkDefault [
          "nats://${cfg.nats.host}:${toString cfg.nats.port}"
        ];
      })
    ];
}
