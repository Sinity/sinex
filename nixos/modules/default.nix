# Sinex NixOS Module - Modularized Structure
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
    if pkgs ? sinexCli then
      pkgs.sinexCli
    else
      null;

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
    # Core configuration modules
    ./database.nix
    ./blob-storage.nix
    ./monitoring.nix
    ./preflight-verification.nix
    ./kitty-shell-integration.nix
    
    # New consolidated service management (recommended)
    ./services
    
    # Satellite architecture services
    ./satellite-services.nix
  ];

  options.services.sinex = {
    enable = mkEnableOption "Sinex Exocortex event capture system";

    package = mkOption {
      type = types.package;
      default = defaultSinexPackage;
      defaultText = literalExpression "pkgs.sinex";
      description = "Sinex package to use";
    };

    cliPackage = mkOption {
      type = types.nullOr types.package;
      default = defaultCliPackage;
      defaultText = literalExpression "pkgs.sinexCli or null";
      description = ''
        Optional Sinex CLI package to install on PATH. When left as null the
        module skips installing a CLI and disables timers that require it.
      '';
    };

    # Simplified target user configuration
    targetUser = mkOption {
      type = types.str;
      description = "Username whose files to monitor for events";
      example = "myuser";
    };

    # Satellite architecture user
    satelliteUser = mkOption {
      type = types.str;
      default = "sinex";
      description = "System user for satellite services";
    };

    shell = mkOption {
      type = types.submodule {
        options = {
          asciinema = mkOption {
            type = types.submodule {
              options = {
                autoRecord = mkOption {
                  type = types.bool;
                  default = false;
                  description = "Automatically start asciinema recording for interactive shells.";
                };

                recordingsPath = mkOption {
                  type = types.str;
                  default = "~/.local/share/asciinema";
                  description = "Directory where automatic asciinema recordings are stored.";
                };
              };
            };
            default = {
              autoRecord = false;
              recordingsPath = "~/.local/share/asciinema";
            };
            description = "Per-shell asciinema capture settings.";
          };

          kitty = mkOption {
            type = types.submodule {
              options = {
                enable = mkOption {
                  type = types.bool;
                  default = true;
                  description = "Enable Kitty shell integration helpers.";
                };

                autoConfigure = mkOption {
                  type = types.bool;
                  default = true;
                  description = "Automatically manage Kitty configuration for Sinex integration.";
                };

                userConfigPath = mkOption {
                  type = types.str;
                  default = "~/.config/kitty/kitty.conf";
                  description = "Path to the user's Kitty configuration.";
                };

                configSnippet = mkOption {
                  type = types.lines;
                  default = defaultKittySnippet;
                  defaultText = literalExpression "defaultKittySnippet";
                  description = "Configuration block that Sinex injects when autoConfigure is enabled.";
                };
              };
            };
            default = {
              enable = true;
              autoConfigure = true;
              userConfigPath = "~/.config/kitty/kitty.conf";
              configSnippet = defaultKittySnippet;
            };
            description = "Kitty-specific integration settings.";
          };
        };
      };
      default = {};
      description = "Local shell helper configuration (asciinema, Kitty, etc.).";
    };

    # Simplified directories - monitoring.nix compatibility
    directories = {
      state = mkOption {
        type = types.path;
        default = "/var/lib/sinex";
        description = "Directory for persistent state data";
      };

      logs = mkOption {
        type = types.path;
        default = "/var/log/sinex";
        description = "Directory for log files";
      };

      runtime = mkOption {
        type = types.path;
        default = "/run/sinex";
        description = "Runtime directory for sockets and pid files";
      };

      dlq = mkOption {
        type = types.path;
        default = "/var/lib/sinex/failures";
        description = "Directory for DLQ payloads and failure artifacts";
      };

      blobRepository = mkOption {
        type = types.path;
        default = "/var/lib/sinex/blob-repository";
        description = "Default blob repository root used by git-annex";
      };

      spool = mkOption {
        type = types.submodule {
          options = {
            base = mkOption {
              type = types.path;
              default = "/var/lib/sinex/spool";
              description = "Base directory for Sinex spool data";
            };

            ingestd = mkOption {
              type = types.path;
              default = "/var/lib/sinex/spool/ingestd";
              description = "Ingestion daemon spool directory";
            };

            satellites = mkOption {
              type = types.path;
              default = "/var/lib/sinex/spool/satellites";
              description = "Satellite service spool directory";
            };
          };
        };
        default = {};
        description = "Spool directories used by ingestion and satellite services";
      };
    };

    # Log level configuration (applies to all services)
    logLevel = mkOption {
      type = types.enum [ "trace" "debug" "info" "warn" "error" ];
      default = "info";
      description = "Global log level for Sinex services";
    };

    # DLQ configuration (used by satellite services)
    dlq = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable Dead Letter Queue for failed events";
      };

      failureStoragePath = mkOption {
        type = types.path;
        default = "/var/lib/sinex/failures";
        description = ''
          Directory for DLQ files and critical failure logs when the database is down.
          Defaults to services.sinex.directories.state + "/failures" unless overridden.
        '';
      };

      maxRetries = mkOption {
        type = types.int;
        default = 3;
        description = "Maximum retry attempts for failed events";
      };

      retryDelaySecs = mkOption {
        type = types.int;
        default = 60;
        description = "Delay between retry attempts in seconds";
      };

      cleanup = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable automatic DLQ file cleanup";
        };

        maxAge = mkOption {
          type = types.str;
          default = "7d";
          description = "Maximum age of DLQ files before cleanup";
        };

        maxFiles = mkOption {
          type = types.int;
          default = 10000;
          description = "Maximum number of DLQ files before cleanup";
        };
      };
    };


    # Resource limits configuration for satellite services
    resources = {
      # Core services
      ingestd = {
        memoryMax = mkOption {
          type = types.str;
          default = "1G";
          description = "Maximum memory for ingestion daemon";
        };

        cpuQuota = mkOption {
          type = types.str;
          default = "100%";
          description = "CPU quota for ingestion daemon";
        };
      };

      gateway = {
        memoryMax = mkOption {
          type = types.str;
          default = "512M";
          description = "Maximum memory for API gateway";
        };

        cpuQuota = mkOption {
          type = types.str;
          default = "50%";
          description = "CPU quota for API gateway";
        };
      };

      # Default resources for satellites
      defaultSatellite = {
        memoryMax = mkOption {
          type = types.str;
          default = "256M";
          description = "Default maximum memory for satellite services";
        };

        cpuQuota = mkOption {
          type = types.str;
          default = "50%";
          description = "Default CPU quota for satellite services";
        };
      };
    };

    # Security configuration
    #
    # The Sinex security model implements defense-in-depth with multiple layers:
    #
    # ## Process Isolation
    # Each satellite runs with minimal privileges through systemd hardening:
    # - NoNewPrivileges: Prevents privilege escalation
    # - ProtectSystem: Read-only system directories
    # - SystemCallFilter: Restricts available system calls
    # - PrivateTmp: Isolated temporary directories
    #
    # ## Trust Boundaries
    # Clear separation between components:
    # - Satellites → ingestd: gRPC with Unix socket permissions
    # - ingestd → PostgreSQL: Database role separation
    # - Automata → Redis: Consumer group isolation
    # - User → Gateway: API authentication (future)
    #
    # ## Resource Limits
    # Prevents resource exhaustion attacks:
    # - Memory limits per service (MemoryMax)
    # - CPU quotas (CPUQuota)
    # - File descriptor limits
    # - Rate limiting on event ingestion
    #
    # ## Data Sanitization
    # Multiple layers of privacy protection:
    # - Input sanitization: Redact secrets before storage
    # - Environment variable filtering
    # - Command argument scrubbing
    # - Access control: Source-based permissions
    #
    security = {
      level = mkOption {
        type = types.enum [ "minimal" "balanced" "strict" ];
        default = "balanced";
        description = ''
          Security level for SystemD hardening:
          - minimal: Basic security, maximum functionality
          - balanced: Reasonable security with event monitoring capabilities
          - strict: Maximum security, may restrict some monitoring features
          
          Each level applies different systemd hardening options:
          
          Minimal:
          - Basic sandboxing (PrivateTmp, ProtectHome)
          - No network isolation
          - Full filesystem access for monitoring
          
          Balanced (recommended):
          - Process isolation (NoNewPrivileges, ProtectSystem)
          - Limited system call filtering
          - Restricted but functional device access
          - Memory and CPU limits enforced
          
          Strict:
          - Full sandboxing (PrivateDevices, ProtectKernelTunables)
          - Aggressive system call filtering
          - Minimal filesystem access
          - May break some event sources
        '';
      };

      allowFileSystemAccess = mkOption {
        type = types.bool;
        default = true;
        description = "Allow filesystem monitoring access (disabling may break file events)";
      };

      allowSocketAccess = mkOption {
        type = types.bool;
        default = true;
        description = "Allow access to Unix sockets for terminal and window manager monitoring";
      };

      allowDeviceAccess = mkOption {
        type = types.bool;
        default = true;
        description = "Allow device access for hardware event monitoring";
      };

      # Privacy features configuration
      sanitization = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable automatic sanitization of sensitive data";
        };

        secretPatterns = mkOption {
          type = types.listOf types.str;
          default = [
            "password[=:]\\s*\\S+"
            "token[=:]\\s*\\S+"
            "api[_-]?key[=:]\\s*\\S+"
            "secret[=:]\\s*\\S+"
          ];
          description = "Regex patterns for detecting secrets to redact";
        };

        envVarFilter = mkOption {
          type = types.listOf types.str;
          default = [
            "PASSWORD"
            "TOKEN"
            "SECRET"
            "API_KEY"
            "PRIVATE_KEY"
          ];
          description = "Environment variables to filter from command captures";
        };
      };

      # Access control
      accessControl = {
        enable = mkOption {
          type = types.bool;
          default = false;
          description = "Enable source-based access control";
        };

        rules = mkOption {
          type = types.listOf (types.submodule {
            options = {
              source = mkOption {
                type = types.str;
                description = "Event source pattern to match";
              };
              allow = mkOption {
                type = types.listOf types.str;
                default = [ "*" ];
                description = "Allowed users/roles for this source";
              };
            };
          });
          default = [];
          description = "Access control rules by event source";
        };
      };

      # Audit configuration
      audit = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable security audit logging";
        };

        logLevel = mkOption {
          type = types.enum [ "info" "warn" "error" ];
          default = "warn";
          description = "Security audit log level";
        };

        retentionDays = mkOption {
          type = types.int;
          default = 90;
          description = "Days to retain security audit logs";
        };
      };
    };

    # Update configuration
    update = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable coordinated update process";
      };

      gracePeriod = mkOption {
        type = types.int;
        default = 30;
        description = "Grace period in seconds for services to complete work before update";
      };

      healthCheckTimeout = mkOption {
        type = types.int;
        default = 60;
        description = "Maximum time to wait for health checks after update";
      };

      rollbackOnFailure = mkOption {
        type = types.bool;
        default = true;
        description = "Automatically rollback if health checks fail";
      };

      preserveData = mkOption {
        type = types.bool;
        default = true;
        description = "Preserve DLQ and failure data during updates";
      };

      units = mkOption {
        type = types.listOf types.str;
        default = [];
        description = ''
          Systemd services to cycle during coordinated updates. When left
          empty, the preflight module derives the list from the enabled
          satellite services.
        '';
      };
    };

  };
  config =
    let
      satelliteEnabled = cfg.satellite.enable or false;
      stateDir = cfg.directories.state;
      logsDir = cfg.directories.logs;
      runtimeDir = cfg.directories.runtime;
      dlqDir = cfg.directories.dlq;
      blobDir = cfg.directories.blobRepository;
      spoolBase = cfg.directories.spool.base;
      spoolIngestd = cfg.directories.spool.ingestd;
      spoolSatellites = cfg.directories.spool.satellites;
      dataOwner = if satelliteEnabled then cfg.satelliteUser else cfg.database.user;
      commonDirRules = [
        { path = stateDir; mode = "0755"; }
        { path = logsDir; mode = "0755"; }
        { path = runtimeDir; mode = "0755"; }
        { path = spoolBase; mode = "0750"; }
        { path = spoolIngestd; mode = "0750"; }
        { path = spoolSatellites; mode = "0750"; }
      ];
    in
    mkMerge [
      {
        services.sinex.directories.runtime = mkDefault (cfg.directories.state + "/run");
        services.sinex.directories.logs = mkDefault (cfg.directories.state + "/logs");
        services.sinex.directories.dlq = mkDefault (cfg.directories.state + "/failures");
        services.sinex.directories.blobRepository = mkDefault (cfg.directories.state + "/blob-repository");
        services.sinex.directories.spool.base = mkDefault (cfg.directories.state + "/spool");
        services.sinex.directories.spool.ingestd = mkDefault (cfg.directories.spool.base + "/ingestd");
        services.sinex.directories.spool.satellites = mkDefault (cfg.directories.spool.base + "/satellites");
        services.sinex.dlq.failureStoragePath = mkDefault cfg.directories.dlq;
        services.sinex.blobStorage.repositoryPath = mkDefault cfg.directories.blobRepository;
      }
      (mkIf (cfg.cliPackage != null) {
        environment.systemPackages = lib.mkAfter [ cfg.cliPackage ];
      })
      (mkIf (cfg.enable || cfg.database.autoSetup) {
        users.groups.${cfg.database.user} = {};
        users.users.${cfg.database.user} = {
          isSystemUser = true;
          group = cfg.database.user;
          description = "Sinex service account";
          home = stateDir;
          createHome = true;
        };
      })
      (mkIf (cfg.enable && cfg.satellite.enable && cfg.satelliteUser != cfg.database.user) {
        users.groups.${cfg.satelliteUser} = {};
        users.users.${cfg.satelliteUser} = {
          isSystemUser = true;
          group = cfg.satelliteUser;
          description = "Sinex satellite services account";
          home = stateDir;
          createHome = true;
        };
      })
      (mkIf cfg.enable {
        services.sinex.satellite.enable =
          mkDefault (cfg.serviceManagement.serviceGroups.core or true);
        services.sinex.monitoring.enable =
          mkDefault (cfg.serviceManagement.serviceGroups.monitoring or true);
        services.sinex.monitoring.dashboards.enable = mkDefault cfg.monitoring.observabilityStack.enable;
        services.sinex.monitoring.dashboards.grafana.enable = mkDefault cfg.monitoring.observabilityStack.enable;
        services.sinex.monitoring.observabilityStack.enable = mkDefault true;
        services.sinex.preflightVerification.enable = mkDefault true;
        services.sinex.update.enable = mkDefault true;

        environment.systemPackages = lib.mkAfter (
          lib.optionals cfg.shell.asciinema.autoRecord [ pkgs.asciinema ]
          ++ [ pkgs.dbus ]
        );

        systemd.tmpfiles.rules = lib.mkAfter (
          [ "d /etc/sinex 0755 root root -" ]
          ++ (map (rule: "d ${rule.path} ${rule.mode} ${dataOwner} ${dataOwner} -") (
            commonDirRules
            ++ lib.optionals cfg.dlq.enable [ { path = dlqDir; mode = "0750"; } ]
            ++ lib.optionals (cfg.blobStorage.enable or false) [ { path = blobDir; mode = "0750"; } ]
          ))
        );

        programs.bash.promptInit = mkIf cfg.shell.asciinema.autoRecord ''
          # Automatic asciinema recording for Sinex
          if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
            export ASCIINEMA_REC=1
            ASCIINEMA_DIR="${cfg.shell.asciinema.recordingsPath}"
            if [[ "$ASCIINEMA_DIR" == "~/"* ]]; then
              ASCIINEMA_DIR="$HOME/''${ASCIINEMA_DIR#~/}"
            fi
            mkdir -p "$ASCIINEMA_DIR"
            exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
              "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
          fi
        '';

        programs.zsh.promptInit = mkIf cfg.shell.asciinema.autoRecord ''
          # Automatic asciinema recording for Sinex
          if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
            export ASCIINEMA_REC=1
            ASCIINEMA_DIR="${cfg.shell.asciinema.recordingsPath}"
            if [[ "$ASCIINEMA_DIR" == "~/"* ]]; then
              ASCIINEMA_DIR="$HOME/''${ASCIINEMA_DIR#~/}"
            fi
            mkdir -p "$ASCIINEMA_DIR"
            exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
              "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
          fi
        '';

        assertions = [
          {
            assertion = cfg.enable -> cfg.targetUser != "";
            message = "services.sinex.targetUser must be set when Sinex is enabled";
          }
          {
            assertion =
              let userDefs = config.users.users or {};
              in cfg.enable -> lib.hasAttr cfg.targetUser userDefs;
            message = "services.sinex.targetUser must reference an existing users.users entry";
          }
          {
            assertion = cfg.monitoring.observabilityStack.enable -> cfg.database.autoSetup || config.services.postgresql.enable;
            message = "PostgreSQL must be enabled for Sinex observability stack";
          }
          {
            assertion = cfg.monitoring.dashboards.grafana.enable -> cfg.monitoring.observabilityStack.enable;
            message = "Grafana dashboards require the observability stack to be enabled";
          }
        ];
      })
    ];
}
