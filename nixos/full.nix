# Sinex NixOS Module - First-class system integration
{
  config,
  lib,
  pkgs,
  ...
}:

with lib;

let
  cfg = config.services.sinex;
  configGen = import ./config-gen.nix { inherit lib pkgs; };

  # Use config generation from separate module
  collectorConfigFile = configGen.mkCollectorConfigFile cfg.unifiedCollector cfg;

  # Database migration script
  migrateDbScript = pkgs.writeShellScript "migrate-sinex-db" ''
    set -e

    # Wait for PostgreSQL to be available
    until ${pkgs.postgresql}/bin/pg_isready -h /run/postgresql; do
      echo "Waiting for PostgreSQL to be ready..."
      sleep 2
    done

    # Run migrations (they create extensions and schema)
    export DATABASE_URL="postgresql://postgres/${cfg.database.name}?host=/run/postgresql"
    ${pkgs.sqlx-cli}/bin/sqlx migrate run --source ${cfg.package}/share/sinex/migrations
  '';

in
{
  options.services.sinex = {
    enable = mkEnableOption "Sinex Exocortex event capture system";

    package = mkOption {
      type = types.package;
      default = pkgs.sinex or (import ./. { }).packages.${pkgs.system}.default;
      defaultText = literalExpression "pkgs.sinex";
      description = "The Sinex package to use";
    };

    database = {
      host = mkOption {
        type = types.str;
        default = "localhost";
        description = "PostgreSQL host";
      };

      port = mkOption {
        type = types.port;
        default = 5432;
        description = "PostgreSQL port";
      };

      name = mkOption {
        type = types.str;
        default = "sinex";
        description = "Database name";
      };

      user = mkOption {
        type = types.str;
        default = "sinex";
        description = "Database user";
      };

      passwordFile = mkOption {
        type = types.nullOr types.path;
        default = null;
        description = "Path to file containing database password";
      };

      url = mkOption {
        type = types.str;
        default = "postgresql:///${cfg.database.name}?host=/run/postgresql&user=${cfg.database.user}";
        defaultText = literalExpression ''"postgresql:///''${name}?host=/run/postgresql&user=''${user}"'';
        description = "PostgreSQL connection URL using local socket authentication";
      };

      autoSetup = mkOption {
        type = types.bool;
        default = true;
        description = "Automatically create database and run migrations";
      };
    };

    unifiedCollector = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the unified event collector";
      };

      metricsPort = mkOption {
        type = types.port;
        default = 2112;
        description = "Port for Prometheus metrics endpoint";
      };

      logLevel = mkOption {
        type = types.enum [
          "trace"
          "debug"
          "info"
          "warn"
          "error"
        ];
        default = "info";
        description = "Log level for the collector";
      };

      dryRun = mkOption {
        type = types.bool;
        default = false;
        description = "Run in dry-run mode (no database writes)";
      };

      outputFile = mkOption {
        type = types.nullOr types.path;
        default = null;
        description = "Write events to file instead of database";
      };

      sources = {
        atuin = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable Atuin shell history ingestion";
          };

          pollInterval = mkOption {
            type = types.int;
            default = 3;
            description = "Polling interval in seconds";
          };

          databasePath = mkOption {
            type = types.str;
            default = "~/.local/share/atuin/history.db";
            description = "Path to Atuin SQLite database";
          };
        };

        shellHistory = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable shell history file ingestion";
          };

          zshPath = mkOption {
            type = types.str;
            default = "~/.zsh_history";
            description = "Path to zsh history file";
          };

          bashPath = mkOption {
            type = types.str;
            default = "~/.bash_history";
            description = "Path to bash history file";
          };
        };

        asciinema = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable asciinema recording detection";
          };

          recordingsPath = mkOption {
            type = types.str;
            default = "~/.local/share/asciinema";
            description = "Path to asciinema recordings directory";
          };

          autoRecord = mkOption {
            type = types.bool;
            default = false;
            description = "Automatically start recording all terminal sessions";
          };

          autoAnnex = mkOption {
            type = types.bool;
            default = true;
            description = "Automatically add recordings to git-annex when they complete";
          };
        };

        kittyScrollback = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable Kitty terminal scrollback capture";
          };

          captureInterval = mkOption {
            type = types.int;
            default = 15;
            description = "Scrollback capture interval in seconds";
          };

          socketPath = mkOption {
            type = types.str;
            default = "/tmp/kitty";
            description = "Kitty remote control socket path";
          };

          maxScrollbackLines = mkOption {
            type = types.int;
            default = 10000;
            description = "Maximum scrollback lines to capture";
          };

          captureOnCommand = mkOption {
            type = types.bool;
            default = true;
            description = "Capture scrollback when commands are executed";
          };

          commandCaptureDelay = mkOption {
            type = types.int;
            default = 500;
            description = "Delay in milliseconds after command execution before capturing";
          };
        };

        filesystem = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable filesystem event monitoring";
          };

          watchPaths = mkOption {
            type = types.listOf types.str;
            default = [
              "~/Documents"
              "~/Projects"
            ];
            description = "Paths to monitor for filesystem events";
          };

          excludePatterns = mkOption {
            type = types.listOf types.str;
            default = [
              "*.tmp"
              "*.cache"
              ".git/*"
            ];
            description = "Patterns to exclude from monitoring";
          };
        };

        dbus = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable D-Bus event monitoring";
          };

          monitorSession = mkOption {
            type = types.bool;
            default = true;
            description = "Monitor session bus";
          };

          monitorSystem = mkOption {
            type = types.bool;
            default = true;
            description = "Monitor system bus";
          };

          logAllSignals = mkOption {
            type = types.bool;
            default = false;
            description = "Log all D-Bus signals (verbose)";
          };

          extractNotifications = mkOption {
            type = types.bool;
            default = true;
            description = "Extract notification events";
          };

          extractMedia = mkOption {
            type = types.bool;
            default = true;
            description = "Extract media playback events";
          };

          extractPower = mkOption {
            type = types.bool;
            default = true;
            description = "Extract power management events";
          };

          extractHardware = mkOption {
            type = types.bool;
            default = true;
            description = "Extract hardware device events";
          };

          extractSession = mkOption {
            type = types.bool;
            default = true;
            description = "Extract session/idle events";
          };

          extractPolicykit = mkOption {
            type = types.bool;
            default = true;
            description = "Extract PolicyKit authorization events";
          };

          extractBluetooth = mkOption {
            type = types.bool;
            default = true;
            description = "Extract Bluetooth device events";
          };

          extractNetwork = mkOption {
            type = types.bool;
            default = true;
            description = "Extract network connection events";
          };

          extractScreensaver = mkOption {
            type = types.bool;
            default = true;
            description = "Extract screen saver/lock events";
          };

          extractMounts = mkOption {
            type = types.bool;
            default = true;
            description = "Extract storage mount/unmount events";
          };
        };

        clipboard = {
          enable = mkOption {
            type = types.bool;
            default = true;
            description = "Enable clipboard monitoring";
          };

          monitorClipboard = mkOption {
            type = types.bool;
            default = true;
            description = "Monitor standard clipboard";
          };

          monitorPrimary = mkOption {
            type = types.bool;
            default = true;
            description = "Monitor primary selection (Linux)";
          };

          monitorSecondary = mkOption {
            type = types.bool;
            default = false;
            description = "Monitor secondary selection (rarely used)";
          };

          pollInterval = mkOption {
            type = types.int;
            default = 500;
            description = "Polling interval in milliseconds";
          };

          hashFileContent = mkOption {
            type = types.bool;
            default = false;
            description = "Include file content hashes";
          };

          maxPreviewLength = mkOption {
            type = types.int;
            default = 100;
            description = "Maximum preview length for text content";
          };

          enableHistory = mkOption {
            type = types.bool;
            default = true;
            description = "Store clipboard history";
          };

          maxHistoryEntries = mkOption {
            type = types.int;
            default = 1000;
            description = "Maximum history entries to keep";
          };
        };
      };

      dlq = {
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

        enableFileDlq = mkOption {
          type = types.bool;
          default = true;
          description = "Enable file-based DLQ for ultimate fallback";
        };

        filePath = mkOption {
          type = types.path;
          default = "/var/lib/sinex/dlq";
          description = "Path for file-based DLQ storage";
        };
      };
    };

    promoWorker = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable the promotion worker";
      };

      metricsPort = mkOption {
        type = types.port;
        default = 2113;
        description = "Port for Prometheus metrics endpoint";
      };

      pollInterval = mkOption {
        type = types.int;
        default = 5;
        description = "Queue polling interval in seconds";
      };

      batchSize = mkOption {
        type = types.int;
        default = 100;
        description = "Number of events to process per batch";
      };
    };

    blobStorage = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable git-annex blob storage integration";
      };

      repositoryPath = mkOption {
        type = types.path;
        default = "/var/lib/sinex/annex";
        description = "Path to git-annex repository";
      };

      autoInit = mkOption {
        type = types.bool;
        default = true;
        description = "Automatically initialize git-annex repository";
      };

      numCopies = mkOption {
        type = types.int;
        default = 2;
        description = "Minimum number of copies for git-annex";
      };
    };

    observability = {
      enablePrometheus = mkOption {
        type = types.bool;
        default = true;
        description = "Configure Prometheus to scrape Sinex metrics";
      };

      enableGrafana = mkOption {
        type = types.bool;
        default = true;
        description = "Configure Grafana with Sinex dashboards";
      };

      logToDatabase = mkOption {
        type = types.bool;
        default = false;
        description = "Store logs as events in database (alternative to Loki)";
      };

      metricsToDatabase = mkOption {
        type = types.bool;
        default = false;
        description = "Store metrics as events in database (in addition to Prometheus)";
      };
    };
  };

  config = mkIf cfg.enable {
    # Ensure PostgreSQL is configured
    assertions = [
      {
        assertion = config.services.postgresql.enable;
        message = "Sinex requires PostgreSQL to be enabled";
      }
      {
        assertion = config.services.postgresql.package.version >= "14";
        message = "Sinex requires PostgreSQL 14 or later";
      }
    ];

    # System packages
    environment.systemPackages = [ cfg.package ];
    
    # Create sinex user and group
    users.users.${cfg.database.user} = mkIf cfg.database.autoSetup {
      isSystemUser = true;
      group = cfg.database.user;
      description = "Sinex service user";
      home = "/var/lib/sinex";
      createHome = true;
    };
    
    users.groups.${cfg.database.user} = mkIf cfg.database.autoSetup {};

    # Database setup
    services.postgresql = mkIf cfg.database.autoSetup {
      enable = true;
      package = mkForce pkgs.postgresql_16;
      extraPlugins = with pkgs.postgresql16Packages; [
        timescaledb
        pgvector
        pgx_ulid
        pg_jsonschema # From our overlay
      ];
      settings = {
        shared_preload_libraries = "timescaledb";
      };
      ensureDatabases = [ cfg.database.name ];
      ensureUsers = [
        {
          name = cfg.database.user;
          ensureDBOwnership = true;
        }
      ];
    };

    # Database migrations
    systemd.services.sinex-migrate = mkIf cfg.database.autoSetup {
      description = "Run Sinex database migrations";
      wantedBy = [ "multi-user.target" ];
      after = [ "postgresql.service" ];
      requires = [ "postgresql.service" ];

      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        ExecStart = migrateDbScript;
        User = "postgres";
        Environment = "PATH=${pkgs.postgresql}/bin:${pkgs.sqlx-cli}/bin";
      };
    };

    # Unified Collector service
    systemd.services.sinex-unified-collector = mkIf cfg.unifiedCollector.enable {
      description = "Sinex Unified Event Collector";
      after = [
        "network.target"
        "postgresql.service"
      ] ++ optional cfg.database.autoSetup "sinex-migrate.service";
      wants = [ "postgresql.service" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        RUST_LOG = cfg.unifiedCollector.logLevel;
        DATABASE_URL = cfg.database.url;
        SINEX_CONFIG = collectorConfigFile;
      };

      serviceConfig = {
        Type = "simple";
        ExecStart = "${cfg.package}/bin/sinex-collector --config ${collectorConfigFile}";
        Restart = "always";
        RestartSec = "10s";

        # Security hardening - use static user to match database
        User = cfg.database.user;
        Group = cfg.database.user;
        StateDirectory = "sinex";
        RuntimeDirectory = "sinex";
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = "read-only";
        NoNewPrivileges = true;

        # Allow access to user files for ingestion
        SupplementaryGroups = [ "users" ];

        # Capability for monitoring
        AmbientCapabilities = "CAP_DAC_READ_SEARCH";
      };
    };

    # Promotion Worker service
    systemd.services.sinex-promo-worker = mkIf cfg.promoWorker.enable {
      description = "Sinex Event Promotion Worker";
      after = [
        "network.target"
        "postgresql.service"
      ] ++ optional cfg.database.autoSetup "sinex-migrate.service";
      wants = [ "postgresql.service" ];
      wantedBy = [ "multi-user.target" ];

      environment = {
        RUST_LOG = "info";
        DATABASE_URL = cfg.database.url;
      };

      serviceConfig = {
        Type = "simple";
        ExecStart = "${cfg.package}/bin/sinex-promo-worker";
        Restart = "always";
        RestartSec = "10s";

        # Security hardening - use static user to match database
        User = cfg.database.user;
        Group = cfg.database.user;
        PrivateTmp = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        NoNewPrivileges = true;
      };
    };

    # Git-annex initialization
    systemd.services.sinex-annex-init = mkIf (cfg.blobStorage.enable && cfg.blobStorage.autoInit) {
      description = "Initialize Sinex git-annex repository";
      wantedBy = [ "multi-user.target" ];
      before = [ "sinex-unified-collector.service" ];

      script = ''
        if [ ! -d "${cfg.blobStorage.repositoryPath}/.git" ]; then
          mkdir -p "$(dirname ${cfg.blobStorage.repositoryPath})"
          cd "$(dirname ${cfg.blobStorage.repositoryPath})"
          git init "$(basename ${cfg.blobStorage.repositoryPath})"
          cd "$(basename ${cfg.blobStorage.repositoryPath})"
          ${pkgs.git-annex}/bin/git-annex init "Sinex Blob Storage"
          git config annex.numcopies ${toString cfg.blobStorage.numCopies}
          git config annex.largefiles "anything"
          git config annex.backend "SHA256E"
        fi
      '';

      serviceConfig = {
        Type = "oneshot";
        RemainAfterExit = true;
        User = "root";
      };
    };

    # Prometheus configuration
    services.prometheus.scrapeConfigs = mkIf cfg.observability.enablePrometheus [
      {
        job_name = "sinex_unified_collector";
        static_configs = [
          {
            targets = [ "localhost:${toString cfg.unifiedCollector.metricsPort}" ];
          }
        ];
      }
      {
        job_name = "sinex_promo_worker";
        static_configs = [
          {
            targets = [ "localhost:${toString cfg.promoWorker.metricsPort}" ];
          }
        ];
      }
    ];

    # Terminal auto-recording for all users
    programs.bash.promptInit = mkIf cfg.unifiedCollector.sources.asciinema.autoRecord ''
      # Automatic asciinema recording for Sinex
      if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
        export ASCIINEMA_REC=1
        ASCIINEMA_DIR="${cfg.unifiedCollector.sources.asciinema.recordingsPath}"
        mkdir -p "$ASCIINEMA_DIR"
        exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
          "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
      fi
    '';

    programs.zsh.promptInit = mkIf cfg.unifiedCollector.sources.asciinema.autoRecord ''
      # Automatic asciinema recording for Sinex
      if [[ ! -n "$ASCIINEMA_REC" ]] && command -v asciinema >/dev/null 2>&1; then
        export ASCIINEMA_REC=1
        ASCIINEMA_DIR="${cfg.unifiedCollector.sources.asciinema.recordingsPath}"
        mkdir -p "$ASCIINEMA_DIR"
        exec asciinema rec --quiet --idle-time-limit 3600 --command "$SHELL" \
          "$ASCIINEMA_DIR/$(hostname)-$(date +%Y%m%d-%H%M%S)-$$.cast"
      fi
    '';

    # DLQ directory
    systemd.tmpfiles.rules = [
      "d ${cfg.unifiedCollector.dlq.filePath} 0755 sinex sinex"
    ];
  };
}
