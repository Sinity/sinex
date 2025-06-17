# Event sources configuration module
{ lib, config, ... }:

with lib;

let
  cfg = config.services.sinex;

  # Common event source options generator
  mkEventSource =
    {
      name,
      defaultPollInterval ? null,
      hasPath ? false,
      defaultPath ? null,
      extraOptions ? { },
    }:
    {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Enable ${name} event source";
      };

      # Every event source can have custom heartbeat settings
      heartbeat = {
        interval = mkOption {
          type = types.int;
          default = 60;
          description = "Heartbeat interval in seconds for ${name}";
        };

        timeout = mkOption {
          type = types.int;
          default = 30;
          description = "Heartbeat timeout in seconds for ${name}";
        };

        enableHealthMetrics = mkOption {
          type = types.bool;
          default = true;
          description = "Enable health metrics collection for ${name}";
        };
      };

      # Circuit breaker settings per event source
      circuitBreaker = {
        enable = mkOption {
          type = types.bool;
          default = true;
          description = "Enable circuit breaker for ${name}";
        };

        threshold = mkOption {
          type = types.int;
          default = 5;
          description = "Failure threshold before circuit breaker opens for ${name}";
        };

        timeout = mkOption {
          type = types.str;
          default = "60s";
          description = "Circuit breaker timeout before retry for ${name}";
        };
      };
    }
    // optionalAttrs (defaultPollInterval != null) {
      pollInterval = mkOption {
        type = types.int;
        default = defaultPollInterval;
        description = "Polling interval in seconds for ${name}";
      };
    }
    // optionalAttrs hasPath {
      ${
        if (name == "filesystem") then
          "watchPaths"
        else if (name == "atuin") then
          "databasePath"
        else
          "path"
      } =
        mkOption {
          type = if (name == "filesystem") then types.listOf types.str else types.str;
          default = defaultPath;
          description = "Path configuration for ${name} (supports ~ expansion)";
        };
    }
    // extraOptions;

in
{
  options.services.sinex.unifiedCollector.sources = {
    # Shell history sources
    atuin = mkEventSource {
      name = "Atuin shell history";
      defaultPollInterval = 3;
      hasPath = true;
      defaultPath = "~/.local/share/atuin/history.db";
    };

    shellHistory = mkEventSource {
      name = "shell history files";
      extraOptions = {
        zshPath = mkOption {
          type = types.str;
          default = "~/.zsh_history";
          description = "Path to zsh history file (supports ~ expansion)";
        };

        bashPath = mkOption {
          type = types.str;
          default = "~/.bash_history";
          description = "Path to bash history file (supports ~ expansion)";
        };
      };
    };

    # Terminal sources
    asciinema = mkEventSource {
      name = "asciinema recording detection";
      hasPath = true;
      defaultPath = "~/.local/share/asciinema";
      extraOptions = {
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
    };

    kittyScrollback = mkEventSource {
      name = "Kitty terminal scrollback capture";
      defaultPollInterval = 15;
      extraOptions = {
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
    };

    # Filesystem monitoring
    filesystem = mkEventSource {
      name = "filesystem event monitoring";
      hasPath = true;
      defaultPath = [ "~/" ]; # Monitor entire home directory
      extraOptions = {
        excludePatterns = mkOption {
          type = types.listOf types.str;
          default = [ ];
          description = "Additional patterns to exclude from monitoring (added to sensible defaults)";
        };

        overrideDefaultExcludes = mkOption {
          type = types.bool;
          default = false;
          description = ''
            If true, only use excludePatterns and ignore sensible defaults.
            Advanced option - most users should leave this false and use excludePatterns instead.
          '';
        };

        # Internal option for the complete exclude list (defaults + user additions)
        _allExcludePatterns = mkOption {
          type = types.listOf types.str;
          internal = true;
          readOnly = true;
          default =
            if cfg.unifiedCollector.sources.filesystem.overrideDefaultExcludes then
              cfg.unifiedCollector.sources.filesystem.excludePatterns
            else
              [
                # Sensible defaults that are always applied
                # Version control
                ".git/*"
                ".svn/*"
                ".hg/*"

                # Build artifacts & dependencies
                "node_modules/*"
                "target/*" # Rust
                "dist/*"
                "build/*"
                ".next/*" # Next.js
                ".nuxt/*" # Nuxt.js
                "__pycache__/*" # Python
                "*.pyc"
                ".venv/*" # Python venv
                "venv/*"
                ".tox/*" # Python tox

                # Package managers
                ".npm/*"
                ".yarn/*"
                ".pnpm-store/*"
                "*.egg-info/*"

                # Temporary files
                "*~" # Editor backups
                ".#*" # Emacs lock files
                "#*#" # Emacs auto-save

                # Cache directories
                "*.cache"
                ".cache/*"
                "cache/*"

                # Editor/IDE files
                ".vscode/*"
                ".idea/*"
                "*.swp" # Vim
                "*.swo" # Vim
                ".DS_Store" # macOS
                "Thumbs.db" # Windows
                "desktop.ini" # Windows

                # Browser data
                ".mozilla/*"
                ".chrome/*"
                ".chromium/*"
                ".config/google-chrome/*"
                ".config/chromium/*"

                # System directories (noisy)
                ".local/share/applications/*"
                ".local/share/Trash/*"
                ".local/share/recently-used.xbel"
                ".thumbnails/*"
                ".gvfs/*"
                ".dbus/*"

                # Common noisy home dirs
                ".steam/*" # Gaming
                ".wine/*" # Wine
                "snap/*" # Snap packages

                # Intermediate binary build files
                "*.o"
                "*.so"
                "*.dylib"
              ]
              ++ cfg.unifiedCollector.sources.filesystem.excludePatterns;
        };
      };
    };

    # D-Bus monitoring with grouped options
    dbus = mkEventSource {
      name = "D-Bus event monitoring";
      extraOptions = {
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

        # Group extraction options
        extractAll = mkOption {
          type = types.bool;
          default = true;
          description = "Extract all supported event types (overrides individual extract options)";
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
    };

    # Clipboard monitoring
    clipboard = mkEventSource {
      name = "clipboard monitoring";
      defaultPollInterval = 500; # milliseconds
      extraOptions = {
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
  };
}

