# Sinex Configuration Generation Module
{ lib, pkgs, ... }:

with lib;

rec {
  # Path resolution utilities (matches full.nix)
  pathUtils = {
    resolvePath = path:
      if lib.hasPrefix "~/" path then
        "\${HOME}/" + lib.removePrefix "~/" path
      else
        path;
    
    validateAbsolutePath = path:
      lib.hasPrefix "/" path || lib.hasPrefix "~/" path;
  };
  # Configuration validation utilities
  validation = {
    # Validate TOML syntax by attempting to parse
    validateToml = content: 
      let
        testResult = pkgs.runCommand "toml-validation" {
          buildInputs = [ pkgs.remarshal ];
          passAsFile = [ "tomlContent" ];
          tomlContent = content;
        } ''
          set -euo pipefail
          if ${pkgs.remarshal}/bin/toml2json < "$tomlContentPath" > /dev/null 2>&1; then
            echo "valid" > "$out"
          else
            echo "invalid" > "$out"
            exit 1
          fi
        '';
      in
        (builtins.readFile testResult) == "valid";
    
    # Validate event type name format
    validateEventType = eventType:
      let
        validPattern = "^[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*(\\.[a-z][a-z0-9_]*)*$";
      in
        builtins.match validPattern eventType != null;
    
    # Validate enabled events list
    validateEnabledEvents = enabledEvents:
      let
        knownEventTypes = [
          # Terminal/command events (matching Rust EVENT_NAME constants)
          "command.executed"         # KittyCommandExecuted
          "command.completed"        # KittyCommandCompleted  
          "command.failed"           # KittyCommandFailed
          "command.imported"         # AtuinCommandImported, ShellHistoryCommandImported
          "session.started"          # ShellSessionStarted
          "session.ended"            # ShellSessionEnded
          
          # Terminal recording (matching Rust EVENT_NAME constants)
          "recording.started"        # AsciinemaSessionStarted
          "recording.ended"          # AsciinemaSessionEnded
          "output.captured"          # ScrollbackCaptured
          
          # Filesystem events (matching Rust EVENT_NAME constants)
          "file.created"             # FileCreated
          "file.modified"            # FileModified
          "file.deleted"             # FileDeleted
          "file.moved"               # FileMoved
          "dir.created"              # DirCreated
          "dir.deleted"              # DirDeleted
          
          # Window manager events (matching Rust EVENT_NAME constants)
          "window.opened"            # WindowOpened
          "window.closed"            # WindowClosed
          "window.focused"           # WindowFocused
          "window.moved"             # WindowMoved
          "window.resized"           # WindowResized
          "workspace.switched"       # WorkspaceSwitched
          "workspace.created"        # WorkspaceCreated
          "workspace.destroyed"      # WorkspaceDestroyed
          "display.connected"        # DisplayConnected
          "display.disconnected"     # DisplayDisconnected
          "monitor.focused"          # MonitorFocused
          "state.captured"           # StateCapture
          
          # D-Bus events (matching Rust EVENT_NAME constants)
          "signal.received"          # DbusSignalReceived
          "method.called"            # DbusMethodCalled
          "notification.sent"        # DbusNotificationSent
          "device.connected"         # DbusDeviceConnected
          "device.disconnected"      # DbusDeviceDisconnected
          "media.state_changed"      # DbusMediaStateChanged
          "power.state_changed"      # DbusPowerStateChanged
          "network.state_changed"    # DbusNetworkStateChanged
          "bluetooth.device_changed" # DbusBluetoothDeviceChanged
          "mount.changed"            # DbusMountChanged
          
          # Clipboard events (matching Rust EVENT_NAME constants)
          "copied"                   # ClipboardCopied
          "selected"                 # ClipboardSelected
          
          # System journal (matching Rust EVENT_NAME constants)
          "entry.written"            # JournaldEntryWritten
        ];
        invalidEvents = lib.filter (e: !(lib.elem e knownEventTypes)) enabledEvents;
        malformedEvents = lib.filter (e: !(validation.validateEventType e)) enabledEvents;
      in {
        valid = (lib.length invalidEvents) == 0 && (lib.length malformedEvents) == 0;
        unknownEvents = invalidEvents;
        malformedEvents = malformedEvents;
        knownEvents = knownEventTypes;
      };
    
    # Validate configuration dependencies
    validateDependencies = cfg: fullCfg:
      let
        errors = lib.flatten [
          # Git annex dependency checks
          (lib.optional
            ((cfg.sources.asciinema.enable or false) && (cfg.sources.asciinema.autoAnnex or false) && !(fullCfg.blobStorage.enable or false))
            "asciinema.autoAnnex requires blobStorage.enable = true")
          
          # Path existence checks - use path resolution utilities from fullCfg
          (lib.optional
            ((cfg.sources.atuin.enable or false) && !(pathUtils.validateAbsolutePath (cfg.sources.atuin.databasePath or "")))
            "atuin.databasePath must be an absolute path after resolution (got '${cfg.sources.atuin.databasePath or ""}')")
          
          (lib.optional
            ((cfg.sources.kittyScrollback.enable or false) && !(lib.hasPrefix "/" (cfg.sources.kittyScrollback.socketPath or "")))
            "kittyScrollback.socketPath must be an absolute path")
          
          # Interval validation
          (lib.optional
            ((cfg.sources.atuin.enable or false) && (cfg.sources.atuin.pollInterval or 1) <= 0)
            "atuin.pollInterval must be greater than 0")
          
          (lib.optional
            ((cfg.sources.kittyScrollback.enable or false) && (cfg.sources.kittyScrollback.captureInterval or 1) <= 0)
            "kittyScrollback.captureInterval must be greater than 0")
          
          (lib.optional
            ((cfg.sources.clipboard.enable or false) && (cfg.sources.clipboard.pollInterval or 1) <= 0)
            "clipboard.pollInterval must be greater than 0")
          
          # Range validation
          (lib.optional
            ((cfg.sources.kittyScrollback.enable or false) && 
             (let maxLines = cfg.sources.kittyScrollback.maxScrollbackLines or 1000; in
             (maxLines < 100 || maxLines > 1000000)))
            "kittyScrollback.maxScrollbackLines must be between 100 and 1000000")
          
          (lib.optional
            ((cfg.sources.clipboard.enable or false) &&
             (let maxEntries = cfg.sources.clipboard.maxHistoryEntries or 1000; in
             (maxEntries < 1 || maxEntries > 100000)))
            "clipboard.maxHistoryEntries must be between 1 and 100000")
        ];
      in {
        valid = (lib.length errors) == 0;
        errors = errors;
      };
    
    # Validate configuration completeness
    validateCompleteness = cfg: fullCfg:
      let
        warnings = lib.flatten [
          # Performance warnings
          (lib.optional
            ((cfg.sources.filesystem.enable or false) && 
             (lib.length (cfg.sources.filesystem.watchPaths or [])) > 10)
            "filesystem: watching more than 10 paths may impact performance")
          
          (lib.optional
            ((cfg.sources.dbus.enable or false) && (cfg.sources.dbus.logAllSignals or false))
            "dbus.logAllSignals can generate very high event volume")
          
          # Security warnings
          (lib.optional
            ((cfg.sources.clipboard.enable or false) && !(cfg.sources.clipboard.hashFileContent or true))
            "clipboard: file content hashing disabled - sensitive data may be stored")
          
          # Configuration completeness
          (lib.optional
            (!(fullCfg.database.autoSetup or true) && !(fullCfg.database.migration.enabled or true))
            "database auto-setup and migrations both disabled - manual setup required")
        ];
        
        recommendations = [];
      in {
        warnings = warnings;
        recommendations = recommendations;
      };
  };
  # Helper to generate collector configuration with correct event names
  mkCollectorConfig = cfg: fullCfg: let
    enabledEvents = lib.flatten [
      (lib.optional (cfg.sources.atuin.enable or false) "command.imported")  # Matches Rust EventType
      (lib.optional (cfg.sources.shellHistory.enable or false) "command.imported")  # Same for shell history
      (lib.optional (cfg.sources.asciinema.enable or false) [
        "recording.started"    # Matches AsciinemaSessionStarted::EVENT_NAME  
        "recording.ended"      # Matches AsciinemaSessionEnded::EVENT_NAME
      ])
      (lib.optional (cfg.sources.kittyScrollback.enable or false) [
        "command.executed"     # Matches KittyCommandExecuted::EVENT_NAME
        "command.completed"    # Matches KittyCommandCompleted::EVENT_NAME
        "output.captured"      # Matches KittyScrollbackCaptured::EVENT_NAME
      ])
      (lib.optional (cfg.sources.filesystem.enable or false) [
        "file.created"         # Matches FileCreated::EVENT_NAME
        "file.modified"        # Matches FileModified::EVENT_NAME
        "file.deleted"         # Matches FileDeleted::EVENT_NAME
        "file.moved"           # Matches FileMoved::EVENT_NAME
        "dir.created"          # Matches DirCreated::EVENT_NAME
        "dir.deleted"          # Matches DirDeleted::EVENT_NAME
      ])
      (lib.optional (cfg.sources.dbus.enable or false) [
        "signal.received"      # Matches DbusSignalReceived::EVENT_NAME
        "method.called"        # Matches DbusMethodCalled::EVENT_NAME
        "notification.sent"    # Matches DbusNotificationSent::EVENT_NAME
        "device.connected"     # Matches DbusDeviceConnected::EVENT_NAME
        "device.disconnected"  # Matches DbusDeviceDisconnected::EVENT_NAME
        "media.state_changed"  # Matches DbusMediaStateChanged::EVENT_NAME
        "power.state_changed"  # Matches DbusPowerStateChanged::EVENT_NAME
        "network.state_changed" # Matches DbusNetworkStateChanged::EVENT_NAME
        "bluetooth.device_changed" # Matches DbusBluetoothDeviceChanged::EVENT_NAME
        "mount.changed"        # Matches DbusMountChanged::EVENT_NAME
        "entry.written"        # Matches JournaldEntryWritten::EVENT_NAME
      ])
      (lib.optional (cfg.sources.clipboard.enable or false) [
        "copied"               # Matches ClipboardCopied::EVENT_NAME
        "selected"             # Matches ClipboardSelected::EVENT_NAME
      ])
    ];

    # Build event configuration sections with resolved paths and correct field names
    eventConfig = lib.optionalAttrs (cfg.sources.atuin.enable or false) {
      "event.shell_command_executed_atuin" = {
        db_path = pathUtils.resolvePath cfg.sources.atuin.databasePath;
        polling_interval_secs = cfg.sources.atuin.pollInterval;
        use_file_watch = true;
        batch_size = 100;
      };
    } // lib.optionalAttrs (cfg.sources.shellHistory.enable or false) {
      "event.shell_history_command" = {
        history_files = [
          (pathUtils.resolvePath cfg.sources.shellHistory.zshPath)
          (pathUtils.resolvePath cfg.sources.shellHistory.bashPath)
        ];
        polling_interval_secs = 10;
        use_file_watch = true;
      };
    } // lib.optionalAttrs (cfg.sources.asciinema.enable or false) {
      "event.recording_started" = {  # Matches Rust EVENT_NAME
        recordings_dir = pathUtils.resolvePath cfg.sources.asciinema.path;
        auto_start_recording = cfg.sources.asciinema.autoRecord;
        file_pattern = "*.cast";
        polling_interval_secs = 5;
        record_command = "asciinema rec --quiet --overwrite";
        git_annex_repo = fullCfg.blobStorage.repositoryPath;
        auto_annex = cfg.sources.asciinema.autoAnnex;
      };
    } // lib.optionalAttrs (cfg.sources.kittyScrollback.enable or false) {
      "event.command_executed" = {  # Matches Rust EVENT_NAME for Kitty
        poll_interval_seconds = cfg.sources.kittyScrollback.captureInterval;  # Rust field name
        socket_path = cfg.sources.kittyScrollback.socketPath;  # Rust field name
        enabled = true;  # Rust field name
      };
    } // lib.optionalAttrs (cfg.sources.filesystem.enable or false) {
      "event.file_created" = {  # Use specific event name, not generic "files"
        watch_patterns = lib.map (path: pathUtils.resolvePath path) cfg.sources.filesystem.watchPaths;
        ignore_patterns = cfg.sources.filesystem._allExcludePatterns or [];
        debounce_ms = 500;  # Add Rust-only field with default
        max_depth = null;   # Add Rust-only field with default
      };
    } // lib.optionalAttrs (cfg.sources.dbus.enable or false) {
      "event.signal_received" = {  # Matches Rust EVENT_NAME  
        monitor_session = cfg.sources.dbus.monitorSession;
        monitor_system = cfg.sources.dbus.monitorSystem;
        include_interfaces = [];  # Add Rust-only field
        exclude_interfaces = [];  # Add Rust-only field
        extract_notifications = cfg.sources.dbus.extractNotifications;
        extract_media = cfg.sources.dbus.extractMedia;
        extract_power = cfg.sources.dbus.extractPower;
        extract_hardware = cfg.sources.dbus.extractHardware;
        extract_session = cfg.sources.dbus.extractSession;
        extract_policykit = cfg.sources.dbus.extractPolicykit;
        extract_bluetooth = cfg.sources.dbus.extractBluetooth;
        extract_network = cfg.sources.dbus.extractNetwork;
        extract_screensaver = cfg.sources.dbus.extractScreensaver;
        extract_mounts = cfg.sources.dbus.extractMounts;
      };
    } // lib.optionalAttrs (cfg.sources.clipboard.enable or false) {
      "event.copied" = {  # Matches Rust EVENT_NAME instead of generic "clipboard"
        monitor_clipboard = cfg.sources.clipboard.monitorClipboard;
        monitor_primary = cfg.sources.clipboard.monitorPrimary;
        monitor_secondary = cfg.sources.clipboard.monitorSecondary;
        poll_interval_ms = cfg.sources.clipboard.pollInterval;
        hash_file_content = cfg.sources.clipboard.hashFileContent;
        max_preview_length = cfg.sources.clipboard.maxPreviewLength;
        enable_history = cfg.sources.clipboard.enableHistory;
        max_history_entries = cfg.sources.clipboard.maxHistoryEntries;
        max_content_size = cfg.sources.clipboard.maxContentSize;
        annex_repo_path = fullCfg.blobStorage.repositoryPath;  # Add Rust-only field
      };
    };

    # Build complete TOML configuration
    tomlConfig = {
      enabled_events = enabledEvents;
      
      output = {
        database = !cfg.dryRun;
        logging = cfg.logLevel == "debug";
      };
      
      logging = {
        level = cfg.logLevel;
      };
    } // lib.optionalAttrs (fullCfg.blobStorage.enable or false) {
      annex_repo_path = fullCfg.blobStorage.repositoryPath;
    } // eventConfig;
  in
    tomlConfig;

  # Enhanced configuration generation with validation
  mkValidatedCollectorConfig = cfg: fullCfg:
    let
      config = mkCollectorConfig cfg fullCfg;
      
      # Validate enabled events
      eventValidation = validation.validateEnabledEvents config.enabled_events;
      
      # Validate dependencies
      depValidation = validation.validateDependencies cfg fullCfg;
      
      # Check completeness
      completenessCheck = validation.validateCompleteness cfg fullCfg;
      
      # Generate validation report
      validationReport = {
        valid = eventValidation.valid && depValidation.valid;
        errors = depValidation.errors;
        warnings = completenessCheck.warnings;
        unknownEvents = eventValidation.unknownEvents;
        malformedEvents = eventValidation.malformedEvents;
      };
      
    in {
      inherit config validationReport;
      # Throw error if validation fails
      validated = 
        if !validationReport.valid then
          throw ''Configuration validation failed:
            Errors: ${lib.concatStringsSep "\n  - " validationReport.errors}
            ${lib.optionalString ((lib.length validationReport.unknownEvents) > 0)
              "Unknown events: ${lib.concatStringsSep ", " validationReport.unknownEvents}"}
            ${lib.optionalString ((lib.length validationReport.malformedEvents) > 0)
              "Malformed events: ${lib.concatStringsSep ", " validationReport.malformedEvents}"}
          ''
        else config;
    };

  # Generate TOML config file with validation
  mkCollectorConfigFile = cfg: fullCfg: 
    let
      validatedResult = mkValidatedCollectorConfig cfg fullCfg;
      tomlContent = pkgs.runCommand "unified-collector.toml" {
        buildInputs = [ pkgs.remarshal ];
        passAsFile = [ "configJson" ];
        configJson = builtins.toJSON validatedResult.validated;
      } ''
        set -euo pipefail
        
        # Convert JSON to TOML
        ${pkgs.remarshal}/bin/json2toml < "$configJsonPath" > "$out.tmp"
        
        # Validate generated TOML
        if ! ${pkgs.remarshal}/bin/toml2json < "$out.tmp" > /dev/null 2>&1; then
          echo "ERROR: Generated TOML is invalid" >&2
          cat "$out.tmp" >&2
          exit 1
        fi
        
        mv "$out.tmp" "$out"
      '';
    in
      pkgs.writeText "unified-collector.toml" (builtins.readFile tomlContent);
  
  
  
}