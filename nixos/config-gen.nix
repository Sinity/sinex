# Sinex Configuration Generation Module
{ lib, pkgs, ... }:

with lib;

rec {
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
        validPattern = "^[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*(?:\\.[a-z][a-z0-9_]*)*$";
      in
        builtins.match validPattern eventType != null;
    
    # Validate enabled events list
    validateEnabledEvents = enabledEvents:
      let
        knownEventTypes = [
          "shell.command.executed_atuin"
          "shell.history.command"
          "terminal.asciinema.session_started"
          "terminal.asciinema.session_ended"
          "terminal.scrollback.captured"
          "terminal.command_output.captured"
          "file.created"
          "file.modified"
          "file.deleted"
          "dbus.signal"
          "dbus.method_call"
          "system.notification"
          "media.playback.changed"
          "system.power.event"
          "hardware.device.event"
          "session.state.changed"
          "security.policykit.authorization"
          "bluetooth.device.event"
          "network.connection.event"
          "screen.saver.event"
          "storage.mount.event"
          "clipboard.content.changed"
          "clipboard.selection.changed"
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
            (cfg.sources.asciinema.enable && cfg.sources.asciinema.autoAnnex && !fullCfg.blobStorage.enable)
            "asciinema.autoAnnex requires blobStorage.enable = true")
          
          # Path existence checks
          (lib.optional
            (cfg.sources.atuin.enable && !(lib.hasPrefix "/" cfg.sources.atuin.databasePath))
            "atuin.databasePath must be an absolute path")
          
          (lib.optional
            (cfg.sources.kittyScrollback.enable && !(lib.hasPrefix "/" cfg.sources.kittyScrollback.socketPath))
            "kittyScrollback.socketPath must be an absolute path")
          
          # Interval validation
          (lib.optional
            (cfg.sources.atuin.enable && cfg.sources.atuin.pollInterval <= 0)
            "atuin.pollInterval must be greater than 0")
          
          (lib.optional
            (cfg.sources.kittyScrollback.enable && cfg.sources.kittyScrollback.captureInterval <= 0)
            "kittyScrollback.captureInterval must be greater than 0")
          
          (lib.optional
            (cfg.sources.clipboard.enable && cfg.sources.clipboard.pollInterval <= 0)
            "clipboard.pollInterval must be greater than 0")
          
          # Range validation
          (lib.optional
            (cfg.sources.kittyScrollback.enable && 
             (cfg.sources.kittyScrollback.maxScrollbackLines < 100 || cfg.sources.kittyScrollback.maxScrollbackLines > 1000000))
            "kittyScrollback.maxScrollbackLines must be between 100 and 1000000")
          
          (lib.optional
            (cfg.sources.clipboard.enable &&
             (cfg.sources.clipboard.maxHistoryEntries < 1 || cfg.sources.clipboard.maxHistoryEntries > 100000))
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
            (cfg.sources.filesystem.enable && 
             (lib.length cfg.sources.filesystem.watchPaths) > 10)
            "filesystem: watching more than 10 paths may impact performance")
          
          (lib.optional
            (cfg.sources.dbus.enable && cfg.sources.dbus.logAllSignals)
            "dbus.logAllSignals can generate very high event volume")
          
          # Security warnings
          (lib.optional
            (cfg.sources.clipboard.enable && !cfg.sources.clipboard.hashFileContent)
            "clipboard: file content hashing disabled - sensitive data may be stored")
          
          # Configuration completeness
          (lib.optional
            (!fullCfg.database.autoSetup && !fullCfg.database.migration.enabled)
            "database auto-setup and migrations both disabled - manual setup required")
        ];
        
        recommendations = lib.flatten [
          (lib.optional
            (cfg.sources.asciinema.enable && !cfg.sources.asciinema.autoAnnex)
            "consider enabling asciinema.autoAnnex for efficient storage")
          
          (lib.optional
            (cfg.sources.kittyScrollback.enable && !cfg.sources.kittyScrollback.captureOnCommand)
            "consider enabling kittyScrollback.captureOnCommand for better context")
        ];
      in {
        warnings = warnings;
        recommendations = recommendations;
      };
  };
  # Helper to generate collector configuration
  mkCollectorConfig = cfg: fullCfg: let
    enabledEvents = lib.flatten [
      (lib.optional cfg.sources.atuin.enable "shell.command.executed_atuin")
      (lib.optional cfg.sources.shellHistory.enable "shell.history.command")
      (lib.optional cfg.sources.asciinema.enable [
        "terminal.asciinema.session_started"
        "terminal.asciinema.session_ended"
      ])
      (lib.optional cfg.sources.kittyScrollback.enable [
        "terminal.scrollback.captured"
        "terminal.command_output.captured"
      ])
      (lib.optional cfg.sources.filesystem.enable [
        "file.created"
        "file.modified"
        "file.deleted"
      ])
      (lib.optional cfg.sources.dbus.enable [
        "dbus.signal"
        "dbus.method_call" 
        "system.notification"
        "media.playback.changed"
        "system.power.event"
        "hardware.device.event"
        "session.state.changed"
        "security.policykit.authorization"
        "bluetooth.device.event"
        "network.connection.event"
        "screen.saver.event"
        "storage.mount.event"
      ])
      (lib.optional cfg.sources.clipboard.enable [
        "clipboard.content.changed"
        "clipboard.selection.changed"
      ])
    ];

    # Build event configuration sections
    eventConfig = lib.optionalAttrs cfg.sources.atuin.enable {
      "event.shell_command_executed_atuin" = {
        db_path = cfg.sources.atuin.databasePath;
        polling_interval_secs = cfg.sources.atuin.pollInterval;
        use_file_watch = true;
        batch_size = 100;
      };
    } // lib.optionalAttrs cfg.sources.shellHistory.enable {
      "event.shell_history_command" = {
        history_files = [cfg.sources.shellHistory.zshPath cfg.sources.shellHistory.bashPath];
        polling_interval_secs = 10;
        use_file_watch = true;
      };
    } // lib.optionalAttrs cfg.sources.asciinema.enable {
      "event.terminal_asciinema" = {
        recordings_dir = cfg.sources.asciinema.recordingsPath;
        auto_start_recording = cfg.sources.asciinema.autoRecord;
        polling_interval_secs = 5;
        git_annex_repo = fullCfg.blobStorage.repositoryPath;
        auto_annex = cfg.sources.asciinema.autoAnnex;
      };
    } // lib.optionalAttrs cfg.sources.kittyScrollback.enable {
      "event.terminal_scrollback" = {
        kitty_socket_path = cfg.sources.kittyScrollback.socketPath;
        capture_interval_secs = cfg.sources.kittyScrollback.captureInterval;
        max_scrollback_lines = cfg.sources.kittyScrollback.maxScrollbackLines;
        capture_command_output = true;
        capture_on_command = cfg.sources.kittyScrollback.captureOnCommand;
        command_capture_delay_ms = cfg.sources.kittyScrollback.commandCaptureDelay;
      };
    } // lib.optionalAttrs cfg.sources.filesystem.enable {
      "event.files" = {
        watch_patterns = cfg.sources.filesystem.watchPaths;
        ignore_patterns = cfg.sources.filesystem.excludePatterns;
      };
    } // lib.optionalAttrs cfg.sources.dbus.enable {
      "event.dbus" = {
        monitor_session = cfg.sources.dbus.monitorSession;
        monitor_system = cfg.sources.dbus.monitorSystem;
        log_all_signals = cfg.sources.dbus.logAllSignals;
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
    } // lib.optionalAttrs cfg.sources.clipboard.enable {
      "event.clipboard" = {
        monitor_clipboard = cfg.sources.clipboard.monitorClipboard;
        monitor_primary = cfg.sources.clipboard.monitorPrimary;
        monitor_secondary = cfg.sources.clipboard.monitorSecondary;
        poll_interval_ms = cfg.sources.clipboard.pollInterval;
        hash_file_content = cfg.sources.clipboard.hashFileContent;
        max_preview_length = cfg.sources.clipboard.maxPreviewLength;
        enable_history = cfg.sources.clipboard.enableHistory;
        max_history_entries = cfg.sources.clipboard.maxHistoryEntries;
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
        recommendations = completenessCheck.recommendations;
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
  
  # Generate configuration with dry-run validation
  mkCollectorConfigDryRun = cfg: fullCfg:
    let
      validatedResult = mkValidatedCollectorConfig cfg fullCfg;
    in {
      inherit (validatedResult) config validationReport;
      
      # Generate summary report
      summary = {
        valid = validatedResult.validationReport.valid;
        enabledEvents = lib.length validatedResult.config.enabled_events;
        enabledSources = lib.length (lib.filter (source: 
          cfg.sources.${source}.enable or false
        ) [ "atuin" "shellHistory" "asciinema" "kittyScrollback" "filesystem" "dbus" "clipboard" ]);
        
        configSections = lib.length (lib.attrNames validatedResult.config) - 2; # minus enabled_events and output
        
        hasErrors = (lib.length validatedResult.validationReport.errors) > 0;
        hasWarnings = (lib.length validatedResult.validationReport.warnings) > 0;
        hasRecommendations = (lib.length validatedResult.validationReport.recommendations) > 0;
      };
    };
  
  # Configuration migration helpers
  migration = {
    # Migrate old configuration format to new
    migrateConfig = oldConfig: 
      let
        # Map old event names to new names
        eventNameMap = {
          "command_executed" = "command.executed";
          "file_created" = "file.created";
          "file_modified" = "file.modified";
          "file_deleted" = "file.deleted";
          "window_focused" = "window.focused";
          "workspace_changed" = "workspace.changed";
        };
        
        migratedEvents = map (event:
          eventNameMap.${event} or event
        ) (oldConfig.enabled_events or []);
      in
        oldConfig // {
          enabled_events = migratedEvents;
        };
    
    # Check if configuration needs migration
    needsMigration = config:
      let
        oldEventNames = [ "command_executed" "file_created" "file_modified" "file_deleted" "window_focused" "workspace_changed" ];
        hasOldEvents = lib.any (event: lib.elem event (config.enabled_events or [])) oldEventNames;
      in
        hasOldEvents;
  };
  
  # Configuration optimization suggestions
  optimization = {
    # Suggest performance optimizations
    getPerformanceSuggestions = cfg: fullCfg:
      let
        suggestions = lib.flatten [
          (lib.optional
            (cfg.sources.filesystem.enable && (lib.length cfg.sources.filesystem.watchPaths) > 5)
            {
              type = "performance";
              component = "filesystem";
              suggestion = "Consider reducing watched paths or using more specific patterns";
              impact = "high";
            })
          
          (lib.optional
            (cfg.sources.atuin.enable && cfg.sources.atuin.pollInterval < 5)
            {
              type = "performance";
              component = "atuin";
              suggestion = "Poll interval < 5s may cause high CPU usage";
              impact = "medium";
            })
          
          (lib.optional
            (cfg.sources.dbus.enable && cfg.sources.dbus.logAllSignals && cfg.sources.dbus.monitorSystem)
            {
              type = "performance";
              component = "dbus";
              suggestion = "Logging all system bus signals can generate very high volume";
              impact = "high";
            })
        ];
      in
        suggestions;
    
    # Suggest security improvements
    getSecuritySuggestions = cfg: fullCfg:
      let
        suggestions = lib.flatten [
          (lib.optional
            (cfg.sources.clipboard.enable && !cfg.sources.clipboard.hashFileContent)
            {
              type = "security";
              component = "clipboard";
              suggestion = "Enable file content hashing to avoid storing sensitive data";
              impact = "medium";
            })
          
          (lib.optional
            (!fullCfg.database.ssl.mode or fullCfg.database.ssl.mode == "disable")
            {
              type = "security";
              component = "database";
              suggestion = "Consider enabling SSL for database connections";
              impact = "high";
            })
        ];
      in
        suggestions;
  };
}