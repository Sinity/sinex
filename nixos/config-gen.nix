# Sinex Configuration Generation Module
{ lib, pkgs, ... }:

with lib;

rec {
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

  # Generate TOML config file
  mkCollectorConfigFile = cfg: fullCfg: pkgs.writeText "unified-collector.toml" 
    (builtins.readFile (pkgs.runCommand "unified-collector.toml" {
      buildInputs = [ pkgs.remarshal ];
      passAsFile = [ "configJson" ];
      configJson = builtins.toJSON (mkCollectorConfig cfg fullCfg);
    } ''
      ${pkgs.remarshal}/bin/json2toml < "$configJsonPath" > "$out"
    ''));
}