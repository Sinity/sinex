# Kitty shell integration auto-configuration module
{ lib, config, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  kittySource = cfg.eventSources.kitty;
  
  # Script to auto-configure kitty shell integration
  configureKittyScript = pkgs.writeShellScript "configure-kitty-integration" ''
    set -euo pipefail
    
    USER_CONFIG_PATH="${kittySource.userConfigPath}"
    
    # Expand ~ in path
    if [[ "$USER_CONFIG_PATH" == "~/"* ]]; then
      USER_CONFIG_PATH="$HOME/''${USER_CONFIG_PATH#~/}"
    fi
    
    # Create config directory if it doesn't exist
    mkdir -p "$(dirname "$USER_CONFIG_PATH")"
    
    # Backup existing config if it exists
    if [[ -f "$USER_CONFIG_PATH" ]]; then
      cp "$USER_CONFIG_PATH" "$USER_CONFIG_PATH.sinex-backup-$(date +%s)"
      echo "Backed up existing kitty config to $USER_CONFIG_PATH.sinex-backup-*"
    fi
    
    # Required configuration for Sinex integration
    SINEX_CONFIG="
# === Sinex Integration Auto-Generated Configuration ===
# This section enables shell integration for command+output capture
# DO NOT MODIFY - Managed by Sinex NixOS module

# Enable shell integration for command boundaries
shell_integration enabled

# Allow remote control via socket for event capture
allow_remote_control socket-only

# Create socket for Sinex to connect to
listen_on unix:/tmp/kitty-$USER

# Enable cursor shape changes during editing
shell_integration no-cursor

# Keep window titles managed by shell
shell_integration no-title

# === End Sinex Auto-Generated Configuration ===
"
    
    # Check if Sinex config already exists
    if grep -q "=== Sinex Integration Auto-Generated Configuration ===" "$USER_CONFIG_PATH" 2>/dev/null; then
      echo "Sinex configuration already present in $USER_CONFIG_PATH"
      
      # Update the configuration section
      # Remove old section and add new one
      sed -i '/=== Sinex Integration Auto-Generated Configuration ===/,/=== End Sinex Auto-Generated Configuration ===/d' "$USER_CONFIG_PATH"
      echo "$SINEX_CONFIG" >> "$USER_CONFIG_PATH"
      echo "Updated Sinex configuration in $USER_CONFIG_PATH"
    else
      # Add configuration to the file
      echo "$SINEX_CONFIG" >> "$USER_CONFIG_PATH"
      echo "Added Sinex shell integration configuration to $USER_CONFIG_PATH"
    fi
    
    # Validate the configuration
    if command -v kitty >/dev/null 2>&1; then
      if kitty --config="$USER_CONFIG_PATH" --check-config 2>/dev/null; then
        echo "Kitty configuration validation successful"
      else
        echo "WARNING: Kitty configuration validation failed"
        echo "Please check $USER_CONFIG_PATH for syntax errors"
      fi
    else
      echo "Kitty not found in PATH - configuration written but not validated"
    fi
    
    echo "Kitty shell integration configuration complete"
    echo "Please restart any running Kitty instances for changes to take effect"
  '';
  
  # Script to remove Sinex configuration from kitty.conf
  removeKittyConfigScript = pkgs.writeShellScript "remove-kitty-integration" ''
    set -euo pipefail
    
    USER_CONFIG_PATH="${kittySource.userConfigPath}"
    
    # Expand ~ in path
    if [[ "$USER_CONFIG_PATH" == "~/"* ]]; then
      USER_CONFIG_PATH="$HOME/''${USER_CONFIG_PATH#~/}"
    fi
    
    if [[ -f "$USER_CONFIG_PATH" ]]; then
      if grep -q "=== Sinex Integration Auto-Generated Configuration ===" "$USER_CONFIG_PATH"; then
        # Remove Sinex configuration section
        sed -i '/=== Sinex Integration Auto-Generated Configuration ===/,/=== End Sinex Auto-Generated Configuration ===/d' "$USER_CONFIG_PATH"
        echo "Removed Sinex configuration from $USER_CONFIG_PATH"
      else
        echo "No Sinex configuration found in $USER_CONFIG_PATH"
      fi
    else
      echo "Kitty config file not found at $USER_CONFIG_PATH"
    fi
  '';

in
{
  config = mkIf (cfg.enable && kittySource.enable && kittySource.autoConfigureShellIntegration) {
    
    # Systemd service to configure kitty on service startup
    systemd.services.sinex-kitty-setup = {
      description = "Configure Kitty shell integration for Sinex";
      wantedBy = [ "sinex-terminal-satellite-1.service" ];
      before = [ "sinex-terminal-satellite-1.service" ];
      serviceConfig = {
        Type = "oneshot";
        User = cfg.targetUser;
        Group = "users";
        ExecStart = "${configureKittyScript}";
        ExecStop = "${removeKittyConfigScript}";
        RemainAfterExit = true;
      };
      environment = {
        HOME = "/home/${cfg.targetUser}";
        USER = cfg.targetUser;
      };
    };
    
    # User script for manual configuration and kitty package
    environment.systemPackages = [
      (pkgs.writeShellScriptBin "sinex-configure-kitty" ''
        echo "Configuring Kitty shell integration for Sinex..."
        sudo -u ${cfg.targetUser} ${configureKittyScript}
      '')
      
      (pkgs.writeShellScriptBin "sinex-remove-kitty-config" ''
        echo "Removing Kitty shell integration configuration..."
        sudo -u ${cfg.targetUser} ${removeKittyConfigScript}
      '')
    ] ++ lib.optionals (cfg.enable) [ pkgs.kitty ];
    
    # Validation warning if targetUser is not set
    warnings = optional (cfg.targetUser == null) ''
      Sinex Kitty shell integration is enabled but services.sinex.targetUser is not set.
      Auto-configuration will not work properly without a target user.
    '';
    
    # Documentation for manual setup if auto-config is disabled
    system.extraDependencies = mkIf (!kittySource.autoConfigureShellIntegration) [
      (pkgs.writeText "sinex-kitty-manual-setup.md" ''
        # Manual Kitty Shell Integration Setup for Sinex
        
        To enable command+output capture in Sinex, add the following to your kitty.conf:
        
        ```
        # Enable shell integration
        shell_integration enabled
        
        # Allow remote control for Sinex
        allow_remote_control socket-only
        listen_on unix:/tmp/kitty-$USER
        ```
        
        Then restart Kitty for changes to take effect.
        
        For more information, see: https://sw.kovidgoyal.net/kitty/shell-integration/
      '')
    ];
  };
}
