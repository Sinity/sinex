# Kitty shell integration auto-configuration module
{ lib, config, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  kittySource = cfg.shell.kitty;
  kittySnippetFile = pkgs.writeText "sinex-kitty-snippet.conf" kittySource.configSnippet;
  
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
    
    if [[ -f "$USER_CONFIG_PATH" ]]; then
      cp "$USER_CONFIG_PATH" "$USER_CONFIG_PATH.sinex-backup-$(date +%s)"
      echo "Backed up existing kitty config to $USER_CONFIG_PATH.sinex-backup-*"
    else
      touch "$USER_CONFIG_PATH"
    fi

    # Strip any existing Sinex-managed block before appending the current snippet
    sed -i '/# --- BEGIN Sinex managed block ---/,/# --- END Sinex managed block ---/d' "$USER_CONFIG_PATH" || true

    cat <<'EOF' >> "$USER_CONFIG_PATH"
# --- BEGIN Sinex managed block ---
EOF
    cat "${kittySnippetFile}" >> "$USER_CONFIG_PATH"
    cat <<'EOF' >> "$USER_CONFIG_PATH"
# --- END Sinex managed block ---
EOF

    echo "Applied Sinex Kitty configuration to $USER_CONFIG_PATH"
    
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
      if grep -q "# --- BEGIN Sinex managed block ---" "$USER_CONFIG_PATH"; then
        # Remove Sinex configuration section
        sed -i '/# --- BEGIN Sinex managed block ---/,/# --- END Sinex managed block ---/d' "$USER_CONFIG_PATH"
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
  config = mkIf (cfg.enable && kittySource.enable && kittySource.autoConfigure) {
    
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
    ] ++ lib.optionals (kittySource.enable) [ pkgs.kitty ];
    
    # Validation warning if targetUser is not set
    warnings = optional (cfg.targetUser == null) ''
      Sinex Kitty shell integration is enabled but services.sinex.targetUser is not set.
      Auto-configuration will not work properly without a target user.
    '';
    
    # Documentation for manual setup if auto-config is disabled
    system.extraDependencies = mkIf (!kittySource.autoConfigure) [
      (pkgs.writeText "sinex-kitty-manual-setup.md" ''
        # Manual Kitty Shell Integration Setup for Sinex

        Add the following block to your kitty.conf and restart Kitty to pick up
        the changes:

        ```
        ${kittySource.configSnippet}
        ```

        For more information, see: https://sw.kovidgoyal.net/kitty/shell-integration/
      '')
    ];
  };
}
