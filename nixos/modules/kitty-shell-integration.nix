# Kitty shell integration auto-configuration module
{ lib, config, pkgs, ... }:

with lib;

let
  cfg = config.services.sinex;
  kittySource = cfg.shell.kitty;
  targetUser = cfg.users.target;
  kittySnippetFile = pkgs.writeText "sinex-kitty-snippet.conf" kittySource.snippet;
  
  # Script to auto-configure kitty shell integration
  configureKittyScript = pkgs.writeShellScript "configure-kitty-integration" ''
    set -euo pipefail
    
    USER_CONFIG_PATH="${kittySource.configFile}"
    
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
    
    USER_CONFIG_PATH="${kittySource.configFile}"
    
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
  config = mkMerge [
    (mkIf (cfg.enable && kittySource.enable && targetUser != null) {
      environment.systemPackages = mkAfter (
        [ pkgs.kitty ]
        ++ lib.optionals kittySource.autoConfigure [
          (pkgs.writeShellScriptBin "sinex-configure-kitty" ''
            echo "Configuring Kitty shell integration for Sinex..."
            sudo -u ${targetUser} ${configureKittyScript}
          '')
          (pkgs.writeShellScriptBin "sinex-remove-kitty-config" ''
            echo "Removing Kitty shell integration configuration..."
            sudo -u ${targetUser} ${removeKittyConfigScript}
          '')
        ]
      );
    })

    (mkIf (cfg.enable && kittySource.enable && targetUser == null) {
      warnings = [ ''
        Sinex Kitty shell integration is enabled but services.sinex.users.target is not set.
        Auto-configuration requires a target user.
      '' ];
    })

    (mkIf (cfg.enable && kittySource.enable && kittySource.autoConfigure && targetUser != null) {
      systemd.services.sinex-kitty-setup = {
        description = "Configure Kitty shell integration for Sinex";
        wantedBy = [ "multi-user.target" ];
        serviceConfig = {
          Type = "oneshot";
          User = targetUser;
          Group = targetUser;
          ExecStart = "${configureKittyScript}";
          ExecStop = "${removeKittyConfigScript}";
          RemainAfterExit = true;
        };
        environment = {
          HOME = "/home/${targetUser}";
          USER = targetUser;
        };
      };
    })

    (mkIf (cfg.enable && kittySource.enable && !kittySource.autoConfigure) {
      system.extraDependencies = [
        (pkgs.writeText "sinex-kitty-manual-setup.md" ''
          # Manual Kitty Shell Integration Setup for Sinex

          Add the following block to your kitty.conf and restart Kitty to pick up the changes:

          ```
          ${kittySource.snippet}
          ```

          For more information, see: https://sw.kovidgoyal.net/kitty/shell-integration/
        '')
      ];
    })
  ];
}
