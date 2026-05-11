{ config, lib, pkgs, ... }:

let
  inherit (lib) mkEnableOption mkOption types;

  bindingModeType = types.enum [
    "stageOnly"
    "stageThenParse"
    "liveCapture"
    "externalProducer"
  ];

  privacyPolicyType = types.enum [
    "allowPlaintext"
    "metadataOnly"
    "encryptedMaterial"
    "localQuarantine"
    "suppressed"
    "explicitImport"
  ];

  scheduleType = types.submodule {
    options = {
      mode = mkOption {
        type = types.enum [ "continuous" "periodic" "oneshot" ];
        default = "continuous";
        description = "Acquisition schedule mode.";
      };
      interval = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "5m";
        description = "Interval for periodic mode.";
      };
      stableFor = mkOption {
        type = types.nullOr types.str;
        default = null;
        example = "5s";
        description = "File-drop stability window.";
      };
    };
  };

  sourceBindingModule = types.submodule {
    options = {
      enable = mkEnableOption "this source binding";
      sourceFamily = mkOption {
        type = types.str;
        description = "Logical source family identifier.";
        example = "terminal.atuin";
      };
      bindingMode = mkOption {
        type = bindingModeType;
        default = "stageThenParse";
        description = "Acquisition and parsing mode.";
      };
      resolverPreset = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "Built-in preset for path resolution (e.g. atuin.default).";
      };
      locator = mkOption {
        type = types.nullOr types.attrs;
        default = null;
        description = "Explicit locator overriding any preset.";
        example = { path = "/home/user/.local/share/atuin/history.db"; };
      };
      inputShapeKind = mkOption {
        type = types.str;
        description = "Input shape adapter kind.";
        example = "SqliteQuery";
      };
      materialFormatHint = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "Optional MIME type or format hint.";
      };
      parserId = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "Parser identifier (null for stage-only bindings).";
      };
      sourceUnitId = mkOption {
        type = types.nullOr types.str;
        default = null;
        description = "Stable source-unit identity.";
      };
      privacyPolicyId = mkOption {
        type = privacyPolicyType;
        default = "allowPlaintext";
        description = "Raw material capture class.";
      };
      rawMaterialPolicy = mkOption {
        type = types.attrs;
        default = {};
        description = "Additional raw-material policy overrides.";
      };
      schedule = mkOption {
        type = scheduleType;
        default = { mode = "continuous"; };
        description = "Acquisition schedule.";
      };
    };
  };
in
{
  options.services.sinex.sources = {
    bindings = mkOption {
      type = types.attrsOf sourceBindingModule;
      default = {};
      description = "Declarative source bindings. Each key names a binding.";
      example = {
        "terminal.atuin" = {
          enable = true;
          sourceFamily = "terminal.atuin";
          resolverPreset = "atuin.default";
          inputShapeKind = "SqliteQuery";
          parserId = "shell.atuin";
          sourceUnitId = "terminal.atuin";
          privacyPolicyId = "allowPlaintext";
        };
      };
    };
  };

  config = {
    services.sinex.sources.bindings = {

      # === terminal ===
      # Wave B target: terminal.{atuin-history,bash-history,zsh-history,fish-history,text-history,monitor}

      # === browser ===
      # Wave B target: browser.{history}

      # === document ===
      # Wave B target: document.{staging}

      # === fs ===
      # Wave B target: fs.{fs}

      # === system ===
      # Wave B target: system.{journald,systemd,dbus,udev,monitor}

      # === desktop ===
      # Wave B target: desktop.{window-manager,clipboard,activitywatch}

    };
  };
}
