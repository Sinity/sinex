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

  # JSON representation of a single source binding for export.
  bindingToJson = name: binding: {
    inherit name;
    sourceUnitId = binding.sourceUnitId;
    sourceFamily = binding.sourceFamily;
    bindingMode = binding.bindingMode;
    inputShapeKind = binding.inputShapeKind;
    privacyPolicyId = binding.privacyPolicyId;
    parserId = binding.parserId;
    enabled = binding.enable;
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
    exportedJson = mkOption {
      type = types.package;
      readOnly = true;
      description = ''
        Derivation that writes a JSON file containing all declared source
        bindings.  Consume from the host config as:

          nix eval --raw .#nixosConfigurations.sinnix-prime.config.services.sinex.sources.exportedJson

        The resulting path can be passed to `xtask verify source-worker
        --bindings-json <path>` for drift detection against Rust descriptors.

        Shape: { bindings: [{ name, sourceUnitId, sourceFamily, bindingMode,
          inputShapeKind, privacyPolicyId, parserId, enabled }] }
      '';
    };
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

    polylogue = {
      enable = mkOption {
        type = types.bool;
        default = false;
        description = ''
          Enable the Polylogue bridge consumer (integration.polylogue source family).

          The Polylogue daemon is an external producer: it publishes
          metadata-only conversation-indexed events directly to NATS JetStream
          without depending on the sinex Rust SDK. ingestd accepts these events
          on the standard {env}.sinex.events.raw.> stream.

          Setting this to true signals that the Polylogue daemon is expected to
          be running and publishing events. This flag gates no sinex-side
          systemd service — the Polylogue publisher is the unblocker (see
          https://github.com/sinity/polylogue for the companion PR).

          The sinex-side source unit descriptor (integration.polylogue) and
          typed payload schema (PolylogueConversationIndexedPayload) land
          unconditionally; only the Polylogue daemon's runtime activation is
          gated here.
        '';
      };
    };
  };

  config = {
    services.sinex.sources.exportedJson = pkgs.writeText "sinex-source-bindings.json"
      (builtins.toJSON {
        bindings = lib.mapAttrsToList bindingToJson
          config.services.sinex.sources.bindings;
      });

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
