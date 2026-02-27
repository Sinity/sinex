{ config, lib, options, ... }:

with lib;

let
  cfg = config.services.sinex;

  # Check if agenix is available (age.secrets option exists)
  agenixAvailable = options ? age && options.age ? secrets;

  # Default to the `secret/` directory adjacent to the Sinex source tree.
  # Overridable via services.sinex.secrets.secretsDirectory for external flake consumers
  # whose secrets live outside the Sinex repository.
  secretDir =
    if cfg.secrets.secretsDirectory != null
    then cfg.secrets.secretsDirectory
    else ../../secret;
  available = builtins.pathExists secretDir;
  files = if available then builtins.readDir secretDir else {};
  ageFiles = filterAttrs (name: kind: kind == "regular" && hasSuffix ".age" name) files;
  serviceUser = cfg.users.nodes;
  defaultOwner = serviceUser;

  mkSpec = filename: {
    file = secretDir + "/" + filename;
    owner = defaultOwner;
    group = defaultOwner;
    mode = "0400";
    path = "/run/agenix/" + removeSuffix ".age" filename;
  };

  specs = mapAttrs' (filename: _: nameValuePair (removeSuffix ".age" filename) (mkSpec filename)) ageFiles;

  nonExport = [
    "sinex-local-db"
    "sinex-gateway-admin-token"  # gateway reads via SINEX_GATEWAY_ADMIN_TOKEN_FILE (file path, not raw content)
    "sinex-remote-db"
    "sinex-remote-nats-ca"
    "sinex-remote-nats-cert"
    "sinex-remote-nats-key"
  ];

  mkExport = name: spec:
    let envName = toUpper (replaceStrings ["-" "."] ["_" "_"] name);
    in optionalString (!elem name nonExport) ''
      if [[ -r "${spec.path}" ]]; then
        export ${envName}="$(<${spec.path})"
      fi
    '';

  exportScript = concatStringsSep "\n" (filter (s: s != "") (mapAttrsToList mkExport specs));

  defaultIdentities = [ "/etc/ssh/ssh_host_ed25519_key" ]
    ++ optional (builtins.pathExists "/home/${defaultOwner}/.ssh/id_ed25519") "/home/${defaultOwner}/.ssh/id_ed25519";

  # Whether to actually configure agenix
  shouldConfigureAgenix = agenixAvailable && cfg.enable && (cfg.secrets.enableAgenix or false);

in
{
  options.sinex.secrets = {
    paths = mkOption {
      type = types.attrsOf types.path;
      description = "Resolved secret paths.";
      default = {};
    };
    exportScript = mkOption {
      type = types.str;
      description = "Shell snippet exporting decrypted secrets.";
      default = "";
    };
  };

  # optionalAttrs agenixAvailable guards age.* options existing at all (safe: checks options, not config).
  # mkIf defers cfg.enable evaluation, avoiding infinite recursion from reading config at module top-level.
  config = mkMerge [
    (mkIf shouldConfigureAgenix {
      sinex.secrets.paths = mapAttrs (_: spec: spec.path) specs;
      sinex.secrets.exportScript = exportScript;
    })
    (optionalAttrs agenixAvailable (mkIf shouldConfigureAgenix {
      age.identityPaths = mkDefault defaultIdentities;
      age.secrets = specs;
    }))
  ];
}
