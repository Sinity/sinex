{ config, lib, options, ... }:

with lib;

let
  cfg = config.services.sinex;

  # Check if agenix is available (age.secrets option exists)
  agenixAvailable = options ? age && options.age ? secrets;

  # Default to the `nixos/secret/` directory in the Sinex source tree.
  # Overridable via services.sinex.secrets.secretsDirectory for external flake consumers
  # whose secrets live outside the Sinex repository.
  secretDir =
    if cfg.secrets.secretsDirectory != null
    then cfg.secrets.secretsDirectory
    else ../secret;
  available = builtins.pathExists secretDir;
  files = if available then builtins.readDir secretDir else {};
  ageFiles = filterAttrs (name: kind: kind == "regular" && hasSuffix ".age" name) files;
  serviceUser = cfg.users.nodes;
  targetUser = cfg.users.target;
  targetUserHome =
    if targetUser == null
    then null
    else attrByPath [ "users" "users" targetUser "home" ] "/home/${targetUser}" config;
  defaultOwner = serviceUser;

  mkSpec = filename: {
    file = secretDir + "/" + filename;
    owner = defaultOwner;
    group = defaultOwner;
    mode = "0400";
    path = "/run/agenix/" + removeSuffix ".age" filename;
  };

  specs = mapAttrs' (filename: _: nameValuePair (removeSuffix ".age" filename) (mkSpec filename)) ageFiles;

  conventionalEtcEntries = {
    sinex-gateway-admin-token = "sinex/gateway-admin-token";
    sinex-local-db = "sinex/db-password";
    sinex-remote-db = "sinex/remote-db-password";
    sinex-grafana-secret-key = "sinex/grafana-secret-key";
    grafana-secret-key = "sinex/grafana-secret-key";
    sinex-nats-server-cert = "sinex/nats-server-cert.pem";
    nats-server-cert = "sinex/nats-server-cert.pem";
    sinex-nats-server-key = "sinex/nats-server-key.pem";
    nats-server-key = "sinex/nats-server-key.pem";
    sinex-nats-client-ca = "sinex/nats-client-ca.pem";
    nats-client-ca = "sinex/nats-client-ca.pem";
    sinex-nats-ca = "sinex/nats-ca.pem";
    nats-ca = "sinex/nats-ca.pem";
    sinex-nats-client-cert = "sinex/nats-client-cert.pem";
    nats-client-cert = "sinex/nats-client-cert.pem";
    sinex-nats-client-key = "sinex/nats-client-key.pem";
    nats-client-key = "sinex/nats-client-key.pem";
    sinex-nats-client-creds = "sinex/nats-client.creds";
    nats-client-creds = "sinex/nats-client.creds";
    sinex-nats-client-nkey = "sinex/nats-client.nk";
    nats-client-nkey = "sinex/nats-client.nk";
    sinex-nats-token = "sinex/nats-token";
    nats-token = "sinex/nats-token";
    sinex-remote-nats-ca = "sinex/remote-nats-ca.pem";
    sinex-remote-nats-cert = "sinex/remote-nats-cert.pem";
    sinex-remote-nats-key = "sinex/remote-nats-key.pem";
  };

  lookupEtcSource =
    etcName:
    let
      entry = attrByPath [ "environment" "etc" etcName ] null config;
    in
    if entry != null && entry ? source then entry.source else null;

  conventionalEtcPaths = listToAttrs (
    filter (item: item != null) (
      mapAttrsToList
        (
          secretName: etcName:
          let
            source = lookupEtcSource etcName;
          in
          if source == null then null else nameValuePair secretName source
        )
        conventionalEtcEntries
    )
  );

  nonExport = [
    "sinex-local-db"
    "sinex-gateway-admin-token"  # gateway reads via SINEX_GATEWAY_ADMIN_TOKEN_FILE (file path, not raw content)
    "sinex-grafana-secret-key"
    "sinex-nats-ca"
    "sinex-nats-client-ca"
    "sinex-nats-client-cert"
    "sinex-nats-client-creds"
    "sinex-nats-client-key"
    "sinex-nats-client-nkey"
    "sinex-nats-server-cert"
    "sinex-nats-server-key"
    "sinex-nats-token"
    "nats-ca"
    "nats-client-ca"
    "nats-client-cert"
    "nats-client-creds"
    "nats-client-key"
    "nats-client-nkey"
    "grafana-secret-key"
    "nats-server-cert"
    "nats-server-key"
    "nats-token"
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
    ++ optional (
      targetUserHome != null && builtins.pathExists "${targetUserHome}/.ssh/id_ed25519"
    ) "${targetUserHome}/.ssh/id_ed25519";

  # Whether to actually configure agenix. Secret-backed provisioning paths such as
  # `sinnix.services.sinex.provisionDatabase` need the resolved secret files even
  # before the full Sinex service stack is enabled.
  shouldConfigureAgenix = agenixAvailable && (cfg.secrets.enableAgenix or false);
  agenixPaths = if shouldConfigureAgenix then mapAttrs (_: spec: spec.path) specs else {};
  resolvedSecretPaths = conventionalEtcPaths // agenixPaths;

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
    {
      sinex.secrets.paths = resolvedSecretPaths;
      sinex.secrets.exportScript = if shouldConfigureAgenix then exportScript else "";
    }
    (optionalAttrs agenixAvailable (mkIf shouldConfigureAgenix {
      age.identityPaths = mkDefault defaultIdentities;
      age.secrets = specs;
    }))
  ];
}
