# Secrets and Secret Material

**Status (2026-03-23)**: agenix is wired into the NixOS module (`services.sinex.secrets.enableAgenix = true` by default). Age files placed under `nixos/secret/*.age` are decrypted to `/run/agenix/<name>` and surfaced via `config.sinex.secrets.paths`. The same secret-path registry also picks up conventional declarative `environment.etc."sinex/..."` entries, so the rest of the module can resolve either source uniformly. The gateway unit requires `sinex-gateway-admin-token.age` or `environment.etc."sinex/gateway-admin-token"` (or an explicit `services.sinex.secrets.gatewayAdminTokenFile`) and refuses to start if the token file is missing. Database-auth surfaces resolve `sinex-local-db` / `sinex-remote-db` automatically when password auth is enabled, and also honor `/etc/sinex/db-password` / `/etc/sinex/remote-db-password` when those are declared via `environment.etc`. The managed NATS surfaces also resolve conventional secret names automatically for local server TLS (`sinex-nats-server-cert`, `sinex-nats-server-key`, `sinex-nats-client-ca`) and the shared client TLS/auth path (`sinex-nats-ca`, `sinex-nats-client-cert`, `sinex-nats-client-key`, `sinex-nats-client-creds`, `sinex-nats-client-nkey`, `sinex-nats-token`, `sinex-remote-nats-ca`, `sinex-remote-nats-cert`, `sinex-remote-nats-key`). Grafana stays declarative: it derives a stable local key by default and will consume `sinex-grafana-secret-key.age` or `environment.etc."sinex/grafana-secret-key"` automatically when present. When `services.sinex.users.target` is known, the module now adds that user's `~/.ssh/id_ed25519` as an additional age identity alongside the host SSH key.

## Overview

The Sinex module can consume secret material from two declarative sources:

- `agenix` for real encrypted secrets committed to the repo
- conventional `environment.etc."sinex/..."` entries for local/dev or
  operator-managed file material

For anything sensitive, prefer agenix. The shared `config.sinex.secrets.paths`
registry lets the rest of the module treat both sources uniformly.

## Core Concepts

- Uses `age` encryption (by Filippo Valsorda)
- Secrets stored as individual `.age` encrypted files
- Decryption based on `age` identities (X25519 keys, SSH keys)
- Plaintext secrets never stored in world-readable Nix store
- Decryption at system activation to `/run/agenix.d/` or `/run/secrets/`
- Optional declarative file fallback through `environment.etc."sinex/..."` for
  deployments that are not using agenix yet

## Usage Guide

### 1. Encrypting a Secret

```bash
# Encrypt for host's SSH key and user's age key
agenix -e my_api_key.txt.age -r ssh-ed25519 AAAAC3... host_key_comment \
                             -r age1qxyz... user_age_key_comment

# Or using a recipients file
agenix -e my_api_key.txt.age -R /path/to/recipients.txt
```

Store the `.age` file in your NixOS configuration repository (e.g., `secrets/` directory).

### 2. Declaring Secrets in NixOS

```nix
{ config, pkgs, ... }:
{
  age.secrets.my_api_key_for_agent_x = {
    file = ./secrets/my_api_key_for_agent_x.secret.age;
    owner = "sinex_agent_x_user";
    group = "sinex_agents_group";
    mode = "0440";
    # Decrypted to: config.age.secrets.my_api_key_for_agent_x.path
  };

  # Example for pgsodium master key
  age.secrets.pgsodium_master_key = {
    file = ./secrets/pgsodium_master_key.age;
    owner = config.services.postgresql.user;
    group = config.services.postgresql.group;
    mode = "0400";
  };
}
```

### 3. Using Secrets in Services

Services can reference the decrypted secret path:

```nix
systemd.services.my-service = {
  serviceConfig = {
    EnvironmentFile = config.age.secrets.my_api_key.path;
  };
};
```

## pgsodium Integration

For PostgreSQL encryption with pgsodium:

```nix
services.postgresql.settings."pgsodium.getkey_script" = 
  pkgs.writeShellScriptBin "pgsodium-getkey-sps" ''
    #!${pkgs.bash}/bin/bash
    set -euo pipefail
    DECRYPTED_KEY_FILE="${config.age.secrets.pgsodium_master_key.path}"
    if [ ! -f "$DECRYPTED_KEY_FILE" ]; then exit 1; fi
    cat "$DECRYPTED_KEY_FILE"
  '';
```

## Key Management

### Host Keys
For system services, encrypt to the host's SSH public key and, when practical, the
interactive target user's SSH public key:
- `/etc/ssh/ssh_host_ed25519_key.pub`
- `~/.ssh/id_ed25519.pub`
- agenix uses the matching private keys during activation

### User Keys
Generate user-specific age keypair:
```bash
age-keygen -o user_identity.key
age-keygen -y user_identity.key  # Shows public key
```

Store in `~/.ssh/` or set `AGE_KEY_FILE` environment variable.

### Recipients File
Maintain a `recipients.txt` with all authorized public keys:
```
ssh-ed25519 AAAAC3... host-key
age1qxyz... user-key
```

## Secret Rotation

For secrets requiring periodic rotation:

1. Generate new secret and encrypt to `api_key_vNEXT.secret.age`
2. Update NixOS config to point to new file
3. Deploy with `nixos-rebuild switch`
4. Application logic handles transition (try new, fallback to old)
5. Remove old secret file after transition period

## Current Implementation Status

✅ Implemented:
- agenix is included in the flake inputs and imported by the Sinex NixOS module.
- `.age` files under `nixos/secret/` are decrypted to `/run/agenix/<name>` and exposed via `config.sinex.secrets.paths`.
- The same `config.sinex.secrets.paths` registry also imports conventional declarative `environment.etc."sinex/..."` entries.
- Gateway requires `sinex-gateway-admin-token.age`, `environment.etc."sinex/gateway-admin-token"`, or `services.sinex.secrets.gatewayAdminTokenFile`.
- Database password consumers resolve `sinex-local-db` / `sinex-remote-db` automatically and also honor `/etc/sinex/db-password` / `/etc/sinex/remote-db-password`.
- Managed local and remote NATS TLS/auth surfaces resolve the conventional `sinex-nats-*` and `sinex-remote-nats-*` secret names automatically.
- Grafana resolves `sinex-grafana-secret-key.age` or `/etc/sinex/grafana-secret-key` automatically when present, otherwise it uses the module-derived stable default.

⚠️ Operator tasks (per deployment):
- Generate age keys for host/user and encrypt secrets.
- Place encrypted `.age` files under `nixos/secret/`, declare conventional `environment.etc."sinex/..."` files, or set explicit secret file paths.
- Rotate secrets by updating the encrypted file and rebuilding.

## Related Documentation
- ADR-006: NixOS Secrets Management Tool Decision
- TIM-PostgreSQLSecurityEncryption: Database encryption details
