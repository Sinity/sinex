# Secrets Management with agenix

**Status (2025-12-02)**: agenix is wired into the NixOS module (`services.sinex.secrets.enableAgenix = true` by default). Age files placed under `secret/*.age` are decrypted to `/run/agenix/<name>` and surfaced via `config.sinex.secrets.paths`. The gateway unit requires `sinex-gateway-admin-token.age` (or an explicit `services.sinex.secrets.gatewayAdminTokenFile`) and will refuse to start if the token file is missing.

## Overview

`agenix` is used for managing secrets (API keys, database passwords, encryption keys) within the NixOS configuration. It ensures secrets are encrypted in the Git repository and decrypted securely at system activation.

## Core Concepts

- Uses `age` encryption (by Filippo Valsorda)
- Secrets stored as individual `.age` encrypted files
- Decryption based on `age` identities (X25519 keys, SSH keys)
- Plaintext secrets never stored in world-readable Nix store
- Decryption at system activation to `/run/agenix.d/` or `/run/secrets/`

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
For system services, encrypt to the host's SSH public key:
- `/etc/ssh/ssh_host_ed25519_key.pub`
- agenix uses the private key for decryption during activation

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
- `.age` files under `secret/` are decrypted to `/run/agenix/<name>` and exposed via `config.sinex.secrets.paths`.
- Gateway requires `sinex-gateway-admin-token.age` (or `services.sinex.secrets.gatewayAdminTokenFile`) and refuses to start without it.

⚠️ Operator tasks (per deployment):
- Generate age keys for host/user and encrypt secrets.
- Place encrypted `.age` files under `secret/` or set explicit secret file paths.
- Rotate secrets by updating the encrypted file and rebuilding.

## Related Documentation
- ADR-006: NixOS Secrets Management Tool Decision
- TIM-PostgreSQLSecurityEncryption: Database encryption details
