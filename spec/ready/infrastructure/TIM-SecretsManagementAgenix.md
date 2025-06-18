# TIM-SecretsManagementAgenix: Secrets Management with `agenix` (NixOS)

## Status Dashboard
**Maturity Level**: L3 - Ready for Implementation
**Implementation**: 75% (Agenix foundation and auto-discovery working, Sinex integration pending)
**Dependencies**: NixOS agenix module, age encryption, SSH/age keys, Sinex service configurations
**Blocks**: Secure API key management, database password encryption, service authentication

## MVP Specification
- Agenix integration with NixOS configuration
- Encrypted secret files with proper key management
- Automatic secret discovery and configuration
- Environment variable export for services
- Basic secret rotation procedures

## Enhanced Features
- Database encryption with pgsodium integration
- Automated secret rotation and key management
- Service-specific secret scoping and permissions
- Secret audit logging and monitoring
- Integration with external secret management systems
- Backup and recovery procedures for secrets

## Implementation Checklist
- [x] Agenix flake integration and setup
- [x] Secret file encryption and storage
- [x] Public key management (users and systems)
- [x] Auto-discovery of encrypted secrets
- [x] Environment variable export
- [x] NixOS service integration
- [ ] Sinex-specific secret integration
- [ ] Database password encryption
- [ ] pgsodium master key management
- [ ] Automated secret rotation procedures

*   **Relevant ADR:** `[ADR-006-NixOSSecretsManagementTool.md](docs/adr/ADR-006-NixOSSecretsManagementTool.md)` (Decision: `agenix`)
*   **Original UG Context:** Section 21

This TIM details the use of `agenix` for managing secrets (API keys, database passwords, encryption keys) within the Exocortex NixOS configuration, as per ADR-006.

## 1. Rationale Summary (from ADR-006)

`agenix` is chosen for its simplicity, suitability for single-user systems, and direct integration with SSH keys or `age` identity files for decryption. It ensures secrets are encrypted in the NixOS configuration Git repository and decrypted securely at system activation.

## 2. `agenix` Core Concepts

*   Uses `age` (by Filippo Valsorda) for encryption.
*   Secrets stored as individual `.age` encrypted files.
*   Decryption based on `age` identities (X25519 keys, SSH keys).
*   Plaintext secrets never stored in world-readable Nix store (`/nix/store/...`).
*   Decryption at system activation, plaintext made available via restricted file in `/run/agenix.d/` or `/run/secrets/`.

## 3. `agenix` Setup and Usage in NixOS

### 3.1. Installation

Include `agenix` in your NixOS configuration's `environment.systemPackages` or ensure it's available for `nixos-rebuild`.

### 3.2. Encrypting a Secret

1.  **Identify Recipients:** Determine which `age` public keys (or SSH public keys) should be able to decrypt the secret. This is often the host's SSH public key (`/etc/ssh/ssh_host_ed25519_key.pub` or `ssh_host_rsa_key.pub`) for system services, or a user's personal `age` public key.
2.  **Create Secret File:** Place plaintext secret in a file (e.g., `my_api_key.txt`).
3.  **Encrypt with `agenix` CLI:**
    ```bash
    # Example: Encrypting for the host's SSH key and a user's age key
    # agenix -e my_api_key.txt.age -r ssh-ed25519 AAAAC3... host_key_comment \
    #                              -r age1qxyz... user_age_key_comment
    # This creates my_api_key.txt.age
    # Or, using a recipients file:
    # agenix -e my_api_key.txt.age -R /path/to/recipients.txt
    # where recipients.txt contains one public key per line.
    ```
    Store the resulting `.age` file in your NixOS configuration Git repository (e.g., in a `secrets/` subdirectory).

### 3.3. Declaring Secrets in NixOS Configuration (`configuration.nix`)

Use the `age.secrets.<name>` NixOS module options.

```nix
# In configuration.nix
# { config, pkgs, ... }:
# {
//   age.secrets.my_api_key_for_agent_x = {
//     file = ./secrets/my_api_key_for_agent_x.secret.age; # Path to encrypted .age file
//     owner = "sinex_agent_x_user"; # User that needs to read the decrypted secret
//     group = "sinex_agents_group"; # Group that can read (optional)
//     mode = "0440";                 # Permissions for decrypted file (e.g., read by owner/group)
//     # Path to decrypted file will be available as:
//     # config.age.secrets.my_api_key_for_agent_x.path
//     # e.g., /run/agenix.d/my_api_key_for_agent_x.secret
//   };

//   # Example for pgsodium master key (see TIM-PostgreSQLSecurityEncryption.md)
//   age.secrets.pgsodium_master_key = {
//     file = ./secrets/pgsodium_master_key.age;
//     owner = config.services.postgresql.user;    # Typically "postgres"
//     group = config.services.postgresql.group;   # Typically "postgres"
//     mode = "0400";                              # Read-only by owner
//   };
// }
```
*   On `nixos-rebuild switch`, `agenix` will attempt to decrypt these files using available private keys on the system (e.g., `/etc/ssh/ssh_host_ed25519_key` for host-encrypted secrets).
*   Decrypted files are placed in a tmpfs mount (e.g., `/run/agenix.d/` or `/run/secrets/`) with specified owner/group/mode.
*   Applications/services then read the plaintext secret from this path.

## 4. `agenix` Integration with `pgsodium` [UG Sec 21.2, SA4]

This is a key use case for `agenix` in Exocortex.
*   The `pgsodium_master_key.age` file contains a raw binary master encryption key.
*   `agenix` decrypts it to e.g., `/run/agenix.d/pgsodium_master_key`.
*   PostgreSQL setting `pgsodium.getkey_script` points to a shell script:
    ```nix
    # From UG Sec 21.2 / TIM-PostgreSQLSecurityEncryption.md
    # services.postgresql.settings."pgsodium.getkey_script" = pkgs.writeShellScriptBin "pgsodium-getkey-sps" ''
    #   #!${pkgs.bash}/bin/bash
    #   set -euo pipefail
    //   DECRYPTED_KEY_FILE="${config.age.secrets.pgsodium_master_key.path}"
    //   if [ ! -f "$DECRYPTED_KEY_FILE" ]; then exit 1; fi
    //   cat "$DECRYPTED_KEY_FILE" # Outputs raw binary key to stdout for pgsodium
    // '';
    ```

## 5. Automated Secret Rotation (Conceptual) [UG Sec 21.3, CR3]

For secrets like API keys that need periodic rotation (less so for `pgsodium` master key).
*   **Process:**
    1.  **Provision New Secret:** Script (e.g., systemd timer) generates new key, encrypts with `agenix` to `api_key_vNEXT.secret.age`.
    2.  **Update NixOS Config:** Manually or semi-automatically update `age.secrets.my_api_key.file` to point to new `.age` file (or use a symlink managed by the script).
    3.  **Deploy Config:** `nixos-rebuild switch`. `agenix` decrypts new secret.
    4.  **Application Logic:** App tries `vNEXT` key, falls back to `vCURRENT`.
    5.  **Deprecate Old Secret:** After overlap, remove old `.age` file from config.
*   Full automation of NixOS config update is complex and has security implications; manual/semi-auto is safer for personal system.

## 6. Key Management for `agenix`

*   **Host Keys:** For secrets needed by system services (like PostgreSQL), encrypting to the host's SSH public key is common. `agenix` will use the corresponding private key at `/etc/ssh/ssh_host_*_key` for decryption during system activation.
*   **User Keys:** For user-specific secrets or development, the user can generate their own `age` keypair:
    ```bash
    # age-keygen -o user_identity.key
    # cat user_identity.key # Shows private key
    # age-keygen -y user_identity.key # Shows public key (age1q...)
    ```
    Encrypt secrets using this public key. The user must have their `user_identity.key` available (e.g., in `~/.ssh/` or `~/.config/sops/age/keys.txt` which `agenix` might also check) on systems where decryption is needed. `AGE_KEY_FILE` env var can also point to it.
*   **`recipients.txt`:** Maintain a file listing all public keys (host, user) that should be able to decrypt a set of secrets, and use `agenix -R recipients.txt ...`.

