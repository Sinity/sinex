# ADR-006: NixOS Secrets Management Tool

*   **Status:** Implemented  
*   **Date:** 2024-03-11
*   **Implementation Date:** 2025-07-17
*   **Context & Problem Statement:**
    The Exocortex system, managed by NixOS, requires a secure method for managing secrets such as API keys (e.g., for LLM providers), database passwords (e.g., for PostgreSQL users if not using peer auth for all local services), and encryption master keys (e.g., for `pgsodium`). These secrets need to be:
    1.  Stored securely (encrypted) within the NixOS configuration Git repository.
    2.  Decrypted at system activation/runtime.
    3.  Made available to specific services or users with appropriate permissions, without exposing them in the world-readable Nix store (`/nix/store`).
    NixOS offers two primary tools for this: `agenix` and `sops-nix`.

*   **Discussed Options:**

    1.  **`agenix`:**
        *   **Description:** Uses `age` (encryption tool by Filippo Valsorda) as its backend. Secrets are typically stored as individual `.age` encrypted files. Decryption is based on `age` identities (X25519 keys or SSH keys).
        *   **Pros:**
            *   **Simplicity:** Generally considered simpler to set up and use for basic scenarios, especially personal machines where host SSH keys or user `age` keys are readily available for decryption.
            *   **Clear Per-File Encryption:** Each secret is its own encrypted file, making it clear who (which `age` recipient key) can decrypt it.
            *   **Minimal Dependencies:** Relies on `age`, a small, modern, and well-regarded encryption tool.
            *   **Good for SSH-Key Based Access:** Easy to encrypt secrets to be decryptable by host SSH private keys, suitable for server-side secrets.
        *   **Cons:**
            *   **Opaque Diffs:** Encrypted `.age` files are binary blobs. `git diff` on these files shows that the file changed but not *which specific secret value within it* (if it were a structured file) changed, only that the ciphertext is different.
            *   **Manual Key Management for Multiple Users/Hosts:** Managing `age` recipient lists across many secrets or for team environments can become manual if not carefully scripted. (Less of an issue for single-user Exocortex).
            *   **No Built-in Cloud KMS Integration:** `age` itself does not directly integrate with cloud KMS services for key management (unlike SOPS).

    2.  **`sops-nix`:**
        *   **Description:** Uses Mozilla SOPS (Secrets OPerationS) as its backend. SOPS encrypts values within structured files (YAML, JSON, .env, binary). SOPS supports multiple encryption backends: `age`, GPG, AWS KMS, GCP KMS, Azure Key Vault, HashiCorp Vault.
        *   **Pros:**
            *   **Structured Secret Files:** Secrets are often managed in a single YAML or JSON file, with individual values encrypted. This makes it easier to see the structure of secrets.
            *   **Better Diffs (for Structure):** `git diff` on a SOPS-encrypted YAML/JSON file will show which *keys* (secret names) were added/removed or had their encrypted values changed, even if the values themselves are opaque. This improves auditability of *what* changed structurally.
            *   **Powerful Key Management & Policy (`.sops.yaml`):** SOPS uses a `.sops.yaml` file to define encryption rules, key groups, PGP fingerprints, KMS ARNs, or `age` recipients. This allows for more complex access control policies.
            *   **Cloud KMS Integration:** Native support for using cloud KMS services to encrypt the Data Encryption Key (DEK) used by SOPS, meaning the root key never leaves the KMS.
            *   **Templating:** `sops-nix` has good support for templating decrypted secrets directly into other NixOS configuration files.
        *   **Cons:**
            *   **More Complex Setup:** Requires understanding SOPS concepts and configuring the `.sops.yaml` policy file, which can be more involved than `agenix` for simple use cases.
            *   **Learning Curve:** SOPS itself has a richer feature set and thus a slightly steeper learning curve.

*   **Decision:**
    **`agenix` will be used as the primary secrets management tool** for the Exocortex NixOS configuration.
    *   Secrets (e.g., `pgsodium` master key, API keys for external services) will be encrypted into individual `.age` files using `agenix -e`.
    *   These `.age` files will be stored in the NixOS configuration Git repository.
    *   Decryption will typically be keyed to the host's SSH private key (for system-level secrets needed by services) or a user-specific `age` key.
    *   The `age.secrets.<name>` NixOS module options will be used to declare these secrets and manage their permissions and ownership when decrypted to `/run/agenix.d/` (or similar path like `/run/secrets/`).

*   **Rationale for Decision:**
    1.  **Simplicity and Suitability for Single-User System:** For a personal, single-host Exocortex deployment, `agenix` offers a simpler setup and mental model compared to `sops-nix`. The overhead of SOPS's more complex policy management (`.sops.yaml`) is less justified when primarily dealing with secrets for one user and one system.
    2.  **Direct SSH Key Integration:** `agenix`'s easy integration with host SSH keys for decryption is convenient for secrets needed by system services running on that host (like PostgreSQL with `pgsodium`).
    3.  **User Preference & Existing Familiarity (SADI ADR-006):** This decision aligns with stated user preference and familiarity with `agenix`.
    4.  **Adequate Security for Use Case:** For the defined Exocortex scope, `agenix` provides sufficient security. The lack of cloud KMS integration is not a concern for a local-first system. Opaque diffs are a minor inconvenience manageable for a single user.
    5.  **Focus on `age` Tooling:** `age` is a modern, well-audited, and focused encryption tool.

*   **Consequences:**
    *   The Exocortex NixOS configuration will include `agenix` setup (see UG Sec 21.2 for `pgsodium` example).
    *   A process for managing `age` recipient keys (e.g., ensuring the host SSH public key or user `age` public key is used for encryption) needs to be documented and followed.
    *   If the Exocortex evolves to a multi-user or team-managed system in the future, or if cloud deployment with KMS integration becomes a requirement, migrating to `sops-nix` might be reconsidered at that point. `sops-nix` also supports `age` as a backend, which could facilitate such a migration.
    *   Automated secret rotation (UG Sec 21.3) will be implemented using `agenix` for encryption of new secret versions.

