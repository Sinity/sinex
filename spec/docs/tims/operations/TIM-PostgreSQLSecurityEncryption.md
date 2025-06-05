# TIM-PostgreSQLSecurityEncryption: Field-Level & Searchable Encryption

*   **Relevant ADR:** `[ADR-006-NixOSSecretsManagementTool.md](docs/adr/ADR-006-NixOSSecretsManagementTool.md)` (for `pgsodium` key management via `agenix`)
*   **Original UG Context:** Section 22.1

This TIM details strategies for encrypting sensitive data within PostgreSQL, focusing on `pgsodium` for robust field-level encryption and techniques for searchable encryption.

## 1. Rationale Summary

Protecting sensitive PII or confidential notes within the Exocortex database requires strong encryption. `pgsodium` leverages libsodium for high-performance, modern cryptography and offers transparent column encryption capabilities when integrated with server-managed keys (via `agenix`).

## 2. `pgcrypto` (Limited Use / Legacy Option) [UG Sec 22.1.1, OR3]

*   **Extension:** `CREATE EXTENSION IF NOT EXISTS pgcrypto;`
*   **Symmetric Encryption:**
    *   `pgp_sym_encrypt(data::text, psw::text [, options::text]) returns bytea`
    *   `pgp_sym_decrypt(msg::bytea, psw::text [, options::text]) returns text`
*   **Key Management:** Passphrase (`psw`) managed by application. **High risk of key exposure if passphrase in logged SQL.** Use parameterized queries.
*   **Exocortex Use:** Generally superseded by `pgsodium` for new development due to `pgsodium`'s superior key management and AEAD ciphers. Might be encountered if dealing with legacy encrypted data.

## 3. `pgsodium` (Preferred for Field-Level Encryption) [UG Sec 22.1.2, CR3, SA4]

### 3.1. Extension Setup

*   **NixOS:** `pkgs.pgsodium` package included in `environment.systemPackages` or PostgreSQL's `extraPlugins`.
*   **`postgresql.conf` / NixOS settings:**
    ```nix
    # services.postgresql.settings = {
    //   shared_preload_libraries = "pgsodium"; // Add to existing, e.g., "timescaledb,pgsodium"
    //   # Configure pgsodium.getkey_script to use agenix (see TIM-SecretsManagementAgenix.md)
    //   "pgsodium.getkey_script" = "${config.age.secrets.pgsodium_getkey_script_path}"; // Path to script
    // };
    ```
    *   Requires PostgreSQL restart after adding to `shared_preload_libraries`.
*   **Database Activation:** `CREATE EXTENSION IF NOT EXISTS pgsodium;`
*   **Root Key Setup (One-time by admin after `getkey_script` is configured):**
    ```sql
    -- This command tells pgsodium to fetch the master key using the getkey_script
    -- and store its ID (or a hash of it) internally.
    -- This is usually done once per database by a superuser.
    SELECT pgsodium.create_key(key_type:='aead-det', name:='primary_exocortex_master_key');
    -- Or for specific key contexts if not using a single master key for everything.
    -- The key fetched by getkey_script is the "root secret key".
    -- pgsodium then derives working keys from this root key using key IDs and contexts.
    ```

### 3.2. Core Encryption with `crypto_secretbox` (AEAD)

Uses XSalsa20 stream cipher + Poly1305 MAC.
*   **Functions:**
    *   `pgsodium.crypto_secretbox(message bytea, nonce bytea, key_id uuid)` returns `bytea` (ciphertext).
    *   `pgsodium.crypto_secretbox_open(ciphertext bytea, nonce bytea, key_id uuid)` returns `bytea` (plaintext) or `NULL` if decryption fails (auth tag mismatch).
    *   Variants exist to pass a raw `key bytea` and `key_context text` instead of `key_id uuid`. The `key_id` method uses `pgsodium.crypto_derive_key` internally with the root key.
*   **Nonces:** `pgsodium.crypto_secretbox_NONCEBYTES()` (24 bytes). **Must be unique for every message encrypted with the same key (key_id + context).** Store nonce alongside ciphertext. Generate with `pgsodium.randombytes_buf(pgsodium.crypto_secretbox_NONCEBYTES())`.
*   **Keys:**
    *   `key_id uuid`: A UUID chosen by the application to identify a derived key. `pgsodium` derives a unique working key from the master root key using this `key_id` and an optional `key_context`.
    *   **Example Usage (Encrypting a text field):**
        ```sql
        -- Assume table 'sensitive_notes' with columns:
        --   id ULID PK,
        --   encrypted_content BYTEA,
        --   content_nonce BYTEA,
        --   encryption_key_id UUID
        
        -- To encrypt:
        -- WITH new_note AS (
        //   SELECT
        //     'My secret note text'::TEXT AS plaintext_content,
        //     pgsodium.randombytes_buf(pgsodium.crypto_secretbox_NONCEBYTES()) AS generated_nonce,
        //     'some-chosen-uuid-for-this-note-type'::UUID AS key_id_for_encryption
        // )
        // INSERT INTO sensitive_notes (encrypted_content, content_nonce, encryption_key_id)
        // SELECT
        //   pgsodium.crypto_secretbox(
        //     convert_to(n.plaintext_content, 'UTF8'), -- Convert text to bytea
        //     n.generated_nonce,
        //     n.key_id_for_encryption
        //   ),
        //   n.generated_nonce,
        //   n.key_id_for_encryption
        // FROM new_note;

        -- To decrypt:
        -- SELECT
        //   convert_from(
        //     pgsodium.crypto_secretbox_open(
        //       sn.encrypted_content,
        //       sn.content_nonce,
        //       sn.encryption_key_id -- Or the specific key_id known to be used
        //     ),
        //     'UTF8' -- Convert bytea back to text
        //   ) AS decrypted_content
        // FROM sensitive_notes sn WHERE sn.id = 'some_ulid_pk';
        ```

### 3.3. Transparent Column Encryption (TCE) with Security Labels [CR3] (Advanced)

`pgsodium` (v3.0.0+) supports associating encryption keys with table columns via security labels. This can make encryption/decryption automatic for privileged roles.

*   **Mechanism:**
    1.  Define roles that have access to specific encryption keys.
    2.  Use `SECURITY LABEL FOR pgsodium ON COLUMN ... IS 'ENCRYPT WITH KEY ID ... ASSOCIATED (primary_key_cols, nonce_col)';`
    3.  `INSERT`/`UPDATE` by authorized roles can automatically encrypt. `SELECT` by authorized roles can automatically decrypt (often via a view defined with `DECRYPT WITH VIEW`).
*   **Status:** This is an advanced feature. Simpler application-level encryption (as in 3.2) is often easier to manage initially. Consult current `pgsodium` documentation for precise TCE syntax and capabilities.

### 3.4. Performance [CR3]

*   Low overhead (<5% for common workloads).
*   ~43,000 JSONB document encryptions/decryptions per second on commodity hardware [CR3].

## 4. Searchable Encryption with Blind Indexes [UG Sec 22.1.3, CR3]

Allows equality searches on encrypted data without full column decryption.
*   **Mechanism (Example using BLAKE2b for blind index token):**
    1.  **Derive Blind Index Key:** For each searchable encrypted field, derive a unique, secret key for its blind index (e.g., `pgsodium.crypto_kdf_derive_from_key(root_key_id, blind_index_subkey_id, 'context_for_email_blind_index')`).
    2.  **Generate Blind Index Token:** On `INSERT`/`UPDATE`:
        a.  Plaintext value (e.g., `user_email`).
        b.  Compute keyed hash: `pgsodium.crypto_shorthash(convert_to(plaintext_value, 'UTF8'), blind_index_key_for_email)`. This produces a short, fixed-size binary hash (e.g., 8 bytes for `crypto_shorthash`).
        c.  This binary hash is the "blind index token." Convert to `TEXT` (hex) or store as `BYTEA`.
    3.  **Storage:** Store token in a separate, unencrypted column (e.g., `user_email_blind_token BYTEA`). Create a standard B-tree index on this token column.
    4.  **Searching:**
        a.  For query `user_email = 'plaintext_query'`: Compute blind index token for `plaintext_query` using same key and process.
        b.  `SELECT * FROM users WHERE user_email_blind_token = 'computed_token_for_query';`
        c.  Application fetches candidate rows, decrypts the actual encrypted `user_email` field, and performs exact comparison to filter false positives.
*   **Performance & False Positives [CR3]:**
    *   With 8-bit tokens (highly truncated from `crypto_shorthash`'s 8 bytes for testing), 0.39% false positive rate on 50k records. p99 search latency 8ms.
    *   Using the full `crypto_shorthash` output (e.g., 8 bytes) dramatically reduces false positives but tokens are larger.
*   **Security:** Vulnerable to frequency analysis if attacker knows plaintext distribution and can see token distribution. Only for equality search. Keys must be secret.

## 5. `pgsodium` Key Rotation & Verification [UG Sec 22.1.4, `openai_sinex_6.md` Sec 7]

*   **Master Key:** The root key (from `getkey_script`) is long-lived.
*   **Derived Key Rotation (for a specific `key_id` or `key_id`+`context`):**
    1.  Choose a new `key_id_new`.
    2.  For each row encrypted with `key_id_old`:
        a.  Decrypt with `key_id_old`.
        b.  Re-encrypt with `key_id_new` (and a new unique nonce).
        c.  Update row with new ciphertext, nonce, and mark it as using `key_id_new`.
*   **SQL Verification Harness:** A PL/pgSQL `DO` block (see UG Sec 22.1.4 for example) to iterate over rows supposedly re-encrypted with `key_id_new` and verify they can be decrypted. Logs errors or success.

