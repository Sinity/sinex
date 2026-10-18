# Credential masking

Every JSON-RPC response, including errors, passes through a credential filter before any bytes are sent. The daemon, CLI and MCP tools share it. The filter is the `credential-output` crate from Polylogue's [`credential-output-v0.1.0` tag](https://github.com/Sinity/polylogue/tree/credential-output-v0.1.0), locked in `Cargo.lock` and checked by hash in the Nix build. It masks a fixed catalog of credential patterns and runs regardless of configured privacy rules. There is no way to reveal masked values through the API.

- Each JSON string is scanned as one value, including nested captured JSON and host metadata. Different fields are never joined.
- Query, show and trace rows carry `secret: true` when masking changed their text. This describes the response, not the stored bytes.
- Search snippets are masked as whole strings before being cut ([search.md](search.md#results)).
- Identifiers, cursors, hashes, query echoes and member names are never rewritten. If masking would change one, the whole response is refused. A request that contains a credential is refused before it runs; its request ID is echoed only if it is safe, otherwise it becomes null.
- Each masked string and the encoded response are limited to 128 MiB. A recognition the filter cannot finish within its bound returns a typed error.

Masking changes only what is presented. Stored rows, payloads, hashes and provenance keep their values. Optional [privacy rules](atuin.md#configuration) are applied once at admission. `trace --bytes` on a stored raw row applies the rules recorded with that row and then any current rules not already among them; it refuses if that would change an identifier. The credential filter then runs on the result.

Masking and encoding run synchronously and are not interrupted by the query deadline.
