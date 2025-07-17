# TIM-BrowserNativeMessaging: Browser Extension to Native Host Communication

*   **Relevant ADR:** (N/A directly, core for browser ingestor)
*   **Original UG Context:** Section 10.2

This TIM details the Native Messaging mechanism used by the Exocortex browser extension to communicate with a local native application (the "native messaging host"), which then relays data to the core Exocortex backend.

## 1. Rationale Summary

Native Messaging is essential for Manifest V3 extensions to offload complex processing, access system resources beyond browser sandbox capabilities (e.g., writing to `core.events` DB), and manage persistent state or connections that are difficult with service worker lifecycles.

## 2. Protocol Details [UG Sec 10.2.1, OR2]

*   **Mechanism:** Browser starts native host process. Communication via host's `stdin` and `stdout`.
*   **Message Format:**
    *   Messages are JSON objects.
    *   Each JSON message string is prefixed by a **4-byte unsigned integer** representing the length of the JSON message in bytes.
    *   Length prefix is in **native byte order** (typically little-endian).
*   **Extension Permissions:** `manifest.json` must declare `"nativeMessaging"` permission and list allowed native host names in `"permissions"`.

## 3. Host Manifest JSON (`com.sinnix.exocortex.nativehost.json`) [UG Sec 10.2.2, OR2]

This JSON file registers the native host application with the browser.

*   **Structure:**
    ```json
    {
      "name": "com.sinnix.exocortex.nativehost", // Unique name used by extension
      "description": "Sinnix Exocortex Native Messaging Host",
      // Path must be absolute. This will be managed by NixOS.
      "path": "/opt/sinnix-exocortex/bin/sinex_browser_native_host",
      "type": "stdio", // Must be "stdio"
      "allowed_origins": [ // For Chromium-based browsers
        "chrome-extension://YOUR_EXOCORTEX_EXTENSION_ID_CHROME/"
      ],
      "allowed_extensions": [ // For Firefox
        "exocortex_extension@sinnix.com" // ID from extension's manifest.json browser_specific_settings.gecko.id
      ]
    }
    ```
*   **Installation Paths (Linux):**
    *   Chrome/Chromium (user): `~/.config/chromium/NativeMessagingHosts/`, `~/.config/google-chrome/NativeMessagingHosts/`
    *   Chrome/Chromium (system): `/etc/chromium/native-messaging-hosts/`, `/etc/opt/chrome/native-messaging-hosts/`
    *   Firefox (user): `~/.mozilla/native-messaging-hosts/`
    *   Firefox (system): `/usr/lib/mozilla/native-messaging-hosts/` (or similar distro path)
    *   The file must be named `<host_name>.json` (e.g., `com.sinnix.exocortex.nativehost.json`).

## 4. NixOS Integration for Host Manifest and Binary [UG Sec 10.2.3, OR2]

NixOS declaratively manages the host manifest and ensures the `path` points to the correct binary from a Nix package.

*   **Example (`configuration.nix`):**
    ```nix
    # { pkgs, config, ... }:
    # let
    #   sinexNativeHostPkg = pkgs.callPackage ./path/to/sinex-native-host-package.nix {}; # Rust binary package
    #   hostName = "com.sinnix.exocortex.nativehost";
    #   # These IDs must match your actual extension IDs
    #   chromeExtensionId = "abcdefghijklmnopabcdefghijklmnop"; # Example, replace with actual
    #   firefoxExtensionId = "exocortex_extension@sinnix.com";
    # in
    # {
    //   programs.chromium.nativeMessagingHosts."${hostName}" = {
    //     path = "${sinexNativeHostPkg}/bin/sinex_browser_native_host"; # Path to binary in Nix store
    //     allowedOrigins = [ "chrome-extension://${chromeExtensionId}/" ];
    //     description = "Sinnix Exocortex Native Messaging Host";
    //   };
    //   programs.firefox.nativeMessagingHosts."${hostName}" = {
    //     path = "${sinexNativeHostPkg}/bin/sinex_browser_native_host";
    //     allowedExtensions = [ firefoxExtensionId ];
    //     description = "Sinnix Exocortex Native Messaging Host";
    //     # mode = "user"; # Or "system", user is often default
    //   };
    // }
    ```

## 5. Native Host Implementation (`sinex_browser_native_host`)

Typically a small Rust or Python application.

*   **Responsibilities:**
    1.  Reads 4-byte length prefix from `stdin`.
    2.  Reads `length` bytes of JSON message from `stdin`.
    3.  Deserializes JSON.
    4.  Processes the message (e.g., validates, transforms, inserts into Exocortex `core.events` DB via `sqlx` or `psycopg2`).
    5.  (Optional) Sends a JSON response back to extension via `stdout`, using same length-prefixing.
    6.  Must **not** write any non-protocol debug output to `stdout`. Debug logs to `stderr` or a file.
    7.  Handles `stdin` closing (extension disconnected) and exits gracefully.

*   **Rust Example (Conceptual Read Loop using `tokio`):**
    ```rust
    // use tokio::io::{stdin, stdout, AsyncReadExt, AsyncWriteExt};
    // use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt}; // From 'byteorder' crate
    // use serde_json::{Value as JsonValue, json};
    // use std::io::Cursor;

    // async fn main_native_host_loop() -> Result<(), anyhow::Error> {
    //     let mut stdin_handle = stdin();
    //     let mut stdout_handle = stdout();

    //     loop {
    //         // 1. Read 4-byte length prefix
    //         let mut len_bytes = [0u8; 4];
    //         match stdin_handle.read_exact(&mut len_bytes).await {
    //             Ok(0) => { // EOF, browser closed connection
    //                 eprintln!("Native host: Browser closed stdin. Exiting.");
    //                 break;
    //             }
    //             Ok(_) => {}
    //             Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
    //                 eprintln!("Native host: Browser closed stdin unexpectedly (EOF). Exiting.");
    //                 break;
    //             }
    //             Err(e) => {
    //                 eprintln!("Native host: Error reading length prefix: {}", e);
    //                 return Err(e.into());
    //             }
    //         }
            
    //         let mut cursor = Cursor::new(len_bytes);
    //         let message_len = cursor.read_u32::<NativeEndian>()? as usize;

    //         if message_len == 0 || message_len > 1_048_576 { // Protect against empty or too large messages (1MB limit example)
    //             eprintln!("Native host: Invalid message length received: {}", message_len);
    //             // Potentially send error back to extension or just break
    //             break; 
    //         }

    //         // 2. Read JSON message
    //         let mut message_bytes = vec![0u8; message_len];
    //         stdin_handle.read_exact(&mut message_bytes).await?;
            
    //         // 3. Deserialize JSON
    //         let received_json: JsonValue = serde_json::from_slice(&message_bytes)?;
    //         eprintln!("Native host: Received from extension: {:?}", received_json); // Log to stderr

    //         // 4. Process message (e.g., insert into Exocortex DB)
    //         // let processing_result = process_message_for_exocortex(received_json).await;
    //         let response_payload = json!({ "status": "received", "original_message_id": received_json.get("id") }); // Example response

    //         // 5. (Optional) Send response
    //         let response_str = response_payload.to_string();
    //         let response_bytes = response_str.as_bytes();
    //         let response_len = response_bytes.len() as u32;

    //         let mut len_prefix_bytes = Vec::new();
    //         len_prefix_bytes.write_u32::<NativeEndian>(response_len)?;
            
    //         stdout_handle.write_all(&len_prefix_bytes).await?;
    //         stdout_handle.write_all(response_bytes).await?;
    //         stdout_handle.flush().await?;
    //     }
    //     Ok(())
    // }
    ```

## 6. Communication from Extension [UG Sec 10.2.1]

*   **Connect (Long-Lived Port):**
    ```javascript
    // const port = chrome.runtime.connectNative("com.sinnix.exocortex.nativehost");
    // port.onMessage.addListener((response) => {
    //   console.log("Received from native host:", response);
    // });
    // port.onDisconnect.addListener(() => {
    //   console.error("Native host disconnected:", chrome.runtime.lastError?.message);
    // });
    // port.postMessage({ type: "EXOCORTEX_EVENT", data: { ... } });
    ```
*   **Single Message (Request/Response):**
    ```javascript
    // chrome.runtime.sendNativeMessage(
    //   "com.sinnix.exocortex.nativehost",
    //   { type: "GET_CONFIG", key: "some_setting" },
    //   (response) => {
    //     if (chrome.runtime.lastError) {
    //       console.error("Error sending to native host:", chrome.runtime.lastError.message);
    //     } else {
    //       console.log("Response from native host:", response);
    //     }
    //   }
    // );
    ```

## 7. Performance and Security [UG Sec 10.2.4, OR2]

*   **Performance:** Not for very high-frequency, low-latency streaming. Suitable for bundles of data (event payloads) a few times per second or less.
*   **Security:**
    *   Native host runs with user privileges, not sandboxed like extension.
    *   `allowed_origins`/`allowed_extensions` in host manifest is key defense against unauthorized extensions connecting.
    *   Native host must validate/sanitize all data received from extension before using it (e.g., in DB queries, file paths).
    *   Path to native host executable in manifest must be absolute to a trusted binary.

