use crate::common::events;
use crate::common::prelude::*;
use sinex_db::queries;
use std::time::Instant;

// ==================== FILESYSTEM EVENT ATTACKS ====================

#[sinex_test]
async fn test_filesystem_unicode_normalization_collision(
    _ctx: TestContext,
) -> Result<(), anyhow::Error> {
    // Different Unicode representations of "same" filename
    let unicode_variants = vec![
        ("NFC", "café"),                      // é as single codepoint
        ("NFD", "café"),                      // é as e + combining accent
        ("Escaped", "caf\u{00E9}"),           // é as escape sequence
        ("Combining", "caf\u{0065}\u{0301}"), // e + combining acute
    ];

    println!("Testing filesystem Unicode normalization attacks:");

    for (variant1_name, variant1) in &unicode_variants {
        for (variant2_name, variant2) in &unicode_variants {
            if variant1_name == variant2_name {
                continue;
            }

            // These might be treated as same file on some systems
            println!(
                "  {} '{}' vs {} '{}'",
                variant1_name, variant1, variant2_name, variant2
            );
            println!(
                "    Bytes: {:?} vs {:?}",
                variant1.as_bytes(),
                variant2.as_bytes()
            );

            if variant1 == variant2 {
                println!("    COLLISION: Rust sees as equal despite different bytes!");
            } else if variant1.to_lowercase() == variant2.to_lowercase() {
                println!("    CASE COLLISION: Equal when case-folded!");
            }
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_filesystem_case_sensitivity_race(_ctx: TestContext) -> Result<(), anyhow::Error> {
    // Test rapid case variations of same filename
    let case_variants = ["test.txt", "Test.txt", "TEST.txt", "TeSt.TxT", "test.TXT", "TEST.TXT"];

    println!("Testing filesystem case sensitivity races:");

    // On case-insensitive FS, these all refer to same file
    // But events might be generated for each "different" name
    let mut event_payloads = vec![];

    for (i, variant) in case_variants.iter().enumerate() {
        let payload = json!({
            "path": format!("/tmp/{}", variant),
            "action": if i % 2 == 0 { "create" } else { "modify" },
            "size": i * 100,
        });

        event_payloads.push(payload);
        println!("  Event {}: {}", i, variant);
    }

    // Check for logical inconsistencies
    println!("\nPotential issues:");
    println!("- Case-insensitive FS: All events refer to same file");
    println!("- Case-sensitive FS: Events refer to different files");
    println!("- Mixed processing: Some components case-sensitive, others not");

    Ok(())
}

#[sinex_test]
async fn test_filesystem_null_byte_injection(_ctx: TestContext) -> Result<(), anyhow::Error> {
    // Paths with null bytes - many systems handle these differently
    let malicious_paths = vec![
        "/etc/passwd\0.txt",
        "/home/user/.ssh/id_rsa\0.backup",
        "config\0.toml",
        "/var/log/\0/secure",
    ];

    println!("Testing null byte injection in paths:");

    for path in malicious_paths {
        println!("  Path: {:?}", path);
        println!("    Length: {} bytes", path.len());

        // Find null byte position
        if let Some(null_pos) = path.bytes().position(|b| b == 0) {
            let truncated = &path[..null_pos];
            println!("    Truncated at null: {:?}", truncated);
            println!("    DANGER: Might access '{}'", truncated);
        }

        // Test JSON encoding
        let event = json!({
            "path": path,
            "action": "read"
        });

        match serde_json::to_string(&event) {
            Ok(json_str) => {
                println!("    JSON encoding succeeded: {}", json_str);
                // Check if null survived
                if json_str.contains("\\u0000") {
                    println!("    NULL PRESERVED in JSON!");
                }
            }
            Err(e) => {
                println!("    JSON encoding failed: {}", e);
            }
        }
    }

    Ok(())
}

// ==================== TERMINAL EVENT ATTACKS ====================

#[sinex_test]
async fn test_terminal_ansi_escape_injection(_ctx: TestContext) -> Result<(), anyhow::Error> {
    // Malicious ANSI escape sequences that could compromise terminal
    let evil_escapes = vec![
        ("\x1b[3J", "Clear scrollback buffer"),
        ("\x1b[2J\x1b[H", "Clear screen and reset cursor"),
        ("\x1b]0;HACKED\x07", "Change terminal title"),
        ("\x1b[?1049h", "Switch to alternate screen"),
        ("\x1b[41m\x1b[37m", "Red background, white text"),
        ("\x1b[0m\x1b[?25l", "Reset format, hide cursor"),
        ("\x1b]11;?\x07", "Query background color (info leak)"),
    ];

    println!("Testing terminal ANSI escape injection:");

    for (escape, description) in evil_escapes {
        let event_payload = json!({
            "output": format!("Normal text {} more text", escape),
            "command": "echo",
            "terminal_id": "pts/1",
        });

        println!("  {}: {:?}", description, escape);
        println!("    Bytes: {:?}", escape.as_bytes());

        // Check if JSON encoding preserves the escapes
        if let Ok(json_str) = serde_json::to_string(&event_payload) {
            if json_str.contains("\x1b") {
                println!("    DANGER: Raw ESC character in JSON!");
            } else if json_str.contains("\\u001b") {
                println!("    Escaped as Unicode (safer)");
            }
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_terminal_control_character_smuggling(_ctx: TestContext) -> Result<(), anyhow::Error> {
    // Control characters that could affect process control
    let control_chars = vec![
        ('\x03', "ETX (Ctrl+C)", "SIGINT - terminates process"),
        ('\x04', "EOT (Ctrl+D)", "EOF - closes shell"),
        ('\x1A', "SUB (Ctrl+Z)", "SIGTSTP - suspends process"),
        ('\x1C', "FS (Ctrl+\\)", "SIGQUIT - quits with core dump"),
        ('\x7F', "DEL", "Delete character"),
        ('\x00', "NUL", "String terminator"),
    ];

    println!("Testing terminal control character smuggling:");

    for (char, name, effect) in control_chars {
        let payload = json!({
            "output": format!("Before{}After", char),
            "raw_bytes": format!("{:02X}", char as u8),
        });

        println!("  {}: {} - {}", name, effect, char as u8);

        match serde_json::to_string(&payload) {
            Ok(json) => {
                if json.contains(&format!("{}", char)) {
                    println!("    DANGER: Raw control char in JSON!");
                }
            }
            Err(e) => {
                println!("    JSON encoding failed: {}", e);
            }
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_terminal_utf8_overlong_encoding(_ctx: TestContext) -> Result<(), anyhow::Error> {
    // Overlong UTF-8 sequences that might bypass filters
    let overlong_sequences = vec![
        (vec![0xC0, 0x80], "Overlong NULL"),
        (vec![0xC0, 0xAF], "Overlong slash '/'"),
        (vec![0xC0, 0xAE], "Overlong dot '.'"),
        (vec![0xE0, 0x80, 0xAF], "Triple-byte overlong slash"),
        (vec![0xF0, 0x80, 0x80, 0xAF], "Quad-byte overlong slash"),
    ];

    println!("Testing UTF-8 overlong encoding attacks:");

    for (bytes, description) in overlong_sequences {
        println!("  {}: {:?}", description, bytes);

        match String::from_utf8(bytes.clone()) {
            Ok(s) => {
                println!("    DANGER: Accepted as valid UTF-8: {:?}", s);
            }
            Err(e) => {
                println!("    Properly rejected: {}", e);
            }
        }
    }

    Ok(())
}

// ==================== WINDOW MANAGER EVENT ATTACKS ====================

#[sinex_test]
async fn test_window_geometry_overflow(ctx: TestContext) -> TestResult {
    let overflow_geometries = vec![
        (i32::MAX, i32::MAX, 100, 100, "Max position"),
        (-2147483648, -2147483648, 100, 100, "Min position"),
        (0, 0, i32::MAX as u32, i32::MAX as u32, "Max size"),
        (0, 0, 0, 0, "Zero size"),
        (-1000, -1000, u32::MAX, u32::MAX, "Negative pos, max size"),
    ];

    println!("Testing window geometry integer overflows:");

    for (x, y, width, height, desc) in overflow_geometries {
        let event = crate::common::events::generic_adversarial_event(
            "wm.hyprland",
            "window.created",
            json!({
                "x": x,
                "y": y,
                "width": width,
                "height": height,
                "title": desc
            }),
            None,
        );

        match queries::insert_event(ctx.pool(), &event).await {
            Ok(_) => {
                println!(
                    "  {}: Accepted geometry ({},{}) {}x{}",
                    desc, x, y, width, height
                );

                // Check for integer overflow in area calculation
                let area = (width as i64) * (height as i64);
                if area > i32::MAX as i64 {
                    println!("    OVERFLOW: Area calculation exceeds i32!");
                }
            }
            Err(e) => {
                println!("  {}: Rejected - {}", desc, e);
            }
        }
    }

    Ok(())
}

#[sinex_test]
async fn test_window_circular_parent_reference(_ctx: TestContext) -> TestResult {
    // Window parent-child relationships that form cycles
    let circular_configs = vec![
        vec![("A", "B"), ("B", "C"), ("C", "A")], // 3-node cycle
        vec![("X", "Y"), ("Y", "X")],             // 2-node cycle
        vec![("W", "W")],                         // Self-parent
    ];

    println!("Testing circular window parent references:");

    for config in circular_configs {
        println!("  Configuration: {:?}", config);

        // Build events
        for (window_id, parent_id) in &config {
            let _event = json!({
                "window_id": window_id,
                "parent_id": parent_id,
                "event": "reparent",
            });

            println!("    {} -> {}", window_id, parent_id);
        }

        // Detect cycle
        let mut visited = std::collections::HashSet::new();
        let mut current = config[0].0;
        let mut cycle_detected = false;

        for _ in 0..config.len() + 1 {
            if visited.contains(current) {
                println!("    CYCLE DETECTED at {}", current);
                cycle_detected = true;
                break;
            }
            visited.insert(current);

            if let Some((_, parent)) = config.iter().find(|(w, _)| w == &current) {
                current = parent;
            }
        }

        if !cycle_detected {
            println!("    No cycle detected");
        }
    }

    Ok(())
}

// ==================== CROSS-EVENT-TYPE INTERACTIONS ====================

#[sinex_test]
async fn test_event_cascade_explosion(ctx: TestContext) -> TestResult {
    // Simulate cascading events: filesystem -> terminal -> window
    println!("Testing cascading event explosion:");

    let start = Instant::now();
    let mut total_events = 0;

    // Initial filesystem event
    let fs_event = events::filesystem_chaos_event("file.modified", "/tmp/trigger.sh", None);

    queries::insert_event(ctx.pool(), &fs_event).await?;
    total_events += 1;

    // Simulate: file change triggers 10 terminal commands
    for i in 0..10 {
        let term_event = crate::common::events::generic_adversarial_event(
            "terminal",
            "command.executed",
            json!({
                "command_index": i,
                "triggered_by": fs_event.id.to_string()
            }),
            None,
        );

        queries::insert_event(ctx.pool(), &term_event).await?;
        total_events += 1;

        // Each terminal command opens a notification window
        let win_event = crate::common::events::generic_adversarial_event(
            "wm.hyprland",
            "window.created",
            json!({"test": true}),
            None,
        );

        queries::insert_event(ctx.pool(), &win_event).await?;
        total_events += 1;
    }

    let elapsed = start.elapsed();
    println!("  Generated {} events in {:?}", total_events, elapsed);
    println!(
        "  Rate: {:.0} events/sec",
        total_events as f64 / elapsed.as_secs_f64()
    );

    if total_events > 20 {
        println!(
            "  CASCADE EXPLOSION: 1 event triggered {} events!",
            total_events
        );
    }

    Ok(())
}

#[sinex_test]
async fn test_event_type_confusion(_ctx: TestContext) -> TestResult {
    // Send events to wrong sources
    let confused_events = vec![
        (
            "fs",
            json!({
                "window_id": "0x12345",  // Window data in filesystem event
                "geometry": {"x": 0, "y": 0},
            }),
        ),
        (
            "terminal",
            json!({
                "path": "/etc/passwd",  // Filesystem data in terminal event
                "inode": 12345,
            }),
        ),
        (
            "wm.hyprland",
            json!({
                "command": "rm -rf /",  // Terminal data in window event
                "exit_code": 0,
            }),
        ),
    ];

    println!("Testing event type confusion:");

    for (source, wrong_payload) in confused_events {
        println!(
            "  Source '{}' with wrong payload: {}",
            source, wrong_payload
        );

        // Check if payload makes sense for source
        match source {
            "fs" => {
                if wrong_payload.get("window_id").is_some() {
                    println!("    TYPE CONFUSION: Window data in filesystem event!");
                }
            }
            "terminal" => {
                if wrong_payload.get("path").is_some() {
                    println!("    TYPE CONFUSION: Filesystem data in terminal event!");
                }
            }
            "wm.hyprland" => {
                if wrong_payload.get("command").is_some() {
                    println!("    TYPE CONFUSION: Terminal data in window event!");
                }
            }
            _ => {}
        }
    }

    Ok(())
}
