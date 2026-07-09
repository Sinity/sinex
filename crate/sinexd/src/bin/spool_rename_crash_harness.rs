//! sinex-r6d.9 VM crash-window harness binary (testing-feature only).
//!
//! Exercises the real recovery-spool write path
//! (`EventBatcher::store_recovery_spool_events_at_path`) against a caller-
//! supplied path, then — when `SINEX_TEST_SPOOL_RENAME_MARKER` is set —
//! blocks forever immediately after the rename and before the parent-
//! directory fsync. A NixOS VM test scenario runs this inside a VM with the
//! spool path on a persistent (non-tmpfs) disk, waits for the marker file to
//! appear, and then crashes the whole VM (`machine.crash()`, a real QEMU
//! power-cut) at exactly that point — proving whether the rename survives
//! without the fsync that never got to run.
//!
//! Not reachable from any production build: `required-features = ["testing"]`.

use sinex_primitives::events::builder::{EventBuilder, HasProvenance};
use sinex_primitives::prelude::{EventSource, EventType, Id, SourceMaterial, Uuid};
use sinexd::runtime::event_transport::EventBatcher;
use std::path::PathBuf;

fn synthetic_event() -> serde_json::Value {
    serde_json::json!({ "harness": "sinex-r6d.9-spool-crash" })
}

fn material_builder() -> EventBuilder<serde_json::Value, HasProvenance> {
    EventBuilder::new_internal(
        EventSource::from_static("test.spool-crash-harness"),
        EventType::new("test.spool_crash_harness").expect("valid event type"),
        synthetic_event(),
    )
    .from_material(Id::<SourceMaterial>::from_uuid(Uuid::now_v7()), 0)
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let mut args = std::env::args_os().skip(1);
    let Some(spool_path) = args.next() else {
        eprintln!("usage: spool_rename_crash_harness <spool-file-path>");
        return std::process::ExitCode::FAILURE;
    };
    let spool_path = PathBuf::from(spool_path);

    let event = match material_builder().build() {
        Ok(event) => event,
        Err(error) => {
            eprintln!("failed to build synthetic event: {error}");
            return std::process::ExitCode::FAILURE;
        }
    };

    match EventBatcher::test_only_write_recovery_spool(&[event], &spool_path).await {
        Ok(()) => {
            println!("spool write completed (fsync ran; no crash was injected)");
            std::process::ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("spool write failed: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
