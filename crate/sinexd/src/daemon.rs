//! The running daemon (architecture §7.4): tasks under one supervisor in one
//! tokio runtime. The writer is built; the other tasks are filled by their
//! Stage 2 lanes and each names its section.
use std::path::PathBuf;

pub async fn run(_database: String, _store: PathBuf) -> Result<(), String> {
    // writer: crate::writer::run, the only database writer (§4.3).
    // sources: one task per enabled binding running its driver and drain on a
    //   schedule (§5); `crate::backfill` shows the same path run to the end.
    todo!(
        "architecture §7.4: supervisor; source tasks; commit-log reader (§4.3); publisher to the confirmed stream (§4.3); NATS ingress consumer (§4.3); automaton driver (§6.2); API server (§7.2); signals (§7.2)"
    )
}
