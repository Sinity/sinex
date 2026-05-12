//! Stub binary — the fs ingestor lives in `sinex-source-worker`. This file
//! is kept until the orchestrator's synchronized crate-deletion commit
//! removes the crate entirely.

fn main() {
    eprintln!(
        "sinex-fs-ingestor has been folded into sinex-source-worker. Use \
         `sinex-source-worker --source-unit fs ...` instead."
    );
    std::process::exit(2);
}
