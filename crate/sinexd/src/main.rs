//! `sinexd` entrypoint.
//!
//! Construct the supervisor, install modules (event_engine, api, sources,
//! automata), and run the lifecycle. Implementation lands in PR-2/PR-3 as the
//! collapse merges the former binaries' bodies into this crate.

fn main() -> color_eyre::eyre::Result<()> {
    color_eyre::install()?;
    eprintln!("sinexd: skeleton — supervisor and module implementations land in PR-2/PR-3 (#1054)");
    Ok(())
}
