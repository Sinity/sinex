//! Strict drift detector — companion to `schema-apply-bootstrap`.
//!
//! Reads `DATABASE_URL`, runs `sinex_schema::strict_diff::check_strict`, and
//! writes the result as JSON on stdout (pretty if running on a TTY, compact
//! otherwise). Exit code is `0` when no drift is detected, `1` when drift is
//! found, `2` on operator errors (missing env, connection failure).
//!
//! Operator workflow:
//!
//! ```bash
//! DATABASE_URL=postgres://... schema-strict-diff
//! ```
//!
//! Pipe to `jq '.[] | select(.category == "trigger_body")'` to filter; pipe
//! to `wc -l` only if the JSON is array-of-objects (which it is).
//!
//! See issue #556 for the categories this detects (and the ones still
//! marked as follow-up).

use sqlx::postgres::PgPoolOptions;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL").map_err(|error| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("DATABASE_URL environment variable is required: {error}"),
        )
    })?;

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await?;

    let drifts = sinex_schema::strict_diff::check_strict(&pool).await?;

    let pretty = atty_like_stdout();
    let serialized = if pretty {
        serde_json::to_string_pretty(&drifts)?
    } else {
        serde_json::to_string(&drifts)?
    };
    println!("{serialized}");

    if drifts.is_empty() {
        Ok(())
    } else {
        std::process::exit(1)
    }
}

/// Best-effort TTY check. We avoid pulling in the `atty` crate because the
/// surrounding workspace already settled on `std::io::IsTerminal` for the
/// same purpose.
fn atty_like_stdout() -> bool {
    use std::io::IsTerminal;
    std::io::stdout().is_terminal()
}
