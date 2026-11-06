// SQLx reads DATABASE_URL without stable compiler environment tracking.
// xtask validates a content-addressed schema before supplying its identity;
// its transport address may change when a disposable server is restarted.
fn main() {
    println!("cargo:rerun-if-env-changed=SINEX_SCHEMA_IDENTITY");
    if std::env::var_os("SINEX_SCHEMA_IDENTITY").is_none() {
        println!("cargo:rerun-if-env-changed=DATABASE_URL");
    }
    println!("cargo:rerun-if-changed=build.rs");
}
