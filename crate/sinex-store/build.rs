// SQLx's query macros read DATABASE_URL at compile time, but cargo does not
// know that. xtask names the database after a hash of the schema, so a
// schema change changes the URL and this makes the checked queries recompile.
fn main() {
    println!("cargo:rerun-if-env-changed=DATABASE_URL");
    println!("cargo:rerun-if-changed=build.rs");
}
