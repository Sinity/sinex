use std::process::Command;

fn main() {
    // Avoid adding rpath if explicitly disabled.
    if std::env::var("SINEX_SKIP_DBUS_RPATH").is_ok() {
        return;
    }

    // Try to discover the dbus libdir via pkg-config.
    if let Ok(output) = Command::new("pkg-config")
        .args(["--variable=libdir", "dbus-1"])
        .output()
        && output.status.success()
        && let Ok(mut libdir) = String::from_utf8(output.stdout)
    {
        libdir.truncate(libdir.trim_end_matches(['\n', '\r']).len());
        if !libdir.is_empty() {
            // Add rpath so test binaries can locate libdbus at runtime (e.g., in Nix builds).
            println!("cargo:rustc-link-arg=-Wl,-rpath,{libdir}");
        }
    }
}
