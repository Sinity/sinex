//! Build script for sensd to compile protocol buffer definitions

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false) // We don't need client code in sensd
        .compile_protos(&["../../../proto/sensd.proto"], &["../../../proto/"])?;

    // Tell cargo to rerun if proto files change
    println!("cargo:rerun-if-changed=../../../proto/sensd.proto");

    Ok(())
}
