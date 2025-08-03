fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the same proto files as the SDK
    tonic_build::compile_protos("../../lib/sinex-satellite-sdk/proto/ingest.proto")?;
    Ok(())
}
