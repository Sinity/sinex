//! Local capture health is refreshed independently of bus requests.
use super::*;
use std::io::{BufRead, Seek};

/// Count captured records (lines or completed media segments) after the
/// acknowledged spool cursor. Partial lines contribute bytes, not records.
fn backlog(binding: &Binding, cursor: &Value) -> Result<Value, String> {
    let media = sources::source_for(binding)?.spec().driver == sinex_source_core::Driver::Media;
    let mut bytes = 0u64;
    let mut records = 0u64;
    let result = (|| -> std::io::Result<()> {
        if media {
            let last = cursor.get("last").and_then(Value::as_str);
            for (stem, path) in sinex_source_core::drivers::media::complete(&binding.input)? {
                if last.is_none_or(|last| stem.as_str() > last) {
                    let length = match std::fs::metadata(path) {
                        Ok(metadata) => metadata.len(),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                        Err(error) => return Err(error),
                    };
                    let sidecar =
                        std::fs::metadata(binding.input.join(format!("{stem}.json")))?.len();
                    bytes += length + sidecar;
                    records += 1;
                }
            }
        } else {
            let segment = cursor.get("segment").and_then(Value::as_u64).unwrap_or(0);
            let offset = cursor
                .pointer("/inner/offset")
                .and_then(Value::as_u64)
                .unwrap_or(0);
            for index in sinex_source_core::drivers::live_stream::segments(&binding.input)? {
                if index < segment {
                    continue;
                }
                let path = binding.input.join(format!("{index:012}.ndjson"));
                let mut file = match std::fs::File::open(path) {
                    Ok(file) => file,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(e) => return Err(e),
                };
                let start = if index == segment { offset } else { 0 };
                // Capture can append during the read. Report a finite snapshot.
                let length = file.metadata()?.len().saturating_sub(start);
                bytes += length;
                file.seek(std::io::SeekFrom::Start(start))?;
                let mut reader = std::io::BufReader::new(std::io::Read::take(file, length));
                loop {
                    let buffer = reader.fill_buf()?;
                    if buffer.is_empty() {
                        break;
                    }
                    records += buffer.iter().filter(|&&byte| byte == b'\n').count() as u64;
                    let length = buffer.len();
                    reader.consume(length);
                }
            }
        }
        Ok(())
    })();
    match result {
        Ok(()) => Ok(serde_json::json!({"bytes": bytes, "records": records})),
        Err(e) => Err(format!("{}: {e}", binding.input.display())),
    }
}

fn snapshot(positions: &Path, bindings: &[Binding]) -> Result<(), String> {
    let spools = bindings.iter().map(|binding| {
        // A corrupt position cannot be repaired by publishing or reconnecting.
        let position = Position::load(positions, &binding.name).map_err(|e| e.to_string())?;
        let frames = Producer::unacknowledged_frames(&binding.name, positions).map_err(|e| e.to_string())?;
        let status = backlog(binding, &position.cursor).map(|mut status| {
            status["frames"] = serde_json::json!(frames);
            status
        });
        Ok(serde_json::json!({
            "binding": binding.name,
            "spool": binding.input,
            "unpublished": status.unwrap_or_else(|error| serde_json::json!({"unavailable": error})),
        }))
    }).collect::<Result<Vec<_>, String>>()?;
    let report = serde_json::json!({
        "observed_at": sinex_model::Timestamp::now(),
        "spools": spools,
    });
    let local = |error| format!("capture health in {}: {error}", positions.display());
    std::fs::create_dir_all(positions).map_err(local)?;
    let temporary = positions.join("health.tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec(&report).expect("health serializes"),
    )
    .map_err(local)?;
    std::fs::rename(temporary, positions.join("health.json")).map_err(local)
}

pub(super) async fn report(positions: &Path, bindings: Vec<Binding>) -> Result<(), String> {
    loop {
        snapshot(positions, &bindings)?;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests;
