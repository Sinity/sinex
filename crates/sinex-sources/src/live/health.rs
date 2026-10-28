//! Local capture health is refreshed independently of bus requests.
use super::*;
use sinex_source_core::drivers::spool_health::{self, Backlog, Counter, Counts};
use std::io::{BufRead, Seek};

/// Count captured records (lines or completed media segments) after the
/// acknowledged spool cursor. Partial lines contribute bytes, not records.
fn backlog(
    binding: &Binding,
    cursor: &Value,
    recovering: Option<&Value>,
) -> Result<Backlog, String> {
    let mut result_counts = Backlog::default();
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
                    if recovering
                        .and_then(|cursor| cursor.get("last"))
                        .and_then(Value::as_str)
                        .is_some_and(|last| stem.as_str() <= last)
                    {
                        result_counts.recovery.bytes += length + sidecar;
                        result_counts.recovery.records += 1;
                    }
                    result_counts.media.insert(stem, length + sidecar);
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
                let recover_through = recovering
                    .map(|cursor| {
                        let segment = cursor.get("segment").and_then(Value::as_u64).unwrap_or(0);
                        if index < segment {
                            length
                        } else if index == segment {
                            cursor
                                .pointer("/inner/offset")
                                .and_then(Value::as_u64)
                                .unwrap_or(0)
                                .saturating_sub(start)
                                .min(length)
                        } else {
                            0
                        }
                    })
                    .unwrap_or(0);
                result_counts.recovery.bytes += recover_through;
                let mut counts = Counts {
                    bytes: length,
                    records: 0,
                };
                let mut read = 0;
                bytes += length;
                file.seek(std::io::SeekFrom::Start(start))?;
                let mut reader = std::io::BufReader::new(std::io::Read::take(file, length));
                loop {
                    let buffer = reader.fill_buf()?;
                    if buffer.is_empty() {
                        break;
                    }
                    counts.records += buffer.iter().filter(|&&byte| byte == b'\n').count() as u64;
                    let prefix = recover_through
                        .saturating_sub(read)
                        .min(buffer.len() as u64) as usize;
                    result_counts.recovery.records += buffer[..prefix]
                        .iter()
                        .filter(|&&byte| byte == b'\n')
                        .count() as u64;
                    read += buffer.len() as u64;
                    let length = buffer.len();
                    reader.consume(length);
                }
                records += counts.records;
                result_counts.segments.insert(index, counts);
            }
        }
        Ok(())
    })();
    match result {
        Ok(()) => {
            result_counts.counts = Counts { bytes, records };
            Ok(result_counts)
        }
        Err(e) => Err(format!("{}: {e}", binding.input.display())),
    }
}

pub(super) fn initialize(positions: &Path, binding: &Binding) -> Result<Counter, String> {
    let position = Position::load(positions, &binding.name).map_err(|e| e.to_string())?;
    let recovering =
        Producer::unacknowledged_position(&binding.name, positions).map_err(|e| e.to_string())?;
    let mut initial = backlog(
        binding,
        &position.cursor,
        recovering.as_ref().map(|position| &position.cursor),
    )?;
    initial.cursor = position.cursor;
    initial.capture_failures = position.capture_failures.len();
    initial.last_capture_failure = position
        .capture_failures
        .last()
        .map(|failure| failure.reason.clone());
    initial.frames =
        Producer::unacknowledged_frames(&binding.name, positions).map_err(|e| e.to_string())?;
    Ok(spool_health::register(&binding.input, initial))
}

fn snapshot(positions: &Path, bindings: &[(Binding, Counter)]) -> Result<(), String> {
    let spools: Vec<_> = bindings.iter().map(|(binding, counter)| {
        let counter = counter.lock().unwrap();
        serde_json::json!({
            "binding": binding.name,
            "spool": binding.input.to_string_lossy(),
            "error": counter.error,
            "state": counter.error.as_ref().map(|reason| format!("error: {reason}")).unwrap_or_else(|| "capturing".into()),
            "capture_failures": counter.capture_failures,
            "last_capture_failure": counter.last_capture_failure,
            "unpublished": {"bytes": counter.counts.bytes, "records": counter.counts.records, "frames": counter.frames},
        })
    }).collect();
    let report = serde_json::json!({
        "observed_at": sinex_model::Timestamp::now(),
        "spools": spools,
    });
    let root = positions
        .parent()
        .ok_or("positions need a state directory")?;
    let local = |error| format!("capture health in {}: {error}", root.display());
    std::fs::create_dir_all(root).map_err(local)?;
    let temporary = root.join("health.tmp");
    std::fs::write(
        &temporary,
        serde_json::to_vec(&report).expect("health serializes"),
    )
    .map_err(local)?;
    std::fs::rename(temporary, root.join("health.json")).map_err(local)
}

pub(super) async fn report(
    positions: &Path,
    bindings: tokio::sync::watch::Receiver<Vec<(Binding, Counter)>>,
) -> Result<(), String> {
    loop {
        let positions = positions.to_owned();
        let bindings = bindings.borrow().clone();
        tokio::task::spawn_blocking(move || snapshot(&positions, &bindings))
            .await
            .map_err(|e| e.to_string())??;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests;
