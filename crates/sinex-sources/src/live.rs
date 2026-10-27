//! Live sources run by an outside producer (`sinex-session`, a satellite's
//! `sinex-capture`): each source's capture writes its spool on its own
//! thread, and one loop produces what the spools hold to `sinexd` over the
//! NATS ingress. Both programs build their sources from the registry's formats
//! and share this loop.
use crate::sources;
use serde_json::Value;
use sinex_source_core::Source;
use sinex_source_core::binding::{Binding, InputFormat, Reader, Settings};
use sinex_source_core::capture::CaptureStop;
use sinex_source_core::drivers::producer::{Position, Producer, Store};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// One live source being captured and produced.
struct Live {
    source: Box<dyn Source>,
    binding: Binding,
    reader: Box<dyn Reader>,
    capture: CaptureTask,
}

struct CaptureTask {
    stop: Arc<CaptureStop>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl CaptureTask {
    fn stop(&mut self) {
        self.stop.stop();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for CaptureTask {
    fn drop(&mut self) {
        self.stop();
    }
}

/// The binding of the live format `format` on `spool`.
pub fn binding(
    format: &str,
    spool: &Path,
    host: &str,
    options: Value,
) -> Result<(&'static InputFormat, Binding), String> {
    let k = sources::format(format)
        .filter(|k| k.capture.is_some())
        .ok_or_else(|| format!("{format}: not a live source"))?;
    let settings = Settings {
        options,
        ..Settings::new(host)
    };
    let plan = k
        .plan
        .ok_or_else(|| format!("{format}: no spool binding"))?;
    let binding = plan(spool, &settings)?
        .into_iter()
        .next()
        .ok_or_else(|| format!("{format}: no binding"))?;
    Ok((k, binding))
}

/// Start capturing `format` into `spool` on its own thread, and return how it
/// is produced. A capture that ends is reported; production of what it
/// already spooled goes on.
fn start(k: &'static InputFormat, binding: Binding) -> Result<Live, String> {
    let capture = k.capture.expect("live formats capture");
    // Construct fallible readers before starting a capture that must be joined.
    let reader = (k
        .reader
        .ok_or_else(|| format!("{}: no spool reader", k.name))?)(&binding)?
    .into_polled()?;
    let source = sources::source_for(&binding)?;
    let stop = Arc::new(CaptureStop::new(None));
    let b = binding.clone();
    let thread_stop = stop.clone();
    let thread = std::thread::Builder::new()
        .spawn(move || {
            if let Err(e) = capture(&b, &thread_stop) {
                eprintln!("{} capture: {e}", b.name);
            }
        })
        .map_err(|e| e.to_string())?;
    Ok(Live {
        source,
        binding,
        reader,
        capture: CaptureTask {
            stop,
            thread: Some(thread),
        },
    })
}

/// Produce what every spool holds, every `every`, until `until` (forever
/// otherwise). Private mode pauses capture and production until it is off.
/// Recheck source selection without interrupting existing captures or their spools.
pub async fn run<F>(
    producer: &mut Producer,
    store: &Store,
    positions: &Path,
    mut select: F,
    every: Duration,
    until: Option<Instant>,
) -> Result<(), String>
where
    F: FnMut() -> Result<Vec<(&'static InputFormat, Binding)>, String>,
{
    // Register before any capture thread or child can start.
    let shutdown = sinex_source_core::capture::shutdown_signal().map_err(|e| e.to_string())?;
    let mut lives = Vec::new();
    extend(&mut lives, select()?);
    let deadline = async {
        match until {
            Some(until) => {
                tokio::time::sleep(until.saturating_duration_since(Instant::now())).await
            }
            None => std::future::pending::<()>().await,
        }
    };
    let result = tokio::select! {
        result = produce_spools(producer, store, positions, &mut lives, every, &mut select) => result,
        _ = shutdown => Ok(()),
        _ = deadline => Ok(()),
    };
    // Interrupt every source before joining any one blocked reader.
    for live in &lives {
        live.capture.stop.stop();
    }
    for live in &mut lives {
        live.capture.stop();
    }
    result
}

fn extend(lives: &mut Vec<Live>, bindings: Vec<(&'static InputFormat, Binding)>) {
    lives.retain(|live| {
        bindings
            .iter()
            .any(|(_, binding)| binding.name == live.binding.name)
    });
    for (format, binding) in bindings {
        if let Some(existing) = lives
            .iter_mut()
            .find(|live| live.binding.name == binding.name)
        {
            if existing
                .capture
                .thread
                .as_ref()
                .is_some_and(|thread| !thread.is_finished())
                && existing.binding.settings == binding.settings
            {
                continue;
            }
            existing.capture.stop();
            match start(format, binding) {
                Ok(live) => *existing = live,
                Err(error) => eprintln!("{}: {error}", format.name),
            }
        } else {
            match start(format, binding) {
                Ok(live) => lives.push(live),
                Err(error) => eprintln!("{}: {error}", format.name),
            }
        }
    }
}

async fn produce_spools<F>(
    producer: &mut Producer,
    store: &Store,
    positions: &Path,
    lives: &mut Vec<Live>,
    every: Duration,
    select: &mut F,
) -> Result<(), String>
where
    F: FnMut() -> Result<Vec<(&'static InputFormat, Binding)>, String>,
{
    let mut selected_at = Instant::now();
    let private = sinex_transport::private_mode::PrivateMode::default();
    loop {
        if selected_at.elapsed() >= Duration::from_secs(60) {
            match select() {
                Ok(bindings) => extend(lives, bindings),
                Err(error) => eprintln!("source discovery: {error}"),
            }
            selected_at = Instant::now();
        }
        if private.on() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        for live in lives.iter_mut() {
            // Drain everything captured so far, one material at a time.
            loop {
                let position = producer
                    .position(&live.binding.name, positions, store)
                    .await
                    .map_err(|e| e.to_string())?;
                let acquired = live
                    .reader
                    .poll(&position.cursor)
                    .map_err(|e| e.to_string())?;
                let had_material = acquired.material.is_some();
                let sent = producer
                    .produce(
                        live.source.as_ref(),
                        &live.binding.name,
                        acquired,
                        store,
                        positions,
                    )
                    .await
                    .map_err(|e| e.to_string())?;
                if had_material {
                    println!(
                        "{}: {} events in {} frames, {} admitted, {} unread",
                        live.binding.name, sent.events, sent.frames, sent.admitted, sent.unread
                    );
                }
                let committed =
                    Position::load(positions, &live.binding.name).map_err(|e| e.to_string())?;
                live.reader
                    .remove_passed(&committed.cursor)
                    .map_err(|e| e.to_string())?;
                if !had_material {
                    break;
                }
            }
        }
        tokio::time::sleep(every).await;
    }
}

/// `key=value` pairs as source options: numbers and booleans keep their types, a repeated key
/// collects a list, and directory lists (`roots`, `skip`) are lists even with
/// one entry.
pub fn options(pairs: &[String]) -> Result<Value, String> {
    let mut map = serde_json::Map::new();
    for pair in pairs {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| format!("option {pair}: expected key=value"))?;
        let value = match v {
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => v
                .parse::<u64>()
                .map(Value::from)
                .unwrap_or_else(|_| Value::from(v)),
        };
        match map.get_mut(k) {
            Some(Value::Array(list)) => list.push(value),
            Some(first) => *first = Value::Array(vec![first.take(), value]),
            None if matches!(k, "roots" | "skip") => {
                map.insert(k.to_owned(), Value::Array(vec![value]));
            }
            None => {
                map.insert(k.to_owned(), value);
            }
        }
    }
    Ok(if map.is_empty() {
        Value::Null
    } else {
        Value::Object(map)
    })
}

#[cfg(test)]
mod tests;
