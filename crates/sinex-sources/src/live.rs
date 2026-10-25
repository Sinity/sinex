//! Live sources run by an outside producer (`sinex-session`, a satellite's
//! `sinex-capture`): each source's capture writes its spool on its own
//! thread, and one loop produces what the spools hold to `sinexd` over the
//! NATS ingress. Both programs build their sources from the registry's kinds
//! and share this loop.
use crate::binding::{Binding, InputKind, Reader, Settings};
use crate::capture::Control;
use crate::drivers::producer::{Position, Producer, Store};
use crate::{Source, sources};
use serde_json::Value;
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
    control: Arc<Control>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl CaptureTask {
    fn stop(&mut self) {
        self.control.stop();
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

/// The binding of the live source `kind` on `spool`.
pub fn binding(
    kind: &str,
    spool: &Path,
    host: &str,
    options: Value,
) -> Result<(&'static InputKind, Binding), String> {
    let k = sources::input_kind(kind)
        .filter(|k| k.capture.is_some())
        .ok_or_else(|| format!("{kind}: not a live source"))?;
    let settings = Settings {
        options,
        ..Settings::new(host)
    };
    let plan = k.plan.ok_or_else(|| format!("{kind}: no spool binding"))?;
    let binding = plan(spool, &settings)?
        .into_iter()
        .next()
        .ok_or_else(|| format!("{kind}: no binding"))?;
    Ok((k, binding))
}

/// Start capturing `kind` into `spool` on its own thread, and return how it
/// is produced. A capture that ends is reported; production of what it
/// already spooled goes on.
fn start(k: &'static InputKind, binding: Binding) -> Result<Live, String> {
    let capture = k.capture.expect("live kinds capture");
    // Construct fallible readers before starting a capture that must be joined.
    let reader = (k
        .reader
        .ok_or_else(|| format!("{}: no spool reader", k.name))?)(&binding)?
    .into_polled()?;
    let source = (k.source)(&binding)?;
    let control = Arc::new(Control::new(None));
    let b = binding.clone();
    let stop = control.clone();
    let thread = std::thread::Builder::new()
        .spawn(move || {
            if let Err(e) = capture(&b, &stop) {
                eprintln!("{} capture: {e}", b.name);
            }
        })
        .map_err(|e| e.to_string())?;
    Ok(Live {
        source,
        binding,
        reader,
        capture: CaptureTask {
            control,
            thread: Some(thread),
        },
    })
}

/// Produce what every spool holds, every `every`, until `until` (forever
/// otherwise). Private mode pauses capture and production until it is off.
pub async fn run(
    producer: &mut Producer,
    store: &Store,
    positions: &Path,
    bindings: Vec<(&'static InputKind, Binding)>,
    every: Duration,
    until: Option<Instant>,
) -> Result<(), String> {
    // Register before any capture thread or child can start.
    let shutdown = crate::capture::shutdown_signal().map_err(|e| e.to_string())?;
    let mut lives = bindings
        .into_iter()
        .map(|(kind, binding)| start(kind, binding))
        .collect::<Result<Vec<_>, _>>()?;
    let deadline = async {
        match until {
            Some(until) => {
                tokio::time::sleep(until.saturating_duration_since(Instant::now())).await
            }
            None => std::future::pending::<()>().await,
        }
    };
    let result = tokio::select! {
        result = produce_spools(producer, store, positions, &mut lives, every) => result,
        _ = shutdown => Ok(()),
        _ = deadline => Ok(()),
    };
    // Interrupt every source before joining any one blocked reader.
    for live in &lives {
        live.capture.control.stop();
    }
    for live in &mut lives {
        live.capture.stop();
    }
    result
}

async fn produce_spools(
    producer: &mut Producer,
    store: &Store,
    positions: &Path,
    lives: &mut [Live],
    every: Duration,
) -> Result<(), String> {
    let private = sinex_transport::private_mode::PrivateMode::default();
    loop {
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

/// `key=value` pairs as source options: numbers stay numbers, a repeated key
/// collects a list, and directory lists (`roots`, `skip`) are lists even with
/// one entry.
pub fn options(pairs: &[String]) -> Result<Value, String> {
    let mut map = serde_json::Map::new();
    for pair in pairs {
        let (k, v) = pair
            .split_once('=')
            .ok_or_else(|| format!("option {pair}: expected key=value"))?;
        let value = v
            .parse::<u64>()
            .map(Value::from)
            .unwrap_or_else(|_| Value::from(v));
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
