//! Live sources run by an outside producer (`sinex-session`, a satellite's
//! `sinex-capture`): each source's capture writes its spool on its own
//! thread, and one loop produces what the spools hold to `sinexd` over the
//! NATS ingress. Both programs build their sources from the registry's kinds
//! and share this loop.
use crate::binding::{Binding, Kind, Reader, Settings};
use crate::drivers::producer::{Position, Producer, Store};
use crate::{Source, sources};
use serde_json::Value;
use std::path::Path;
use std::time::{Duration, Instant};

/// One live source being captured and produced.
pub struct Live {
    pub source: Box<dyn Source>,
    pub binding: Binding,
    pub reader: Box<dyn Reader>,
}

/// The binding of the live source `kind` on `spool`.
pub fn binding(
    kind: &str,
    spool: &Path,
    host: &str,
    options: Value,
) -> Result<(&'static Kind, Binding), String> {
    let k = sources::kind(kind)
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
pub fn start(kind: &str, spool: &Path, host: &str, options: Value) -> Result<Live, String> {
    let (k, binding) = binding(kind, spool, host, options)?;
    let capture = k.capture.expect("live kinds capture");
    let b = binding.clone();
    std::thread::spawn(move || {
        if let Err(e) = capture(&b, None) {
            eprintln!("{} capture: {e}", b.name);
        }
    });
    let reader = (k.reader.ok_or_else(|| format!("{kind}: no spool reader"))?)(&binding)?;
    Ok(Live {
        source: (k.source)(&binding)?,
        binding,
        reader,
    })
}

/// Produce what every spool holds, every `every`, until `until` (forever
/// otherwise). Private mode pauses capture and production until it is off.
pub async fn produce(
    producer: &mut Producer,
    store: &Store,
    positions: &Path,
    lives: &mut [Live],
    every: Duration,
    until: Option<Instant>,
) -> Result<(), String> {
    let private = sinex_protocol::private_mode::PrivateMode::default();
    loop {
        if private.on() {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        for live in lives.iter_mut() {
            // Drain everything captured so far, one material at a time.
            loop {
                let position = producer
                    .position(&live.binding.name, positions)
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
        if until.is_some_and(|d| Instant::now() >= d) {
            return Ok(());
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
