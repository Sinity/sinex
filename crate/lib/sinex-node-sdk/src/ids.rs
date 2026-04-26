//! Event ID helpers for node runtimes.

use sha2::{Digest, Sha256};
use sinex_primitives::{Timestamp, Uuid};

const MATERIAL_EVENT_ID_SOURCE: &[u8] = b"sinex:material-event:v1";

/// Build a deterministic RFC4122 `UUIDv7` for a source occurrence.
///
/// `source` should identify the stable source namespace, while `anchor` should
/// identify the occurrence inside that source. The timestamp supplies the `UUIDv7`
/// ordering prefix; the hashed `(source, anchor)` pair supplies deterministic
/// entropy so re-reading the same occurrence yields the same event ID.
#[must_use]
pub fn deterministic_event_id(
    source: impl AsRef<[u8]>,
    anchor: impl AsRef<[u8]>,
    timestamp: Timestamp,
) -> Uuid {
    deterministic_event_id_from_entropy(timestamp_to_millis(timestamp), entropy(source, anchor))
}

/// Build a deterministic RFC4122 `UUIDv7` for a material-provenance event.
///
/// The stable occurrence key is the event namespace plus material byte range.
/// This keeps common ingestor paths idempotent while still allowing specialized
/// sources, such as systemd journal cursors, to call [`deterministic_event_id`]
/// directly with a domain-specific anchor.
#[must_use]
pub fn deterministic_material_event_id(
    event_source: impl AsRef<str>,
    event_type: impl AsRef<str>,
    source_material_id: Uuid,
    anchor_byte: i64,
    offset_start: Option<i64>,
    offset_end: Option<i64>,
    timestamp: Timestamp,
) -> Uuid {
    let mut anchor = Vec::new();
    append_anchor_field(
        &mut anchor,
        b"event_source",
        event_source.as_ref().as_bytes(),
    );
    append_anchor_field(&mut anchor, b"event_type", event_type.as_ref().as_bytes());
    append_anchor_field(
        &mut anchor,
        b"source_material_id",
        source_material_id.as_bytes(),
    );
    append_anchor_field(&mut anchor, b"anchor_byte", &anchor_byte.to_be_bytes());
    append_optional_i64(&mut anchor, b"offset_start", offset_start);
    append_optional_i64(&mut anchor, b"offset_end", offset_end);
    deterministic_event_id(MATERIAL_EVENT_ID_SOURCE, anchor, timestamp)
}

fn entropy(source: impl AsRef<[u8]>, anchor: impl AsRef<[u8]>) -> u128 {
    let source = source.as_ref();
    let anchor = anchor.as_ref();
    let mut hasher = Sha256::new();
    hasher.update(b"sinex:event-id:v1");
    hasher.update(
        u64::try_from(source.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(source);
    hasher.update(
        u64::try_from(anchor.len())
            .unwrap_or(u64::MAX)
            .to_be_bytes(),
    );
    hasher.update(anchor);
    let hash = hasher.finalize();

    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&hash[0..16]);
    u128::from_be_bytes(bytes)
}

fn append_optional_i64(anchor: &mut Vec<u8>, name: &[u8], value: Option<i64>) {
    match value {
        Some(value) => append_anchor_field(anchor, name, &value.to_be_bytes()),
        None => append_anchor_field(anchor, name, b""),
    }
}

fn append_anchor_field(anchor: &mut Vec<u8>, name: &[u8], value: &[u8]) {
    anchor.extend_from_slice(&u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
    anchor.extend_from_slice(name);
    anchor.extend_from_slice(&u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
    anchor.extend_from_slice(value);
}

fn deterministic_event_id_from_entropy(timestamp_ms: u64, entropy: u128) -> Uuid {
    let mut bytes = [0u8; 16];
    let ts = (timestamp_ms & 0x0000_FFFF_FFFF_FFFF).to_be_bytes();
    bytes[..6].copy_from_slice(&ts[2..]);

    bytes[6] = 0x70 | (((entropy >> 72) as u8) & 0x0f);
    bytes[7] = (entropy >> 64) as u8;
    bytes[8] = 0x80 | (((entropy >> 56) as u8) & 0x3f);
    bytes[9] = (entropy >> 48) as u8;
    bytes[10] = (entropy >> 40) as u8;
    bytes[11] = (entropy >> 32) as u8;
    bytes[12] = (entropy >> 24) as u8;
    bytes[13] = (entropy >> 16) as u8;
    bytes[14] = (entropy >> 8) as u8;
    bytes[15] = entropy as u8;

    Uuid::from_bytes(bytes)
}

fn timestamp_to_millis(timestamp: Timestamp) -> u64 {
    let nanos = timestamp.inner().unix_timestamp_nanos();
    if nanos <= 0 {
        return 0;
    }
    u64::try_from(nanos / 1_000_000).unwrap_or(u64::MAX)
}
