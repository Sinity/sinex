//! Google Play Store (`Google Play Store/*.json`, arrays of one-key wrapper
//! objects) and Purchases (`Purchases _ Reservations/order_*.json`, one order
//! per file), on `ExportArchive`. Each item's time is read from the wrapped
//! item's own field (Lynchpin unwrapped only `orderHistory`, so most rows had
//! no time); its identity is the provider's ID where there is one.
use crate::drivers::export_archive::{ExportArchive, Framing, PART_BYTES};
use crate::registry::{DeclaredSource, Driver, SourceSpec};
use crate::sources::fields;
use crate::{Parser, RecordError};
use serde_json::{Map, Value};
use sinex_model::event_types::account::PurchaseObserved;
use sinex_model::{Class, Timestamp};
use std::path::PathBuf;

pub const PLAY_STORE: SourceSpec = SourceSpec {
    name: "google.play-store",
    driver: Driver::ExportArchive,
    consumer: "apps installed, bought and reviewed",
    class: Class::Open,
    parser: "google-play-store",
    parser_version: 1,
    event_types: &["purchase.observed"],
    framer: crate::Framer::export(
        frame_play_store,
        crate::drivers::export_archive::Framing::JsonArray(""),
    ),
};

pub const PURCHASES: SourceSpec = SourceSpec {
    name: "google.purchases",
    driver: Driver::ExportArchive,
    consumer: "orders and reservations",
    class: Class::Open,
    parser: "google-purchases",
    parser_version: 1,
    event_types: &["purchase.observed"],
    framer: crate::Framer::export(
        frame_purchases,
        crate::drivers::export_archive::Framing::Whole,
    ),
};

/// The wrapper keys and the field holding each wrapped item's time.
const WRAPPERS: &[(&str, &str)] = &[
    ("orderHistory", "creationTime"),
    ("purchaseHistory", "purchaseTime"),
    ("install", "firstInstallationTime"),
    ("libraryDoc", "acquisitionTime"),
    ("review", "creationTime"),
    ("subscription", "creationTime"),
    ("userSetting", ""),
    ("device", ""),
];

pub struct PurchasesParser;

impl Parser for PurchasesParser {
    type Record<'a> = Map<String, Value>;
    type Carry = ();
    type Payload = PurchaseObserved;

    fn parse(
        &self,
        row: &Map<String, Value>,
        _: &(),
        _context: &crate::RecordContext<'_>,
    ) -> Result<(Vec<PurchaseObserved>, ()), RecordError> {
        // An order file: the object itself is the item.
        if row.contains_key("merchantOrderId") || row.contains_key("lineItem") {
            let at = row
                .get("creationTime")
                .and_then(|t| t.get("usecSinceEpochUtc"))
                .and_then(|u| {
                    u.as_str()
                        .and_then(|s| s.parse().ok())
                        .or_else(|| u.as_i64())
                })
                .map(fields::from_unix_us)
                .transpose()?;
            let merchant = row
                .get("transactionMerchant")
                .and_then(|m| m.get("name"))
                .and_then(Value::as_str);
            let id = fields::optional_text(row, "merchantOrderId")
                .map(|id| format!("{}:{id}", merchant.unwrap_or("")));
            return Ok((
                vec![observed("order", id, at, merchant.map(str::to_owned), row)],
                (),
            ));
        }
        let (kind, item) = match row.iter().next() {
            Some((k, v)) if row.len() == 1 => (k.as_str(), v),
            _ => return Err(RecordError::new("not a one-key wrapper object or an order")),
        };
        let item = item
            .as_object()
            .ok_or_else(|| RecordError::new(format!("{kind} is not an object")))?;
        let time_field = WRAPPERS.iter().find(|(k, _)| *k == kind).map(|(_, f)| *f);
        let at = match time_field {
            Some("") | None => None,
            Some(f) => fields::optional_text(item, f)
                .map(|t| fields::rfc3339(&t))
                .transpose()?,
        };
        let doc = item.get("doc").or_else(|| item.get("document"));
        let title = doc
            .and_then(|d| d.get("title"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let doc_type = doc
            .and_then(|d| d.get("documentType"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let id = match kind {
            "orderHistory" => fields::optional_text(item, "orderId"),
            "install" => title.as_ref().map(|t| {
                let device = item
                    .get("deviceAttribute")
                    .and_then(|d| d.get("deviceDisplayName"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                format!("{doc_type}:{t}@{device}")
            }),
            "libraryDoc" => title.as_ref().map(|t| format!("{doc_type}:{t}")),
            "purchaseHistory" | "review" => title
                .as_ref()
                .zip(at)
                .map(|(t, at)| format!("{doc_type}:{t}@{}", at.unix_ns())),
            _ => None,
        };
        Ok((vec![observed(kind, id, at, title, item)], ()))
    }
}

/// The item, identified by the provider's ID or, without one, by its content.
fn observed(
    kind: &str,
    id: Option<String>,
    at: Option<Timestamp>,
    title: Option<String>,
    item: &Map<String, Value>,
) -> PurchaseObserved {
    let item = Value::Object(item.clone());
    let native_id = id.unwrap_or_else(|| {
        format!(
            "blake3:{}",
            blake3::hash(item.to_string().as_bytes()).to_hex()
        )
    });
    PurchaseObserved {
        service: "google".into(),
        record_kind: kind.into(),
        native_id,
        at,
        title,
        item,
    }
}

pub fn play_store() -> DeclaredSource<PurchasesParser> {
    DeclaredSource {
        spec: PLAY_STORE,
        parser: PurchasesParser,
    }
}

pub fn purchases() -> DeclaredSource<PurchasesParser> {
    DeclaredSource {
        spec: PURCHASES,
        parser: PurchasesParser,
    }
}

pub fn frame_play_store(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::JsonArray("").frame(bytes, true)
}

pub fn frame_purchases(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
    Framing::Whole.frame(bytes, true)
}

pub fn select_play_store(member: &str) -> bool {
    member.contains("/Google Play Store/") && member.ends_with(".json")
}

pub fn select_purchases(member: &str) -> bool {
    member.contains("/Purchases _ Reservations/") && member.ends_with(".json")
}

pub fn play_store_driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: select_play_store,
        framing: Framing::JsonArray(""),
        part_bytes: PART_BYTES,
        join: None,
    }
}

pub fn purchases_driver(path: PathBuf, host: String) -> ExportArchive {
    ExportArchive {
        path,
        host,
        select: select_purchases,
        framing: Framing::Whole,
        part_bytes: PART_BYTES,
        join: None,
    }
}

#[cfg(test)]
mod tests;
