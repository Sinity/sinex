//! The writer owns optional producer reading orders, in the admission transaction.
use super::*;
use sinex_model::ReadingOrder;

pub(super) async fn assign(
    conn: &mut PgConnection,
    batch: &Batch,
    events: &mut [Event],
) -> Result<()> {
    let BatchKind::Ingress(frame) = &batch.kind else {
        return Ok(());
    };
    let assigned: std::collections::HashSet<_> = frame.assign_orders.iter().copied().collect();
    for event in events {
        let record_key = match (&event.equivalence_key, &event.origin) {
            (Some(key), _) => format!("key:{}", key.0),
            (
                None,
                Origin::Record {
                    location,
                    output_index,
                },
            ) => format!(
                "record:{}:{}:{}",
                location.material, location.anchor, output_index
            ),
            _ => {
                return Err(crate::Error::Invalid(
                    "producer reading order needs a stable record key".into(),
                ));
            }
        };
        // Only a digest survives excision; source record keys do not enter another ledger.
        let hash = blake3::hash(record_key.as_bytes());
        let previous=sqlx::query_scalar!("SELECT reading_order FROM core.producer_orders WHERE source=$1 AND event_type=$2 AND key_hash=$3 FOR UPDATE",event.source,event.event_type,hash.as_bytes().as_slice()).fetch_optional(&mut *conn).await?;
        let previous = match previous {
            Some(previous) => Some(previous),
            None => {
                let (material, anchor, output) = match &event.origin {
                    Origin::Record {
                        location,
                        output_index,
                    } => (
                        Some(location.material.0),
                        Some(i64::try_from(location.anchor).map_err(|_| {
                            crate::Error::Invalid("record anchor exceeds stored range".into())
                        })?),
                        Some(i64::try_from(*output_index).map_err(|_| {
                            crate::Error::Invalid("record output exceeds stored range".into())
                        })?),
                    ),
                    _ => (None, None, None),
                };
                sqlx::query_scalar!("SELECT max(reading_order) FROM core.events WHERE source=$1 AND event_type=$2 AND (equivalence_key=$3 OR ($3::text IS NULL AND material_id=$4 AND anchor=$5 AND output_index=$6))",event.source,event.event_type,event.equivalence_key.as_ref().map(|key|&key.0),material,anchor,output).fetch_one(&mut *conn).await?
            }
        };
        if assigned.contains(&event.id) {
            event.reading_order =
                ReadingOrder(previous.unwrap_or(-1).checked_add(1).ok_or_else(|| {
                    crate::Error::Invalid(
                        "producer reading order exhausted its stored integer range".into(),
                    )
                })?);
        }
        let last = previous.map_or(event.reading_order.0, |previous| {
            previous.max(event.reading_order.0)
        });
        sqlx::query!("INSERT INTO core.producer_orders(source,event_type,key_hash,reading_order) VALUES($1,$2,$3,$4) ON CONFLICT(source,event_type,key_hash) DO UPDATE SET reading_order=greatest(core.producer_orders.reading_order,EXCLUDED.reading_order)",event.source,event.event_type,hash.as_bytes().as_slice(),last).execute(&mut *conn).await?;
    }
    Ok(())
}
