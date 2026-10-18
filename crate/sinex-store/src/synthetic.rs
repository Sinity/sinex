//! Synthetic test events: one event per call, stored with its source bytes.
//! Not a registered source and not reachable over RPC.
#[cfg(feature = "product")]
use crate::{Error, writer::coordinate_hash};
use crate::{Result, WriterPool};
use serde::Serialize;
#[cfg(feature = "product")]
use sqlx::Connection;
use uuid::Uuid;

/// The event's original time is always unknown; ingestion time never stands in for it.
pub struct MaterialEvent {
    pub source: String,
    pub event_type: String,
    pub bytes: Vec<u8>,
    pub anchor_byte: i64,
    pub ordinal: i16,
    pub payload: serde_json::Value,
}

#[derive(Debug, Serialize)]
pub struct Receipt {
    pub event_id: Uuid,
    pub material_id: Uuid,
    pub commit_seq: i64,
    pub blob_name: String,
}

#[cfg(feature = "product")]
const MAX_BYTES: usize = 1024 * 1024;

impl WriterPool {
    #[cfg(not(feature = "product"))]
    pub(crate) async fn admit(&mut self, _event: MaterialEvent) -> Result<Receipt> {
        Err(crate::Error::ProductUnavailable)
    }

    #[cfg(feature = "product")]
    pub(crate) async fn admit(&mut self, event: MaterialEvent) -> Result<Receipt> {
        if event.bytes.len() > MAX_BYTES {
            return Err(Error::Invalid("synthetic proof material exceeds 1 MiB"));
        }
        if event.bytes.is_empty()
            || event.anchor_byte < 0
            || event.anchor_byte as usize >= event.bytes.len()
            || event.ordinal < 0
        {
            return Err(Error::Invalid(
                "anchor must address stored bytes and ordinal must be nonnegative",
            ));
        }
        if !dotted(&event.source)
            || event.event_type != "synthetic.observed"
            || !event.payload.is_object()
        {
            return Err(Error::Invalid(
                "dotted source/type and object payload required",
            ));
        }
        let length = event.bytes.len() as i64;
        let record_hash = blake3::hash(&event.bytes).as_bytes().to_vec();
        let hash = hashed_blobs::Hash::of(&event.bytes);
        let blob_name = hash.to_string();
        // Register the blob row as 'writing' before writing the file, so startup
        // recovery can find a file left behind by an interrupted write.
        let new_blob_id = Uuid::now_v7();
        sqlx::query!(
            "INSERT INTO core.blobs (id,blake3,size_bytes,root,status) \
             VALUES ($1,$2,$3,$4,'writing') ON CONFLICT (blake3) DO NOTHING",
            new_blob_id,
            blob_name,
            length,
            self.root
        )
        .execute(&mut self.connection)
        .await?;
        let row = sqlx::query!(
            "SELECT id,size_bytes,root FROM core.blobs WHERE blake3=$1",
            blob_name
        )
        .fetch_one(&mut self.connection)
        .await?;
        let blob_id = row.id;
        if row.size_bytes != length || row.root != self.root {
            return Err(Error::Invalid("existing blob registry identity differs"));
        }
        let store = self.blobs.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            store.put(event.bytes.as_slice(), 3)?;
            if store.verify(&hash)? != hashed_blobs::Verified::Intact {
                return Err(Error::Invalid(
                    "stored material failed integrity verification",
                ));
            }
            Ok(())
        })
        .await??;
        let event_id = Uuid::now_v7();
        let resolved_time = sinex_model::events::TsOrig::unknown()
            .project(
                sinex_model::events::EventId::try_from(event_id)
                    .map_err(|e| Error::Invalid(e.0))?,
                sinex_model::events::CaptureClock::Sinex,
            )
            .map_err(|e| Error::Invalid(e.0))?;
        let original = resolved_time.at.map(|at| at.to_string());
        let until = resolved_time.until.map(|at| at.to_string());
        let material_id = Uuid::now_v7();
        let mut tx = self.connection.begin().await?;
        // UPDATE locks the counter row until commit, assigning visibility order.
        let seq =
            sqlx::query_scalar!("UPDATE core.commit_counter SET seq=seq+1 WHERE one RETURNING seq")
                .fetch_one(&mut *tx)
                .await?;
        sqlx::query!(
            "UPDATE core.blobs SET status='present' WHERE id=$1",
            blob_id
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "INSERT INTO raw.source_material_registry \
             (id,material_kind,source_identifier,status,ts_quality,blob_id,total_bytes,durable_bytes) \
             VALUES ($1,'file',$2,'completed','unknown',$3,$4,$4)",
            material_id,
            event.source,
            blob_id,
            length
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "INSERT INTO core.events (id,source,event_type,payload,ts_orig,ts_quality,commit_seq,\
             source_material_id,anchor_byte,ordinal,record_hash,run_id,ts_precision,ts_orig_until,\
             ts_clock,time_suspect) VALUES ($1,$2,$3,$4,$11::text::timestamptz,$12,$5,$6,$7,$8,$9,\
             $10,$13,$14::text::timestamptz,$15,$16)",
            event_id,
            event.source,
            event.event_type,
            event.payload,
            seq,
            material_id,
            event.anchor_byte,
            event.ordinal,
            record_hash,
            self.run_id,
            original,
            resolved_time.quality,
            resolved_time.precision,
            until,
            resolved_time.clock.label(),
            resolved_time.suspect
        )
        .execute(&mut *tx)
        .await?;
        let coordinate = coordinate_hash(
            material_id,
            event.anchor_byte,
            &event.event_type,
            event.ordinal,
        );
        sqlx::query!(
            "INSERT INTO core.live_coordinates (store,occ_hash,event_id) VALUES (0,$1,$2)",
            coordinate,
            event_id
        )
        .execute(&mut *tx)
        .await?;
        sqlx::query!(
            "INSERT INTO core.commit_log (commit_seq,event_id,change,store,event_type_id) \
             VALUES ($1,$2,0,0,1)",
            seq,
            event_id
        )
        .execute(&mut *tx)
        .await?;
        let position = serde_json::json!({"material":material_id,"byte_offset":length});
        sqlx::query!(
            "INSERT INTO raw.source_cursors (binding,input,position,carry) \
             VALUES ($1,'synthetic-proof',$2,'{}'::jsonb) ON CONFLICT (binding,input) \
             DO UPDATE SET position=EXCLUDED.position,carry=EXCLUDED.carry,updated_at=now()",
            event.source,
            position
        )
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(Receipt {
            event_id,
            material_id,
            commit_seq: seq,
            blob_name,
        })
    }
}

/// Lowercase dot-separated name, at most 128 bytes, with no empty segment.
#[cfg(any(feature = "product", test))]
fn dotted(s: &str) -> bool {
    s.len() <= 128
        && s.contains('.')
        && s.split('.').all(|part| {
            !part.is_empty()
                && part
                    .bytes()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == b'_' || c == b'-')
        })
}

#[cfg(test)]
mod tests {
    use super::dotted;
    #[test]
    fn rejects_empty_source_segments() {
        assert!(!dotted("synthetic..case"));
        assert!(dotted("synthetic.case"));
    }
}
