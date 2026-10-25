//! Bytes a producer hands over for a frame, through its frame directory
//! (`sinex_transport::frame_bytes`): stored before the frame is admitted, and
//! removed once the frame is recorded.
use super::*;
use sinex_transport::frame_bytes;

impl Ingest {
    /// Add one chunk to the frame directory of the frame it is for.
    pub(super) async fn receive_chunk(&self, producer: &str, chunk: &Ingress) -> Result<(), Check> {
        let Ingress::Chunk {
            for_frame,
            blob,
            offset,
            total,
            data,
            ..
        } = chunk
        else {
            return Err(Check::Refused("expected a chunk".into()));
        };
        let data = base64::engine::general_purpose::STANDARD
            .decode(data)
            .map_err(|e| Check::Refused(format!("chunk data is not base64: {e}")))?;
        let root = self.blobs.root().to_owned();
        let (producer, for_frame, blob, offset, total) = (
            producer.to_owned(),
            for_frame.clone(),
            blob.clone(),
            *offset,
            *total,
        );
        tokio::task::spawn_blocking(move || {
            frame_bytes::receive(&root, &producer, &for_frame, &blob, total, offset, &data)
        })
        .await
        .map_err(|e| Check::Unavailable(e.to_string()))?
        .map(drop)
        .map_err(Check::Refused)
    }

    /// The frame a large frame stands for, read from its frame directory.
    pub(super) async fn unwrap_large_frame(
        &self,
        producer: &str,
        frame: Ingress,
    ) -> Result<Ingress, Check> {
        let Ingress::LargeFrame {
            frame: name,
            blob,
            length,
        } = frame
        else {
            return Ok(frame);
        };
        let root = self.blobs.root().to_owned();
        let producer = producer.to_owned();
        let inner = tokio::task::spawn_blocking(move || {
            let file = frame_bytes::snapshot(&root, &producer, &name, &blob, length)
                .map_err(Check::Refused)?;
            serde_json::from_reader::<_, Ingress>(std::io::BufReader::new(file))
                .map_err(|e| Check::Refused(format!("large-frame bytes are not a frame: {e}")))
        })
        .await
        .map_err(|e| Check::Unavailable(e.to_string()))??;
        match inner {
            Ingress::Material { .. } | Ingress::Events { .. } => Ok(inner),
            _ => Err(Check::Refused(
                "a large frame stands for a material or events frame".into(),
            )),
        }
    }

    /// Store a material's bytes from the frame directory of `frame`, unless the
    /// content store already holds them.
    pub(super) async fn store_material(
        &self,
        producer: &str,
        frame: &str,
        material: &MaterialFrame,
    ) -> Result<(), Check> {
        if self
            .blobs
            .contains(&material.blob)
            .map_err(|e| Check::Unavailable(e.to_string()))?
        {
            return Ok(());
        }
        let root = self.blobs.root().to_owned();
        let blobs = self.blobs.clone();
        let (producer, frame, blob, length) = (
            producer.to_owned(),
            frame.to_owned(),
            material.blob.clone(),
            material.length,
        );
        tokio::task::spawn_blocking(move || {
            let file = frame_bytes::snapshot(&root, &producer, &frame, &blob, length).map_err(|e| {
                Check::Refused(format!(
                    "the bytes {blob} are neither in the content store nor in this frame's directory: {e}"
                ))
            })?;
            let stored = blobs
                .put_reader(std::io::BufReader::new(file))
                .map_err(|e| Check::Unavailable(e.to_string()))?;
            if stored != blob {
                return Err(Check::Refused(format!(
                    "the frame bytes are {stored}, not {blob}"
                )));
            }
            Ok(())
        })
        .await
        .map_err(|e| Check::Unavailable(e.to_string()))?
    }

    /// Whether the producer's privacy settings ask for sealed storage, which
    /// producer bytes do not support.
    pub(super) async fn sealed(&mut self, producer: &str) -> Result<bool, String> {
        let privacy = sinex_store::privacy::load(&mut self.conn, std::iter::once(producer))
            .await
            .map_err(|e| e.to_string())?;
        Ok(
            privacy.floors.get(producer) == Some(&sinex_model::Class::Sealed)
                || privacy
                    .rules
                    .iter()
                    .any(|r| r.class == sinex_model::Class::Sealed),
        )
    }

    /// Remove the frame directory of a recorded frame.
    pub(super) fn discard(&self, producer: &str, frame: &str) {
        if let Err(error) = frame_bytes::discard(self.blobs.root(), producer, frame) {
            eprintln!("sinexd: removing the directory of frame {frame} from {producer}: {error}");
        }
    }
}
