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
            .map_err(|e| Check::Bytes(format!("chunk data is not base64: {e}")))?;
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
        .map_err(check_bytes)
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
                .map_err(check_bytes)?;
            serde_json::from_reader::<_, Ingress>(std::io::BufReader::new(file)).map_err(|e| {
                if e.is_io() {
                    Check::Unavailable(e.to_string())
                } else {
                    Check::Malformed(format!("large-frame bytes are not a frame: {e}"))
                }
            })
        })
        .await
        .map_err(|e| Check::Unavailable(e.to_string()))??;
        if inner.frame().is_empty() {
            return Err(Check::Malformed(
                "a large frame's inner frame needs a name".into(),
            ));
        }
        if inner.frame().contains('\0') {
            return Err(Check::Malformed("a large frame's inner name contains NUL".into()));
        }
        match inner {
            Ingress::Material { .. } | Ingress::Events { .. } => Ok(inner),
            _ => Err(Check::Malformed(
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
            let file = frame_bytes::snapshot(&root, &producer, &frame, &blob, length)
                .map_err(check_bytes)?;
            let stored = blobs
                .put_seekable(std::io::BufReader::new(file))
                .map_err(|e| Check::Unavailable(e.to_string()))?;
            if stored != blob {
                return Err(Check::Bytes(format!(
                    "the frame bytes are {stored}, not {blob}"
                )));
            }
            Ok(())
        })
        .await
        .map_err(|e| Check::Unavailable(e.to_string()))?
    }

    /// Remove the frame directory of a recorded frame.
    pub(super) fn discard(&self, producer: &str, frame: &str) {
        if let Err(error) = frame_bytes::discard(self.blobs.root(), producer, frame) {
            eprintln!("sinexd: removing the directory of frame {frame} from {producer}: {error}");
        }
    }
}

fn check_bytes(error: frame_bytes::Error) -> Check {
    match error {
        frame_bytes::Error::Invalid(reason) => Check::Malformed(reason),
        frame_bytes::Error::Mismatch(reason) => Check::Bytes(reason),
        frame_bytes::Error::Storage(reason) => Check::Unavailable(reason),
    }
}
