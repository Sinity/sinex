use super::*;
use sinex_model::producer_bytes::{Material, Publication, Role};
use sinex_transport::spool;
use sqlx::Connection;

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Point {
    Received,
    BeforeInstall,
    Verified,
}

impl Ingest {
    pub(super) async fn cleanup_bytes(&mut self) -> Result<(), String> {
        loop {
            let rows = sinex_store::producer_bytes::acknowledged(&mut self.conn, 64)
                .await
                .map_err(|e| e.to_string())?;
            if rows.is_empty() {
                return Ok(());
            }
            for (producer, frame) in rows {
                let mut tx = self.conn.begin().await.map_err(|e| e.to_string())?;
                let result=async {
                    sinex_store::producer_bytes::lock(&mut tx,&producer,&frame,false).await.map_err(|e|e.to_string())?;
                    let Some((declaration,_))=sinex_store::producer_bytes::load(&mut tx,&producer,&frame).await.map_err(|e|e.to_string())? else {return Ok(());};
                    let terminal:bool=sqlx::query_scalar("SELECT acknowledged FROM raw.pending_producer_bytes WHERE producer=$1 AND frame=$2")
                        .bind(&producer).bind(&frame).fetch_one(&mut *tx).await.map_err(|e|e.to_string())?;
                    if !terminal {return Err("producer spool is not acknowledged".to_owned());}
                    spool::discard_bytes(self.blobs.root(),&producer,&declaration)?;
                    sinex_store::producer_bytes::retire(&mut tx,&producer,&frame).await.map_err(|e|e.to_string())?;
                    Ok::<_,String>(())
                }.await;
                match result {
                    Ok(()) => tx.commit().await.map_err(|e| e.to_string())?,
                    Err(error) => {
                        tx.rollback().await.map_err(|e| e.to_string())?;
                        return Err(error);
                    }
                }
            }
        }
    }
    pub(super) async fn reserve_bytes(
        &mut self,
        producer: &str,
        name: &str,
        publication: Publication,
    ) -> Result<Acknowledgment, String> {
        publication.validate()?;
        if let Some(done) = self.stored(producer, &publication.frame).await? {
            return Ok(Acknowledgment {
                frame: name.into(),
                material: None,
                admitted: vec![],
                outcome: serde_json::json!({"producer_bytes":1,"complete":acknowledgment(done)}),
                refusal: None,
                commit_seq: None,
            });
        }
        self.byte_class(producer, &publication).await?;
        let mut tx = self.conn.begin().await.map_err(|e| e.to_string())?;
        let result = sinex_store::producer_bytes::reserve(&mut tx, producer, &publication).await;
        match result {
            Ok(()) => tx.commit().await.map_err(|e| e.to_string())?,
            Err(error) => {
                tx.rollback().await.map_err(|e| e.to_string())?;
                return Err(error.to_string());
            }
        }
        Ok(Acknowledgment {
            frame: name.into(),
            material: None,
            admitted: vec![],
            outcome: serde_json::json!({"producer_bytes":1,"complete":null}),
            refusal: None,
            commit_seq: None,
        })
    }

    async fn byte_class(
        &mut self,
        producer: &str,
        publication: &Publication,
    ) -> Result<(), String> {
        let privacy = sinex_store::privacy::load(&mut self.conn, std::iter::once(producer))
            .await
            .map_err(|e| e.to_string())?;
        if privacy
            .floors
            .get(producer)
            .is_some_and(|c| *c > publication.class)
            || privacy
                .rules
                .iter()
                .any(|r| r.class == sinex_model::Class::Sealed)
        {
            return Err("producer byte class is below current source privacy requirements; sealed producer storage is unsupported".into());
        }
        Ok(())
    }

    pub(super) async fn chunk_bytes(
        &mut self,
        producer: &str,
        chunk: &Ingress,
        guard: &mut Option<crate::excision::AcquisitionGuard>,
    ) -> Result<(), Check> {
        let Ingress::Chunk {
            publication,
            role,
            blob,
            offset,
            total,
            data,
            ..
        } = chunk
        else {
            return Err(Check::Refused("expected a producer chunk".into()));
        };
        let (role, offset, total) = (*role, *offset, *total);
        let Some((declaration, _)) =
            sinex_store::producer_bytes::load(&mut self.conn, producer, publication)
                .await
                .map_err(|e| Check::Unavailable(e.to_string()))?
        else {
            return Err(Check::Refused(
                "producer chunk has no durable byte reservation".into(),
            ));
        };
        let Some(bytes) = declaration
            .roles()
            .find_map(|(r, b)| (r == role).then_some(b))
        else {
            return Err(Check::Refused("producer chunk role is not reserved".into()));
        };
        if bytes.blob != *blob || bytes.length != total {
            return Err(Check::Refused(
                "producer chunk differs from its exact reservation".into(),
            ));
        }
        self.byte_class(producer, &declaration)
            .await
            .map_err(Check::Refused)?;
        let data = base64::engine::general_purpose::STANDARD
            .decode(data)
            .map_err(|e| Check::Refused(format!("chunk data is not base64: {e}")))?;
        let root = self.blobs.root().to_owned();
        let producer = producer.to_owned();
        #[cfg(test)]
        let hook = self.byte_hook.clone();
        let owned = guard.take().expect("guard follows detached receive");
        let (owned, result) = tokio::task::spawn_blocking(move || {
            let result = spool::receive(&root, &producer, &declaration, role, offset, &data);
            #[cfg(test)]
            if let Some(hook) = hook {
                hook(Point::Received);
            }
            (owned, result)
        })
        .await
        .map_err(|e| Check::Unavailable(e.to_string()))?;
        *guard = Some(owned);
        result.map(drop).map_err(Check::Refused)
    }

    pub(super) async fn unwrap_bytes(
        &mut self,
        producer: &str,
        frame: Ingress,
        guard: &mut Option<crate::excision::AcquisitionGuard>,
    ) -> Result<Ingress, Check> {
        if !matches!(frame, Ingress::Material { .. } | Ingress::Indirect { .. }) {
            return Ok(frame);
        }
        let Some((declaration, _)) =
            sinex_store::producer_bytes::load(&mut self.conn, producer, frame.frame())
                .await
                .map_err(|e| Check::Unavailable(e.to_string()))?
        else {
            return Err(Check::Refused(
                "producer frame has no durable byte reservation; update the producer protocol"
                    .into(),
            ));
        };
        self.byte_class(producer, &declaration)
            .await
            .map_err(Check::Refused)?;
        let root = self.blobs.root().to_owned();
        let task_producer = producer.to_owned();
        let blobs = self.blobs.clone();
        let frozen = declaration.clone();
        #[cfg(test)]
        let hook = self.byte_hook.clone();
        let owned = guard.take().expect("guard follows detached install");
        let (owned,result)=tokio::task::spawn_blocking(move || {
            let result=(|| {
            let inner=match frame {
                Ingress::Indirect{blob,..}=> {
                    if frozen.envelope.as_ref().is_none_or(|b|b.blob!=blob) {return Err(Check::Refused("indirect frame differs from its reserved envelope".into()));}
                    let file=spool::snapshot(&root,&task_producer,&frozen,Role::Envelope).map_err(Check::Unavailable)?;
                    serde_json::from_reader::<_,Ingress>(file).map_err(|e|Check::Refused(format!("indirect bytes are not a frame: {e}")))?
                }
                frame=> {
                    if frozen.envelope.is_some() {return Err(Check::Refused("reserved indirect envelope is required".into()));}
                    frame
                }
            };
            match &inner {
                Ingress::Material{material,..} if frozen.material.as_ref()==Some(&Material::from_frame(material))=> {
                    if !blobs.contains(&material.blob).map_err(|e|Check::Unavailable(e.to_string()))? {
                        let mut bytes=spool::snapshot(&root,&task_producer,&frozen,Role::Material).map_err(Check::Unavailable)?;
                        let prepared=blobs.prepare_plain(&mut bytes).map_err(|e|Check::Unavailable(e.to_string()))?;
                        if prepared.name()!=material.blob || prepared.length()!=material.length {return Err(Check::Refused("prepared producer body differs".into()));}
                        #[cfg(test)]
                        if let Some(hook)=&hook {hook(Point::BeforeInstall);}
                        prepared.install().map_err(|e|Check::Unavailable(e.to_string()))?;
                    }
                    let mut verified=blobs.read_plain(&material.blob,Some(material.length),||Ok(())).map_err(|e|Check::Unavailable(e.to_string()))?;
                    std::io::copy(&mut verified,&mut std::io::sink()).map_err(|e|Check::Unavailable(e.to_string()))?;
                }
                Ingress::Events{..} if frozen.material.is_none() && frozen.envelope.is_some()=>{},
                _=>return Err(Check::Refused("reserved producer descriptor differs; only material and events frames are supported".into())),
            }
            #[cfg(test)]
            if let Some(hook)=&hook {hook(Point::Verified);}
            Ok::<_,Check>(inner)
            })();
            (owned,result)
        }).await.map_err(|e|Check::Unavailable(e.to_string()))?;
        *guard = Some(owned);
        let inner = result?;
        let mut tx = self
            .conn
            .begin()
            .await
            .map_err(|e| Check::Unavailable(e.to_string()))?;
        match sinex_store::producer_bytes::installed(&mut tx, producer, &declaration).await {
            Ok(()) => tx
                .commit()
                .await
                .map_err(|e| Check::Unavailable(e.to_string()))?,
            Err(error) => {
                tx.rollback()
                    .await
                    .map_err(|e| Check::Unavailable(e.to_string()))?;
                return Err(Check::Unavailable(error.to_string()));
            }
        }
        Ok(inner)
    }
}
