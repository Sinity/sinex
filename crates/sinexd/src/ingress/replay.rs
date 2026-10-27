//! Complete outside replacements use the same writer transaction as local replay.
use super::*;
use sinex_model::{ArchiveRequest, KindDeclaration, KindRef, MaterialId, ReplayScope};
use sinex_protocol::{ProducerReplay, ReplayMaterial, ReplayScopeQuery, ReplaySelection};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
pub(crate) struct CheckedReplay {
    pub producer: String,
    pub frame: String,
    pub sequence: ProducerReplay,
}

pub(super) async fn select(
    conn: &mut PgConnection,
    producer: &str,
    query: &ReplayScopeQuery,
) -> Result<Vec<ReplaySelection>, Check> {
    let rows = sinex_store::replay::producer_materials(conn, producer)
        .await
        .map_err(|e| Check::Unavailable(e.to_string()))?;
    let by_id: BTreeMap<_, _> = rows.iter().map(|r| (r.0, r)).collect();
    for id in &query.materials {
        if !by_id.contains_key(&id.0) {
            return Err(Check::Refused(format!(
                "material {id} is not registered by {producer}"
            )));
        }
    }
    let mut grouped: BTreeMap<Uuid, Vec<ReplayMaterial>> = BTreeMap::new();
    for (id, _, blob) in &rows {
        let mut root = *id;
        let mut seen = BTreeSet::new();
        while let Some(previous) = by_id[&root].1 {
            if !seen.insert(root) {
                return Err("material part sequence contains a cycle".into());
            }
            if !by_id.contains_key(&previous) {
                return Err("part sequence includes another producer's material".into());
            }
            root = previous;
        }
        grouped.entry(root).or_default().push(ReplayMaterial {
            id: MaterialId(*id),
            blob: blob.clone(),
        });
    }
    let mut selections = Vec::new();
    for (root, materials) in grouped {
        if !query.materials.is_empty() && !materials.iter().any(|m| query.materials.contains(&m.id))
        {
            continue;
        }
        let ids: Vec<_> = materials.iter().map(|m| m.id.0).collect();
        let (rows, expected_live_hash) = live_hash(conn, producer, &ids, query).await?;
        selections.push(ReplaySelection {
            root: MaterialId(root),
            materials,
            rows,
            expected_live_hash,
        });
    }
    Ok(selections)
}

async fn live_hash(
    conn: &mut PgConnection,
    producer: &str,
    ids: &[Uuid],
    query: &ReplayScopeQuery,
) -> Result<(u64, String), Check> {
    sinex_store::replay::producer_live_hash(
        conn,
        producer,
        &ReplayScope {
            materials: ids.iter().copied().map(MaterialId).collect(),
            event_types: query.event_types.clone(),
            from: query.from,
            until: query.until,
        },
    )
    .await
    .map_err(|e| Check::Unavailable(e.to_string()))
}

pub(crate) async fn recorded(
    conn: &mut PgConnection,
    request: &CheckedReplay,
) -> Result<Option<sinex_store::Outcome>, Check> {
    let frames = commit_log::frames(
        conn,
        &request.producer,
        std::slice::from_ref(&request.frame),
    )
    .await
    .map_err(|e| Check::Unavailable(e.to_string()))?;
    Ok(frames.first().map(|_| sinex_store::Outcome::default()))
}

pub(crate) async fn check_scope(
    conn: &mut PgConnection,
    request: &CheckedReplay,
) -> Result<BTreeMap<KindRef, KindDeclaration>, Check> {
    let sequence = &request.sequence;
    if sequence.parser_build.trim().is_empty() || sequence.parser_version.trim().is_empty() {
        return Err("replay needs the producer parser build and version".into());
    }
    let query = ReplayScopeQuery {
        materials: sequence.materials.iter().map(|m| m.id).collect(),
        event_types: sequence.event_types.clone(),
        from: sequence.from,
        until: sequence.until,
    };
    let selected = select(conn, &request.producer, &query).await?;
    if selected.len() != 1
        || selected[0].root != sequence.root
        || selected[0].materials != sequence.materials
    {
        return Err(
            "replacement must name the complete registered part sequence and its exact digests"
                .into(),
        );
    }
    if selected[0].expected_live_hash != sequence.expected_live_hash {
        return Err("replay_live_changed: readings changed while parsing".into());
    }
    let references: Vec<_> = sequence
        .declarations
        .iter()
        .map(|d| d.kind.clone())
        .collect();
    let declarations = sinex_schema::kinds::load(conn, &references)
        .await
        .map_err(|e| Check::Unavailable(e.to_string()))?;
    for supplied in &sequence.declarations {
        if !declarations
            .get(&supplied.kind)
            .is_some_and(|d| d.definition_hash() == supplied.hash)
        {
            return Err("replay_build_changed: target declaration differs".into());
        }
    }
    Ok(declarations)
}

pub(crate) async fn check(conn: &mut PgConnection, request: &CheckedReplay) -> Result<(), Check> {
    let declarations = check_scope(conn, request).await?;
    let sequence = &request.sequence;
    for batch in &sequence.batches {
        for event in &batch.events {
            let declaration = declarations
                .get(&event.kind)
                .ok_or("replacement uses an unpinned declaration")?;
            let payload = if declaration.normalization == sinex_model::kinds::Normalization::Rust {
                let validator = sinex_model::event_types::find(&event.kind.name)
                    .filter(|k| (k.declaration)() == *declaration)
                    .ok_or("validator_unavailable")?;
                (validator.build)(
                    &event.payload,
                    &request.producer,
                    Origin::Record {
                        location: event.location.ok_or("replacement needs a location")?,
                        output_index: event.output_index,
                    },
                    ReadingOrder(event.reading_order),
                    [0; 32],
                    event.class,
                )
                .map_err(|_| "invalid bundled replacement")?
                .payload
            } else {
                event.payload.clone()
            };
            let evaluated = declaration
                .evaluate(&payload, event.class)
                .map_err(Check::Refused)?;
            if sequence
                .event_types
                .as_ref()
                .is_some_and(|types| !types.contains(&event.kind.name))
                || sequence
                    .from
                    .is_some_and(|from| evaluated.ts_orig.at.is_none_or(|at| at < from))
                || sequence
                    .until
                    .is_some_and(|until| evaluated.ts_orig.at.is_none_or(|at| at >= until))
            {
                return Err("replacement emits readings outside its scope".into());
            }
            let Some(location) = event.location else {
                return Err("outside replay replaces material readings only".into());
            };
            if !sequence.materials.iter().any(|m| m.id == location.material) {
                return Err("replacement location is outside the sequence".into());
            }
            if !event.parents.is_empty() {
                return Err("a material reading cannot also name parents".into());
            }
        }
        for failure in &batch.failures {
            if !sequence
                .materials
                .iter()
                .any(|m| m.id == failure.location.material)
            {
                return Err("failure location is outside the sequence".into());
            }
        }
    }
    Ok(())
}

pub(crate) async fn finish(
    conn: &mut PgConnection,
    request: &CheckedReplay,
    outcomes: &[sinex_store::Outcome],
) -> Result<(), Check> {
    let ids: Vec<_> = outcomes
        .iter()
        .flat_map(|o| o.admitted_ids.iter().map(|e| e.0))
        .collect();
    let s = &request.sequence;
    let in_scope = sinex_store::replay::producer_outputs_in_scope(
        conn,
        &request.producer,
        &ids,
        &ReplayScope {
            materials: s.materials.iter().map(|m| m.id).collect(),
            event_types: s.event_types.clone(),
            from: s.from,
            until: s.until,
        },
    )
    .await
    .map_err(|e| Check::Unavailable(e.to_string()))?;
    if !in_scope {
        return Err("replacement emits readings outside its scope".into());
    }
    let last = outcomes
        .iter()
        .filter_map(|o| o.commit_seq.map(|s| s.1))
        .max();
    commit_log::record_replay_frame(
        conn,
        &request.producer,
        &request.frame,
        s.operation,
        &ids,
        last,
    )
    .await
    .map_err(|e| Check::Unavailable(e.to_string()))?;
    Ok(())
}

impl Ingest {
    pub(super) async fn replay_sequence(
        &mut self,
        producer: &str,
        frame: &str,
        run: RunId,
        sequence: ProducerReplay,
    ) -> Result<Acknowledgment, String> {
        let request = CheckedReplay {
            producer: producer.into(),
            frame: frame.into(),
            sequence,
        };
        let result = self.prepare_replay(&request, run).await;
        match result {
            Ok(replay) => match self.writer.replay(replay).await {
                Ok(_) => {}
                Err(WriteError::Unavailable(error)) => return Err(error),
                Err(WriteError::Stopped) => return Err("the writer stopped".into()),
                Err(WriteError::Refused(reason)) => {
                    self.writer
                        .submit(Priority::Live, refusal(run, producer, frame, reason))
                        .await
                        .map_err(|e| e.to_string())?;
                }
            },
            Err(Check::Unavailable(error)) => return Err(error),
            Err(Check::Refused(reason)) => {
                self.writer
                    .submit(Priority::Live, refusal(run, producer, frame, reason))
                    .await
                    .map_err(|e| e.to_string())?;
            }
        }
        let stored = self
            .stored(producer, frame)
            .await?
            .ok_or("replay acknowledgment is missing")?;
        self.discard(producer, frame);
        Ok(acknowledgment(stored))
    }

    async fn prepare_replay(
        &mut self,
        request: &CheckedReplay,
        run: RunId,
    ) -> Result<crate::writer::ReplaySequence, Check> {
        let s = &request.sequence;
        let mut batches = vec![Batch {
            run,
            kind: BatchKind::Replay {
                operation: s.operation,
            },
            records: vec![],
            archive: vec![ArchiveRequest::Replay {
                operation: s.operation,
                scope: ReplayScope {
                    materials: s.materials.iter().map(|m| m.id).collect(),
                    event_types: s.event_types.clone(),
                    from: s.from,
                    until: s.until,
                },
            }],
        }];
        for batch in &s.batches {
            batches.push(Batch {
                run,
                kind: BatchKind::Ingress(Box::new(IngressFrame {
                    producer: request.producer.clone(),
                    frame: String::new(),
                    material: None,
                    refusal: None,
                    declarations: vec![],
                })),
                records: self
                    .events(&request.producer, &batch.events, &batch.failures)
                    .await?,
                archive: vec![],
            });
        }
        let (send, receive) = tokio::sync::mpsc::channel(batches.len());
        for batch in batches {
            send.try_send(Ok(batch))
                .map_err(|_| Check::Unavailable("replacement channel closed".into()))?;
        }
        drop(send);
        let content =
            serde_json::to_vec(s).map_err(|_| Check::Refused("invalid replay scope".into()))?;
        let mut scope = serde_json::json!({"root":s.root,"materials":s.materials,"event_types":s.event_types,"from":s.from,"until":s.until,
            "declarations":s.declarations,"parser_build":s.parser_build,"parser_version":s.parser_version,
            "expected_live_hash":s.expected_live_hash,"content_hash":blake3::hash(&content).to_hex().to_string()});
        scope["identity"] = crate::replay::build_identity().map_err(Check::Unavailable)?;
        Ok(crate::writer::ReplaySequence {
            operation: s.operation,
            actor: request.producer.clone(),
            scope,
            root: s.root.0,
            compare: false,
            batches: receive,
            ingress: Some(request.clone()),
        })
    }
}
