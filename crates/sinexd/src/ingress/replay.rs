//! Complete outside replacements use the same writer transaction as local replay.
use super::*;
use sinex_model::{ArchiveRequest, KindDeclaration, KindRef, MaterialId, ReplayScope};
use sinex_protocol::{ProducerReplay, ReplayMaterial, ReplayScopeQuery, ReplaySelection};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
pub(crate) struct CheckedReplay {
    pub content_hash: String,
    pub host: Option<String>,
    pub family: String,
    pub write_kind_prefixes: Vec<String>,
    pub producer: String,
    pub frame: String,
    pub sequence: ProducerReplay,
}

pub(super) async fn select(
    conn: &mut PgConnection,
    producer: &str,
    family: &str,
    query: &ReplayScopeQuery,
) -> Result<Vec<ReplaySelection>, Check> {
    let rows = sinex_store::replay::producer_materials(conn, producer)
        .await
        .map_err(|e| Check::Unavailable(e.to_string()))?;
    let ids: Vec<_> = rows.iter().map(|row| row.0).collect();
    let unknown = sinex_store::producer_reads::family_unknown(conn, producer, &ids, family)
        .await
        .map_err(|error| Check::Unavailable(error.to_string()))?;
    if unknown {
        return Err(Check::Refused(
            "historical material family ownership requires an explicit mapping".into(),
        ));
    }
    let descriptors = sinex_store::producer_reads::material_extents(conn, &ids)
        .await
        .map_err(|error| Check::Unavailable(error.to_string()))?;
    let descriptors: BTreeMap<_, _> = descriptors
        .into_iter()
        .map(|(id, length, descriptor)| (id, (length, descriptor)))
        .collect();
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
            length: u64::try_from(
                descriptors
                    .get(id)
                    .ok_or_else(|| {
                        Check::Unavailable(format!("material {id} has no committed extent"))
                    })?
                    .0,
            )
            .map_err(|error| Check::Unavailable(error.to_string()))?,
            extents: vec![descriptors[id].1.clone()],
        });
    }
    let mut selections = Vec::new();
    for (root, materials) in grouped {
        if !query.materials.is_empty() && !materials.iter().any(|m| query.materials.contains(&m.id))
        {
            continue;
        }
        let ids: Vec<_> = materials.iter().map(|m| m.id.0).collect();
        let (rows, expected_live_hash) = live_hash(conn, family, &ids, query).await?;
        selections.push(ReplaySelection {
            root: MaterialId(root),
            materials,
            rows,
            expected_live_hash,
        });
    }
    Ok(selections)
}

pub(super) async fn select_page(
    conn: &mut PgConnection,
    producer: &str,
    family: &str,
    query: &ReplayScopeQuery,
) -> Result<sinex_protocol::ReplayScopePage, Check> {
    let selections = select(conn, producer, family, query).await?;
    let sequences = selections
        .into_iter()
        .filter(|selection| query.after.is_none_or(|after| selection.root > after))
        .take(64)
        .collect::<Vec<_>>();
    let next = (sequences.len() == 64).then(|| sequences.last().expect("full page").root);
    Ok(sinex_protocol::ReplayScopePage { sequences, next })
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
    if let Some(frame) = frames.first() {
        if frame
            .outcome
            .get("frame_content_hash")
            .and_then(Value::as_str)
            .is_some_and(|hash| hash != request.content_hash)
        {
            return Err(Check::Refused("frame_body_conflict".into()));
        }
        if let Some(reason) = &frame.refusal {
            return Err(Check::Refused(format!("recorded_frame_refused: {reason}")));
        }
    }
    Ok(frames.first().map(|_| sinex_store::Outcome::default()))
}

pub(crate) async fn check_scope(
    conn: &mut PgConnection,
    request: &CheckedReplay,
) -> Result<BTreeMap<KindRef, KindDeclaration>, Check> {
    let sequence = &request.sequence;
    let source = sinex_store::source_declarations::load(conn, &sequence.source)
        .await
        .map_err(|error| Check::Unavailable(error.to_string()))?
        .ok_or_else(|| {
            Check::Configuration(
                "replay source version is not in the immutable owner catalog".into(),
            )
        })?;
    if source.family != request.family {
        return Err(Check::Configuration(
            "replay source belongs to another family".into(),
        ));
    }
    let material_ids = sequence
        .materials
        .iter()
        .map(|material| material.id.0)
        .collect::<Vec<_>>();
    let material_sources = sinex_store::producer_reads::material_sources(conn, &material_ids)
        .await
        .map_err(|error| Check::Unavailable(error.to_string()))?;
    if material_ids.iter().any(|id| {
        !material_sources.iter().any(|(known, name, _)| {
            known == id && name.as_deref() == Some(sequence.source.name.as_str())
        })
    }) {
        return Err(Check::Configuration(
            "replay cannot relabel a material source or invent historical source authority".into(),
        ));
    }
    if sequence.parser_build.trim().is_empty() || sequence.parser_version.trim().is_empty() {
        return Err("replay needs the producer parser build and version".into());
    }
    let query = ReplayScopeQuery {
        after: None,
        materials: sequence.materials.iter().map(|m| m.id).collect(),
        event_types: sequence.event_types.clone(),
        from: sequence.from,
        until: sequence.until,
    };
    let selected = select(conn, &request.producer, &request.family, &query).await?;
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
        if !source.allows(&supplied.kind, &supplied.hash) {
            return Err(Check::Configuration(
                "replay kind is outside its exact owner source declaration".into(),
            ));
        }
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
                (validator.normalize)(&event.payload).map_err(|_| "invalid bundled replacement")?
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
    number: i64,
) -> Result<(), Check> {
    let ids: Vec<_> = outcomes
        .iter()
        .flat_map(|o| o.admitted_ids.iter().map(|e| e.0))
        .collect();
    let s = &request.sequence;
    let in_scope = sinex_store::replay::producer_outputs_in_scope(
        conn,
        &request.family,
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
    let declaration = sinex_store::source_declarations::load(conn, &request.sequence.source)
        .await
        .map_err(|error| Check::Unavailable(error.to_string()))?
        .ok_or("replay source declaration disappeared")?;
    commit_log::record_replay_frame(
        conn,
        &request.producer,
        &request.frame,
        &request.family,
        &request.write_kind_prefixes,
        s.operation,
        &ids,
        last,
        number,
        &request.content_hash,
        &request.sequence.source,
        &declaration.fingerprint(),
        request.host.as_deref(),
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
        content_hash: String,
    ) -> Result<Acknowledgment, String> {
        self.native_declaration(producer, &sequence.source)
            .await
            .map_err(|error| error.to_string())?;
        let write = authority::write(&self.grants, producer).map_err(|error| error.to_string())?;
        let request = CheckedReplay {
            content_hash,
            host: write.host.clone(),
            family: write.family.clone(),
            write_kind_prefixes: write.kind_prefixes.clone(),
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
                        .submit(
                            Priority::Live,
                            frozen_refusal(
                                refusal(
                                    run,
                                    producer,
                                    frame,
                                    sinex_model::FrameRefusal::Content,
                                    reason,
                                ),
                                &request.content_hash,
                            ),
                        )
                        .await
                        .map_err(|e| e.to_string())?;
                }
            },
            Err(error) => {
                let (kind, reason) = match error {
                    Check::Unavailable(error) => return Err(error),
                    Check::Refused(reason) => (sinex_model::FrameRefusal::Content, reason),
                    Check::Bytes(reason) => (sinex_model::FrameRefusal::Bytes, reason),
                    Check::Malformed(reason) => (sinex_model::FrameRefusal::Malformed, reason),
                    Check::Configuration(reason) => {
                        (sinex_model::FrameRefusal::Configuration, reason)
                    }
                };
                self.writer
                    .submit(
                        Priority::Live,
                        frozen_refusal(
                            refusal(run, producer, frame, kind, reason),
                            &request.content_hash,
                        ),
                    )
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
                source: None,
                host: None,
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
            let records = self
                .events(
                    &request.producer,
                    Some(&request.sequence.source),
                    &batch.events,
                    &batch.failures,
                    true,
                )
                .await?;
            let assign_orders = batch
                .events
                .iter()
                .zip(records.iter().flat_map(|record| match record {
                    RecordOutcome::Events { events, .. } => events.as_slice(),
                    RecordOutcome::Failure { .. } => &[],
                }))
                .filter_map(|(offered, stored)| {
                    offered.reading_order.is_none().then_some(stored.id)
                })
                .collect();
            batches.push(Batch {
                run,
                kind: BatchKind::Ingress(Box::new(IngressFrame {
                    content_hash: None,
                    receipt_number: None,
                    coverage: None,
                    native_source: Some(request.sequence.source.clone()),
                    source_fingerprint: None,
                    host: self
                        .grants
                        .get(&request.producer)
                        .and_then(|grant| grant.write.as_ref())
                        .and_then(|write| write.host.clone()),
                    family: Some(request.family.clone()),
                    write_kind_prefixes: request.write_kind_prefixes.clone(),
                    assign_orders,
                    producer: request.producer.clone(),
                    frame: String::new(),
                    material: None,
                    refusal: None,
                    declarations: vec![],
                    refusal_kind: None,
                    source: None,
                    event_types: vec![],
                })),
                records,
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

fn frozen_refusal(mut batch: Batch, hash: &str) -> Batch {
    if let BatchKind::Ingress(frame) = &mut batch.kind {
        frame.content_hash = Some(hash.into());
    }
    batch
}
