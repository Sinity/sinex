//! Inactive physical prefixes are copies, not new parser/counter transitions.
use super::*;
use sinex_model::RecordLocation;
use sinex_sources::drain::RecordBytes;

pub(super) type Copies = BTreeMap<MaterialId, BTreeMap<usize, usize>>;

pub(super) async fn select(
    conn: &mut PgConnection,
    blobs: &Blobs,
    archive: &Archive,
    selected: &mut BTreeMap<MaterialId, BTreeSet<i64>>,
    whole: Option<MaterialId>,
    authority: &Authority,
) -> Result<Copies, String> {
    let mut selected_digests = BTreeSet::new();
    for material in selected.keys() {
        archive
            .walk(conn, blobs, *material, authority, |record| {
                if selected[material].contains(&(record.start as i64)) {
                    selected_digests.insert(*blake3::hash(record.raw).as_bytes());
                }
                Ok(())
            })
            .await?;
    }
    let policy = crate::source_excision::AcquisitionPolicy::load(
        conn,
        blobs,
        &archive.candidates[0].sources[0],
        Some(authority),
    )
    .await?;
    let whole_domain = if let Some(material) = whole {
        let opened = crate::retained::open(
            conn,
            blobs,
            &archive.candidates[0].sources[0],
            material,
            Some(authority),
        )
        .await?;
        Some((
            material,
            opened
                .secrets
                .ok_or("whole material lacks its naming capability")?
                .1
                .naming_only(),
            opened.metadata.into_iter().fold(
                BTreeMap::<[u8; 32], usize>::new(),
                |mut counts, r| {
                    *counts.entry(r.raw_digest).or_default() += 1;
                    counts
                },
            ),
        ))
    } else {
        None
    };
    let mut copies = Copies::new();
    for candidate in &archive.candidates {
        let source = archive.source(candidate.material)?;
        if source.spec().driver != sinex_sources::Driver::AppendFile
            || candidate.manifest.get("records").is_none()
        {
            continue;
        }
        let ancestors = archive.predecessors(candidate.material)?;
        let opened = crate::retained::open(
            conn,
            blobs,
            &candidate.sources[0],
            candidate.material,
            Some(authority),
        )
        .await?;
        let Some(body) = &opened.body else { continue };
        let active = body
            .records(&candidate.manifest, source.spec().framer)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let mut possible = BTreeMap::<[u8; 32], Vec<(usize, usize, usize, bool)>>::new();
        for (start, end) in body.records(&Value::Null, source.spec().framer)? {
            authority.check()?;
            if active.contains(&(start, end))
                || opened.metadata.iter().any(|r| r.start == start as u64)
            {
                continue;
            }
            let raw = RecordBytes::Reader(&**body)
                .record(start, end)
                .map_err(|_| "inactive retained prefix cannot be read")?;
            let digest = *blake3::hash(&raw).as_bytes();
            let domain_count = whole_domain
                .as_ref()
                .filter(|(material, _, _)| ancestors.contains(material))
                .and_then(|(_, names, digests)| digests.get(&names.name_raw_record(&raw)).copied())
                .unwrap_or(0);
            let context = sinex_sources::RecordContext {
                manifest: &candidate.manifest,
                member: sinex_sources::drain::member_at(&candidate.manifest, start),
            };
            let old_exclusion = policy.excludes(
                candidate.material,
                start as u64,
                &raw,
                source
                    .record_hash(&raw, &context)
                    .map_err(|_| "inactive prefix hash is unavailable")?,
                &[],
            )?;
            if selected_digests.contains(&digest)
                || whole == Some(candidate.material)
                || domain_count != 0
                || old_exclusion
            {
                possible
                    .entry(digest)
                    .or_default()
                    .push((start, end, domain_count, old_exclusion));
            }
        }
        if possible.is_empty() {
            continue;
        }
        let mut hits = BTreeMap::<[u8; 32], BTreeSet<RecordLocation>>::new();
        for material in &ancestors {
            archive
                .walk(conn, blobs, *material, authority, |record| {
                    let digest = *blake3::hash(record.raw).as_bytes();
                    if possible.contains_key(&digest) {
                        hits.entry(digest).or_default().insert(RecordLocation {
                            material: *material,
                            anchor: record.start as u64,
                        });
                    }
                    Ok(())
                })
                .await?;
        }
        for (digest, spans) in possible {
            let locations = hits.get(&digest).cloned().unwrap_or_default();
            let selected_hit = locations.iter().any(|l| {
                selected
                    .get(&l.material)
                    .is_some_and(|a| a.contains(&(l.anchor as i64)))
            });
            let domain_count = spans
                .iter()
                .map(|(_, _, count, _)| *count)
                .max()
                .unwrap_or(0);
            let domain_hit = domain_count != 0;
            if whole == Some(candidate.material) && !selected_hit && !domain_hit {
                if locations.len() != 1 {
                    return Err("inactive whole-material prefix has ambiguous or missing predecessor evidence".into());
                }
                let location = locations.first().expect("one predecessor");
                selected
                    .entry(location.material)
                    .or_default()
                    .insert(location.anchor as i64);
            } else if !selected_hit && !domain_hit {
                if spans.iter().any(|(_, _, _, excluded)| *excluded) {
                    return Err("retained inactive prefix lacks its prior exclusion lineage".into());
                }
                continue;
            }
            if locations.iter().any(|l| {
                !selected
                    .get(&l.material)
                    .is_some_and(|a| a.contains(&(l.anchor as i64)))
            }) {
                return Err("inactive prefix bytes have ambiguous selected and unselected predecessor occurrences".into());
            }
            if spans.len() > locations.len().saturating_add(domain_count) {
                return Err("inactive prefix copies exceed proven predecessor cardinality".into());
            }
            for (start, end, _, _) in spans {
                selected
                    .entry(candidate.material)
                    .or_default()
                    .insert(start as i64);
                copies
                    .entry(candidate.material)
                    .or_default()
                    .insert(start, end);
            }
        }
    }
    policy.revalidate(conn).await?;
    Ok(copies)
}
