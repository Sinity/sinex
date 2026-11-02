//! Curation requests, views, and curation imported from other applications'
//! sidecar files. A request's acts are planned into curation records against
//! one snapshot of the live records (`sinex_library::plan`), stored as the
//! request's material, and admitted only if no curation record changed since
//! that snapshot.
use super::{Library, R, Stored, VIA_API, e};
use serde_json::{Value, json};
use sinex_library::plan::{self, Caller, Draft, Key, Outcome, Planned};
use sinex_library::{import, items, reads};
use sinex_model::Timestamp;
use sinex_model::event_types::library::{
    Act, CurationKind, LibraryRecord, Target, ValueOrigin, View,
};
use sinex_sources::sources::library as library_source;
use sqlx::PgConnection;
use std::path::{Path, PathBuf};

/// Imported values planned and admitted per request; an import continues
/// over as many requests as it needs.
const IMPORT_REQUEST_RECORDS: usize = 2000;

impl Library<'_> {
    /// Plan, store and admit one curation request.
    pub(crate) async fn curate(
        &self,
        conn: &mut PgConnection,
        request: Option<String>,
        acts: &[Act],
        actor: &str,
    ) -> R<Value> {
        if acts.is_empty() {
            return Err("a curation request needs at least one write".into());
        }
        let caller = Caller {
            author: actor.to_owned(),
            via: VIA_API.to_owned(),
        };
        let _planning = self.writer.lock_curation().await;
        let at = Timestamp::now();
        match plan::plan(conn, request, acts, &caller, at)
            .await
            .map_err(e)?
        {
            Outcome::Repeat(records) => Ok(json!({
                "request": records[0].request,
                "repeat": true,
                "written": plan::written(&records),
            })),
            Outcome::Written(planned) => {
                self.store_planned(conn, planned, actor, at, "curate").await
            }
        }
    }

    async fn store_planned(
        &self,
        conn: &mut PgConnection,
        planned: Planned,
        actor: &str,
        at: Timestamp,
        action: &str,
    ) -> R<Value> {
        let placed: Vec<Value> = planned
            .placed
            .iter()
            .map(|placed| match placed {
                plan::Placed::Written { revision, revised } => json!({
                    "written": revision.to_string(),
                    "revised": revised.iter().map(ToString::to_string).collect::<Vec<_>>(),
                }),
                plan::Placed::Already { heads } => json!({
                    "already": heads.iter().map(ToString::to_string).collect::<Vec<_>>(),
                }),
            })
            .collect();
        let written = plan::written(&planned.records);
        let records: Vec<LibraryRecord> = planned
            .records
            .into_iter()
            .map(LibraryRecord::Curation)
            .collect();
        let stored = self
            .write(
                conn,
                records,
                actor,
                Stored {
                    action,
                    via: if action == "import" {
                        "import"
                    } else {
                        VIA_API
                    },
                    at,
                    cited: Vec::new(),
                    curation: true,
                },
            )
            .await?;
        Ok(json!({
            "request": planned.request,
            "writes": placed,
            "written": written,
            "material": stored.get("material").cloned().unwrap_or(Value::Null),
        }))
    }

    pub(crate) async fn viewed(
        &self,
        conn: &mut PgConnection,
        item: &str,
        seconds: Option<f64>,
        player: &str,
        actor: &str,
    ) -> R<Value> {
        if seconds.is_some_and(|s| !s.is_finite() || s < 0.0) {
            return Err("a view's duration is a finite number of seconds, zero or more".into());
        }
        let it = self.item_or(conn, item).await?;
        let now = Timestamp::now();
        let started = seconds
            .and_then(|s| Timestamp::from_unix_ns(now.unix_ns() - (s * 1e9) as i128).ok())
            .unwrap_or(now);
        let v = View {
            view: items::new_id(),
            target: Target {
                item: item.to_owned(),
                revision: Some(it.revision),
                rendition: None,
                part: None,
            },
            player: player.to_owned(),
            started_at: started,
            seconds,
            count: 1,
            by: String::new(),
        };
        self.write(
            conn,
            vec![LibraryRecord::View(v)],
            actor,
            Stored {
                action: "viewed",
                via: VIA_API,
                at: now,
                cited: Vec::new(),
                curation: false,
            },
        )
        .await?;
        Ok(json!({ "item": item, "seconds": seconds, "player": player }))
    }

    /// Bring in curation from sidecar files beside media under `path`, as
    /// imported values: they rank below the operator's and agents' authored
    /// values, and the same sidecar imported again writes nothing.
    pub(crate) async fn import(
        &self,
        conn: &mut PgConnection,
        path: &Path,
        actor: &str,
    ) -> R<Value> {
        let root =
            std::fs::canonicalize(path).map_err(|err| format!("{}: {err}", path.display()))?;
        let found =
            tokio::task::spawn_blocking(move || -> Vec<(PathBuf, String, Vec<import::Sidecar>)> {
                let mut out = Vec::new();
                let mut files = Vec::new();
                media_files(&root, &mut files);
                for f in files {
                    let sidecars: Vec<import::Sidecar> = import::sidecars_of(&f)
                        .into_iter()
                        .filter_map(|(p, fmt)| import::read(&p, fmt))
                        .collect();
                    if sidecars.is_empty() {
                        continue;
                    }
                    if let Ok(d) = library_source::digest(&f) {
                        let mut sc = sidecars;
                        // A site's MD5 that disagrees with the file says the
                        // sidecar is about other bytes.
                        sc.retain(|s| s.md5.as_deref().is_none_or(|m| m == d.md5));
                        if !sc.is_empty() {
                            out.push((f, format!("blake3:{}", d.blake3), sc));
                        }
                    }
                }
                out
            })
            .await
            .map_err(e)?;
        let (mut matched, mut unmatched) = (0, 0);
        let mut drafts = Vec::new();
        for (file, content, sidecars) in found {
            let items = reads::items_with(conn, &content).await.map_err(e)?;
            if items.is_empty() {
                unmatched += 1;
                continue;
            }
            matched += 1;
            for it in items {
                for sc in &sidecars {
                    let source = sc
                        .source_ref
                        .clone()
                        .unwrap_or_else(|| file.display().to_string());
                    for v in &sc.values {
                        let tag = v.field == "tag";
                        drafts.push(Draft {
                            kind: if tag {
                                CurationKind::Tag
                            } else {
                                CurationKind::Property
                            },
                            target: Some(Target {
                                item: it.item.clone(),
                                ..Target::default()
                            }),
                            key: Key::Text(if tag {
                                v.value.clone()
                            } else {
                                v.field.clone()
                            }),
                            value: (!tag).then(|| Value::String(v.value.clone())),
                            origin: ValueOrigin::Imported,
                            cites: Some(source.clone()),
                            revises: Vec::new(),
                            withdraws: Vec::new(),
                        });
                    }
                }
            }
        }
        let caller = Caller {
            author: actor.to_owned(),
            via: "import".to_owned(),
        };
        let (mut written, mut already, mut requests) = (0, 0, Vec::new());
        let mut remaining = drafts;
        while !remaining.is_empty() {
            let rest = remaining.split_off(remaining.len().min(IMPORT_REQUEST_RECORDS));
            let _planning = self.writer.lock_curation().await;
            let at = Timestamp::now();
            let planned = plan::plan_drafts(conn, remaining, &caller, at)
                .await
                .map_err(e)?;
            written += planned.records.len();
            already += planned
                .placed
                .iter()
                .filter(|p| matches!(p, plan::Placed::Already { .. }))
                .count();
            if !planned.records.is_empty() {
                let answer = self
                    .store_planned(conn, planned, actor, at, "import")
                    .await?;
                requests.push(answer["request"].clone());
            }
            remaining = rest;
        }
        Ok(json!({
            "files_with_sidecars_matched": matched,
            "files_with_sidecars_not_in_library": unmatched,
            "values_imported": written,
            "values_already_imported": already,
            "requests": requests,
        }))
    }
}

fn media_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let p = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') {
            continue;
        }
        match entry.file_type() {
            Ok(t) if t.is_dir() => media_files(&p, out),
            Ok(t) if t.is_file() && !name.ends_with(".json") && !name.ends_with(".txt") => {
                out.push(p)
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests;
