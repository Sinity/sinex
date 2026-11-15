//! The current-state snapshot a consumer installs before following a stream
//! that starts at a current-state origin. Pages are read at one commit
//! position; the cursor is plain data, and each page recomputes what the
//! cursor states from current grants, the broker and the tables.
use super::*;
use crate::{Error, Result};
use sinex_protocol::{
    LastStreamMessage, SnapshotComplete, SnapshotCursor, SnapshotEvent, SnapshotNext,
    SnapshotReading, SnapshotReply, WithheldReason,
};
use sinex_store::snapshot::Selection;

const PAGE: i64 = 256;

/// What a producer's grants select for its snapshot.
struct Scope {
    scope: String,
    catalog: String,
    kinds: Vec<(sinex_model::KindRef, String)>,
    names: Vec<String>,
    classes: Vec<String>,
}

fn scope(grant: &fetch::Grants, producer: &str, subjects: &Subjects) -> Result<Scope> {
    let catalog = grant
        .indexed_catalog
        .as_ref()
        .ok_or_else(|| Error::refused("snapshot_catalog_not_installed"))?;
    let (_, scope) = grant.consumer_scope(producer, subjects);
    let mut kinds = catalog
        .kinds
        .iter()
        .filter(|kind| write_access::matches(&grant.kind_prefixes, &kind.kind.name))
        .map(|kind| (kind.kind.clone(), kind.definition_hash()))
        .collect::<Vec<_>>();
    kinds.sort();
    kinds.dedup();
    let mut names = kinds
        .iter()
        .map(|(kind, _)| kind.name.clone())
        .collect::<Vec<_>>();
    names.dedup();
    let mut classes = grant
        .classes
        .iter()
        .map(|class| {
            serde_json::to_value(class)
                .expect("classes serialize")
                .as_str()
                .expect("class string")
                .to_owned()
        })
        .collect::<Vec<_>>();
    classes.sort();
    classes.dedup();
    Ok(Scope {
        scope,
        catalog: catalog.fingerprint(),
        kinds,
        names,
        classes,
    })
}

enum Failure {
    Restart(String),
    Unavailable(String),
}
impl<E: std::fmt::Display> From<E> for Failure {
    fn from(error: E) -> Self {
        Self::Unavailable(error.to_string())
    }
}

impl Requests {
    pub(super) async fn snapshot(
        &self,
        producer: &str,
        cursor: Option<SnapshotCursor>,
    ) -> SnapshotReply {
        match self.snapshot_page(producer, cursor).await {
            Ok(reply) => reply,
            Err(Failure::Restart(reason)) => SnapshotReply::Restart { reason },
            Err(Failure::Unavailable(reason)) => SnapshotReply::Unavailable { reason },
        }
    }

    async fn snapshot_page(
        &self,
        producer: &str,
        cursor: Option<SnapshotCursor>,
    ) -> std::result::Result<SnapshotReply, Failure> {
        let grant = self
            .grants(producer)
            .map_err(|refusal| Failure::Restart(refusal.reason))?;
        let current = scope(grant, producer, &self.subjects)
            .map_err(|error| Failure::Restart(error.to_string()))?;
        let stream = crate::publisher::context(self.client.clone())
            .get_stream(self.subjects.confirmed_stream())
            .await?;
        let instance = crate::publisher::stream_instance(&stream);
        // The last stream message is read before the database read starts, so
        // the read covers every event it delivered.
        let last: Option<Option<LastStreamMessage>> = if cursor.is_none() {
            Some(super::stream_messages::last(&stream).await?)
        } else {
            None
        };
        let mut connection = self.reads.acquire().await?;
        use sqlx::Connection;
        let mut tx = connection.begin().await?;
        sinex_store::query::read_one_snapshot(&mut tx).await?;
        let mut cursor = match cursor {
            Some(cursor) => {
                if cursor.scope != current.scope {
                    return Err(Failure::Restart("snapshot_scope_changed".into()));
                }
                if cursor.catalog != current.catalog {
                    return Err(Failure::Restart("snapshot_catalog_changed".into()));
                }
                if cursor.stream_instance != instance {
                    return Err(Failure::Restart("snapshot_stream_instance_changed".into()));
                }
                if cursor.position < 0
                    || cursor.position > sinex_store::query::current_seq(&mut tx).await?
                {
                    return Err(Failure::Restart("snapshot_position_outside_history".into()));
                }
                if sinex_store::snapshot::tombstoned_since(&mut tx, cursor.position, &current.names)
                    .await?
                {
                    return Err(Failure::Restart("snapshot_reading_tombstoned".into()));
                }
                cursor
            }
            None => SnapshotCursor {
                position: sinex_store::query::current_seq(&mut tx).await?,
                scope: current.scope.clone(),
                catalog: current.catalog.clone(),
                stream_instance: instance,
                last: last.expect("a new snapshot reads the last stream message"),
                after: None,
            },
        };
        if !sinex_store::snapshot::kinds_registered(&mut tx, &current.kinds).await? {
            return Err(Failure::Restart("snapshot_kind_catalog_changed".into()));
        }
        let mut readings = sinex_store::snapshot::readings(
            &mut tx,
            &Selection {
                position: cursor.position,
                kinds: &current.names,
                classes: &current.classes,
            },
            cursor.after.map(|(position, event)| (position, event.0)),
            PAGE + 1,
        )
        .await?;
        let more = readings.len() > PAGE as usize;
        if more {
            readings.pop();
        }
        if readings
            .iter()
            .any(|row| row.history_unknown || row.archived && row.lifecycle_position.is_none())
        {
            return Err(Failure::Restart(
                "snapshot_archived_position_unknown".into(),
            ));
        }
        // The consumer's grants decide which classes arrive complete; a
        // sealed reading needs an operator unlock and is fetched by ID.
        let granted = current
            .classes
            .iter()
            .filter(|class| class.as_str() != "sealed")
            .cloned()
            .collect::<Vec<_>>()
            .join(",");
        sinex_store::query::set_grants(&mut tx, &granted).await?;
        let complete = readings
            .iter()
            .filter(|row| row.class != "sealed")
            .map(|row| row.id)
            .collect::<Vec<_>>();
        let rows = sinex_store::confirmed::rows(&mut tx, &complete).await?;
        let mut built: std::collections::BTreeMap<_, _> =
            crate::confirmed_event::build(&mut tx, rows)
                .await?
                .into_iter()
                .map(|event| (event.id.0, event))
                .collect();
        let mut events = Vec::with_capacity(readings.len());
        for row in &readings {
            let event = match built.remove(&row.id) {
                Some(event) => SnapshotReading::Inline(Box::new(event)),
                None => {
                    let header = sinex_store::producer_reads::event_output(&mut tx, row.id)
                        .await?
                        .ok_or_else(|| Failure::Restart("snapshot_reading_tombstoned".into()))?;
                    SnapshotReading::Withheld(crate::confirmed_event::withheld(
                        row.commit_seq,
                        row.id,
                        header.kind,
                        &row.class,
                        WithheldReason::Class,
                    )?)
                }
            };
            events.push(SnapshotEvent {
                event,
                archived: row.archived,
                lifecycle_position: row.lifecycle_position,
            });
        }
        cursor.after = readings
            .last()
            .map(|row| (row.commit_seq, sinex_model::EventId(row.id)))
            .or(cursor.after);
        tx.rollback().await?;
        let next = if more {
            SnapshotNext::More { cursor }
        } else {
            SnapshotNext::Complete {
                complete: SnapshotComplete {
                    position: cursor.position,
                    scope: cursor.scope,
                    stream_instance: cursor.stream_instance,
                    last: cursor.last,
                },
            }
        };
        Ok(SnapshotReply::Page { events, next })
    }
}
