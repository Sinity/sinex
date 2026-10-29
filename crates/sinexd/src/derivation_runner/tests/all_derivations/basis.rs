//! Validate each resume pack's physical position before cross-history comparison.
use super::*;
use sinex_derivations::work::ContextPacks;
use sinex_model::EventId;
use sinex_model::query::Context;
use sinex_store::query::{self, Page};

/// Reconstruct the complete declared owner selection AS OF the retained P.
/// This fixture has no policy changes; the compiler applies the current policy
/// and the historical admission/archive lifecycle. ExactInputs supplies actual
/// stored Events, never fabricated metadata or a raw-payload fallback.
pub(super) async fn validate(w: &World) {
    let mut conn = PgConnection::connect(&w.url).await.unwrap();
    let read = ReadMark::now(&mut conn).await.unwrap();
    // Internal ContextPacks reads the private transformed projection, rather
    // than a public caller's narrower grants. This synthetic owner scope sees
    // all fixture classes for the independent compiler reconstruction.
    query::set_grants(&mut conn, "guarded,sealed")
        .await
        .unwrap();
    let retained: Vec<(Value, Vec<uuid::Uuid>, String, i64)> = sqlx::query_as(
        "SELECT e.payload,e.parents,e.class,e.commit_seq FROM core.derivation_events e
         WHERE e.source='context.packs' AND e.event_type='context.pack'
           AND EXISTS(SELECT 1 FROM core.live_equivalence l WHERE l.event_id=e.id)",
    )
    .fetch_all(&mut conn)
    .await
    .unwrap();
    for (payload, parents, class, admitted_at) in retained {
        let env = Env::at(
            at(86_400 * 5),
            TimeSettings::new("Europe/Warsaw", 4, 0).expect("fixture day setting"),
        );
        assert_eq!(payload["evaluated_at"], json!(env.now));
        assert_eq!(payload["day_setting"], json!(env.time_settings.label()));
        let position = payload["read_as"]
            .as_i64()
            .expect("retained physical read position");
        assert!(
            position >= 0 && position < admitted_at,
            "the reconstructed read must precede this pack reading's admission"
        );
        let key = json!({"episode":payload["episode"]});
        let owner = Keyed::inputs(&ContextPacks, &key, &env).unwrap();
        // Translate only this owner's captured, unbounded known-time shape.
        // An expanded declaration must extend the oracle, never lose scope.
        assert!(owner.range.is_none() && owner.window.is_none());
        assert!(!owner.unknown_times && owner.before.is_empty() && owner.after.is_empty());
        assert!(owner.intervals.is_empty() && !owner.kinds.is_empty());
        let branches: Vec<_> = owner
            .kinds
            .iter()
            .map(|kind| {
                let mut terms = vec![format!("event_type = {}", json!(kind.event_type))];
                for (field, value) in &kind.equal {
                    assert!(field.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
                    assert!(
                        value.is_string(),
                        "this declared owner's selectors are text"
                    );
                    terms.push(format!("payload.{field} = {value}"));
                }
                format!("({})", terms.join(" AND "))
            })
            .collect();
        let question = format!(
            "events where ({}) AND NOT when:unknown as of seq {position} fields id, payload",
            branches.join(" OR "),
        );
        let ctx = Context::new(
            jiff::Timestamp::from_nanosecond(env.now.unix_ns()).unwrap(),
            jiff::tz::TimeZone::get(env.time_settings.zone_name()).unwrap(),
            jiff::civil::Time::constant(4, 0, 0, 0),
        );
        let mut page = Page {
            size: Some(64),
            cursor: None,
        };
        let mut historical = BTreeMap::<EventId, Value>::new();
        loop {
            let answer = query::run(&mut conn, &question, None, &ctx, &page)
                .await
                .unwrap();
            assert_eq!(answer.resolved.commit_seq, position);
            assert_eq!(answer.resolved.zone, env.time_settings.zone_name());
            for row in answer.rows {
                let id: EventId = serde_json::from_value(row["id"].clone()).unwrap();
                assert!(historical.insert(id, row["payload"].clone()).is_none());
            }
            page.cursor = answer.page.cursor;
            if page.cursor.is_none() {
                break;
            }
        }
        let expected_ids: std::collections::BTreeSet<_> = historical.keys().copied().collect();
        let retained_ids: std::collections::BTreeSet<_> =
            parents.iter().copied().map(EventId).collect();
        assert_eq!(
            parents.len(),
            retained_ids.len(),
            "no duplicate parent evidence"
        );
        assert_eq!(
            expected_ids, retained_ids,
            "pack {} must cite exactly the complete owner selection at {position}",
            payload["pack"]
        );
        let mut exact = derivations::ExactInputs::new(
            expected_ids.iter().copied().collect(),
            64,
            sinex_store::PAGE_BYTES,
        )
        .unwrap();
        let mut inputs = vec![];
        while let Some(page) = exact.next(&mut conn).await.unwrap() {
            inputs.extend(page.into_iter().map(|row| row.event));
        }
        assert_eq!(
            inputs.len(),
            expected_ids.len(),
            "every historic input remains readable"
        );
        for event in &inputs {
            assert!(
                owner.admits(event),
                "exact Event still satisfies the declared owner scope"
            );
            assert_eq!(
                Some(&event.payload),
                historical.get(&event.id),
                "historical compiler and actual transformed Event payload agree"
            );
        }
        let computed = Keyed::compute(&ContextPacks, &key, &inputs, &env);
        assert!(computed.error.is_none() && !computed.waiting);
        assert_eq!(computed.outputs.len(), 1);
        let mut rebuilt = computed.outputs.into_iter().next().unwrap();
        let Origin::Derived {
            parents: rebuilt_parents,
        } = &rebuilt.origin
        else {
            panic!("resume pack is derived")
        };
        assert_eq!(
            rebuilt_parents
                .iter()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            expected_ids
        );
        assert_eq!(serde_json::to_value(rebuilt.class).unwrap(), json!(class));
        // Reproduce only the declared stored metadata after independently
        // proving the whole historical input set and deterministic product.
        rebuilt.payload["read_as"] = json!(position);
        rebuilt.payload["producer_versions"] = derivations::producer_versions(
            &mut conn,
            &expected_ids.iter().copied().collect::<Vec<_>>(),
        )
        .await
        .unwrap();
        assert_eq!(
            rebuilt.payload, payload,
            "the exact owner basis reproduces every pack field"
        );
    }
    assert_eq!(
        ReadMark::now(&mut conn).await.unwrap(),
        read,
        "the independent reconstruction saw one quiescent lifecycle/policy basis"
    );
}
