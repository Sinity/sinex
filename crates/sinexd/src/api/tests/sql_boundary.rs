//! Arbitrary SELECT expressions cannot change the SQL reader's authority.
use super::*;

#[devkit::test]
async fn sql_cannot_change_grants_or_escape_its_role_through_nested_queries(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    let mut guarded = command("synthetic guarded record", 0);
    guarded.class = Class::Guarded;
    admit_events(&w, vec![guarded, command("synthetic open record", 1)]).await;
    for statement in [
        "SELECT pg_catalog.set_config('sinex.grants','guarded',true)",
        "WITH changed AS MATERIALIZED (SELECT pg_catalog.set_config('sinex.grants','guarded',true) AS value) SELECT changed.value,e.payload FROM changed CROSS JOIN LATERAL (SELECT payload FROM core.visible_events WHERE source <> changed.value) e",
        "SELECT pg_catalog.set_config('role','none',true)",
        "SELECT pg_catalog.set_config('role','sinex_writer',true)",
        "WITH changed AS MATERIALIZED (SELECT pg_catalog.set_config('role','none',true) AS value) SELECT query_to_xml('SELECT encode(hash_key,''hex'') FROM privacy.settings',true,false,changed.value) FROM changed",
        "WITH changed AS MATERIALIZED (SELECT pg_catalog.set_config('role','sinex_writer',true) AS value) SELECT query_to_xml('SELECT payload FROM core.events',true,false,changed.value) FROM changed",
    ] {
        let (_, reply) = call(&w, Some(READ), json!({"method":"sql","sql":statement})).await;
        assert_eq!(reply["outcome"], "refused", "{statement}: {reply}");
        let reason = reply["reason"].as_str().unwrap();
        assert!(
            reason.contains("permission denied") && reason.contains("set_config"),
            "{statement}: {reply}"
        );
        assert!(!reply.to_string().contains("synthetic guarded record"));
    }
    for statement in [
        "SELECT encode(hash_key,'hex') FROM privacy.settings",
        "SELECT wrapped_key FROM privacy.data_keys",
        "SELECT payload FROM core.admission_stage",
        "SELECT settings FROM raw.bindings",
        "SELECT examples FROM derivation.comparisons",
        "SELECT secret FROM core.query_cursor_key",
        "SELECT secret FROM core.owner_protocol_key",
        "SELECT payload FROM privacy.event_outputs",
        "SELECT baseline_class FROM privacy.original_events",
        "SELECT state FROM core.signal_checkpoints",
    ] {
        let (_, reply) = call(&w, Some(READ), json!({"method":"sql","sql":statement})).await;
        assert_eq!(reply["outcome"], "refused", "{statement}: {reply}");
        assert!(
            reply["reason"]
                .as_str()
                .unwrap()
                .contains("permission denied"),
            "{reply}"
        );
    }
    let (_, reply) = call(
        &w,
        Some(READ),
        json!({"method":"sql","sql":"SELECT payload FROM core.visible_events ORDER BY commit_seq"}),
    )
    .await;
    assert_eq!(reply["outcome"], "ok", "{reply}");
    assert_eq!(
        reply["result"]["rows"][0]["payload"]["withheld"], "guarded",
        "{reply}"
    );
    assert_eq!(
        reply["result"]["rows"][1]["payload"]["command"], "synthetic open record",
        "{reply}"
    );
}
