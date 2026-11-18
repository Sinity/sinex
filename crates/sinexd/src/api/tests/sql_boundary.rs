//! Arbitrary SELECT expressions cannot change the SQL reader's access.
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
        let (_, reply) = surface::request_as(
            &w,
            "/v1/sql",
            READ,
            sql_request(statement, None, None, command_surface::Deadline::None),
        )
        .await;
        assert_eq!(reply["outcome"], "error", "{statement}: {reply}");
        assert_eq!(reply["error"]["kind"], "refused", "{reply}");
        assert_eq!(reply["error"]["code"], "42501", "{reply}");
        let reason = reply["error"]["message"].as_str().unwrap();
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
        "SELECT secret FROM core.store_secret",
        "SELECT state FROM core.signal_checkpoints",
    ] {
        let (_, reply) = surface::request_as(
            &w,
            "/v1/sql",
            READ,
            sql_request(statement, None, None, command_surface::Deadline::None),
        )
        .await;
        assert_eq!(reply["outcome"], "error", "{statement}: {reply}");
        assert_eq!(reply["error"]["kind"], "refused", "{reply}");
        assert_eq!(reply["error"]["code"], "42501", "{reply}");
        assert!(
            reply["error"]["message"]
                .as_str()
                .unwrap()
                .contains("permission denied"),
            "{reply}"
        );
    }
    let (_, reply) = surface::request_as(
        &w,
        "/v1/sql",
        READ,
        sql_request(
            "SELECT payload FROM core.visible_events ORDER BY commit_seq",
            None,
            None,
            command_surface::Deadline::None,
        ),
    )
    .await;
    assert_eq!(reply["outcome"], "ok", "{reply}");
    assert_eq!(
        reply["value"]["rows"][0]["payload"]["withheld"], "guarded",
        "{reply}"
    );
    assert_eq!(
        reply["value"]["rows"][1]["payload"]["command"], "synthetic open record",
        "{reply}"
    );
}

#[devkit::test]
async fn typed_sql_keeps_native_errors_and_read_continuation_snapshot(
    db: devkit::pg::TestDatabase,
) {
    let w = world(db).await;
    for (sql, code) in [("SELECT FROM", "42601"), ("SELECT 1 / 0", "22012")] {
        let (_, reply) = surface::request_as(
            &w,
            "/v1/sql",
            READ,
            sql_request(sql, None, None, command_surface::Deadline::None),
        )
        .await;
        assert_eq!(reply["error"]["kind"], "internal", "{reply}");
        assert_eq!(reply["error"]["code"], code, "{reply}");
    }
    admit(
        &w,
        &["synthetic first", "synthetic second", "synthetic third"],
        0,
    )
    .await;
    let statement =
        "SELECT payload->>'command' AS command FROM core.visible_events ORDER BY commit_seq";
    let mut request = sql_request(statement, Some(2), None, command_surface::Deadline::None);
    request["format"] = json!("json");
    request["budget_tokens"] = json!(317);
    let (_, first) = surface::request_as(&w, "/v1/sql", READ, request.clone()).await;
    assert_eq!(
        first["value"]["rows"],
        json!([{"command":"synthetic first"},{"command":"synthetic second"}]),
        "{first}"
    );
    let next = first["next"][0]["request"].clone();
    assert_eq!(next["operation"], "sql");
    assert_eq!(next["arguments"], request["arguments"]);
    for option in ["deadline", "format", "budget_tokens"] {
        assert_eq!(next[option], request[option], "{option}: {next}");
    }
    assert_ne!(next["request_id"], request["request_id"]);
    assert_eq!(next["cursor"], first["value"]["page"]["cursor"]);
    admit(&w, &["synthetic later"], 10).await;
    let (_, last) = surface::request_as(&w, "/v1/sql", READ, next).await;
    assert_eq!(
        last["value"]["rows"],
        json!([{"command":"synthetic third"}]),
        "{last}"
    );
    assert!(
        last.get("next")
            .is_none_or(|next| next.as_array().unwrap().is_empty()),
        "{last}"
    );
}
