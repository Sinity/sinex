use super::*;

#[devkit::test]
fn ten_thousand_field_values_share_one_paged_read() {
    let windows = (0..10_000)
        .map(|index| Window {
            range: None,
            unknown_times: false,
            intervals: vec![],
            kinds: vec![Kind {
                event_type: "synthetic.reading".into(),
                contains: serde_json::json!({"item": format!("item-{index}")}),
            }],
            start: Some(OffsetDateTime::UNIX_EPOCH),
            end: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::days(1)),
            before: vec![],
            after: vec![],
        })
        .collect();
    let reader = Windows::new(windows, 100, 4096);
    assert_eq!(reader.values.len(), 1);
    assert_eq!(reader.values[0].open.len(), 10_000);
    assert!(reader.groups.is_empty());
    assert!(reader.alone.is_empty());
}

#[devkit::test]
async fn input_provenance_keeps_parser_versions_and_missing_manifests(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = crate::AdmissionContext::connect(&db.url(None))
        .await
        .unwrap();
    let run = crate::runs::start(
        &mut conn,
        &crate::runs::Manifest {
            name: "source runner",
            kind: "source",
            version: "1",
            build: "synthetic-build",
            emits: &["command.executed"],
        },
        "synthetic-host",
    )
    .await
    .unwrap();
    let hash = format!("blake3:{}", "0".repeat(64));
    sqlx::query!(
        "INSERT INTO core.blobs (hash,size,status) VALUES ($1,0,'present')",
        hash
    )
    .execute(&mut *conn)
    .await
    .unwrap();
    let mut selected=Vec::new();
    for (index,command) in ["pwd","ls"].into_iter().enumerate() {
        let material=sinex_model::MaterialId::new();
        sqlx::query!("INSERT INTO raw.materials (id,blob,length,manifest,parser,parser_version) VALUES ($1,$2,0,'{}','atuin-history',$3)",material.0,hash,i32::try_from(index+1).unwrap())
            .execute(&mut *conn).await.unwrap();
        let at=sinex_model::Timestamp::from_unix_secs(1_800_000_000+i64::try_from(index).unwrap()).unwrap();
        let observed=(sinex_model::event_types::find("command.executed").unwrap().build)(
            &serde_json::json!({"native_id":format!("provenance-{index}"),"command":command,"cwd":"/work","session":"synthetic","hostname":"synthetic-host","started_at":at,"duration_ns":null,"exit_code":null,"deleted_at":null,"author":null,"intent":null,"shell":null}),
            "terminal.atuin",sinex_model::Origin::Record{location:sinex_model::RecordLocation{material,anchor:0},output_index:0},sinex_model::ReadingOrder(1),[1;32],sinex_model::Class::Open,
        ).unwrap();
        let fused=(sinex_model::event_types::find("command.fused").unwrap().build)(
            &serde_json::json!({"record":format!("command.executed:{}",observed.id),"command":command,"host":"synthetic-host","started_at":at,"cwd":"/work","session":"synthetic","duration_ns":null,"exit_code":null,"observed_by":["terminal.atuin"]}),
            "derive.shell-fusion",sinex_model::Origin::Derived{parents:vec![observed.id]},sinex_model::ReadingOrder(1),[2;32],sinex_model::Class::Open,
        ).unwrap();
        selected.push(fused.id);
        // The source manifest belongs to the immutable reading on first append;
        // the derived reading deliberately has no run manifest.
        for (event,event_run) in [(observed,run),(fused,sinex_model::RunId::new())] {
            let mut tx=conn.begin().await.unwrap();
            tx.admit(&sinex_model::Batch{run:event_run,kind:sinex_model::BatchKind::Api{actor:"synthetic".into()},records:vec![sinex_model::RecordOutcome::Events{location:None,events:vec![event]}],archive:vec![]},&crate::NoDerivations).await.unwrap();
            tx.commit().await.unwrap();
        }
    }
    let versions = producer_versions(&mut conn, &selected).await.unwrap();
    let rows = versions.as_array().unwrap();
    let readings: Vec<_> = rows
        .iter()
        .filter(|row| row["producer"] == "terminal.atuin")
        .collect();
    assert_eq!(readings.len(), 2);
    assert_eq!(readings[0]["parser_version"], 1);
    assert_eq!(readings[1]["parser_version"], 2);
    assert!(
        readings
            .iter()
            .all(|row| row["manifest"] == "source runner" && row["build"] == "synthetic-build")
    );
    let unknown = rows
        .iter()
        .find(|row| row["producer"] == "derive.shell-fusion")
        .unwrap();
    assert!(unknown["version"].is_null());
    assert!(unknown["build"].is_null());
    assert_eq!(unknown["events"], 2);
}
