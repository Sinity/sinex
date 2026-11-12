use super::*;
use crate::{Applied, VERSION, apply, catalog};
use sqlx::{Connection, Executor, PgConnection};

async fn previous(conn: &mut PgConnection) {
    conn.execute(
        "DROP INDEX core.ingress_frames_material, core.ingress_frames_refused_at, core.ingress_frames_malformed_producer; ALTER TABLE core.event_tombstones DROP CONSTRAINT IF EXISTS tombstone_history_complete",
    )
    .await
    .unwrap();
    conn.execute("DROP VIEW core.visible_search_trigram; DROP TABLE core.search_trigram; DROP VIEW core.visible_search_text; ALTER TABLE core.search_text ADD COLUMN trigram_indexed boolean NOT NULL DEFAULT true; CREATE INDEX search_text_trigram ON core.search_text USING gin (text gin_trgm_ops)")
        .await.unwrap();
    conn.execute(PREVIOUS_VIEW).await.unwrap();
    conn.execute("GRANT SELECT ON core.visible_search_text TO sinex_reader,sinex_writer; CREATE TABLE sinex_schema.search_trigram_rewrite(id boolean PRIMARY KEY,rewritten_through uuid); INSERT INTO sinex_schema.search_trigram_rewrite VALUES(true,uuidv7()); DELETE FROM sinex_schema.applied")
        .await.unwrap();
    sqlx::query("INSERT INTO sinex_schema.applied(version) VALUES($1)")
        .bind(crate::version_before("search_trigram_text").unwrap())
        .execute(conn)
        .await
        .unwrap();
}

async fn populate(conn: &mut PgConnection) {
    use sinex_model::{KindDeclaration, KindRef};
    let mut words = KindDeclaration::schemaless(
        KindRef {
            name: "example.words".into(),
            version: 2,
        },
        "example".into(),
    );
    words.search = Search::Words;
    let trigram = KindDeclaration::schemaless(
        KindRef {
            name: "example.trigram".into(),
            version: 2,
        },
        "example".into(),
    );
    crate::kinds::register(conn, "example", &[words, trigram])
        .await
        .unwrap();
    conn.execute("INSERT INTO core.events(id,event_type,source,run_id,material_id,anchor,ts_quality,commit_seq,payload,reading_order,record_hash,class,kind_version)
      SELECT uuidv7(),(ARRAY['command.imported','journal.entry','lake.observed','example.words','example.trigram'])[1+n%5],
       'synthetic',uuidv7(),uuidv7(),0,'unknown',n,'{}',1,decode(repeat('01',32),'hex'),
       CASE WHEN n%2=0 THEN 'guarded' ELSE 'open' END, CASE WHEN n%5 IN (3,4) THEN 2 ELSE 1 END
      FROM generate_series(1,25) n;
      INSERT INTO core.search_text(event_id,text) SELECT id,'cargo test '||commit_seq FROM core.events;
      INSERT INTO core.search_text(event_id,text) VALUES(uuidv7(),'orphan retained')").await.unwrap();
}

async fn answers(conn: &mut PgConnection) -> Vec<String> {
    sqlx::query_scalar("SELECT s.event_id::text FROM core.visible_search_text s WHERE s.text ~ '(^| )car' AND s.tsv @@ plainto_tsquery('simple','cargo') ORDER BY 1")
        .fetch_all(conn).await.unwrap()
}

#[devkit::test]
async fn bulk_copy_handles_every_predecessor_index_state_without_rewriting_words(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    populate(&mut conn).await;
    let fresh = catalog(&mut conn).await.unwrap();
    for state in 0..4 {
        previous(&mut conn).await;
        if state == 1 {
            assert!(conn.execute("CREATE UNIQUE INDEX CONCURRENTLY search_text_trigram_partial ON core.search_text((left(text,5)))").await.is_err());
        } else if state >= 2 {
            conn.execute("CREATE INDEX search_text_trigram_partial ON core.search_text USING gin(text gin_trgm_ops) WHERE trigram_indexed").await.unwrap();
            if state == 3 {
                conn.execute("DROP INDEX core.search_text_trigram")
                    .await
                    .unwrap();
            }
        }
        let before: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT event_id::text,text,ctid::text FROM core.search_text ORDER BY event_id",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        let visible = answers(&mut conn).await;
        assert_eq!(
            apply(&mut conn).await.unwrap(),
            Applied::Upgraded {
                from: crate::version_before("search_trigram_text").unwrap(),
                to: VERSION
            }
        );
        assert_eq!(answers(&mut conn).await, visible);
        let after: Vec<(String, String, String)> = sqlx::query_as(
            "SELECT event_id::text,text,ctid::text FROM core.search_text ORDER BY event_id",
        )
        .fetch_all(&mut conn)
        .await
        .unwrap();
        assert_eq!(
            before, after,
            "the word heap is unchanged for predecessor state {state}"
        );
        let kinds:Vec<(String,i64)>=sqlx::query_as("SELECT e.event_type,count(*) FROM core.search_trigram s JOIN core.events e ON e.id=s.event_id GROUP BY 1 ORDER BY 1").fetch_all(&mut conn).await.unwrap();
        assert_eq!(
            kinds,
            [
                ("command.imported".into(), 5),
                ("example.trigram".into(), 5)
            ]
        );
        assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
    }
}

#[devkit::test]
async fn a_failure_after_bulk_index_build_rolls_back_and_restarts_the_whole_step(
    db: devkit::pg::TestDatabase,
) {
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    populate(&mut conn).await;
    let fresh = catalog(&mut conn).await.unwrap();
    previous(&mut conn).await;
    conn.execute(format!("CREATE FUNCTION pg_temp.fail_search_upgrade() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.version={} THEN RAISE EXCEPTION 'synthetic version failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_search_upgrade BEFORE INSERT ON sinex_schema.applied FOR EACH ROW EXECUTE FUNCTION pg_temp.fail_search_upgrade()",VERSION).as_str()).await.unwrap();
    let before = catalog(&mut conn).await.unwrap();
    let rows: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(s) FROM core.search_text s ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert!(apply(&mut conn).await.is_err());
    assert_eq!(catalog(&mut conn).await.unwrap(), before);
    let after: Vec<serde_json::Value> =
        sqlx::query_scalar("SELECT to_jsonb(s) FROM core.search_text s ORDER BY event_id")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(after, rows);
    conn.execute("DROP TRIGGER fail_search_upgrade ON sinex_schema.applied")
        .await
        .unwrap();
    conn.close().await.unwrap();
    let mut conn = PgConnection::connect(&db.url(None)).await.unwrap();
    assert_eq!(
        apply(&mut conn).await.unwrap(),
        Applied::Upgraded {
            from: crate::version_before("search_trigram_text").unwrap(),
            to: VERSION
        }
    );
    assert_eq!(catalog(&mut conn).await.unwrap(), fresh);
}
