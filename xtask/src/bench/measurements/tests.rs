use super::*;

#[devkit::test]
fn nats_is_reaped_when_a_measurement_returns_an_error() {
    let mut child_id = None;
    let measured = (|| -> Result<(), String> {
        let child = Command::new("nats-server")
            .args(["-a", "127.0.0.1", "-p", &free_port()?.to_string()])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .map_err(|e| e.to_string())?;
        child_id = Some(child.id());
        let _nats = Process(child, None);
        Err("measurement failed".into())
    })();
    assert_eq!(measured.unwrap_err(), "measurement failed");
    assert!(!Path::new(&format!("/proc/{}", child_id.unwrap())).exists());
}

#[devkit::test]
fn query_probes_keep_results_across_search_upgrade() {
    let root = env::temp_dir().join(format!("qp-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let mut cluster =
        Cluster::start(&root.join("pg"), true, &["shared_buffers=16MB".into()]).unwrap();
    cluster
        .psql(
            "CREATE ROLE sinex_reader; CREATE SCHEMA core;
             CREATE TABLE core.search_text(event_id uuid PRIMARY KEY,text text NOT NULL);
             INSERT INTO core.search_text VALUES
               ('00000000-0000-0000-0000-000000000001','echo synthetic 12345'),
               ('00000000-0000-0000-0000-000000000002','synthetic café record 12345 ');
             CREATE VIEW core.visible_search_text AS SELECT * FROM core.search_text;
             GRANT USAGE ON SCHEMA core TO sinex_reader;
             GRANT SELECT ON core.visible_search_text TO sinex_reader;",
        )
        .unwrap();
    let before = query_probes(&cluster, &root).unwrap();
    assert_eq!(before["partial_index"], false);
    cluster
        .psql(
            "ALTER TABLE core.search_text ADD COLUMN trigram_indexed boolean NOT NULL DEFAULT true;
             UPDATE core.search_text SET trigram_indexed=false
               WHERE event_id='00000000-0000-0000-0000-000000000002';
             CREATE OR REPLACE VIEW core.visible_search_text AS SELECT * FROM core.search_text;",
        )
        .unwrap();
    let after = query_probes(&cluster, &root).unwrap();
    assert_eq!(after["partial_index"], true);
    for result in [before, after] {
        let observations = result["observations"].as_array().unwrap();
        assert_eq!(observations.len(), 4);
        for observation in observations {
            assert_eq!(observation["matches"], json!({"count": 1}));
            assert_eq!(observation["samples"].as_array().unwrap().len(), 3);
        }
    }
    cluster.stop().unwrap();
    fs::remove_dir_all(root).unwrap();
}
