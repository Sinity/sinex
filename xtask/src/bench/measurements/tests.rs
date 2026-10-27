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
fn readiness_waits_for_the_termination_handler() {
    use std::io::{BufRead, Write as _};

    let child = Command::new("python3")
        .args([
            "-c",
            "import signal, sys\nsignal.signal(signal.SIGTERM, signal.SIG_DFL)\nprint('starting', flush=True)\nsys.stdin.readline()\nsignal.signal(signal.SIGTERM, lambda *_: sys.exit(0))\nprint('ready', flush=True)\nsignal.pause()\n",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = Process(child, None);
    let mut output = std::io::BufReader::new(child.0.stdout.take().unwrap());
    let status = PathBuf::from(format!("/proc/{}/status", child.0.id()));
    let mut line = String::new();
    output.read_line(&mut line).unwrap();
    assert_eq!(line, "starting\n");
    assert!(!handles_termination(&fs::read_to_string(&status).unwrap()));
    child
        .0
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"continue\n")
        .unwrap();
    line.clear();
    output.read_line(&mut line).unwrap();
    assert_eq!(line, "ready\n");
    assert!(handles_termination(&fs::read_to_string(&status).unwrap()));
    assert!(child.stop().unwrap().success());
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

#[devkit::test]
fn publisher_completion_waits_for_archive_notices() {
    let root = env::temp_dir().join(format!("pa-{}", std::process::id()));
    fs::create_dir(&root).unwrap();
    let mut cluster =
        Cluster::start(&root.join("pg"), true, &["shared_buffers=16MB".into()]).unwrap();
    cluster
        .psql(
            "CREATE SCHEMA core; CREATE SCHEMA audit;
             CREATE TABLE core.source_counts(events bigint NOT NULL);
             CREATE TABLE core.counters(name text PRIMARY KEY,value bigint NOT NULL);
             CREATE TABLE core.consumer_positions(consumer text PRIMARY KEY,commit_seq bigint NOT NULL,change_seq bigint NOT NULL);
             CREATE TABLE audit.archive_changes(change_seq bigint PRIMARY KEY);
             INSERT INTO core.source_counts VALUES(17);
             INSERT INTO core.counters VALUES('commit_seq',11);
             INSERT INTO core.consumer_positions VALUES('read.search_text',11,0),('publisher',11,1);
             INSERT INTO audit.archive_changes VALUES(1),(7);",
        )
        .unwrap();
    let positions = consumer_positions(&cluster).unwrap();
    assert_eq!(positions["publisher"], positions["final_commit"]);
    assert_eq!(positions["publisher_change"], 1);
    assert_eq!(positions["final_change"], 7);
    assert!(!publisher_caught_up(&positions));
    cluster
        .psql("UPDATE core.consumer_positions SET change_seq=7 WHERE consumer='publisher'")
        .unwrap();
    assert!(publisher_caught_up(&consumer_positions(&cluster).unwrap()));
    cluster
        .psql("DELETE FROM core.consumer_positions WHERE consumer='publisher'")
        .unwrap();
    assert!(!publisher_caught_up(&consumer_positions(&cluster).unwrap()));
    cluster.stop().unwrap();
    fs::remove_dir_all(root).unwrap();
}

#[devkit::test]
fn nats_readiness_requires_its_client_listener() {
    use std::io::BufRead as _;

    let monitoring = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let monitoring_port = monitoring.local_addr().unwrap().port();
    let server = std::thread::spawn(move || {
        for _ in 0..2 {
            let (mut stream, _) = monitoring.accept().unwrap();
            let mut request = std::io::BufReader::new(stream.try_clone().unwrap());
            loop {
                let mut line = String::new();
                request.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
            }
            std::io::Write::write_all(
                &mut stream,
                b"HTTP/1.0 200 OK\r\nContent-Length: 2\r\n\r\n{}",
            )
            .unwrap();
        }
    });
    assert!(!nats_ready(0, monitoring_port));
    let clients = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    assert!(nats_ready(
        clients.local_addr().unwrap().port(),
        monitoring_port
    ));
    server.join().unwrap();
}
