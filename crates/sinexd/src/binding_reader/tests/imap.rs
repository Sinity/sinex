use super::*;
use sinex_sources::{drivers, sources::mail_imap};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Mutex;

const FIRST: &str = "Message-ID: <one@fixture>\r\nSubject: first\r\n\r\nfirst retained body\r\n";
const SECOND: &str = "Message-ID: <two@fixture>\r\nSubject: second\r\n\r\nsecond retained body\r\n";

struct Mailbox {
    messages: Vec<(u32, &'static str)>,
    validity: u32,
    next: u32,
}
struct Server {
    port: u16,
    mailbox: Arc<Mutex<Mailbox>>,
    fetches: Arc<Mutex<Vec<(u32, u32)>>>,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Server {
    fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let mailbox = Arc::new(Mutex::new(Mailbox {
            messages: vec![(4, FIRST), (7, SECOND)],
            validity: 99,
            next: 201,
        }));
        let fetches = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (state, log, halted) = (mailbox.clone(), fetches.clone(), stop.clone());
        let thread = std::thread::spawn(move || {
            while !halted.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => Self::answer(stream, &state, &log),
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(std::time::Duration::from_millis(2))
                    }
                    Err(e) => panic!("synthetic IMAP accept: {e}"),
                }
            }
        });
        Self {
            port,
            mailbox,
            fetches,
            stop,
            thread: Some(thread),
        }
    }
    fn answer(mut stream: TcpStream, state: &Mutex<Mailbox>, log: &Mutex<Vec<(u32, u32)>>) {
        stream
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();
        stream.write_all(b"* OK synthetic IMAP\r\n").unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        while reader.read_line(&mut line).unwrap_or(0) > 0 {
            let (tag, command) = line.trim_end().split_once(' ').unwrap();
            let reply = if command.starts_with("LOGIN ") {
                format!("{tag} OK authenticated\r\n")
            } else if command.starts_with("SELECT ") {
                let state = state.lock().unwrap();
                format!(
                    "* {} EXISTS\r\n* OK [UIDVALIDITY {}] epoch\r\n* OK [UIDNEXT {}] next\r\n{tag} OK selected\r\n",
                    state.messages.len(),
                    state.validity,
                    state.next
                )
            } else if let Some(range) = command.strip_prefix("UID FETCH ") {
                let (start, end) = range
                    .split_whitespace()
                    .next()
                    .unwrap()
                    .split_once(':')
                    .unwrap();
                let (start, end) = (start.parse::<u32>().unwrap(), end.parse::<u32>().unwrap());
                log.lock().unwrap().push((start, end));
                let state = state.lock().unwrap();
                let mut out = String::new();
                for (index, (uid, body)) in state.messages.iter().enumerate() {
                    if *uid >= start && *uid <= end {
                        out.push_str(&format!(
                            "* {} FETCH (UID {uid} BODY[] {{{}}}\r\n{body})\r\n",
                            index + 1,
                            body.len()
                        ));
                    }
                }
                out + &format!("{tag} OK fetched\r\n")
            } else if command == "LOGOUT" {
                stream
                    .write_all(format!("* BYE\r\n{tag} OK bye\r\n").as_bytes())
                    .unwrap();
                break;
            } else {
                format!("{tag} BAD unsupported\r\n")
            };
            stream.write_all(reply.as_bytes()).unwrap();
            line.clear();
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}

#[tokio::test]
async fn restart_finishes_retained_imap_answer_before_fetching_an_expunged_mailbox() {
    restart_case(Scenario::Current).await;
}
#[tokio::test]
async fn legacy_imap_answer_resumes_only_its_proven_prefix_and_rechecks_sparse_holes() {
    restart_case(Scenario::Legacy).await;
}
#[tokio::test]
async fn retained_imap_answer_finishes_before_uidvalidity_renumbering() {
    restart_case(Scenario::Renumbered).await;
}

#[tokio::test]
async fn locked_retained_imap_answer_refuses_before_contacting_the_mailbox() {
    restart_case(Scenario::Locked).await;
}
#[derive(Clone, Copy)]
enum Scenario {
    Current,
    Legacy,
    Renumbered,
    Locked,
}
async fn restart_case(scenario: Scenario) {
    let legacy = matches!(scenario, Scenario::Legacy);
    let renumbered = matches!(scenario, Scenario::Renumbered);
    let locked = matches!(scenario, Scenario::Locked);
    let (_db, url, _) = crate::testdb::fresh().await;
    let dir = tempfile::tempdir().unwrap();
    let server = Server::new();
    let password = dir.path().join("password");
    std::fs::write(&password, "synthetic").unwrap();
    let config = dir.path().join("imap.json");
    std::fs::write(
        &config,
        serde_json::to_vec(&mail_imap::ImapConfig {
            host: "127.0.0.1".into(),
            port: server.port,
            username: "synthetic".into(),
            password_file: Some(password),
            token_file: None,
            mailbox: "INBOX".into(),
            tls: false,
            batch: 200,
            idle_seconds: 0,
        })
        .unwrap(),
    )
    .unwrap();
    let binding = (mail_imap::KIND.plan.unwrap())(&config, &Settings::new("synthetic"))
        .unwrap()
        .remove(0);
    let mut first = Bound::new(&mail_imap::KIND, binding.clone()).unwrap();
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let mut acquired = first.reader.poll(&Value::Null).unwrap();
    if legacy {
        let material = acquired.material.as_mut().unwrap();
        material
            .manifest
            .as_object_mut()
            .unwrap()
            .remove("imap_resume");
        let cursor_at = std::mem::replace(&mut material.cursor_at, Box::new(|_| unreachable!()));
        material.cursor_at = Box::new(move |end| {
            let mut value = cursor_at(end)?;
            value.as_object_mut().unwrap().remove("through");
            Ok(value)
        });
    }
    let material = acquired.material.as_ref().unwrap().id;
    let sink = Sink(ctx.blobs.clone());
    let identity = sinex_store::sealed::Identity::generate();
    let sealing = crate::one_shot::SealingSink::new(ctx.blobs.clone(), identity.to_public());
    let material_sink: &dyn drivers::MaterialSink = if locked { &sealing } else { &sink };
    let prepared = drivers::prepare(
        first.source.as_ref(),
        &binding.name,
        RunId::new(),
        acquired,
        Value::Null,
        material_sink,
        sinex_sources::Limits {
            max_events: 1,
            max_bytes: usize::MAX,
        },
    )
    .unwrap();
    let mut batches = prepared.batches().unwrap();
    let mut first_batch = batches.next().unwrap().unwrap();
    if locked {
        sealing
            .seal(std::slice::from_mut(&mut first_batch))
            .unwrap();
    }
    ctx.writer
        .submit(crate::writer::Priority::Bulk, first_batch)
        .await
        .unwrap();
    drop(batches);
    drop(prepared);
    drop(first);
    let first_id: uuid::Uuid =
        sqlx::query_scalar("SELECT id FROM core.events WHERE event_type='mail.message'")
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(server.fetches.lock().unwrap().len(), 1);
    ctx.close().await.unwrap();
    // The server retains UIDNEXT: expunging a message never reuses its UID.
    {
        let mut mailbox = server.mailbox.lock().unwrap();
        mailbox.messages = vec![(4, FIRST)];
        if renumbered {
            mailbox.validity = 200;
            mailbox.next = 2;
            mailbox.messages = vec![(
                1,
                "Message-ID: <third@fixture>\r\nSubject: third\r\n\r\nnew epoch body\r\n",
            )];
        }
    }
    let mut ctx = OneShot::open(&url, &dir.path().join("store"))
        .await
        .unwrap();
    let sink = Sink(ctx.blobs.clone());
    let mut resumed = [Bound::new(&mail_imap::KIND, binding.clone()).unwrap()];
    let (_stop, stop) = watch::channel(false);
    let result = read(
        &mut ctx.read_conn,
        &ctx.blobs,
        &ctx.writer,
        RunId::new(),
        &sink,
        &mut resumed,
        stop,
        Access::default(),
    )
    .await;
    let outcome = result.into_iter().next().unwrap().1;
    if locked {
        let error = outcome.err().expect("locked retained bytes must refuse");
        assert!(
            error.contains("sealed") || error.contains("authority"),
            "{error}"
        );
        assert_eq!(
            server.fetches.lock().unwrap().len(),
            1,
            "locked bytes never permit refetch"
        );
        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='mail.message'")
                .fetch_one(&mut ctx.read_conn)
                .await
                .unwrap();
        assert_eq!(count, 1);
        ctx.close().await.unwrap();
        return;
    }
    outcome.unwrap();
    let rows: Vec<(uuid::Uuid,uuid::Uuid,i64)> = sqlx::query_as(
        "SELECT id,material_id,anchor FROM core.events WHERE event_type='mail.message' AND material_id=$1 ORDER BY anchor")
        .bind(material.0).fetch_all(&mut ctx.read_conn).await.unwrap();
    assert_eq!(
        rows.len(),
        2,
        "the expunged message must come from retained bytes"
    );
    assert_eq!(
        rows[0],
        (first_id, material.0, 0),
        "the first admitted reading must remain unchanged"
    );
    assert_eq!(rows[1].1, material.0);
    assert_eq!(rows[1].2, FIRST.len() as i64);
    assert_eq!(
        server.fetches.lock().unwrap()[1].0,
        if renumbered {
            1
        } else if legacy {
            8
        } else {
            201
        },
        "server refetch starts only after the retained final cursor"
    );
    let final_cursor = positions::load(&mut ctx.read_conn, &binding.name)
        .await
        .unwrap()
        .unwrap()
        .0;
    assert_eq!(
        final_cursor["uid_validity"],
        if renumbered { 200 } else { 99 }
    );
    let count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='mail.message'")
            .fetch_one(&mut ctx.read_conn)
            .await
            .unwrap();
    assert_eq!(count, if renumbered { 3 } else { 2 });
    let reference = sinex_store::material_bytes::reference(&mut ctx.read_conn, material)
        .await
        .unwrap()
        .unwrap();
    let body = ctx.blobs.read(&reference.blob).unwrap();
    assert_eq!(&body[FIRST.len()..], SECOND.as_bytes());
    ctx.close().await.unwrap();
}
