//! Native API proof using the actual 745bb3f4b82a88154f87db74a08a5b56c85c36b0 Python producer.

use super::{ConsumerSetup, spawn_consumer_and_wait_ready};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sinex_db::DbPoolExt;
use sinex_primitives::{
    Id, Uuid,
    nats::JetStreamTopology,
    polylogue_protocol::PolylogueRevisionManifest,
    rpc::polylogue::{PolyloguePublicationState, PolylogueRevisionPublicationResponse},
};
use sinexd::api::{ServiceContainer, config::GatewayConfig, rpc_server};
use std::{
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::mpsc,
};
use tokio_rustls::{
    TlsAcceptor,
    rustls::{
        self,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    },
};
use xtask::sandbox::prelude::*;

const TEST_NAME: &str = "actual_polylogue_source_outbox_survives_native_rpc_restart_and_settles";
const API_CHILD: &str = "SINEX_POLYLOGUE_API_CHILD";
const API_READY: &str = "SINEX_POLYLOGUE_API_READY";
const API_TOPOLOGY: &str = "SINEX_POLYLOGUE_API_TOPOLOGY";
const PRODUCER_COMMIT: &str = "745bb3f4b82a88154f87db74a08a5b56c85c36b0";
const FIXTURE_TOKEN: &str = "sinex_polylogue_fixture:write";

struct ProducerInputs {
    python: PathBuf,
    checkout: PathBuf,
    script: PathBuf,
}
impl ProducerInputs {
    fn required() -> TestResult<Self> {
        let python = PathBuf::from(std::env::var("SINEX_POLYLOGUE_PYTHON")?);
        let checkout = PathBuf::from(std::env::var("SINEX_POLYLOGUE_CHECKOUT")?);
        let script = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../tests/fixtures/polylogue/v1/source_outbox_driver.py");
        for path in [&python, &checkout, &script] {
            if !path.exists() {
                return Err(color_eyre::eyre::eyre!(
                    "required producer proof path is absent: {}",
                    path.display()
                ));
            }
        }
        let revision = std::process::Command::new("git")
            .args([
                "-C",
                checkout
                    .to_str()
                    .ok_or_else(|| color_eyre::eyre::eyre!("checkout path is not UTF-8"))?,
                "rev-parse",
                "HEAD",
            ])
            .output()?;
        if !revision.status.success()
            || String::from_utf8(revision.stdout)?.trim() != PRODUCER_COMMIT
        {
            return Err(color_eyre::eyre::eyre!(
                "producer checkout must be exact clean successor {PRODUCER_COMMIT}"
            ));
        }
        let status = std::process::Command::new("git")
            .args(["-C", checkout.to_str().unwrap(), "status", "--porcelain"])
            .output()?;
        if !status.status.success() || !status.stdout.is_empty() {
            return Err(color_eyre::eyre::eyre!("producer checkout must be clean"));
        }
        Ok(Self {
            python,
            checkout,
            script,
        })
    }

    fn spawn(
        &self,
        endpoint: &str,
        root: &Path,
        cert: &Path,
        redrive: bool,
        extra_responses: usize,
    ) -> TestResult<Child> {
        let mut command = Command::new(&self.python);
        command
            .current_dir(&self.checkout)
            .arg(&self.script)
            .args(["--endpoint", endpoint, "--root"])
            .arg(root);
        if redrive {
            command.arg("--redrive");
        }
        if extra_responses > 0 {
            command
                .arg("--extra-grok-responses")
                .arg(extra_responses.to_string());
        }
        Ok(command
            .env("PYTHONPATH", &self.checkout)
            .env("SINEX_API_TOKEN", FIXTURE_TOKEN)
            .env("SSL_CERT_FILE", cert)
            .env("HTTPS_PROXY", "")
            .env("HTTP_PROXY", "")
            .env("https_proxy", "")
            .env("http_proxy", "")
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?)
    }

    fn decode_output(output: std::process::Output) -> TestResult<Value> {
        if !output.status.success() {
            return Err(color_eyre::eyre::eyre!(
                "actual producer failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }
        let stdout = String::from_utf8(output.stdout)?;
        let line = stdout
            .lines()
            .rev()
            .find(|line| line.trim_start().starts_with('{'))
            .ok_or_else(|| color_eyre::eyre::eyre!("producer emitted no JSON result"))?;
        Ok(serde_json::from_str(line)?)
    }

    async fn run(
        &self,
        endpoint: &str,
        root: &Path,
        cert: &Path,
        redrive: bool,
    ) -> TestResult<Value> {
        Self::decode_output(
            self.spawn(endpoint, root, cert, redrive, 0)?
                .wait_with_output()
                .await?,
        )
    }
}

struct FixtureTls {
    cert_path: PathBuf,
    key_path: PathBuf,
    cert_pem: String,
    cert_der: Vec<u8>,
    key_der: Vec<u8>,
}
impl FixtureTls {
    fn create(root: &Path) -> TestResult<Self> {
        let cert =
            rcgen::generate_simple_self_signed(vec!["localhost".into(), "127.0.0.1".into()])?;
        let cert_pem = cert.cert.pem();
        let key_pem = cert.key_pair.serialize_pem();
        let cert_path = root.join("fixture-cert.pem");
        let key_path = root.join("fixture-key.pem");
        std::fs::write(&cert_path, &cert_pem)?;
        std::fs::write(&key_path, &key_pem)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(Self {
            cert_path,
            key_path,
            cert_pem,
            cert_der: cert.cert.der().to_vec(),
            key_der: cert.key_pair.serialize_der(),
        })
    }
    fn server_acceptor(&self) -> TestResult<TlsAcceptor> {
        rpc_server::ensure_rustls_crypto_provider()?;
        let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.key_der.clone()));
        let config = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(vec![CertificateDer::from(self.cert_der.clone())], key)?;
        Ok(TlsAcceptor::from(Arc::new(config)))
    }
    fn reqwest_client(&self) -> TestResult<reqwest::Client> {
        Ok(reqwest::Client::builder()
            .no_proxy()
            .add_root_certificate(reqwest::Certificate::from_pem(self.cert_pem.as_bytes())?)
            .build()?)
    }
}

#[derive(Clone)]
struct ProxyState {
    drop_first_publish: Arc<AtomicBool>,
    status_calls: Arc<AtomicUsize>,
    lost_tx: mpsc::UnboundedSender<Value>,
}
struct HttpsReplyProxy {
    endpoint: String,
    state: ProxyState,
    task: tokio::task::JoinHandle<()>,
}
impl HttpsReplyProxy {
    async fn start(
        tls: &FixtureTls,
        upstream: String,
    ) -> TestResult<(Self, mpsc::UnboundedReceiver<Value>)> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let (lost_tx, lost_rx) = mpsc::unbounded_channel();
        let state = ProxyState {
            drop_first_publish: Arc::new(AtomicBool::new(true)),
            status_calls: Arc::new(AtomicUsize::new(0)),
            lost_tx,
        };
        let acceptor = tls.server_acceptor()?;
        let client = tls.reqwest_client()?;
        let worker_state = state.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    break;
                };
                let acceptor = acceptor.clone();
                let client = client.clone();
                let state = worker_state.clone();
                let upstream = upstream.clone();
                tokio::spawn(async move {
                    let _ = proxy_one(socket, acceptor, client, state, upstream).await;
                });
            }
        });
        Ok((
            Self {
                endpoint: format!("https://127.0.0.1:{}/rpc", addr.port()),
                state,
                task,
            },
            lost_rx,
        ))
    }
    fn status_calls(&self) -> usize {
        self.state.status_calls.load(Ordering::SeqCst)
    }
    fn stop(&self) {
        self.task.abort();
    }
}
impl Drop for HttpsReplyProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn proxy_one(
    socket: TcpStream,
    acceptor: TlsAcceptor,
    client: reqwest::Client,
    state: ProxyState,
    upstream: String,
) -> TestResult<()> {
    let mut tls = acceptor.accept(socket).await?;
    let (headers, body) = read_http_request(&mut tls).await?;
    let auth = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("authorization")
                .then(|| value.trim().to_owned())
        })
        .ok_or_else(|| {
            color_eyre::eyre::eyre!("authenticated RPC request omitted Authorization")
        })?;
    let request: Value = serde_json::from_slice(&body)?;
    let method = request
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if method == "polylogue.revision.status" {
        state.status_calls.fetch_add(1, Ordering::SeqCst);
    }
    let duplicate =
        method == "polylogue.revision.publish" && state.drop_first_publish.load(Ordering::SeqCst);
    let send = || {
        client
            .post(&upstream)
            .header(reqwest::header::AUTHORIZATION, &auth)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.clone())
            .send()
    };
    let (response, second) = if duplicate {
        // Both requests enter the real controller concurrently while producer locators
        // are alive. A blocked request lock cannot consume a CAS connection.
        let (first, second) = tokio::join!(send(), send());
        (first?, Some(second?))
    } else {
        (send().await?, None)
    };
    let status = response.status();
    let bytes = response.bytes().await?;
    let decoded: Value = serde_json::from_slice(&bytes)?;
    if let Some(second) = second {
        let second: Value = second.error_for_status()?.json().await?;
        let first_receipt: PolylogueRevisionPublicationResponse =
            serde_json::from_value(decoded.get("result").cloned().ok_or_else(|| {
                color_eyre::eyre::eyre!("first concurrent publish failed: {decoded}")
            })?)?;
        let second_receipt: PolylogueRevisionPublicationResponse =
            serde_json::from_value(second.get("result").cloned().ok_or_else(|| {
                color_eyre::eyre::eyre!("second concurrent publish failed: {second}")
            })?)?;
        assert_eq!(
            identity_tuple(&first_receipt),
            identity_tuple(&second_receipt)
        );
        assert_eq!(
            first_receipt.state,
            Some(PolyloguePublicationState::RawAccepted)
        );
        assert_eq!(second_receipt.state, first_receipt.state);
    }
    let lost = method == "polylogue.revision.publish"
        && decoded.pointer("/result/state").and_then(Value::as_str) == Some("raw_accepted")
        && state
            .drop_first_publish
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok();
    if lost {
        // Evidence is the actual successful native response, observed before it
        // is discarded at the client-facing TLS connection.
        let _ = state.lost_tx.send(decoded);
        return Ok(());
    }
    let reason = status.canonical_reason().unwrap_or("OK");
    let head = format!(
        "HTTP/1.1 {} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        status.as_u16(),
        reason,
        bytes.len()
    );
    tls.write_all(head.as_bytes()).await?;
    tls.write_all(&bytes).await?;
    tls.shutdown().await?;
    Ok(())
}

async fn read_http_request<R: tokio::io::AsyncRead + Unpin>(
    stream: &mut R,
) -> TestResult<(String, Vec<u8>)> {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 8192];
    let mut header_end = None;
    let mut content_len = None;
    loop {
        let count = stream.read(&mut chunk).await?;
        if count == 0 {
            return Err(color_eyre::eyre::eyre!(
                "proxy client closed before request completed"
            ));
        }
        bytes.extend_from_slice(&chunk[..count]);
        if header_end.is_none() {
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                let end = index + 4;
                let header = std::str::from_utf8(&bytes[..end])?;
                content_len = Some(
                    header
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().ok())
                                .flatten()
                        })
                        .ok_or_else(|| color_eyre::eyre::eyre!("proxy requires Content-Length"))?,
                );
                header_end = Some(end);
            } else {
                continue;
            }
        }
        if bytes.len() >= header_end.unwrap() + content_len.unwrap() {
            break;
        }
    }
    let end = header_end.unwrap();
    let body_end = end + content_len.unwrap();
    Ok((
        String::from_utf8(bytes[..end].to_vec())?,
        bytes[end..body_end].to_vec(),
    ))
}

async fn start_api_child(
    test_name: &str,
    ctx: &TestContext,
    topology: &JetStreamTopology,
    tls: &FixtureTls,
    token: &str,
    root: &Path,
    ordinal: usize,
) -> TestResult<Child> {
    let ready = root.join(format!("api-{ordinal}.ready"));
    let topo = serde_json::to_string(topology)?;
    let mut child = Command::new(std::env::current_exe()?)
        .arg(test_name)
        .arg("--exact")
        .arg("--ignored")
        .arg("--nocapture")
        .env(API_CHILD, "1")
        .env(API_READY, &ready)
        .env(API_TOPOLOGY, topo)
        .env("DATABASE_URL", ctx.database_url())
        .env("SINEX_NATS_URL", ctx.nats_handle()?.client_url())
        .env("SINEX_NAMESPACE", ctx.pipeline_namespace().prefix())
        .env("SINEX_API_TCP_LISTEN", "127.0.0.1:0")
        .env("SINEX_API_REQUIRE_CLIENT_TLS", "0")
        .env("SINEX_API_TLS_CERT", &tls.cert_path)
        .env("SINEX_API_TLS_KEY", &tls.key_path)
        .env("SINEX_API_CONTENT_STORE_PATH", root.join("native-cas"))
        .env("SINEX_STATE_DIR", root.join("native-state"))
        .env("SINEX_API_TOKEN", token)
        .env("SINEX_API_RATE_LIMIT_ENABLED", "false")
        .env("SINEX_API_POOL_MAX_CONNECTIONS", "6")
        .env_remove("SINEX_API_TLS_CLIENT_CA")
        .env_remove("SINEX_API_TOKEN_FILE")
        .env_remove("SINEX_API_ADMIN_TOKEN_FILE")
        .env_remove("SINEX_API_READONLY_TOKEN_FILE")
        .kill_on_drop(true)
        .spawn()?;
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            if tokio::fs::try_exists(&ready).await? {
                break;
            }
            if let Some(status) = child.try_wait()? {
                return Err(color_eyre::eyre::eyre!(
                    "native API child exited early: {status}"
                ));
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        Ok::<_, color_eyre::Report>(())
    })
    .await??;
    Ok(child)
}

async fn wait_child_url(root: &Path, ordinal: usize) -> TestResult<String> {
    let path = root.join(format!("api-{ordinal}.ready"));
    let address = tokio::fs::read_to_string(path).await?;
    Ok(format!("https://{}/rpc", address.trim()))
}

async fn status_rpc(
    client: &reqwest::Client,
    endpoint: &str,
    request_id: &str,
) -> TestResult<PolylogueRevisionPublicationResponse> {
    let response = client
        .post(endpoint)
        .bearer_auth(FIXTURE_TOKEN)
        .json(&json!({"jsonrpc":"2.0", "id":request_id,
            "method":"polylogue.revision.status", "params":{"request_id":request_id}}))
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?;
    if response.get("id").and_then(Value::as_str) != Some(request_id) {
        return Err(color_eyre::eyre::eyre!(
            "status RPC response identity mismatch"
        ));
    }
    if let Some(error) = response.get("error") {
        return Err(color_eyre::eyre::eyre!("status RPC failed: {error}"));
    }
    Ok(serde_json::from_value(
        response
            .get("result")
            .cloned()
            .ok_or_else(|| color_eyre::eyre::eyre!("status RPC omitted result"))?,
    )?)
}

async fn events_query_rpc(
    client: &reqwest::Client,
    endpoint: &str,
    expected_event_ids: &[Uuid],
    manifest_digest: &str,
) -> TestResult<()> {
    let expected = expected_event_ids
        .iter()
        .map(ToString::to_string)
        .collect::<std::collections::BTreeSet<_>>();
    let mut queried = std::collections::BTreeSet::new();
    let mut cursor: Option<Value> = None;
    // One extra page allows an empty final page. A repeated/broken cursor must
    // fail rather than turn an incomplete real-client read into a successful loop.
    for _ in 0..=(expected_event_ids.len() / 1000 + 1) {
        let mut params = serde_json::to_value(sinex_primitives::query::EventQuery {
            sources: vec![
                "integration.polylogue"
                    .parse()
                    .map_err(|error: String| color_eyre::eyre::eyre!(error))?,
            ],
            payload: Some(sinex_primitives::query::PayloadFilter::Contains {
                value: json!({"manifest_digest": manifest_digest}),
            }),
            limit: 1000,
            ..Default::default()
        })?;
        if let Some(ref cursor) = cursor {
            params["cursor"] = cursor.clone();
        }
        let response = client.post(endpoint).bearer_auth(FIXTURE_TOKEN)
            .json(&json!({"jsonrpc":"2.0", "id":"polylogue-events-query", "method":"events.query", "params":params}))
            .send().await?.error_for_status()?.json::<Value>().await?;
        if let Some(error) = response.get("error") {
            return Err(color_eyre::eyre::eyre!("events.query failed: {error}"));
        }
        let events = response
            .pointer("/result/events")
            .and_then(Value::as_array)
            .ok_or_else(|| {
                color_eyre::eyre::eyre!("events.query did not return an Events result")
            })?;
        for event in events {
            let id = event
                .get("id")
                .and_then(Value::as_str)
                .ok_or_else(|| color_eyre::eyre::eyre!("queried event omitted ID"))?;
            assert!(
                queried.insert(id.to_owned()),
                "cursor pagination returned a duplicate event"
            );
        }
        let next = response
            .pointer("/result/next_cursor")
            .filter(|value| !value.is_null())
            .cloned();
        if next.is_none() {
            assert_eq!(
                queried, expected,
                "actual events.query must expose every confirmed child exactly once"
            );
            return Ok(());
        }
        assert_ne!(next, cursor, "events.query cursor did not advance");
        cursor = next;
    }
    Err(color_eyre::eyre::eyre!(
        "events.query exceeded expected page bound"
    ))
}

fn identity_tuple(
    response: &PolylogueRevisionPublicationResponse,
) -> (
    Uuid,
    Uuid,
    std::collections::BTreeMap<i32, Uuid>,
    Vec<Uuid>,
    Vec<Uuid>,
) {
    (
        response.operation_id,
        response.material_refs.manifest_material_id,
        response.material_refs.segment_material_ids.clone(),
        response.material_refs.original_material_ids.clone(),
        response.event_ids.clone(),
    )
}

async fn material_cas_bytes(services: &ServiceContainer, material_id: Uuid) -> TestResult<Vec<u8>> {
    let record = services
        .pool()
        .source_materials()
        .get_by_id(Id::from_uuid(material_id))
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("prepared material was not registered"))?;
    if record.status != sinex_primitives::domain::MaterialStatus::Completed {
        return Err(color_eyre::eyre::eyre!(
            "registered material is not complete"
        ));
    }
    let blob_id = record
        .optional_blob_id
        .ok_or_else(|| color_eyre::eyre::eyre!("registered material lacks CAS blob"))?;
    let blob = services
        .pool()
        .blobs()
        .get_by_id(blob_id.into())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("registered CAS blob is absent"))?;
    let path = services
        .content
        .content_store()
        .get_blob_path(&blob.content_key())
        .await?
        .into_std_path_buf();
    Ok(tokio::fs::read(path).await?)
}

async fn verify_cas_and_event_facts(
    services: &ServiceContainer,
    response: &PolylogueRevisionPublicationResponse,
    originals: &[Value],
) -> TestResult<()> {
    let manifest_bytes =
        material_cas_bytes(services, response.material_refs.manifest_material_id).await?;
    let manifest = PolylogueRevisionManifest::from_json(&manifest_bytes)?;
    let mut segment_paths = std::collections::BTreeMap::new();
    for (&index, &material_id) in &response.material_refs.segment_material_ids {
        let bytes = material_cas_bytes(services, material_id).await?;
        let record = services
            .pool()
            .source_materials()
            .get_by_id(Id::from_uuid(material_id))
            .await?
            .unwrap();
        let blob_id = record.optional_blob_id.unwrap();
        let blob = services
            .pool()
            .blobs()
            .get_by_id(blob_id.into())
            .await?
            .unwrap();
        let path = services
            .content
            .content_store()
            .get_blob_path(&blob.content_key())
            .await?
            .into_std_path_buf();
        assert_eq!(tokio::fs::read(&path).await?, bytes);
        segment_paths.insert(index, path);
    }
    let verified_manifest = manifest.clone();
    tokio::task::spawn_blocking(move || verified_manifest.verify_files(&segment_paths)).await??;
    if response.material_refs.original_material_ids.len() != originals.len() {
        return Err(color_eyre::eyre::eyre!(
            "retained original ID count differs from actual Source payload"
        ));
    }
    for (material_id, original) in response
        .material_refs
        .original_material_ids
        .iter()
        .zip(originals)
    {
        let bytes = material_cas_bytes(services, *material_id).await?;
        assert_eq!(bytes.len() as u64, original["size_bytes"].as_u64().unwrap());
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            original["sha256"].as_str().unwrap()
        );
    }
    let typed_ids = response
        .event_ids
        .iter()
        .copied()
        .map(Id::from_uuid)
        .collect::<Vec<_>>();
    let mut events = Vec::with_capacity(typed_ids.len());
    for chunk in typed_ids.chunks(1000) {
        events.extend(services.pool().events().get_by_ids(chunk).await?);
    }
    assert_eq!(events.len(), response.event_ids.len());
    let mut seen_records = std::collections::BTreeSet::new();
    let manifest_digest = format!("{:x}", Sha256::digest(&manifest_bytes));
    for event in events {
        let id = event
            .id
            .ok_or_else(|| color_eyre::eyre::eyre!("persisted event has no ID"))?
            .to_uuid();
        let record_id = event
            .payload
            .get("record_id")
            .and_then(Value::as_str)
            .ok_or_else(|| color_eyre::eyre::eyre!("persisted event has no manifest record ID"))?;
        let anchor = manifest
            .anchors
            .get(record_id)
            .ok_or_else(|| color_eyre::eyre::eyre!("persisted event names no manifest anchor"))?;
        assert!(
            seen_records.insert(record_id.to_owned()),
            "manifest anchor emitted more than once"
        );
        assert_eq!(event.source.as_str(), "integration.polylogue");
        assert_eq!(
            event.event_type.as_str(),
            format!("integration.polylogue.{}.observed", anchor.kind)
        );
        assert_eq!(event.payload["seq"].as_u64(), Some(anchor.seq));
        assert_eq!(
            event.payload["record_kind"].as_str(),
            Some(anchor.kind.as_str())
        );
        assert_eq!(
            event.payload["line_index"].as_u64(),
            Some(anchor.line_index as u64)
        );
        assert_eq!(
            event.payload["record_sha256"].as_str(),
            Some(anchor.sha256.as_str())
        );
        assert_eq!(
            event.payload["manifest_digest"].as_str(),
            Some(manifest_digest.as_str())
        );
        let expected_material = response
            .material_refs
            .segment_material_ids
            .get(&anchor.segment_index)
            .map(ToString::to_string);
        assert_eq!(
            event.payload["material_id"].as_str(),
            expected_material.as_deref()
        );
        assert!(response.event_ids.contains(&id));
    }
    assert_eq!(seen_records, manifest.anchors.keys().cloned().collect());
    Ok(())
}

#[sinex_test(timeout = 300)]
#[ignore = "requires exact external Python producer; run xtask test --heavy with SINEX_POLYLOGUE_PYTHON and SINEX_POLYLOGUE_CHECKOUT"]
async fn actual_polylogue_source_outbox_survives_native_rpc_restart_and_settles() -> TestResult<()>
{
    if std::env::var_os(API_CHILD).is_some() {
        let topology: JetStreamTopology = serde_json::from_str(&std::env::var(API_TOPOLOGY)?)?;
        let config = GatewayConfig::load()?;
        let services = ServiceContainer::new(&config)
            .await?
            .with_raw_activity_topology(topology);
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let (address, _server) = rpc_server::spawn(&config, services, shutdown_rx).await?;
        tokio::fs::write(std::env::var(API_READY)?, address.to_string()).await?;
        std::future::pending::<()>().await;
        return Ok(());
    }

    let producer = ProducerInputs::required()?;
    let token = FIXTURE_TOKEN;
    let ctx = TestContext::with_name(TEST_NAME)
        .await?
        .with_nats()
        .shared()
        .await?;
    let namespace = ctx.pipeline_namespace().prefix().to_owned();
    let topology = JetStreamTopology::new(
        ctx.env(),
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-polylogue-native-proof"),
        Some(&namespace),
    );
    let work = tempfile::tempdir()?;
    let tls = FixtureTls::create(work.path())?;
    let cert = reqwest::Certificate::from_pem(tls.cert_pem.as_bytes())?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .add_root_certificate(cert)
        .build()?;
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    let test_name = format!("{module}::{TEST_NAME}");
    let api1 = start_api_child(&test_name, &ctx, &topology, &tls, &token, work.path(), 1).await?;
    let api1_url = wait_child_url(work.path(), 1).await?;
    let (proxy, mut lost_rx) = HttpsReplyProxy::start(&tls, api1_url.clone()).await?;
    let source_root = work.path().join("source-root");
    let first = producer
        .run(&proxy.endpoint, &source_root, &tls.cert_path, false)
        .await?;
    let request_id = first["request_id"]
        .as_str()
        .ok_or_else(|| color_eyre::eyre::eyre!("producer request ID absent"))?;
    let lost = tokio::time::timeout(Duration::from_secs(10), lost_rx.recv())
        .await?
        .ok_or_else(|| color_eyre::eyre::eyre!("proxy did not observe a committed native reply"))?;
    assert_eq!(
        lost.pointer("/result/state").and_then(Value::as_str),
        Some("raw_accepted")
    );
    assert_eq!(
        lost.pointer("/result/request_id").and_then(Value::as_str),
        first["request_id"].as_str()
    );
    assert_eq!(
        proxy.status_calls(),
        1,
        "real Python transport must recover using its status fallback"
    );
    assert_eq!(first["attempted"], 1);
    assert_eq!(first["confirmed"], 0);
    assert_eq!(first["outbox_status"]["last_receipt_state"], "raw_accepted");
    assert_eq!(first["outbox_status"]["pending"], 1);
    assert_eq!(first["outbox_status"]["confirmed"], 0);
    assert!(
        first["originals"]
            .as_array()
            .unwrap()
            .iter()
            .any(|original| original["role"] == "provider_native")
    );
    assert!(
        first["originals"]
            .as_array()
            .unwrap()
            .iter()
            .any(|original| original["role"] == "attachment")
    );
    let rpc_files = source_root.join("rpc-files");
    let mut entries = tokio::fs::read_dir(&rpc_files).await?;
    assert!(
        entries.next_entry().await?.is_none(),
        "temporary transport locators must retire after the exchange"
    );

    let js_before_consumer = ctx.nats_handle()?.jetstream_with_client(ctx.nats_client());
    let mut raw_stream = js_before_consumer
        .get_stream(&topology.events_stream)
        .await?;
    assert_eq!(
        raw_stream.info().await?.state.messages,
        1,
        "concurrent identical requests must publish one physical raw envelope"
    );

    let original_raw = raw_stream.get_raw_message(1).await?;

    let before = status_rpc(&client, &api1_url, first["request_id"].as_str().unwrap()).await?;
    assert_eq!(before.state, Some(PolyloguePublicationState::RawAccepted));
    let mut api1 = api1;
    api1.kill().await?;
    assert_eq!(
        api1.wait().await?.signal(),
        Some(9),
        "API child must be SIGKILLed"
    );
    let mut api2 =
        start_api_child(&test_name, &ctx, &topology, &tls, &token, work.path(), 2).await?;
    let api2_url = wait_child_url(work.path(), 2).await?;
    let after = status_rpc(&client, &api2_url, first["request_id"].as_str().unwrap()).await?;
    assert_eq!(
        identity_tuple(&before),
        identity_tuple(&after),
        "request identity must survive process restart"
    );
    assert_eq!(after.state, Some(PolyloguePublicationState::RawAccepted));

    // Construct the reading service against the same DB, NATS namespace, and
    // CAS root used by both API children; this is not an in-memory CAS double.
    let mut config = GatewayConfig::load()?;
    config.database_url = ctx.database_url().to_owned();
    config.nats.url = ctx.nats_handle()?.client_url().to_owned();
    config.content_store_path = work
        .path()
        .join("native-cas")
        .to_string_lossy()
        .into_owned();
    config = config.with_nats_namespace(Some(namespace.clone()));
    let services = ServiceContainer::new(&config)
        .await?
        .with_raw_activity_topology(topology.clone());
    // The existing start_isolated_consumer helper omits ContentStoreManager;
    // the CAS-backed Polylogue admission requires it. Reuse its readiness
    // barrier with a native consumer built with the exact same topology/CAS.
    let nats_client = ctx.nats_client();
    let js = ctx
        .nats_handle()?
        .jetstream_with_client(nats_client.clone());
    let validator = sinexd::event_engine::validator::IngestEventValidator::new(false);
    let native_consumer = sinexd::event_engine::JetStreamConsumer::new(
        nats_client,
        ctx.pool.clone(),
        Arc::new(tokio::sync::RwLock::new(validator)),
        topology.clone(),
    )
    // This isolated fixture deliberately retained the complete raw stream
    // before starting its first reader, after transport locators retired.
    .with_reject_initial_replay(false)
    .with_content_store(Arc::clone(services.content.content_store()))
    .with_policy_engine()
    .await?;
    let handle = spawn_consumer_and_wait_ready(&ctx, &js, &topology, native_consumer).await?;
    let consumer = ConsumerSetup {
        handle,
        js,
        topology: topology.clone(),
        namespace: namespace.clone(),
    };
    let ids = after.event_ids.clone();
    assert!(
        !ids.is_empty(),
        "actual producer revision must yield child event IDs"
    );
    tokio::time::timeout(Duration::from_secs(150), async {
        loop {
            let typed = ids.iter().copied().map(Id::from_uuid).collect::<Vec<_>>();
            if services.pool().events().get_by_ids(&typed).await?.len() == ids.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<_, color_eyre::Report>(())
    })
    .await??;
    verify_cas_and_event_facts(&services, &after, first["originals"].as_array().unwrap()).await?;
    let manifest_bytes =
        material_cas_bytes(&services, after.material_refs.manifest_material_id).await?;
    let manifest_digest = format!("{:x}", Sha256::digest(&manifest_bytes));
    events_query_rpc(&client, &api2_url, &after.event_ids, &manifest_digest).await?;
    let settled = tokio::time::timeout(Duration::from_secs(150), async {
        loop {
            let current = status_rpc(&client, &api2_url, request_id).await?;
            if current.state == Some(PolyloguePublicationState::PersistedConfirmed) {
                break Ok::<_, color_eyre::Report>(current);
            }
            if current.state == Some(PolyloguePublicationState::Rejected) {
                break Err(color_eyre::eyre::eyre!(
                    "native admission rejected the producer envelope"
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await??;
    assert_eq!(identity_tuple(&settled), identity_tuple(&before));

    consumer.handle.abort();
    let _ = consumer.handle.await;
    // Force the identical logical intent into a second physical envelope, the
    // consequence possible after a lost PubAck receipt outlives transport dedup.
    // Omitting MsgId here isolates native same-ID handling without a cache wait;
    // this is not a claim of cross-store exactly-once publication.
    let replay_js = ctx.nats_handle()?.jetstream_with_client(ctx.nats_client());
    let mut duplicate_headers = async_nats::HeaderMap::new();
    for (name, values) in original_raw.headers.iter() {
        if !name.to_string().eq_ignore_ascii_case("Nats-Msg-Id") {
            for value in values {
                duplicate_headers.append(name.clone(), value.clone());
            }
        }
    }
    let duplicate_ack = replay_js
        .publish_with_headers(
            original_raw.subject,
            duplicate_headers,
            original_raw.payload,
        )
        .await?
        .await?;
    assert!(!duplicate_ack.duplicate);
    assert!(duplicate_ack.sequence > 1);
    let retry_consumer = sinexd::event_engine::JetStreamConsumer::new(
        ctx.nats_client(),
        ctx.pool.clone(),
        Arc::new(tokio::sync::RwLock::new(
            sinexd::event_engine::validator::IngestEventValidator::new(false),
        )),
        topology.clone(),
    )
    .with_content_store(Arc::clone(services.content.content_store()))
    .with_policy_engine()
    .await?;
    let retry_handle =
        spawn_consumer_and_wait_ready(&ctx, &replay_js, &topology, retry_consumer).await?;
    tokio::time::timeout(Duration::from_secs(90), async {
        let stream = replay_js
            .get_stream(topology.events_stream.to_string())
            .await?;
        let mut raw_owner = stream
            .get_consumer::<async_nats::jetstream::consumer::pull::Config>(
                &topology.consumer_durable,
            )
            .await
            .map_err(|error| color_eyre::eyre::eyre!("raw owner lookup failed: {error}"))?;
        loop {
            if raw_owner.info().await?.ack_floor.stream_sequence >= duplicate_ack.sequence {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok::<_, color_eyre::Report>(())
    })
    .await??;
    let typed = after
        .event_ids
        .iter()
        .copied()
        .map(Id::from_uuid)
        .collect::<Vec<_>>();
    assert_eq!(
        services.pool().events().get_by_ids(&typed).await?.len(),
        typed.len()
    );
    assert!(
        services
            .pool()
            .dlq_events()
            .unresolved_polylogue_raw_ingest_event_ids(&after.event_ids)
            .await?
            .is_empty(),
        "same-ID physical redelivery must not reject already admitted children"
    );
    assert_eq!(
        status_rpc(&client, &api2_url, request_id).await?.state,
        Some(PolyloguePublicationState::PersistedConfirmed)
    );

    // An independently recorded late duplicate failure cannot revoke an exact
    // prepared child's existing durable row. Exercise the real status RPC/DB path.
    services
        .pool()
        .dlq_events()
        .insert_failure_evidence(
            after.event_ids[0],
            "event-engine.raw-ingest",
            "integration.polylogue",
            "integration.polylogue.session.observed",
            "permanent",
            "synthetic late duplicate after durable success",
            json!({"fixture":"late_duplicate"}),
            json!({}),
            0,
        )
        .await?;
    assert_eq!(
        status_rpc(&client, &api2_url, request_id).await?.state,
        Some(PolyloguePublicationState::PersistedConfirmed)
    );

    let redrive = producer
        .run(&api2_url, &source_root, &tls.cert_path, true)
        .await?;
    assert_eq!(redrive["request_id"], first["request_id"]);
    assert_eq!(redrive["attempted"], 1);
    assert_eq!(redrive["confirmed"], 1);
    assert_eq!(redrive["outbox_status"]["confirmed"], 1);
    assert_eq!(
        redrive["outbox_status"]["last_receipt_state"],
        "persisted_confirmed"
    );
    let final_status = status_rpc(&client, &api2_url, request_id).await?;
    assert_eq!(identity_tuple(&final_status), identity_tuple(&settled));

    retry_handle.abort();
    let _ = retry_handle.await;
    api2.kill().await?;
    assert_eq!(api2.wait().await?.signal(), Some(9));
    proxy.stop();
    Ok(())
}

/// Stop the actual API between committed partition receipts using its existing
/// scoped request lock. Dropping the transaction releases this test-only barrier
/// on cancellation/error as well as the normal path.
async fn kill_api_after_committed_prefix(
    ctx: &TestContext,
    api: &mut Child,
    request_id: &str,
) -> TestResult<Value> {
    // Same documented token-prefix actor derivation as RpcAuthContext::from_token.
    let actor = format!(
        "token:{}",
        FIXTURE_TOKEN.chars().take(8).collect::<String>()
    );
    let repo = ctx.pool.state();
    tokio::time::timeout(Duration::from_secs(180), async {
        loop {
            if repo
                .find_polylogue_publication(&actor, request_id)
                .await?
                .is_none()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
                continue;
            }
            let (tx, record) = repo.lock_polylogue_publication(&actor, request_id).await?;
            let evidence = record.preview_summary.clone();
            if let Some(evidence) = evidence {
                let ack_count = evidence["publish_acks"].as_array().map_or(0, Vec::len);
                let part_count = evidence["plan"]["partitions"]
                    .as_array()
                    .map_or(0, Vec::len);
                if part_count > 0 && ack_count == part_count {
                    return Err(color_eyre::eyre::eyre!(
                        "publication completed before the partial-ACK crash barrier"
                    ));
                }
                if ack_count > 0 && ack_count < part_count {
                    api.kill().await?;
                    assert_eq!(api.wait().await?.signal(), Some(9));
                    tx.rollback().await?;
                    return Ok(evidence);
                }
            }
            tx.rollback().await?;
            // Rejoin the same lock queue, allowing the waiting publisher to
            // commit the first partition. No synthetic receipt is written.
            tokio::task::yield_now().await;
        }
    })
    .await?
}

#[sinex_test(timeout = 600)]
#[ignore = "requires exact external Python producer; run xtask test --heavy with SINEX_POLYLOGUE_PYTHON and SINEX_POLYLOGUE_CHECKOUT"]
async fn actual_large_polylogue_revision_resumes_committed_partition_prefix() -> TestResult<()> {
    let producer = ProducerInputs::required()?;
    let ctx = TestContext::with_name(
        "actual_large_polylogue_revision_resumes_committed_partition_prefix",
    )
    .await?
    .with_nats()
    .shared()
    .await?;
    let namespace = ctx.pipeline_namespace().prefix().to_owned();
    let topology = JetStreamTopology::new(
        ctx.env(),
        ctx.pipeline_namespace().stream("SINEX_RAW_EVENTS"),
        ctx.pipeline_namespace()
            .consumer_name("event-engine-polylogue-large-proof"),
        Some(&namespace),
    );
    let work = tempfile::tempdir()?;
    let tls = FixtureTls::create(work.path())?;
    let client = tls.reqwest_client()?;
    let module = module_path!()
        .split_once("::")
        .map_or(module_path!(), |(_, rest)| rest);
    // Reuse the actual API child entry point; each parent owns its isolated DB,
    // namespace, CAS and process. The child does not execute the small proof.
    let child_test = format!("{module}::{TEST_NAME}");
    let mut api1 = start_api_child(
        &child_test,
        &ctx,
        &topology,
        &tls,
        FIXTURE_TOKEN,
        work.path(),
        1,
    )
    .await?;
    let api1_url = wait_child_url(work.path(), 1).await?;
    let source_root = work.path().join("large-source-root");
    let producer_child = producer.spawn(&api1_url, &source_root, &tls.cert_path, false, 1536)?;
    let marker = source_root.join("source-obligation-ready.json");
    let ready: Value = tokio::time::timeout(Duration::from_secs(90), async {
        loop {
            if tokio::fs::try_exists(&marker).await? {
                break Ok::<_, color_eyre::Report>(serde_json::from_slice(
                    &tokio::fs::read(&marker).await?,
                )?);
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await??;
    assert_eq!(ready["anchor_count"], 3091);
    let request_id = ready["request_id"].as_str().unwrap();
    let prefix = kill_api_after_committed_prefix(&ctx, &mut api1, request_id).await?;
    let first =
        tokio::time::timeout(Duration::from_secs(45), producer_child.wait_with_output()).await??;
    let first = ProducerInputs::decode_output(first)?;
    assert_eq!(first["request_id"], ready["request_id"]);
    assert_eq!(first["outbox_status"]["pending"], 1);
    assert_eq!(
        first["confirmed"], 0,
        "partial transport acceptance must leave Source work owed"
    );
    let mut locators = tokio::fs::read_dir(source_root.join("rpc-files")).await?;
    assert!(locators.next_entry().await?.is_none());

    let mut api2 = start_api_child(
        &child_test,
        &ctx,
        &topology,
        &tls,
        FIXTURE_TOKEN,
        work.path(),
        2,
    )
    .await?;
    let api2_url = wait_child_url(work.path(), 2).await?;
    let partial = status_rpc(&client, &api2_url, request_id).await?;
    assert_eq!(partial.state, None);
    assert_eq!(serde_json::to_value(&partial.phase)?, "materials_ready");
    let actor = format!(
        "token:{}",
        FIXTURE_TOKEN.chars().take(8).collect::<String>()
    );
    let retained = ctx
        .pool
        .state()
        .find_polylogue_publication(&actor, request_id)
        .await?
        .unwrap();
    assert_eq!(retained.preview_summary.as_ref(), Some(&prefix));
    let published = producer
        .run(&api2_url, &source_root, &tls.cert_path, true)
        .await?;
    assert_eq!(published["request_id"], first["request_id"]);
    assert_eq!(
        published["outbox_status"]["last_receipt_state"],
        "raw_accepted"
    );
    assert_eq!(published["confirmed"], 0);
    let mut locators = tokio::fs::read_dir(source_root.join("rpc-files")).await?;
    assert!(
        locators.next_entry().await?.is_none(),
        "decoder must consume immutable CAS after locator retirement"
    );
    let after = status_rpc(&client, &api2_url, request_id).await?;
    assert_eq!(identity_tuple(&partial), identity_tuple(&after));
    assert_eq!(after.state, Some(PolyloguePublicationState::RawAccepted));
    assert_eq!(after.event_ids.len(), 3091);
    let completed = ctx
        .pool
        .state()
        .find_polylogue_publication(&actor, request_id)
        .await?
        .unwrap()
        .preview_summary
        .unwrap();
    assert_eq!(
        completed["plan"], prefix["plan"],
        "fixed admitted_at/header and partitions must survive restart"
    );
    let prefix_acks = prefix["publish_acks"].as_array().unwrap();
    let completed_acks = completed["publish_acks"].as_array().unwrap();
    assert_eq!(&completed_acks[..prefix_acks.len()], prefix_acks.as_slice());
    assert!(completed_acks.len() > 1);
    assert_eq!(
        completed_acks.len(),
        completed["plan"]["partitions"].as_array().unwrap().len()
    );

    let js = ctx.nats_handle()?.jetstream_with_client(ctx.nats_client());
    let mut stream = js.get_stream(&topology.events_stream).await?;
    let count = stream.info().await?.state.messages;
    assert_eq!(
        count as usize,
        completed_acks.len(),
        "resume must publish only owed partitions"
    );
    let mut full = completed["plan"]["header"].clone();
    let mut all_children = Vec::new();
    for sequence in 1..=count {
        let raw = stream.get_raw_message(sequence).await?;
        assert!(raw.payload.len() <= 900 * 1024);
        let mut envelope: Value = serde_json::from_slice(&raw.payload)?;
        let children = envelope
            .get_mut("events")
            .and_then(Value::as_array_mut)
            .unwrap();
        all_children.append(children);
        assert_eq!(
            envelope, completed["plan"]["header"],
            "each physical envelope must preserve full revision proof and fixed header"
        );
    }
    assert_eq!(all_children.len(), 3091);
    full["events"] = Value::Array(all_children);
    assert!(
        serde_json::to_vec(&full)?.len() > 900 * 1024,
        "actual unsplit producer intent must exceed native transport budget"
    );

    let mut config = GatewayConfig::load()?;
    config.database_url = ctx.database_url().to_owned();
    config.nats.url = ctx.nats_handle()?.client_url().to_owned();
    config.content_store_path = work
        .path()
        .join("native-cas")
        .to_string_lossy()
        .into_owned();
    config = config.with_nats_namespace(Some(namespace));
    let services = ServiceContainer::new(&config)
        .await?
        .with_raw_activity_topology(topology.clone());
    let consumer = sinexd::event_engine::JetStreamConsumer::new(
        ctx.nats_client(),
        ctx.pool.clone(),
        Arc::new(tokio::sync::RwLock::new(
            sinexd::event_engine::validator::IngestEventValidator::new(false),
        )),
        topology.clone(),
    )
    // This isolated fixture deliberately retained the complete raw stream
    // before starting its first reader, after transport locators retired.
    .with_reject_initial_replay(false)
    .with_content_store(Arc::clone(services.content.content_store()))
    .with_policy_engine()
    .await?;
    let handle = spawn_consumer_and_wait_ready(&ctx, &js, &topology, consumer).await?;
    let settled = tokio::time::timeout(Duration::from_secs(240), async {
        loop {
            let current = status_rpc(&client, &api2_url, request_id).await?;
            match current.state {
                Some(PolyloguePublicationState::PersistedConfirmed) => {
                    break Ok::<_, color_eyre::Report>(current);
                }
                Some(
                    PolyloguePublicationState::Rejected | PolyloguePublicationState::DurableDebt,
                ) => {
                    break Err(color_eyre::eyre::eyre!(
                        "large native revision did not settle every child: {}",
                        current.detail
                    ));
                }
                _ => tokio::time::sleep(Duration::from_millis(100)).await,
            }
        }
    })
    .await??;
    assert_eq!(identity_tuple(&after), identity_tuple(&settled));
    verify_cas_and_event_facts(
        &services,
        &settled,
        published["originals"].as_array().unwrap(),
    )
    .await?;
    let manifest_bytes =
        material_cas_bytes(&services, settled.material_refs.manifest_material_id).await?;
    events_query_rpc(
        &client,
        &api2_url,
        &settled.event_ids,
        &format!("{:x}", Sha256::digest(manifest_bytes)),
    )
    .await?;
    assert!(
        ctx.pool
            .dlq_events()
            .unresolved_polylogue_raw_ingest_event_ids(&settled.event_ids)
            .await?
            .is_empty()
    );
    let confirmed = producer
        .run(&api2_url, &source_root, &tls.cert_path, true)
        .await?;
    assert_eq!(confirmed["request_id"], first["request_id"]);
    assert_eq!(confirmed["confirmed"], 1);
    assert_eq!(confirmed["outbox_status"]["confirmed"], 1);
    handle.abort();
    let _ = handle.await;
    api2.kill().await?;
    assert_eq!(api2.wait().await?.signal(), Some(9));
    Ok(())
}
