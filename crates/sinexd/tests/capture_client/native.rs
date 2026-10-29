//! Native declarations and host attestations are exercised through real clients.
use super::*;
use serde_json::json;
use std::{collections::HashMap, io::BufRead, process::{Child, Stdio}, sync::atomic::{AtomicU32, Ordering}};
use zbus::zvariant::OwnedValue;

// A source name may have several parsers and declaration versions. These
// real capture fixtures authorize the adapters they actually launch.
fn native_spec(name: &str) -> &'static sinex_source_core::SourceSpec {
    match name {
        "machine.file-watch" => &sinex_sources::sources::file_watch::SPEC,
        "desktop.notifications" => &sinex_sources::sources::notifications::SPEC,
        _ => panic!("fixture has no native capture adapter for {name}"),
    }
}

pub(super) fn server(binary:&Path,database:&str,store:&Path,root:&Path,nats:&devkit::nats::TestNats,owners:&[(&str,&str,&[&str])])->Process {
    let mut grants=serde_json::Map::new();
    for (producer,host,names) in owners {
        let declarations:Vec<_>=names.iter().map(|name| {
            let spec=native_spec(name);
            let kinds:Vec<_>=spec.event_types.iter().map(|expected| {
                sinex_model::event_types::declarations().iter().find(|kind|kind.kind.name==*expected)
                    .unwrap_or_else(||panic!("native source {} parser {} version {} emits missing bundled kind {}",spec.name,spec.parser,spec.parser_version,expected)).clone()
            }).collect();
            for expected in spec.event_types {
                assert!(kinds.iter().any(|kind|kind.kind.name==*expected),"native source {} lacks exact emitted kind {}",spec.name,expected);
            }
            sinex_model::SourceDeclaration{
                source:sinex_model::SourceRef{name:(*name).into(),version:spec.parser_version},family:"sinex".into(),
                kinds,
            }
        }).collect();
        let kinds:Vec<_>=declarations.iter().flat_map(|decl|decl.kinds.iter().map(|kind|kind.kind.name.clone())).collect();
        grants.insert((*producer).into(),json!({"kind_prefixes":[],"classes":[],"write":{"family":"sinex","host":host,"source_prefixes":names,"source_declarations":declarations,"kind_prefixes":kinds,"classes":["open","guarded"]}}));
    }
    let grant_file=root.join("producer-grants.json");
    std::fs::write(&grant_file,serde_json::to_vec(&grants).unwrap()).unwrap();
    let tokens=root.join("tokens");std::fs::create_dir_all(&tokens).unwrap();
    let config=root.join("sources.json");sources(&config,&["sinex.telemetry"]);
    Process::start(run(binary,database,store,&["run","--nats",nats.url(),"--subject-root",nats.prefix(),"--host","daemon-host"])
        .arg("--tokens").arg(tokens).arg("--sources").arg(config).arg("--producer-grants").arg(grant_file).arg("--socket").arg(root.join("api.sock")),root.join("daemon.log"))
}
struct Bus {process:Child,address:String}
impl Bus {fn start()->Self {let mut process=Command::new("dbus-daemon").args(["--session","--nofork","--print-address=1"]).stdout(Stdio::piped()).spawn().unwrap();let mut address=String::new();std::io::BufReader::new(process.stdout.take().unwrap()).read_line(&mut address).unwrap();Self{process,address:address.trim().into()}}}
impl Drop for Bus {fn drop(&mut self){let _=self.process.kill();let _=self.process.wait();}}
struct Notifications(AtomicU32);
#[zbus::interface(name="org.freedesktop.Notifications")]
impl Notifications {
    #[allow(clippy::too_many_arguments)]
    fn notify(&self,_app:String,_replaces:u32,_icon:String,_summary:String,_body:String,_actions:Vec<String>,_hints:HashMap<String,OwnedValue>,_expire:i32)->u32 {self.0.fetch_add(1,Ordering::SeqCst)+1}
}
async fn notify(connection:&zbus::Connection,marker:&str) {
    connection.call_method(Some("org.freedesktop.Notifications"),"/org/freedesktop/Notifications",Some("org.freedesktop.Notifications"),"Notify",&("Synthetic",0u32,"",marker,"fixture body",Vec::<String>::new(),HashMap::<String,OwnedValue>::new(),1000i32)).await.unwrap();
}

#[devkit::test]
async fn two_native_sources_keep_exact_declarations_and_attested_hosts() {
    let db=devkit::pg::test_database().unwrap();let database=db.url(None);let nats=devkit::nats::test_nats().unwrap();let dir=tempfile::tempdir().unwrap();let root=dir.path();
    let bus=Bus::start();
    let service=zbus::connection::Builder::address(bus.address.as_str()).unwrap().name("org.freedesktop.Notifications").unwrap().serve_at("/org/freedesktop/Notifications",Notifications(AtomicU32::new(0))).unwrap().build().await.unwrap();
    let native=["machine.file-watch","desktop.notifications"];
    let binary=Path::new(env!("CARGO_BIN_EXE_sinexd"));
    let mut server=server(binary,&database,&root.join("daemon-store"),root,&nats,&[("satellite-a","host-a",&native),("satellite-b","host-b",&native),("files-only","host-c",&["machine.file-watch"])]);
    let a=root.join("a");let b=root.join("b");
    for node in [&a,&b] {std::fs::create_dir_all(node.join("watched")).unwrap();sources(&node.join("capture-sources.json"),&native);}
    // Identical, untrusted input host claims cannot replace owner attestations.
    let mut first=capture_node(&a,&nats,"satellite-a","untrusted-input-host",Some(&bus.address));
    let mut second=capture_node(&b,&nats,"satellite-b","untrusted-input-host",Some(&bus.address));
    watching(&mut first,&a.join("watched")).await;watching(&mut second,&b.join("watched")).await;
    for node in [&a,&b] {std::fs::write(node.join("watched/synthetic-native-file"),b"synthetic").unwrap();}
    let mut conn=PgConnection::connect(&database).await.unwrap();
    let deadline=Instant::now()+Duration::from_secs(40);
    loop {
        first.running();second.running();server.running();notify(&service,"native-source-fixture").await;
        let count:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT DISTINCT producer_identity,source FROM core.events WHERE producer_identity IN ('satellite-a','satellite-b') AND ((source='machine.file-watch' AND payload->>'path' LIKE '%synthetic-native-file') OR (source='desktop.notifications' AND payload::text LIKE '%native-source-fixture%'))) selected").fetch_one(&mut conn).await.unwrap();
        if count==4 {break;}
        assert!(Instant::now()<deadline,"native sources not admitted: {}\n{}\n{}",first.log(),second.log(),server.log());tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let identities:Vec<(String,String,String,i64)>=sqlx::query_as("SELECT DISTINCT producer_identity,producer_host,source,source_version FROM core.events WHERE producer_identity IN ('satellite-a','satellite-b') ORDER BY 1,3").fetch_all(&mut conn).await.unwrap();
    for (producer,host,source,version) in &identities {
        assert_eq!(host,if producer=="satellite-a" {"host-a"}else{"host-b"});
        let spec=native_spec(source);assert_eq!(*version,i64::from(spec.parser_version));
    }
    assert_eq!(identities.len(),4);
    let mismatches:i64=sqlx::query_scalar("SELECT count(*) FROM core.events e JOIN core.runs r ON r.id=e.run_id JOIN core.manifests manifest ON manifest.id=r.manifest_id LEFT JOIN registry.source_declarations d ON d.name=e.source AND d.version=e.source_version WHERE e.producer_identity IN ('satellite-a','satellite-b') AND (r.host IS DISTINCT FROM e.producer_host OR d.fingerprint IS DISTINCT FROM e.source_fingerprint OR d.name IS NULL OR manifest.name<>e.source OR manifest.version<>e.source_version::text OR manifest.build NOT LIKE '%;dirty=%')").fetch_one(&mut conn).await.unwrap();assert_eq!(mismatches,0);
    let inputs:i64=sqlx::query_scalar("SELECT count(DISTINCT m.input_id) FROM core.events e JOIN raw.materials m ON m.id=e.material_id WHERE e.producer_identity IN ('satellite-a','satellite-b') AND e.source='machine.file-watch'").fetch_one(&mut conn).await.unwrap();assert_eq!(inputs,2,"identical raw input claims remain distinct per attested producer");
    let deadline=Instant::now()+Duration::from_secs(30);
    loop {
        let fused:i64=sqlx::query_scalar("SELECT count(*) FROM core.events WHERE event_type='notification.fused' AND payload::text LIKE '%native-source-fixture%' AND payload->'observed_by' @> '[\"desktop.notifications\"]'::jsonb AND cardinality(parents)>0").fetch_one(&mut conn).await.unwrap();
        if fused>0 {break;}
        server.running();assert!(Instant::now()<deadline,"native source was not selected for notification fusion: {}",server.log());tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // An otherwise real client cannot mint authority with its producer name.
    let denied=root.join("denied");std::fs::create_dir_all(denied.join("watched")).unwrap();sources(&denied.join("capture-sources.json"),&["machine.file-watch"]);
    let mut unauthorized=capture_node(&denied,&nats,"unconfigured-satellite","host-a",Some(&bus.address));watching(&mut unauthorized,&denied.join("watched")).await;
    std::fs::write(denied.join("watched/denied-native-file"),b"synthetic unauthorized").unwrap();
    let deadline=Instant::now()+Duration::from_secs(15);
    loop {
        unauthorized.running();server.running();
        if unauthorized.log().contains("producer has no configured write family") {break;}
        assert!(Instant::now()<deadline,"client refusal not observed: {}\n{}",unauthorized.log(),server.log());tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let denied_events:i64=sqlx::query_scalar("SELECT count(*) FROM core.events WHERE producer_identity='unconfigured-satellite'").fetch_one(&mut conn).await.unwrap();assert_eq!(denied_events,0);
    unauthorized.stop().await;
    let restricted=root.join("restricted");std::fs::create_dir_all(restricted.join("watched")).unwrap();sources(&restricted.join("capture-sources.json"),&native);
    let mut unauthorized_source=capture_node(&restricted,&nats,"files-only","host-c",Some(&bus.address));watching(&mut unauthorized_source,&restricted.join("watched")).await;
    let deadline=Instant::now()+Duration::from_secs(15);
    loop {
        unauthorized_source.running();server.running();
        if unauthorized_source.log().contains("producer hello is outside its write kind or class capability") {break;}
        assert!(Instant::now()<deadline,"undelegated native source not refused: {}\n{}",unauthorized_source.log(),server.log());tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let denied_source_events:i64=sqlx::query_scalar("SELECT count(*) FROM core.events WHERE producer_identity='files-only'").fetch_one(&mut conn).await.unwrap();assert_eq!(denied_source_events,0);
    unauthorized_source.stop().await;
    first.stop().await;second.stop().await;server.stop().await;
}
