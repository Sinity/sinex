//! Explicit synthetic qualification. Run through `cargo xtask db exec` with a pinned exporter path.
use sinex_store::{Blobs, blobs::migration::prepare};
use hashed_blobs::{Hash, Extent};
use std::path::{Path, PathBuf};
use serde::Deserialize;
use sqlx::Connection;
use std::io::{Read, Seek, SeekFrom, Write};
use std::process::{Child, ChildStdout, Command, Stdio};

#[derive(Deserialize)]
struct Catalog {
    blobs: Vec<Blob>,
}
#[derive(Deserialize)]
struct Blob {
    hash: [u8; 32],
    len: u64,
    key_ref: Option<String>,
    state: String,
}

fn exporter(helper: &Path, root: &Path, operation: &str, hash: Option<&str>) -> Child {
    // The helper receives the synthetic data key through inherited FD3 only.
    let mut command = Command::new("bash");
    command
        .args(["-c", "exec 3<&0; exec \"$@\"", "predecessor-fixture"])
        .arg(helper)
        .arg(operation)
        .arg(root);
    if let Some(hash) = hash {
        command.arg(hash);
    }
    let mut child = command
        .env("CONVERSION_DATA_KEY_FD", "3")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(&[7; 32]).unwrap();
    child
}
/// A repeatable plaintext pipe. Rewinding reexecutes the qualified exporter;
/// the new sealed encoder needs two passes without a plaintext scratch file.
struct Exported {
    helper: PathBuf,
    root: PathBuf,
    hash: String,
    child: Option<Child>,
    stdout: Option<ChildStdout>,
    at: u64,
}
impl Exported {
    fn new(helper: &Path, root: &Path, hash: &Hash) -> Self {
        let mut value = Self {
            helper: helper.into(),
            root: root.into(),
            hash: hash.hex(),
            child: None,
            stdout: None,
            at: 0,
        };
        value.restart();
        value
    }
    fn restart(&mut self) {
        self.stop();
        let mut child = exporter(&self.helper, &self.root, "payload", Some(&self.hash));
        self.stdout = child.stdout.take();
        self.child = Some(child);
        self.at = 0;
    }
    fn stop(&mut self) {
        self.stdout.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
impl Read for Exported {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let n = self.stdout.as_mut().expect("export pipe open").read(out)?;
        if n == 0 {
            if let Some(mut child) = self.child.take() {
                if !child.wait()?.success() {
                    return Err(std::io::Error::other("predecessor exporter failed"));
                }
            }
        }
        self.at += n as u64;
        Ok(n)
    }
}
impl Seek for Exported {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        match from {
            SeekFrom::Current(0) => Ok(self.at),
            SeekFrom::Start(0) => {
                self.restart();
                Ok(0)
            }
            _ => Err(std::io::Error::other("export pipe only supports rewind")),
        }
    }
}
impl Drop for Exported {
    fn drop(&mut self) {
        self.stop();
    }
}

async fn preserved(conn: &mut sqlx::PgConnection) -> Vec<serde_json::Value> {
    let mut result = Vec::new();
    for table in [
        "core.blobs",
        "raw.materials",
        "privacy.data_keys",
        "raw.positions",
    ] {
        let rows: Vec<serde_json::Value> =
            sqlx::query_scalar(&format!("SELECT to_jsonb(t) FROM {table} t ORDER BY 1"))
                .fetch_all(&mut *conn)
                .await
                .unwrap();
        result.push(serde_json::json!({"table":table,"rows":rows}));
    }
    result
}
fn inventory(root: &Path) -> Vec<(PathBuf, blake3::Hash)> {
    let mut paths = vec![root.to_owned()];
    let mut files = Vec::new();
    while let Some(path) = paths.pop() {
        for entry in std::fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                paths.push(entry.path());
            } else {
                files.push((
                    entry.path().strip_prefix(root).unwrap().to_owned(),
                    blake3::hash(&std::fs::read(entry.path()).unwrap()),
                ));
            }
        }
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    files
}

#[tokio::main]
async fn main()->Result<(),Box<dyn std::error::Error>> {
    let helper=PathBuf::from(std::env::args_os().nth(1).ok_or("usage: predecessor_conversion QUALIFIED_EXPORTER")?);
    if !helper.is_file() {return Err("qualified predecessor exporter is missing".into());}
    let db=devkit::pg::test_database()?;
    qualify(&helper,db).await;
    println!("predecessor conversion fixture: names, keys, catalog rows, custody and crash resume passed");
    Ok(())
}
async fn qualify(helper:&Path,db:devkit::pg::TestDatabase) {
    let source = tempfile::tempdir().unwrap();
    let mut fixture = exporter(&helper, source.path(), "fixture", None);
    drop(fixture.stdout.take());
    assert!(fixture.wait().unwrap().success());
    let catalog: Catalog =
        serde_json::from_slice(&std::fs::read(source.path().join("catalog.json")).unwrap())
            .unwrap();
    let custody = inventory(source.path());
    let mut conn = sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    assert!(sinex_store::writer_lock::try_claim(&mut conn).await.unwrap());
    let mut key_refs = std::collections::BTreeSet::new();
    for blob in &catalog.blobs {
        let hash = Hash(blob.hash).to_string();
        sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,$3)")
            .bind(&hash)
            .bind(blob.len as i64)
            .bind(&blob.state)
            .execute(&mut conn)
            .await
            .unwrap();
        if blob.state == "present"
            && blob
                .key_ref
                .as_ref()
                .is_none_or(|reference| key_refs.insert(reference.clone()))
        {
            let material = sinex_model::MaterialId::new();
            sqlx::query("INSERT INTO raw.materials(id,blob,length,manifest,parser,parser_version) VALUES($1,$2,$3,'{}','conversion-fixture',1)").bind(material.0).bind(&hash).bind(blob.len as i64).execute(&mut conn).await.unwrap();
            if let Some(key_ref) = &blob.key_ref {
                sinex_store::sealed::store_key(&mut conn, material, key_ref, &[0x7a; 32])
                    .await
                    .unwrap();
            }
        }
    }
    sqlx::query("INSERT INTO raw.positions(binding,cursor,carry,committed_seq) VALUES('deleted-source','{\"position\":7}','{\"fixture\":true}',0)").execute(&mut conn).await.unwrap();
    let before = preserved(&mut conn).await;
    let target = tempfile::tempdir().unwrap();
    let blobs = Blobs::open(target.path()).unwrap();
    blobs.load(&mut conn).await.unwrap();
    let first = catalog
        .blobs
        .iter()
        .find(|blob| blob.state == "present")
        .unwrap();
    let key = first
        .key_ref
        .as_ref()
        .map(|key_ref| sinex_sealing::DataKey {
            key_ref: key_ref.clone(),
            key: [7; 32],
        });
    let export=Exported::new(helper,source.path(),&Hash(first.hash));
    let name=match &key {Some(key)=>blobs.put_sealed_reader(export,key),None=>blobs.put_seekable(export)}.unwrap();
    assert_eq!(name,Hash(first.hash).to_string());
    let descriptor=blobs.descriptors(&name).unwrap();
    let extent=Extent::decode(&descriptor[0]).unwrap();
    assert!(std::fs::metadata(target.path().join(&extent.segment)).unwrap().len()>0);
    drop(blobs); // Stop after segment sync and before the extent transaction.
    let blobs = Blobs::open(target.path()).unwrap();
    blobs.load(&mut conn).await.unwrap();
    for blob in &catalog.blobs {
        if blob.state == "deleting" {
            continue;
        }
        let name = Hash(blob.hash).to_string();
        let key = blob.key_ref.as_ref().map(|key_ref| sinex_sealing::DataKey {
            key_ref: key_ref.clone(),
            key: [7; 32],
        });
        let prepared = prepare(
            &target.path().join("incoming"),
            &name,
            blob.len,
            Exported::new(&helper, source.path(), &Hash(blob.hash)),
            key.as_ref(),
        )
        .unwrap();
        blobs.convert_prepared(&mut conn, prepared).await.unwrap();
        let mut reader=match &key {
            Some(key)=>blobs.read_sealed(&name,Some(blob.len),key,sinex_store::blobs::SealedReadLimits {frame_bytes:sinex_store::blobs::BLOB_FRAME_BYTES,index_bytes:65536},||Ok(())),
            None=>blobs.read_plain(&name,Some(blob.len),||Ok(())),
        }.unwrap();
        assert_eq!(
            std::io::copy(&mut reader, &mut std::io::sink()).unwrap(),
            blob.len
        );
    }
    assert_eq!(preserved(&mut conn).await, before);
    assert_eq!(
        inventory(source.path()),
        custody,
        "predecessor files remain unchanged"
    );
    drop(blobs);
    let reopened = Blobs::open(target.path()).unwrap();
    reopened.recover(&mut conn).await.unwrap();
    let deleting = catalog
        .blobs
        .iter()
        .find(|blob| blob.state == "deleting")
        .unwrap();
    assert!(!reopened.contains(&Hash(deleting.hash).to_string()).unwrap());
    assert_eq!(preserved(&mut conn).await, before);
    assert_eq!(inventory(source.path()), custody);
}

