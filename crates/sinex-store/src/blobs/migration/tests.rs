use super::*;

#[devkit::test]
fn conversion_refuses_a_wrong_export_name_length_or_key_before_installing_bytes() {
    let root = tempfile::tempdir().unwrap();
    let bytes = b"synthetic retained bytes";
    let name = Hash::of(bytes).to_string();
    assert!(
        prepare(
            root.path(),
            &name,
            bytes.len() as u64 + 1,
            std::io::Cursor::new(bytes),
            None
        )
        .is_err()
    );
    assert!(
        prepare(
            root.path(),
            &Hash::of(b"different").to_string(),
            bytes.len() as u64,
            std::io::Cursor::new(bytes),
            None
        )
        .is_err()
    );
    let key = sinex_sealing::DataKey {
        key_ref: "synthetic-key".into(),
        key: [7; 32],
    };
    let name = Preparation::sealed_name(&key, bytes).to_string();
    let wrong = sinex_sealing::DataKey {
        key_ref: key.key_ref.clone(),
        key: [8; 32],
    };
    assert!(
        prepare(
            root.path(),
            &name,
            bytes.len() as u64,
            std::io::Cursor::new(bytes),
            Some(&wrong)
        )
        .is_err()
    );
    assert_eq!(
        std::fs::read_dir(root.path()).unwrap().count(),
        0,
        "failed preparations leave no named plaintext or encoding"
    );
}


#[devkit::test]
async fn converted_streams_keep_existing_material_and_key_rows(db:devkit::pg::TestDatabase) {
    use sqlx::Connection;
    let mut conn=sqlx::PgConnection::connect(&db.url(None)).await.unwrap();
    assert!(crate::writer_lock::try_claim(&mut conn).await.unwrap());
    let root=tempfile::tempdir().unwrap();let blobs=Blobs::open(root.path()).unwrap();
    let bytes=b"synthetic preserved source bytes";
    let key=sinex_sealing::DataKey{key_ref:"synthetic-key".into(),key:[7;32]};
    let name=Preparation::sealed_name(&key,bytes).to_string();
    let material=sinex_model::MaterialId::new();
    sqlx::query("INSERT INTO core.blobs(hash,size,status) VALUES($1,$2,'present')").bind(&name).bind(bytes.len() as i64).execute(&mut conn).await.unwrap();
    sqlx::query("INSERT INTO raw.materials(id,blob,length,manifest,parser,parser_version) VALUES($1,$2,$3,'{}','synthetic',1)").bind(material.0).bind(&name).bind(bytes.len() as i64).execute(&mut conn).await.unwrap();
    crate::sealed::store_key(&mut conn,material,&key.key_ref,&[0x7a;32]).await.unwrap();
    let before:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(m)||jsonb_build_object('key',(SELECT to_jsonb(k) FROM privacy.data_keys k WHERE k.material_id=m.id)) FROM raw.materials m WHERE id=$1").bind(material.0).fetch_one(&mut conn).await.unwrap();
    let prepared=prepare(&root.path().join("incoming"),&name,bytes.len() as u64,std::io::Cursor::new(bytes),Some(&key)).unwrap();
    blobs.convert_prepared(&mut conn,prepared).await.unwrap();
    let after:serde_json::Value=sqlx::query_scalar("SELECT to_jsonb(m)||jsonb_build_object('key',(SELECT to_jsonb(k) FROM privacy.data_keys k WHERE k.material_id=m.id)) FROM raw.materials m WHERE id=$1").bind(material.0).fetch_one(&mut conn).await.unwrap();
    assert_eq!(before,after);
    drop(blobs);
    let reopened=Blobs::open(root.path()).unwrap();reopened.recover(&mut conn).await.unwrap();
    let mut reader=reopened.read_sealed(&name,Some(bytes.len() as u64),&key,SealedReadLimits{frame_bytes:BLOB_FRAME_BYTES,index_bytes:65536},||Ok(())).unwrap();
    let mut actual=Vec::new();reader.read_to_end(&mut actual).unwrap();assert_eq!(actual,bytes);
}
