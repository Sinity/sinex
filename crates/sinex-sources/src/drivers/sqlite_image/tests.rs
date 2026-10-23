use super::*;
fn policy(class:Class)->Policy {Policy {scope:"whole_database".into(),class,interval_secs:3600,turn_pages:1,rebase_threshold:1.0}}
fn pinned(path:&std::path::Path)->Connection {
    let c=Connection::open(path).unwrap();c.execute_batch("BEGIN; SELECT count(*) FROM sqlite_master;").unwrap();c
}
fn finish(mut scan:Scan)->Arc<Image> {while !scan.turn().unwrap() {} scan.finish().unwrap()}

#[test]
fn sqlite_images_keep_one_wal_view_across_turns_and_reconstruct_changed_pages() {
    let dir=tempfile::tempdir().unwrap();let path=dir.path().join("source.db");
    let db=Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA page_size=1024; PRAGMA journal_mode=WAL; CREATE TABLE data(x TEXT); INSERT INTO data VALUES('before'); CREATE TABLE unrelated(x BLOB); INSERT INTO unrelated VALUES(zeroblob(15000));").unwrap();
    let mut scan=Scan::start(pinned(&path),policy(Class::Guarded),None,"synthetic".into(),"test".into(),Timestamp::now()).unwrap();
    assert!(!scan.turn().unwrap());
    db.execute_batch("UPDATE data SET x='after';").unwrap();
    let base=finish(scan);
    let row:String=base.view().unwrap().0.lock().unwrap().query_row("SELECT x FROM data",[],|r|r.get(0)).unwrap();assert_eq!(row,"before");
    let mut old=Vec::new();base.reader(Arc::new(|h|*h),true).unwrap().read_to_end(&mut old).unwrap();
    let previous=Previous {metadata:base.metadata.clone(),extents:base.extents("base").unwrap(),map:base.map.clone()};
    let patch=finish(Scan::start(pinned(&path),policy(Class::Guarded),Some(previous),"synthetic".into(),"test".into(),Timestamp::now()).unwrap());
    assert!(!patch.metadata.full);assert!(patch.metadata.map_offset<patch.metadata.image_bytes);
    let mut body=Vec::new();patch.reader(Arc::new(|h|*h),false).unwrap().read_to_end(&mut body).unwrap();
    let extents=patch.extents("patch").unwrap();patch.metadata.validate(&extents).unwrap();
    let mut reconstructed=Vec::new();for e in &extents {let bytes=if e.blob=="base" {&old} else {&body};reconstructed.extend_from_slice(&bytes[e.offset as usize..(e.offset+e.length) as usize]);}
    let mut expected=Vec::new();patch.reader(Arc::new(|h|*h),true).unwrap().read_to_end(&mut expected).unwrap();assert_eq!(reconstructed,expected);assert_ne!(old,expected);
    for (n,page) in expected.chunks(patch.metadata.page_size as usize).enumerate() {let start=patch.metadata.map_offset as usize+n*32;assert_eq!(&body[start..start+32],blake3::hash(page).as_bytes());}
    let mut bad=patch.metadata.clone();bad.map_offset+=1;assert!(bad.validate(&extents).is_err());
    let mut bad=patch.metadata.clone();bad.page_count+=1;assert!(bad.validate(&extents).is_err());
}

#[test]
fn sqlite_image_overlay_rejects_out_of_range_duplicate_and_overflowing_pages() {
    let old=[ImageExtent {blob:"old".into(),offset:0,length:2048}];
    for changed in [vec![0],vec![1,1],vec![2,1],vec![3],vec![u64::MAX]] {assert!(overlay(&old,2048,1024,&changed,"new").is_err());}
    assert!(overlay(&[ImageExtent {blob:"old".into(),offset:u64::MAX-1023,length:2048}],2048,1024,&[1],"new").is_err());
    assert!(overlay(&old,4096,1024,&[4],"new").is_err());
    let extents=overlay(&old,4096,1024,&[3,4],"new").unwrap();assert_eq!(extents.iter().map(|e|e.length).sum::<u64>(),4096);
}

#[test]
fn sealed_sqlite_images_always_take_full_bases_and_keep_plain_hashes_out_of_metadata() {
    let db=Connection::open_in_memory().unwrap();db.execute_batch("CREATE TABLE data(x); INSERT INTO data VALUES('private synthetic'); BEGIN;").unwrap();
    let image=finish(Scan::start(db,policy(Class::Sealed),None,"synthetic".into(),"test".into(),Timestamp::now()).unwrap());
    assert!(image.metadata.full);assert!(image.metadata.image_hash.is_empty());assert!(image.map.is_none());
    let mut bytes=Vec::new();image.reader(Arc::new(|h|*blake3::keyed_hash(&[7;32],h).as_bytes()),false).unwrap().read_to_end(&mut bytes).unwrap();
    assert_eq!(bytes.len() as u64,image.metadata.evidence_bytes);
    assert_ne!(&bytes[image.metadata.map_offset as usize..image.metadata.map_offset as usize+32],blake3::hash(&bytes[..image.metadata.page_size as usize]).as_bytes());
}
