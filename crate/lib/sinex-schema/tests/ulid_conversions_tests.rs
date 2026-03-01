use sinex_schema::primitives::Ulid;
use sinex_schema::primitives::conversions::{UlidArrayExt, ulid_to_uuid, uuid_to_ulid};
use xtask::sandbox::sinex_test;

#[sinex_test]
async fn ulid_round_trips() -> color_eyre::eyre::Result<()> {
    let ulid = Ulid::new();
    let uuid = ulid_to_uuid(ulid);
    let converted = uuid_to_ulid(uuid);
    assert_eq!(ulid, converted);
    Ok(())
}

#[sinex_test]
async fn ulid_array_conversions() -> color_eyre::eyre::Result<()> {
    let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
    let uuids = ulids.to_uuid_vec();
    assert_eq!(ulids.len(), uuids.len());

    for (ulid, uuid) in ulids.iter().zip(uuids.iter()) {
        assert_eq!(*ulid, uuid_to_ulid(*uuid));
    }

    Ok(())
}
