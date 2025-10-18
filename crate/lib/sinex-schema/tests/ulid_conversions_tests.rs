use sinex_schema::ulid::Ulid;
use sinex_schema::ulid_conversions::{ulid_to_uuid, uuid_to_ulid, UlidArrayExt};
use sinex_test_utils::sinex_test;

#[sinex_test]
fn ulid_round_trips() -> color_eyre::eyre::Result<()> {
    let ulid = Ulid::new();
    let uuid = ulid_to_uuid(ulid);
    let converted = uuid_to_ulid(uuid);
    assert_eq!(ulid, converted);
    Ok(())
}

#[sinex_test]
fn ulid_array_conversions() -> color_eyre::eyre::Result<()> {
    let ulids = vec![Ulid::new(), Ulid::new(), Ulid::new()];
    let uuids = ulids.to_uuid_vec();
    assert_eq!(ulids.len(), uuids.len());

    for (ulid, uuid) in ulids.iter().zip(uuids.iter()) {
        assert_eq!(*ulid, uuid_to_ulid(*uuid));
    }

    Ok(())
}
