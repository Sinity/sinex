pub(crate) fn body(material: &NewMaterial) -> &[u8] {
    match &material.stored {
        Some(StoredBytes::FramedMemory(bytes)) => bytes,
        _ => &material.bytes,
    }
}
