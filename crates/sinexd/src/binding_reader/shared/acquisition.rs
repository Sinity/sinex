//! Retain a converter's native input through the ordinary material writer.
use super::*;
use sinex_model::{InputRef, MaterialId, ReadingOrder, RevisionChange};

#[allow(clippy::too_many_arguments)]
pub(super) async fn retain(
    conn: &mut PgConnection,
    blobs: &Blobs,
    writer: &WriterHandle,
    run: RunId,
    binding: &BindingReader,
    class: Class,
    access: Access<'_>,
    acquisition: shared_pass::Acquisition,
    totals: &mut Totals,
) -> Result<MaterialId, String> {
    let (cursor, carry) = positions::load(conn, &binding.binding.name)
        .await
        .map_err(|error| error.to_string())?
        .unwrap_or((Value::Null, Value::Null));
    let id = MaterialId::new();
    let next = cursor.clone();
    let path = acquisition.path.clone();
    let expected = acquisition.facts.clone();
    let check = Arc::new(move || {
        let now = drivers::file_facts(&path).map_err(|error| error.to_string())?;
        if same_file(&expected, &now) {
            Ok(())
        } else {
            Err("native input changed after conversion; retry an unchanged revision".into())
        }
    });
    let file = std::fs::File::open(&acquisition.path).map_err(|error| error.to_string())?;
    let acquired = drivers::Acquired {
        input: InputRef {
            kind: "export".into(),
            locator: acquisition.path.display().to_string(),
            host: binding.binding.settings.host.clone(),
        },
        observation: acquisition.facts,
        revision: RevisionChange::Snapshot,
        material: Some(drivers::NewMaterial {
            id,
            bytes: Vec::new(),
            stored: Some(drivers::StoredBytes::FramedFile(Arc::new(file))),
            records: Vec::new(),
            part_index: 0,
            previous_part: None,
            manifest: acquisition.manifest,
            reading_order: Box::new(|_| ReadingOrder(0)),
            cursor_at: Box::new(move |_| Ok(next.clone())),
            aligned: false,
            removed: Vec::new(),
            memory: None,
            owned_files: Vec::new(),
        }),
        cursor_without_material: cursor,
    };
    crate::intake::Intake {
        host: Some(&binding.binding.settings.host),
        writer,
        source: binding.source.clone(),
        binding: &binding.binding.name,
        run,
        material: crate::material_access::MaterialAccess::new(blobs.clone(), class, access)?,
        check,
        priority: crate::writer::Priority::Bulk,
        atomic: true,
        registered: None,
    }
    .submit(acquired, carry, totals)
    .await?;
    Ok(id)
}
