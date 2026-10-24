//! The alignment tail of an append input, rebuilt from its admitted history.
use super::*;
use sinex_model::{MaterialId, RecordLocation};
use sinex_sources::drain::RecordRead;
use sinex_sources::drivers::append_file::AppendTail;

pub(crate) async fn completed_tail(
    conn: &mut PgConnection,
    blobs: &Blobs,
    source: &dyn Source,
    cursor: &Value,
    authority: Option<&crate::sealed_access::Authority>,
    stop: watch::Receiver<bool>,
) -> Result<AppendTail, String> {
    let mut tail = AppendTail::default();
    let Some(last) = cursor
        .get("material")
        .and_then(Value::as_str)
        .and_then(|v| uuid::Uuid::parse_str(v).ok())
    else {
        if cursor.is_null() {
            return Ok(tail);
        }
        return Err("append history lacks its retained material identity".into());
    };
    let check: crate::retained::ReadCheck = Arc::new(move || {
        if *stop.borrow() || sinex_transport::private_mode::PrivateMode::default().on() {
            return Err("append history restore cancelled".into());
        }
        Ok(())
    });
    check()?;
    let chain = sinex_store::replay::current_revision_chain_ending(conn, last)
        .await
        .map_err(|e| e.to_string())?;
    let through = cursor
        .get("through")
        .and_then(Value::as_u64)
        .ok_or("append history lacks its admitted byte cut")?;
    let mut child_start = None;
    let mut oldest_start = None;
    let mut full = false;
    for (id, _) in chain.into_iter().rev() {
        check()?;
        let opened =
            crate::retained::open_checked(conn, blobs, MaterialId(id), authority, check.clone())
                .await?;
        let reference = &opened.reference;
        if reference.parser != source.spec().parser
            || reference.parser_version != source.spec().parser_version
        {
            return Err("append history requires its recorded parser version".into());
        }
        let start = reference
            .manifest
            .get("file_offset")
            .and_then(Value::as_u64)
            .ok_or("append history has no physical start")?;
        let cut = if id == last {
            if through < reference.length {
                return Err(
                    "partially admitted append requires exact retained pending resumption".into(),
                );
            }
            through
        } else {
            // Same-revision child start is the actual preceding provider cut;
            // the predecessor link supplies its material identity.
            child_start
                .and_then(|offset: u64| offset.checked_sub(start))
                .ok_or("append predecessor cut is contradictory")?
        };
        if cut > reference.length {
            return Err("append admitted cut exceeds its retained body".into());
        }
        let records = opened.body.records(&Value::Null, source.spec().framer)?;
        for (start, end) in records
            .into_iter()
            .rev()
            .filter(|(_, end)| *end as u64 <= cut)
        {
            check()?;
            if end <= start || end as u64 > reference.length {
                return Err("invalid completed append range".into());
            }
            let location = RecordLocation {
                material: reference.id,
                anchor: start as u64,
            };
            let mut raw = vec![0; end - start];
            opened
                .body
                .read_exact_at(start as u64, &mut raw)
                .map_err(|e| e.to_string())?;
            if !tail.push_raw(location, &raw).map_err(|e| e.to_string())? {
                full = true;
                break;
            }
        }
        child_start = Some(start);
        oldest_start = Some(start);
        if full {
            break;
        }
    }
    if !full && oldest_start.is_some_and(|start| start != 0) {
        return Err("append history lacks its earlier physical prefix".into());
    }
    check()?;
    Ok(tail)
}
