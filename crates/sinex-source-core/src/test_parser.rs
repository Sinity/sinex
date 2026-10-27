use crate::drivers::export_archive::{Framing, frame_whole};
use crate::{DeclaredSource, Driver, Framer, Parser, RecordContext, RecordError, SourceSpec};
use sinex_model::{Class, Timestamp, event_types::media::NoteDocument};
pub(crate) struct NotesParser;
impl Parser for NotesParser {
    type Record<'a> = &'a [u8];
    type Carry = ();
    type Payload = NoteDocument;
    fn parse(
        &self,
        bytes: &&[u8],
        _: &(),
        context: &RecordContext<'_>,
    ) -> Result<(Vec<NoteDocument>, ()), RecordError> {
        let member = context
            .member
            .ok_or_else(|| RecordError::new("member missing"))?;
        let removed_at = member
            .details
            .get("removed_at_ns")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse().ok())
            .and_then(|v| Timestamp::from_unix_ns(v).ok());
        Ok((
            vec![NoteDocument {
                vault: "synthetic".into(),
                path: member.path.clone(),
                text: removed_at
                    .is_none()
                    .then(|| String::from_utf8_lossy(bytes).into_owned()),
                size: bytes.len() as u64,
                modified_at: None,
                removed_at,
                title: None,
                note_id: None,
                tags: None,
                wikilinks: None,
                front_matter: None,
            }],
            (),
        ))
    }
}
pub(crate) fn source() -> DeclaredSource<NotesParser> {
    DeclaredSource {
        parser: NotesParser,
        spec: SourceSpec {
            name: "synthetic.notes",
            parser: "synthetic-notes",
            parser_version: 1,
            driver: Driver::DirSnapshot,
            excisable: false,
            purpose: "test",
            class: Class::Open,
            event_types: &["note.document"],
            framer: Framer::export(frame_whole, Framing::Whole),
        },
    }
}
