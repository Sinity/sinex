use crate::RecordResult;
use crate::drivers::export_archive::{Framing, frame_whole};
use crate::{
    DeclaredSource, Expectation, Framer, Origin, RecordContext, RecordError, Shape, SourceSpec,
};
use crate::{Stateless, StatelessParser};
use sinex_model::json::JsonExt;
use sinex_model::{Class, Timestamp, event_types::media::NoteDocument};
pub(crate) struct NotesParser;
impl StatelessParser for NotesParser {
    type Record<'a> = &'a [u8];
    type Payload = NoteDocument;
    fn record_hash(&self, raw: &[u8], context: &RecordContext<'_>) -> [u8; 32] {
        context.hash_with_path(raw)
    }
    fn parse(
        &self,
        bytes: &&[u8],
        context: &RecordContext<'_>,
    ) -> Result<Vec<NoteDocument>, RecordError> {
        let member = context.member.record("member missing")?;
        let removed_at = member
            .details
            .str("removed_at_ns")
            .and_then(|v| v.parse().ok())
            .and_then(|v| Timestamp::from_unix_ns(v).ok());
        Ok(vec![NoteDocument {
            vault: "synthetic".into(),
            host: context.manifest.detail("host").as_str().map(str::to_owned),
            vault_path: context.manifest.path.clone(),
            native_vault_path_bytes: context
                .manifest
                .detail("native_path_bytes")
                .as_str()
                .map(str::to_owned),
            native_path_bytes: member.details.str("native_path_bytes").map(str::to_owned),
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
        }])
    }
}
pub(crate) fn source() -> DeclaredSource<Stateless<NotesParser>> {
    DeclaredSource {
        parser: Stateless(NotesParser),
        spec: SourceSpec {
            name: "synthetic.notes",
            parser: "synthetic-notes",
            parser_version: 1,
            origin: Origin::Input(Shape::DirSnapshot),
            expect: Expectation::OnDemand,
            expires_after: None,
            purpose: "test",
            class: Class::Open,
            event_types: &["note.document"],
            framer: Framer::export(frame_whole, Framing::Whole),
        },
    }
}
