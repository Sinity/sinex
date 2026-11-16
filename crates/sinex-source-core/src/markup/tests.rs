use super::{MarkupText, text as markup_text};

const SPLIT_MARKUP: &str = "<w:document><w:p title='a > b'><w:t>Zażółć &amp; gęślą</w:t></w:p>\
    <!-- hidden <w:p> --><w:p><w:t>jaźń &#0000000000000000000065; &#x1F34B;</w:t><w:br/></w:p>\
    <style>skip &amp; this</style>tail &unknown; & bare</w:document>";

proptest::proptest! {
    #[test]
    fn markup_read_in_pieces_matches_markup_read_whole(cuts in proptest::collection::vec(0usize..200, 0..12)) {
        let blocks = ["w:p", "w:br"];
        let dropped = ["style"];
        let whole = markup_text(SPLIT_MARKUP, &blocks, &dropped);
        let mut reader = MarkupText::new(&blocks, &dropped);
        let mut cuts: Vec<usize> = cuts
            .into_iter()
            .map(|c| c.min(SPLIT_MARKUP.len()))
            .filter(|c| SPLIT_MARKUP.is_char_boundary(*c))
            .collect();
        cuts.sort_unstable();
        let mut from = 0;
        for cut in cuts {
            reader.feed(&SPLIT_MARKUP[from..cut]);
            from = cut;
        }
        reader.feed(&SPLIT_MARKUP[from..]);
        proptest::prop_assert_eq!(reader.finish(), whole);
    }
}

#[devkit::test]
fn markup_read_in_pieces_reads_references_tags_and_comments() {
    assert_eq!(
        markup_text(SPLIT_MARKUP, &["w:p", "w:br"], &["style"]),
        "Zażółć & gęślą\njaźń A \u{1F34B}\ntail &unknown; & bare"
    );
}
