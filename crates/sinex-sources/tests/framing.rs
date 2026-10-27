use sinex_source_core::Framer;
use sinex_source_core::drivers::export_archive::Framing;
use std::io::{Cursor, Read};
type SliceFramer = fn(&[u8]) -> (Vec<(usize, usize)>, usize);

struct Tiny<R>(R, usize);
impl<R: Read> Read for Tiny<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = out.len().min(self.1);
        self.0.read(&mut out[..n])
    }
}

#[devkit::test]
fn reader_framing_keeps_exact_ranges_across_boundaries() {
    use sinex_source_core::drivers::export_archive::{frame_csv, frame_lines, frame_mbox};
    use sinex_sources::sources::{asciinema, operator, zsh};
    let examples: Vec<(Framer, &[u8])> = vec![
        (
            Framer::export(frame_lines, Framing::Lines),
            b"first\r\n\nlast",
        ),
        (
            Framer::export(frame_csv, Framing::Csv),
            b"a,b\r\n1,\"line\nline\"\r\n2,last",
        ),
        (
            Framer::export(frame_mbox, Framing::Mbox),
            b"From a\nBody A\nFrom b\nBody B\n",
        ),
        (
            Framer::incremental(zsh::frame),
            b": 1:0;first\\\ncontinued\n: 2:0;last\npartial",
        ),
        (
            Framer::incremental(operator::frame_log),
            b"front matter\n- **a**\nfirst\n- **b**\nlast\npartial",
        ),
        (
            Framer::incremental(asciinema::cast_spans),
            b"{\"version\":2}\n[0,\"o\",\"a\"]\n[1,\"r\",\"80x24\"]\n[2,\"o\",\"b\"]\n",
        ),
        (
            sinex_sources::sources::raindrop::SPEC.framer,
            b"title,url\n{csv-cell,https://example.test/\n",
        ),
    ];
    for (framer, bytes) in examples {
        for size in [1, 2, 7, 65536] {
            assert_eq!(
                framer.ranges(&mut Tiny(Cursor::new(bytes), size)).unwrap(),
                framer.frame(bytes).0,
                "chunk {size}, {framer:?}"
            );
        }
    }
}

#[devkit::test]
fn streamed_json_ranges_keep_raw_offsets_and_malformed_document_fallback() {
    use sinex_source_core::drivers::export_archive::frame_json_array;
    use sinex_sources::sources::takeout_chrome;
    let cases: Vec<(Framer, &[u8])> = vec![
        (
            Framer::export(frame_json_array, Framing::JsonArray("")),
            r#" [ 12, -3.5e2,true,false,null,"hello",{"unicode":"ż😀","nested":[1,2]}, [ ] ] "#
                .as_bytes(),
        ),
        (
            Framer::export(
                takeout_chrome::frame,
                Framing::JsonArray("/Browser History"),
            ),
            br#"{"ignore":[1,2],"Browser History": [ {"url":"a"}, {"url":"a"} ]}"#,
        ),
        (
            Framer::export(frame_json_array, Framing::JsonArray("")),
            b"[{\"a\":1},2,broken tail",
        ),
        (
            Framer::export(
                takeout_chrome::frame,
                Framing::JsonArray("/Browser History"),
            ),
            b"{\"Browser History\":[1],\"Browser History\":[2,3]}",
        ),
        (
            Framer::export(
                takeout_chrome::frame,
                Framing::JsonArray("/Browser History"),
            ),
            b"{\"Browser History\":{\"wrong\":1},\"Browser History\":[2,3]}",
        ),
    ];
    let extra = (
        Framer::export(
            takeout_chrome::frame,
            Framing::JsonArray("/Browser History"),
        ),
        b"{\"Browser History\":1e400,\"Browser History\":[2,3]}".as_slice(),
    );
    let cases = cases.into_iter().chain([extra]);
    for (framer, bytes) in cases {
        for size in [1, 2, 5, 65536] {
            assert_eq!(
                framer.ranges(&mut Tiny(Cursor::new(bytes), size)).unwrap(),
                framer.frame(bytes).0,
                "chunk {size}"
            );
        }
    }
}

#[devkit::test]
fn custom_append_framing_keeps_only_an_undecided_record_window() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static PEAK: AtomicUsize = AtomicUsize::new(0);
    fn measured(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
        PEAK.fetch_max(bytes.len(), Ordering::Relaxed);
        sinex_source_core::drivers::append_file::lines(bytes)
    }
    let body = b"synthetic row\n".repeat(100_000);
    let ranges = Framer::incremental(measured)
        .ranges(&mut Cursor::new(&body))
        .unwrap();
    assert_eq!(ranges.len(), 100_000);
    assert_eq!(ranges.last().unwrap().1, body.len());
    assert!(PEAK.load(Ordering::Relaxed) < (128 << 10));
    assert!(body.len() > 10 * PEAK.load(Ordering::Relaxed));
}

#[devkit::test]
fn whole_reader_keeps_each_declarations_empty_record_semantics() {
    use sinex_source_core::drivers::{export_archive::frame_whole, media};
    for slice in [frame_whole as SliceFramer, media::whole] {
        let framer = Framer::export(slice, Framing::Whole);
        for bytes in [b"".as_slice(), b"one record".as_slice()] {
            assert_eq!(
                framer.ranges(&mut Cursor::new(bytes)).unwrap(),
                framer.frame(bytes).0
            );
        }
    }
}

#[devkit::test]
fn tail_ranges_match_completed_records_with_multiline_and_large_records() {
    use sinex_source_core::drivers::append_file::TailBounds;
    let framer = Framer::incremental(sinex_sources::sources::zsh::frame);
    let bytes = b": 1:0;first\\\ncontinued\n: 2:0;last\npartial";
    let expected = framer.frame(bytes).0;
    for chunk in [1, 7, 65536] {
        let ranges = framer
            .tail_ranges(
                &mut Tiny(Cursor::new(bytes), chunk),
                TailBounds {
                    records: 1,
                    bytes: 1,
                },
            )
            .unwrap();
        assert_eq!(ranges.0, expected[expected.len() - 1..]);
        assert!(!ranges.1);
    }
}
