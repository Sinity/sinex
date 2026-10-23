use super::*;
use std::io::{Cursor, Read};

struct Tiny<R>(R, usize);
impl<R: Read> Read for Tiny<R> {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        let n = out.len().min(self.1);
        self.0.read(&mut out[..n])
    }
}

#[test]
fn reader_framing_keeps_exact_legacy_ranges_across_boundaries() {
    use crate::drivers::export_archive::{frame_csv, frame_lines, frame_mbox};
    use crate::sources::{asciinema, operator, zsh};
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
            crate::sources::raindrop::SPEC.framer,
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

#[test]
fn streamed_json_ranges_keep_raw_offsets_and_malformed_document_fallback() {
    use crate::drivers::export_archive::frame_json_array;
    use crate::sources::takeout_chrome;
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

#[test]
fn custom_append_framing_keeps_only_an_undecided_record_window() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static PEAK: AtomicUsize = AtomicUsize::new(0);
    fn measured(bytes: &[u8]) -> (Vec<(usize, usize)>, usize) {
        PEAK.fetch_max(bytes.len(), Ordering::Relaxed);
        crate::drivers::append_file::lines(bytes)
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
