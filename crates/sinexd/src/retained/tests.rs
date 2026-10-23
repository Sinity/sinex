use super::*;
use std::io::Cursor;

#[test]
fn streamed_blanking_matches_original_bytes_across_small_buffers_and_overlaps() {
    let original=b"a\nb\nc\nlast\nend";
    let spans=vec![(0,6),(4,10),(10,14)];
    let mut expected=original.to_vec();
    sinex_store::excision::blank(&mut expected,&spans);
    for size in [1,2,3,7,64] {
        let mut reader=Blanked {reader:Cursor::new(original),spans:spans.clone(),position:0};
        let mut actual=Vec::new();
        let mut block=vec![0;size];
        loop {let n=reader.read(&mut block).unwrap();if n==0 {break;}actual.extend_from_slice(&block[..n]);}
        assert_eq!(actual,expected,"buffer {size}");
    }
}

#[test]
fn retained_record_reads_are_exact_and_propagate_backing_faults() {
    use sinex_sources::drain::RecordRead;
    let retained=Retained::new(Cursor::new(b"alpha\nbeta\n"),11);
    let mut row=[0;5];
    retained.read_exact_at(6,&mut row).unwrap();
    assert_eq!(&row,b"beta\n");
    assert!(retained.read_exact_at(7,&mut row).is_err());
}
