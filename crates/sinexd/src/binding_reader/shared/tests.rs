use super::*;
use std::io::{Cursor, Seek, SeekFrom};

#[devkit::test]
fn interruptible_readers_honor_cancellation_for_reads_and_seeks() {
    let stop = Arc::new(AtomicBool::new(false));
    let mut first = InterruptibleReader {
        input: Cursor::new(b"first"),
        stop: stop.clone(),
    };
    let mut second = InterruptibleReader {
        input: Cursor::new(b"second"),
        stop: stop.clone(),
    };
    let mut byte = [0];
    first.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [b'f']);
    second.seek(SeekFrom::Start(1)).unwrap();
    second.read_exact(&mut byte).unwrap();
    assert_eq!(byte, [b'e']);
    stop.store(true, Ordering::Relaxed);
    assert!(first.read(&mut byte).is_err());
    assert!(second.read(&mut byte).is_err());
    assert!(first.seek(SeekFrom::Start(0)).is_err());
    assert!(second.seek(SeekFrom::Start(0)).is_err());
}
