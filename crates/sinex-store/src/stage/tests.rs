use super::*;

#[devkit::test]
fn escapes_copy_metacharacters() {
    let mut s = String::new();
    escape(&mut s, "a\tb\nc\\d\re");
    assert_eq!(s, "a\\tb\\nc\\\\d\\re");
}
