use super::parse_event_dispatch_attr;

#[test]
#[ignore = "sinex-uofj open: parse_event_dispatch_attr strips surrounding quote characters from \
            the raw literal token text instead of decoding it as a Rust string literal, so escape \
            sequences (\\n, \\t, \\\", ...) are never decoded"]
fn parse_event_dispatch_attr_decodes_escape_sequences() {
    let attr: syn::Attribute = syn::parse_quote!(#[event_dispatch("line\nbreak" => "evt")]);
    let cases = parse_event_dispatch_attr(&attr).expect("valid event_dispatch attribute");

    assert_eq!(cases.len(), 1);
    let (discriminator, event_type) = &cases[0];
    assert_eq!(
        discriminator, "line\nbreak",
        "discriminator must be the DECODED string (real newline), not the raw \
         escaped token text `line\\nbreak` (backslash-n, two characters) -- \
         parse_event_dispatch_attr currently strips quotes via string slicing \
         instead of using syn::LitStr::value() to decode escapes"
    );
    assert_eq!(event_type, "evt");
}
