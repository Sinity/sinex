use super::*;

#[test]
fn tag_name_reports_unsupported_rename_rule() {
    let input = syn::parse_quote! {
        #[serde(tag = "method", rename_all = "camelCase")]
        enum Request { OneValue }
    };
    let error = expand_tag_name(&input).unwrap_err();
    assert_eq!(
        error.to_string(),
        "TagName supports snake_case and kebab-case"
    );
}

#[test]
fn tag_name_requires_a_method_name() {
    let input = syn::parse_quote! {
        #[serde(tag = "not-a-method", rename_all = "kebab-case")]
        enum Request { OneValue }
    };
    let error = expand_tag_name(&input).unwrap_err();
    assert_eq!(error.to_string(), "the tag must be a Rust method name");
}
