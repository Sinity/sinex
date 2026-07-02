use super::*;
use xtask::sandbox::prelude::sinex_test;

#[sinex_test]
async fn public_ref_roundtrips_punctuated_ids() -> xtask::sandbox::TestResult<()> {
    let parsed: PublicSinexRef = "source-material:0199:abc/def".parse().unwrap();
    assert_eq!(parsed.kind, SinexObjectKind::SourceMaterial);
    assert_eq!(parsed.id, "0199:abc/def");
    assert_eq!(parsed.to_string(), "source-material:0199:abc/def");
    Ok(())
}

#[sinex_test]
async fn public_ref_rejects_invalid_forms() -> xtask::sandbox::TestResult<()> {
    assert_eq!(
        "event".parse::<PublicSinexRef>().unwrap_err(),
        PublicSinexRefParseError::MissingSeparator
    );
    assert_eq!(
        ":id".parse::<PublicSinexRef>().unwrap_err(),
        PublicSinexRefParseError::EmptyKind
    );
    assert_eq!(
        "event:".parse::<PublicSinexRef>().unwrap_err(),
        PublicSinexRefParseError::EmptyId
    );
    assert_eq!(
        "source_material:id".parse::<PublicSinexRef>().unwrap_err(),
        PublicSinexRefParseError::UnknownKind("source_material".to_string())
    );
    Ok(())
}

#[sinex_test]
async fn resolved_object_view_distinguishes_not_found_and_unsupported()
-> xtask::sandbox::TestResult<()> {
    let ref_ = SinexObjectRef::new(SinexObjectKind::SourceDriver, "terminal.fish-history");

    let missing = ResolvedObjectView::not_found(ref_.clone(), "sinexctl.sources.status");
    assert_eq!(missing.public_ref, "source-driver:terminal.fish-history");
    assert_eq!(missing.status, ResolvedObjectStatus::NotFound);
    assert_eq!(
        missing.source_surface.as_deref(),
        Some("sinexctl.sources.status")
    );

    let unsupported = ResolvedObjectView::unsupported(ref_);
    assert_eq!(unsupported.status, ResolvedObjectStatus::Unsupported);
    assert!(unsupported.source_surface.is_none());
    Ok(())
}
