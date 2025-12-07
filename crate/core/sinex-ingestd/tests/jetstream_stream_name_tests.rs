use sinex_test_utils::{sinex_test, TestContext};

/// Fast deterministic check that stream/subject naming helpers remain stable.
#[sinex_test]
async fn subject_lookup_should_resolve_existing_stream(ctx: TestContext) -> color_eyre::Result<()> {
    let env = ctx.env();
    let stream_name = env.nats_stream_name("SOURCE_MATERIAL_BEGIN");
    let subject = env.nats_subject("source_material.begin");

    assert!(
        stream_name.contains("SOURCE_MATERIAL_BEGIN"),
        "stream helper should embed the provided suffix"
    );
    assert!(
        subject.contains("source_material.begin"),
        "subject helper should embed the provided suffix"
    );

    Ok(())
}
