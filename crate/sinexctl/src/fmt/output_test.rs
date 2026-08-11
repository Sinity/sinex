use super::{format_list, format_single};
use crate::model::OutputFormat;
use serde::Serialize;
use xtask::sandbox::sinex_test;

#[derive(Serialize)]
struct Item {
    value: u32,
}

/// sinex-c96w: `OutputFormat::Dot` is handled inconsistently across the sinexctl
/// formatting layers. `render_envelope`/`render_finite_envelope` (envelope.rs)
/// explicitly reject `Dot` with an error, since Dot output only makes sense for
/// graph commands. `format_list` instead silently renders it as JSON, giving a
/// non-graph command misleading JSON-like output under `--format dot` instead of
/// the same hard rejection `render_envelope` gives.
#[sinex_test]
async fn format_list_rejects_dot_format_like_render_envelope_does() -> xtask::sandbox::TestResult<()>
{
    let items = vec![Item { value: 1 }, Item { value: 2 }];
    let result = format_list(&items, &OutputFormat::Dot, "no items", |_| String::new());

    assert!(
        result.is_err(),
        "format_list must reject OutputFormat::Dot the same way render_envelope does \
         instead of silently rendering it as JSON; sinex-c96w",
    );
    Ok(())
}

#[sinex_test]
async fn format_single_rejects_dot_format_like_render_envelope_does()
-> xtask::sandbox::TestResult<()> {
    let item = Item { value: 1 };
    let result = format_single(&item, &OutputFormat::Dot, |_| String::new());

    assert!(
        result.is_err(),
        "format_single must reject OutputFormat::Dot the same way render_envelope does \
         instead of silently rendering it as JSON; sinex-c96w",
    );
    Ok(())
}
