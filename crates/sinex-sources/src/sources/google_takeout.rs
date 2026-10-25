//! Google Takeout products, each its own source over the same archives. Chrome
//! history is `takeout_chrome`, mail is `mail`, calendars are `calendar`.
pub mod activity;
pub mod cells;
pub mod fit;
pub mod keep;
pub mod location;
pub mod purchases;
pub mod tasks;

use crate::Source;
use crate::binding::{Binding, InputKind, Reading, shared};
use crate::drivers::export_archive::ExportArchive;
use crate::registry::SourceSpec;
use std::path::PathBuf;

/// Each product read from a Takeout archive: its spec, its parser and its
/// driver.
type Product = (
    &'static SourceSpec,
    fn() -> Box<dyn Source>,
    fn(PathBuf, String) -> ExportArchive,
);

const PRODUCTS: &[Product] = &[
    (
        &activity::MY_ACTIVITY,
        || Box::new(activity::my_activity()),
        activity::my_activity_driver,
    ),
    (
        &activity::YOUTUBE,
        || Box::new(activity::youtube()),
        activity::youtube_driver,
    ),
    (&keep::SPEC, || Box::new(keep::source()), keep::driver),
    (&tasks::SPEC, || Box::new(tasks::source()), tasks::driver),
    (
        &purchases::PLAY_STORE,
        || Box::new(purchases::play_store()),
        purchases::play_store_driver,
    ),
    (
        &purchases::PURCHASES,
        || Box::new(purchases::purchases()),
        purchases::purchases_driver,
    ),
    (
        &location::SPEC,
        || Box::new(location::source()),
        location::driver,
    ),
    (&fit::SPEC, || Box::new(fit::source()), fit::driver),
];

fn product(b: &Binding) -> Result<&'static Product, String> {
    PRODUCTS
        .iter()
        .find(|p| p.0.name == b.source)
        .ok_or_else(|| format!("{}: not a Takeout product", b.source))
}

/// `--source takeout=PATH`: every product of each Takeout archive in PATH,
/// plus the calendars in it; each product its own binding, so each keeps its
/// own cursor.
pub const KIND: InputKind = InputKind {
    name: "takeout",
    specs: || PRODUCTS.iter().map(|p| p.0).collect(),
    plan: Some(|input, settings| {
        let zoned = settings.clone().with_zone()?;
        let mut out = Vec::new();
        for file in crate::binding::files(input, false, crate::binding::any) {
            out.push(Binding::of(&super::calendar::SPEC, &file, zoned.clone()));
            out.extend(
                PRODUCTS
                    .iter()
                    .map(|p| Binding::of(p.0, &file, settings.clone())),
            );
        }
        Ok(out)
    }),
    source: |b| {
        if b.source == super::calendar::SPEC.name {
            return (super::calendar::KIND.source)(b);
        }
        Ok((product(b)?.1)())
    },
    reader: Some(|b| -> Result<Reading, String> {
        if b.source == super::calendar::SPEC.name {
            return (super::calendar::KIND.reader.expect("calendar reads"))(b);
        }
        Ok(shared((product(b)?.2)(
            b.input.clone(),
            b.settings.host.clone(),
        )))
    }),
    capture: None,
};
