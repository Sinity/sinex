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
use crate::binding::{Binding, InputKind, Reader, Settings, stateless};
use crate::drivers::export_archive::ExportArchive;
use crate::registry::SourceSpec;
use std::path::PathBuf;

/// Each product read from a Takeout archive: its spec, its parser and its
/// driver, built from the binding's settings.
type Product = (
    &'static SourceSpec,
    fn(&Settings) -> Result<Box<dyn Source>, String>,
    fn(PathBuf, &Settings) -> Result<ExportArchive, String>,
);

const PRODUCTS: &[Product] = &[
    (
        &activity::MY_ACTIVITY,
        |_| Ok(Box::new(activity::my_activity())),
        |path, s| Ok(activity::my_activity_driver(path, s.host.clone())),
    ),
    (
        &activity::YOUTUBE,
        |_| Ok(Box::new(activity::youtube())),
        |path, s| Ok(activity::youtube_driver(path, s.host.clone())),
    ),
    (
        &keep::SPEC,
        |_| Ok(Box::new(keep::source())),
        |path, s| Ok(keep::driver(path, s.host.clone())),
    ),
    (
        &tasks::SPEC,
        |_| Ok(Box::new(tasks::source())),
        |path, s| Ok(tasks::driver(path, s.host.clone())),
    ),
    (
        &purchases::PLAY_STORE,
        |_| Ok(Box::new(purchases::play_store())),
        |path, s| Ok(purchases::play_store_driver(path, s.host.clone())),
    ),
    (
        &purchases::PURCHASES,
        |_| Ok(Box::new(purchases::purchases())),
        |path, s| Ok(purchases::purchases_driver(path, s.host.clone())),
    ),
    (
        &location::SPEC,
        |_| Ok(Box::new(location::source())),
        |path, s| Ok(location::driver(path, s.host.clone())),
    ),
    (
        &fit::SPEC,
        |s| Ok(Box::new(fit::source(s.zone()?))),
        |path, s| Ok(fit::driver(path, s.host.clone(), s.zone()?)),
    ),
];

/// Products whose records are grouped by local day; their bindings record a
/// zone.
const ZONED: &[&str] = &[fit::SPEC.name];

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
            out.extend(PRODUCTS.iter().map(|p| {
                let settings = if ZONED.contains(&p.0.name) {
                    &zoned
                } else {
                    settings
                };
                Binding::of(p.0, &file, settings.clone())
            }));
        }
        Ok(out)
    }),
    source: |b| {
        if b.source == super::calendar::SPEC.name {
            return (super::calendar::KIND.source)(b);
        }
        (product(b)?.1)(&b.settings)
    },
    reader: Some(|b| -> Result<Box<dyn Reader>, String> {
        if b.source == super::calendar::SPEC.name {
            return (super::calendar::KIND.reader.expect("calendar reads"))(b);
        }
        Ok(stateless((product(b)?.2)(b.input.clone(), &b.settings)?))
    }),
    capture: None,
};
