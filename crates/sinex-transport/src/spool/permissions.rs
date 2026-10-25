//! The configured local store shares spool bytes through its protected group.
//! Other roots keep the temporary-file defaults, including private uploads.
use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
use std::path::Path;

pub(super) fn prepare_directory(root: &Path, leaf: &Path) -> Result<Option<u32>, String> {
    let group = match std::fs::metadata(root) {
        Ok(meta) if meta.is_dir() && meta.mode() & 0o7777 == 0o2770 => meta.gid(),
        Ok(_) => {
            std::fs::create_dir_all(leaf).map_err(super::error)?;
            return Ok(None);
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir_all(leaf).map_err(super::error)?;
            return Ok(None);
        }
        Err(e) => return Err(super::error(e)),
    };
    let mut at = root.to_owned();
    for component in leaf.strip_prefix(root).map_err(super::error)?.components() {
        if !matches!(component, std::path::Component::Normal(_)) {
            return Err("producer spool directory leaves the store root".into());
        }
        at.push(component);
        let created = match std::fs::DirBuilder::new().mode(0o2770).create(&at) {
            Ok(()) => true,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => false,
            Err(e) => return Err(super::error(e)),
        };
        let meta = std::fs::symlink_metadata(&at).map_err(super::error)?;
        if !meta.is_dir() || meta.gid() != group {
            return Err("producer spool directory does not belong to the store group".into());
        }
        if created {
            // Override a restrictive caller umask only for this newly created
            // managed directory. Never adopt or chmod an existing private tree.
            std::fs::File::open(&at)
                .and_then(|dir| dir.set_permissions(std::fs::Permissions::from_mode(0o2770)))
                .map_err(super::error)?;
        } else if meta.mode() & 0o7777 != 0o2770 {
            return Err("producer spool directory requires configured group mode 2770".into());
        }
    }
    Ok(Some(group))
}

pub(super) fn share_completed_file(file: &std::fs::File, group: Option<u32>) -> Result<(), String> {
    if let Some(group) = group {
        if file.metadata().map_err(super::error)?.gid() != group {
            return Err("producer spool file does not belong to the store group".into());
        }
        // The receiver reads a verified copy and unlinks roles through the
        // shared directory. It does not need to write the sender's inode.
        file.set_permissions(std::fs::Permissions::from_mode(0o640))
            .map_err(super::error)?;
    }
    Ok(())
}

pub(super) fn check_existing_file(path: &Path, group: Option<u32>) -> Result<(), String> {
    if let Some(group) = group {
        match std::fs::symlink_metadata(path) {
            Ok(meta) if meta.is_file() && meta.gid() == group && meta.mode() & 0o7777 == 0o640 => {}
            Ok(_) => {
                return Err(
                    "existing producer spool file requires configured group mode 0640".into(),
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(super::error(e)),
        }
    }
    Ok(())
}
