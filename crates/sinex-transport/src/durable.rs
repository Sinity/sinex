//! Sync file contents before publishing a replacement name.
use std::{
    fs::{File, OpenOptions},
    io,
    path::Path,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplaceStep {
    Write,
    Rename,
    SyncDirectory,
}

/// Temporary sibling, sync, rename over `path`, sync the directory.
pub fn write_replacing(
    path: &Path,
    mode: u32,
    write: impl FnOnce(&mut File) -> io::Result<()>,
) -> Result<(), (ReplaceStep, io::Error)> {
    let directory = path.parent().unwrap_or(Path::new("."));
    let mut temporary = tempfile::NamedTempFile::with_prefix_in(".replace-", directory)
        .map_err(|error| (ReplaceStep::Write, error))?;
    temporary
        .as_file()
        .set_permissions(std::os::unix::fs::PermissionsExt::from_mode(mode))
        .map_err(|error| (ReplaceStep::Write, error))?;
    write(temporary.as_file_mut())
        .and_then(|()| temporary.as_file().sync_all())
        .map_err(|error| (ReplaceStep::Write, error))?;
    temporary
        .persist(path)
        .map_err(|error| (ReplaceStep::Rename, error.error))?;
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|error| (ReplaceStep::SyncDirectory, error))
}

/// Sync `from`, rename it to `to`, sync the directory.
pub fn place(from: &Path, to: &Path) -> io::Result<()> {
    OpenOptions::new().write(true).open(from)?.sync_all()?;
    std::fs::rename(from, to)?;
    File::open(to.parent().unwrap_or(Path::new(".")))?.sync_all()
}

#[cfg(test)]
mod tests;
