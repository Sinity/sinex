//! Strict parser selection for `stage PATH`. A format declares the exact
//! structures it accepts: a database's tables, a text file's first line, an
//! archive's member layout. Detection tests only these declarations; it never
//! guesses from a file name, and a format that declares nothing is assigned
//! only by name (`--parser`) or by its canonical location.
use std::io::Read;
use std::path::Path;

/// One structure a format accepts.
#[derive(Clone, Copy, Debug)]
pub enum Structure {
    /// A SQLite database holding `table` with at least `columns`.
    SqliteTable {
        table: &'static str,
        columns: &'static [&'static str],
    },
    /// A text file whose first line has this form.
    FirstLine(fn(&[u8]) -> bool),
    /// An archive (ZIP, tar or gzip-compressed tar) with at least one member
    /// whose path has this layout. Which members each source then reads is its
    /// binding's own selection.
    ArchiveLayout(fn(&str) -> bool),
}

/// The first bytes of every SQLite database file.
#[cfg(feature = "sqlite")]
const SQLITE_HEADER: &[u8] = b"SQLite format 3\0";

/// How much of a file is read to find its first line. Detection needs only
/// the line's form; a file whose first line is longer simply matches no
/// first-line structure and is still staged.
const FIRST_LINE_BYTES: usize = 64 << 10;

impl Structure {
    /// Whether the file at `path` has this structure as a whole. An archive
    /// layout is tested against members, never here.
    pub fn matches_file(&self, path: &Path) -> Result<bool, String> {
        match self {
            Structure::SqliteTable { table, columns } => sqlite_table(path, table, columns),
            Structure::FirstLine(form) => Ok(first_line(path)?.is_some_and(|line| form(&line))),
            Structure::ArchiveLayout(_) => Ok(false),
        }
    }

    /// Whether an archive member at `member` has this layout.
    pub fn matches_member(&self, member: &str) -> bool {
        match self {
            Structure::ArchiveLayout(layout) => layout(member),
            _ => false,
        }
    }

    pub fn is_archive_layout(&self) -> bool {
        matches!(self, Structure::ArchiveLayout(_))
    }
}

/// The file's first line without its line ending, or `None` when the file is
/// empty or its first line does not end within [`FIRST_LINE_BYTES`].
fn first_line(path: &Path) -> Result<Option<Vec<u8>>, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut head = Vec::new();
    file.take(FIRST_LINE_BYTES as u64)
        .read_to_end(&mut head)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let end = match head.iter().position(|b| *b == b'\n') {
        Some(end) => end,
        None if head.len() < FIRST_LINE_BYTES && !head.is_empty() => head.len(),
        None => return Ok(None),
    };
    let mut line = head[..end].to_vec();
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    Ok(Some(line))
}

#[cfg(feature = "sqlite")]
fn starts_with(path: &Path, prefix: &[u8]) -> Result<bool, String> {
    let file = std::fs::File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    let mut head = Vec::with_capacity(prefix.len());
    file.take(prefix.len() as u64)
        .read_to_end(&mut head)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(head == prefix)
}

/// The database is opened read-only, never with `immutable`, so a live
/// database's committed write-ahead log is seen and nothing is written.
#[cfg(feature = "sqlite")]
fn sqlite_table(path: &Path, table: &str, columns: &[&str]) -> Result<bool, String> {
    if !starts_with(path, SQLITE_HEADER)? {
        return Ok(false);
    }
    let connection = rusqlite::Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| format!("{}: {e}", path.display()))?;
    let mut statement = connection
        .prepare("SELECT name FROM pragma_table_info(?1)")
        .map_err(|e| format!("{}: {e}", path.display()))?;
    let present: Vec<String> = statement
        .query_map([table], |row| row.get(0))
        .and_then(Iterator::collect)
        .map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(!present.is_empty() && columns.iter().all(|c| present.iter().any(|p| p == c)))
}

/// Without SQLite support no file is recognised as a database.
#[cfg(not(feature = "sqlite"))]
fn sqlite_table(_path: &Path, _table: &str, _columns: &[&str]) -> Result<bool, String> {
    Ok(false)
}

#[cfg(test)]
mod tests;
