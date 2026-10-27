//! PAX members keep native names, file metadata and sparse data ranges.
use super::{DriverError, io, metadata_mtime, same_file};
use sinex_model::manifest::{ExtendedAttribute, FileExtent, FileNode};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

pub(super) type Archive = tar::Builder<File>;

pub(super) struct Captured {
    pub offset: usize,
    pub length: usize,
    pub hash: [u8; 32],
    pub node: FileNode,
}

/// The outer error belongs to the archive; the inner error belongs to the file.
pub(super) fn capture(
    path: &Path,
    relative: &Path,
    output: &mut Archive,
    size: u64,
    changed: i128,
) -> Result<Result<Option<Captured>, String>, DriverError> {
    let mut input = match File::open(path) {
        Ok(file) => file,
        Err(error) => return Ok(Err(error.to_string())),
    };
    let facts = (|| -> std::io::Result<_> {
        let before = input.metadata()?;
        if !before.is_file() {
            return Err(std::io::Error::other("input is not a regular file"));
        }
        if before.len() != size || change_time(&before) != changed {
            return Ok(None);
        }
        let attributes = attributes(&input)?;
        let sparse = extents(&input, size)?;
        input.rewind()?;
        let mut hash = blake3::Hasher::new();
        let mut bytes = [0; 64 * 1024];
        loop {
            let count = input.read(&mut bytes)?;
            if count == 0 {
                break;
            }
            hash.update(&bytes[..count]);
        }
        if !same_file(&before, &input.metadata()?) {
            return Ok(None);
        }
        Ok(Some((
            before,
            attributes,
            sparse,
            *hash.finalize().as_bytes(),
        )))
    })();
    let (before, attributes, sparse, hash) = match facts {
        Ok(Some(facts)) => facts,
        Ok(None) => return Ok(Ok(None)),
        Err(error) => return Ok(Err(error.to_string())),
    };
    let node = FileNode {
        mode: before.mode(),
        uid: before.uid(),
        gid: before.gid(),
        size,
        inode: before.ino(),
        device_id: before.dev(),
        links: before.nlink(),
        mtime_ns: metadata_mtime(&before).to_string(),
        atime_ns: nanos(before.atime(), before.atime_nsec()).to_string(),
        ctime_ns: change_time(&before).to_string(),
        extended_attributes: attributes,
        sparse,
    };
    let mut fields = BTreeMap::from([
        (b"path".to_vec(), relative.as_os_str().as_bytes().to_vec()),
        (
            b"mtime".to_vec(),
            time(metadata_mtime(&before)).into_bytes(),
        ),
        (
            b"atime".to_vec(),
            time(nanos(before.atime(), before.atime_nsec())).into_bytes(),
        ),
        (b"ctime".to_vec(), time(change_time(&before)).into_bytes()),
    ]);
    if relative.to_str().is_none() {
        fields.insert(b"hdrcharset".to_vec(), b"BINARY".to_vec());
    }
    for attribute in &node.extended_attributes {
        let mut name = b"SCHILY.xattr.".to_vec();
        name.extend_from_slice(&attribute.name);
        fields.insert(name, attribute.value.clone());
        if attribute.name == b"system.posix_acl_access" {
            match acl(&attribute.value) {
                Ok(value) => {
                    fields.insert(b"SCHILY.acl.access".to_vec(), value.into_bytes());
                }
                Err(error) => return Ok(Err(error.to_string())),
            }
        }
    }
    let length = match &node.sparse {
        None => size,
        Some(extents) => {
            let mut map = extents
                .iter()
                .flat_map(|extent| [extent.offset.to_string(), extent.length.to_string()])
                .collect::<Vec<_>>();
            // The trailing zero range retains a hole reaching the logical EOF.
            map.extend([size.to_string(), "0".into()]);
            fields.insert(b"GNU.sparse.map".to_vec(), map.join(",").into_bytes());
            fields.insert(b"GNU.sparse.size".to_vec(), size.to_string().into_bytes());
            fields.insert(
                b"GNU.sparse.numblocks".to_vec(),
                (extents.len() + 1).to_string().into_bytes(),
            );
            fields.insert(
                b"GNU.sparse.name".to_vec(),
                relative.as_os_str().as_bytes().to_vec(),
            );
            extents.iter().map(|extent| extent.length).sum()
        }
    };
    extensions(output, fields).map_err(io)?;
    let mut header = header(
        length,
        before.mode(),
        before.uid(),
        before.gid(),
        before.mtime().max(0) as u64,
    )?;
    if node.sparse.is_some() {
        header.set_path("GNUSparseFile/data").map_err(io)?;
    }
    header.set_cksum();
    let offset = position(output)?
        .checked_add(512)
        .ok_or_else(|| DriverError::Io("tar offset exceeds addressable size".into()))?;
    let mut ranges = Ranges {
        input: &mut input,
        ranges: node
            .sparse
            .clone()
            .unwrap_or_else(|| {
                vec![FileExtent {
                    offset: 0,
                    length: size,
                }]
            })
            .into_iter(),
        left: 0,
        failure: None,
    };
    if let Err(error) = output.append(&header, &mut ranges) {
        if let Some(reason) = ranges.failure {
            return Ok(Err(reason));
        }
        return Err(io(error));
    }
    match input.metadata() {
        Ok(after) if same_file(&before, &after) => Ok(Ok(Some(Captured {
            offset,
            length: usize::try_from(length)
                .map_err(|_| DriverError::Io("file exceeds addressable size".into()))?,
            hash,
            node,
        }))),
        Ok(_) => Ok(Ok(None)),
        Err(error) => Ok(Err(error.to_string())),
    }
}

pub(super) fn marker(output: &mut Archive, name: &str, bytes: &[u8]) -> Result<usize, DriverError> {
    extensions(
        output,
        BTreeMap::from([(b"path".to_vec(), name.as_bytes().to_vec())]),
    )
    .map_err(io)?;
    let header = header(bytes.len() as u64, 0o600, 0, 0, 0)?;
    let offset = position(output)? + 512;
    output.append(&header, bytes).map_err(io)?;
    Ok(offset)
}

pub(super) fn position(output: &mut Archive) -> Result<usize, DriverError> {
    usize::try_from(output.get_mut().stream_position().map_err(io)?)
        .map_err(|_| DriverError::Io("tar exceeds addressable size".into()))
}

pub(super) fn rewind(output: &mut Archive, at: usize) -> Result<(), DriverError> {
    output.get_mut().set_len(at as u64).map_err(io)?;
    output
        .get_mut()
        .seek(SeekFrom::Start(at as u64))
        .map_err(io)?;
    Ok(())
}

fn header(
    length: u64,
    mode: u32,
    uid: u32,
    gid: u32,
    mtime: u64,
) -> Result<tar::Header, DriverError> {
    let mut header = tar::Header::new_ustar();
    header.set_path("file").map_err(io)?;
    header.set_entry_type(tar::EntryType::Regular);
    header.set_size(length);
    header.set_mode(mode & 0o7777);
    header.set_uid(uid.into());
    header.set_gid(gid.into());
    header.set_mtime(mtime);
    header.set_cksum();
    Ok(header)
}

fn extensions(output: &mut Archive, fields: BTreeMap<Vec<u8>, Vec<u8>>) -> std::io::Result<()> {
    let mut bytes = Vec::new();
    for (key, value) in fields {
        let rest = key.len() + value.len() + 3;
        let mut length = rest + 1;
        loop {
            let next = rest + length.to_string().len();
            if next == length {
                break;
            }
            length = next;
        }
        write!(bytes, "{length} ")?;
        bytes.extend(key);
        bytes.push(b'=');
        bytes.extend(value);
        bytes.push(b'\n');
    }
    let mut header = tar::Header::new_ustar();
    header.set_path("PaxHeaders/file")?;
    header.set_entry_type(tar::EntryType::XHeader);
    header.set_size(bytes.len() as u64);
    header.set_cksum();
    output.append(&header, bytes.as_slice())
}

fn attributes(input: &File) -> std::io::Result<Vec<ExtendedAttribute>> {
    use rustix::fs::{fgetxattr, fgetxattr_query, flistxattr, flistxattr_query};
    let mut names = vec![0; flistxattr_query(input)?];
    let count = flistxattr(input, names.as_mut_slice())?;
    names.truncate(count);
    let mut attributes = Vec::new();
    for name in names
        .split(|byte| *byte == 0)
        .filter(|name| !name.is_empty())
    {
        let name_arg = std::ffi::OsStr::from_bytes(name);
        let mut value = vec![0; fgetxattr_query(input, name_arg)?];
        let count = fgetxattr(input, name_arg, value.as_mut_slice())?;
        value.truncate(count);
        attributes.push(ExtendedAttribute {
            name: name.to_vec(),
            value,
        });
    }
    attributes.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(attributes)
}

fn extents(input: &File, size: u64) -> std::io::Result<Option<Vec<FileExtent>>> {
    use rustix::fs::{SeekFrom, seek};
    let mut at = 0;
    let mut ranges = Vec::new();
    while at < size {
        let data = match seek(input, SeekFrom::Data(at)) {
            Ok(data) => data.min(size),
            Err(rustix::io::Errno::NXIO) => break,
            Err(rustix::io::Errno::INVAL | rustix::io::Errno::OPNOTSUPP) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if data == size {
            break;
        }
        let end = seek(input, SeekFrom::Hole(data))?.min(size);
        if end <= data {
            return Err(std::io::Error::other(
                "filesystem returned an invalid data range",
            ));
        }
        ranges.push(FileExtent {
            offset: data,
            length: end - data,
        });
        at = end;
    }
    if ranges.iter().map(|range| range.length).sum::<u64>() == size {
        Ok(None)
    } else {
        Ok(Some(ranges))
    }
}

struct Ranges<'a> {
    input: &'a mut File,
    ranges: std::vec::IntoIter<FileExtent>,
    left: u64,
    failure: Option<String>,
}
impl Read for Ranges<'_> {
    fn read(&mut self, bytes: &mut [u8]) -> std::io::Result<usize> {
        if bytes.is_empty() {
            return Ok(0);
        }
        while self.left == 0 {
            let Some(range) = self.ranges.next() else {
                return Ok(0);
            };
            self.input.seek(SeekFrom::Start(range.offset))?;
            self.left = range.length;
        }
        let length = bytes
            .len()
            .min(usize::try_from(self.left).unwrap_or(usize::MAX));
        let count = match self.input.read(&mut bytes[..length]) {
            Ok(0) => {
                self.failure = Some("file changed while capturing".into());
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "file changed while capturing",
                ));
            }
            Ok(count) => count,
            Err(error) => {
                self.failure = Some(error.to_string());
                return Err(error);
            }
        };
        self.left -= count as u64;
        Ok(count)
    }
}

pub(super) fn change_time(metadata: &std::fs::Metadata) -> i128 {
    nanos(metadata.ctime(), metadata.ctime_nsec())
}
fn nanos(seconds: i64, nanos: i64) -> i128 {
    i128::from(seconds) * 1_000_000_000 + i128::from(nanos)
}
fn time(ns: i128) -> String {
    let sign = if ns < 0 { "-" } else { "" };
    let absolute = ns.unsigned_abs();
    format!(
        "{sign}{}.{:09}",
        absolute / 1_000_000_000,
        absolute % 1_000_000_000
    )
}

fn acl(bytes: &[u8]) -> std::io::Result<String> {
    if bytes.len() < 4 || bytes[..4] != 2u32.to_le_bytes() || !(bytes.len() - 4).is_multiple_of(8) {
        return Err(std::io::Error::other("invalid POSIX access ACL"));
    }
    let mut entries = Vec::new();
    for entry in bytes[4..].chunks_exact(8) {
        let tag = u16::from_le_bytes(entry[..2].try_into().unwrap());
        let permissions = u16::from_le_bytes(entry[2..4].try_into().unwrap());
        let id = u32::from_le_bytes(entry[4..].try_into().unwrap());
        let name = match tag {
            1 => "user:".into(),
            2 => format!("user:{id}"),
            4 => "group:".into(),
            8 => format!("group:{id}"),
            16 => "mask:".into(),
            32 => "other:".into(),
            _ => return Err(std::io::Error::other("unknown POSIX access ACL tag")),
        };
        let perms: String = [(4, 'r'), (2, 'w'), (1, 'x')]
            .into_iter()
            .map(|(bit, character)| {
                if permissions & bit != 0 {
                    character
                } else {
                    '-'
                }
            })
            .collect();
        entries.push(format!("{name}:{perms}"));
    }
    Ok(entries.join(","))
}
