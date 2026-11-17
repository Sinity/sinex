//! Prepare a held prefix with shared extents or a sparse copy, and hash its logical bytes.
use super::*;
use std::io::{self, Seek, SeekFrom};

fn check_length(input: &File, length: u64) -> Result<()> {
    if input.metadata()?.len() < length {
        return Err(Error::bad_request("short committed segment"));
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn data_range(input: &File, position: u64, length: u64) -> io::Result<(u64, u64)> {
    use rustix::fs::{SeekFrom, seek};
    use rustix::io::Errno;
    let data = match seek(input, SeekFrom::Data(position)) {
        Ok(data) => data.min(length),
        Err(Errno::NXIO) => return Ok((length, length)),
        Err(Errno::INVAL | Errno::NOTSUP) => return Err(io::ErrorKind::Unsupported.into()),
        Err(error) => return Err(error.into()),
    };
    if data == length {
        return Ok((length, length));
    }
    match seek(input, SeekFrom::Hole(data)) {
        Ok(end) if end > data => Ok((data, end.min(length))),
        Ok(_) => Err(io::Error::other("segment hole did not advance")),
        Err(Errno::INVAL | Errno::NOTSUP) => Err(io::ErrorKind::Unsupported.into()),
        Err(error) => Err(error.into()),
    }
}

#[cfg(not(target_os = "linux"))]
fn data_range(_: &File, _: u64, _: u64) -> io::Result<(u64, u64)> {
    Err(io::ErrorKind::Unsupported.into())
}

pub(super) fn copy(
    input: &mut File,
    output: &mut File,
    length: u64,
    stop: &watch::Receiver<bool>,
) -> Result<blake3::Hash> {
    stopped(stop)?;
    check_length(input, length)?;
    if clone_prefix(input, output, length)? {
        read_ranges(output, None, length, stop, data_range)
    } else {
        read_ranges(input, Some(output), length, stop, data_range)
    }
}

#[cfg(target_os = "linux")]
fn clone_prefix(input: &File, output: &File, length: u64) -> Result<bool> {
    use rustix::{fs::ioctl_ficlone, io::Errno};
    match ioctl_ficlone(output, input) {
        Ok(()) => {
            // A concurrent append may be included in the clone; only the held
            // prefix belongs to the artifact. Never extend a short clone.
            check_length(output, length)?;
            output.set_len(length)?;
            Ok(true)
        }
        Err(Errno::XDEV | Errno::NOTSUP | Errno::INVAL | Errno::NOTTY | Errno::NOSYS) => {
            output.set_len(0)?;
            Ok(false)
        }
        Err(error) => Err(std::io::Error::from(error).into()),
    }
}

#[cfg(not(target_os = "linux"))]
fn clone_prefix(_: &File, _: &File, _: u64) -> Result<bool> {
    Ok(false)
}

fn read_ranges(
    input: &mut File,
    mut copy_to: Option<&mut File>,
    length: u64,
    stop: &watch::Receiver<bool>,
    mut range: impl FnMut(&File, u64, u64) -> io::Result<(u64, u64)>,
) -> Result<blake3::Hash> {
    // A missing suffix is an error, including when SEEK_DATA returns ENXIO.
    // The backup hold forbids punching this prefix; the live writer only appends.
    check_length(input, length)?;
    let mut buffer = vec![0; 1 << 20];
    let zeros = vec![0; buffer.len()];
    let mut hasher = blake3::Hasher::new();
    let mut position = 0;
    let mut dense = false;
    while position < length {
        stopped(stop)?;
        let (data, end) = if dense {
            (position, length)
        } else {
            match range(input, position, length) {
                Ok(range) => range,
                Err(error) if error.kind() == io::ErrorKind::Unsupported => {
                    dense = true;
                    (position, length)
                }
                Err(error) => return Err(error.into()),
            }
        };
        if data < position || end < data || end > length || end == position {
            return Err(Error::bad_request("invalid committed segment data range"));
        }
        if data > position {
            check_length(input, length)?;
            while position < data {
                stopped(stop)?;
                let take = (data - position).min(zeros.len() as u64) as usize;
                hasher.update(&zeros[..take]);
                position += take as u64;
            }
        }
        input.seek(SeekFrom::Start(position))?;
        if let Some(output) = copy_to.as_deref_mut() {
            output.seek(SeekFrom::Start(position))?;
        }
        while position < end {
            stopped(stop)?;
            let take = (end - position).min(buffer.len() as u64) as usize;
            input.read_exact(&mut buffer[..take]).map_err(|error| {
                Error::bad_request(format!("short or unreadable committed segment: {error}"))
            })?;
            if let Some(output) = copy_to.as_deref_mut() {
                output.write_all(&buffer[..take])?;
            }
            hasher.update(&buffer[..take]);
            position += take as u64;
        }
    }
    stopped(stop)?;
    check_length(input, length)?;
    // A final hole still belongs to the logical prefix and its digest.
    if let Some(output) = copy_to {
        output.set_len(length)?;
    }
    Ok(hasher.finalize())
}

#[cfg(test)]
mod tests;
