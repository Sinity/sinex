//! Retained material records over one verified backing. Sealed bodies stay off disk.
use sinex_sources::{drivers, Framer};
use sinex_sources::drivers::export_archive::group::ReadSeek;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Mutex;

pub(crate) struct Retained {
    reader: Mutex<Box<dyn ReadSeek>>,
    length: u64,
}
impl Retained {
    pub fn new(reader: impl Read + Seek + Send + 'static, length: u64) -> Self {
        Self {reader:Mutex::new(Box::new(reader)),length}
    }
    pub fn records(&self, manifest:&serde_json::Value, framer:Framer)->Result<Vec<(usize,usize)>,String> {
        let mut reader=self.reader.lock().map_err(|_|"retained reader poisoned")?;
        reader.rewind().map_err(|e|e.to_string())?;
        drivers::stored_records_from(manifest,&mut *reader,framer).map_err(|e|e.to_string())
    }
    pub fn len(&self)->u64 {self.length}
    pub fn into_reader(self)->Result<Box<dyn ReadSeek>,String> {
        let mut reader=self.reader.into_inner().map_err(|_|"retained reader poisoned")?;
        reader.rewind().map_err(|e|e.to_string())?;
        Ok(reader)
    }
}
impl sinex_sources::drain::RecordRead for Retained {
    fn len(&self)->u64 {self.length}
    fn read_exact_at(&self,start:u64,bytes:&mut[u8])->std::io::Result<()> {
        let mut reader=self.reader.lock().map_err(|_|std::io::Error::other("retained reader poisoned"))?;
        reader.seek(SeekFrom::Start(start))?;
        reader.read_exact(bytes)
    }
}

/// Same-length rewrite; preserve only each selected span's actual closing newline.
/// Reading in small buffers must not preserve a newline merely because a buffer ends.
pub(crate) struct Blanked<R> {
    pub reader:R,
    pub spans:Vec<(usize,usize)>,
    pub position:usize,
}
impl<R:Read> Read for Blanked<R> {
    fn read(&mut self,out:&mut[u8])->std::io::Result<usize> {
        let n=self.reader.read(out)?;
        let after=self.position.checked_add(n).ok_or_else(||std::io::Error::other("material exceeds address space"))?;
        for &(start,end) in &self.spans {
            for (i,byte) in out[..n].iter_mut().enumerate().take(end.saturating_sub(self.position)).skip(start.saturating_sub(self.position)) {
                if self.position+i+1!=end || *byte!=b'\n' {*byte=b' ';}
            }
        }
        self.position=after;
        Ok(n)
    }
}

#[cfg(test)]
mod tests;
