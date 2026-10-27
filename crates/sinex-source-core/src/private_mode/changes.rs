//! Keep switch transitions until the gate reads them, including short pauses.
use rustix::fd::OwnedFd;
use rustix::fs::inotify::{self, CreateFlags, ReadFlags, WatchFlags};
use std::ffi::OsString;
use std::io;
use std::mem::MaybeUninit;
use std::os::unix::ffi::OsStrExt;
use std::path::Path;

pub(super) enum Change {
    On,
    Off,
    Permissions,
}

pub(super) struct SwitchWatch {
    fd: OwnedFd,
    name: OsString,
}

impl SwitchWatch {
    pub(super) fn open(path: &Path) -> io::Result<Self> {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("switch has no directory"))?;
        let name = path
            .file_name()
            .ok_or_else(|| io::Error::other("switch has no file name"))?
            .to_owned();
        let fd = inotify::init(CreateFlags::NONBLOCK | CreateFlags::CLOEXEC)?;
        inotify::add_watch(
            &fd,
            parent,
            WatchFlags::ONLYDIR
                | WatchFlags::CREATE
                | WatchFlags::DELETE
                | WatchFlags::MOVED_TO
                | WatchFlags::MOVED_FROM
                | WatchFlags::ATTRIB
                | WatchFlags::DELETE_SELF
                | WatchFlags::MOVE_SELF,
        )?;
        Ok(Self { fd, name })
    }

    pub(super) fn read(&mut self) -> io::Result<Vec<Change>> {
        let mut buffer = [MaybeUninit::uninit(); 4096];
        let mut reader = inotify::Reader::new(&self.fd, &mut buffer);
        let mut changes = Vec::new();
        loop {
            let event = match reader.next() {
                Ok(event) => event,
                Err(rustix::io::Errno::AGAIN) => break,
                Err(rustix::io::Errno::INTR) => continue,
                Err(error) => return Err(error.into()),
            };
            let flags = event.events();
            if flags.intersects(
                ReadFlags::QUEUE_OVERFLOW
                    | ReadFlags::IGNORED
                    | ReadFlags::UNMOUNT
                    | ReadFlags::DELETE_SELF
                    | ReadFlags::MOVE_SELF,
            ) {
                return Err(io::Error::other("switch directory notifications were lost"));
            }
            match event.file_name() {
                Some(name) if name.to_bytes() == self.name.as_bytes() => {
                    if flags.intersects(ReadFlags::CREATE | ReadFlags::MOVED_TO) {
                        changes.push(Change::On);
                    }
                    if flags.intersects(ReadFlags::DELETE | ReadFlags::MOVED_FROM) {
                        changes.push(Change::Off);
                    }
                }
                None if flags.contains(ReadFlags::ATTRIB) => changes.push(Change::Permissions),
                _ => {}
            }
        }
        Ok(changes)
    }
}
