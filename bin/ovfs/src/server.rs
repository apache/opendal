use std::error;
use std::ffi::{CStr, FromBytesWithNulError};
use std::fmt;
use std::io::{self, Read, Write};
use std::mem::size_of;
use std::sync::atomic::{AtomicU64, Ordering};

use vm_memory::ByteValued;

use crate::descriptor_utils::{Reader, Writer};
use crate::filesystem::*;
use crate::fuse::*;

#[derive(Debug)]
pub enum Error {
    DecodeMessage(io::Error),
    EncodeMessage(io::Error),
    MissingParameter,
    InvalidHeaderLength,
    InvalidCString(FromBytesWithNulError),
}

impl error::Error for Error {}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Error::*;
        match self {
            DecodeMessage(err) => write!(f, "failed to decode fuse message: {err}"),
            EncodeMessage(err) => write!(f, "failed to encode fuse message: {err}"),
            MissingParameter => write!(f, "one or more parameters are missing"),
            InvalidHeaderLength => write!(f, "the `len` field of the header is too small"),
            InvalidCString(err) => write!(f, "a c string parameter is invalid: {err}"),
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

const FUSE_BUFFER_HEADER_SIZE: u32 = 0x1000;
const MAX_BUFFER_SIZE: u32 = 1 << 20;

pub struct Server<F: FileSystem + Sync> {
    fs: F,
    options: AtomicU64,
}

impl<F: FileSystem + Sync> Server<F> {
    pub fn new(fs: F) -> Server<F> {
        Server {
            fs,
            options: AtomicU64::new(FsOptions::empty().bits()),
        }
    }

    pub fn handle_message(&self, mut r: Reader, w: Writer) -> Result<usize> {
        let in_header: InHeader = r.read_obj().map_err(Error::DecodeMessage)?;

        if in_header.len > (MAX_BUFFER_SIZE + FUSE_BUFFER_HEADER_SIZE) {
            return reply_error(
                io::Error::from_raw_os_error(libc::ENOMEM),
                in_header.unique,
                w,
            );
        }

        if let Ok(opcode) = Opcode::try_from(in_header.opcode) {
            match opcode {
                Opcode::Init => self.init(in_header, r, w),
                Opcode::Lookup => self.lookup(in_header, r, w),
                Opcode::Getattr => self.getattr(in_header, r, w),
                Opcode::Create => self.create(in_header, r, w),
                Opcode::Unlink => self.unlink(in_header, r, w),
                Opcode::Access => self.access(in_header, r, w),
                Opcode::Forget => self.forget(in_header, r),
                Opcode::Destroy => self.destroy(),
                _ => {
                    debug!(
                        "Received unknown request: opcode={}, inode={}",
                        in_header.opcode, in_header.nodeid
                    );
                    reply_error(
                        io::Error::from_raw_os_error(libc::ENOSYS),
                        in_header.unique,
                        w,
                    )
                }
            }
        } else {
            debug!(
                "Received unknown request: opcode={}, inode={}",
                in_header.opcode, in_header.nodeid
            );
            reply_error(
                io::Error::from_raw_os_error(libc::ENOSYS),
                in_header.unique,
                w,
            )
        }
    }

    fn init(&self, in_header: InHeader, mut r: Reader, w: Writer) -> Result<usize> {
        let InitInCompat {
            major,
            minor,
            max_readahead,
            flags,
        } = r.read_obj().map_err(Error::DecodeMessage)?;

        let options = FsOptions::from_bits_truncate(flags as u64);

        let InitInExt { flags2, .. } = if options.contains(FsOptions::INIT_EXT) {
            r.read_obj().map_err(Error::DecodeMessage)?
        } else {
            InitInExt::default()
        };

        if major != KERNEL_VERSION || minor < MIN_KERNEL_MINOR_VERSION {
            error!("Unsupported fuse protocol version: {}.{}", major, minor);
            return reply_error(
                io::Error::from_raw_os_error(libc::EPROTO),
                in_header.unique,
                w,
            );
        }

        let flags_64 = ((flags2 as u64) << 32) | (flags as u64);
        let capable = FsOptions::from_bits_truncate(flags_64);

        let page_size: u32 = unsafe { libc::sysconf(libc::_SC_PAGESIZE).try_into().unwrap() };
        let max_pages = ((MAX_BUFFER_SIZE - 1) / page_size) + 1;

        match self.fs.init(capable) {
            Ok(want) => {
                let enabled = (capable & want).bits();
                self.options.store(enabled, Ordering::Relaxed);

                let out = InitOut {
                    major: KERNEL_VERSION,
                    minor: KERNEL_MINOR_VERSION,
                    max_readahead,
                    flags: enabled as u32,
                    max_background: u16::MAX,
                    congestion_threshold: (u16::MAX / 4) * 3,
                    max_write: MAX_BUFFER_SIZE,
                    time_gran: 1, // nanoseconds
                    max_pages: max_pages.try_into().unwrap(),
                    map_alignment: 0,
                    flags2: (enabled >> 32) as u32,
                    ..Default::default()
                };

                reply_ok(Some(out), None, in_header.unique, w)
            }
            Err(e) => reply_error(e, in_header.unique, w),
        }
    }

    fn destroy(&self) -> Result<usize> {
        self.fs.destroy();
        self.options
            .store(FsOptions::empty().bits(), Ordering::Relaxed);
        Ok(0)
    }

    fn lookup(&self, in_header: InHeader, mut r: Reader, w: Writer) -> Result<usize> {
        let namelen = (in_header.len as usize)
            .checked_sub(size_of::<InHeader>())
            .ok_or(Error::InvalidHeaderLength)?;

        let mut buf = vec![0u8; namelen];

        r.read_exact(&mut buf).map_err(Error::DecodeMessage)?;

        let name = bytes_to_cstr(buf.as_ref())?;

        match self.fs.lookup(in_header.nodeid.into(), name) {
            Ok(entry) => {
                let out = EntryOut::from(entry);

                reply_ok(Some(out), None, in_header.unique, w)
            }
            Err(e) => reply_error(e, in_header.unique, w),
        }
    }

    fn getattr(&self, in_header: InHeader, mut r: Reader, w: Writer) -> Result<usize> {
        let GetattrIn { flags, fh, .. } = r.read_obj().map_err(Error::DecodeMessage)?;
        match self.fs.getattr(in_header.nodeid.into(), None) {
            Ok((st, timeout)) => {
                let out = AttrOut {
                    attr_valid: timeout.as_secs(),
                    attr_valid_nsec: timeout.subsec_nanos(),
                    dummy: 0,
                    attr: st.into(),
                };
                reply_ok(Some(out), None, in_header.unique, w)
            }
            Err(e) => reply_error(e, in_header.unique, w),
        }
    }

    fn create(&self, in_header: InHeader, mut r: Reader, w: Writer) -> Result<usize> {
        let CreateIn {
            flags,
            mode,
            umask,
            open_flags,
            ..
        } = r.read_obj().map_err(Error::DecodeMessage)?;

        let remaining_len = (in_header.len as usize)
            .checked_sub(size_of::<InHeader>())
            .and_then(|l| l.checked_sub(size_of::<CreateIn>()))
            .ok_or(Error::InvalidHeaderLength)?;

        let mut buf = vec![0; remaining_len];

        r.read_exact(&mut buf).map_err(Error::DecodeMessage)?;
        let mut components = buf.split_inclusive(|c| *c == b'\0');
        let name = components.next().ok_or(Error::MissingParameter)?;

        match self.fs.create(
            in_header.nodeid.into(),
            bytes_to_cstr(name)?,
            mode,
            flags,
            umask,
        ) {
            Ok((entry, handle, opts)) => {
                let entry_out = EntryOut {
                    nodeid: entry.inode,
                    entry_valid: entry.entry_timeout.as_secs(),
                    attr_valid: entry.attr_timeout.as_secs(),
                    entry_valid_nsec: entry.entry_timeout.subsec_nanos(),
                    attr_valid_nsec: entry.attr_timeout.subsec_nanos(),
                    attr: Attr::with_flags(entry.attr, entry.attr_flags),
                };
                let open_out = OpenOut {
                    fh: handle.map(Into::into).unwrap_or(0),
                    open_flags: opts.bits(),
                    ..Default::default()
                };
                reply_ok(
                    Some(entry_out),
                    Some(open_out.as_slice()),
                    in_header.unique,
                    w,
                )
            }
            Err(e) => reply_error(e, in_header.unique, w),
        }
    }

    fn unlink(&self, in_header: InHeader, mut r: Reader, w: Writer) -> Result<usize> {
        let namelen = (in_header.len as usize)
            .checked_sub(size_of::<InHeader>())
            .ok_or(Error::InvalidHeaderLength)?;
        let mut name = vec![0; namelen];

        r.read_exact(&mut name).map_err(Error::DecodeMessage)?;

        match self
            .fs
            .unlink(in_header.nodeid.into(), bytes_to_cstr(&name)?)
        {
            Ok(()) => reply_ok(None::<u8>, None, in_header.unique, w),
            Err(e) => reply_error(e, in_header.unique, w),
        }
    }

    fn access(&self, in_header: InHeader, mut r: Reader, w: Writer) -> Result<usize> {
        let AccessIn { mask, .. } = r.read_obj().map_err(Error::DecodeMessage)?;
        match self.fs.access(in_header.nodeid.into(), mask) {
            Ok(()) => reply_ok(None::<u8>, None, in_header.unique, w),
            Err(e) => reply_error(e, in_header.unique, w),
        }
    }

    fn forget(&self, in_header: InHeader, mut r: Reader) -> Result<usize> {
        let ForgetIn { nlookup } = r.read_obj().map_err(Error::DecodeMessage)?;
        self.fs.forget(in_header.nodeid.into(), nlookup);
        Ok(0)
    }
}

fn reply_ok<T: ByteValued>(
    out: Option<T>,
    data: Option<&[u8]>,
    unique: u64,
    mut w: Writer,
) -> Result<usize> {
    let mut len = size_of::<OutHeader>();
    if out.is_some() {
        len += size_of::<T>();
    }
    if let Some(data) = data {
        len += data.len();
    }
    let header = OutHeader {
        len: len as u32,
        error: 0,
        unique,
    };
    w.write_all(header.as_slice())
        .map_err(Error::EncodeMessage)?;
    if let Some(out) = out {
        w.write_all(out.as_slice()).map_err(Error::EncodeMessage)?;
    }
    if let Some(data) = data {
        w.write_all(data).map_err(Error::EncodeMessage)?;
    }
    Ok(w.bytes_written())
}

fn reply_error(e: io::Error, unique: u64, mut w: Writer) -> Result<usize> {
    let header = OutHeader {
        len: size_of::<OutHeader>() as u32,
        error: -e.raw_os_error().unwrap_or(libc::EIO),
        unique,
    };
    w.write_all(header.as_slice())
        .map_err(Error::EncodeMessage)?;
    Ok(w.bytes_written())
}

fn bytes_to_cstr(buf: &[u8]) -> Result<&CStr> {
    CStr::from_bytes_with_nul(buf).map_err(Error::InvalidCString)
}
