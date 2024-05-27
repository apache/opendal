use bitflags::bitflags;
use vm_memory::ByteValued;

pub const KERNEL_VERSION: u32 = 7;
pub const KERNEL_MINOR_VERSION: u32 = 38;
pub const MIN_KERNEL_MINOR_VERSION: u32 = 27;

const INIT_EXT: u64 = 1 << 30;

bitflags! {
    pub struct FsOptions: u64 {
        const INIT_EXT = INIT_EXT;
    }
}

bitflags! {
    pub struct OpenOptions: u32 {
    }
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct EntryOut {
    pub nodeid: u64,
    pub entry_valid: u64,
    pub attr_valid: u64,
    pub entry_valid_nsec: u32,
    pub attr_valid_nsec: u32,
    pub attr: Attr,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct Attr {
    pub ino: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: u64,
    pub mtime: u64,
    pub ctime: u64,
    pub atimensec: u32,
    pub mtimensec: u32,
    pub ctimensec: u32,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

impl From<libc::stat64> for Attr {
    fn from(st: libc::stat64) -> Attr {
        Attr::with_flags(st, 0)
    }
}

impl Attr {
    pub fn with_flags(st: libc::stat64, flags: u32) -> Attr {
        Attr {
            ino: st.st_ino,
            size: st.st_size as u64,
            blocks: st.st_blocks as u64,
            atime: st.st_atime as u64,
            mtime: st.st_mtime as u64,
            ctime: st.st_ctime as u64,
            atimensec: st.st_atime_nsec as u32,
            mtimensec: st.st_mtime_nsec as u32,
            ctimensec: st.st_ctime_nsec as u32,
            mode: st.st_mode,
            nlink: st.st_nlink as u32,
            uid: st.st_uid,
            gid: st.st_gid,
            rdev: st.st_rdev as u32,
            blksize: st.st_blksize as u32,
            flags,
        }
    }
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct InHeader {
    pub len: u32,
    pub opcode: u32,
    pub unique: u64,
    pub nodeid: u64,
    pub uid: u32,
    pub gid: u32,
    pub pid: u32,
    pub total_extlen: u16,
    pub padding: u16,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct OutHeader {
    pub len: u32,
    pub error: i32,
    pub unique: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct InitInCompat {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct InitInExt {
    pub flags2: u32,
    pub unused: [u32; 11],
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct InitOut {
    pub major: u32,
    pub minor: u32,
    pub max_readahead: u32,
    pub flags: u32,
    pub max_background: u16,
    pub congestion_threshold: u16,
    pub max_write: u32,
    pub time_gran: u32,
    pub max_pages: u16,
    pub map_alignment: u16,
    pub flags2: u32,
    pub unused: [u32; 7],
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct GetattrIn {
    pub flags: u32,
    pub dummy: u32,
    pub fh: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct AttrOut {
    pub attr_valid: u64,
    pub attr_valid_nsec: u32,
    pub dummy: u32,
    pub attr: Attr,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct CreateIn {
    pub flags: u32,
    pub mode: u32,
    pub umask: u32,
    pub open_flags: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct AccessIn {
    pub mask: u32,
    pub padding: u32,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct ForgetIn {
    pub nlookup: u64,
}

#[repr(C)]
#[derive(Debug, Default, Clone, Copy)]
pub struct OpenOut {
    pub fh: u64,
    pub open_flags: u32,
    pub padding: u32,
}

unsafe impl ByteValued for EntryOut {}
unsafe impl ByteValued for Attr {}
unsafe impl ByteValued for InHeader {}
unsafe impl ByteValued for OutHeader {}
unsafe impl ByteValued for InitInCompat {}
unsafe impl ByteValued for InitInExt {}
unsafe impl ByteValued for InitOut {}
unsafe impl ByteValued for GetattrIn {}
unsafe impl ByteValued for AttrOut {}
unsafe impl ByteValued for CreateIn {}
unsafe impl ByteValued for AccessIn {}
unsafe impl ByteValued for ForgetIn {}
unsafe impl ByteValued for OpenOut {}

macro_rules! enum_value {
    (
        $(#[$meta:meta])*
        $vis:vis enum $enum:ident: $T:tt {
            $(
                $(#[$variant_meta:meta])*
                $variant:ident $(= $val:expr)?,
            )*
        }
    ) => {
        #[repr($T)]
        $(#[$meta])*
        $vis enum $enum {
            $($(#[$variant_meta])* $variant $(= $val)?,)*
        }

        impl std::convert::TryFrom<$T> for $enum {
            type Error = ();

            fn try_from(v: $T) -> Result<Self, Self::Error> {
                match v {
                    $(v if v == $enum::$variant as $T => Ok($enum::$variant),)*
                    _ => Err(()),
                }
            }
        }
    }
}

enum_value! {
    pub enum Opcode: u32 {
        Lookup = 1,
        Forget = 2,
        Getattr = 3,
        Mknod = 8,
        Unlink = 10,
        Init = 26,
        Access = 34,
        Create = 35,
        Destroy = 38,
    }
}
