use std::time::Duration;

use crate::fuse::*;

pub const DEFAULT_GENERATION: u64 = 0;
pub const DEFAULT_ATTR_FLAGS: u32 = 0;
pub const DEFAULT_FILE_HANDLE: Option<u64> = Some(0);

pub const DEFAULT_ATTR_TIMEOUT: Duration = Duration::from_secs(5);
pub const DEFAULT_ENTRY_TIMEOUT: Duration = Duration::from_secs(5);

pub const DEFAULT_FS_OPTIONS: FsOptions = FsOptions::empty();
pub const DEFAULT_OPEN_OPTIONS: OpenOptions = OpenOptions::empty();

pub const DEFAULT_UID: u32 = 1000;
pub const DEFAULT_GID: u32 = 1000;
pub const DEFAULT_DIR_MODE: u32 = 0o755;
pub const DEFAULT_FILE_MODE: u32 = 0o755;
