// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::fs::{File, OpenOptions};
use std::io::{self, ErrorKind};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::Path;

use bytes::{Buf, Bytes};
use opendal_core::raw::{ServiceOperation, new_std_io_error};
use opendal_core::{Buffer, Error, ErrorKind as OpendalErrorKind, Result};
use rustix::fs::{AtFlags, OFlags, StatxFlags, fstatfs, statx};
use rustix::io::Errno;

use super::core::{ErrorContext, parse_error};

const EXT4_SUPER_MAGIC: u32 = 0xef53;
const XFS_SUPER_MAGIC: u32 = 0x5846_5342;
const WRITE_BATCH_SIZE: usize = 1024 * 1024;

#[derive(Clone, Copy)]
pub(super) struct DirectIoAlignment {
    mem_align: usize,
    pub(super) offset_align: usize,
}

impl DirectIoAlignment {
    pub(super) fn open(
        path: &Path,
        mut options: OpenOptions,
        operation: ServiceOperation,
        temporary: bool,
    ) -> Result<(File, Self)> {
        let file = options
            .custom_flags(OFlags::DIRECT.bits() as i32)
            .open(path)
            .map_err(|err| match err.raw_os_error() {
                Some(code)
                    if code == Errno::INVAL.raw_os_error()
                        || code == Errno::OPNOTSUPP.raw_os_error() =>
                {
                    Error::new(OpendalErrorKind::Unsupported, "O_DIRECT open failed")
                        .with_context("service_operation", operation.0)
                        .set_source(err)
                }
                _ => parse_error(ErrorContext::new(operation), err),
            })?;

        match Self::discover(&file) {
            Ok(alignment) => Ok((file, alignment)),
            Err(err) => {
                if temporary {
                    drop(file);
                    if let Err(cleanup) = std::fs::remove_file(path) {
                        return Err(err.with_context("tempfile_cleanup", cleanup.to_string()));
                    }
                }
                Err(err.with_context("service_operation", operation.0))
            }
        }
    }

    fn discover(file: &File) -> Result<Self> {
        match statx(file, "", AtFlags::EMPTY_PATH, StatxFlags::DIOALIGN) {
            Ok(stat) if stat.stx_mask & StatxFlags::DIOALIGN.bits() != 0 => {
                if stat.stx_dio_mem_align == 0 || stat.stx_dio_offset_align == 0 {
                    return Err(Error::new(
                        OpendalErrorKind::Unsupported,
                        "file does not support direct I/O",
                    ));
                }
                return Ok(Self {
                    mem_align: stat.stx_dio_mem_align as usize,
                    offset_align: stat.stx_dio_offset_align as usize,
                });
            }
            Ok(_) | Err(Errno::NOSYS) => {}
            Err(err) => return Err(new_std_io_error(err.into())),
        }

        // Some filesystems (including tmpfs) accept O_DIRECT while still using the
        // page cache. Only fall back to legacy alignment on known filesystems.
        let fs_type = fstatfs(file)
            .map_err(|err| new_std_io_error(err.into()))?
            .f_type as u32;
        match fs_type {
            EXT4_SUPER_MAGIC | XFS_SUPER_MAGIC => Ok(Self {
                mem_align: 4096,
                offset_align: 4096,
            }),
            _ => Err(Error::new(
                OpendalErrorKind::Unsupported,
                "filesystem does not report direct I/O alignment",
            )
            .with_context("filesystem_type", format!("{fs_type:#x}"))),
        }
    }

    pub(super) fn read_at(self, file: &File, offset: u64, size: usize) -> Result<Buffer> {
        let prefix = (offset % self.offset_align as u64) as usize;
        let len = prefix
            .checked_add(size)
            .and_then(|size| {
                size.div_ceil(self.offset_align)
                    .checked_mul(self.offset_align)
            })
            .ok_or_else(|| Error::new(OpendalErrorKind::Unexpected, "direct read size overflow"))?;
        let mut aligned = AlignedBuffer::new(len, self.mem_align)?;
        let read_offset = offset - prefix as u64;
        let read = loop {
            match file.read_at(aligned.as_mut_slice(), read_offset) {
                Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                result => break result.map_err(new_std_io_error)?,
            }
        };

        let available = size.min(read.saturating_sub(prefix));
        if available == 0 {
            return Ok(Buffer::new());
        }
        let start = aligned.start + prefix;
        Ok(Bytes::from(aligned.buf)
            .slice(start..start + available)
            .into())
    }

    pub(super) fn write_all_at(self, file: &File, mut offset: u64, mut buf: Buffer) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let blocks = (WRITE_BATCH_SIZE / self.offset_align).max(1);
        let batch_size = blocks.min(buf.len().div_ceil(self.offset_align)) * self.offset_align;
        let mut aligned = AlignedBuffer::new(batch_size, self.mem_align)?;
        while !buf.is_empty() {
            let len = buf.len().min(batch_size);
            let aligned_len = len.div_ceil(self.offset_align) * self.offset_align;
            let batch = &mut aligned.as_mut_slice()[..aligned_len];
            buf.copy_to_slice(&mut batch[..len]);
            batch[len..].fill(0);

            let mut remaining = batch.len();
            while remaining > 0 {
                let written = match file.write_at(&batch[..remaining], offset) {
                    Err(err) if err.kind() == ErrorKind::Interrupted => continue,
                    result => result.map_err(new_std_io_error)?,
                };
                if written == 0 {
                    return Err(new_std_io_error(io::Error::from(ErrorKind::WriteZero)));
                }
                if written % self.offset_align != 0 {
                    return Err(Error::new(
                        OpendalErrorKind::Unexpected,
                        "unaligned short direct I/O write",
                    ));
                }
                if written < remaining {
                    // Keep the next syscall's buffer address aligned even when the
                    // memory alignment exceeds the offset alignment.
                    batch.copy_within(written..remaining, 0);
                }
                offset += written as u64;
                remaining -= written;
            }
        }
        Ok(())
    }
}

struct AlignedBuffer {
    buf: Vec<u8>,
    start: usize,
    len: usize,
}

impl AlignedBuffer {
    fn new(len: usize, align: usize) -> Result<Self> {
        let capacity = len.checked_add(align - 1).ok_or_else(|| {
            Error::new(OpendalErrorKind::Unexpected, "aligned buffer size overflow")
        })?;
        let mut buf = Vec::new();
        buf.try_reserve_exact(capacity).map_err(|err| {
            Error::new(OpendalErrorKind::Unexpected, "allocate aligned buffer").set_source(err)
        })?;
        buf.resize(capacity, 0);
        let misalignment = buf.as_ptr() as usize % align;
        let start = if misalignment == 0 {
            0
        } else {
            align - misalignment
        };
        Ok(Self { buf, start, len })
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf[self.start..self.start + self.len]
    }
}
