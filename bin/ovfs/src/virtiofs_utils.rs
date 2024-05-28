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

use std::cmp;
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::mem::{size_of, MaybeUninit};
use std::ops::Deref;
use std::ptr::copy_nonoverlapping;

use virtio_queue::DescriptorChain;
use vm_memory::bitmap::{Bitmap, BitmapSlice};
use vm_memory::{
    Address, ByteValued, GuestMemory, GuestMemoryMmap, GuestMemoryRegion, VolatileMemory,
    VolatileMemoryError, VolatileSlice,
};

/// The Error here represents shared memory related errors.
#[derive(Debug)]
pub enum Error {
    FindMemoryRegion,
    DescriptorChainOverflow,
    VolatileMemoryError(VolatileMemoryError),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        use self::Error::*;

        unimplemented!()
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

/// Used to consume and use data areas in shared memory between host and VMs.
struct DescriptorChainConsumer<'a, B> {
    buffers: VecDeque<VolatileSlice<'a, B>>,
    bytes_consumed: usize,
}

impl<'a, B: BitmapSlice> DescriptorChainConsumer<'a, B> {
    fn bytes_consumed(&self) -> usize {
        self.bytes_consumed
    }

    fn consume<F>(&mut self, count: usize, f: F) -> io::Result<usize>
    where
        F: FnOnce(&[&VolatileSlice<B>]) -> io::Result<usize>,
    {
        let mut len = 0;
        let mut bufs = Vec::with_capacity(self.buffers.len());
        for vs in &self.buffers {
            if len >= count {
                break;
            }
            bufs.push(vs);
            let remain = count - len;
            if remain < vs.len() {
                len += remain;
            } else {
                len += vs.len();
            }
        }
        if bufs.is_empty() {
            return Ok(0);
        }
        let bytes_consumed = f(&bufs)?;
        let total_bytes_consumed =
            self.bytes_consumed
                .checked_add(bytes_consumed)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, Error::DescriptorChainOverflow)
                })?;
        let mut reamin = bytes_consumed;
        while let Some(vs) = self.buffers.pop_front() {
            if reamin < vs.len() {
                self.buffers.push_front(vs.offset(reamin).unwrap());
                break;
            }
            reamin -= vs.len();
        }
        self.bytes_consumed = total_bytes_consumed;
        Ok(bytes_consumed)
    }
}

/// Provides a high-level interface for reading data in shared memory sequences.
pub struct Reader<'a, B = ()> {
    buffer: DescriptorChainConsumer<'a, B>,
}

impl<'a, B: Bitmap + BitmapSlice + 'static> Reader<'a, B> {
    pub fn new<M>(
        mem: &'a GuestMemoryMmap<B>,
        desc_chain: DescriptorChain<M>,
    ) -> Result<Reader<'a, B>>
    where
        M: Deref,
        M::Target: GuestMemory + Sized,
    {
        let mut len: usize = 0;
        let buffers = desc_chain
            .readable()
            .map(|desc| {
                len = len
                    .checked_add(desc.len() as usize)
                    .ok_or(Error::DescriptorChainOverflow)?;
                let region = mem
                    .find_region(desc.addr())
                    .ok_or(Error::FindMemoryRegion)?;
                let offset = desc
                    .addr()
                    .checked_sub(region.start_addr().raw_value())
                    .unwrap();
                region
                    .deref()
                    .get_slice(offset.raw_value() as usize, desc.len() as usize)
                    .map_err(Error::VolatileMemoryError)
            })
            .collect::<Result<VecDeque<VolatileSlice<'a, B>>>>()?;
        Ok(Reader {
            buffer: DescriptorChainConsumer {
                buffers,
                bytes_consumed: 0,
            },
        })
    }

    pub fn read_obj<T: ByteValued>(&mut self) -> io::Result<T> {
        let mut obj = MaybeUninit::<T>::uninit();
        let buf = unsafe {
            std::slice::from_raw_parts_mut(obj.as_mut_ptr() as *mut u8, size_of::<T>())
        };
        self.read_exact(buf)?;
        Ok(unsafe { obj.assume_init() })
    }
}

impl<'a, B: BitmapSlice> io::Read for Reader<'a, B> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.buffer.consume(buf.len(), |bufs| {
            let mut rem = buf;
            let mut total = 0;
            for vs in bufs {
                let copy_len = cmp::min(rem.len(), vs.len());
                unsafe {
                    copy_nonoverlapping(vs.ptr_guard().as_ptr(), rem.as_mut_ptr(), copy_len);
                }
                rem = &mut rem[copy_len..];
                total += copy_len;
            }
            Ok(total)
        })
    }
}

/// Provides a high-level interface for writing data in shared memory sequences.
pub struct Writer<'a, B = ()> {
    buffer: DescriptorChainConsumer<'a, B>,
}

impl<'a, B: Bitmap + BitmapSlice + 'static> Writer<'a, B> {
    pub fn new<M>(
        mem: &'a GuestMemoryMmap<B>,
        desc_chain: DescriptorChain<M>,
    ) -> Result<Writer<'a, B>>
    where
        M: Deref,
        M::Target: GuestMemory + Sized,
    {
        let mut len: usize = 0;
        let buffers = desc_chain
            .writable()
            .map(|desc| {
                len = len
                    .checked_add(desc.len() as usize)
                    .ok_or(Error::DescriptorChainOverflow)?;
                let region = mem
                    .find_region(desc.addr())
                    .ok_or(Error::FindMemoryRegion)?;
                let offset = desc
                    .addr()
                    .checked_sub(region.start_addr().raw_value())
                    .unwrap();
                region
                    .deref()
                    .get_slice(offset.raw_value() as usize, desc.len() as usize)
                    .map_err(Error::VolatileMemoryError)
            })
            .collect::<Result<VecDeque<VolatileSlice<'a, B>>>>()?;
        Ok(Writer {
            buffer: DescriptorChainConsumer {
                buffers,
                bytes_consumed: 0,
            },
        })
    }

    pub fn bytes_written(&self) -> usize {
        self.buffer.bytes_consumed()
    }
}

impl<'a, B: BitmapSlice> Write for Writer<'a, B> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.buffer.consume(buf.len(), |bufs| {
            let mut rem = buf;
            let mut total = 0;
            for vs in bufs {
                let copy_len = cmp::min(rem.len(), vs.len());
                unsafe {
                    copy_nonoverlapping(rem.as_ptr(), vs.ptr_guard_mut().as_ptr(), copy_len);
                }
                vs.bitmap().mark_dirty(0, copy_len);
                rem = &rem[copy_len..];
                total += copy_len;
            }
            Ok(total)
        })
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
