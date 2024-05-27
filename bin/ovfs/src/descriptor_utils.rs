use std::cmp;
use std::collections::VecDeque;
use std::fmt::{self, Display};
use std::io::{self, Read, Write};
use std::mem::{size_of, MaybeUninit};
use std::ops::Deref;
use std::ptr::copy_nonoverlapping;
use std::result;

use virtio_queue::DescriptorChain;
use vm_memory::bitmap::{Bitmap, BitmapSlice};
use vm_memory::{
    Address, ByteValued, GuestMemory, GuestMemoryMmap, GuestMemoryRegion, VolatileMemory,
    VolatileMemoryError, VolatileSlice,
};

#[derive(Debug)]
pub enum Error {
    FindMemoryRegion,
    DescriptorChainOverflow,
    VolatileMemoryError(VolatileMemoryError),
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Error::*;

        match self {
            FindMemoryRegion => write!(f, "no memory region for this address range"),
            DescriptorChainOverflow => write!(
                f,
                "the combined length of all the buffers in a `DescriptorChain` would overflow"
            ),
            VolatileMemoryError(e) => write!(f, "volatile memory error: {e}"),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl std::error::Error for Error {}

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
        let mut buflen = 0;
        let mut bufs = Vec::with_capacity(self.buffers.len());
        for vs in &self.buffers {
            if buflen >= count {
                break;
            }
            bufs.push(vs);
            let rem = count - buflen;
            if rem < vs.len() {
                buflen += rem;
            } else {
                buflen += vs.len();
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
        let mut rem = bytes_consumed;
        while let Some(vs) = self.buffers.pop_front() {
            if rem < vs.len() {
                self.buffers.push_front(vs.offset(rem).unwrap());
                break;
            }
            rem -= vs.len();
        }
        self.bytes_consumed = total_bytes_consumed;
        Ok(bytes_consumed)
    }
}

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
        let mut total_len: usize = 0;
        let buffers = desc_chain
            .readable()
            .map(|desc| {
                total_len = total_len
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
            ::std::slice::from_raw_parts_mut(obj.as_mut_ptr() as *mut u8, size_of::<T>())
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
        let mut total_len: usize = 0;
        let buffers = desc_chain
            .writable()
            .map(|desc| {
                total_len = total_len
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
