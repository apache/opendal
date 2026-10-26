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

use std::ffi::{CStr, CString};
use std::fs::File;
use std::os::fd::AsRawFd;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use opendal_core::raw::new_std_io_error;
use opendal_core::{Buffer, Error, ErrorKind, Result};
use tokio::sync::{mpsc, oneshot};

use crate::ffi::{Api, Cqe, Ior, Iov};

pub(crate) struct RegisteredFile {
    pub file: File,
    api: Arc<Api>,
}

impl RegisteredFile {
    pub fn new(api: Arc<Api>, file: File) -> Result<Arc<Self>> {
        // Registration borrows the descriptor until deregistration in Drop.
        let result = unsafe { (api.reg_fd)(file.as_raw_fd(), 0) };
        // Unlike other USRBIO functions, registration returns positive errno.
        if result > 0 {
            return Err(new_std_io_error(std::io::Error::from_raw_os_error(result)));
        }
        Ok(Arc::new(Self { file, api }))
    }
}

impl Drop for RegisteredFile {
    fn drop(&mut self) {
        // Every prepared request owns an Arc, including cancelled callers.
        unsafe { (self.api.dereg_fd)(self.file.as_raw_fd()) };
    }
}

struct Slots {
    free: Vec<usize>,
    retained: usize,
}

struct Pool {
    api: Arc<Api>,
    iov: Iov,
    block_size: usize,
    retain_limit: usize,
    slots: Mutex<Slots>,
}

// The mapping is stable. Only the driver writes to exclusively checked-out
// slots; completed, retained slots are immutable until their final owner drops.
unsafe impl Send for Pool {}
unsafe impl Sync for Pool {}

impl Pool {
    fn new(
        api: Arc<Api>,
        mount: &CStr,
        depth: usize,
        block_size: usize,
        read: bool,
    ) -> Result<Arc<Self>> {
        let count = if read { depth * 2 } else { depth };
        // Zero is a valid initial state for the C struct, which the API fills.
        let mut iov = unsafe { std::mem::zeroed() };
        let result = unsafe {
            (api.iovcreate)(&mut iov, mount.as_ptr(), count * block_size, block_size, -1)
        };
        if result < 0 {
            return Err(new_std_io_error(std::io::Error::from_raw_os_error(-result)));
        }
        Ok(Arc::new(Self {
            api,
            iov,
            block_size,
            retain_limit: depth,
            slots: Mutex::new(Slots {
                free: (0..count).collect(),
                retained: 0,
            }),
        }))
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        // No ring, pending request, or returned Bytes can still use this mapping.
        unsafe { (self.api.iovdestroy)(&mut self.iov) };
    }
}

struct Slot {
    pool: Arc<Pool>,
    index: usize,
    len: usize,
    retained: bool,
}

impl AsRef<[u8]> for Slot {
    fn as_ref(&self) -> &[u8] {
        // Only completed reads use this view. A slot never crosses an Iov block.
        unsafe {
            std::slice::from_raw_parts(
                self.pool.iov.base.add(self.index * self.pool.block_size),
                self.len,
            )
        }
    }
}

impl Slot {
    fn into_buffer(mut self) -> Buffer {
        let mut slots = self.pool.slots.lock().unwrap();
        if slots.retained < self.pool.retain_limit {
            slots.retained += 1;
            self.retained = true;
            drop(slots);
            Bytes::from_owner(self).into()
        } else {
            drop(slots);
            // Reserve a full ring's worth of slots for progress even when an
            // application collects a file larger than the shared-memory pool.
            Bytes::copy_from_slice(self.as_ref()).into()
        }
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        let mut slots = self.pool.slots.lock().unwrap();
        if self.retained {
            slots.retained -= 1;
        }
        slots.free.push(self.index);
    }
}

struct Request {
    file: Arc<RegisteredFile>,
    offset: usize,
    size: usize,
    data: Option<Buffer>,
    reply: oneshot::Sender<Result<(usize, Buffer)>>,
}

struct Pending {
    request: Request,
    slot: Slot,
}

#[derive(Debug, Clone)]
pub(crate) struct IoDriver {
    tx: mpsc::Sender<Request>,
    pub block_size: usize,
    read: bool,
}

impl IoDriver {
    pub fn start(
        api: Arc<Api>,
        mount: CString,
        depth: usize,
        block_size: usize,
        read: bool,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(depth);
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
        std::thread::Builder::new()
            .name(if read { "hf3fs-read" } else { "hf3fs-write" }.into())
            .spawn(
                move || match Worker::new(api, &mount, depth, block_size, read, rx) {
                    Ok(mut worker) => {
                        if ready_tx.send(Ok(())).is_ok()
                            && let Err(error) = worker.run()
                        {
                            worker.failure = error.to_string();
                        }
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                },
            )
            .map_err(new_std_io_error)?;
        ready_rx.recv().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "USRBIO driver initialization failed").set_source(e)
        })??;
        Ok(Self {
            tx,
            block_size,
            read,
        })
    }

    pub async fn submit(
        &self,
        file: Arc<RegisteredFile>,
        offset: u64,
        size: usize,
        data: Option<Buffer>,
    ) -> Result<(usize, Buffer)> {
        if size == 0
            || size > self.block_size
            || self.read != data.is_none()
            || data.as_ref().is_some_and(|buffer| buffer.len() != size)
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "invalid USRBIO buffer size or direction",
            ));
        }
        let offset = usize::try_from(offset).map_err(|e| {
            Error::new(
                ErrorKind::RangeNotSatisfied,
                "file offset exceeds platform limits",
            )
            .set_source(e)
        })?;
        let (reply, result) = oneshot::channel();
        self.tx
            .send(Request {
                file,
                offset,
                size,
                data,
                reply,
            })
            .await
            .map_err(|_| Error::new(ErrorKind::Unexpected, "USRBIO driver stopped"))?;
        result
            .await
            .map_err(|_| Error::new(ErrorKind::Unexpected, "USRBIO driver stopped"))?
    }
}

struct Worker {
    pool: Arc<Pool>,
    ring: Option<Ior>,
    read: bool,
    depth: usize,
    rx: mpsc::Receiver<Request>,
    pending: Vec<Option<Pending>>,
    active: usize,
    failure: String,
}

impl Worker {
    fn new(
        api: Arc<Api>,
        mount: &CStr,
        depth: usize,
        block_size: usize,
        read: bool,
        rx: mpsc::Receiver<Request>,
    ) -> Result<Self> {
        let pool = Pool::new(api.clone(), mount, depth, block_size, read)?;
        let mut ring = unsafe { std::mem::zeroed() };
        // io_depth=0 dispatches partial batches immediately, including one I/O.
        // Leave headroom for a completion whose submission slot is not reclaimed yet.
        let result = unsafe {
            (api.iorcreate4)(
                &mut ring,
                mount.as_ptr(),
                (depth + 1) as i32,
                read,
                0,
                0,
                -1,
                0,
            )
        };
        if result < 0 {
            return Err(new_std_io_error(std::io::Error::from_raw_os_error(-result)));
        }
        Ok(Self {
            pool,
            ring: Some(ring),
            read,
            depth,
            rx,
            pending: (0..depth * 2).map(|_| None).collect(),
            active: 0,
            failure: "USRBIO driver stopped with pending I/O".into(),
        })
    }

    fn prepare(&mut self, request: Request) -> Option<Request> {
        if request.reply.is_closed() {
            return None;
        }
        let index = self
            .pool
            .slots
            .lock()
            .unwrap()
            .free
            .pop()
            .expect("reserved I/O slot");
        let slot = Slot {
            pool: self.pool.clone(),
            index,
            len: request.size,
            retained: false,
        };
        // The slot is exclusive, and the request fits within one Iov block.
        let ptr = unsafe { self.pool.iov.base.add(index * self.pool.block_size) };
        if let Some(data) = &request.data {
            let dst = unsafe { std::slice::from_raw_parts_mut(ptr, request.size) };
            let mut offset = 0;
            for part in data.clone() {
                dst[offset..offset + part.len()].copy_from_slice(&part);
                offset += part.len();
            }
        }
        let result = unsafe {
            (self.pool.api.prep_io)(
                self.ring.as_ref().unwrap(),
                &self.pool.iov,
                self.read,
                ptr.cast(),
                request.file.file.as_raw_fd(),
                request.offset,
                request.size as u64,
                index as *const _,
            )
        };
        if result == -libc::EAGAIN {
            return Some(request);
        } else if result < 0 {
            let _ = request
                .reply
                .send(Err(new_std_io_error(std::io::Error::from_raw_os_error(
                    -result,
                ))));
        } else {
            // A prepared I/O may start before submit_ios; retain all resources.
            self.pending[index] = Some(Pending { request, slot });
            self.active += 1;
        }
        None
    }

    fn run(&mut self) -> Result<()> {
        let mut cqes: Vec<Cqe> = (0..self.depth)
            .map(|_| unsafe { std::mem::zeroed() })
            .collect();
        let mut waiting = None;
        loop {
            let mut prepared = false;
            // A completed request can leave its submission slot occupied until
            // an earlier batch finishes. Drain before retrying a full ring.
            while self.active < self.depth && (waiting.is_none() || self.active == 0) {
                let request = if waiting.is_some() {
                    waiting.take()
                } else if self.active == 0 {
                    self.rx.blocking_recv()
                } else {
                    self.rx.try_recv().ok()
                };
                let Some(request) = request else { break };
                waiting = self.prepare(request);
                prepared = true;
                if waiting.is_some() {
                    break;
                }
            }
            if self.active == 0 {
                if waiting.is_some() {
                    std::thread::sleep(Duration::from_millis(1));
                    continue;
                }
                return Ok(());
            }
            if prepared {
                let result = unsafe { (self.pool.api.submit_ios)(self.ring.as_ref().unwrap()) };
                if result < 0 {
                    return Err(new_std_io_error(std::io::Error::from_raw_os_error(-result)));
                }
            }
            // A bounded wait lets newly queued requests join an outstanding batch.
            let deadline = (SystemTime::now() + Duration::from_millis(1))
                .duration_since(UNIX_EPOCH)
                .unwrap();
            let timeout = libc::timespec {
                tv_sec: deadline.as_secs() as _,
                tv_nsec: deadline.subsec_nanos() as _,
            };
            let count = unsafe {
                (self.pool.api.wait_for_ios)(
                    self.ring.as_ref().unwrap(),
                    cqes.as_mut_ptr(),
                    cqes.len() as i32,
                    1,
                    &timeout,
                )
            };
            if count == -libc::ETIMEDOUT || count == -libc::EINTR {
                continue;
            }
            if count < 0 {
                return Err(new_std_io_error(std::io::Error::from_raw_os_error(-count)));
            }
            for cqe in &cqes[..count as usize] {
                let index = cqe.userdata as usize;
                let Pending { request, mut slot } =
                    self.pending[index].take().expect("known USRBIO completion");
                self.active -= 1;
                let result = if cqe.result < 0 {
                    Err(new_std_io_error(std::io::Error::from_raw_os_error(
                        -cqe.result as i32,
                    )))
                } else if cqe.result as u64 > request.size as u64 {
                    Err(Error::new(
                        ErrorKind::Unexpected,
                        "USRBIO completion exceeds request size",
                    ))
                } else {
                    let size = cqe.result as usize;
                    slot.len = size;
                    let buffer = if self.read && size > 0 {
                        slot.into_buffer()
                    } else {
                        Buffer::new()
                    };
                    Ok((size, buffer))
                };
                let _ = request.reply.send(result);
            }
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        let Some(mut ring) = self.ring.take() else {
            return;
        };
        if self.active == 0 {
            unsafe { (self.pool.api.iordestroy)(&mut ring) };
        } else {
            // USRBIO has no cancellation fence. On a fatal driver error, native
            // I/O may still touch these resources: quarantine them until exit.
            // Dropping a caller future alone never enters this path.
            for pending in self.pending.iter_mut().filter_map(Option::take) {
                let _ = pending
                    .request
                    .reply
                    .send(Err(Error::new(ErrorKind::Unexpected, self.failure.clone())));
                std::mem::forget((pending.request.file, pending.slot));
            }
            // The ring's native allocation remains live; leaked slots retain
            // the shared mapping and library needed by the in-flight requests.
        }
    }
}
