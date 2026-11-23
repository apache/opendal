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
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use opendal_core::raw::new_std_io_error;
use opendal_core::{Buffer, Error, ErrorKind, Result};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

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
    available: Condvar,
    permits: Arc<Semaphore>,
    #[cfg(test)]
    _backing: Option<Box<[u8]>>,
}

// The mapping is stable. Each slot has one owner until I/O completes;
// completed, retained read slots are immutable until their final owner drops.
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
        // Unlink the POSIX shared-memory name immediately after registration so
        // crashes or fatal-error quarantine cannot leave orphaned /dev/shm files.
        unsafe { (api.iovunlink)(&mut iov) };
        Ok(Arc::new(Self {
            available: Condvar::new(),
            api,
            iov,
            block_size,
            retain_limit: depth,
            permits: Arc::new(Semaphore::new(depth)),
            slots: Mutex::new(Slots {
                free: (0..count).collect(),
                retained: 0,
            }),
            #[cfg(test)]
            _backing: None,
        }))
    }
}

#[cfg(test)]
impl Pool {
    fn in_memory(api: Arc<Api>, depth: usize, block_size: usize) -> Arc<Self> {
        let mut backing = vec![0u8; depth * block_size].into_boxed_slice();
        let mut iov: Iov = unsafe { std::mem::zeroed() };
        iov.base = backing.as_mut_ptr();
        Arc::new(Self {
            available: Condvar::new(),
            api,
            iov,
            block_size,
            retain_limit: depth,
            permits: Arc::new(Semaphore::new(depth)),
            slots: Mutex::new(Slots {
                free: (0..depth).collect(),
                retained: 0,
            }),
            _backing: Some(backing),
        })
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
    _permit: Option<OwnedSemaphorePermit>,
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
        drop(slots);
        self.pool.available.notify_one();
    }
}

pub(crate) struct Request {
    file: Arc<RegisteredFile>,
    offset: usize,
    size: usize,
    slot: Option<Slot>,
    reply: oneshot::Sender<Result<(usize, Option<Slot>)>>,
}

#[cfg(test)]
impl Request {
    pub(crate) fn is_cancelled(&self) -> bool {
        self.reply.is_closed()
    }

    pub(crate) async fn cancelled(&mut self) {
        self.reply.closed().await;
    }
}

struct Pending {
    request: Request,
    slot: Slot,
}

#[derive(Clone)]
pub(crate) struct IoDriver {
    tx: mpsc::Sender<Request>,
    pub block_size: usize,
    read: bool,
    write_pool: Option<Arc<Pool>>,
    failure: Arc<OnceLock<String>>,
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
        let failure = Arc::new(OnceLock::new());
        let worker_failure = failure.clone();
        std::thread::Builder::new()
            .name(if read { "hf3fs-read" } else { "hf3fs-write" }.into())
            .spawn(move || {
                match Worker::new(api, &mount, depth, block_size, read, rx, worker_failure) {
                    Ok(mut worker) => {
                        if ready_tx.send(Ok(worker.pool.clone())).is_ok()
                            && let Err(error) = worker.run()
                        {
                            let _ = worker.failure.set(error.to_string());
                        }
                    }
                    Err(error) => {
                        let _ = ready_tx.send(Err(error));
                    }
                }
            })
            .map_err(new_std_io_error)?;
        let pool = ready_rx.recv().map_err(|e| {
            Error::new(ErrorKind::Unexpected, "USRBIO driver initialization failed").set_source(e)
        })??;
        Ok(Self {
            tx,
            block_size,
            read,
            write_pool: (!read).then_some(pool),
            failure,
        })
    }

    fn stopped_error(&self) -> Error {
        Error::new(
            ErrorKind::Unexpected,
            self.failure
                .get()
                .map_or("USRBIO driver stopped", String::as_str),
        )
    }

    #[cfg(test)]
    pub(crate) fn for_test(
        api: Arc<Api>,
        depth: usize,
        block_size: usize,
        read: bool,
    ) -> (Self, mpsc::Receiver<Request>, Arc<OnceLock<String>>) {
        let (tx, rx) = mpsc::channel(depth);
        let failure = Arc::new(OnceLock::new());
        let write_pool = (!read).then(|| Pool::in_memory(api, depth, block_size));
        (
            Self {
                tx,
                block_size,
                read,
                write_pool,
                failure: failure.clone(),
            },
            rx,
            failure,
        )
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
        let slot = if let Some(data) = data {
            let pool = self.write_pool.as_ref().expect("write pool");
            let permit = pool
                .permits
                .clone()
                .acquire_owned()
                .await
                .map_err(|_| self.stopped_error())?;
            let index = pool.slots.lock().unwrap().free.pop().expect("write slot");
            let slot = Slot {
                pool: pool.clone(),
                index,
                len: size,
                retained: false,
                _permit: Some(permit),
            };
            // Copy on the caller's thread before handing the exclusive slot to
            // the driver, so concurrent writers do not serialize their copies.
            let dst = unsafe {
                std::slice::from_raw_parts_mut(pool.iov.base.add(index * pool.block_size), size)
            };
            let mut offset = 0;
            for part in data {
                dst[offset..offset + part.len()].copy_from_slice(&part);
                offset += part.len();
            }
            Some(slot)
        } else {
            None
        };
        self.tx
            .send(Request {
                file,
                offset,
                size,
                slot,
                reply,
            })
            .await
            .map_err(|_| self.stopped_error())?;
        // Read overflow copies run on the caller, leaving the driver free to
        // submit and reap other requests.
        let (size, slot) = result.await.map_err(|_| self.stopped_error())??;
        Ok((size, slot.map_or_else(Buffer::new, Slot::into_buffer)))
    }
}

pub(crate) fn ring_entries(depth: usize) -> i32 {
    // Submission entries retire in order, even when I/O completes out of
    // order. Leave headroom for delayed entries without raising the I/O limit.
    // Clamp strictly below i32::MAX because upstream computes `entries + 1` in
    // signed 32-bit arithmetic.
    depth
        .saturating_mul(64)
        .saturating_add(1)
        .min((i32::MAX - 1) as usize) as i32
}

struct Worker {
    pool: Arc<Pool>,
    ring: Option<Ior>,
    read: bool,
    depth: usize,
    rx: mpsc::Receiver<Request>,
    pending: Vec<Option<Pending>>,
    active: usize,
    failure: Arc<OnceLock<String>>,
}

impl Worker {
    fn new(
        api: Arc<Api>,
        mount: &CStr,
        depth: usize,
        block_size: usize,
        read: bool,
        rx: mpsc::Receiver<Request>,
        failure: Arc<OnceLock<String>>,
    ) -> Result<Self> {
        let pool = Pool::new(api.clone(), mount, depth, block_size, read)?;
        let mut ring = unsafe { std::mem::zeroed() };
        let entries = ring_entries(depth);
        let result = unsafe {
            (api.iorcreate4)(
                &mut ring,
                mount.as_ptr(),
                entries,
                read,
                // Complete requests independently instead of waiting for a batch.
                -1,
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
            failure,
        })
    }

    fn prepare(&mut self, mut request: Request) -> Option<Request> {
        if request.reply.is_closed() {
            return None;
        }
        let slot = if let Some(slot) = request.slot.take() {
            slot
        } else {
            let Some(index) = self.pool.slots.lock().unwrap().free.pop() else {
                return Some(request);
            };
            Slot {
                pool: self.pool.clone(),
                index,
                len: request.size,
                retained: false,
                _permit: None,
            }
        };
        let index = slot.index;
        // The slot is exclusive, and the request fits within one Iov block.
        let ptr = unsafe { self.pool.iov.base.add(index * self.pool.block_size) };
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
            request.slot = Some(slot);
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
        let mut refill_until = Instant::now();
        loop {
            let mut prepared = false;
            // Retry submission after reaping completions, including when earlier
            // requests are still in flight.
            while self.active < self.depth {
                let request = if waiting.is_some() {
                    waiting.take()
                } else if self.active == 0 {
                    self.rx.blocking_recv()
                } else {
                    self.rx.try_recv().ok()
                };
                let Some(request) = request else { break };
                let active_before = self.active;
                waiting = self.prepare(request);
                if self.active > active_before {
                    prepared = true;
                }
                if waiting.is_some() {
                    break;
                }
            }
            if self.active == 0 {
                if let Some(request) = &waiting {
                    if request.slot.is_none() {
                        // Callers still copying completed reads can hold all slots.
                        drop(
                            self.pool
                                .available
                                .wait_while(self.pool.slots.lock().unwrap(), |slots| {
                                    slots.free.is_empty()
                                })
                                .unwrap(),
                        );
                    } else {
                        std::thread::sleep(Duration::from_millis(1));
                    }
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
            // Give callers just woken by completions a brief chance to refill the queue.
            let poll = self.read && self.active < self.depth && Instant::now() < refill_until;
            let timeout = if poll || self.active == self.depth {
                None
            } else {
                let deadline = (SystemTime::now() + Duration::from_micros(100))
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                Some(libc::timespec {
                    tv_sec: deadline.as_secs() as _,
                    tv_nsec: deadline.subsec_nanos() as _,
                })
            };
            let timeout_ptr = timeout.as_ref().map_or(std::ptr::null(), |t| t as *const _);
            let count = unsafe {
                (self.pool.api.wait_for_ios)(
                    self.ring.as_ref().unwrap(),
                    cqes.as_mut_ptr(),
                    cqes.len() as i32,
                    i32::from(!poll),
                    timeout_ptr,
                )
            };
            if poll && count == 0 {
                std::hint::spin_loop();
            }
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
                    let slot = (self.read && size > 0).then_some(slot);
                    Ok((size, slot))
                };
                let _ = request.reply.send(result);
            }
            if self.read && count > 0 {
                refill_until = Instant::now() + Duration::from_micros(20);
            }
        }
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        self.pool.permits.close();
        let Some(mut ring) = self.ring.take() else {
            return;
        };
        if self.active == 0 {
            unsafe { (self.pool.api.iordestroy)(&mut ring) };
        } else {
            // USRBIO has no cancellation fence. On a fatal driver error, native
            // I/O may still touch these resources: quarantine them until exit.
            // Dropping a caller future alone never enters this path.
            let message = self
                .failure
                .get()
                .map_or("USRBIO driver stopped with pending I/O", String::as_str);
            for pending in self.pending.iter_mut().filter_map(Option::take) {
                let _ = pending
                    .request
                    .reply
                    .send(Err(Error::new(ErrorKind::Unexpected, message)));
                std::mem::forget((pending.request.file, pending.slot));
            }
            // The ring's native allocation remains live; leaked slots retain
            // the shared mapping and library needed by the in-flight requests.
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_entries_stays_below_i32_max() {
        assert_eq!(ring_entries(1), 65);
        assert_eq!(ring_entries(32), 2049);
        assert_eq!(ring_entries(i32::MAX as usize - 1), i32::MAX - 1);
        assert_eq!(ring_entries(usize::MAX), i32::MAX - 1);
        assert!(ring_entries(usize::MAX).checked_add(1).is_some());
    }

    #[tokio::test]
    async fn stopped_driver_surfaces_shared_fatal_error() {
        let api = Api::noop();
        let (driver, rx, failure) = IoDriver::for_test(api.clone(), 2, 4096, true);
        let file = RegisteredFile::new(api, tempfile::tempfile().unwrap()).unwrap();

        let _ = failure.set("simulated fatal USRBIO submit error".to_string());
        drop(rx);

        let err = driver.submit(file, 0, 16, None).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(
            err.to_string()
                .contains("simulated fatal USRBIO submit error")
        );
    }
}
