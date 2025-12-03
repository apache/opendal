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

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;

use compio::buf::{IoBuf, IoBuffer, IoVectoredBuf};
use compio::dispatcher::Dispatcher;

use crate::raw::*;
use crate::*;

#[derive(Clone, Debug)]
pub struct CompioBuffer(Buffer);

impl From<Buffer> for CompioBuffer {
    fn from(value: Buffer) -> Self {
        CompioBuffer(value)
    }
}

unsafe impl IoBuf for CompioBuffer {
    fn as_buf_ptr(&self) -> *const u8 {
        self.0.current().as_ptr()
    }

    fn buf_len(&self) -> usize {
        self.0.current().len()
    }

    fn buf_capacity(&self) -> usize {
        // `Bytes` doesn't expose uninitialized capacity, so treat it as the same as `len`
        self.0.current().len()
    }
}

#[derive(Debug)]
pub(super) struct CompfsCore {
    pub info: Arc<AccessorInfo>,

    pub root: PathBuf,
    pub dispatcher: Dispatcher,
    pub buf_pool: oio::PooledBuf,
}

impl CompfsCore {
    pub fn prepare_path(&self, path: &str) -> PathBuf {
        self.root.join(path.trim_end_matches('/'))
    }

    pub async fn exec<Fn, Fut, R>(&self, f: Fn) -> crate::Result<R>
    where
        Fn: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = std::io::Result<R>> + 'static,
        R: Send + 'static,
    {
        self.dispatcher
            .dispatch(f)
            .map_err(|_| Error::new(ErrorKind::Unexpected, "compio spawn io task failed"))?
            .await
            .map_err(|_| Error::new(ErrorKind::Unexpected, "compio task cancelled"))?
            .map_err(new_std_io_error)
    }

    pub async fn exec_blocking<Fn, R>(&self, f: Fn) -> Result<R>
    where
        Fn: FnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        self.dispatcher
            .dispatch_blocking(f)
            .map_err(|_| Error::new(ErrorKind::Unexpected, "compio spawn blocking task failed"))?
            .await
            .map_err(|_| Error::new(ErrorKind::Unexpected, "compio task cancelled"))
    }
}

impl IoVectoredBuf for CompioBuffer {
    unsafe fn iter_io_buffer(&self) -> impl Iterator<Item = IoBuffer> {
        self.0.clone().map(|b| unsafe { b.as_io_buffer() })
    }
}

#[cfg(test)]
mod tests {
    use bytes::Buf;
    use bytes::Bytes;
    use rand::Rng;
    use rand::thread_rng;

    use super::*;

    fn setup_buffer() -> (CompioBuffer, usize, Bytes) {
        let mut rng = thread_rng();

        let bs = (0..100)
            .map(|_| {
                let len = rng.gen_range(1..100);
                let mut buf = vec![0; len];
                rng.fill(&mut buf[..]);
                Bytes::from(buf)
            })
            .collect::<Vec<_>>();

        let total_size = bs.iter().map(|b| b.len()).sum::<usize>();
        let total_content = bs.iter().flatten().copied().collect::<Bytes>();
        let buf = CompioBuffer(Buffer::from(bs));

        (buf, total_size, total_content)
    }

    #[test]
    fn test_io_buf() {
        let (buf, _len, _bytes) = setup_buffer();
        let slice = IoBuf::as_slice(&buf);

        assert_eq!(slice, buf.0.current().chunk())
    }
}
