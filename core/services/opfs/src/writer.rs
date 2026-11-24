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

#[cfg(target_arch = "wasm32")]
use std::sync::Arc;

#[cfg(target_arch = "wasm32")]
use send_wrapper::SendWrapper;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsCast;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsValue;
#[cfg(target_arch = "wasm32")]
use wasm_bindgen_futures::JsFuture;
#[cfg(target_arch = "wasm32")]
use web_sys::FileSystemWritableFileStream;
#[cfg(target_arch = "wasm32")]
use web_sys::WriteCommandType;
#[cfg(target_arch = "wasm32")]
use web_sys::WriteParams;

#[cfg(target_arch = "wasm32")]
use opendal_core::raw::*;
use opendal_core::*;

#[cfg(target_arch = "wasm32")]
use super::core::OpfsCore;
#[cfg(target_arch = "wasm32")]
use super::core::*;

pub(crate) trait StreamBackend {
    type Stream;

    async fn open_stream(&self) -> Result<Self::Stream>;
    async fn write_stream(&self, stream: &Self::Stream, bs: Buffer) -> Result<()>;
    async fn close_stream(&self, stream: &Self::Stream) -> Result<()>;
    async fn abort_stream(&self, stream: &Self::Stream) -> Result<()>;
}

enum WriterState<S> {
    Idle,
    Open(S),
    Closed,
    Aborted,
}

pub(crate) struct WriterMachine<B: StreamBackend> {
    backend: B,
    state: WriterState<B::Stream>,
    bytes_written: u64,
}

impl<B: StreamBackend> WriterMachine<B> {
    pub(crate) fn new(backend: B) -> Self {
        Self {
            backend,
            state: WriterState::Idle,
            bytes_written: 0,
        }
    }

    fn ensure_open(&self) -> Result<()> {
        match self.state {
            WriterState::Idle | WriterState::Open(_) => Ok(()),
            WriterState::Closed => Err(Error::new(
                ErrorKind::Unexpected,
                "writer is already closed",
            )),
            WriterState::Aborted => Err(Error::new(
                ErrorKind::Unexpected,
                "writer is already aborted",
            )),
        }
    }

    async fn ensure_stream(&mut self) -> Result<()> {
        self.ensure_open()?;
        if matches!(self.state, WriterState::Idle) {
            let stream = self.backend.open_stream().await?;
            self.state = WriterState::Open(stream);
        }
        Ok(())
    }

    pub(crate) async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.ensure_stream().await?;
        let len = bs.len() as u64;
        let WriterState::Open(stream) = &self.state else {
            unreachable!("opfs writable stream must be initialized");
        };
        self.backend.write_stream(stream, bs).await?;
        self.bytes_written += len;
        Ok(())
    }

    pub(crate) async fn close(&mut self) -> Result<Metadata> {
        self.ensure_stream().await?;
        // Keep the stream in `WriterState::Open` while awaiting `close` so a
        // failed close does not fall back to `Idle` and reopen a fresh
        // `FileSystemWritableFileStream` (which defaults to `keepExistingData: false`
        // and would truncate the file to 0 bytes).
        let WriterState::Open(stream) = &self.state else {
            unreachable!("opfs writable stream must be initialized");
        };
        self.backend.close_stream(stream).await?;
        self.state = WriterState::Closed;

        // We cannot set LastModified here - stream does not have such metadata.
        Ok(MetadataBuilder::file(self.bytes_written).build())
    }

    pub(crate) async fn abort(&mut self) -> Result<()> {
        match &self.state {
            WriterState::Idle => {
                self.state = WriterState::Aborted;
                Ok(())
            }
            WriterState::Open(stream) => {
                self.backend.abort_stream(stream).await?;
                self.state = WriterState::Aborted;
                Ok(())
            }
            WriterState::Closed => Err(Error::new(
                ErrorKind::Unexpected,
                "writer is already closed",
            )),
            WriterState::Aborted => Ok(()),
        }
    }
}

#[cfg(target_arch = "wasm32")]
struct OpfsStreamBackend {
    core: Arc<OpfsCore>,
    path: String,
}

#[cfg(target_arch = "wasm32")]
impl StreamBackend for OpfsStreamBackend {
    type Stream = SendWrapper<FileSystemWritableFileStream>;

    async fn open_stream(&self) -> Result<Self::Stream> {
        let p = build_absolute_path(&self.core.root, &self.path);
        let handle = get_file_handle(&p, true).await?;
        let stream: FileSystemWritableFileStream = JsFuture::from(handle.create_writable())
            .await
            .and_then(JsCast::dyn_into)
            .map_err(parse_js_error)?;
        Ok(SendWrapper::new(stream))
    }

    async fn write_stream(&self, stream: &Self::Stream, bs: Buffer) -> Result<()> {
        let bytes = bs.to_bytes();
        let params = WriteParams::new(WriteCommandType::Write);
        params.set_size(Some(bytes.len() as f64));
        let data: JsValue = js_sys::Uint8Array::from(bytes.as_ref()).into();
        params.set_data(&data);
        JsFuture::from(
            stream
                .write_with_write_params(&params)
                .map_err(parse_js_error)?,
        )
        .await
        .map_err(parse_js_error)?;
        Ok(())
    }

    async fn close_stream(&self, stream: &Self::Stream) -> Result<()> {
        JsFuture::from(stream.close())
            .await
            .map_err(parse_js_error)?;
        Ok(())
    }

    async fn abort_stream(&self, stream: &Self::Stream) -> Result<()> {
        JsFuture::from(stream.abort())
            .await
            .map_err(parse_js_error)?;
        Ok(())
    }
}

#[cfg(target_arch = "wasm32")]
pub struct OpfsWriter {
    inner: WriterMachine<OpfsStreamBackend>,
}

#[cfg(target_arch = "wasm32")]
impl OpfsWriter {
    pub fn new(core: Arc<OpfsCore>, path: String) -> Self {
        Self {
            inner: WriterMachine::new(OpfsStreamBackend { core, path }),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl oio::Write for OpfsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        self.inner.write(bs).await
    }

    async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }

    async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::future::Future;
    use std::pin::pin;
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};

    use super::*;

    fn block_on_ready<F: Future>(fut: F) -> F::Output {
        let mut fut = pin!(fut);
        let mut cx = Context::from_waker(Waker::noop());
        match fut.as_mut().poll(&mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("test future unexpectedly yielded Pending"),
        }
    }

    #[derive(Default)]
    struct MockState {
        open_calls: usize,
        close_calls: usize,
        abort_calls: usize,
        fail_next_close: bool,
        fail_next_abort: bool,
        committed: Option<Vec<u8>>,
    }

    #[derive(Clone)]
    struct MockStream {
        id: usize,
        staged: Rc<RefCell<Vec<u8>>>,
    }

    #[derive(Clone, Default)]
    struct MockBackend {
        state: Rc<RefCell<MockState>>,
    }

    impl StreamBackend for MockBackend {
        type Stream = MockStream;

        async fn open_stream(&self) -> Result<Self::Stream> {
            let mut state = self.state.borrow_mut();
            state.open_calls += 1;
            let id = state.open_calls;
            // Mirrors `createWritable()` default `keepExistingData: false`:
            // opening a new stream stages an empty replacement file.
            Ok(MockStream {
                id,
                staged: Rc::new(RefCell::new(Vec::new())),
            })
        }

        async fn write_stream(&self, stream: &Self::Stream, bs: Buffer) -> Result<()> {
            stream.staged.borrow_mut().extend_from_slice(&bs.to_bytes());
            Ok(())
        }

        async fn close_stream(&self, stream: &Self::Stream) -> Result<()> {
            let mut state = self.state.borrow_mut();
            state.close_calls += 1;
            if state.fail_next_close {
                state.fail_next_close = false;
                return Err(
                    Error::new(ErrorKind::Unexpected, "transient close error").set_temporary()
                );
            }
            let _ = stream.id;
            state.committed = Some(stream.staged.borrow().clone());
            Ok(())
        }

        async fn abort_stream(&self, _stream: &Self::Stream) -> Result<()> {
            let mut state = self.state.borrow_mut();
            state.abort_calls += 1;
            if state.fail_next_abort {
                state.fail_next_abort = false;
                return Err(
                    Error::new(ErrorKind::Unexpected, "transient abort error").set_temporary()
                );
            }
            Ok(())
        }
    }

    #[test]
    fn double_close_rejects_without_reopening_or_truncating() {
        let backend = MockBackend::default();
        let mut writer = WriterMachine::new(backend.clone());

        block_on_ready(writer.write(Buffer::from("hello "))).unwrap();
        block_on_ready(writer.write(Buffer::from("world"))).unwrap();
        let meta = block_on_ready(writer.close()).unwrap();
        assert_eq!(meta.content_length(), 11);
        assert_eq!(
            backend.state.borrow().committed.as_deref(),
            Some(b"hello world".as_slice())
        );

        // Second close must fail permanently without opening a new stream.
        let err = block_on_ready(writer.close()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        // Write and abort after close must also fail permanently.
        let err = block_on_ready(writer.write(Buffer::from("more"))).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err = block_on_ready(writer.abort()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let state = backend.state.borrow();
        assert_eq!(state.open_calls, 1);
        assert_eq!(state.close_calls, 1);
        assert_eq!(state.abort_calls, 0);
        assert_eq!(state.committed.as_deref(), Some(b"hello world".as_slice()));
    }

    #[test]
    fn retry_after_failed_close_reuses_open_stream_without_truncating() {
        let backend = MockBackend::default();
        backend.state.borrow_mut().fail_next_close = true;
        let mut writer = WriterMachine::new(backend.clone());

        block_on_ready(writer.write(Buffer::from("important data"))).unwrap();

        let err = block_on_ready(writer.close()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_temporary());
        assert_eq!(backend.state.borrow().committed, None);

        // Retrying close must reuse the existing open stream rather than
        // opening a fresh stream that would truncate the file to 0 bytes.
        let meta = block_on_ready(writer.close()).unwrap();
        assert_eq!(meta.content_length(), 14);

        let state = backend.state.borrow();
        assert_eq!(state.open_calls, 1);
        assert_eq!(state.close_calls, 2);
        assert_eq!(
            state.committed.as_deref(),
            Some(b"important data".as_slice())
        );
    }

    #[test]
    fn empty_close_opens_once_and_rejects_second_close() {
        let backend = MockBackend::default();
        let mut writer = WriterMachine::new(backend.clone());

        let meta = block_on_ready(writer.close()).unwrap();
        assert_eq!(meta.content_length(), 0);

        let err = block_on_ready(writer.close()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let state = backend.state.borrow();
        assert_eq!(state.open_calls, 1);
        assert_eq!(state.close_calls, 1);
        assert_eq!(state.committed.as_deref(), Some(b"".as_slice()));
    }

    #[test]
    fn abort_from_idle_is_idempotent_and_blocks_write_and_close() {
        let backend = MockBackend::default();
        let mut writer = WriterMachine::new(backend.clone());

        block_on_ready(writer.abort()).unwrap();
        block_on_ready(writer.abort()).unwrap();

        let err = block_on_ready(writer.write(Buffer::from("data"))).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err = block_on_ready(writer.close()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let state = backend.state.borrow();
        assert_eq!(state.open_calls, 0);
        assert_eq!(state.close_calls, 0);
        assert_eq!(state.abort_calls, 0);
        assert_eq!(state.committed, None);
    }

    #[test]
    fn abort_from_open_retries_on_failure_and_is_idempotent_after_success() {
        let backend = MockBackend::default();
        backend.state.borrow_mut().fail_next_abort = true;
        let mut writer = WriterMachine::new(backend.clone());

        block_on_ready(writer.write(Buffer::from("staged"))).unwrap();

        let err = block_on_ready(writer.abort()).unwrap_err();
        assert!(err.is_temporary());

        block_on_ready(writer.abort()).unwrap();
        block_on_ready(writer.abort()).unwrap();

        let err = block_on_ready(writer.close()).unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let state = backend.state.borrow();
        assert_eq!(state.open_calls, 1);
        assert_eq!(state.close_calls, 0);
        assert_eq!(state.abort_calls, 2);
        assert_eq!(state.committed, None);
    }
}
