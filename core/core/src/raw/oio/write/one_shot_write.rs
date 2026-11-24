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

use crate::raw::*;
use crate::*;

/// OneShotWrite is used to implement [`oio::Write`] based on one shot operation.
/// By implementing OneShotWrite, services don't need to care about the details.
///
/// For example, S3 `PUT Object` and fs `write_all`.
///
/// The layout after adopting [`OneShotWrite`]:
pub trait OneShotWrite: Send + Sync + Unpin + 'static {
    /// write_once write all data at once.
    ///
    /// Implementations should make sure that the data is written correctly at once.
    fn write_once(&self, bs: Buffer) -> impl Future<Output = Result<Metadata>> + MaybeSend;
}

enum State {
    /// The writer is active and can accept a single buffer, close, or abort.
    Running,
    /// A close has been initiated and can be retried or aborted.
    Closing,
    /// The writer completed and committed the object.
    Completed,
    /// The writer was aborted.
    Aborted,
}

/// OneShotWrite is used to implement [`oio::Write`] based on one shot.
pub struct OneShotWriter<W: OneShotWrite> {
    inner: W,
    buffer: Option<Buffer>,
    state: State,
}

impl<W: OneShotWrite> OneShotWriter<W> {
    /// Create a new one shot writer.
    pub fn new(inner: W) -> Self {
        Self {
            inner,
            buffer: None,
            state: State::Running,
        }
    }
}

impl<W: OneShotWrite> oio::Write for OneShotWriter<W> {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        match self.state {
            State::Running => {}
            State::Closing => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "one shot writer is closing and can't accept more writes",
                ));
            }
            State::Completed => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "one shot writer has already been closed",
                ));
            }
            State::Aborted => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "one shot writer has already been aborted",
                ));
            }
        }

        match &self.buffer {
            Some(_) => Err(Error::new(
                ErrorKind::Unsupported,
                "OneShotWriter doesn't support multiple writes to the same upload. Streaming \
                 multiple buffers requires Capability::write_can_multi. Pass the complete payload \
                 as a single buffer to Operator::write or Writer::write, or set \
                 writer_with(...).chunk(size) to fit the complete payload when its size is known.",
            )),
            None => {
                self.buffer = Some(bs);
                Ok(())
            }
        }
    }

    async fn close(&mut self) -> Result<Metadata> {
        match self.state {
            State::Running | State::Closing => {}
            State::Completed => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "one shot writer has already been closed",
                ));
            }
            State::Aborted => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "one shot writer has already been aborted and can't be closed",
                ));
            }
        }

        self.state = State::Closing;
        let bs = self.buffer.clone().unwrap_or_default();
        let meta = self.inner.write_once(bs).await?;
        self.buffer = None;
        self.state = State::Completed;
        Ok(meta)
    }

    async fn abort(&mut self) -> Result<()> {
        match self.state {
            State::Completed | State::Aborted => return Ok(()),
            State::Running | State::Closing => {}
        }

        self.buffer = None;
        self.state = State::Aborted;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;

    use super::*;
    use crate::raw::oio::Write;

    struct TestWriter;

    impl OneShotWrite for TestWriter {
        async fn write_once(&self, _: Buffer) -> Result<Metadata> {
            panic!("OneShotWriter should defer writing until close")
        }
    }

    #[derive(Default)]
    struct RecordingState {
        payloads: Vec<bytes::Bytes>,
        failures: usize,
    }

    struct RecordingOneShotWrite {
        state: Arc<Mutex<RecordingState>>,
    }

    impl RecordingOneShotWrite {
        fn new(failures: usize) -> (Self, Arc<Mutex<RecordingState>>) {
            let state = Arc::new(Mutex::new(RecordingState {
                payloads: Vec::new(),
                failures,
            }));
            (
                Self {
                    state: state.clone(),
                },
                state,
            )
        }
    }

    impl OneShotWrite for RecordingOneShotWrite {
        async fn write_once(&self, bs: Buffer) -> Result<Metadata> {
            let mut state = self.state.lock().unwrap();
            state.payloads.push(bs.to_bytes());
            if state.failures > 0 {
                state.failures -= 1;
                return Err(
                    Error::new(ErrorKind::Unexpected, "transient write_once failure")
                        .set_temporary(),
                );
            }
            Ok(MetadataBuilder::file(bs.len() as u64).build())
        }
    }

    #[tokio::test]
    async fn second_write_returns_single_buffer_guidance() {
        let mut writer = OneShotWriter::new(TestWriter);
        writer.write(Buffer::from("first")).await.unwrap();

        let err = writer.write(Buffer::from("second")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert!(err.is_permanent());
        assert!(err.message().contains("Capability::write_can_multi"));
        assert!(err.message().contains("Operator::write"));
        assert!(err.message().contains("Writer::write"));
        assert!(err.message().contains(".chunk(size)"));
    }

    #[tokio::test]
    async fn test_one_shot_writer_close_retry_and_post_close_guards() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (inner, state) = RecordingOneShotWrite::new(1);
            let mut writer = OneShotWriter::new(inner);

            writer.write(Buffer::from("hello")).await?;

            let err = writer
                .close()
                .await
                .expect_err("first close must fail transiently");
            assert!(err.is_temporary());

            let meta = writer.close().await?;
            assert_eq!(meta.content_length(), 5);

            let err = writer
                .close()
                .await
                .expect_err("second close after success must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = writer
                .write(Buffer::from("world"))
                .await
                .expect_err("write after close must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            writer.abort().await?;

            let state = state.lock().unwrap();
            assert_eq!(
                state.payloads,
                vec![
                    bytes::Bytes::from_static(b"hello"),
                    bytes::Bytes::from_static(b"hello"),
                ]
            );
            Ok::<(), Error>(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_one_shot_writer_abort_rejects_write_and_close() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (inner, state) = RecordingOneShotWrite::new(0);
            let mut writer = OneShotWriter::new(inner);

            writer.write(Buffer::from("hello")).await?;
            writer.abort().await?;
            writer.abort().await?;

            let err = writer
                .close()
                .await
                .expect_err("close after abort must not publish 0-byte object");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            let err = writer
                .write(Buffer::from("world"))
                .await
                .expect_err("write after abort must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            assert!(state.lock().unwrap().payloads.is_empty());
            Ok::<(), Error>(())
        })
        .await
        .expect("test timed out")
    }

    #[tokio::test]
    async fn test_one_shot_writer_failed_close_rejects_write_and_can_abort() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (inner, state) = RecordingOneShotWrite::new(1);
            let mut writer = OneShotWriter::new(inner);

            writer.write(Buffer::from("hello")).await?;

            let err = writer
                .close()
                .await
                .expect_err("first close must fail transiently");
            assert!(err.is_temporary());

            let err = writer
                .write(Buffer::from("world"))
                .await
                .expect_err("write while closing must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            writer.abort().await?;

            let err = writer
                .close()
                .await
                .expect_err("close after aborting failed close must fail");
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert!(err.is_permanent());

            assert_eq!(
                state.lock().unwrap().payloads,
                vec![bytes::Bytes::from_static(b"hello")]
            );
            Ok::<(), Error>(())
        })
        .await
        .expect("test timed out")
    }
}
