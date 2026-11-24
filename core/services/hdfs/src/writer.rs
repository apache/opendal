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

use std::io;
use std::sync::Arc;

use bytes::Buf;
use futures::AsyncWrite;
use futures::AsyncWriteExt;

use opendal_core::raw::*;
use opendal_core::*;

pub(super) trait HdfsFileOps: Send + Sync {
    fn metadata(&self, path: &str) -> io::Result<()>;
    fn remove_file(&self, path: &str) -> io::Result<()>;
    fn rename_file(&self, from: &str, to: &str) -> io::Result<()>;
}

impl HdfsFileOps for hdrs::Client {
    fn metadata(&self, path: &str) -> io::Result<()> {
        hdrs::Client::metadata(self, path).map(|_| ())
    }

    fn remove_file(&self, path: &str) -> io::Result<()> {
        hdrs::Client::remove_file(self, path)
    }

    fn rename_file(&self, from: &str, to: &str) -> io::Result<()> {
        hdrs::Client::rename_file(self, from, to)
    }
}

enum HdfsWriteState<F> {
    Open(F),
    FileClosed,
    Published,
}

pub struct HdfsWriter<F, C = hdrs::Client> {
    target_path: String,
    tmp_path: Option<String>,
    state: HdfsWriteState<F>,
    client: Arc<C>,
    size: u64,
}

/// # Safety
///
/// We will only take `&mut Self` reference for HdfsWriter.
unsafe impl<F: Send, C: Send + Sync> Sync for HdfsWriter<F, C> {}

impl<F, C> HdfsWriter<F, C> {
    pub fn new(
        target_path: String,
        tmp_path: Option<String>,
        f: F,
        client: Arc<C>,
        initial_size: u64,
    ) -> Self {
        Self {
            target_path,
            tmp_path,
            state: HdfsWriteState::Open(f),
            client,
            size: initial_size,
        }
    }
}

impl<F, C> oio::Write for HdfsWriter<F, C>
where
    F: AsyncWrite + Unpin + Send,
    C: HdfsFileOps,
{
    async fn write(&mut self, mut bs: Buffer) -> Result<()> {
        let HdfsWriteState::Open(f) = &mut self.state else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "writer is already closed",
            ));
        };

        while bs.has_remaining() {
            let n = f.write(bs.chunk()).await.map_err(new_std_io_error)?;
            if n == 0 {
                return Err(new_std_io_error(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                )));
            }
            bs.advance(n);
            self.size += n as u64;
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        match &mut self.state {
            HdfsWriteState::Open(f) => {
                f.close().await.map_err(new_std_io_error)?;
                self.state = if self.tmp_path.is_some() {
                    HdfsWriteState::FileClosed
                } else {
                    HdfsWriteState::Published
                };
            }
            HdfsWriteState::FileClosed => {}
            HdfsWriteState::Published => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "writer is already closed",
                ));
            }
        }

        // TODO: we need to make rename async.
        if let Some(tmp_path) = &self.tmp_path {
            // HDFS rename does not overwrite an existing destination, and
            // libhdfs reports EIO (not ENOENT) when deleting a missing path.
            // Check metadata first and only remove when the target exists.
            match self.client.metadata(&self.target_path) {
                Ok(()) => {
                    if let Err(err) = self.client.remove_file(&self.target_path) {
                        // Re-check metadata in case the target was removed
                        // concurrently between metadata() and remove_file().
                        match self.client.metadata(&self.target_path) {
                            Err(meta_err) if meta_err.kind() == io::ErrorKind::NotFound => {}
                            _ => return Err(new_std_io_error(err)),
                        }
                    }
                }
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(new_std_io_error(err)),
            }
            self.client
                .rename_file(tmp_path, &self.target_path)
                .map_err(new_std_io_error)?;
            self.state = HdfsWriteState::Published;
        }

        Ok(MetadataBuilder::file(self.size).build())
    }

    async fn abort(&mut self) -> Result<()> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "HdfsWriter doesn't support abort",
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::task::Context;
    use std::task::Poll;
    use std::time::Duration;

    use opendal_core::raw::oio::Write;

    use super::*;

    struct ZeroAsyncWriter;

    impl AsyncWrite for ZeroAsyncWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            _buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            Poll::Ready(Ok(0))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Default)]
    struct MockAsyncWriter {
        written: Vec<u8>,
        close_calls: Arc<AtomicUsize>,
        close_results: VecDeque<io::Result<()>>,
    }

    impl MockAsyncWriter {
        fn with_close_results(
            close_calls: Arc<AtomicUsize>,
            results: impl IntoIterator<Item = io::Result<()>>,
        ) -> Self {
            Self {
                written: Vec::new(),
                close_calls,
                close_results: results.into_iter().collect(),
            }
        }
    }

    impl AsyncWrite for MockAsyncWriter {
        fn poll_write(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.written.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_close(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.close_calls.fetch_add(1, Ordering::SeqCst);
            let result = self.close_results.pop_front().unwrap_or(Ok(()));
            Poll::Ready(result)
        }
    }

    struct MockFileOps {
        target_exists: Mutex<bool>,
        metadata_results: Mutex<VecDeque<io::Result<()>>>,
        remove_results: Mutex<VecDeque<io::Result<()>>>,
        rename_results: Mutex<VecDeque<io::Result<()>>>,
        ops: Mutex<Vec<String>>,
    }

    impl MockFileOps {
        fn new(target_exists: bool) -> Self {
            Self {
                target_exists: Mutex::new(target_exists),
                metadata_results: Mutex::new(VecDeque::new()),
                remove_results: Mutex::new(VecDeque::new()),
                rename_results: Mutex::new(VecDeque::new()),
                ops: Mutex::new(Vec::new()),
            }
        }

        fn with_metadata_results(self, results: impl IntoIterator<Item = io::Result<()>>) -> Self {
            *self.metadata_results.lock().unwrap() = results.into_iter().collect();
            self
        }

        fn with_remove_results(self, results: impl IntoIterator<Item = io::Result<()>>) -> Self {
            *self.remove_results.lock().unwrap() = results.into_iter().collect();
            self
        }

        fn with_rename_results(self, results: impl IntoIterator<Item = io::Result<()>>) -> Self {
            *self.rename_results.lock().unwrap() = results.into_iter().collect();
            self
        }
    }

    impl HdfsFileOps for MockFileOps {
        fn metadata(&self, path: &str) -> io::Result<()> {
            self.ops.lock().unwrap().push(format!("metadata:{path}"));
            if let Some(res) = self.metadata_results.lock().unwrap().pop_front() {
                return res;
            }
            if *self.target_exists.lock().unwrap() {
                Ok(())
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "target missing"))
            }
        }

        fn remove_file(&self, path: &str) -> io::Result<()> {
            self.ops.lock().unwrap().push(format!("remove:{path}"));
            if let Some(res) = self.remove_results.lock().unwrap().pop_front() {
                return res;
            }
            let mut exists = self.target_exists.lock().unwrap();
            if *exists {
                *exists = false;
                Ok(())
            } else {
                // Mirror libhdfs hdfsDelete, which sets errno = EIO (Other) rather
                // than ENOENT when FileSystem#delete returns false for a missing path.
                Err(io::Error::other("EIO: delete returned false"))
            }
        }

        fn rename_file(&self, from: &str, to: &str) -> io::Result<()> {
            self.ops
                .lock()
                .unwrap()
                .push(format!("rename:{from}->{to}"));
            if let Some(res) = self.rename_results.lock().unwrap().pop_front() {
                return res;
            }
            *self.target_exists.lock().unwrap() = true;
            Ok(())
        }
    }

    #[tokio::test]
    async fn write_returns_error_on_zero_byte_write() {
        let client = Arc::new(MockFileOps::new(false));
        let mut writer = HdfsWriter::new("/target".to_string(), None, ZeroAsyncWriter, client, 0);

        let err = tokio::time::timeout(Duration::from_secs(5), writer.write(Buffer::from("hello")))
            .await
            .expect("zero-byte write must not loop infinitely")
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn close_skips_remove_when_new_target_is_missing() {
        let client = Arc::new(MockFileOps::new(false));
        let mut writer = HdfsWriter::new(
            "/target".to_string(),
            Some("/tmp/atomic/target.tmp".to_string()),
            MockAsyncWriter::default(),
            Arc::clone(&client),
            0,
        );

        tokio::time::timeout(Duration::from_secs(5), writer.write(Buffer::from("hello")))
            .await
            .unwrap()
            .unwrap();
        let meta = tokio::time::timeout(Duration::from_secs(5), writer.close())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(meta.content_length(), 5);
        assert_eq!(
            *client.ops.lock().unwrap(),
            vec![
                "metadata:/target".to_string(),
                "rename:/tmp/atomic/target.tmp->/target".to_string(),
            ]
        );

        let write_after_close = writer.write(Buffer::from("more")).await.unwrap_err();
        assert_eq!(write_after_close.kind(), ErrorKind::Unexpected);
        assert!(!write_after_close.is_temporary());

        let second_close = writer.close().await.unwrap_err();
        assert_eq!(second_close.kind(), ErrorKind::Unexpected);
        assert!(!second_close.is_temporary());
        assert_eq!(client.ops.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn close_removes_existing_target_before_rename() {
        let client = Arc::new(MockFileOps::new(true));
        let mut writer = HdfsWriter::new(
            "/target".to_string(),
            Some("/tmp/atomic/target.tmp".to_string()),
            MockAsyncWriter::default(),
            Arc::clone(&client),
            0,
        );

        writer.write(Buffer::from("updated")).await.unwrap();
        let meta = writer.close().await.unwrap();
        assert_eq!(meta.content_length(), 7);
        assert_eq!(
            *client.ops.lock().unwrap(),
            vec![
                "metadata:/target".to_string(),
                "remove:/target".to_string(),
                "rename:/tmp/atomic/target.tmp->/target".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn close_ignores_eio_when_target_disappears_before_remove() {
        let client = Arc::new(
            MockFileOps::new(false)
                .with_metadata_results([
                    Ok(()),
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        "concurrently deleted",
                    )),
                ])
                .with_remove_results([Err(io::Error::other("EIO: delete returned false"))]),
        );
        let mut writer = HdfsWriter::new(
            "/target".to_string(),
            Some("/tmp/atomic/target.tmp".to_string()),
            MockAsyncWriter::default(),
            Arc::clone(&client),
            0,
        );

        writer.write(Buffer::from("raced")).await.unwrap();
        let meta = writer.close().await.unwrap();
        assert_eq!(meta.content_length(), 5);
        assert_eq!(
            *client.ops.lock().unwrap(),
            vec![
                "metadata:/target".to_string(),
                "remove:/target".to_string(),
                "metadata:/target".to_string(),
                "rename:/tmp/atomic/target.tmp->/target".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn close_propagates_non_not_found_metadata_and_remove_errors() {
        let meta_client = Arc::new(MockFileOps::new(false).with_metadata_results([Err(
            io::Error::new(io::ErrorKind::PermissionDenied, "permission denied"),
        )]));
        let mut meta_writer = HdfsWriter::new(
            "/target".to_string(),
            Some("/tmp/atomic/target.tmp".to_string()),
            MockAsyncWriter::default(),
            Arc::clone(&meta_client),
            0,
        );

        let err = meta_writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert_eq!(
            *meta_client.ops.lock().unwrap(),
            vec!["metadata:/target".to_string()]
        );

        let remove_client = Arc::new(MockFileOps::new(true).with_remove_results([Err(
            io::Error::new(io::ErrorKind::PermissionDenied, "permission denied"),
        )]));
        let mut remove_writer = HdfsWriter::new(
            "/target".to_string(),
            Some("/tmp/atomic/target.tmp".to_string()),
            MockAsyncWriter::default(),
            Arc::clone(&remove_client),
            0,
        );

        let err = remove_writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert_eq!(
            *remove_client.ops.lock().unwrap(),
            vec![
                "metadata:/target".to_string(),
                "remove:/target".to_string(),
                "metadata:/target".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn close_retries_after_transient_file_close_error() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(MockFileOps::new(false));
        let mut writer = HdfsWriter::new(
            "/target".to_string(),
            Some("/tmp/atomic/target.tmp".to_string()),
            MockAsyncWriter::with_close_results(
                Arc::clone(&close_calls),
                [
                    Err(io::Error::new(io::ErrorKind::TimedOut, "transient close")),
                    Ok(()),
                ],
            ),
            Arc::clone(&client),
            0,
        );

        writer.write(Buffer::from("hello")).await.unwrap();

        let err = writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_temporary());
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert!(client.ops.lock().unwrap().is_empty());

        let meta = writer.close().await.unwrap();
        assert_eq!(meta.content_length(), 5);
        assert_eq!(close_calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            *client.ops.lock().unwrap(),
            vec![
                "metadata:/target".to_string(),
                "rename:/tmp/atomic/target.tmp->/target".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn close_retries_publish_from_file_closed_without_reclosing_file() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(MockFileOps::new(false).with_rename_results([
            Err(io::Error::new(io::ErrorKind::TimedOut, "transient rename")),
            Ok(()),
        ]));
        let mut writer = HdfsWriter::new(
            "/target".to_string(),
            Some("/tmp/atomic/target.tmp".to_string()),
            MockAsyncWriter::with_close_results(Arc::clone(&close_calls), [Ok(())]),
            Arc::clone(&client),
            0,
        );

        writer.write(Buffer::from("hello")).await.unwrap();

        let err = writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(err.is_temporary());
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);

        let write_in_file_closed = writer.write(Buffer::from("more")).await.unwrap_err();
        assert_eq!(write_in_file_closed.kind(), ErrorKind::Unexpected);
        assert!(!write_in_file_closed.is_temporary());

        let meta = writer.close().await.unwrap();
        assert_eq!(meta.content_length(), 5);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            *client.ops.lock().unwrap(),
            vec![
                "metadata:/target".to_string(),
                "rename:/tmp/atomic/target.tmp->/target".to_string(),
                "metadata:/target".to_string(),
                "rename:/tmp/atomic/target.tmp->/target".to_string(),
            ]
        );

        let second_close = writer.close().await.unwrap_err();
        assert_eq!(second_close.kind(), ErrorKind::Unexpected);
        assert!(!second_close.is_temporary());
    }

    #[tokio::test]
    async fn direct_close_transitions_directly_to_published() {
        let close_calls = Arc::new(AtomicUsize::new(0));
        let client = Arc::new(MockFileOps::new(false));
        let mut writer = HdfsWriter::new(
            "/target".to_string(),
            None,
            MockAsyncWriter::with_close_results(Arc::clone(&close_calls), [Ok(())]),
            Arc::clone(&client),
            0,
        );

        writer.write(Buffer::from("direct")).await.unwrap();
        let meta = writer.close().await.unwrap();
        assert_eq!(meta.content_length(), 6);
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
        assert!(client.ops.lock().unwrap().is_empty());

        let err = writer.write(Buffer::from("more")).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());

        let err = writer.close().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert!(!err.is_temporary());
        assert_eq!(close_calls.load(Ordering::SeqCst), 1);
    }
}
