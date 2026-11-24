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
    fn remove_file(&self, path: &str) -> io::Result<()>;
    fn rename_file(&self, from: &str, to: &str) -> io::Result<()>;
}

impl HdfsFileOps for hdrs::Client {
    fn remove_file(&self, path: &str) -> io::Result<()> {
        hdrs::Client::remove_file(self, path)
    }

    fn rename_file(&self, from: &str, to: &str) -> io::Result<()> {
        hdrs::Client::rename_file(self, from, to)
    }
}

pub struct HdfsWriter<F, C = hdrs::Client> {
    target_path: String,
    tmp_path: Option<String>,
    f: Option<F>,
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
            f: Some(f),
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
        let Some(f) = self.f.as_mut() else {
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
        let Some(mut f) = self.f.take() else {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "writer is already closed",
            ));
        };
        f.close().await.map_err(new_std_io_error)?;

        // TODO: we need to make rename async.
        if let Some(tmp_path) = &self.tmp_path {
            // HDFS rename does not overwrite an existing destination; remove any
            // existing target first and ignore NotFound when absent.
            match self.client.remove_file(&self.target_path) {
                Ok(()) => {}
                Err(err) if err.kind() == io::ErrorKind::NotFound => {}
                Err(err) => return Err(new_std_io_error(err)),
            }
            self.client
                .rename_file(tmp_path, &self.target_path)
                .map_err(new_std_io_error)?;
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
    use std::pin::Pin;
    use std::sync::Mutex;
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
        close_calls: usize,
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
            self.close_calls += 1;
            Poll::Ready(Ok(()))
        }
    }

    struct MockFileOps {
        remove_result: Mutex<io::Result<()>>,
        ops: Mutex<Vec<String>>,
    }

    impl MockFileOps {
        fn new(remove_result: io::Result<()>) -> Self {
            Self {
                remove_result: Mutex::new(remove_result),
                ops: Mutex::new(Vec::new()),
            }
        }
    }

    impl HdfsFileOps for MockFileOps {
        fn remove_file(&self, path: &str) -> io::Result<()> {
            self.ops.lock().unwrap().push(format!("remove:{path}"));
            let res = self.remove_result.lock().unwrap();
            match &*res {
                Ok(()) => Ok(()),
                Err(err) => Err(io::Error::new(err.kind(), err.to_string())),
            }
        }

        fn rename_file(&self, from: &str, to: &str) -> io::Result<()> {
            self.ops
                .lock()
                .unwrap()
                .push(format!("rename:{from}->{to}"));
            Ok(())
        }
    }

    #[tokio::test]
    async fn write_returns_error_on_zero_byte_write() {
        let client = Arc::new(MockFileOps::new(Ok(())));
        let mut writer = HdfsWriter::new("/target".to_string(), None, ZeroAsyncWriter, client, 0);

        let err = tokio::time::timeout(Duration::from_secs(5), writer.write(Buffer::from("hello")))
            .await
            .expect("zero-byte write must not loop infinitely")
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Unexpected);
    }

    #[tokio::test]
    async fn close_consumes_handle_and_ignores_missing_target_before_rename() {
        let client = Arc::new(MockFileOps::new(Err(io::Error::new(
            io::ErrorKind::NotFound,
            "already removed",
        ))));
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
                "remove:/target".to_string(),
                "rename:/tmp/atomic/target.tmp->/target".to_string(),
            ]
        );

        let write_after_close = writer.write(Buffer::from("more")).await.unwrap_err();
        assert_eq!(write_after_close.kind(), ErrorKind::Unexpected);

        let second_close = writer.close().await.unwrap_err();
        assert_eq!(second_close.kind(), ErrorKind::Unexpected);
        assert_eq!(client.ops.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn close_propagates_non_not_found_remove_errors() {
        let client = Arc::new(MockFileOps::new(Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "permission denied",
        ))));
        let mut writer = HdfsWriter::new(
            "/target".to_string(),
            Some("/tmp/atomic/target.tmp".to_string()),
            MockAsyncWriter::default(),
            Arc::clone(&client),
            0,
        );

        let err = tokio::time::timeout(Duration::from_secs(5), writer.close())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::PermissionDenied);
        assert_eq!(
            *client.ops.lock().unwrap(),
            vec!["remove:/target".to_string()]
        );
    }
}
