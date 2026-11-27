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

use std::fmt;
use std::sync::Arc;

use bytes::Buf;

use crate::raw::*;
use crate::*;

/// Writer is designed to write data into given path in an asynchronous
/// manner.
///
/// ## Notes
///
/// Please make sure either `close` or `abort` has been called before
/// dropping the writer otherwise the data could be lost.
///
/// ## Usage
///
/// ### Write Multiple Chunks
///
/// Sending multiple chunks to the service requires [`Capability::write_can_multi`].
/// A writer that accepts only one chunk returns [`ErrorKind::Unsupported`] if it
/// receives a second chunk. See [`Operator::writer_with`] for buffering options.
///
/// ```
/// use opendal_core::Operator;
/// use opendal_core::Result;
///
/// async fn test(op: Operator) -> Result<()> {
///     let mut w = op.writer("path/to/file").await?;
///     w.write(vec![1; 1024]).await?;
///     w.write(vec![2; 1024]).await?;
///     w.close().await?;
///     Ok(())
/// }
/// ```
///
/// ### Write like `Sink`
///
/// ```
/// use anyhow::Result;
/// use futures::SinkExt;
/// use opendal_core::Operator;
///
/// async fn test(op: Operator) -> Result<()> {
///     let mut w = op.writer("path/to/file").await?.into_bytes_sink();
///     w.send(vec![1; 1024].into()).await?;
///     w.send(vec![2; 1024].into()).await?;
///     w.close().await?;
///     Ok(())
/// }
/// ```
///
/// ### Write like `AsyncWrite`
///
/// ```
/// use anyhow::Result;
/// use futures::AsyncWriteExt;
/// use opendal_core::Operator;
///
/// async fn test(op: Operator) -> Result<()> {
///     let mut w = op.writer("path/to/file").await?.into_futures_async_write();
///     w.write(&vec![1; 1024]).await?;
///     w.write(&vec![2; 1024]).await?;
///     w.close().await?;
///     Ok(())
/// }
/// ```
///
/// ### Write with append enabled
///
/// Writer also supports to write with append enabled. This is useful when users want to append
/// some data to the end of the file.
///
/// - If file doesn't exist, it will be created and just like calling `write`.
/// - If file exists, data will be appended to the end of the file.
///
/// Possible Errors:
///
/// - Some services store normal file and appendable file in different way. Trying to append
///   on non-appendable file could return [`ErrorKind::Conflict`] error.
/// - Services that doesn't support append will return [`ErrorKind::Unsupported`] error when
///   creating writer with `append` enabled.
pub struct Writer {
    /// Keep a reference to write context in writer.
    ctx: Arc<WriteContext>,
    inner: WriteState,
}

impl fmt::Debug for Writer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Writer")
            .field("chunk", &self.ctx.options().chunk())
            .field("append", &self.ctx.args().append())
            .finish_non_exhaustive()
    }
}

impl Writer {
    /// Create a new writer from an `oio::Writer`.
    pub(crate) fn new(ctx: WriteContext) -> Result<Self> {
        let ctx = Arc::new(ctx);
        let inner = WriteState::create(ctx.clone())?;

        Ok(Self { ctx, inner })
    }

    /// Write [`Buffer`] into writer.
    ///
    /// This operation will write all data in given buffer into writer.
    ///
    /// When a single call spans multiple chunks, a failure after the underlying
    /// writer accepts part of the input returns a persistent error. Call
    /// [`Writer::abort`] and start a new writer if you need to retry that write.
    ///
    /// ## Examples
    ///
    /// ```
    /// use bytes::Bytes;
    /// use opendal_core::Operator;
    /// use opendal_core::Result;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let mut w = op.writer("hello.txt").await?;
    ///     // Buffer can be created from continues bytes.
    ///     w.write("hello, world").await?;
    ///     // Buffer can also be created from non-continues bytes.
    ///     w.write(vec![Bytes::from("hello,"), Bytes::from("world!")])
    ///         .await?;
    ///
    ///     // Make sure file has been written completely.
    ///     w.close().await?;
    ///     Ok(())
    /// }
    /// ```
    pub async fn write(&mut self, bs: impl Into<Buffer>) -> Result<()> {
        self.inner.write_all(bs.into()).await
    }

    /// Write [`bytes::Buf`] into inner writer.
    ///
    /// This operation will write all data in given buffer into writer.
    ///
    /// # Notes
    ///
    /// This function iterates over each chunk in the `Buf` and collects them
    /// into a `Buffer`. For `Bytes` chunks, `copy_to_bytes` is zero-copy
    /// (only a ref-count increment). For other chunk types (e.g., `&[u8]`),
    /// the data will be copied.
    ///
    /// This approach minimizes copies for chained buffers (`Chain<Bytes, Bytes>`)
    /// where each individual chunk can be extracted without copying.
    pub async fn write_from(&mut self, bs: impl Buf) -> Result<()> {
        let mut bs = bs;

        // Fast path: single contiguous chunk.
        // If chunk length equals remaining bytes, the entire buffer is contiguous.
        // This avoids Vec allocation and loop overhead for the common case.
        if bs.chunk().len() == bs.remaining() {
            let bytes = bs.copy_to_bytes(bs.remaining());
            return self.write(Buffer::from(bytes)).await;
        }

        // Slow path: multiple chunks (e.g., Chain<T, U>).
        // Iterate over each chunk and collect them into a Vec<Bytes>.
        // For Bytes chunks, copy_to_bytes is zero-copy.
        // For other types, only the current chunk is copied, not the entire buffer.
        let mut chunks = Vec::new();
        while bs.has_remaining() {
            let chunk_len = bs.chunk().len();
            chunks.push(bs.copy_to_bytes(chunk_len));
        }
        self.write(Buffer::from(chunks)).await
    }

    /// Copy a range of an existing object into this writer.
    ///
    /// The source path is resolved by the same [`Operator`] that created this
    /// writer. Calls to `write` and `copy_from` may be interleaved, and their
    /// call order defines the destination bytes. OpenDAL uses a native
    /// writer-side copy when the composed service supports it and otherwise
    /// streams the range through the client. Check
    /// [`Capability::write_can_copy_from`] to determine whether eligible
    /// non-append writes use the native path.
    ///
    /// A writer created with [`options::WriteOptions::checksum`] always
    /// streams the range through the client, even when the service supports
    /// native copies. Every copied byte is then downloaded from the source
    /// and uploaded again.
    ///
    /// The source path must differ from the writer's destination path.
    /// Copying a file onto itself returns [`ErrorKind::IsSameFile`].
    ///
    /// Each call accepts at most 5 GiB. Applications must split larger source
    /// ranges into multiple ordered calls.
    ///
    /// If the streaming source returns [`ErrorKind::ChecksumMismatch`], the
    /// call returns the source error. If the writer accepted any bytes from
    /// that logical copy, including bytes queued in the writer's buffer or
    /// accepted on an earlier attempt, further copies, writes, and
    /// [`Writer::close`] fail. The writer requires [`Writer::abort`].
    ///
    /// If the copy accepted no bytes, the writer can still copy from another
    /// source or continue writing. If you ignore the source error and close
    /// without appending anything else, a successful [`Writer::close`] commits
    /// only the previously accepted prefix.
    ///
    /// For other failures after accepting bytes from the copy, retrying the
    /// same path, range, and read options resumes after the accepted prefix.
    /// Until that retry succeeds or [`Writer::abort`] runs, other writes and
    /// [`Writer::close`] fail.
    pub async fn copy_from(&mut self, path: &str, range: impl Into<BytesRange>) -> Result<()> {
        self.copy_from_options(
            path,
            options::ReadOptions {
                range: range.into(),
                ..Default::default()
            },
        )
        .await
    }

    /// Copy an existing object range with read conditions and execution options.
    ///
    /// [`options::ReadOptions::limit`] bounds the number of bytes copied from
    /// the start of the selected range and accepts EOF after a valid starting
    /// byte, matching [`Operator::read_options`]. A zero limit copies nothing.
    /// A non-zero limit on a forward selection that starts at or beyond EOF
    /// returns [`crate::ErrorKind::RangeNotSatisfied`].
    /// Combining a limit with a positive `chunk` or with `concurrent` greater
    /// than one returns [`ErrorKind::ConfigInvalid`].
    ///
    /// The source path must differ from the writer's destination path unless
    /// [`options::ReadOptions::version`] selects a specific source version to
    /// restore. Copying the current object onto itself returns
    /// [`ErrorKind::IsSameFile`].
    ///
    /// The retry and abort rules in [`Writer::copy_from`] apply. After a source
    /// [`ErrorKind::ChecksumMismatch`], abort if this logical copy accepted any
    /// bytes across its attempts. If it accepted none and you ignore the error
    /// and close without appending anything else, a successful [`Writer::close`]
    /// commits only the previously accepted prefix.
    pub async fn copy_from_options(
        &mut self,
        path: &str,
        options: options::ReadOptions,
    ) -> Result<()> {
        let path = normalize_path(path);
        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "copy source path is a directory")
                    .with_operation("Writer::copy_from")
                    .with_context("service", self.ctx.service().info().scheme())
                    .with_context("path", &path),
            );
        }
        if path == self.ctx.path() && options.version.is_none() {
            return Err(Error::new(
                ErrorKind::IsSameFile,
                "source and destination paths are the same",
            )
            .with_operation("Writer::copy_from")
            .with_context("service", self.ctx.service().info().scheme())
            .with_context("source", &path)
            .with_context("destination", self.ctx.path()));
        }
        if options.limit.is_some()
            && (options.chunk.is_some_and(|chunk| chunk != 0) || options.concurrent > 1)
        {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "read limit cannot be combined with chunk or concurrent reads",
            ));
        }
        let limit = options.limit;
        let (range, args, options) =
            OpRead::from_options(&self.ctx.service().capability(), options)
                .map_err(|err| err.with_context("service", self.ctx.service().info().scheme()))?;
        self.inner
            .copy_from(&path, args, options, range, limit)
            .await
    }

    /// Abort the writer, discarding buffered data and asking the backend to
    /// clean up the unfinished write.
    ///
    /// Cleanup depends on the backend. In particular, abort cannot guarantee
    /// that already visible data is removed or that a previous object is restored.
    /// Backend cleanup errors are returned and abort can be retried. Once abort
    /// starts, writes and close return [`ErrorKind::Unexpected`].
    pub async fn abort(&mut self) -> Result<()> {
        self.inner.abort().await
    }

    /// Close the writer and make sure all data have been committed.
    ///
    /// ## Notes
    ///
    /// Close should only be called when the writer is not closed or
    /// aborted, otherwise an unexpected error could be returned.
    pub async fn close(&mut self) -> Result<Metadata> {
        self.inner.close().await
    }

    /// Convert this writer into a [`BufferSink`] that implements
    /// [`Sink<Buffer>`][futures::Sink].
    ///
    /// # Notes
    ///
    /// BufferSink is a zero-cost abstraction. The underlying writer
    /// will reuse the Bytes and won't perform any copy operation over data.
    ///
    /// # Examples
    ///
    /// ## Basic Usage
    ///
    /// ```
    /// use std::io;
    ///
    /// use bytes::Bytes;
    /// use futures::SinkExt;
    /// use opendal_core::Buffer;
    /// use opendal_core::Operator;
    /// use opendal_core::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut s = op.writer("hello.txt").await?.into_sink();
    ///     let bs = "Hello, World!".as_bytes();
    ///     s.send(Buffer::from(bs)).await?;
    ///     s.close().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Concurrent Write
    ///
    /// ```
    /// use std::io;
    ///
    /// use bytes::Bytes;
    /// use futures::SinkExt;
    /// use opendal_core::Buffer;
    /// use opendal_core::Operator;
    /// use opendal_core::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut w = op
    ///         .writer_with("hello.txt")
    ///         .concurrent(8)
    ///         .chunk(256)
    ///         .await?
    ///         .into_sink();
    ///     let bs = "Hello, World!".as_bytes();
    ///     w.send(Buffer::from(bs)).await?;
    ///     w.close().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn into_sink(self) -> BufferSink {
        BufferSink::new(self.inner)
    }

    /// Convert this writer into a [`FuturesAsyncWriter`] that implements [`futures::AsyncWrite`].
    ///
    /// The adapter copies input into a 256 KiB buffer. Writing beyond that buffer
    /// or calling `flush()` can produce multiple chunks. Sending multiple chunks
    /// to the service requires [`Capability::write_can_multi`]. Call `close()` to
    /// commit the write.
    ///
    /// When this capability is false, pass the complete [`Buffer`] to
    /// [`Operator::write`] or to a single [`Writer::write`] call on a default writer.
    /// To use this adapter with a known payload size, set `chunk(size)` with
    /// [`Operator::writer_with`] to buffer the full payload in one chunk.
    ///
    /// # Examples
    ///
    /// ## Basic Usage
    ///
    /// ```
    /// use std::io;
    ///
    /// use futures::io::AsyncWriteExt;
    /// use opendal_core::Operator;
    /// use opendal_core::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut w = op.writer("hello.txt").await?.into_futures_async_write();
    ///     let bs = "Hello, World!".as_bytes();
    ///     w.write_all(bs).await?;
    ///     w.close().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Concurrent Write
    ///
    /// ```
    /// use std::io;
    ///
    /// use futures::io::AsyncWriteExt;
    /// use opendal_core::Operator;
    /// use opendal_core::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut w = op
    ///         .writer_with("hello.txt")
    ///         .concurrent(8)
    ///         .chunk(256)
    ///         .await?
    ///         .into_futures_async_write();
    ///     let bs = "Hello, World!".as_bytes();
    ///     w.write_all(bs).await?;
    ///     w.close().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn into_futures_async_write(self) -> FuturesAsyncWriter {
        FuturesAsyncWriter::new(self.inner)
    }

    /// Convert this writer into a [`FuturesBytesSink`] that implements [`futures::Sink<Bytes>`].
    ///
    /// The sink passes owned [`bytes::Bytes`] to the underlying writer without copying
    /// the payload. Sending multiple chunks to the service requires
    /// [`Capability::write_can_multi`].
    ///
    /// When this capability is false, pass the complete payload as one
    /// [`bytes::Bytes`] item on a default writer. You can also pass the full
    /// [`Buffer`] to [`Operator::write`] or to a single [`Writer::write`] call.
    /// If you know the payload size in advance, set `chunk(size)` with
    /// [`Operator::writer_with`] to buffer the full payload in one chunk.
    ///
    /// # Examples
    ///
    /// ## Basic Usage
    ///
    /// ```
    /// use std::io;
    ///
    /// use bytes::Bytes;
    /// use futures::SinkExt;
    /// use opendal_core::Operator;
    /// use opendal_core::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut w = op.writer("hello.txt").await?.into_bytes_sink();
    ///     let bs = "Hello, World!".as_bytes();
    ///     w.send(Bytes::from(bs)).await?;
    ///     w.close().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## Concurrent Write
    ///
    /// ```
    /// use std::io;
    ///
    /// use bytes::Bytes;
    /// use futures::SinkExt;
    /// use opendal_core::Operator;
    /// use opendal_core::Result;
    ///
    /// async fn test(op: Operator) -> io::Result<()> {
    ///     let mut w = op
    ///         .writer_with("hello.txt")
    ///         .concurrent(8)
    ///         .chunk(256)
    ///         .await?
    ///         .into_bytes_sink();
    ///     let bs = "Hello, World!".as_bytes();
    ///     w.send(Bytes::from(bs)).await?;
    ///     w.close().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn into_bytes_sink(self) -> FuturesBytesSink {
        FuturesBytesSink::new(self.inner)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{Buf, Bytes};
    use rand::{Rng, RngExt};

    use crate::Buffer;
    use crate::BytesRange;
    use crate::Capability;
    use crate::Error;
    use crate::ErrorKind;
    use crate::Metadata;
    use crate::OperationContext;
    use crate::Operator;
    use crate::Result;
    use crate::options;
    use crate::raw::*;
    use crate::services;

    fn gen_random_bytes() -> Vec<u8> {
        let mut rng = rand::rng();
        // Generate size between 1B..16MB.
        let size = rng.random_range(1..16 * 1024 * 1024);
        let mut content = vec![0; size];
        rng.fill_bytes(&mut content);
        content
    }

    #[tokio::test]
    async fn test_writer_write() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let path = "test_file";

        let content = gen_random_bytes();
        let mut writer = op.writer(path).await.unwrap();
        writer
            .write(content.clone())
            .await
            .expect("write must succeed");
        writer.close().await.expect("close must succeed");

        let buf = op.read(path).await.expect("read to end mut succeed");

        assert_eq!(buf.to_bytes(), content);
    }

    #[tokio::test]
    async fn test_writer_write_with_chunk_zero() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        let path = "test_file";
        let content = vec![b'x'; 4096];

        let mut writer = op.writer_with(path).chunk(0).await?;
        assert_eq!(writer.ctx.options().chunk(), None);
        writer.write(content.clone()).await?;
        writer.close().await?;
        assert_eq!(op.read(path).await?.to_bytes(), content);

        let content = vec![b'y'; 4096];
        op.write_with(path, content.clone()).chunk(0).await?;
        assert_eq!(op.read(path).await?.to_bytes(), content);
        Ok(())
    }

    /// `copy_from_options` must copy the same bytes that `read_options`
    /// returns for the same options.
    #[tokio::test]
    async fn test_writer_copy_from_options_honors_limit() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789").await?;

        let cases = [
            // A zero limit copies nothing.
            (
                options::ReadOptions {
                    range: (0..6).into(),
                    limit: Some(0),
                    ..Default::default()
                },
                b"".as_slice(),
            ),
            // The limit bounds the supplied range from its start.
            (
                options::ReadOptions {
                    range: (0..6).into(),
                    limit: Some(3),
                    ..Default::default()
                },
                b"012".as_slice(),
            ),
            // A limit larger than the supplied range keeps the whole range.
            (
                options::ReadOptions {
                    range: (0..6).into(),
                    limit: Some(10),
                    ..Default::default()
                },
                b"012345".as_slice(),
            ),
            // The limit applies to an open-ended range.
            (
                options::ReadOptions {
                    range: (4_u64..).into(),
                    limit: Some(3),
                    ..Default::default()
                },
                b"456".as_slice(),
            ),
            // The limit applies to a suffix range.
            (
                options::ReadOptions {
                    range: BytesRange::suffix(4),
                    limit: Some(2),
                    ..Default::default()
                },
                b"67".as_slice(),
            ),
            // A limit accepts EOF after a valid starting byte.
            (
                options::ReadOptions {
                    range: (0..12).into(),
                    limit: Some(20),
                    ..Default::default()
                },
                b"0123456789".as_slice(),
            ),
        ];

        for (idx, (options, expected)) in cases.into_iter().enumerate() {
            assert_eq!(
                op.read_options("source", options.clone()).await?.to_vec(),
                expected,
                "read_options must return the expected bytes for case {idx}"
            );

            let target = format!("target-{idx}");
            let mut writer = op.writer(&target).await?;
            writer.copy_from_options("source", options).await?;
            writer.close().await?;
            assert_eq!(
                op.read(&target).await?.to_vec(),
                expected,
                "copy_from_options must copy the same bytes for case {idx}"
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_copy_from_options_rejects_limit_with_chunk_or_concurrent() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "0123456789").await?;
        let mut writer = op.writer("target").await?;

        for (idx, options) in [
            options::ReadOptions {
                range: (0..6).into(),
                limit: Some(3),
                chunk: Some(2),
                ..Default::default()
            },
            options::ReadOptions {
                range: (0..6).into(),
                limit: Some(3),
                concurrent: 2,
                ..Default::default()
            },
        ]
        .into_iter()
        .enumerate()
        {
            let err = writer
                .copy_from_options("source", options)
                .await
                .expect_err("limit with chunk or concurrent reads must be rejected");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid, "case {idx}");
        }
        writer.abort().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_write_from() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let path = "test_file";

        let content = gen_random_bytes();
        let mut writer = op.writer(path).await.unwrap();
        writer
            .write_from(Bytes::from(content.clone()))
            .await
            .expect("write must succeed");
        writer.close().await.expect("close must succeed");

        let buf = op.read(path).await.expect("read to end mut succeed");

        assert_eq!(buf.to_bytes(), content);
    }

    #[tokio::test]
    async fn test_writer_write_from_chain() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let path = "test_file";

        let part1 = Bytes::from(gen_random_bytes());
        let part2 = Bytes::from(gen_random_bytes());

        let mut chain_same = part1.clone().chain(part2.clone());
        let chain = part1.chain(part2);

        let mut writer = op.writer(path).await.unwrap();
        writer.write_from(chain).await.expect("write must succeed");
        writer.close().await.expect("close must succeed");

        let buf = op.read(path).await.expect("read to end mut succeed");

        assert_eq!(
            buf.to_bytes(),
            chain_same.copy_to_bytes(chain_same.remaining())
        );
    }

    #[tokio::test]
    async fn test_copy_from_same_path_is_rejected() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        let content = "copy source must survive";
        op.write("f", content).await?;

        for source in ["f", "./f", "/f", "//f"] {
            let mut writer = op.writer("f").await?;
            let err = writer
                .copy_from(source, 0..content.len() as u64)
                .await
                .expect_err("copying a file onto itself must fail");
            assert_eq!(err.kind(), ErrorKind::IsSameFile, "source: {source}");
            writer.abort().await?;

            assert_eq!(
                op.read("f").await?.to_vec(),
                content.as_bytes(),
                "source must survive copy from {source}"
            );
        }

        Ok(())
    }

    /// A memory service that resolves a versioned read to a stored snapshot.
    #[derive(Debug)]
    struct VersionedReadService {
        inner: Servicer,
    }

    impl Service for VersionedReadService {
        type Reader = oio::Reader;
        type Writer = oio::Writer;
        type Lister = ();
        type Deleter = ();
        type Copier = ();
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> Capability {
            Capability {
                read_with_version: true,
                ..self.inner.capability()
            }
        }

        async fn create_dir(
            &self,
            _: &OperationContext,
            _: &str,
            _: OpCreateDir,
        ) -> Result<RpCreateDir> {
            unreachable!()
        }

        async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
            unreachable!()
        }

        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            let version = args.version().map(str::to_string);
            match version {
                Some(version) => {
                    let snapshot = format!(".versions/{version}/{path}");
                    self.inner.read(ctx, &snapshot, OpRead::new())
                }
                None => self.inner.read(ctx, path, args),
            }
        }

        fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
            self.inner.write(ctx, path, args)
        }

        fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
            unreachable!()
        }

        fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            unreachable!()
        }

        fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
            unreachable!()
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: OpRename,
        ) -> Result<RpRename> {
            unreachable!()
        }

        async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn test_copy_from_same_path_with_version_is_allowed() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        let (ctx, inner) = op.into_parts();
        let op = Operator::from_parts(ctx, Arc::new(VersionedReadService { inner }));

        let current = "current object content";
        let previous = "previous version content";
        op.write("f", current).await?;
        op.write(".versions/v1/f", previous).await?;

        let mut writer = op.writer("f").await?;
        writer
            .copy_from_options(
                "f",
                options::ReadOptions {
                    range: (0..previous.len() as u64).into(),
                    version: Some("v1".to_string()),
                    ..Default::default()
                },
            )
            .await?;
        writer.close().await?;

        assert_eq!(op.read("f").await?.to_vec(), previous.as_bytes());
        Ok(())
    }

    #[tokio::test]
    async fn test_copy_from_options_rejects_unsupported_read_options() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        op.write("source", "hello world").await?;
        let since = Timestamp::from_second(1_700_000_000).unwrap();

        let unsupported_cases = [
            (
                "version",
                options::ReadOptions {
                    version: Some("v1".to_string()),
                    ..Default::default()
                },
            ),
            (
                "if_match",
                options::ReadOptions {
                    if_match: Some("\"etag\"".to_string()),
                    ..Default::default()
                },
            ),
            (
                "if_none_match",
                options::ReadOptions {
                    if_none_match: Some("\"etag\"".to_string()),
                    ..Default::default()
                },
            ),
            (
                "if_version_match",
                options::ReadOptions {
                    if_version_match: Some("v1".to_string()),
                    ..Default::default()
                },
            ),
            (
                "if_version_not_match",
                options::ReadOptions {
                    if_version_not_match: Some("v1".to_string()),
                    ..Default::default()
                },
            ),
            (
                "if_modified_since",
                options::ReadOptions {
                    if_modified_since: Some(since),
                    ..Default::default()
                },
            ),
            (
                "if_unmodified_since",
                options::ReadOptions {
                    if_unmodified_since: Some(since),
                    ..Default::default()
                },
            ),
        ];

        for (name, read_options) in unsupported_cases {
            let mut writer = op.writer("target").await?;
            let err = writer
                .copy_from_options("source", read_options)
                .await
                .expect_err(name);
            assert_eq!(err.kind(), ErrorKind::Unsupported, "{name}: {err}");
            assert!(err.to_string().contains(name), "{name}: {err}");
            writer.abort().await?;
        }

        // Disabling `read_with_version` via `CapabilityOverrideLayer` rejects
        // versioned `copy_from_options` even when the wrapped service supports it.
        let (ctx, inner) = op.into_parts();
        let versioned = Operator::from_parts(ctx, Arc::new(VersionedReadService { inner })).layer(
            crate::layers::CapabilityOverrideLayer::from_overrides("read_with_version=false")?,
        );
        let mut writer = versioned.writer("target").await?;
        let err = writer
            .copy_from_options(
                "source",
                options::ReadOptions {
                    version: Some("v1".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("overridden read_with_version=false must reject copy_from_options");
        assert_eq!(err.kind(), ErrorKind::Unsupported);
        assert!(err.to_string().contains("version"));
        writer.abort().await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_writer_terminal_state_rejects_writes_across_chunking_and_adapters() -> Result<()>
    {
        use futures::{AsyncWriteExt, SinkExt};

        let op = Operator::new(services::Memory::default())?;

        // 1. Default writer: close() is terminal.
        let mut w = op.writer("default-closed").await?;
        w.write("old").await?;
        w.close().await?;
        let err = w.write("x").await.expect_err("write after close must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        let err = w.close().await.expect_err("second close must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(op.read("default-closed").await?.to_bytes(), "old");

        // 2. Chunked writer: write("x") after close() must fail before buffering.
        let mut w = op.writer_with("chunked-closed").chunk(8).await?;
        w.write("old").await?;
        w.close().await?;
        let err = w
            .write("x")
            .await
            .expect_err("chunked write after close must fail immediately");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        let err = w.close().await.expect_err("second close must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(op.read("chunked-closed").await?.to_bytes(), "old");

        // 3. Chunked writer: write("x") after abort() must fail immediately.
        let mut w = op.writer_with("chunked-aborted").chunk(8).await?;
        w.write("old").await?;
        w.abort().await?;
        let err = w
            .write("x")
            .await
            .expect_err("chunked write after abort must fail immediately");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        // 4. Adapter conversions after close() must reject immediately.
        let mut w = op.writer("closed-async").await?;
        w.write("old").await?;
        w.close().await?;
        let mut async_w = w.into_futures_async_write();
        let err = async_w
            .write_all(b"x")
            .await
            .expect_err("FuturesAsyncWriter::write after close must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(op.read("closed-async").await?.to_bytes(), "old");

        let mut w = op.writer("closed-sink").await?;
        w.write("old").await?;
        w.close().await?;
        let mut sink = w.into_sink();
        let err = sink
            .send(Buffer::from("x"))
            .await
            .expect_err("BufferSink::send after close must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);
        assert_eq!(op.read("closed-sink").await?.to_bytes(), "old");

        let mut w = op.writer("closed-bytes-sink").await?;
        w.write("old").await?;
        w.close().await?;
        let mut bytes_sink = w.into_bytes_sink();
        let err = bytes_sink
            .send(Bytes::from_static(b"x"))
            .await
            .expect_err("FuturesBytesSink::send after close must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);
        assert_eq!(op.read("closed-bytes-sink").await?.to_bytes(), "old");

        // 5. Adapter conversion after abort() must reject immediately.
        let mut w = op.writer("aborted-async").await?;
        w.write("old").await?;
        w.abort().await?;
        let mut async_w = w.into_futures_async_write();
        let err = async_w
            .write_all(b"x")
            .await
            .expect_err("FuturesAsyncWriter::write after abort must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::Other);

        Ok(())
    }

    #[tokio::test]
    async fn test_chunked_writer_temporary_close_failure_remains_retryable() -> Result<()> {
        use std::sync::atomic::{AtomicBool, Ordering};

        #[derive(Debug)]
        struct FailOnceCloseService {
            inner: Servicer,
            fail_close: Arc<AtomicBool>,
        }

        impl Service for FailOnceCloseService {
            type Reader = oio::Reader;
            type Writer = oio::Writer;
            type Lister = ();
            type Deleter = ();
            type Copier = ();
            type Composer = ();

            fn info(&self) -> ServiceInfo {
                self.inner.info()
            }

            fn capability(&self) -> Capability {
                self.inner.capability()
            }

            async fn create_dir(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpCreateDir,
            ) -> Result<RpCreateDir> {
                unreachable!()
            }

            async fn stat(&self, _: &OperationContext, _: &str, _: OpStat) -> Result<RpStat> {
                unreachable!()
            }

            fn read(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpRead,
            ) -> Result<Self::Reader> {
                self.inner.read(ctx, path, args)
            }

            fn write(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpWrite,
            ) -> Result<Self::Writer> {
                let w = self.inner.write(ctx, path, args)?;
                Ok(Box::new(FailOnceCloseWriter {
                    inner: w,
                    fail_close: self.fail_close.clone(),
                }))
            }

            fn delete(&self, _: &OperationContext) -> Result<Self::Deleter> {
                unreachable!()
            }

            fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
                unreachable!()
            }

            fn copy(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpCopy,
            ) -> Result<Self::Copier> {
                unreachable!()
            }

            async fn rename(
                &self,
                _: &OperationContext,
                _: &str,
                _: &str,
                _: OpRename,
            ) -> Result<RpRename> {
                unreachable!()
            }

            async fn presign(
                &self,
                _: &OperationContext,
                _: &str,
                _: OpPresign,
            ) -> Result<RpPresign> {
                unreachable!()
            }
        }

        struct FailOnceCloseWriter {
            inner: oio::Writer,
            fail_close: Arc<AtomicBool>,
        }

        impl oio::Write for FailOnceCloseWriter {
            async fn write(&mut self, bs: Buffer) -> Result<()> {
                self.inner.write(bs).await
            }

            async fn close(&mut self) -> Result<Metadata> {
                if self.fail_close.swap(false, Ordering::SeqCst) {
                    return Err(Error::new(ErrorKind::Unexpected, "temporary close failure")
                        .set_temporary());
                }
                self.inner.close().await
            }

            async fn abort(&mut self) -> Result<()> {
                self.inner.abort().await
            }
        }

        let op = Operator::new(services::Memory::default())?;
        let (ctx, inner) = op.into_parts();
        let op = Operator::from_parts(
            ctx,
            Arc::new(FailOnceCloseService {
                inner,
                fail_close: Arc::new(AtomicBool::new(true)),
            }),
        );

        let mut w = op.writer_with("retry-close").chunk(8).await?;
        w.write("old").await?;

        let err = w.close().await.expect_err("first close must fail");
        assert!(err.to_string().contains("temporary close failure"));

        // Retry after temporary close failure must still commit and then transition to terminal closed.
        w.close().await?;
        assert_eq!(op.read("retry-close").await?.to_bytes(), "old");

        let err = w
            .write("x")
            .await
            .expect_err("write after retried close succeeds must fail");
        assert_eq!(err.kind(), ErrorKind::Unexpected);

        Ok(())
    }
}
