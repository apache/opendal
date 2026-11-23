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

use bytes::Bytes;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::errors::{ParquetError, Result};

use futures::future::BoxFuture;
use opendal::Writer;

/// AsyncWriter implements AsyncFileWriter trait by using opendal.
///
/// A failed [`AsyncFileWriter::write`] or [`AsyncFileWriter::complete`] call
/// aborts the underlying [`Writer`] so that staged files and multipart uploads
/// are released, then returns the original error.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use arrow::array::{ArrayRef, Int64Array, RecordBatch};
///
/// use opendal::{services::S3Config, Operator};
/// use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, AsyncArrowWriter};
/// use parquet_opendal::AsyncWriter;
///
/// #[tokio::main]
/// async fn main() {
///     let mut cfg = S3Config::default();
///     cfg.access_key_id = Some("my_access_key".to_string());
///     cfg.secret_access_key = Some("my_secret_key".to_string());
///     cfg.endpoint = Some("my_endpoint".to_string());
///     cfg.region = Some("my_region".to_string());
///     cfg.bucket = "my_bucket".to_string();
///
///     // Create a new operator
///     let operator = Operator::from_config(cfg).unwrap();
///     let path = "/path/to/file.parquet";
///
///     // Create an async writer
///     let writer = AsyncWriter::new(
///         operator
///             .writer_with(path)
///             .chunk(32 * 1024 * 1024)
///             .concurrent(8)
///             .await
///             .unwrap(),
///     );
///
///     let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
///     let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();
///     let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), None).unwrap();
///     writer.write(&to_write).await.unwrap();
///     writer.close().await.unwrap();
///
///     let buffer = operator.read(path).await.unwrap().to_bytes();
///     let mut reader = ParquetRecordBatchReaderBuilder::try_new(buffer)
///         .unwrap()
///         .build()
///         .unwrap();
///     let read = reader.next().unwrap().unwrap();
///     assert_eq!(to_write, read);
/// }
/// ```
pub struct AsyncWriter {
    inner: Writer,
    /// Whether the underlying [`Writer`] has been aborted.
    ///
    /// Aborting releases staged data and leaves the writer unusable, so later
    /// calls must not reach the underlying writer again.
    aborted: bool,
}

impl AsyncWriter {
    /// Create a [`AsyncWriter`] by given [`Writer`].
    pub fn new(writer: Writer) -> Self {
        Self {
            inner: writer,
            aborted: false,
        }
    }

    /// Aborts the write and cleans up data written by the underlying [`Writer`].
    ///
    /// Failed [`AsyncFileWriter::write`] and [`AsyncFileWriter::complete`] calls
    /// already abort the underlying writer, so calling this method afterwards
    /// returns `Ok(())`.
    pub async fn abort(&mut self) -> opendal::Result<()> {
        if self.aborted {
            return Ok(());
        }

        self.inner.abort().await?;
        self.aborted = true;
        Ok(())
    }

    fn ensure_active(&self) -> Result<()> {
        if self.aborted {
            return Err(ParquetError::External(Box::new(opendal::Error::new(
                opendal::ErrorKind::Unexpected,
                "the underlying writer has been aborted",
            ))));
        }

        Ok(())
    }
}

impl AsyncFileWriter for AsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.ensure_active()?;

            match self.inner.write(bs).await {
                Ok(()) => Ok(()),
                Err(err) => {
                    // AsyncArrowWriter hands over the encoded row group and
                    // drops it on failure, so the file can never be completed.
                    // Release staged data before returning the write error.
                    let _ = self.abort().await;
                    Err(ParquetError::External(Box::new(err)))
                }
            }
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            self.ensure_active()?;

            match self.inner.close().await {
                Ok(_) => Ok(()),
                Err(err) => {
                    // AsyncArrowWriter::close consumes the writer, so this is
                    // the last chance to release staged files or multipart
                    // uploads. Report the close error, not the abort error.
                    let _ = self.abort().await;
                    Err(ParquetError::External(Box::new(err)))
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::Arc;

    use super::*;
    use arrow::array::{ArrayRef, BinaryArray, Int64Array, RecordBatch};
    use opendal::raw::{self, oio};
    use opendal::{
        Buffer, Capability, Error, ErrorKind, Metadata, OperationContext, Operator, services,
    };
    use parquet::arrow::{AsyncArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder};

    /// Builds an operator whose writes are staged and renamed on close, the
    /// layout used by OpenDAL's atomic writers.
    fn staged_fs_operator(root: &Path) -> Operator {
        let staging = root.join("staging");
        std::fs::create_dir_all(&staging).unwrap();

        Operator::new(
            services::Fs::default()
                .root(root.to_str().unwrap())
                .atomic_write_dir(staging.to_str().unwrap()),
        )
        .unwrap()
    }

    fn staged_files(root: &Path) -> Vec<String> {
        std::fs::read_dir(root.join("staging"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect()
    }

    fn test_batch() -> RecordBatch {
        let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
        RecordBatch::try_from_iter([("col", col)]).unwrap()
    }

    /// A batch large enough that flushing a row group writes through parquet's
    /// internal buffer instead of only buffering it for the final close.
    fn large_batch() -> RecordBatch {
        let bytes = vec![b'x'; 64 * 1024];
        let col = Arc::new(BinaryArray::from(vec![bytes.as_slice()])) as ArrayRef;
        RecordBatch::try_from_iter([("col", col)]).unwrap()
    }

    #[tokio::test]
    async fn test_basic() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let path = "data/test.txt";
        let mut writer = AsyncWriter::new(op.writer(path).await.unwrap());
        let bytes = Bytes::from_static(b"hello, world!");
        writer.write(bytes).await.unwrap();
        let bytes = Bytes::from_static(b"hello, OpenDAL!");
        writer.write(bytes).await.unwrap();
        writer.complete().await.unwrap();

        let bytes = op.read(path).await.unwrap().to_vec();
        assert_eq!(bytes, b"hello, world!hello, OpenDAL!");
    }

    #[tokio::test]
    async fn test_abort() {
        let op = Operator::new(services::Memory::default()).unwrap();
        let path = "data/test.txt";
        let mut writer = AsyncWriter::new(op.writer(path).await.unwrap());
        let bytes = Bytes::from_static(b"hello, world!");
        writer.write(bytes).await.unwrap();
        let bytes = Bytes::from_static(b"hello, OpenDAL!");
        writer.write(bytes).await.unwrap();
        writer.abort().await.unwrap();

        let exist = op.exists(path).await.unwrap();
        assert!(!exist);
    }

    #[tokio::test]
    async fn test_async_writer() {
        let operator = Operator::new(services::Memory::default()).unwrap();
        let path = "/path/to/file.parquet";

        let writer = AsyncWriter::new(
            operator
                .writer_with(path)
                .chunk(32 * 1024 * 1024)
                .concurrent(8)
                .await
                .unwrap(),
        );

        let to_write = test_batch();
        let mut writer = AsyncArrowWriter::try_new(writer, to_write.schema(), None).unwrap();
        writer.write(&to_write).await.unwrap();
        writer.close().await.unwrap();

        let buffer = operator.read(path).await.unwrap().to_bytes();
        let mut reader = ParquetRecordBatchReaderBuilder::try_new(buffer)
            .unwrap()
            .build()
            .unwrap();
        let read = reader.next().unwrap().unwrap();
        assert_eq!(to_write, read);
    }

    /// `AsyncArrowWriter::close` consumes the writer, so a failed commit must
    /// release the staged file before the error is returned.
    #[tokio::test]
    async fn test_complete_failure_cleans_staged_file() {
        let root = tempfile::tempdir().unwrap();
        let op = staged_fs_operator(root.path());
        // Renaming the staged file over an existing directory fails.
        std::fs::create_dir_all(root.path().join("data/blocked.parquet")).unwrap();

        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(op.writer("data/blocked.parquet").await.unwrap()),
            large_batch().schema(),
            None,
        )
        .unwrap();
        writer.write(&large_batch()).await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!(staged_files(root.path()).len(), 1);

        writer
            .close()
            .await
            .expect_err("committing over an existing directory must fail");
        assert!(
            staged_files(root.path()).is_empty(),
            "staged file left behind: {:?}",
            staged_files(root.path())
        );
    }

    /// `AsyncArrowWriter` drops the row group it passed to `write` and cannot
    /// retry, so a failed write must clean up the staged file immediately.
    #[tokio::test]
    async fn test_write_failure_cleans_staged_file() {
        let root = tempfile::tempdir().unwrap();
        let op = staged_fs_operator(root.path()).layer(FailingWriteLayer);

        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(op.writer("data/test.parquet").await.unwrap()),
            large_batch().schema(),
            None,
        )
        .unwrap();
        // The first flush forwards data and creates the staged file.
        writer.write(&large_batch()).await.unwrap();
        writer.flush().await.unwrap();
        assert_eq!(staged_files(root.path()).len(), 1);

        writer.write(&large_batch()).await.unwrap();
        let err = writer
            .flush()
            .await
            .expect_err("injected write failure must surface");
        assert!(err.to_string().contains("write error injected by the test"));
        assert!(
            staged_files(root.path()).is_empty(),
            "staged file left behind: {:?}",
            staged_files(root.path())
        );

        // The failed write already released the staging area, so an explicit
        // abort must not fail on the cleaned up file.
        writer
            .into_inner()
            .abort()
            .await
            .expect("abort after automatic cleanup must succeed");
    }

    fn unsupported<T>() -> opendal::Result<T> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not used by AsyncWriter tests",
        ))
    }

    /// Forwards the first write so the wrapped writer has staged data, then
    /// fails later writes. The remaining operations, including `abort`, are
    /// forwarded unchanged.
    #[derive(Debug)]
    struct FailingWriteLayer;

    impl raw::Layer for FailingWriteLayer {
        fn apply_service(&self, inner: raw::Servicer) -> raw::Servicer {
            Arc::new(FailingWriteService { inner })
        }
    }

    #[derive(Debug)]
    struct FailingWriteService {
        inner: raw::Servicer,
    }

    impl raw::Service for FailingWriteService {
        type Reader = ();
        type Writer = oio::Writer;
        type Lister = ();
        type Deleter = ();
        type Copier = ();
        type Composer = ();

        fn info(&self) -> raw::ServiceInfo {
            self.inner.info()
        }

        fn capability(&self) -> Capability {
            self.inner.capability()
        }

        async fn create_dir(
            &self,
            _: &OperationContext,
            _: &str,
            _: raw::OpCreateDir,
        ) -> opendal::Result<raw::RpCreateDir> {
            unsupported()
        }

        async fn stat(
            &self,
            _: &OperationContext,
            _: &str,
            _: raw::OpStat,
        ) -> opendal::Result<raw::RpStat> {
            unsupported()
        }

        fn read(
            &self,
            _: &OperationContext,
            _: &str,
            _: raw::OpRead,
        ) -> opendal::Result<Self::Reader> {
            unsupported()
        }

        fn write(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: raw::OpWrite,
        ) -> opendal::Result<Self::Writer> {
            let inner = self.inner.write(ctx, path, args)?;
            Ok(Box::new(FailingWriteWriter {
                inner,
                forwarded: false,
            }))
        }

        fn delete(&self, _: &OperationContext) -> opendal::Result<Self::Deleter> {
            unsupported()
        }

        fn list(
            &self,
            _: &OperationContext,
            _: &str,
            _: raw::OpList,
        ) -> opendal::Result<Self::Lister> {
            unsupported()
        }

        fn copy(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: raw::OpCopy,
        ) -> opendal::Result<Self::Copier> {
            unsupported()
        }

        async fn rename(
            &self,
            _: &OperationContext,
            _: &str,
            _: &str,
            _: raw::OpRename,
        ) -> opendal::Result<raw::RpRename> {
            unsupported()
        }

        async fn presign(
            &self,
            _: &OperationContext,
            _: &str,
            _: raw::OpPresign,
        ) -> opendal::Result<raw::RpPresign> {
            unsupported()
        }
    }

    struct FailingWriteWriter {
        inner: oio::Writer,
        forwarded: bool,
    }

    impl oio::Write for FailingWriteWriter {
        async fn write(&mut self, bs: Buffer) -> opendal::Result<()> {
            if !self.forwarded {
                self.forwarded = true;
                return oio::Write::write(&mut self.inner, bs).await;
            }

            Err(
                Error::new(ErrorKind::Unexpected, "write error injected by the test")
                    .set_permanent(),
            )
        }

        async fn close(&mut self) -> opendal::Result<Metadata> {
            oio::Write::close(&mut self.inner).await
        }

        async fn abort(&mut self) -> opendal::Result<()> {
            oio::Write::abort(&mut self.inner).await
        }
    }
}
