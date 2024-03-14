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
use std::time::Duration;

use bytes::Bytes;
use futures::stream;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use super::BlockingOperator;
use crate::operator_futures::*;
use crate::raw::*;
use crate::*;

/// Operator is the entry for all public async APIs.
///
/// Developer should manipulate the data from storage service through Operator only by right.
///
/// We will usually do some general checks and data transformations in this layer,
/// like normalizing path from input, checking whether the path refers to one file or one directory,
/// and so on.
/// Read [`Operator::concepts`][docs::concepts] for more about [`Operator::Operator`].
///
/// # Examples
///
/// Read more backend init examples in [`Operator::services`]
///
/// ```
/// # use anyhow::Result;
/// use opendal::services::Fs;
/// use opendal::Operator;
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Create fs backend builder.
///     let mut builder = Fs::default();
///     // Set the root for fs, all operations will happen under this root.
///     //
///     // NOTE: the root must be absolute path.
///     builder.root("/tmp");
///
///     // Build an `Operator` to start operating the storage.
///     let _: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct Operator {
    // accessor is what Operator delegates for
    accessor: FusedAccessor,

    // limit is usually the maximum size of data that operator will handle in one operation
    limit: usize,
}

/// # Operator basic API.
impl Operator {
    pub(super) fn inner(&self) -> &FusedAccessor {
        &self.accessor
    }

    pub(crate) fn from_inner(accessor: FusedAccessor) -> Self {
        let limit = accessor
            .info()
            .full_capability()
            .batch_max_operations
            .unwrap_or(1000);
        Self { accessor, limit }
    }

    pub(super) fn into_inner(self) -> FusedAccessor {
        self.accessor
    }

    /// Get current operator's limit.
    /// Limit is usually the maximum size of data that operator will handle in one operation.
    pub fn limit(&self) -> usize {
        self.limit
    }

    /// Specify the batch limit.
    ///
    /// Default: 1000
    pub fn with_limit(&self, limit: usize) -> Self {
        let mut op = self.clone();
        op.limit = limit;
        op
    }

    /// Get information of underlying accessor.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let info = op.info();
    /// # Ok(())
    /// # }
    /// ```
    pub fn info(&self) -> OperatorInfo {
        OperatorInfo::new(self.accessor.info())
    }

    /// Create a new blocking operator.
    ///
    /// This operation is nearly no cost.
    pub fn blocking(&self) -> BlockingOperator {
        BlockingOperator::from_inner(self.accessor.clone()).with_limit(self.limit)
    }
}

/// # Operator async API.
impl Operator {
    /// Check if this operator can work correctly.
    ///
    /// We will send a `list` request to path and return any errors we met.
    ///
    /// ```
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.check().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check(&self) -> Result<()> {
        let mut ds = self.lister("/").await?;

        match ds.next().await {
            Some(Err(e)) if e.kind() != ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Get given path's metadata.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// [`Operator::stat`] is a wrapper of [`Operator::stat_with`] without any options. To use extra
    /// options like `if_match` and `if_none_match`, please use [`Operator::stat_with`] instead.
    ///
    /// ## Reuse Metadata
    ///
    /// For fetch metadata of entries returned by [`Operator::Lister`], it's better to use
    /// [`Operator::list_with`] and [`Operator::lister_with`] with `metakey` query like
    /// `Metakey::ContentLength | Metakey::LastModified` so that we can avoid extra stat requests.
    ///
    /// # Examples
    ///
    /// ## Check if file exists
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// use opendal::ErrorKind;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// if let Err(e) = op.stat("test").await {
    ///     if e.kind() == ErrorKind::NotFound {
    ///         println!("file not exist")
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stat(&self, path: &str) -> Result<Metadata> {
        self.stat_with(path).await
    }

    /// Get given path's metadata with extra options.
    ///
    /// # Notes
    ///
    /// ## Reuse Metadata
    ///
    /// For fetch metadata of entries returned by [`Operator::Lister`], it's better to use
    /// [`Operator::list_with`] and [`Operator::lister_with`] with `metakey` query like
    /// `Metakey::ContentLength | Metakey::LastModified` so that we can avoid extra requests.
    ///
    /// # Options
    ///
    /// ## `if_match`
    ///
    /// Set `if_match` for this `stat` request.
    ///
    /// This feature can be used to check if the file's `ETag` matches the given `ETag`.
    ///
    /// If file exists and it's etag doesn't match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let mut metadata = op.stat_with("path/to/file").if_match(etag).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `if_none_match`
    ///
    /// Set `if_none_match` for this `stat` request.
    ///
    /// This feature can be used to check if the file's `ETag` doesn't match the given `ETag`.
    ///
    /// If file exists and it's etag match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let mut metadata = op.stat_with("path/to/file").if_none_match(etag).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Examples
    ///
    /// ## Get metadata while `ETag` matches
    ///
    /// `stat_with` will
    ///
    /// - return `Ok(metadata)` if `ETag` matches
    /// - return `Err(error)` and `error.kind() == ErrorKind::ConditionNotMatch` if file exists but
    ///   `ETag` mismatch
    /// - return `Err(err)` if other errors occur, for example, `NotFound`.
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// use opendal::ErrorKind;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// if let Err(e) = op.stat_with("test").if_match("<etag>").await {
    ///     if e.kind() == ErrorKind::ConditionNotMatch {
    ///         println!("file exists, but etag mismatch")
    ///     }
    ///     if e.kind() == ErrorKind::NotFound {
    ///         println!("file not exist")
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ---
    ///
    /// # Behavior
    ///
    /// ## Services that support `create_dir`
    ///
    /// `test` and `test/` may vary in some services such as S3. However, on a local file system,
    /// they're identical. Therefore, the behavior of `stat("test")` and `stat("test/")` might differ
    /// in certain edge cases. Always use `stat("test/")` when you need to access a directory if possible.
    ///
    /// Here are the behavior list:
    ///
    /// | Case                   | Path            | Result                                     |
    /// |------------------------|-----------------|--------------------------------------------|
    /// | stat existing dir      | `abc/`          | Metadata with dir mode                     |
    /// | stat existing file     | `abc/def_file`  | Metadata with file mode                    |
    /// | stat dir without `/`   | `abc/def_dir`   | Error `NotFound` or metadata with dir mode |
    /// | stat file with `/`     | `abc/def_file/` | Error `NotFound`                           |
    /// | stat not existing path | `xyz`           | Error `NotFound`                           |
    ///
    /// Refer to [RFC: List Prefix][crate::docs::rfcs::rfc_3243_list_prefix] for more details.
    ///
    /// ## Services that not support `create_dir`
    ///
    /// For services that not support `create_dir`, `stat("test/")` will return `NotFound` even
    /// when `test/abc` exists since the service won't have the concept of dir. There is nothing
    /// we can do about this.
    pub fn stat_with(&self, path: &str) -> FutureStat<impl Future<Output = Result<Metadata>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            OpStat::default(),
            |inner, path, args| async move {
                let rp = inner.stat(&path, args).await?;
                Ok(rp.into_metadata())
            },
        )
    }

    /// Check if this path exists or not.
    ///
    /// # Example
    ///
    /// ```
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    ///
    /// #[tokio::main]
    /// async fn test(op: Operator) -> Result<()> {
    ///     let _ = op.is_exist("test").await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn is_exist(&self, path: &str) -> Result<bool> {
        let r = self.stat(path).await;
        match r {
            Ok(_) => Ok(true),
            Err(err) => match err.kind() {
                ErrorKind::NotFound => Ok(false),
                _ => Err(err),
            },
        }
    }

    /// Create a dir at given path.
    ///
    /// # Notes
    ///
    /// To indicate that a path is a directory, it is compulsory to include
    /// a trailing / in the path. Failure to do so may result in
    /// `NotADirectory` error being returned by OpenDAL.
    ///
    /// # Behavior
    ///
    /// - Create on existing dir will succeed.
    /// - Create dir is always recursive, works like `mkdir -p`
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.create_dir("path/to/dir/").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_dir(&self, path: &str) -> Result<()> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::DIR) {
            return Err(Error::new(
                ErrorKind::NotADirectory,
                "the path trying to create should end with `/`",
            )
            .with_operation("create_dir")
            .with_context("service", self.inner().info().scheme())
            .with_context("path", &path));
        }

        self.inner().create_dir(&path, OpCreateDir::new()).await?;

        Ok(())
    }

    /// Read the whole path into a bytes.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// [`Operator::read`] is a wrapper of [`Operator::read_with`] without any options. To use
    /// extra options like `range` and `if_match`, please use [`Operator::read_with`] instead.
    ///
    /// ## Streaming Read
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Operator::reader`]
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op.read("path/to/file").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(&self, path: &str) -> Result<Vec<u8>> {
        self.read_with(path).await
    }

    /// Read the whole path into a bytes with extra options.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Operator::reader`]
    ///
    /// # Notes
    ///
    /// ## Streaming Read
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Operator::reader`]
    ///
    /// # Options
    ///
    /// ## `range`
    ///
    /// Set `range` for this `read` request.
    ///
    /// If we have a file with size `n`.
    ///
    /// - `..` means read bytes in range `[0, n)` of file.
    /// - `0..1024` means read bytes in range `[0, 1024)` of file
    /// - `1024..` means read bytes in range `[1024, n)` of file
    /// - `..1024` means read bytes in range `(n - 1024, n)` of file
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op.read_with("path/to/file").range(0..1024).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `if_match`
    ///
    /// Set `if_match` for this `read` request.
    ///
    /// This feature can be used to check if the file's `ETag` matches the given `ETag`.
    ///
    /// If file exists and it's etag doesn't match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let mut metadata = op.read_with("path/to/file").if_match(etag).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `if_none_match`
    ///
    /// Set `if_none_match` for this `read` request.
    ///
    /// This feature can be used to check if the file's `ETag` doesn't match the given `ETag`.
    ///
    /// If file exists and it's etag match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let mut metadata = op.read_with("path/to/file").if_none_match(etag).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Examples
    ///
    /// Read the whole path into a bytes.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op.read_with("path/to/file").await?;
    /// let bs = op.read_with("path/to/file").range(0..10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_with(&self, path: &str) -> FutureRead<impl Future<Output = Result<Vec<u8>>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            OpRead::default(),
            |inner, path, args| async move {
                if !validate_path(&path, EntryMode::FILE) {
                    return Err(
                        Error::new(ErrorKind::IsADirectory, "read path is a directory")
                            .with_operation("read")
                            .with_context("service", inner.info().scheme())
                            .with_context("path", &path),
                    );
                }

                let range = args.range();
                let (size_hint, range) = if let Some(size) = range.size() {
                    (size, range)
                } else {
                    let size = inner
                        .stat(&path, OpStat::default())
                        .await?
                        .into_metadata()
                        .content_length();
                    let range = range.complete(size);
                    (range.size().unwrap(), range)
                };

                let (_, r) = inner.read(&path, args.with_range(range)).await?;
                let mut r = Reader::new(r);
                let mut buf = Vec::with_capacity(size_hint as usize);
                r.read_to_end(&mut buf).await?;

                Ok(buf)
            },
        )
    }

    /// Create a new reader which can read the whole path.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// [`Operator::reader`] is a wrapper of [`Operator::reader_with`] without any options. To use
    /// extra options like `range` and `if_match`, please use [`Operator::reader_with`] instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # use opendal::Scheme;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op.reader("path/to/file").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn reader(&self, path: &str) -> Result<Reader> {
        self.reader_with(path).await
    }

    /// Create a new reader with extra options
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// [`Operator::reader`] is a wrapper of [`Operator::reader_with`] without any options. To use
    /// extra options like `range` and `if_match`, please use [`Operator::reader_with`] instead.
    ///
    /// # Options
    ///
    /// ## `range`
    ///
    /// Set `range` for this `read` request.
    ///
    /// If we have a file with size `n`.
    ///
    /// - `..` means read bytes in range `[0, n)` of file.
    /// - `0..1024` means read bytes in range `[0, 1024)` of file
    /// - `1024..` means read bytes in range `[1024, n)` of file
    /// - `..1024` means read bytes in range `(n - 1024, n)` of file
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op.reader_with("path/to/file").range(0..1024).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `buffer`
    ///
    /// Set `buffer` for the reader.
    ///
    /// OpenDAL by default to read file without buffer. This is not efficient for cases like `seek`
    /// after read or reading file with small chunks. To improve performance, we can set a buffer.
    ///
    /// The following example will create a reader with 4 MiB buffer internally. All seek operations
    /// happened in buffered data will be zero cost.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op
    ///     .reader_with("path/to/file")
    ///     .buffer(4 * 1024 * 1024)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `if_match`
    ///
    /// Set `if_match` for this `read` request.
    ///
    /// This feature can be used to check if the file's `ETag` matches the given `ETag`.
    ///
    /// If file exists and it's etag doesn't match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let mut metadata = op.reader_with("path/to/file").if_match(etag).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `if_none_match`
    ///
    /// Set `if_none_match` for this `read` request.
    ///
    /// This feature can be used to check if the file's `ETag` doesn't match the given `ETag`.
    ///
    /// If file exists and it's etag match, an error with kind [`ErrorKind::ConditionNotMatch`]
    /// will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let mut metadata = op.reader_with("path/to/file").if_none_match(etag).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # use opendal::Scheme;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op.reader_with("path/to/file").range(0..10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reader_with(&self, path: &str) -> FutureRead<impl Future<Output = Result<Reader>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            OpRead::default(),
            |inner, path, args| async move {
                if !validate_path(&path, EntryMode::FILE) {
                    return Err(
                        Error::new(ErrorKind::IsADirectory, "read path is a directory")
                            .with_operation("Operator::reader")
                            .with_context("service", inner.info().scheme())
                            .with_context("path", path),
                    );
                }

                Reader::create(inner.clone(), &path, args).await
            },
        )
    }

    /// Write bytes into path.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// [`Operator::write`] is a wrapper of [`Operator::write_with`] without any options. To use
    /// extra options like `content_type` and `cache_control`, please use [`Operator::write_with`]
    /// instead.
    ///
    /// ## Streaming Write
    ///
    /// This function will write all bytes at once. For more precise memory control or
    /// writing data continuously, please use [`Operator::writer`].
    ///
    /// ## Multipart Uploads Write
    ///
    /// OpenDAL abstracts the multipart uploads into [`Writer`]. It will automatically
    /// handle the multipart uploads for you. You can control the behavior of multipart uploads
    /// by setting `buffer`, `concurrent` via [`Operator::writer_with`]
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.write("path/to/file", vec![0; 4096]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(&self, path: &str, bs: impl Into<Bytes>) -> Result<()> {
        let bs = bs.into();
        self.write_with(path, bs).await
    }

    /// Copy a file from `from` to `to`.
    ///
    /// # Notes
    ///
    /// - `from` and `to` must be a file.
    /// - `to` will be overwritten if it exists.
    /// - If `from` and `to` are the same,  an `IsSameFile` error will occur.
    /// - `copy` is idempotent. For same `from` and `to` input, the result will be the same.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.copy("path/to/file", "path/to/file2").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn copy(&self, from: &str, to: &str) -> Result<()> {
        let from = normalize_path(from);

        if !validate_path(&from, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "from path is a directory")
                    .with_operation("Operator::copy")
                    .with_context("service", self.info().scheme())
                    .with_context("from", from),
            );
        }

        let to = normalize_path(to);

        if !validate_path(&to, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "to path is a directory")
                    .with_operation("Operator::copy")
                    .with_context("service", self.info().scheme())
                    .with_context("to", to),
            );
        }

        if from == to {
            return Err(
                Error::new(ErrorKind::IsSameFile, "from and to paths are same")
                    .with_operation("Operator::copy")
                    .with_context("service", self.info().scheme())
                    .with_context("from", from)
                    .with_context("to", to),
            );
        }

        self.inner().copy(&from, &to, OpCopy::new()).await?;

        Ok(())
    }

    /// Rename a file from `from` to `to`.
    ///
    /// # Notes
    ///
    /// - `from` and `to` must be a file.
    /// - `to` will be overwritten if it exists.
    /// - If `from` and `to` are the same, an `IsSameFile` error will occur.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.rename("path/to/file", "path/to/file2").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn rename(&self, from: &str, to: &str) -> Result<()> {
        let from = normalize_path(from);

        if !validate_path(&from, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "from path is a directory")
                    .with_operation("Operator::move_")
                    .with_context("service", self.info().scheme())
                    .with_context("from", from),
            );
        }

        let to = normalize_path(to);

        if !validate_path(&to, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "to path is a directory")
                    .with_operation("Operator::move_")
                    .with_context("service", self.info().scheme())
                    .with_context("to", to),
            );
        }

        if from == to {
            return Err(
                Error::new(ErrorKind::IsSameFile, "from and to paths are same")
                    .with_operation("Operator::move_")
                    .with_context("service", self.info().scheme())
                    .with_context("from", from)
                    .with_context("to", to),
            );
        }

        self.inner().rename(&from, &to, OpRename::new()).await?;

        Ok(())
    }

    /// Write multiple bytes into path.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// [`Operator::write`] is a wrapper of [`Operator::write_with`] without any options. To use
    /// extra options like `content_type` and `cache_control`, please use [`Operator::write_with`]
    /// instead.
    ///
    /// ## Buffer
    ///
    /// OpenDAL is designed to write files directly without buffering by default, giving users
    /// control over the exact size of their writes and helping avoid unnecessary costs.
    ///
    /// This is not efficient for cases when users write small chunks of data. Some storage services
    /// like `s3` could even return hard errors like `EntityTooSmall`. Besides, cloud storage services
    /// will cost more money if we write data in small chunks.
    ///
    /// Users can use [`Operator::write_with`] to set a good buffer size might improve the performance,
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op.writer("path/to/file").await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn writer(&self, path: &str) -> Result<Writer> {
        self.writer_with(path).await
    }

    /// Write multiple bytes into path with extra options.
    ///
    /// # Options
    ///
    /// ## `append`
    ///
    /// Set `append` for this `write` request.
    ///
    /// `write` by default to overwrite existing files. To append to the end of file instead,
    /// please set `append` to true.
    ///
    /// The following example will append data to existing file instead.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op.writer_with("path/to/file").append(true).await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `buffer`
    ///
    /// Set `buffer` for the writer.
    ///
    /// OpenDAL is designed to write files directly without buffering by default, giving users
    /// control over the exact size of their writes and helping avoid unnecessary costs.
    ///
    /// This is not efficient for cases when users write small chunks of data. Some storage services
    /// like `s3` could even return hard errors like `EntityTooSmall`. Besides, cloud storage services
    /// will cost more money if we write data in small chunks.
    ///
    /// Set a good buffer size might improve the performance, reduce the API calls and save money.
    ///
    /// The following example will set the writer buffer to 8MiB. Only one API call will be sent at
    /// `close` instead.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op
    ///     .writer_with("path/to/file")
    ///     .buffer(8 * 1024 * 1024)
    ///     .await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `concurrent`
    ///
    /// Set `concurrent` for the writer.
    ///
    /// OpenDAL by default to write file without concurrent. This is not efficient for cases when users
    /// write large chunks of data. By setting `concurrent`, opendal will writing files concurrently
    /// on support storage services.
    ///
    /// The following example will set the writer concurrent to 8.
    ///
    /// - The first write will start and return immediately.
    /// - The second write will start and return immediately.
    /// - The close will make sure all writes are done in order and return result.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op.writer_with("path/to/file").concurrent(8).await?;
    /// w.write(vec![0; 4096]).await?; // Start the first write
    /// w.write(vec![1; 4096]).await?; // Second write will be concurrent without wait
    /// w.close().await?; // Close will make sure all writes are done and success
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `cache_control`
    ///
    /// Set the `cache_control` for this `write` request.
    ///
    /// Some storage services support setting `cache_control` as system metadata.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op
    ///     .writer_with("path/to/file")
    ///     .cache_control("max-age=604800")
    ///     .await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `content_type`
    ///
    /// Set the `content_type` for this `write` request.
    ///
    /// Some storage services support setting `content_type` as system metadata.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op
    ///     .writer_with("path/to/file")
    ///     .content_type("text/plain")
    ///     .await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `content_disposition`
    ///
    /// Set the `content_disposition` for this `write` request.
    ///
    /// Some storage services support setting `content_disposition` as system metadata.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op
    ///     .writer_with("path/to/file")
    ///     .content_disposition("attachment; filename=\"filename.jpg\"")
    ///     .await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op
    ///     .writer_with("path/to/file")
    ///     .content_type("application/octet-stream")
    ///     .await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn writer_with(&self, path: &str) -> FutureWriter<impl Future<Output = Result<Writer>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            OpWrite::default(),
            |inner, path, args| async move {
                if !validate_path(&path, EntryMode::FILE) {
                    return Err(
                        Error::new(ErrorKind::IsADirectory, "write path is a directory")
                            .with_operation("Operator::writer")
                            .with_context("service", inner.info().scheme().into_static())
                            .with_context("path", &path),
                    );
                }

                Writer::create(inner, &path, args).await
            },
        )
    }

    /// Write data with extra options.
    ///
    /// # Notes
    ///
    /// ## Streaming Write
    ///
    /// This function will write all bytes at once. For more precise memory control or
    /// writing data lazily, please use [`Operator::writer_with`].
    ///
    /// ## Multipart Uploads Write
    ///
    /// OpenDAL abstracts the multipart uploads into [`Writer`]. It will automatically
    /// handle the multipart uploads for you. You can control the behavior of multipart uploads
    /// by setting `buffer`, `concurrent` via [`Operator::writer_with`]
    ///
    /// # Options
    ///
    /// ## `append`
    ///
    /// Set `append` for this `write` request.
    ///
    /// `write` by default to overwrite existing files. To append to the end of file instead,
    /// please set `append` to true.
    ///
    /// The following example will append data to existing file instead.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let _ = op.write_with("path/to/file", bs).append(true).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `cache_control`
    ///
    /// Set the `cache_control` for this `write` request.
    ///
    /// Some storage services support setting `cache_control` as system metadata.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let _ = op
    ///     .write_with("path/to/file", bs)
    ///     .cache_control("max-age=604800")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `content_type`
    ///
    /// Set the `content_type` for this `write` request.
    ///
    /// Some storage services support setting `content_type` as system metadata.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let _ = op
    ///     .write_with("path/to/file", bs)
    ///     .content_type("text/plain")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `content_disposition`
    ///
    /// Set the `content_disposition` for this `write` request.
    ///
    /// Some storage services support setting `content_disposition` as system metadata.
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let _ = op
    ///     .write_with("path/to/file", bs)
    ///     .content_disposition("attachment; filename=\"filename.jpg\"")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let _ = op
    ///     .write_with("path/to/file", bs)
    ///     .content_type("text/plain")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn write_with(
        &self,
        path: &str,
        bs: impl Into<Bytes>,
    ) -> FutureWrite<impl Future<Output = Result<()>>> {
        let path = normalize_path(path);
        let bs = bs.into();

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (OpWrite::default(), bs),
            |inner, path, (args, bs)| async move {
                if !validate_path(&path, EntryMode::FILE) {
                    return Err(
                        Error::new(ErrorKind::IsADirectory, "write path is a directory")
                            .with_operation("Operator::write_with")
                            .with_context("service", inner.info().scheme().into_static())
                            .with_context("path", &path),
                    );
                }

                let (_, w) = inner.write(&path, args).await?;
                let mut w = Writer::new(w);
                w.write(bs.clone()).await?;
                w.close().await?;

                Ok(())
            },
        )
    }

    /// Delete the given path.
    ///
    /// # Notes
    ///
    /// - Deleting a file that does not exist won't return errors.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.delete("test").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self, path: &str) -> Result<()> {
        self.delete_with(path).await
    }

    /// Delete the given path with extra options.
    ///
    /// # Notes
    ///
    /// - Deleting a file that does not exist won't return errors.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.delete_with("test").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_with(&self, path: &str) -> FutureDelete<impl Future<Output = Result<()>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            OpDelete::default(),
            |inner, path, args| async move {
                let _ = inner.delete(&path, args).await?;
                Ok(())
            },
        )
    }

    ///
    /// # Notes
    ///
    /// If underlying services support delete in batch, we will use batch
    /// delete instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.remove(vec!["abc".to_string(), "def".to_string()])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove(&self, paths: Vec<String>) -> Result<()> {
        self.remove_via(stream::iter(paths)).await
    }

    /// remove will remove files via the given paths.
    ///
    /// remove_via will remove files via the given stream.
    ///
    /// We will delete by chunks with given batch limit on the stream.
    ///
    /// # Notes
    ///
    /// If underlying services support delete in batch, we will use batch
    /// delete instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// use futures::stream;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let stream = stream::iter(vec!["abc".to_string(), "def".to_string()]);
    /// op.remove_via(stream).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_via(&self, input: impl Stream<Item = String> + Unpin) -> Result<()> {
        let input = input.map(|v| normalize_path(&v));

        if self.info().full_capability().batch {
            let mut input = input
                .map(|v| (v, OpDelete::default().into()))
                .chunks(self.limit());

            while let Some(batches) = input.next().await {
                let results = self
                    .inner()
                    .batch(OpBatch::new(batches))
                    .await?
                    .into_results();

                // TODO: return error here directly seems not a good idea?
                for (_, result) in results {
                    let _ = result?;
                }
            }
        } else {
            input
                .map(Ok)
                .try_for_each_concurrent(self.limit, |path| async move {
                    let _ = self.inner().delete(&path, OpDelete::default()).await?;
                    Ok::<(), Error>(())
                })
                .await?;
        }

        Ok(())
    }

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Notes
    ///
    /// If underlying services support delete in batch, we will use batch
    /// delete instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.remove_all("path/to/dir").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_all(&self, path: &str) -> Result<()> {
        let meta = match self.stat(path).await {
            // If object exists.
            Ok(metadata) => metadata,

            // If object not found, return success.
            Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),

            // Pass on any other error.
            Err(e) => return Err(e),
        };

        if meta.mode() != EntryMode::DIR {
            return self.delete(path).await;
        }

        let obs = self.lister_with(path).recursive(true).await?;

        if self.info().full_capability().batch {
            let mut obs = obs.try_chunks(self.limit());

            while let Some(batches) = obs.next().await {
                let batches = batches
                    .map_err(|err| err.1)?
                    .into_iter()
                    .map(|v| (v.path().to_string(), OpDelete::default().into()))
                    .collect();

                let results = self
                    .inner()
                    .batch(OpBatch::new(batches))
                    .await?
                    .into_results();

                // TODO: return error here directly seems not a good idea?
                for (_, result) in results {
                    let _ = result?;
                }
            }
        } else {
            obs.try_for_each(|v| async move { self.delete(v.path()).await })
                .await?;
        }

        // Remove the directory itself.
        self.delete(path).await?;

        Ok(())
    }

    /// List entries that starts with given `path` in parent dir.
    ///
    /// # Notes
    ///
    /// ## Recursively List
    ///
    /// This function only read the children of the given directory. To read
    /// all entries recursively, use `Operator::list_with("path").recursive(true)`
    /// instead.
    ///
    /// ## Streaming List
    ///
    /// This function will read all entries in the given directory. It could
    /// take very long time and consume a lot of memory if the directory
    /// contains a lot of entries.
    ///
    /// In order to avoid this, you can use [`Operator::lister`] to list entries in
    /// a streaming way.
    ///
    /// ## Reuse Metadata
    ///
    /// The only metadata that is guaranteed to be available is the `Mode`.
    /// For fetching more metadata, please use [`Operator::list_with`] and `metakey`.
    ///
    /// # Examples
    ///
    /// ## List entries under a dir
    ///
    /// This example will list all entries under the dir `path/to/dir/`.
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use opendal::EntryMode;
    /// use opendal::Metakey;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op.list("path/to/dir/").await?;
    /// for entry in entries {
    ///     match entry.metadata().mode() {
    ///         EntryMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir {}", entry.path())
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## List entries with prefix
    ///
    /// This example will list all entries under the prefix `path/to/prefix`.
    ///
    /// NOTE: it's possible that the prefix itself is also a dir. In this case, you could get
    /// `path/to/prefix/`, `path/to/prefix_1` and so on. If you do want to list a dir, please
    /// make sure the path is end with `/`.
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use opendal::EntryMode;
    /// use opendal::Metakey;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op.list("path/to/prefix").await?;
    /// for entry in entries {
    ///     match entry.metadata().mode() {
    ///         EntryMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir {}", entry.path())
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list(&self, path: &str) -> Result<Vec<Entry>> {
        self.list_with(path).await
    }

    /// List entries that starts with given `path` in parent dir with more options.
    ///
    /// # Notes
    ///
    /// ## Streaming list
    ///
    /// This function will read all entries in the given directory. It could
    /// take very long time and consume a lot of memory if the directory
    /// contains a lot of entries.
    ///
    /// In order to avoid this, you can use [`Operator::lister`] to list entries in
    /// a streaming way.
    ///
    /// # Options
    ///
    /// ## `start_after`
    ///
    /// Specify the specified key to start listing from.
    ///
    /// This feature can be used to resume a listing from a previous point.
    ///
    /// The following example will resume the list operation from the `breakpoint`.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op
    ///     .list_with("path/to/dir/")
    ///     .start_after("breakpoint")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `recursive`
    ///
    /// Specify whether to list recursively or not.
    ///
    /// If `recursive` is set to `true`, we will list all entries recursively. If not, we'll only
    /// list the entries in the specified dir.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op.list_with("path/to/dir/").recursive(true).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `metakey`
    ///
    /// Specify the metadata that required to be fetched in entries.
    ///
    /// If `metakey` is not set, we will fetch only the entry's `mode`. Otherwise, we will retrieve
    /// the required metadata from storage services. Even if `metakey` is specified, the metadata
    /// may still be `None`, indicating that the storage service does not supply this information.
    ///
    /// Some storage services like `s3` could return more metadata like `content-length` and
    /// `last-modified`. By using `metakey`, we can fetch those metadata without an extra `stat` call.
    /// Please pick up the metadata you need to reduce the extra `stat` cost.
    ///
    /// This example shows how to list entries with `content-length` and `last-modified` metadata:
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use opendal::EntryMode;
    /// use opendal::Metakey;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op
    ///     .list_with("dir/")
    ///     // Make sure content-length and last-modified been fetched.
    ///     .metakey(Metakey::ContentLength | Metakey::LastModified)
    ///     .await?;
    /// for entry in entries {
    ///     let meta = entry.metadata();
    ///     match meta.mode() {
    ///         EntryMode::FILE => {
    ///             println!(
    ///                 "Handling file {} with size {}",
    ///                 entry.path(),
    ///                 meta.content_length()
    ///             )
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir {}", entry.path())
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Examples
    ///
    /// ## List all entries recursively
    ///
    /// This example will list all entries under the dir `path/to/dir/`
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use opendal::EntryMode;
    /// use opendal::Metakey;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op.list_with("path/to/dir/").recursive(true).await?;
    /// for entry in entries {
    ///     match entry.metadata().mode() {
    ///         EntryMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir like start a new list via meta.path()")
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## List all entries start with prefix
    ///
    /// This example will list all entries starts with prefix `path/to/prefix`
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use opendal::EntryMode;
    /// use opendal::Metakey;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op.list_with("path/to/prefix").recursive(true).await?;
    /// for entry in entries {
    ///     match entry.metadata().mode() {
    ///         EntryMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir like start a new list via meta.path()")
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn list_with(&self, path: &str) -> FutureList<impl Future<Output = Result<Vec<Entry>>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            OpList::default(),
            |inner, path, args| async move {
                let lister = Lister::create(inner, &path, args).await?;

                lister.try_collect().await
            },
        )
    }

    /// List entries that starts with given `path` in parent dir.
    ///
    /// This function will create a new [`Operator::Lister`] to list entries. Users can stop
    /// listing via dropping this [`Operator::Lister`].
    ///
    /// # Notes
    ///
    /// ## Recursively list
    ///
    /// This function only read the children of the given directory. To read
    /// all entries recursively, use [`Operator::lister_with`] and `recursive(true)`
    /// instead.
    ///
    /// ## Reuse Metadata
    ///
    /// The only metadata that is guaranteed to be available is the `Mode`.
    /// For fetching more metadata, please use [`Operator::lister_with`] and `metakey`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// # use futures::io;
    /// use futures::TryStreamExt;
    /// use opendal::EntryMode;
    /// use opendal::Metakey;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut ds = op.lister("path/to/dir/").await?;
    /// while let Some(mut de) = ds.try_next().await? {
    ///     match de.metadata().mode() {
    ///         EntryMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir like start a new list via meta.path()")
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn lister(&self, path: &str) -> Result<Lister> {
        self.lister_with(path).await
    }

    /// List entries that starts with given `path` in parent dir with options.
    ///
    /// This function will create a new [`Operator::Lister`] to list entries. Users can stop listing via
    /// dropping this [`Operator::Lister`].
    ///
    /// # Options
    ///
    /// ## `start_after`
    ///
    /// Specify the specified key to start listing from.
    ///
    /// This feature can be used to resume a listing from a previous point.
    ///
    /// The following example will resume the list operation from the `breakpoint`.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut lister = op
    ///     .lister_with("path/to/dir/")
    ///     .start_after("breakpoint")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `recursive`
    ///
    /// Specify whether to list recursively or not.
    ///
    /// If `recursive` is set to `true`, we will list all entries recursively. If not, we'll only
    /// list the entries in the specified dir.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut lister = op.lister_with("path/to/dir/").recursive(true).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `metakey`
    ///
    /// Specify the metadata that required to be fetched in entries.
    ///
    /// If `metakey` is not set, we will fetch only the entry's `mode`. Otherwise, we will retrieve
    /// the required metadata from storage services. Even if `metakey` is specified, the metadata
    /// may still be `None`, indicating that the storage service does not supply this information.
    ///
    /// Some storage services like `s3` could return more metadata like `content-length` and
    /// `last-modified`. By using `metakey`, we can fetch those metadata without an extra `stat` call.
    /// Please pick up the metadata you need to reduce the extra `stat` cost.
    ///
    /// This example shows how to list entries with `content-length` and `last-modified` metadata:
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use futures::TryStreamExt;
    /// use opendal::EntryMode;
    /// use opendal::Metakey;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut lister = op
    ///     .lister_with("dir/")
    ///     // Make sure content-length and last-modified been fetched.
    ///     .metakey(Metakey::ContentLength | Metakey::LastModified)
    ///     .await?;
    /// while let Some(mut entry) = lister.try_next().await? {
    ///     let meta = entry.metadata();
    ///     match meta.mode() {
    ///         EntryMode::FILE => {
    ///             println!(
    ///                 "Handling file {} with size {}",
    ///                 entry.path(),
    ///                 meta.content_length()
    ///             )
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir {}", entry.path())
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Examples
    ///
    /// ## List all files recursively
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use futures::TryStreamExt;
    /// use opendal::EntryMode;
    /// use opendal::Metakey;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut lister = op.lister_with("path/to/dir/").recursive(true).await?;
    /// while let Some(mut entry) = lister.try_next().await? {
    ///     match entry.metadata().mode() {
    ///         EntryMode::FILE => {
    ///             println!("Handling file {}", entry.path())
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir {}", entry.path())
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## List files with required metadata
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// # use futures::io;
    /// use futures::TryStreamExt;
    /// use opendal::EntryMode;
    /// use opendal::Metakey;
    /// use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut ds = op
    ///     .lister_with("path/to/dir/")
    ///     .metakey(Metakey::ContentLength | Metakey::LastModified)
    ///     .await?;
    /// while let Some(mut entry) = ds.try_next().await? {
    ///     let meta = entry.metadata();
    ///     match meta.mode() {
    ///         EntryMode::FILE => {
    ///             println!(
    ///                 "Handling file {} with size {}",
    ///                 entry.path(),
    ///                 meta.content_length()
    ///             )
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir {}", entry.path())
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn lister_with(&self, path: &str) -> FutureList<impl Future<Output = Result<Lister>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            OpList::default(),
            |inner, path, args| async move { Lister::create(inner, &path, args).await },
        )
    }
}

/// Operator presign API.
impl Operator {
    /// Presign an operation for stat(head).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_stat("test",Duration::from_secs(3600)).await?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub async fn presign_stat(&self, path: &str, expire: Duration) -> Result<PresignedRequest> {
        let path = normalize_path(path);

        let op = OpPresign::new(OpStat::new(), expire);

        let rp = self.inner().presign(&path, op).await?;
        Ok(rp.into_presigned_request())
    }

    /// Presign an operation for stat(head).
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_stat_with("test",Duration::from_secs(3600)).override_content_disposition("attachment; filename=\"othertext.txt\"").await?;
    /// #    Ok(())
    /// # }
    /// ```
    pub fn presign_stat_with(
        &self,
        path: &str,
        expire: Duration,
    ) -> FuturePresignStat<impl Future<Output = Result<PresignedRequest>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (OpStat::default(), expire),
            |inner, path, (args, dur)| async move {
                let op = OpPresign::new(args, dur);
                let rp = inner.presign(&path, op).await?;
                Ok(rp.into_presigned_request())
            },
        )
    }

    /// Presign an operation for read.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// `presign_read` is a wrapper of [`Self::presign_read_with`] without any options. To use
    /// extra options like `override_content_disposition`, please use [`Self::presign_read_with`]
    /// instead.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    /// use std::time::Duration;
    ///
    /// #[tokio::main]
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_read("test.txt", Duration::from_secs(3600)).await?;
    /// #    Ok(())
    /// # }
    /// ```
    ///
    /// - `signed_req.method()`: `GET`
    /// - `signed_req.uri()`: `https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>`
    /// - `signed_req.headers()`: `{ "host": "s3.amazonaws.com" }`
    ///
    /// We can download this file via `curl` or other tools without credentials:
    ///
    /// ```shell
    /// curl "https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>" -O /tmp/test.txt
    /// ```
    pub async fn presign_read(&self, path: &str, expire: Duration) -> Result<PresignedRequest> {
        let path = normalize_path(path);

        let op = OpPresign::new(OpRead::new(), expire);

        let rp = self.inner().presign(&path, op).await?;
        Ok(rp.into_presigned_request())
    }

    /// Presign an operation for read with extra options.
    ///
    /// # Options
    ///
    /// ## `override_content_disposition`
    ///
    /// Override the [`content-disposition`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition) header returned by storage services.
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op
    ///         .presign_read_with("test.txt", Duration::from_secs(3600))
    ///         .override_content_disposition("attachment; filename=\"othertext.txt\"")
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## `override_cache_control`
    ///
    /// Override the [`cache-control`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control) header returned by storage services.
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op
    ///         .presign_read_with("test.txt", Duration::from_secs(3600))
    ///         .override_cache_control("no-store")
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## `override_content_type`
    ///
    /// Override the [`content-type`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type) header returned by storage services.
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op
    ///         .presign_read_with("test.txt", Duration::from_secs(3600))
    ///         .override_content_type("text/plain")
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    pub fn presign_read_with(
        &self,
        path: &str,
        expire: Duration,
    ) -> FuturePresignRead<impl Future<Output = Result<PresignedRequest>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (OpRead::default(), expire),
            |inner, path, (args, dur)| async move {
                let op = OpPresign::new(args, dur);
                let rp = inner.presign(&path, op).await?;
                Ok(rp.into_presigned_request())
            },
        )
    }

    /// Presign an operation for write.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// `presign_write` is a wrapper of [`Self::presign_write_with`] without any options. To use
    /// extra options like `content_type`, please use [`Self::presign_write_with`] instead.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op
    ///         .presign_write("test.txt", Duration::from_secs(3600))
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// - `signed_req.method()`: `PUT`
    /// - `signed_req.uri()`: `https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>`
    /// - `signed_req.headers()`: `{ "host": "s3.amazonaws.com" }`
    ///
    /// We can upload file as this file via `curl` or other tools without credential:
    ///
    /// ```shell
    /// curl -X PUT "https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>" -d "Hello, World!"
    /// ```
    pub async fn presign_write(&self, path: &str, expire: Duration) -> Result<PresignedRequest> {
        self.presign_write_with(path, expire).await
    }

    /// Presign an operation for write with extra options.
    ///
    /// # Options
    ///
    /// ## `content_type`
    ///
    /// Set the [`content-type`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Type) header returned by storage services.
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op
    ///         .presign_write_with("test", Duration::from_secs(3600))
    ///         .content_type("text/csv")
    ///         .await?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## `content_disposition`
    ///
    /// Set the [`content-disposition`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Disposition) header returned by storage services.
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op
    ///         .presign_write_with("test", Duration::from_secs(3600))
    ///         .content_disposition("attachment; filename=\"cool.html\"")
    ///         .await?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    ///     Ok(())
    /// }
    /// ```
    ///
    /// ## `cache_control`
    ///
    /// Set the [`cache-control`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control) header returned by storage services.
    ///
    /// ```no_run
    /// use std::time::Duration;
    ///
    /// use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op
    ///         .presign_write_with("test", Duration::from_secs(3600))
    ///         .cache_control("no-store")
    ///         .await?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn presign_write_with(
        &self,
        path: &str,
        expire: Duration,
    ) -> FuturePresignWrite<impl Future<Output = Result<PresignedRequest>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (OpWrite::default(), expire),
            |inner, path, (args, dur)| async move {
                let op = OpPresign::new(args, dur);
                let rp = inner.presign(&path, op).await?;
                Ok(rp.into_presigned_request())
            },
        )
    }
}
