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

use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;

use super::BlockingOperator;
use crate::operator_futures::*;
use crate::raw::oio::Delete;
use crate::raw::*;
use crate::types::delete::Deleter;
use crate::*;

/// Operator is the entry for all public async APIs.
///
/// Developer should manipulate the data from storage service through Operator only by right.
///
/// We will usually do some general checks and data transformations in this layer,
/// like normalizing path from input, checking whether the path refers to one file or one directory,
/// and so on.
///
/// Read [`concepts`][crate::docs::concepts] for more about [`Operator`].
///
/// # Examples
///
/// Read more backend init examples in [`services`]
///
/// ```
/// # use anyhow::Result;
/// use opendal::services::Fs;
/// use opendal::Operator;
/// async fn test() -> Result<()> {
///     // Create fs backend builder.
///     let mut builder = Fs::default().root("/tmp");
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
    accessor: Accessor,

    // limit is usually the maximum size of data that operator will handle in one operation
    limit: usize,
    /// The default executor that used to run futures in background.
    default_executor: Option<Executor>,
}

/// # Operator basic API.
impl Operator {
    /// Fetch the internal accessor.
    pub fn inner(&self) -> &Accessor {
        &self.accessor
    }

    /// Convert inner accessor into operator.
    pub fn from_inner(accessor: Accessor) -> Self {
        let limit = accessor
            .info()
            .full_capability()
            .batch_max_operations
            .unwrap_or(1000);
        Self {
            accessor,
            limit,
            default_executor: None,
        }
    }

    /// Convert operator into inner accessor.
    pub fn into_inner(self) -> Accessor {
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

    /// Get the default executor.
    pub fn default_executor(&self) -> Option<Executor> {
        self.default_executor.clone()
    }

    /// Specify the default executor.
    pub fn with_default_executor(&self, executor: Executor) -> Self {
        let mut op = self.clone();
        op.default_executor = Some(executor);
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
    ///
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
    ///
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let mut metadata = op.stat_with("path/to/file").if_none_match(etag).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `version`
    ///
    /// Set `version` for this `stat` request.
    ///
    /// This feature can be used to retrieve the metadata of a specific version of the given path
    ///
    /// If the version doesn't exist, an error with kind [`ErrorKind::NotFound`] will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    ///
    /// # async fn test(op: Operator, version: &str) -> Result<()> {
    /// let mut metadata = op.stat_with("path/to/file").version(version).await?;
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
    /// async fn test(op: Operator) -> Result<()> {
    ///     let _ = op.exists("test").await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn exists(&self, path: &str) -> Result<bool> {
        let r = self.stat(path).await;
        match r {
            Ok(_) => Ok(true),
            Err(err) => match err.kind() {
                ErrorKind::NotFound => Ok(false),
                _ => Err(err),
            },
        }
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
    /// async fn test(op: Operator) -> Result<()> {
    ///     let _ = op.is_exist("test").await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    #[deprecated(note = "rename to `exists` for consistence with `std::fs::exists`")]
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
    /// # use opendal::Result;
    /// # use opendal::Operator;
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
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op.read("path/to/file").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(&self, path: &str) -> Result<Buffer> {
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
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
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
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let mut metadata = op.read_with("path/to/file").if_none_match(etag).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `concurrent`
    ///
    /// Set `concurrent` for the reader.
    ///
    /// OpenDAL by default to write file without concurrent. This is not efficient for cases when users
    /// read large chunks of data. By setting `concurrent`, opendal will read files concurrently
    /// on support storage services.
    ///
    /// By setting `concurrent`, opendal will fetch chunks concurrently with
    /// the given chunk size.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use opendal::Scheme;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op.read_with("path/to/file").concurrent(8).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `chunk`
    ///
    /// OpenDAL will use services' preferred chunk size by default. Users can set chunk based on their own needs.
    ///
    /// This following example will make opendal read data in 4MiB chunks:
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use opendal::Scheme;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op.read_with("path/to/file").chunk(4 * 1024 * 1024).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `version`
    ///
    /// Set `version` for this `read` request.
    ///
    /// This feature can be used to retrieve the data of a specified version of the given path.
    ///
    /// If the version doesn't exist, an error with kind [`ErrorKind::NotFound`] will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    ///
    /// # async fn test(op: Operator, version: &str) -> Result<()> {
    /// let mut bs = op.read_with("path/to/file").version(version).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Examples
    ///
    /// Read the whole path into a bytes.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op.read_with("path/to/file").await?;
    /// let bs = op.read_with("path/to/file").range(0..10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_with(&self, path: &str) -> FutureRead<impl Future<Output = Result<Buffer>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (
                OpRead::default().merge_executor(self.default_executor.clone()),
                OpReader::default(),
            ),
            |inner, path, (args, options)| async move {
                if !validate_path(&path, EntryMode::FILE) {
                    return Err(
                        Error::new(ErrorKind::IsADirectory, "read path is a directory")
                            .with_operation("read")
                            .with_context("service", inner.info().scheme())
                            .with_context("path", &path),
                    );
                }

                let range = args.range();
                let context = ReadContext::new(inner, path, args, options);
                let r = Reader::new(context);
                let buf = r.read(range.to_range()).await?;
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
    /// extra options like `concurrent`, please use [`Operator::reader_with`] instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # use opendal::Scheme;
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
    /// extra options like `version`, please use [`Operator::reader_with`] instead.
    ///
    /// # Options
    ///
    /// ## `concurrent`
    ///
    /// Set `concurrent` for the reader.
    ///
    /// OpenDAL by default to write file without concurrent. This is not efficient for cases when users
    /// read large chunks of data. By setting `concurrent`, opendal will reading files concurrently
    /// on support storage services.
    ///
    /// By setting `concurrent``, opendal will fetch chunks concurrently with
    /// the give chunk size.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use opendal::Scheme;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op.reader_with("path/to/file").concurrent(8).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `chunk`
    ///
    /// OpenDAL will use services' preferred chunk size by default. Users can set chunk based on their own needs.
    ///
    /// This following example will make opendal read data in 4MiB chunks:
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use opendal::Scheme;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op
    ///     .reader_with("path/to/file")
    ///     .chunk(4 * 1024 * 1024)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `version`
    ///
    /// Set `version` for this `reader`.
    ///
    /// This feature can be used to retrieve the data of a specified version of the given path.
    ///
    /// If the version doesn't exist, an error with kind [`ErrorKind::NotFound`] will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    ///
    /// # async fn test(op: Operator, version: &str) -> Result<()> {
    /// let mut bs = op.reader_with("path/to/file").version(version).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use opendal::Scheme;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op.reader_with("path/to/file").version("version_id").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reader_with(&self, path: &str) -> FutureReader<impl Future<Output = Result<Reader>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (
                OpRead::default().merge_executor(self.default_executor.clone()),
                OpReader::default(),
            ),
            |inner, path, (args, options)| async move {
                if !validate_path(&path, EntryMode::FILE) {
                    return Err(
                        Error::new(ErrorKind::IsADirectory, "read path is a directory")
                            .with_operation("Operator::reader")
                            .with_context("service", inner.info().scheme())
                            .with_context("path", path),
                    );
                }

                let context = ReadContext::new(inner, path, args, options);
                Ok(Reader::new(context))
            },
        )
    }

    /// Write bytes into path.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// [`Operator::write`] is a simplified version of [`Operator::write_with`] without additional options.
    /// For advanced features like `content_type` and `cache_control`, use [`Operator::write_with`] instead.
    ///
    /// ## Streaming Write
    ///
    /// This method performs a single bulk write operation. For finer-grained memory control
    /// or streaming data writes, use [`Operator::writer`] instead.
    ///
    /// ## Multipart Uploads
    ///
    /// OpenDAL provides multipart upload functionality through the [`Writer`] abstraction,
    /// handling all upload details automatically. You can customize the upload behavior by
    /// configuring `chunk` size and `concurrent` operations via [`Operator::writer_with`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.write("path/to/file", vec![0; 4096]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(&self, path: &str, bs: impl Into<Buffer>) -> Result<()> {
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
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    ///
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
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    ///
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

    /// Create a writer for streaming data to the given path.
    ///
    /// # Notes
    ///
    /// ## Writer Features
    ///
    /// The writer provides several powerful capabilities:
    /// - Streaming writes for continuous data transfer
    /// - Automatic multipart upload handling
    /// - Memory-efficient chunk-based writing
    ///
    /// ## Extra Options
    ///
    /// [`Operator::writer`] is a simplified version of [`Operator::writer_with`] without additional options.
    /// For advanced features like `content_type` and `cache_control`, use [`Operator::writer_with`] instead.
    ///
    /// ## Chunk Size Handling
    ///
    /// Storage services often have specific requirements for chunk sizes:
    /// - Services like `s3` may return `EntityTooSmall` errors for undersized chunks
    /// - Using small chunks in cloud storage services can lead to increased costs
    ///
    /// OpenDAL automatically determines optimal chunk sizes based on the service's
    /// [Capability](crate::types::Capability). However, you can override this by explicitly
    /// setting the `chunk` parameter.
    ///
    /// For improved performance, consider setting an appropriate chunk size using
    /// [`Operator::writer_with`].
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
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

    /// Create a writer for streaming data to the given path with more options.
    ///
    /// # Usages
    ///
    /// ## `append`
    ///
    /// Sets append mode for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_can_append`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - By default, write operations overwrite existing files
    /// - When append is set to true:
    ///   - New data will be appended to the end of existing file
    ///   - If file doesn't exist, it will be created
    /// - If not supported, will return an error
    ///
    /// This operation allows adding data to existing files instead of overwriting them.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op.writer_with("path/to/file").append(true).await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `chunk`
    ///
    /// Sets chunk size for buffered writes.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_multi_min_size`] and [`Capability::write_multi_max_size`] for size limits.
    ///
    /// ### Behavior
    ///
    /// - By default, OpenDAL sets optimal chunk size based on service capabilities
    /// - When chunk size is set:
    ///   - Data will be buffered until reaching chunk size
    ///   - One API call will be made per chunk
    ///   - Last chunk may be smaller than chunk size
    /// - Important considerations:
    ///   - Some services require minimum chunk sizes (e.g. S3's EntityTooSmall error)
    ///   - Smaller chunks increase API calls and costs
    ///   - Larger chunks increase memory usage, but improve performance and reduce costs
    ///
    /// ### Performance Impact
    ///
    /// Setting appropriate chunk size can:
    /// - Reduce number of API calls
    /// - Improve overall throughput
    /// - Lower operation costs
    /// - Better utilize network bandwidth
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// // Set 8MiB chunk size - data will be sent in one API call at close
    /// let mut w = op
    ///     .writer_with("path/to/file")
    ///     .chunk(8 * 1024 * 1024)
    ///     .await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # `concurrent`
    ///
    /// Sets concurrent write operations for this writer.
    ///
    /// ## Behavior
    ///
    /// - By default, OpenDAL writes files sequentially
    /// - When concurrent is set:
    ///   - Multiple write operations can execute in parallel
    ///   - Write operations return immediately without waiting if tasks space are available
    ///   - Close operation ensures all writes complete in order
    ///   - Memory usage increases with concurrency level
    /// - If not supported, falls back to sequential writes
    ///
    /// This feature significantly improves performance when:
    /// - Writing large files
    /// - Network latency is high
    /// - Storage service supports concurrent uploads like multipart uploads
    ///
    /// ## Performance Impact
    ///
    /// Setting appropriate concurrency can:
    /// - Increase write throughput
    /// - Reduce total write time
    /// - Better utilize available bandwidth
    /// - Trade memory for performance
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// // Enable concurrent writes with 8 parallel operations
    /// let mut w = op.writer_with("path/to/file").concurrent(8).await?;
    ///
    /// // First write starts immediately
    /// w.write(vec![0; 4096]).await?;
    ///
    /// // Second write runs concurrently with first
    /// w.write(vec![1; 4096]).await?;
    ///
    /// // Ensures all writes complete successfully and in order
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `cache_control`
    ///
    /// Sets Cache-Control header for this write operation.
    ///
    /// ### Capability
    ///
    /// Sets `Cache-Control` header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_cache_control`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Cache-Control as system metadata on the target file
    /// - The value should follow HTTP Cache-Control header format
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows controlling caching behavior for the written content.
    ///
    /// ### Use Cases
    ///
    /// - Setting browser cache duration
    /// - Configuring CDN behavior
    /// - Optimizing content delivery
    /// - Managing cache invalidation
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// // Cache content for 7 days (604800 seconds)
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
    /// ### References
    ///
    /// - [MDN Cache-Control](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Cache-Control)
    /// - [RFC 7234 Section 5.2](https://tools.ietf.org/html/rfc7234#section-5.2)
    ///
    /// ## `content_type`
    ///
    /// Sets `Content-Type` header for this write operation.
    ///
    /// ## Capability
    ///
    /// Check [`Capability::write_with_content_type`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Type as system metadata on the target file
    /// - The value should follow MIME type format (e.g. "text/plain", "image/jpeg")
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows specifying the media type of the content being written.
    ///
    /// ## Example
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// // Set content type for plain text file
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
    /// Sets Content-Disposition header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_content_disposition`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Disposition as system metadata on the target file
    /// - The value should follow HTTP Content-Disposition header format
    /// - Common values include:
    ///   - `inline` - Content displayed within browser
    ///   - `attachment` - Content downloaded as file
    ///   - `attachment; filename="example.jpg"` - Downloaded with specified filename
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows controlling how the content should be displayed or downloaded.
    ///
    /// ### Example
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
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
    pub fn writer_with(&self, path: &str) -> FutureWriter<impl Future<Output = Result<Writer>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (
                OpWrite::default().merge_executor(self.default_executor.clone()),
                OpWriter::default(),
            ),
            |inner, path, (args, options)| async move {
                if !validate_path(&path, EntryMode::FILE) {
                    return Err(
                        Error::new(ErrorKind::IsADirectory, "write path is a directory")
                            .with_operation("Operator::writer")
                            .with_context("service", inner.info().scheme().into_static())
                            .with_context("path", &path),
                    );
                }

                let context = WriteContext::new(inner, path, args, options);
                let w = Writer::new(context).await?;
                Ok(w)
            },
        )
    }

    /// Write data with extra options.
    ///
    /// # Notes
    ///
    /// ## Streaming Write
    ///
    /// This method performs a single bulk write operation for all bytes. For finer-grained
    /// memory control or lazy writing, consider using [`Operator::writer_with`] instead.
    ///
    /// ## Multipart Uploads
    ///
    /// OpenDAL handles multipart uploads through the [`Writer`] abstraction, managing all
    /// the upload details automatically. You can customize the upload behavior by configuring
    /// `chunk` size and `concurrent` operations via [`Operator::writer_with`].
    ///
    /// # Usages
    ///
    /// ## `append`
    ///
    /// Sets `append` mode for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_append`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If append is true, data will be appended to the end of existing file
    /// - If append is false (default), existing file will be overwritten
    ///
    /// This operation allows appending data to existing files instead of overwriting them.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let _ = op.write_with("path/to/file", bs).append(true).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `cache_control`
    ///
    /// Sets `Cache-Control` header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_cache_control`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Cache-Control as system metadata on the target file
    /// - The value should follow HTTP Cache-Control header format
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows controlling caching behavior for the written content.
    ///
    /// ## Use Cases
    ///
    /// - Setting browser cache duration
    /// - Configuring CDN behavior
    /// - Optimizing content delivery
    /// - Managing cache invalidation
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
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
    /// Sets Content-Type header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_content_type`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Type as system metadata on the target file
    /// - The value should follow MIME type format (e.g. "text/plain", "image/jpeg")
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows specifying the media type of the content being written.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
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
    /// Sets Content-Disposition header for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_content_disposition`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If supported, sets Content-Disposition as system metadata on the target file
    /// - The value should follow HTTP Content-Disposition header format
    /// - Common values include:
    ///   - `inline` - Content displayed within browser
    ///   - `attachment` - Content downloaded as file
    ///   - `attachment; filename="example.jpg"` - Downloaded with specified filename
    /// - If not supported, the value will be ignored
    ///
    /// This operation allows controlling how the content should be displayed or downloaded.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
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
    /// ## `if_none_match`
    ///
    /// Sets an `if none match` condition with specified ETag for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_if_none_match`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If the target file's ETag equals the specified one, returns [`ErrorKind::ConditionNotMatch`]
    /// - If the target file's ETag differs from the specified one, proceeds with the write operation
    ///
    /// This operation will succeed when the target's ETag is different from the specified one,
    /// providing a way for concurrency control.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::{ErrorKind, Result};
    /// use opendal::Operator;
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let res = op.write_with("path/to/file", bs).if_none_match(etag).await;
    /// assert!(res.is_err());
    /// assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `if_not_exists`
    ///
    /// Sets an `if not exists` condition for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_if_not_exists`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If the target file exists, returns [`ErrorKind::ConditionNotMatch`]
    /// - If the target file doesn't exist, proceeds with the write operation
    ///
    /// This operation provides atomic file creation that is concurrency-safe.
    /// Only one write operation will succeed while others will fail.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::{ErrorKind, Result};
    /// use opendal::Operator;
    /// # async fn test(op: Operator, etag: &str) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let res = op.write_with("path/to/file", bs).if_not_exists(true).await;
    /// assert!(res.is_err());
    /// assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `if_match`
    ///
    /// Sets an `if match` condition with specified ETag for this write request.
    ///
    /// ### Capability
    ///
    /// Check [`Capability::write_with_if_match`] before using this feature.
    ///
    /// ### Behavior
    ///
    /// - If the target file's ETag matches the specified one, proceeds with the write operation
    /// - If the target file's ETag does not match the specified one, returns [`ErrorKind::ConditionNotMatch`]
    ///
    /// This operation will succeed when the target's ETag matches the specified one,
    /// providing a way for conditional writes.
    ///
    /// ### Example
    ///
    /// ```no_run
    /// # use opendal::{ErrorKind, Result};
    /// use opendal::Operator;
    /// # async fn test(op: Operator, incorrect_etag: &str) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let res = op.write_with("path/to/file", bs).if_match(incorrect_etag).await;
    /// assert!(res.is_err());
    /// assert_eq!(res.unwrap_err().kind(), ErrorKind::ConditionNotMatch);
    /// # Ok(())
    /// # }
    /// ```
    pub fn write_with(
        &self,
        path: &str,
        bs: impl Into<Buffer>,
    ) -> FutureWrite<impl Future<Output = Result<()>>> {
        let path = normalize_path(path);
        let bs = bs.into();

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (
                OpWrite::default().merge_executor(self.default_executor.clone()),
                OpWriter::default(),
                bs,
            ),
            |inner, path, (args, options, bs)| async move {
                if !validate_path(&path, EntryMode::FILE) {
                    return Err(
                        Error::new(ErrorKind::IsADirectory, "write path is a directory")
                            .with_operation("Operator::write_with")
                            .with_context("service", inner.info().scheme().into_static())
                            .with_context("path", &path),
                    );
                }

                let context = WriteContext::new(inner, path, args, options);
                let mut w = Writer::new(context).await?;
                w.write(bs).await?;
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
    /// # Options
    ///
    /// ## `version`
    ///
    /// Set `version` for this `delete` request.
    ///
    /// remove a specific version of the given path.
    ///
    /// If the version doesn't exist, OpenDAL will not return errors.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    ///
    /// # async fn test(op: Operator, version: &str) -> Result<()> {
    /// op.delete_with("path/to/file").version(version).await?;
    /// # Ok(())
    /// # }
    ///```
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    ///
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
                let (_, mut deleter) = inner.delete().await?;
                deleter.delete(&path, args)?;
                deleter.flush().await?;
                Ok(())
            },
        )
    }

    /// Delete an infallible iterator of paths.
    ///
    /// Also see:
    ///
    /// - [`Operator::delete_try_iter`]: delete an fallible iterator of paths.
    /// - [`Operator::delete_stream`]: delete an infallible stream of paths.
    /// - [`Operator::delete_try_stream`]: delete an fallible stream of paths.
    pub async fn delete_iter<I, D>(&self, iter: I) -> Result<()>
    where
        I: IntoIterator<Item = D>,
        D: IntoDeleteInput,
    {
        let mut deleter = self.deleter().await?;
        deleter.delete_iter(iter).await?;
        deleter.close().await?;
        Ok(())
    }

    /// Delete an infallible iterator of paths.
    ///
    /// Also see:
    ///
    /// - [`Operator::delete_iter`]: delete an infallible iterator of paths.
    /// - [`Operator::delete_stream`]: delete an infallible stream of paths.
    /// - [`Operator::delete_try_stream`]: delete an fallible stream of paths.
    pub async fn delete_try_iter<I, D>(&self, try_iter: I) -> Result<()>
    where
        I: IntoIterator<Item = Result<D>>,
        D: IntoDeleteInput,
    {
        let mut deleter = self.deleter().await?;
        deleter.delete_try_iter(try_iter).await?;
        deleter.close().await?;
        Ok(())
    }

    /// Delete an infallible stream of paths.
    ///
    /// Also see:
    ///
    /// - [`Operator::delete_iter`]: delete an infallible iterator of paths.
    /// - [`Operator::delete_try_iter`]: delete an fallible iterator of paths.
    /// - [`Operator::delete_try_stream`]: delete an fallible stream of paths.
    pub async fn delete_stream<S, D>(&self, stream: S) -> Result<()>
    where
        S: Stream<Item = D>,
        D: IntoDeleteInput,
    {
        let mut deleter = self.deleter().await?;
        deleter.delete_stream(stream).await?;
        deleter.close().await?;
        Ok(())
    }

    /// Delete an infallible stream of paths.
    ///
    /// Also see:
    ///
    /// - [`Operator::delete_iter`]: delete an infallible iterator of paths.
    /// - [`Operator::delete_try_iter`]: delete an fallible iterator of paths.
    /// - [`Operator::delete_stream`]: delete an infallible stream of paths.
    pub async fn delete_try_stream<S, D>(&self, try_stream: S) -> Result<()>
    where
        S: Stream<Item = Result<D>>,
        D: IntoDeleteInput,
    {
        let mut deleter = self.deleter().await?;
        deleter.delete_try_stream(try_stream).await?;
        deleter.close().await?;
        Ok(())
    }

    /// Create a [`Deleter`] to continuously remove content from storage.
    ///
    /// It leverages batch deletion capabilities provided by storage services for efficient removal.
    ///
    /// Users can have more control over the deletion process by using [`Deleter`] directly.
    pub async fn deleter(&self) -> Result<Deleter> {
        Deleter::create(self.inner().clone()).await
    }

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
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.remove(vec!["abc".to_string(), "def".to_string()])
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(note = "use `Operator::delete_iter` instead", since = "0.52.0")]
    pub async fn remove(&self, paths: Vec<String>) -> Result<()> {
        let mut deleter = self.deleter().await?;
        deleter.delete_iter(paths).await?;
        deleter.close().await?;
        Ok(())
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
    /// # async fn test(op: Operator) -> Result<()> {
    /// let stream = stream::iter(vec!["abc".to_string(), "def".to_string()]);
    /// op.remove_via(stream).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(note = "use `Operator::delete_stream` instead", since = "0.52.0")]
    pub async fn remove_via(&self, input: impl Stream<Item = String> + Unpin) -> Result<()> {
        let mut deleter = self.deleter().await?;
        deleter
            .delete_stream(input.map(|v| normalize_path(&v)))
            .await?;
        deleter.close().await?;

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
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.remove_all("path/to/dir").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_all(&self, path: &str) -> Result<()> {
        match self.stat(path).await {
            // If object exists.
            Ok(metadata) => {
                // If the object is a file, we can delete it.
                if metadata.mode() != EntryMode::DIR {
                    self.delete(path).await?;
                    // There may still be objects prefixed with the path in some backend, so we can't return here.
                }
            }

            // If dir not found, it may be a prefix in object store like S3,
            // and we still need to delete objects under the prefix.
            Err(e) if e.kind() == ErrorKind::NotFound => {}

            // Pass on any other error.
            Err(e) => return Err(e),
        };

        let lister = self.lister_with(path).recursive(true).await?;
        self.delete_try_stream(lister).await?;
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
    /// # Examples
    ///
    /// ## List entries under a dir
    ///
    /// This example will list all entries under the dir `path/to/dir/`.
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// use opendal::EntryMode;
    /// use opendal::Operator;
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
    /// use opendal::Operator;
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
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op.list_with("path/to/dir/").recursive(true).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `version`
    ///
    /// Specify whether to list files along with all their versions
    ///
    /// if `version` is enabled, all file versions will be returned; otherwise,
    /// only the current files will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op.list_with("path/to/dir/").version(true).await?;
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
    /// use opendal::Operator;
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
    /// use opendal::Operator;
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
    /// This function will create a new [`Lister`] to list entries. Users can stop
    /// listing via dropping this [`Lister`].
    ///
    /// # Notes
    ///
    /// ## Recursively list
    ///
    /// This function only read the children of the given directory. To read
    /// all entries recursively, use [`Operator::lister_with`] and `recursive(true)`
    /// instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// # use futures::io;
    /// use futures::TryStreamExt;
    /// use opendal::EntryMode;
    /// use opendal::Operator;
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
    /// This function will create a new [`Lister`] to list entries. Users can stop listing via
    /// dropping this [`Lister`].
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
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut lister = op.lister_with("path/to/dir/").recursive(true).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## `version`
    ///
    /// Specify whether to list files along with all their versions
    ///
    /// if `version` is enabled, all file versions will be returned; otherwise,
    /// only the current files will be returned.
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op.lister_with("path/to/dir/").version(true).await?;
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
    /// use opendal::Operator;
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
    pub fn lister_with(&self, path: &str) -> FutureLister<impl Future<Output = Result<Lister>>> {
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
