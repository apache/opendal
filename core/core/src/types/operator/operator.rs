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

use crate::operator_futures::*;
use crate::raw::oio::DeleteDyn;
use crate::raw::*;
use crate::types::delete::Deleter;
use crate::*;

/// The `Operator` serves as the entry point for all public asynchronous APIs.
///
/// For more details about the `Operator`, refer to the [`concepts`][crate::docs::concepts] section.
///
/// All cloned `Operator` instances share the same internal state, such as
/// `HttpClient` and `Runtime`. Some layers may modify the internal state of
/// the `Operator` too like inject logging and metrics for `HttpClient`.
///
/// ## Build
///
/// Users can initialize an `Operator` through the following methods:
///
/// - [`Operator::new`]: Creates an operator using a [`services`] builder, such as [`services::S3`].
/// - [`Operator::from_config`]: Creates an operator using a [`services`] configuration, such as [`services::S3Config`].
/// - [`Operator::from_iter`]: Creates an operator from an iterator of configuration key-value pairs.
///
/// ```
/// # use anyhow::Result;
/// use opendal::services::Memory;
/// use opendal::Operator;
/// async fn test() -> Result<()> {
///     // Build an `Operator` to start operating the storage.
///     let _: Operator = Operator::new(Memory::default())?.finish();
///
///     Ok(())
/// }
/// ```
///
/// ## Layer
///
/// After the operator is built, users can add the layers they need on top of it.
///
/// OpenDAL offers various layers for users to choose from, such as `RetryLayer`, `LoggingLayer`, and more. Visit [`layers`] for further details.
///
/// Please note that `Layer` can modify internal contexts such as `HttpClient`
/// and `Runtime` for all clones of given operator. Therefore, it is recommended
/// to add layers before interacting with the storage. Adding or duplicating
/// layers after accessing the storage may result in unexpected behavior.
///
/// ```
/// # use anyhow::Result;
/// use opendal::layers::RetryLayer;
/// use opendal::services::Memory;
/// use opendal::Operator;
/// async fn test() -> Result<()> {
///     let op: Operator = Operator::new(Memory::default())?.finish();
///
///     // OpenDAL will retry failed operations now.
///     let op = op.layer(RetryLayer::default());
///
///     Ok(())
/// }
/// ```
///
/// ## Operate
///
/// After the operator is built and the layers are added, users can start operating the storage.
///
/// The operator is `Send`, `Sync`, and `Clone`. It has no internal state, and all APIs only take
/// a `&self` reference, making it safe to share the operator across threads.
///
/// Operator provides a consistent API pattern for data operations. For reading operations, it exposes:
///
/// - [`Operator::read`]: Executes a read operation.
/// - [`Operator::read_with`]: Executes a read operation with additional options using the builder pattern.
/// - [`Operator::read_options`]: Executes a read operation with extra options provided via a [`options::ReadOptions`] struct.
/// - [`Operator::reader`]: Creates a reader for streaming data, allowing for flexible access.
/// - [`Operator::reader_with`]: Creates a reader with advanced options using the builder pattern.
/// - [`Operator::reader_options`]: Creates a reader with extra options provided via a [`options::ReadOptions`] struct.
///
/// The [`Reader`] created by [`Operator`] supports custom read control methods and can be converted
/// into [`futures::AsyncRead`] or [`futures::Stream`] for broader ecosystem compatibility.
///
/// ```no_run
/// use opendal::layers::LoggingLayer;
/// use opendal::options;
/// use opendal::services;
/// use opendal::Operator;
/// use opendal::Result;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // Pick a builder and configure it.
///     let mut builder = services::S3::default().bucket("test");
///
///     // Init an operator
///     let op = Operator::new(builder)?
///         // Init with logging layer enabled.
///         .layer(LoggingLayer::default())
///         .finish();
///
///     // Fetch this file's metadata
///     let meta = op.stat("hello.txt").await?;
///     let length = meta.content_length();
///
///     // Read data from `hello.txt` with options.
///     let bs = op
///         .read_with("hello.txt")
///         .range(0..8 * 1024 * 1024)
///         .chunk(1024 * 1024)
///         .concurrent(4)
///         .await?;
///
///     // The same to:
///     let bs = op
///         .read_options("hello.txt", options::ReadOptions {
///             range: (0..8 * 1024 * 1024).into(),
///             chunk: Some(1024 * 1024),
///             concurrent: 4,
///             ..Default::default()
///         })
///         .await?;
///
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct Operator {
    // accessor is what Operator delegates for
    accessor: Accessor,
}

/// # Operator basic API.
impl Operator {
    /// Fetch the internal accessor.
    pub fn inner(&self) -> &Accessor {
        &self.accessor
    }

    /// Convert inner accessor into operator.
    pub fn from_inner(accessor: Accessor) -> Self {
        Self { accessor }
    }

    /// Convert operator into inner accessor.
    pub fn into_inner(self) -> Accessor {
        self.accessor
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

    /// Get the executor used by current operator.
    pub fn executor(&self) -> Executor {
        self.accessor.info().executor()
    }

    /// Update executor for the context.
    ///
    /// All cloned `Operator` instances share the same internal state, such as
    /// `HttpClient` and `Runtime`. Some layers may modify the internal state of
    /// the `Operator` too like inject logging and metrics for `HttpClient`.
    ///
    /// # Note
    ///
    /// Tasks must be forwarded to the old executor after the update. Otherwise, features such as retry, timeout, and metrics may not function properly.
    pub fn update_executor(&self, f: impl FnOnce(Executor) -> Executor) {
        self.accessor.info().update_executor(f);
    }

    /// Get the http client used by current operator.
    #[deprecated(
        since = "0.54.0",
        note = "Use HttpClientLayer instead. This method will be removed in next version."
    )]
    pub fn http_client(&self) -> HttpClient {
        self.accessor.info().http_client()
    }

    /// Update http client for the context.
    ///
    /// All cloned `Operator` instances share the same internal state, such as
    /// `HttpClient` and `Runtime`. Some layers may modify the internal state of
    /// the `Operator` too like inject logging and metrics for `HttpClient`.
    ///
    /// # Note
    ///
    /// Tasks must be forwarded to the old executor after the update. Otherwise, features such as retry, timeout, and metrics may not function properly.
    ///
    /// # Deprecated
    ///
    /// This method is deprecated since v0.54.0. Use [`HttpClientLayer`] instead.
    ///
    /// ## Migration Example
    ///
    /// Instead of:
    /// ```ignore
    /// let operator = Operator::new(service)?;
    /// operator.update_http_client(|_| custom_client);
    /// ```
    ///
    /// Use:
    /// ```ignore
    /// use opendal::layers::HttpClientLayer;
    ///
    /// let operator = Operator::new(service)?
    ///     .layer(HttpClientLayer::new(custom_client))
    ///     .finish();
    /// ```
    ///
    /// [`HttpClientLayer`]: crate::layers::HttpClientLayer
    #[deprecated(
        since = "0.54.0",
        note = "Use HttpClientLayer instead. This method will be removed in next version"
    )]
    pub fn update_http_client(&self, f: impl FnOnce(HttpClient) -> HttpClient) {
        self.accessor.info().update_http_client(f);
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
        let mut ds = self.lister_with("/").limit(1).await?;

        match ds.next().await {
            Some(Err(e)) if e.kind() != ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Retrieve the metadata for the specified path.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// [`Operator::stat`] is a wrapper around [`Operator::stat_with`] that uses no additional options.
    /// To specify extra options such as `if_match` and `if_none_match`, please use [`Operator::stat_with`] instead.
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

    /// Retrieve the metadata of the specified path with additional options.
    ///
    /// # Options
    ///
    /// Check [`options::StatOptions`] for all available options.
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
    pub fn stat_with(&self, path: &str) -> FutureStat<impl Future<Output = Result<Metadata>>> {
        let path = normalize_path(path);
        OperatorFuture::new(
            self.inner().clone(),
            path,
            options::StatOptions::default(),
            Self::stat_inner,
        )
    }

    /// Retrieve the metadata of the specified path with additional options.
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
    /// use opendal::options;
    /// use opendal::ErrorKind;
    /// #
    /// # async fn test(op: Operator) -> Result<()> {
    /// let res = op
    ///     .stat_options("test", options::StatOptions {
    ///         if_match: Some("<etag>".to_string()),
    ///         ..Default::default()
    ///     })
    ///     .await;
    /// if let Err(e) = res {
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
    pub async fn stat_options(&self, path: &str, opts: options::StatOptions) -> Result<Metadata> {
        let path = normalize_path(path);
        Self::stat_inner(self.accessor.clone(), path, opts).await
    }

    #[inline]
    async fn stat_inner(
        acc: Accessor,
        path: String,
        opts: options::StatOptions,
    ) -> Result<Metadata> {
        let rp = acc.stat(&path, opts.into()).await?;
        Ok(rp.into_metadata())
    }

    /// Check whether this path exists.
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
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(false),
            Err(err) => Err(err),
        }
    }

    /// Create a directory at the specified path.
    ///
    /// # Notes
    ///
    /// To specify that a path is a directory, you must include a trailing slash (/).
    /// Omitting the trailing slash may cause OpenDAL to return a `NotADirectory` error.
    ///
    /// # Behavior
    ///
    /// - Creating a directory that already exists will succeed.
    /// - Directory creation is always recursive, functioning like `mkdir -p`.
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

    /// Read the entire file into bytes from given path.
    ///
    /// # Notes
    ///
    /// ## Additional Options
    ///
    /// [`Operator::read`] is a simplified method that does not support additional options. To access features like `range` and `if_match`, please use [`Operator::read_with`] or [`Operator::read_options`] instead.
    ///
    /// ## Streaming Read
    ///
    /// This function reads all content into memory at once. For more precise memory management or to read big file lazily, please use [`Operator::reader`].
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op.read("path/to/file").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(&self, path: &str) -> Result<Buffer> {
        self.read_options(path, options::ReadOptions::default())
            .await
    }

    /// Read the entire file into bytes from given path with additional options.
    ///
    /// # Notes
    ///
    /// ## Streaming Read
    ///
    /// This function reads all content into memory at once. For more precise memory management or to read big file lazily, please use [`Operator::reader`].
    ///
    /// # Options
    ///
    /// Visit [`options::ReadOptions`] for all available options.
    ///
    /// # Examples
    ///
    /// Read the first 10 bytes of a file:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op.read_with("path/to/file").range(0..10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn read_with(&self, path: &str) -> FutureRead<impl Future<Output = Result<Buffer>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            options::ReadOptions::default(),
            Self::read_inner,
        )
    }

    /// Read the entire file into bytes from given path with additional options.
    ///
    /// # Notes
    ///
    /// ## Streaming Read
    ///
    /// This function reads all content into memory at once. For more precise memory management or to read big file lazily, please use [`Operator::reader`].
    ///
    /// # Examples
    ///
    /// Read the first 10 bytes of a file:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use opendal::options;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op
    ///     .read_options("path/to/file", options::ReadOptions {
    ///         range: (0..10).into(),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_options(&self, path: &str, opts: options::ReadOptions) -> Result<Buffer> {
        let path = normalize_path(path);
        Self::read_inner(self.inner().clone(), path, opts).await
    }

    #[inline]
    async fn read_inner(acc: Accessor, path: String, opts: options::ReadOptions) -> Result<Buffer> {
        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "read path is a directory")
                    .with_operation("read")
                    .with_context("service", acc.info().scheme())
                    .with_context("path", &path),
            );
        }

        let (args, opts) = opts.into();
        let range = args.range();
        let context = ReadContext::new(acc, path, args, opts);
        let r = Reader::new(context);
        let buf = r.read(range.to_range()).await?;
        Ok(buf)
    }

    /// Create a new reader of given path.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// [`Operator::reader`] is a simplified method without any options. To use additional options such as `concurrent` or `if_match`, please use [`Operator::reader_with`] or [`Operator::reader_options`] instead.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op.reader("path/to/file").await?;
    /// // Read the first 10 bytes of the file
    /// let data = r.read(0..10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn reader(&self, path: &str) -> Result<Reader> {
        self.reader_with(path).await
    }

    /// Create a new reader of given path with additional options.
    ///
    /// # Options
    ///
    /// Visit [`options::ReaderOptions`] for all available options.
    ///
    /// # Examples
    ///
    /// Create a reader with a specific version ID:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op.reader_with("path/to/file").version("version_id").await?;
    /// // Read the first 10 bytes of the file
    /// let data = r.read(0..10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reader_with(&self, path: &str) -> FutureReader<impl Future<Output = Result<Reader>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            options::ReaderOptions::default(),
            Self::reader_inner,
        )
    }

    /// Create a new reader of given path with additional options.
    ///
    /// # Examples
    ///
    /// Create a reader with a specific version ID:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use opendal::options;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op
    ///     .reader_options("path/to/file", options::ReaderOptions {
    ///         version: Some("version_id".to_string()),
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// // Read the first 10 bytes of the file
    /// let data = r.read(0..10).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn reader_options(&self, path: &str, opts: options::ReaderOptions) -> Result<Reader> {
        let path = normalize_path(path);
        Self::reader_inner(self.inner().clone(), path, opts).await
    }

    /// Allow this unused async since we don't want
    /// to change our public API.
    #[allow(clippy::unused_async)]
    #[inline]
    async fn reader_inner(
        acc: Accessor,
        path: String,
        options: options::ReaderOptions,
    ) -> Result<Reader> {
        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "read path is a directory")
                    .with_operation("Operator::reader")
                    .with_context("service", acc.info().scheme())
                    .with_context("path", path),
            );
        }

        let (args, opts) = options.into();
        let context = ReadContext::new(acc, path, args, opts);
        Ok(Reader::new(context))
    }

    /// Write all data to the specified path at once.
    ///
    /// # Notes
    ///
    /// Visit [`performance::concurrent_write`][crate::docs::performance::concurrent_write] for more details on concurrent writes.
    ///
    /// ## Extra Options
    ///
    /// [`Operator::write`] is a simplified method that does not include additional options.
    /// For advanced features such as `chunk` and `concurrent`, use [`Operator::write_with`] or [`Operator::write_options`] instead.
    ///
    /// ## Streaming Write
    ///
    /// This method executes a single bulk write operation. For more precise memory management
    /// or to write data in a streaming fashion, use [`Operator::writer`] instead.
    ///
    /// ## Multipart Uploads
    ///
    /// OpenDAL offers multipart upload capabilities through the [`Writer`] abstraction,
    /// automatically managing all upload details for you. You can fine-tune the upload process
    /// by adjusting the `chunk` size and the number of `concurrent` operations using [`Operator::writer_with`].
    ///
    /// # Examples
    ///
    /// ```
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
    pub async fn write(&self, path: &str, bs: impl Into<Buffer>) -> Result<Metadata> {
        let bs = bs.into();
        self.write_with(path, bs).await
    }

    /// Write all data to the specified path at once with additional options.
    ///
    /// # Notes
    ///
    /// Visit [`performance::concurrent_write`][crate::docs::performance::concurrent_write] for more details on concurrent writes.
    ///
    /// ## Streaming Write
    ///
    /// This method executes a single bulk write operation. For more precise memory management
    /// or to write data in a streaming fashion, use [`Operator::writer`] instead.
    ///
    /// ## Multipart Uploads
    ///
    /// OpenDAL offers multipart upload capabilities through the [`Writer`] abstraction,
    /// automatically managing all upload details for you. You can fine-tune the upload process
    /// by adjusting the `chunk` size and the number of `concurrent` operations using [`Operator::writer_with`].
    ///
    /// # Options
    ///
    /// Visit [`options::WriteOptions`] for all available options.
    ///
    /// # Examples
    ///
    /// Write data to a file only when it does not already exist:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// let _ = op
    ///     .write_with("path/to/file", vec![0; 4096])
    ///     .if_not_exists(true)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn write_with(
        &self,
        path: &str,
        bs: impl Into<Buffer>,
    ) -> FutureWrite<impl Future<Output = Result<Metadata>>> {
        let path = normalize_path(path);
        let bs = bs.into();

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (options::WriteOptions::default(), bs),
            |inner, path, (opts, bs)| Self::write_inner(inner, path, bs, opts),
        )
    }

    /// Write all data to the specified path at once with additional options.
    ///
    /// # Notes
    ///
    /// Visit [`performance::concurrent_write`][crate::docs::performance::concurrent_write] for more details on concurrent writes.
    ///
    /// ## Streaming Write
    ///
    /// This method executes a single bulk write operation. For more precise memory management
    /// or to write data in a streaming fashion, use [`Operator::writer`] instead.
    ///
    /// ## Multipart Uploads
    ///
    /// OpenDAL offers multipart upload capabilities through the [`Writer`] abstraction,
    /// automatically managing all upload details for you. You can fine-tune the upload process
    /// by adjusting the `chunk` size and the number of `concurrent` operations using [`Operator::writer_with`].
    ///
    /// # Examples
    ///
    /// Write data to a file only when it does not already exist:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use opendal::options;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// let _ = op
    ///     .write_options("path/to/file", vec![0; 4096], options::WriteOptions {
    ///         if_not_exists: true,
    ///         ..Default::default()
    ///     })
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_options(
        &self,
        path: &str,
        bs: impl Into<Buffer>,
        opts: options::WriteOptions,
    ) -> Result<Metadata> {
        let path = normalize_path(path);
        Self::write_inner(self.inner().clone(), path, bs.into(), opts).await
    }

    #[inline]
    async fn write_inner(
        acc: Accessor,
        path: String,
        bs: Buffer,
        opts: options::WriteOptions,
    ) -> Result<Metadata> {
        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "write path is a directory")
                    .with_operation("Operator::write")
                    .with_context("service", acc.info().scheme())
                    .with_context("path", &path),
            );
        }

        let (args, opts) = opts.into();
        let context = WriteContext::new(acc, path, args, opts);
        let mut w = Writer::new(context).await?;
        w.write(bs).await?;
        w.close().await
    }

    /// Create a new writer of given path.
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
    /// [`Operator::writer`] is a simplified version that does not include additional options.
    /// For advanced features such as `chunk` and `concurrent`, use [`Operator::writer_with`] or [`Operator::writer_options`] instead.
    ///
    /// # Examples
    ///
    /// ```
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

    /// Create a new writer of given path with additional options.
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
    /// Visit [`performance::concurrent_write`][crate::docs::performance::concurrent_write] for more details on concurrent writes.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op
    ///     .writer_with("path/to/file")
    ///     .chunk(4 * 1024 * 1024)
    ///     .concurrent(8)
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
            options::WriteOptions::default(),
            Self::writer_inner,
        )
    }

    /// Create a new writer of given path with additional options.
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
    /// Visit [`performance::concurrent_write`][crate::docs::performance::concurrent_write] for more details on concurrent writes.
    ///
    /// # Examples
    ///
    /// Write data to a file in 4MiB chunk size and at 8 concurrency:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut w = op
    ///     .writer_with("path/to/file")
    ///     .chunk(4 * 1024 * 1024)
    ///     .concurrent(8)
    ///     .await?;
    /// w.write(vec![0; 4096]).await?;
    /// w.write(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn writer_options(&self, path: &str, opts: options::WriteOptions) -> Result<Writer> {
        let path = normalize_path(path);
        Self::writer_inner(self.inner().clone(), path, opts).await
    }

    #[inline]
    async fn writer_inner(
        acc: Accessor,
        path: String,
        opts: options::WriteOptions,
    ) -> Result<Writer> {
        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "write path is a directory")
                    .with_operation("Operator::writer")
                    .with_context("service", acc.info().scheme())
                    .with_context("path", &path),
            );
        }

        let (args, opts) = opts.into();
        let context = WriteContext::new(acc, path, args, opts);
        let w = Writer::new(context).await?;
        Ok(w)
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

    /// Copy a file from `from` to `to` with additional options.
    ///
    /// # Notes
    ///
    /// - `from` and `to` must be a file.
    /// - If `from` and `to` are the same, an `IsSameFile` error will occur.
    /// - `copy` is idempotent. For same `from` and `to` input, the result will be the same.
    ///
    /// # Options
    ///
    /// Visit [`options::CopyOptions`] for all available options.
    ///
    /// # Examples
    ///
    /// Copy a file only if the destination doesn't exist:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// op.copy_with("path/to/file", "path/to/file2")
    ///     .if_not_exists(true)
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn copy_with(&self, from: &str, to: &str) -> FutureCopy<impl Future<Output = Result<()>>> {
        let from = normalize_path(from);
        let to = normalize_path(to);

        OperatorFuture::new(
            self.inner().clone(),
            from,
            (options::CopyOptions::default(), to),
            Self::copy_inner,
        )
    }

    /// Copy a file from `from` to `to` with additional options.
    ///
    /// # Notes
    ///
    /// - `from` and `to` must be a file.
    /// - If `from` and `to` are the same, an `IsSameFile` error will occur.
    /// - `copy` is idempotent. For same `from` and `to` input, the result will be the same.
    ///
    /// # Options
    ///
    /// Check [`options::CopyOptions`] for all available options.
    ///
    /// # Examples
    ///
    /// Copy a file only if the destination doesn't exist:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// # use opendal::options::CopyOptions;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut opts = CopyOptions::default();
    /// opts.if_not_exists = true;
    /// op.copy_options("path/to/file", "path/to/file2", opts).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn copy_options(
        &self,
        from: &str,
        to: &str,
        opts: impl Into<options::CopyOptions>,
    ) -> Result<()> {
        let from = normalize_path(from);
        let to = normalize_path(to);
        let opts = opts.into();

        Self::copy_inner(self.inner().clone(), from, (opts, to)).await
    }

    async fn copy_inner(
        acc: Accessor,
        from: String,
        (opts, to): (options::CopyOptions, String),
    ) -> Result<()> {
        if !validate_path(&from, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "from path is a directory")
                    .with_operation("Operator::copy")
                    .with_context("service", acc.info().scheme())
                    .with_context("from", from),
            );
        }

        if !validate_path(&to, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "to path is a directory")
                    .with_operation("Operator::copy")
                    .with_context("service", acc.info().scheme())
                    .with_context("to", to),
            );
        }

        if from == to {
            return Err(
                Error::new(ErrorKind::IsSameFile, "from and to paths are same")
                    .with_operation("Operator::copy")
                    .with_context("service", acc.info().scheme())
                    .with_context("from", &from)
                    .with_context("to", &to),
            );
        }

        let mut op = OpCopy::new();
        if opts.if_not_exists {
            op = op.with_if_not_exists(true);
        }

        acc.copy(&from, &to, op).await.map(|_| ())
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

    /// Delete the given path with additional options.
    ///
    /// # Notes
    ///
    /// - Deleting a file that does not exist won't return errors.
    ///
    /// # Options
    ///
    /// Visit [`options::DeleteOptions`] for all available options.
    ///
    /// # Examples
    ///
    /// Delete a specific version of a file:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    ///
    /// # async fn test(op: Operator, version: &str) -> Result<()> {
    /// op.delete_with("path/to/file").version(version).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete_with(&self, path: &str) -> FutureDelete<impl Future<Output = Result<()>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            options::DeleteOptions::default(),
            Self::delete_inner,
        )
    }

    /// Delete the given path with additional options.
    ///
    /// # Notes
    ///
    /// - Deleting a file that does not exist won't return errors.
    ///
    /// # Examples
    ///
    /// Delete a specific version of a file:
    ///
    /// ```
    /// # use opendal::Result;
    /// # use opendal::Operator;
    /// use opendal::options;
    ///
    /// # async fn test(op: Operator, version: &str) -> Result<()> {
    /// op.delete_options("path/to/file", options::DeleteOptions {
    ///     version: Some(version.to_string()),
    ///     ..Default::default()
    /// })
    /// .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete_options(&self, path: &str, opts: options::DeleteOptions) -> Result<()> {
        let path = normalize_path(path);
        Self::delete_inner(self.inner().clone(), path, opts).await
    }

    async fn delete_inner(acc: Accessor, path: String, opts: options::DeleteOptions) -> Result<()> {
        let (_, mut deleter) = acc.delete_dyn().await?;
        let args = opts.into();
        deleter.delete_dyn(&path, args)?;
        deleter.flush_dyn().await?;
        Ok(())
    }

    /// Delete an infallible iterator of paths.
    ///
    /// Also see:
    ///
    /// - [`Operator::delete_try_iter`]: delete a fallible iterator of paths.
    /// - [`Operator::delete_stream`]: delete an infallible stream of paths.
    /// - [`Operator::delete_try_stream`]: delete a fallible stream of paths.
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

    /// Delete a fallible iterator of paths.
    ///
    /// Also see:
    ///
    /// - [`Operator::delete_iter`]: delete an infallible iterator of paths.
    /// - [`Operator::delete_stream`]: delete an infallible stream of paths.
    /// - [`Operator::delete_try_stream`]: delete a fallible stream of paths.
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
    /// - [`Operator::delete_try_iter`]: delete a fallible iterator of paths.
    /// - [`Operator::delete_try_stream`]: delete a fallible stream of paths.
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

    /// Delete a fallible stream of paths.
    ///
    /// Also see:
    ///
    /// - [`Operator::delete_iter`]: delete an infallible iterator of paths.
    /// - [`Operator::delete_try_iter`]: delete a fallible iterator of paths.
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

    /// Remove the path and all nested dirs and files recursively.
    ///
    /// # Deprecated
    ///
    /// This method is deprecated since v0.55.0. Use [`Operator::delete_try_stream`] with
    /// [`Operator::lister_with`] instead.
    ///
    /// ## Migration Example
    ///
    /// Instead of:
    /// ```ignore
    /// op.remove_all("path/to/dir").await?;
    /// ```
    ///
    /// Use:
    /// ```ignore
    /// let lister = op.lister_with("path/to/dir").recursive(true).await?;
    /// op.delete_try_stream(lister).await?;
    /// ```
    ///
    /// Or use [`Deleter`] for more control:
    /// ```ignore
    /// let mut deleter = op.deleter().await?;
    /// let lister = op.lister_with("path/to/dir").recursive(true).await?;
    /// deleter.delete_try_stream(lister).await?;
    /// deleter.close().await?;
    /// ```
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
    #[deprecated(
        since = "0.55.0",
        note = "Use `delete_try_stream` with `lister_with().recursive(true)` instead"
    )]
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

    /// List entries in the parent directory that start with the specified `path`.
    ///
    /// # Notes
    ///
    /// ## Recursively List
    ///
    /// This function only reads the immediate children of the specified directory.
    /// To list all entries recursively, use `Operator::list_with("path").recursive(true)` instead.
    ///
    /// ## Streaming List
    ///
    /// This function reads all entries in the specified directory. If the directory contains many entries, this process may take a long time and use significant memory.
    ///
    /// To prevent this, consider using [`Operator::lister`] to stream the entries instead.
    ///
    /// # Examples
    ///
    /// This example will list all entries under the dir `path/to/dir/`.
    ///
    /// ```
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
    pub async fn list(&self, path: &str) -> Result<Vec<Entry>> {
        self.list_with(path).await
    }

    /// List entries in the parent directory that start with the specified `path` with additional options.
    ///
    /// # Notes
    ///
    /// ## Streaming List
    ///
    /// This function reads all entries in the specified directory. If the directory contains many entries, this process may take a long time and use significant memory.
    ///
    /// To prevent this, consider using [`Operator::lister`] to stream the entries instead.
    ///
    /// # Options
    ///
    /// Visit [`options::ListOptions`] for all available options.
    ///
    /// # Examples
    ///
    /// This example will list all entries recursively under the prefix `path/to/prefix`.
    ///
    /// ```
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
            options::ListOptions::default(),
            Self::list_inner,
        )
    }

    /// List entries in the parent directory that start with the specified `path` with additional options.
    ///
    /// # Notes
    ///
    /// ## Streaming List
    ///
    /// This function reads all entries in the specified directory. If the directory contains many entries, this process may take a long time and use significant memory.
    ///
    /// To prevent this, consider using [`Operator::lister`] to stream the entries instead.
    ///
    /// # Options
    ///
    /// Visit [`options::ListOptions`] for all available options.
    ///
    /// # Examples
    ///
    /// This example will list all entries recursively under the prefix `path/to/prefix`.
    ///
    /// ```
    /// # use anyhow::Result;
    /// use opendal::options;
    /// use opendal::EntryMode;
    /// use opendal::Operator;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut entries = op
    ///     .list_options("path/to/prefix", options::ListOptions {
    ///         recursive: true,
    ///         ..Default::default()
    ///     })
    ///     .await?;
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
    pub async fn list_options(&self, path: &str, opts: options::ListOptions) -> Result<Vec<Entry>> {
        let path = normalize_path(path);
        Self::list_inner(self.inner().clone(), path, opts).await
    }

    #[inline]
    async fn list_inner(
        acc: Accessor,
        path: String,
        opts: options::ListOptions,
    ) -> Result<Vec<Entry>> {
        let args = opts.into();
        let lister = Lister::create(acc, &path, args).await?;
        lister.try_collect().await
    }

    /// Create a new lister to list entries that starts with given `path` in parent dir.
    ///
    /// # Notes
    ///
    /// ## Recursively list
    ///
    /// This function only reads the immediate children of the specified directory.
    /// To retrieve all entries recursively, use [`Operator::lister_with`] with `recursive(true)` instead.
    ///
    /// # Examples
    ///
    /// ```
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

    /// Create a new lister to list entries that starts with given `path` in parent dir with additional options.
    ///
    /// # Options
    ///
    /// Visit [`options::ListOptions`] for all available options.
    ///
    /// # Examples
    ///
    /// ## List all files recursively
    ///
    /// ```
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
            options::ListOptions::default(),
            Self::lister_inner,
        )
    }

    /// Create a new lister to list entries that starts with given `path` in parent dir with additional options.
    ///
    /// # Examples
    ///
    /// ## List all files recursively
    ///
    /// ```
    /// # use anyhow::Result;
    /// use futures::TryStreamExt;
    /// use opendal::options;
    /// use opendal::EntryMode;
    /// use opendal::Operator;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut lister = op
    ///     .lister_options("path/to/dir/", options::ListOptions {
    ///         recursive: true,
    ///         ..Default::default()
    ///     })
    ///     .await?;
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
    pub async fn lister_options(&self, path: &str, opts: options::ListOptions) -> Result<Lister> {
        let path = normalize_path(path);
        Self::lister_inner(self.inner().clone(), path, opts).await
    }

    #[inline]
    async fn lister_inner(
        acc: Accessor,
        path: String,
        opts: options::ListOptions,
    ) -> Result<Lister> {
        let args = opts.into();
        let lister = Lister::create(acc, &path, args).await?;
        Ok(lister)
    }
}

/// Operator presign API.
impl Operator {
    /// Presign an operation for stat(head).
    ///
    /// # Example
    ///
    /// ```
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
    /// ```
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
            (options::StatOptions::default(), expire),
            Self::presign_stat_inner,
        )
    }

    /// Presign an operation for stat(head) with additional options.
    ///
    /// # Options
    ///
    /// Visit [`options::StatOptions`] for all available options.
    ///
    /// # Example
    ///
    /// ```
    /// use anyhow::Result;
    /// use opendal::Operator;
    /// use opendal::options;
    /// use std::time::Duration;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_stat_options(
    ///         "test",
    ///         Duration::from_secs(3600),
    ///         options::StatOptions {
    ///             if_match: Some("<etag>".to_string()),
    ///             ..Default::default()
    ///         }
    ///     ).await?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub async fn presign_stat_options(
        &self,
        path: &str,
        expire: Duration,
        opts: options::StatOptions,
    ) -> Result<PresignedRequest> {
        let path = normalize_path(path);
        Self::presign_stat_inner(self.inner().clone(), path, (opts, expire)).await
    }

    #[inline]
    async fn presign_stat_inner(
        acc: Accessor,
        path: String,
        (opts, expire): (options::StatOptions, Duration),
    ) -> Result<PresignedRequest> {
        let op = OpPresign::new(OpStat::from(opts), expire);
        let rp = acc.presign(&path, op).await?;
        Ok(rp.into_presigned_request())
    }

    /// Presign an operation for read.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// `presign_read` is a wrapper of [`Self::presign_read_with`] without any options. To use
    /// extra options like `override_content_disposition`, please use [`Self::presign_read_with`] or
    /// [`Self::presign_read_options] instead.
    ///
    /// # Example
    ///
    /// ```
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
    /// Visit [`options::ReadOptions`] for all available options.
    ///
    /// # Example
    ///
    /// ```
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
            (options::ReadOptions::default(), expire),
            Self::presign_read_inner,
        )
    }

    /// Presign an operation for read with additional options.
    ///
    /// # Options
    ///
    /// Visit [`options::ReadOptions`] for all available options.
    ///
    /// # Example
    ///
    /// ```
    /// use anyhow::Result;
    /// use opendal::Operator;
    /// use opendal::options;
    /// use std::time::Duration;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_read_options(
    ///         "file",
    ///         Duration::from_secs(3600),
    ///         options::ReadOptions {
    ///             override_content_disposition: Some("attachment; filename=\"othertext.txt\"".to_string()),
    ///             ..Default::default()
    ///         }
    ///     ).await?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub async fn presign_read_options(
        &self,
        path: &str,
        expire: Duration,
        opts: options::ReadOptions,
    ) -> Result<PresignedRequest> {
        let path = normalize_path(path);
        Self::presign_read_inner(self.inner().clone(), path, (opts, expire)).await
    }

    #[inline]
    async fn presign_read_inner(
        acc: Accessor,
        path: String,
        (opts, expire): (options::ReadOptions, Duration),
    ) -> Result<PresignedRequest> {
        let (op_read, _) = opts.into();
        let op = OpPresign::new(op_read, expire);
        let rp = acc.presign(&path, op).await?;
        Ok(rp.into_presigned_request())
    }

    /// Presign an operation for write.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// `presign_write` is a wrapper of [`Self::presign_write_with`] without any options. To use
    /// extra options like `content_type`, please use [`Self::presign_write_with`] or
    /// [`Self::presign_write_options`] instead.
    ///
    /// # Example
    ///
    /// ```
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
    /// Visit [`options::WriteOptions`] for all available options.
    ///
    /// # Example
    ///
    /// ```
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
            (options::WriteOptions::default(), expire),
            Self::presign_write_inner,
        )
    }

    /// Presign an operation for write with additional options.
    ///
    /// # Options
    ///
    /// Check [`options::WriteOptions`] for all available options.
    ///
    /// # Example
    ///
    /// ```
    /// use anyhow::Result;
    /// use opendal::Operator;
    /// use opendal::options;
    /// use std::time::Duration;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_write_options(
    ///         "file",
    ///         Duration::from_secs(3600),
    ///         options::WriteOptions {
    ///             content_type: Some("application/json".to_string()),
    ///             cache_control: Some("max-age=3600".to_string()),
    ///             if_not_exists: true,
    ///             ..Default::default()
    ///         }
    ///     ).await?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub async fn presign_write_options(
        &self,
        path: &str,
        expire: Duration,
        opts: options::WriteOptions,
    ) -> Result<PresignedRequest> {
        let path = normalize_path(path);
        Self::presign_write_inner(self.inner().clone(), path, (opts, expire)).await
    }

    #[inline]
    async fn presign_write_inner(
        acc: Accessor,
        path: String,
        (opts, expire): (options::WriteOptions, Duration),
    ) -> Result<PresignedRequest> {
        let (op_write, _) = opts.into();
        let op = OpPresign::new(op_write, expire);
        let rp = acc.presign(&path, op).await?;
        Ok(rp.into_presigned_request())
    }

    /// Presign an operation for delete.
    ///
    /// # Notes
    ///
    /// ## Extra Options
    ///
    /// `presign_delete` is a wrapper of [`Self::presign_delete_with`] without any options.
    ///
    /// # Example
    ///
    /// ```
    /// use std::time::Duration;
    ///
    /// use anyhow::Result;
    /// use opendal::Operator;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op
    ///         .presign_delete("test.txt", Duration::from_secs(3600))
    ///         .await?;
    ///     Ok(())
    /// }
    /// ```
    ///
    /// - `signed_req.method()`: `DELETE`
    /// - `signed_req.uri()`: `https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>`
    /// - `signed_req.headers()`: `{ "host": "s3.amazonaws.com" }`
    ///
    /// We can delete file as this file via `curl` or other tools without credential:
    ///
    /// ```shell
    /// curl -X DELETE "https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>"
    /// ```
    pub async fn presign_delete(&self, path: &str, expire: Duration) -> Result<PresignedRequest> {
        self.presign_delete_with(path, expire).await
    }

    /// Presign an operation for delete without extra options.
    pub fn presign_delete_with(
        &self,
        path: &str,
        expire: Duration,
    ) -> FuturePresignDelete<impl Future<Output = Result<PresignedRequest>>> {
        let path = normalize_path(path);

        OperatorFuture::new(
            self.inner().clone(),
            path,
            (options::DeleteOptions::default(), expire),
            Self::presign_delete_inner,
        )
    }

    /// Presign an operation for delete with additional options.
    ///
    /// # Options
    ///
    /// Visit [`options::DeleteOptions`] for all available options.
    ///
    /// # Example
    ///
    /// ```
    /// use anyhow::Result;
    /// use opendal::Operator;
    /// use opendal::options;
    /// use std::time::Duration;
    ///
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_delete_options(
    ///         "path/to/file",
    ///         Duration::from_secs(3600),
    ///         options::DeleteOptions {
    ///             ..Default::default()
    ///         }
    ///     ).await?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub async fn presign_delete_options(
        &self,
        path: &str,
        expire: Duration,
        opts: options::DeleteOptions,
    ) -> Result<PresignedRequest> {
        let path = normalize_path(path);
        Self::presign_delete_inner(self.inner().clone(), path, (opts, expire)).await
    }

    #[inline]
    async fn presign_delete_inner(
        acc: Accessor,
        path: String,
        (opts, expire): (options::DeleteOptions, Duration),
    ) -> Result<PresignedRequest> {
        let op = OpPresign::new(OpDelete::from(opts), expire);
        let rp = acc.presign(&path, op).await?;
        Ok(rp.into_presigned_request())
    }
}
