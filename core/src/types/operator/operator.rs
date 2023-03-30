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

use std::ops::RangeBounds;

use bytes::Bytes;
use flagset::FlagSet;
use futures::stream;
use futures::AsyncReadExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use time::Duration;
use tokio::io::ReadBuf;

use super::BlockingOperator;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Operator is the entry for all public async APIs.
///
/// Read [`concepts`][docs::concepts] for know more about [`Operator`].
///
/// # Examples
///
/// Read more backend init examples in [`services`]
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
    accessor: FusedAccessor,

    limit: usize,
}

/// # Operator basic API.
impl Operator {
    pub(super) fn inner(&self) -> &FusedAccessor {
        &self.accessor
    }

    pub(crate) fn from_inner(accessor: FusedAccessor) -> Self {
        let limit = accessor.info().max_batch_operations().unwrap_or(1000);
        Self { accessor, limit }
    }

    pub(super) fn into_inner(self) -> FusedAccessor {
        self.accessor
    }

    /// Get current operator's limit
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

/// Operator async API.
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
        let mut ds = self.list("/").await?;

        match ds.next().await {
            Some(Err(e)) if e.kind() != ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Get current path's metadata **without cache** directly.
    ///
    /// # Notes
    ///
    /// Use `stat` if you:
    ///
    /// - Want detect the outside changes of path.
    /// - Don't want to read from cached metadata.
    ///
    /// You may want to use `metadata` if you are working with entries
    /// returned by [`Lister`]. It's highly possible that metadata
    /// you want has already been cached.
    ///
    /// # Examples
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
        let path = normalize_path(path);

        let rp = self.inner().stat(&path, OpStat::new()).await?;
        let meta = rp.into_metadata();

        Ok(meta)
    }

    /// Get current metadata with cache.
    ///
    /// `metadata` will check the given query with already cached metadata
    ///  first. And query from storage if not found.
    ///
    /// # Notes
    ///
    /// Use `metadata` if you are working with entries returned by
    /// [`Lister`]. It's highly possible that metadata you want
    /// has already been cached.
    ///
    /// You may want to use `stat`, if you:
    ///
    /// - Want detect the outside changes of path.
    /// - Don't want to read from cached metadata.
    ///
    /// # Behavior
    ///
    /// Visiting not fetched metadata will lead to panic in debug build.
    /// It must be a bug, please fix it instead.
    ///
    /// # Examples
    ///
    /// ## Query already cached metadata
    ///
    /// By query metadata with `None`, we can only query in-memory metadata
    /// cache. In this way, we can make sure that no API call will send.
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use opendal::Operator;
    /// use opendal::Entry;
    /// # #[tokio::main]
    /// # async fn test(op: Operator, entry: Entry) -> Result<()> {
    /// let meta = op.metadata(&entry, None).await?;
    /// // content length COULD be correct.
    /// let _ = meta.content_length();
    /// // etag COULD be correct.
    /// let _ = meta.etag();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Query content length and content type
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use opendal::Operator;
    /// use opendal::Entry;
    /// use opendal::Metakey;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator, entry: Entry) -> Result<()> {
    /// let meta = op
    ///     .metadata(&entry, Metakey::ContentLength | Metakey::ContentType)
    ///     .await?;
    /// // content length MUST be correct.
    /// let _ = meta.content_length();
    /// // etag COULD be correct.
    /// let _ = meta.etag();
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Query all metadata
    ///
    /// By query metadata with `Complete`, we can make sure that we have fetched all metadata of this entry.
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use opendal::Operator;
    /// use opendal::Entry;
    /// use opendal::Metakey;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator, entry: Entry) -> Result<()> {
    /// let meta = op.metadata(&entry, Metakey::Complete).await?;
    /// // content length MUST be correct.
    /// let _ = meta.content_length();
    /// // etag MUST be correct.
    /// let _ = meta.etag();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn metadata(
        &self,
        entry: &Entry,
        flags: impl Into<FlagSet<Metakey>>,
    ) -> Result<Metadata> {
        // Check if cached metadata saticifies the query.
        if let Some(meta) = entry.metadata() {
            if meta.bit().contains(flags) || meta.bit().contains(Metakey::Complete) {
                return Ok(meta.clone());
            }
        }

        // Else request from backend..
        let meta = self.stat(entry.path()).await?;
        Ok(meta)
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

        self.inner()
            .create(&path, OpCreate::new(EntryMode::DIR))
            .await?;

        Ok(())
    }

    /// Read the whole path into a bytes.
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
        self.range_read(path, ..).await
    }

    /// Read the specified range of path into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Operator::range_reader`]
    ///
    /// # Notes
    ///
    /// - The returning content's length may be smaller than the range specified.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = op.range_read("path/to/file", 1024..2048).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn range_read(&self, path: &str, range: impl RangeBounds<u64>) -> Result<Vec<u8>> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "read path is a directory")
                    .with_operation("range_read")
                    .with_context("service", self.inner().info().scheme())
                    .with_context("path", &path),
            );
        }

        let br = BytesRange::from(range);

        let op = OpRead::new().with_range(br);

        let (rp, mut s) = self.inner().read(&path, op).await?;

        let length = rp.into_metadata().content_length() as usize;
        let mut buffer = Vec::with_capacity(length);

        let dst = buffer.spare_capacity_mut();
        let mut buf = ReadBuf::uninit(dst);

        // Safety: the input buffer is created with_capacity(length).
        unsafe { buf.assume_init(length) };

        // TODO: use native read api
        s.read_exact(buf.initialized_mut()).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "read from storage")
                .with_operation("range_read")
                .with_context("service", self.inner().info().scheme().into_static())
                .with_context("path", &path)
                .with_context("range", br.to_string())
                .set_source(err)
        })?;

        // Safety: read_exact makes sure this buffer has been filled.
        unsafe { buffer.set_len(length) }

        Ok(buffer)
    }

    /// Create a new reader which can read the whole path.
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
        self.range_reader(path, ..).await
    }

    /// Create a new reader which can read the specified range.
    ///
    /// # Notes
    ///
    /// - The returning content's length may be smaller than the range specified.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let r = op.range_reader("path/to/file", 1024..2048).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn range_reader(&self, path: &str, range: impl RangeBounds<u64>) -> Result<Reader> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "read path is a directory")
                    .with_operation("Operator::range_reader")
                    .with_context("service", self.info().scheme())
                    .with_context("path", path),
            );
        }

        let op = OpRead::new().with_range(range.into());

        Reader::create(self.inner().clone(), &path, op).await
    }

    /// Write bytes into path.
    ///
    /// # Notes
    ///
    /// - Write will make sure all bytes has been written, or an error will be returned.
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
        self.write_with(path, OpWrite::new(), bs).await
    }

    /// Write multiple bytes into path.
    ///
    /// # Notes
    ///
    /// - Write will make sure all bytes has been written, or an error will be returned.
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
    /// w.append(vec![0; 4096]).await?;
    /// w.append(vec![1; 4096]).await?;
    /// w.close().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn writer(&self, path: &str) -> Result<Writer> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "write path is a directory")
                    .with_operation("Operator::writer")
                    .with_context("service", self.inner().info().scheme().into_static())
                    .with_context("path", &path),
            );
        }

        let op = OpWrite::default().with_append();
        Writer::create(self.inner().clone(), &path, op).await
    }

    /// Write data with extra options.
    ///
    /// # Notes
    ///
    /// - Write will make sure all bytes has been written, or an error will be returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    /// use opendal::ops::OpWrite;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let args = OpWrite::new().with_content_type("text/plain");
    /// let _ = op.write_with("path/to/file", args, bs).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_with(&self, path: &str, args: OpWrite, bs: impl Into<Bytes>) -> Result<()> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "write path is a directory")
                    .with_operation("Operator::write_with")
                    .with_context("service", self.info().scheme().into_static())
                    .with_context("path", &path),
            );
        }

        let (_, mut w) = self.inner().write(&path, args).await?;
        w.write(bs.into()).await?;
        w.close().await?;

        Ok(())
    }

    /// Delete the given path.
    ///
    /// # Notes
    ///
    /// - Delete not existing error won't return errors.
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
        let path = normalize_path(path);

        let _ = self.inner().delete(&path, OpDelete::new()).await?;

        Ok(())
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

    /// remove will given paths.

    /// remove_via will remove files via given stream.
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
        if self.info().can_batch() {
            let mut input = input.map(|v| (v, OpDelete::default())).chunks(self.limit());

            while let Some(batches) = input.next().await {
                let results = self
                    .inner()
                    .batch(OpBatch::new(BatchOperations::Delete(batches)))
                    .await?;

                let BatchedResults::Delete(results) = results.into_results();

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

        let obs = self.scan(path).await?;

        if self.info().can_batch() {
            let mut obs = obs.try_chunks(self.limit());

            while let Some(batches) = obs.next().await {
                let batches = batches
                    .map_err(|err| err.1)?
                    .into_iter()
                    .map(|v| (v.path().to_string(), OpDelete::default()))
                    .collect();

                let results = self
                    .inner()
                    .batch(OpBatch::new(BatchOperations::Delete(batches)))
                    .await?;

                let BatchedResults::Delete(results) = results.into_results();

                // TODO: return error here directly seems not a good idea?
                for (_, result) in results {
                    let _ = result?;
                }
            }
        } else {
            obs.try_for_each(|v| async move { self.delete(v.path()).await })
                .await?;
        }

        Ok(())
    }

    /// List given path.
    ///
    /// This function will create a new handle to list entries.
    ///
    /// An error will be returned if given path doesn't end with `/`.
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
    /// let mut ds = op.list("path/to/dir/").await?;
    /// while let Some(mut de) = ds.try_next().await? {
    ///     let meta = op.metadata(&de, Metakey::Mode).await?;
    ///     match meta.mode() {
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
    pub async fn list(&self, path: &str) -> Result<Lister> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::DIR) {
            return Err(Error::new(
                ErrorKind::NotADirectory,
                "the path trying to list should end with `/`",
            )
            .with_operation("Operator::list")
            .with_context("service", self.info().scheme().into_static())
            .with_context("path", &path));
        }

        let (_, pager) = self.inner().list(&path, OpList::new()).await?;

        Ok(Lister::new(pager))
    }

    /// List dir in flat way.
    ///
    /// This function will create a new handle to list entries.
    ///
    /// An error will be returned if given path doesn't end with `/`.
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
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut ds = op.scan("/path/to/dir/").await?;
    /// while let Some(mut de) = ds.try_next().await? {
    ///     let meta = op.metadata(&de, Metakey::Mode).await?;
    ///     match meta.mode() {
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
    pub async fn scan(&self, path: &str) -> Result<Lister> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::DIR) {
            return Err(Error::new(
                ErrorKind::NotADirectory,
                "the path trying to scan should end with `/`",
            )
            .with_operation("scan")
            .with_context("service", self.info().scheme().into_static())
            .with_context("path", &path));
        }

        let (_, pager) = self.inner().scan(&path, OpScan::new()).await?;

        Ok(Lister::new(pager))
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
    /// use time::Duration;
    ///
    /// #[tokio::main]
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_stat("test",Duration::hours(1))?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub fn presign_stat(&self, path: &str, expire: Duration) -> Result<PresignedRequest> {
        let path = normalize_path(path);

        let op = OpPresign::new(OpStat::new(), expire);

        let rp = self.inner().presign(&path, op)?;
        Ok(rp.into_presigned_request())
    }

    /// Presign an operation for read.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    /// use time::Duration;
    ///
    /// #[tokio::main]
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_read("test.txt", Duration::hours(1))?;
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
    pub fn presign_read(&self, path: &str, expire: Duration) -> Result<PresignedRequest> {
        let path = normalize_path(path);

        let op = OpPresign::new(OpRead::new(), expire);

        let rp = self.inner().presign(&path, op)?;
        Ok(rp.into_presigned_request())
    }

    /// Presign an operation for read option described in OpenDAL [rfc-1735](../../docs/rfcs/1735_operation_extension.md).
    ///
    /// You can pass `OpRead` to this method to specify the content disposition.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    /// use time::Duration;
    /// use opendal::ops::OpRead;
    ///
    /// #[tokio::main]
    /// async fn test(op: Operator) -> Result<()> {
    ///     let args = OpRead::new()
    ///         .with_override_content_disposition("attachment; filename=\"othertext.txt\"");
    ///     let signed_req = op.presign_read_with("test.txt", args, Duration::hours(1))?;
    /// #    Ok(())
    /// # }
    /// ```
    pub fn presign_read_with(
        &self,
        path: &str,
        op: OpRead,
        expire: Duration,
    ) -> Result<PresignedRequest> {
        let path = normalize_path(path);

        let op = OpPresign::new(op, expire);

        let rp = self.inner().presign(&path, op)?;
        Ok(rp.into_presigned_request())
    }

    /// Presign an operation for write.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    /// use time::Duration;
    ///
    /// #[tokio::main]
    /// async fn test(op: Operator) -> Result<()> {
    ///     let signed_req = op.presign_write("test.txt", Duration::hours(1))?;
    /// #    Ok(())
    /// # }
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
    pub fn presign_write(&self, path: &str, expire: Duration) -> Result<PresignedRequest> {
        self.presign_write_with(path, OpWrite::new(), expire)
    }

    /// Presign an operation for write with option described in OpenDAL [rfc-0661](../../docs/rfcs/0661-path-in-accessor.md)
    ///
    /// You can pass `OpWrite` to this method to specify the content length and content type.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::ops::OpWrite;
    /// use opendal::Operator;
    /// use time::Duration;
    ///
    /// #[tokio::main]
    /// async fn test(op: Operator) -> Result<()> {
    ///     let args = OpWrite::new().with_content_type("text/csv");
    ///     let signed_req = op.presign_write_with("test", args, Duration::hours(1))?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub fn presign_write_with(
        &self,
        path: &str,
        op: OpWrite,
        expire: Duration,
    ) -> Result<PresignedRequest> {
        let path = normalize_path(path);

        let op = OpPresign::new(op, expire);

        let rp = self.inner().presign(&path, op)?;
        Ok(rp.into_presigned_request())
    }
}
