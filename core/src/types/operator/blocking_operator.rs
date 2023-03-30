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

use std::io::Read;
use std::ops::RangeBounds;

use bytes::Bytes;
use flagset::FlagSet;

use crate::ops::*;
use crate::raw::*;
use crate::*;

/// BlockingOperator is the entry for all public blocking APIs.
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
/// use opendal::BlockingOperator;
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
///     // Build an `BlockingOperator` to start operating the storage.
///     let _: BlockingOperator = Operator::new(builder)?.finish().blocking();
///
///     Ok(())
/// }
/// ```
#[derive(Clone, Debug)]
pub struct BlockingOperator {
    accessor: FusedAccessor,

    limit: usize,
}

impl BlockingOperator {
    pub(super) fn inner(&self) -> &FusedAccessor {
        &self.accessor
    }

    /// create a new blocking operator from inner accessor.
    ///
    /// # Note
    /// default batch limit is 1000.
    pub(crate) fn from_inner(accessor: FusedAccessor) -> Self {
        let limit = accessor.info().max_batch_operations().unwrap_or(1000);
        Self { accessor, limit }
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
    /// use opendal::BlockingOperator;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: BlockingOperator) -> Result<()> {
    /// let info = op.info();
    /// # Ok(())
    /// # }
    /// ```
    pub fn info(&self) -> OperatorInfo {
        OperatorInfo::new(self.accessor.info())
    }
}

/// # Operator blocking API.
impl BlockingOperator {
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
    /// # use opendal::BlockingOperator;
    /// use opendal::ErrorKind;
    /// #
    /// # fn test(op: BlockingOperator) -> Result<()> {
    /// if let Err(e) = op.stat("test") {
    ///     if e.kind() == ErrorKind::NotFound {
    ///         println!("file not exist")
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn stat(&self, path: &str) -> Result<Metadata> {
        let path = normalize_path(path);

        let rp = self.inner().blocking_stat(&path, OpStat::new())?;
        let meta = rp.into_metadata();

        Ok(meta)
    }

    /// Get current metadata with cache in blocking way.
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
    /// - Want detect the outside changes of file.
    /// - Don't want to read from cached file metadata.
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
    /// # use opendal::BlockingOperator;
    /// use opendal::Entry;
    ///
    /// # fn test(op: BlockingOperator, entry: Entry) -> Result<()> {
    /// let meta = op.metadata(&entry, None)?;
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
    /// # use opendal::BlockingOperator;
    /// use opendal::Entry;
    /// use opendal::Metakey;
    ///
    /// # fn test(op: BlockingOperator, entry: Entry) -> Result<()> {
    /// let meta = op.metadata(&entry, { Metakey::ContentLength | Metakey::ContentType })?;
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
    /// # use opendal::BlockingOperator;
    /// use opendal::Entry;
    /// use opendal::Metakey;
    ///
    /// # fn test(op: BlockingOperator, entry: Entry) -> Result<()> {
    /// let meta = op.metadata(&entry, { Metakey::Complete })?;
    /// // content length MUST be correct.
    /// let _ = meta.content_length();
    /// // etag MUST be correct.
    /// let _ = meta.etag();
    /// # Ok(())
    /// # }
    /// ```
    pub fn metadata(&self, entry: &Entry, flags: impl Into<FlagSet<Metakey>>) -> Result<Metadata> {
        // Check if cached metadata saticifies the query.
        if let Some(meta) = entry.metadata() {
            if meta.bit().contains(flags) || meta.bit().contains(Metakey::Complete) {
                return Ok(meta.clone());
            }
        }

        // Else request from backend..
        let meta = self.stat(entry.path())?;
        Ok(meta)
    }

    /// Check if this path exists or not.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use opendal::BlockingOperator;
    /// fn test(op: BlockingOperator) -> Result<()> {
    ///     let _ = op.is_exist("test")?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn is_exist(&self, path: &str) -> Result<bool> {
        let r = self.stat(path);
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
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::BlockingOperator;
    /// # use futures::TryStreamExt;
    /// # async fn test(op: BlockingOperator) -> Result<()> {
    /// op.create_dir("path/to/dir/")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn create_dir(&self, path: &str) -> Result<()> {
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
            .blocking_create(&path, OpCreate::new(EntryMode::DIR))?;

        Ok(())
    }

    /// Read the whole path into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`BlockingOperator::reader`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::BlockingOperator;
    /// #
    /// # fn test(op: BlockingOperator) -> Result<()> {
    /// let bs = op.read("path/to/file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn read(&self, path: &str) -> Result<Vec<u8>> {
        self.range_read(path, ..)
    }

    /// Read the specified range of path into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`BlockingOperator::range_reader`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::BlockingOperator;
    /// # use futures::TryStreamExt;
    /// # use opendal::Scheme;
    /// # async fn test(op: BlockingOperator) -> Result<()> {
    /// let bs = op.range_read("path/to/file", 1024..2048)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn range_read(&self, path: &str, range: impl RangeBounds<u64>) -> Result<Vec<u8>> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "read path is a directory")
                    .with_operation("BlockingOperator::range_read")
                    .with_context("service", self.info().scheme().into_static())
                    .with_context("path", &path),
            );
        }

        let br = BytesRange::from(range);
        let (rp, mut s) = self
            .inner()
            .blocking_read(&path, OpRead::new().with_range(br))?;

        let mut buffer = Vec::with_capacity(rp.into_metadata().content_length() as usize);
        s.read_to_end(&mut buffer).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "blocking range read failed")
                .with_operation("BlockingOperator::range_read")
                .with_context("service", self.info().scheme().into_static())
                .with_context("path", path)
                .with_context("range", br.to_string())
                .set_source(err)
        })?;

        Ok(buffer)
    }

    /// Create a new reader which can read the whole path.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::BlockingOperator;
    /// # use futures::TryStreamExt;
    /// # fn test(op: BlockingOperator) -> Result<()> {
    /// let r = op.reader("path/to/file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn reader(&self, path: &str) -> Result<BlockingReader> {
        self.range_reader(path, ..)
    }

    /// Create a new reader which can read the specified range.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::BlockingOperator;
    /// # use futures::TryStreamExt;
    /// # fn test(op: BlockingOperator) -> Result<()> {
    /// let r = op.range_reader("path/to/file", 1024..2048)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn range_reader(&self, path: &str, range: impl RangeBounds<u64>) -> Result<BlockingReader> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "read path is a directory")
                    .with_operation("BlockingOperator::range_reader")
                    .with_context("service", self.info().scheme().into_static())
                    .with_context("path", &path),
            );
        }

        let op = OpRead::new().with_range(range.into());

        BlockingReader::create(self.inner().clone(), &path, op)
    }

    /// Write bytes into given path.
    ///
    /// # Notes
    ///
    /// - Write will make sure all bytes has been written, or an error will be returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::BlockingOperator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # fn test(op: BlockingOperator) -> Result<()> {
    /// op.write("path/to/file", vec![0; 4096])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn write(&self, path: &str, bs: impl Into<Bytes>) -> Result<()> {
        self.write_with(path, OpWrite::new(), bs)
    }

    /// Write data with option described in OpenDAL [rfc-0661](../../docs/rfcs/0661-path-in-accessor.md)
    ///
    /// # Notes
    ///
    /// - Write will make sure all bytes has been written, or an error will be returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use opendal::BlockingOperator;
    /// use bytes::Bytes;
    /// use opendal::ops::OpWrite;
    ///
    /// # async fn test(op: BlockingOperator) -> Result<()> {
    /// let bs = b"hello, world!".to_vec();
    /// let ow = OpWrite::new().with_content_type("text/plain");
    /// let _ = op.write_with("hello.txt", ow, bs)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn write_with(&self, path: &str, args: OpWrite, bs: impl Into<Bytes>) -> Result<()> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "write path is a directory")
                    .with_operation("BlockingOperator::write_with")
                    .with_context("service", self.info().scheme().into_static())
                    .with_context("path", &path),
            );
        }

        let (_, mut w) = self.inner().blocking_write(&path, args)?;
        w.write(bs.into())?;
        w.close()?;

        Ok(())
    }

    /// Write multiple bytes into given path.
    ///
    /// # Notes
    ///
    /// - Write will make sure all bytes has been written, or an error will be returned.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::BlockingOperator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # fn test(op: BlockingOperator) -> Result<()> {
    /// let mut w = op.writer("path/to/file")?;
    /// w.append(vec![0; 4096])?;
    /// w.append(vec![1; 4096])?;
    /// w.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn writer(&self, path: &str) -> Result<BlockingWriter> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::FILE) {
            return Err(
                Error::new(ErrorKind::IsADirectory, "write path is a directory")
                    .with_operation("BlockingOperator::writer")
                    .with_context("service", self.info().scheme().into_static())
                    .with_context("path", &path),
            );
        }

        let op = OpWrite::default().with_append();
        BlockingWriter::create(self.inner().clone(), &path, op)
    }

    /// Delete given path.
    ///
    /// # Notes
    ///
    /// - Delete not existing error won't return errors.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::BlockingOperator;
    /// # fn test(op: BlockingOperator) -> Result<()> {
    /// op.delete("path/to/file")?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn delete(&self, path: &str) -> Result<()> {
        let path = normalize_path(path);

        let _ = self.inner().blocking_delete(&path, OpDelete::new())?;

        Ok(())
    }

    /// List current dir path.
    ///
    /// This function will create a new handle to list entries.
    ///
    /// An error will be returned if path doesn't end with `/`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use futures::io;
    /// # use opendal::BlockingOperator;
    /// # use opendal::EntryMode;
    /// # fn test(op: BlockingOperator) -> Result<()> {
    /// let mut ds = op.list("path/to/dir/")?;
    /// while let Some(mut de) = ds.next() {
    ///     let meta = op.metadata(&de?, {
    ///         use opendal::Metakey::*;
    ///         Mode
    ///     })?;
    ///     match meta.mode() {
    ///         EntryMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         EntryMode::DIR => {
    ///             println!("Handling dir like start a new list via de.path()")
    ///         }
    ///         EntryMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn list(&self, path: &str) -> Result<BlockingLister> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::DIR) {
            return Err(Error::new(
                ErrorKind::NotADirectory,
                "the path trying to list should end with `/`",
            )
            .with_operation("BlockingOperator::list")
            .with_context("service", self.info().scheme().into_static())
            .with_context("path", &path));
        }

        let (_, pager) = self.inner().blocking_list(&path, OpList::new())?;
        Ok(BlockingLister::new(pager))
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
    /// # use opendal::Result;
    /// # use futures::io;
    /// # use opendal::BlockingOperator;
    /// # use opendal::EntryMode;
    /// # fn test(op: BlockingOperator) -> Result<()> {
    /// let mut ds = op.list("path/to/dir/")?;
    /// while let Some(mut de) = ds.next() {
    ///     let meta = op.metadata(&de?, {
    ///         use opendal::Metakey::*;
    ///         Mode
    ///     })?;
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
    pub fn scan(&self, path: &str) -> Result<BlockingLister> {
        let path = normalize_path(path);

        if !validate_path(&path, EntryMode::DIR) {
            return Err(Error::new(
                ErrorKind::NotADirectory,
                "the path trying to scan should end with `/`",
            )
            .with_operation("BlockingOperator::scan")
            .with_context("service", self.info().scheme().into_static())
            .with_context("path", path));
        }

        let (_, pager) = self.inner().blocking_scan(&path, OpScan::new())?;
        Ok(BlockingLister::new(pager))
    }
}
