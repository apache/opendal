// Copyright 2022 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::fmt::Debug;
use std::io::Read;
use std::ops::RangeBounds;
use std::sync::Arc;

use flagset::FlagSet;
use futures::io::Cursor;
use futures::AsyncReadExt;
use time::Duration;
use tokio::io::ReadBuf;

use super::BlockingObjectLister;
use super::BlockingObjectReader;
use super::ObjectLister;
use crate::ops::*;
use crate::raw::*;
use crate::*;

/// Object is the handler for all object related operations.
///
/// # Notes
///
/// Object will cache part of object metadata that pre-fetch by list or stat
/// operations. It's better to reuse the same object whenever possible.
#[derive(Clone, Debug)]
pub struct Object {
    acc: FusedAccessor,
    path: Arc<String>,

    meta: Option<Arc<ObjectMetadata>>,
}

impl Object {
    /// Creates a new Object with normalized path.
    ///
    /// - All path will be converted into relative path (without any leading `/`)
    /// - Path endswith `/` means it's a dir path.
    /// - Otherwise, it's a file path.
    pub fn new(op: Operator, path: &str) -> Self {
        Self::with(op, path, None)
    }

    pub(crate) fn with(op: Operator, path: &str, meta: Option<ObjectMetadata>) -> Self {
        Self {
            acc: op.inner(),
            path: Arc::new(normalize_path(path)),
            meta: meta.map(Arc::new),
        }
    }

    /// Fetch the operator that used by this object.
    pub fn operator(&self) -> Operator {
        self.acc.clone().into()
    }

    pub(crate) fn accessor(&self) -> FusedAccessor {
        self.acc.clone()
    }

    /// ID of object.
    ///
    /// ID is the unique id of object in the underlying backend. In different backend,
    /// the id could have different meaning.
    ///
    /// For example:
    ///
    /// - In `fs`: id is the absolute path of file, like `/path/to/dir/test_object`.
    /// - In `s3`: id is the full object key, like `path/to/dir/test_object`
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
    ///     let id = op.object("test").id();
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn id(&self) -> String {
        format!("{}{}", self.acc.metadata().root(), self.path)
    }

    /// Path of object. Path is relative to operator's root.
    /// Only valid in current operator.
    ///
    /// The value is the same with `Metadata::path()`.
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
    ///     let path = op.object("test").path();
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Name of object. Name is the last segment of path.
    ///
    /// If this object is a dir, `Name` MUST endswith `/`
    /// Otherwise, `Name` MUST NOT endswith `/`.
    ///
    /// The value is the same with `Metadata::name()`.
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
    ///     let name = op.object("test").name();
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn name(&self) -> &str {
        get_basename(&self.path)
    }

    /// Create an empty object, like using the following linux commands:
    ///
    /// - `touch path/to/file`
    /// - `mkdir path/to/dir/`
    ///
    /// # Behavior
    ///
    /// - Create on existing dir will succeed.
    /// - Create on existing file will overwrite and truncate it.
    ///
    /// # Examples
    ///
    /// ## Create an empty file
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/file");
    /// let _ = o.create().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Create a dir
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/dir/");
    /// let _ = o.create().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create(&self) -> Result<()> {
        let _ = if self.path.ends_with('/') {
            self.acc
                .create(self.path(), OpCreate::new(ObjectMode::DIR))
                .await?
        } else {
            self.acc
                .create(self.path(), OpCreate::new(ObjectMode::FILE))
                .await?
        };

        Ok(())
    }

    /// Create an empty object, like using the following linux commands:
    ///
    /// - `touch path/to/file`
    /// - `mkdir path/to/dir/`
    ///
    /// # Behavior
    ///
    /// - Create on existing dir will succeed.
    /// - Create on existing file will overwrite and truncate it.
    ///
    /// # Examples
    ///
    /// ## Create an empty file
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/file");
    /// let _ = o.blocking_create()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Create a dir
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/dir/");
    /// let _ = o.blocking_create()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_create(&self) -> Result<()> {
        if self.path.ends_with('/') {
            self.acc
                .blocking_create(self.path(), OpCreate::new(ObjectMode::DIR))?;
        } else {
            self.acc
                .blocking_create(self.path(), OpCreate::new(ObjectMode::FILE))?;
        };

        Ok(())
    }

    /// Read the whole object into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Object::reader`]
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/file");
    /// # o.write(vec![0; 4096]).await?;
    /// let bs = o.read().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(&self) -> Result<Vec<u8>> {
        self.range_read(..).await
    }

    /// Read the whole object into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Object::blocking_reader`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// #
    /// # fn test(op: Operator) -> Result<()> {
    /// # let mut o = op.object("path/to/file");
    /// # o.blocking_write(vec![0; 4096])?;
    /// let bs = o.blocking_read()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_read(&self) -> Result<Vec<u8>> {
        self.blocking_range_read(..)
    }

    /// Read the specified range of object into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Object::range_reader`]
    ///
    /// # Notes
    ///
    /// - The returning contnet's length may be smaller than the range specified.
    ///
    /// # Examples
    ///
    /// ```
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/file");
    /// # o.write(vec![0; 4096]).await?;
    /// let bs = o.range_read(1024..2048).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn range_read(&self, range: impl RangeBounds<u64>) -> Result<Vec<u8>> {
        if !validate_path(self.path(), ObjectMode::FILE) {
            return Err(
                Error::new(ErrorKind::ObjectIsADirectory, "read path is a directory")
                    .with_operation("Object:range_read")
                    .with_context("service", self.accessor().metadata().scheme().into_static())
                    .with_context("path", self.path()),
            );
        }

        let br = BytesRange::from(range);

        let op = OpRead::new().with_range(br);

        let (rp, mut s) = self.acc.read(self.path(), op).await?;

        let length = rp.into_metadata().content_length() as usize;
        let mut buffer = Vec::with_capacity(length);

        let dst = buffer.spare_capacity_mut();
        let mut buf = ReadBuf::uninit(dst);
        unsafe { buf.assume_init(length) };

        s.read_exact(buf.initialized_mut()).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "read from storage")
                .with_operation("Object:range_read")
                .with_context("service", self.accessor().metadata().scheme().into_static())
                .with_context("path", self.path())
                .with_context("range", br.to_string())
                .set_source(err)
        })?;

        // Safety: this buffer has been filled.
        unsafe { buffer.set_len(length) }

        Ok(buffer)
    }

    /// Read the specified range of object into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Object::blocking_range_reader`]
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # use opendal::Scheme;
    /// # async fn test(op: Operator) -> Result<()> {
    /// # let mut o = op.object("path/to/file");
    /// # o.blocking_write(vec![0; 4096])?;
    /// let bs = o.blocking_range_read(1024..2048)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_range_read(&self, range: impl RangeBounds<u64>) -> Result<Vec<u8>> {
        if !validate_path(self.path(), ObjectMode::FILE) {
            return Err(
                Error::new(ErrorKind::ObjectIsADirectory, "read path is a directory")
                    .with_operation("Object::blocking_range_read")
                    .with_context("service", self.accessor().metadata().scheme().into_static())
                    .with_context("path", self.path()),
            );
        }

        let br = BytesRange::from(range);
        let (rp, mut s) = self
            .acc
            .blocking_read(self.path(), OpRead::new().with_range(br))?;

        let mut buffer = Vec::with_capacity(rp.into_metadata().content_length() as usize);
        s.read_to_end(&mut buffer).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "blocking range read failed")
                .with_operation("Object::blocking_range_read")
                .with_context("service", self.accessor().metadata().scheme().into_static())
                .with_context("path", self.path())
                .with_context("range", br.to_string())
                .set_source(err)
        })?;

        Ok(buffer)
    }

    /// Create a new reader which can read the whole object.
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
    /// let o = op.object("path/to/file");
    /// let r = o.reader().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn reader(&self) -> Result<ObjectReader> {
        self.range_reader(..).await
    }

    /// Create a new reader which can read the whole object.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # fn test(op: Operator) -> Result<()> {
    /// let o = op.object("path/to/file");
    /// let r = o.blocking_reader()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_reader(&self) -> Result<BlockingObjectReader> {
        self.blocking_range_reader(..)
    }

    /// Create a new reader which can read the specified range.
    ///
    /// # Notes
    ///
    /// - The returning contnet's length may be smaller than the range specified.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let o = op.object("path/to/file");
    /// let r = o.range_reader(1024..2048).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn range_reader(&self, range: impl RangeBounds<u64>) -> Result<ObjectReader> {
        if !validate_path(self.path(), ObjectMode::FILE) {
            return Err(
                Error::new(ErrorKind::ObjectIsADirectory, "read path is a directory")
                    .with_operation("Object::range_reader")
                    .with_context("service", self.accessor().metadata().scheme().into_static())
                    .with_context("path", self.path()),
            );
        }

        let op = OpRead::new().with_range(range.into());

        ObjectReader::create(self.accessor(), self.path(), op).await
    }

    /// Create a new reader which can read the specified range.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # fn test(op: Operator) -> Result<()> {
    /// let o = op.object("path/to/file");
    /// let r = o.blocking_range_reader(1024..2048)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_range_reader(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<BlockingObjectReader> {
        if !validate_path(self.path(), ObjectMode::FILE) {
            return Err(
                Error::new(ErrorKind::ObjectIsADirectory, "read path is a directory")
                    .with_operation("Object::blocking_range_reader")
                    .with_context("service", self.accessor().metadata().scheme().into_static())
                    .with_context("path", self.path()),
            );
        }

        let op = OpRead::new().with_range(range.into());

        BlockingObjectReader::create(self.accessor(), self.path(), op)
    }
    /// Write bytes into object.
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
    /// let mut o = op.object("path/to/file");
    /// let _ = o.write(vec![0; 4096]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(&self, bs: impl Into<Vec<u8>>) -> Result<()> {
        let bs: Vec<u8> = bs.into();
        let op = OpWrite::new(bs.len() as u64);
        self.write_with(op, bs).await
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
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    /// use opendal::ops::OpWrite;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/file");
    /// let bs = b"hello, world!".to_vec();
    /// let args = OpWrite::new(bs.len() as u64).with_content_type("text/plain");
    /// let _ = o.write_with(args, bs).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_with(&self, args: OpWrite, bs: impl Into<Vec<u8>>) -> Result<()> {
        if !validate_path(self.path(), ObjectMode::FILE) {
            return Err(
                Error::new(ErrorKind::ObjectIsADirectory, "write path is a directory")
                    .with_operation("Object::write_with")
                    .with_context("service", self.accessor().metadata().scheme().into_static())
                    .with_context("path", self.path()),
            );
        }

        let bs = bs.into();
        let r = Cursor::new(bs);
        let _ = self.acc.write(self.path(), args, Box::new(r)).await?;

        Ok(())
    }

    /// Write bytes into object.
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
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/file");
    /// let _ = o.blocking_write(vec![0; 4096])?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_write(&self, bs: impl Into<Vec<u8>>) -> Result<()> {
        let bs: Vec<u8> = bs.into();
        let op = OpWrite::new(bs.len() as u64);
        self.blocking_write_with(op, bs)
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
    /// # use opendal::Operator;
    /// use bytes::Bytes;
    /// use opendal::ops::OpWrite;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("hello.txt");
    /// let bs = b"hello, world!".to_vec();
    /// let ow = OpWrite::new(bs.len() as u64).with_content_type("text/plain");
    /// let _ = o.blocking_write_with(ow, bs)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_write_with(&self, args: OpWrite, bs: impl Into<Vec<u8>>) -> Result<()> {
        if !validate_path(self.path(), ObjectMode::FILE) {
            return Err(
                Error::new(ErrorKind::ObjectIsADirectory, "write path is a directory")
                    .with_operation("Object::blocking_write_with")
                    .with_context("service", self.accessor().metadata().scheme().into_static())
                    .with_context("path", self.path()),
            );
        }

        let bs = bs.into();
        let r = std::io::Cursor::new(bs);
        let _ = self.acc.blocking_write(self.path(), args, Box::new(r))?;

        Ok(())
    }

    /// Write data into object from a [`input::Read`].
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
    /// use futures::io::Cursor;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/file");
    /// let r = Cursor::new(vec![0; 4096]);
    /// let _ = o.write_from(4096, r).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write_from(&self, size: u64, br: impl input::Read + 'static) -> Result<()> {
        if !validate_path(self.path(), ObjectMode::FILE) {
            return Err(
                Error::new(ErrorKind::ObjectIsADirectory, "write path is a directory")
                    .with_operation("Object::write_from")
                    .with_context("service", self.accessor().metadata().scheme().into_static())
                    .with_context("path", self.path()),
            );
        }

        let _ = self
            .acc
            .write(self.path(), OpWrite::new(size), Box::new(br))
            .await?;
        Ok(())
    }

    /// Write data into object from a [`input::BlockingRead`].
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
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use std::io::Cursor;
    ///
    /// use bytes::Bytes;
    ///
    /// # async fn test(op: Operator) -> Result<()> {
    /// let mut o = op.object("path/to/file");
    /// let r = Cursor::new(vec![0; 4096]);
    /// let _ = o.blocking_write_from(4096, r)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_write_from(
        &self,
        size: u64,
        br: impl input::BlockingRead + 'static,
    ) -> Result<()> {
        if !validate_path(self.path(), ObjectMode::FILE) {
            return Err(
                Error::new(ErrorKind::ObjectIsADirectory, "write path is a directory")
                    .with_operation("Object::blocking_write_from")
                    .with_context("service", self.accessor().metadata().scheme().into_static())
                    .with_context("path", self.path()),
            );
        }

        let _ = self
            .acc
            .blocking_write(self.path(), OpWrite::new(size), Box::new(br))?;
        Ok(())
    }

    /// Delete object.
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
    /// op.object("test").delete().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self) -> Result<()> {
        let _ = self.acc.delete(self.path(), OpDelete::new()).await?;

        Ok(())
    }

    /// Delete object.
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
    /// # use opendal::Operator;
    /// # fn test(op: Operator) -> Result<()> {
    /// op.object("test").blocking_delete()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_delete(&self) -> Result<()> {
        let _ = self.acc.blocking_delete(self.path(), OpDelete::new())?;

        Ok(())
    }

    /// List current dir object.
    ///
    /// This function will create a new handle to list objects.
    ///
    /// An error will be returned if object path doesn't end with `/`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// # use opendal::ObjectMode;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let o = op.object("path/to/dir/");
    /// let mut ds = o.list().await?;
    /// while let Some(mut de) = ds.try_next().await? {
    ///     let meta = de
    ///         .metadata({
    ///             use opendal::ObjectMetakey::*;
    ///             Mode
    ///         })
    ///         .await?;
    ///     match meta.mode() {
    ///         ObjectMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         ObjectMode::DIR => {
    ///             println!("Handling dir like start a new list via meta.path()")
    ///         }
    ///         ObjectMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list(&self) -> Result<ObjectLister> {
        if !validate_path(self.path(), ObjectMode::DIR) {
            return Err(Error::new(
                ErrorKind::ObjectNotADirectory,
                "the path trying to list is not a directory",
            )
            .with_operation("Object::list")
            .with_context("service", self.accessor().metadata().scheme().into_static())
            .with_context("path", self.path()));
        }

        let (_, pager) = self.acc.list(self.path(), OpList::new()).await?;

        Ok(ObjectLister::new(self.acc.clone(), pager))
    }

    /// List current dir object.
    ///
    /// This function will create a new handle to list objects.
    ///
    /// An error will be returned if object path doesn't end with `/`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// # use opendal::ObjectMode;
    /// # fn test(op: Operator) -> Result<()> {
    /// let o = op.object("path/to/dir/");
    /// let mut ds = o.blocking_list()?;
    /// while let Some(mut de) = ds.next() {
    ///     let meta = de?.blocking_metadata({
    ///         use opendal::ObjectMetakey::*;
    ///         Mode
    ///     })?;
    ///     match meta.mode() {
    ///         ObjectMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         ObjectMode::DIR => {
    ///             println!("Handling dir like start a new list via meta.path()")
    ///         }
    ///         ObjectMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_list(&self) -> Result<BlockingObjectLister> {
        if !validate_path(self.path(), ObjectMode::DIR) {
            return Err(Error::new(
                ErrorKind::ObjectNotADirectory,
                "the path trying to list is not a directory",
            )
            .with_operation("Object::blocking_list")
            .with_context("service", self.accessor().metadata().scheme().into_static())
            .with_context("path", self.path()));
        }

        let (_, pager) = self.acc.blocking_list(self.path(), OpList::new())?;
        Ok(BlockingObjectLister::new(self.acc.clone(), pager))
    }

    /// List dir in flat way.
    ///
    /// This function will create a new handle to list objects.
    ///
    /// An error will be returned if object path doesn't end with `/`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// # use opendal::ObjectMode;
    /// # use futures::TryStreamExt;
    /// #
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let o = op.object("path/to/dir/");
    /// let mut ds = o.scan().await?;
    /// while let Some(mut de) = ds.try_next().await? {
    ///     let meta = de
    ///         .metadata({
    ///             use opendal::ObjectMetakey::*;
    ///             Mode
    ///         })
    ///         .await?;
    ///     match meta.mode() {
    ///         ObjectMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         ObjectMode::DIR => {
    ///             println!("Handling dir like start a new list via meta.path()")
    ///         }
    ///         ObjectMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn scan(&self) -> Result<ObjectLister> {
        if !validate_path(self.path(), ObjectMode::DIR) {
            return Err(Error::new(
                ErrorKind::ObjectNotADirectory,
                "the path trying to list is not a directory",
            )
            .with_operation("Object::list")
            .with_context("service", self.accessor().metadata().scheme().into_static())
            .with_context("path", self.path()));
        }

        let (_, pager) = self.acc.scan(self.path(), OpScan::new()).await?;

        Ok(ObjectLister::new(self.acc.clone(), pager))
    }

    /// List dir in flat way.
    ///
    /// This function will create a new handle to list objects.
    ///
    /// An error will be returned if object path doesn't end with `/`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// # use opendal::ObjectMode;
    /// # fn test(op: Operator) -> Result<()> {
    /// let o = op.object("path/to/dir/");
    /// let mut ds = o.blocking_list()?;
    /// while let Some(mut de) = ds.next() {
    ///     let meta = de?.blocking_metadata({
    ///         use opendal::ObjectMetakey::*;
    ///         Mode
    ///     })?;
    ///     match meta.mode() {
    ///         ObjectMode::FILE => {
    ///             println!("Handling file")
    ///         }
    ///         ObjectMode::DIR => {
    ///             println!("Handling dir like start a new list via meta.path()")
    ///         }
    ///         ObjectMode::Unknown => continue,
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_scan(&self) -> Result<BlockingObjectLister> {
        if !validate_path(self.path(), ObjectMode::DIR) {
            return Err(Error::new(
                ErrorKind::ObjectNotADirectory,
                "the path trying to list is not a directory",
            )
            .with_operation("Object::blocking_scan")
            .with_context("service", self.accessor().metadata().scheme().into_static())
            .with_context("path", self.path()));
        }

        let (_, pager) = self.acc.blocking_scan(self.path(), OpScan::new())?;
        Ok(BlockingObjectLister::new(self.acc.clone(), pager))
    }

    /// Get current object's metadata **without cache** directly.
    ///
    /// # Notes
    ///
    /// Use `stat` if you:
    ///
    /// - Want detect the outside changes of object.
    /// - Don't want to read from cached object metadata.
    ///
    /// You may want to use `metadata` if you are working with objects
    /// returned by [`ObjectLister`]. It's highly possible that metadata
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
    /// if let Err(e) = op.object("test").stat().await {
    ///     if e.kind() == ErrorKind::ObjectNotFound {
    ///         println!("object not exist")
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn stat(&self) -> Result<ObjectMetadata> {
        let rp = self.acc.stat(self.path(), OpStat::new()).await?;
        let meta = rp.into_metadata();

        Ok(meta)
    }

    /// Get current object's metadata **without cache** directly.
    ///
    /// # Notes
    ///
    /// Use `stat` if you:
    ///
    /// - Want detect the outside changes of object.
    /// - Don't want to read from cached object metadata.
    ///
    /// You may want to use `metadata` if you are working with objects
    /// returned by [`ObjectLister`]. It's highly possible that metadata
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
    /// # fn test(op: Operator) -> Result<()> {
    /// if let Err(e) = op.object("test").blocking_stat() {
    ///     if e.kind() == ErrorKind::ObjectNotFound {
    ///         println!("object not exist")
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_stat(&self) -> Result<ObjectMetadata> {
        let rp = self.acc.blocking_stat(self.path(), OpStat::new())?;
        let meta = rp.into_metadata();

        Ok(meta)
    }

    /// Get current object's metadata with cache.
    ///
    /// `metadata` will check the given query with already cached metadata
    ///  first. And query from storage if not found.
    ///
    /// # Notes
    ///
    /// Use `metadata` if you are working with objects returned by
    /// [`ObjectLister`]. It's highly possible that metadata you want
    /// has already been cached.
    ///
    /// You may want to use `stat`, if you:
    ///
    /// - Want detect the outside changes of object.
    /// - Don't want to read from cached object metadata.
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
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let meta = op.object("test").metadata(None).await?;
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
    /// use opendal::ObjectMetakey;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let meta = op
    ///     .object("test")
    ///     .metadata({
    ///         use ObjectMetakey::*;
    ///         ContentLength | ContentType
    ///     })
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
    /// By query metadata with `Complete`, we can make sure that we have fetched all metadata of this object.
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use opendal::Operator;
    /// use opendal::ObjectMetakey;
    ///
    /// # #[tokio::main]
    /// # async fn test(op: Operator) -> Result<()> {
    /// let meta = op
    ///     .object("test")
    ///     .metadata({ ObjectMetakey::Complete })
    ///     .await?;
    /// // content length MUST be correct.
    /// let _ = meta.content_length();
    /// // etag MUST be correct.
    /// let _ = meta.etag();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn metadata(
        &self,
        flags: impl Into<FlagSet<ObjectMetakey>>,
    ) -> Result<Arc<ObjectMetadata>> {
        if let Some(meta) = &self.meta {
            if meta.bit().contains(flags) || meta.bit().contains(ObjectMetakey::Complete) {
                return Ok(meta.clone());
            }
        }

        let meta = Arc::new(self.stat().await?);
        Ok(meta)
    }

    /// Get current object's metadata with cache in blocking way.
    ///
    /// `metadata` will check the given query with already cached metadata
    ///  first. And query from storage if not found.
    ///
    /// # Notes
    ///
    /// Use `metadata` if you are working with objects returned by
    /// [`ObjectLister`]. It's highly possible that metadata you want
    /// has already been cached.
    ///
    /// You may want to use `stat`, if you:
    ///
    /// - Want detect the outside changes of object.
    /// - Don't want to read from cached object metadata.
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
    /// # fn test(op: Operator) -> Result<()> {
    /// let meta = op.object("test").blocking_metadata(None)?;
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
    /// use opendal::ObjectMetakey;
    ///
    /// # fn test(op: Operator) -> Result<()> {
    /// let meta = op.object("test").blocking_metadata({
    ///     use ObjectMetakey::*;
    ///     ContentLength | ContentType
    /// })?;
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
    /// By query metadata with `Complete`, we can make sure that we have fetched all metadata of this object.
    ///
    /// ```
    /// # use anyhow::Result;
    /// # use opendal::Operator;
    /// use opendal::ObjectMetakey;
    ///
    /// # fn test(op: Operator) -> Result<()> {
    /// let meta = op
    ///     .object("test")
    ///     .blocking_metadata({ ObjectMetakey::Complete })?;
    /// // content length MUST be correct.
    /// let _ = meta.content_length();
    /// // etag MUST be correct.
    /// let _ = meta.etag();
    /// # Ok(())
    /// # }
    /// ```
    pub fn blocking_metadata(
        &self,
        flags: impl Into<FlagSet<ObjectMetakey>>,
    ) -> Result<Arc<ObjectMetadata>> {
        if let Some(meta) = &self.meta {
            if meta.bit().contains(flags) || meta.bit().contains(ObjectMetakey::Complete) {
                return Ok(meta.clone());
            }
        }

        let meta = Arc::new(self.blocking_stat()?);
        Ok(meta)
    }

    /// Check if this object exists or not.
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
    ///     let _ = op.object("test").is_exist().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn is_exist(&self) -> Result<bool> {
        let r = self.stat().await;
        match r {
            Ok(_) => Ok(true),
            Err(err) => match err.kind() {
                ErrorKind::ObjectNotFound => Ok(false),
                _ => Err(err),
            },
        }
    }

    /// Check if this object exists or not.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use opendal::Operator;
    /// fn test(op: Operator) -> Result<()> {
    ///     let _ = op.object("test").blocking_is_exist()?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn blocking_is_exist(&self) -> Result<bool> {
        let r = self.blocking_stat();
        match r {
            Ok(_) => Ok(true),
            Err(err) => match err.kind() {
                ErrorKind::ObjectNotFound => Ok(false),
                _ => Err(err),
            },
        }
    }

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
    ///     let signed_req = op.object("test").presign_stat(Duration::hours(1))?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub fn presign_stat(&self, expire: Duration) -> Result<PresignedRequest> {
        let op = OpPresign::new(OpStat::new(), expire);

        let rp = self.acc.presign(self.path(), op)?;
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
    ///     let signed_req = op.object("test.txt").presign_read(Duration::hours(1))?;
    /// #    Ok(())
    /// # }
    /// ```
    ///
    /// - `signed_req.method()`: `GET`
    /// - `signed_req.uri()`: `https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>`
    /// - `signed_req.headers()`: `{ "host": "s3.amazonaws.com" }`
    ///
    /// We can download this object via `curl` or other tools without credentials:
    ///
    /// ```shell
    /// curl "https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>" -O /tmp/test.txt
    /// ```
    pub fn presign_read(&self, expire: Duration) -> Result<PresignedRequest> {
        let op = OpPresign::new(OpRead::new(), expire);

        let rp = self.acc.presign(self.path(), op)?;
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
    ///     let signed_req = op.object("test.txt").presign_write(Duration::hours(1))?;
    /// #    Ok(())
    /// # }
    /// ```
    ///
    /// - `signed_req.method()`: `PUT`
    /// - `signed_req.uri()`: `https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>`
    /// - `signed_req.headers()`: `{ "host": "s3.amazonaws.com" }`
    ///
    /// We can upload file as this object via `curl` or other tools without credential:
    ///
    /// ```shell
    /// curl -X PUT "https://s3.amazonaws.com/examplebucket/test.txt?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=access_key_id/20130721/us-east-1/s3/aws4_request&X-Amz-Date=20130721T201207Z&X-Amz-Expires=86400&X-Amz-SignedHeaders=host&X-Amz-Signature=<signature-value>" -d "Hello, World!"
    /// ```
    pub fn presign_write(&self, expire: Duration) -> Result<PresignedRequest> {
        self.presign_write_with(OpWrite::new(0), expire)
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
    ///     let args = OpWrite::new(0).with_content_type("text/csv");
    ///     let signed_req = op.object("test").presign_write_with(args, Duration::hours(1))?;
    ///     let req = http::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub fn presign_write_with(&self, op: OpWrite, expire: Duration) -> Result<PresignedRequest> {
        let op = OpPresign::new(op, expire);

        let rp = self.acc.presign(self.path(), op)?;
        Ok(rp.into_presigned_request())
    }

    /// Construct a multipart with existing upload id.
    pub fn to_multipart(&self, upload_id: &str) -> ObjectMultipart {
        ObjectMultipart::new(self.operator(), &self.path, upload_id)
    }

    /// Create a new multipart for current path.
    pub async fn create_multipart(&self) -> Result<ObjectMultipart> {
        let rp = self
            .acc
            .create_multipart(self.path(), OpCreateMultipart::new())
            .await?;
        Ok(self.to_multipart(rp.upload_id()))
    }
}
