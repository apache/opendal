// Copyright 2022 Datafuse Labs.
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
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::ErrorKind;
use std::io::Result;
use std::ops::RangeBounds;
use std::sync::Arc;

use futures::io;
use futures::io::Cursor;
use futures::AsyncWriteExt;
use time::Duration;
use time::OffsetDateTime;

use crate::io::BytesRead;
use crate::io_util::seekable_read;
#[cfg(feature = "compress")]
use crate::io_util::CompressAlgorithm;
#[cfg(feature = "compress")]
use crate::io_util::DecompressReader;
use crate::io_util::SeekableReader;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpPresign;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::ops::PresignedRequest;
use crate::path::get_basename;
use crate::path::normalize_path;
use crate::Accessor;
use crate::BytesWrite;
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Handler for all object related operations.
#[derive(Clone, Debug)]
pub struct Object {
    acc: Arc<dyn Accessor>,
    path: String,
}

impl Object {
    /// Creates a new Object with normalized path.
    ///
    /// - All path will be converted into relative path (without any leading `/`)
    /// - Path endswith `/` means it's a dir path.
    /// - Otherwise, it's a file path.
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: normalize_path(path),
        }
    }

    pub(crate) fn accessor(&self) -> Arc<dyn Accessor> {
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
    /// use opendal::services::memory;
    /// use opendal::Operator;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let op = Operator::new(memory::Backend::build().finish().await?);
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
    /// use opendal::services::memory;
    /// use opendal::Operator;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let op = Operator::new(memory::Backend::build().finish().await?);
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
    /// use opendal::services::memory;
    /// use opendal::Operator;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let op = Operator::new(memory::Backend::build().finish().await?);
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
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/file");
    /// let _ = o.create().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// ## Create a dir
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/dir/");
    /// let _ = o.create().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create(&self) -> Result<()> {
        if self.path.ends_with('/') {
            let op = OpCreate::new(self.path(), ObjectMode::DIR)?;
            self.acc.create(&op).await
        } else {
            let op = OpCreate::new(self.path(), ObjectMode::FILE)?;
            self.acc.create(&op).await
        }
    }

    /// Read the whole object into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Object::reader`]
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// # let o = op.object("path/to/file");
    /// # o.write(&vec![0; 4096]).await?;
    /// let bs = o.read().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read(&self) -> Result<Vec<u8>> {
        self.range_read(..).await
    }

    /// Read the specified range of object into a bytes.
    ///
    /// This function will allocate a new bytes internally. For more precise memory control or
    /// reading data lazily, please use [`Object::range_reader`]
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// # let o = op.object("path/to/file");
    /// # o.write(&vec![0; 4096]).await?;
    /// let bs = o.range_read(1024..2048).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn range_read(&self, range: impl RangeBounds<u64>) -> Result<Vec<u8>> {
        let op = OpRead::new(self.path(), range)?;
        let s = self.acc.read(&op).await?;

        let mut bs = Cursor::new(Vec::new());

        io::copy(s, &mut bs).await?;

        Ok(bs.into_inner())
    }

    /// Create a new reader which can read the whole object.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// # let o = op.object("path/to/file");
    /// # o.write(&vec![0; 4096]).await?;
    /// let r = o.reader().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn reader(&self) -> Result<impl BytesRead> {
        self.range_reader(..).await
    }

    /// Create a new reader which can read the specified range.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// # let o = op.object("path/to/file");
    /// # o.write(&vec![0; 4096]).await?;
    /// let r = o.range_reader(1024..2048).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn range_reader(&self, range: impl RangeBounds<u64>) -> Result<impl BytesRead> {
        let op = OpRead::new(self.path(), range)?;
        self.acc.read(&op).await
    }

    /// Create a reader which implements AsyncRead and AsyncSeek inside specified range.
    ///
    /// # Notes
    ///
    /// It's not a zero-cost operations. In order to support seeking, we have extra internal
    /// state which maintains the reader contents:
    ///
    /// - Seeking is pure in memory operation.
    /// - Every first read after seeking will start a new read operation on backend.
    ///
    /// This operation is neither async nor returning result, because real IO happens while
    /// users call `read` or `seek`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// # let o = op.object("path/to/file");
    /// let r = o.seekable_reader(1024..2048);
    /// # Ok(())
    /// # }
    /// ```
    pub fn seekable_reader(&self, range: impl RangeBounds<u64>) -> SeekableReader {
        seekable_read(self, range)
    }

    /// Read the whole object into a bytes with auto detected compress algorithm.
    ///
    /// If we can't find the correct algorithm, we return `Ok(None)` instead.
    ///
    /// # Feature
    ///
    /// This function needs to enable feature `compress`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/file.gz");
    /// # o.write(&vec![0; 4096]).await?;
    /// let bs = o.decompress_read().await?.expect("must read succeed");
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "compress")]
    pub async fn decompress_read(&self) -> Result<Option<Vec<u8>>> {
        let algo = match CompressAlgorithm::from_path(self.path()) {
            None => return Ok(None),
            Some(algo) => algo,
        };

        self.decompress_read_with(algo).await.map(Some)
    }

    /// Create a reader with auto detected compress algorithm.
    ///
    /// If we can't find the correct algorithm, we will return `Ok(None)`.
    ///
    /// # Feature
    ///
    /// This function needs to enable feature `compress`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/file.gz");
    /// # o.write(&vec![0; 4096]).await?;
    /// let r = o.decompress_reader().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "compress")]
    pub async fn decompress_reader(&self) -> Result<Option<impl BytesRead>> {
        let algo = match CompressAlgorithm::from_path(self.path()) {
            Some(v) => v,
            None => return Ok(None),
        };

        let r = self.reader().await?;

        Ok(Some(DecompressReader::new(r, algo)))
    }

    /// Read the whole object into a bytes with specific compress algorithm.
    ///
    /// # Feature
    ///
    /// This function needs to enable feature `compress`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # use opendal::io_util::CompressAlgorithm;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/file.gz");
    /// # o.write(&vec![0; 4096]).await?;
    /// let bs = o.decompress_read_with(CompressAlgorithm::Gzip).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "compress")]
    pub async fn decompress_read_with(&self, algo: CompressAlgorithm) -> Result<Vec<u8>> {
        let r = self.decompress_reader_with(algo).await?;
        let mut bs = Cursor::new(Vec::new());

        io::copy(r, &mut bs).await?;

        Ok(bs.into_inner())
    }

    /// Create a reader with specific compress algorithm.
    ///
    /// # Feature
    ///
    /// This function needs to enable feature `compress`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::TryStreamExt;
    /// # use opendal::io_util::CompressAlgorithm;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/file.gz");
    /// # o.write(&vec![0; 4096]).await?;
    /// let r = o.decompress_reader_with(CompressAlgorithm::Gzip).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "compress")]
    pub async fn decompress_reader_with(&self, algo: CompressAlgorithm) -> Result<impl BytesRead> {
        let r = self.reader().await?;

        Ok(DecompressReader::new(r, algo))
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
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/file");
    /// let _ = o.write(vec![0; 4096]).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn write(&self, bs: impl AsRef<[u8]>) -> Result<()> {
        let op = OpWrite::new(self.path(), bs.as_ref().len() as u64)?;
        let mut s = self.acc.write(&op).await?;

        s.write_all(bs.as_ref()).await?;
        s.close().await?;

        Ok(())
    }

    /// Create a new writer which can write data into the object.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use std::io::Result;
    /// # use opendal::Operator;
    /// # use futures::StreamExt;
    /// # use futures::SinkExt;
    /// use bytes::Bytes;
    ///
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # use futures::AsyncWriteExt;
    /// let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/file");
    /// let mut w = o.writer(4096).await?;
    /// w.write(&[1; 4096]);
    /// w.close();
    /// # Ok(())
    /// # }
    /// ```
    pub async fn writer(&self, size: u64) -> Result<impl BytesWrite> {
        let op = OpWrite::new(self.path(), size);
        let s = self.acc.write(&op?).await?;

        Ok(s)
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
    /// # use opendal::services::memory;
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// op.object("test").delete().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn delete(&self) -> Result<()> {
        let op = &OpDelete::new(self.path())?;

        self.acc.delete(op).await
    }

    /// List current dir object.
    ///
    /// This function will create a new [`DirStreamer`] handle to list objects.
    ///
    /// An error will be returned if object path doesn't end with `/`.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// # use opendal::ObjectMode;
    /// # use futures::TryStreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/dir/");
    /// let mut ds = o.list().await?;
    /// // DirStreamer implements `futures::Stream`
    /// while let Some(de) = ds.try_next().await? {
    ///     match de.mode() {
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
    pub async fn list(&self) -> Result<DirStreamer> {
        let op = OpList::new(self.path())?;

        self.acc.list(&op).await
    }

    /// Get current object's metadata.
    ///
    /// # Examples
    ///
    /// ```
    /// # use opendal::services::memory;
    /// # use anyhow::Result;
    /// # use futures::io;
    /// # use opendal::Operator;
    /// use std::io::ErrorKind;
    /// #
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// if let Err(e) = op.object("test").metadata().await {
    ///     if e.kind() == ErrorKind::NotFound {
    ///         println!("object not exist")
    ///     }
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn metadata(&self) -> Result<ObjectMetadata> {
        let op = OpStat::new(self.path())?;

        self.acc.stat(&op).await
    }

    /// Check if this object exists or not.
    ///
    /// # Example
    ///
    /// ```
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::services::memory;
    /// use opendal::Operator;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let op = Operator::new(memory::Backend::build().finish().await?);
    ///     let _ = op.object("test").is_exist().await?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn is_exist(&self) -> Result<bool> {
        let r = self.metadata().await;
        match r {
            Ok(_) => Ok(true),
            Err(err) => match err.kind() {
                ErrorKind::NotFound => Ok(false),
                _ => Err(err),
            },
        }
    }

    /// Presign an operation for read.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::services::memory;
    /// use opendal::Operator;
    /// use time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    /// #    let op = Operator::new(memory::Backend::build().finish().await?);
    ///     let signed_req = op.object("test").presign_read(Duration::hours(1))?;
    ///     let req = hyper::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(hyper::Body::empty())?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub fn presign_read(&self, expire: Duration) -> Result<PresignedRequest> {
        let op = OpPresign::new(self.path(), Operation::Read, expire)?;

        self.acc.presign(&op)
    }

    /// Presign an operation for write.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::services::memory;
    /// use opendal::Operator;
    /// use time::Duration;
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    /// #    let op = Operator::new(memory::Backend::build().finish().await?);
    ///     let signed_req = op.object("test").presign_write(Duration::hours(1))?;
    ///     let req = hyper::Request::builder()
    ///         .method(signed_req.method())
    ///         .uri(signed_req.uri())
    ///         .body(hyper::Body::from("Hello, World!"))?;
    ///
    /// #    Ok(())
    /// # }
    /// ```
    pub fn presign_write(&self, expire: Duration) -> Result<PresignedRequest> {
        let op = OpPresign::new(self.path(), Operation::Write, expire)?;

        self.acc.presign(&op)
    }
}

/// Metadata carries all object metadata.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ObjectMetadata {
    mode: ObjectMode,

    content_length: u64,
    content_md5: Option<String>,
    last_modified: Option<OffsetDateTime>,
    etag: Option<String>,
}

impl ObjectMetadata {
    /// Object mode represent this object' mode.
    pub fn mode(&self) -> ObjectMode {
        self.mode
    }

    pub(crate) fn set_mode(&mut self, mode: ObjectMode) -> &mut Self {
        self.mode = mode;
        self
    }

    /// Content length of this object
    ///
    /// `Content-Length` is defined by [RFC 7230](https://httpwg.org/specs/rfc7230.html#header.content-length)
    /// Refer to [MDN Content-Length](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Length) for more information.
    pub fn content_length(&self) -> u64 {
        self.content_length
    }

    pub(crate) fn set_content_length(&mut self, content_length: u64) -> &mut Self {
        self.content_length = content_length;
        self
    }

    /// Content MD5 of this object.
    ///
    /// Content Length is defined by [RFC 2616](http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html).
    /// And removed by [RFC 7231](https://www.rfc-editor.org/rfc/rfc7231).
    ///
    /// OpenDAL will try its best to set this value, but not guarantee this value is the md5 of content.
    pub fn content_md5(&self) -> Option<&str> {
        self.content_md5.as_deref()
    }

    pub(crate) fn set_content_md5(&mut self, content_md5: &str) -> &mut Self {
        self.content_md5 = Some(content_md5.to_string());
        self
    }

    /// Last modified of this object.
    ///
    /// `Last-Modified` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.last-modified)
    /// Refer to [MDN Last-Modified](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) for more information.
    ///
    /// OpenDAL parse the raw value into [`OffsetDateTime`] for convenient.
    pub fn last_modified(&self) -> Option<OffsetDateTime> {
        self.last_modified
    }

    pub(crate) fn set_last_modified(&mut self, last_modified: OffsetDateTime) -> &mut Self {
        self.last_modified = Some(last_modified);
        self
    }

    /// ETag of this object.
    ///
    /// `ETag` is defined by [RFC 7232](https://httpwg.org/specs/rfc7232.html#header.etag)
    /// Refer to [MDN ETag](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) for more information.
    ///
    /// OpenDAL will return this value AS-IS like the following:
    ///
    /// - `"33a64df551425fcc55e4d42a148795d9f25f89d4"`
    /// - `W/"0815"`
    ///
    /// `"` is part of etag.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    pub(crate) fn set_etag(&mut self, etag: &str) -> &mut Self {
        self.etag = Some(etag.to_string());
        self
    }
}

/// ObjectMode represents the corresponding object's mode.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ObjectMode {
    /// FILE means the object has data to read.
    FILE,
    /// DIR means the object can be listed.
    DIR,
    /// Unknown means we don't know what we can do on thi object.
    Unknown,
}

impl ObjectMode {
    /// Check if this object mode is FILE.
    pub fn is_file(self) -> bool {
        self == ObjectMode::FILE
    }
    /// Check if this object mode is DIR.
    pub fn is_dir(self) -> bool {
        self == ObjectMode::DIR
    }
}

impl Default for ObjectMode {
    fn default() -> Self {
        Self::Unknown
    }
}

impl Display for ObjectMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ObjectMode::FILE => write!(f, "file"),
            ObjectMode::DIR => write!(f, "dir"),
            ObjectMode::Unknown => write!(f, "unknown"),
        }
    }
}

/// DirStream represents a stream of Dir.
pub trait DirStream: futures::Stream<Item = Result<DirEntry>> + Unpin + Send {}
impl<T> DirStream for T where T: futures::Stream<Item = Result<DirEntry>> + Unpin + Send {}

/// DirStreamer is a boxed dyn [`DirStream`]
pub type DirStreamer = Box<dyn DirStream>;

/// DirEntry is returned by [`DirStream`] during object list.
///
/// DirEntry carries two information: path and mode. Users can check returning dir
/// entry's mode or convert into an object without overhead.
#[derive(Clone, Debug)]
pub struct DirEntry {
    acc: Arc<dyn Accessor>,

    mode: ObjectMode,
    path: String,
}

impl DirEntry {
    pub(crate) fn new(acc: Arc<dyn Accessor>, mode: ObjectMode, path: &str) -> DirEntry {
        debug_assert!(
            mode.is_dir() == path.ends_with('/'),
            "mode {:?} not match with path {}",
            mode,
            path
        );

        DirEntry {
            acc,
            mode,
            path: path.to_string(),
        }
    }

    /// Convert [`DirEntry`] into [`Object`].
    ///
    /// This function is the same with already implemented `From` trait.
    /// This function will make our users happier to avoid writing
    /// generic type parameter
    pub fn into_object(self) -> Object {
        self.into()
    }

    /// Return this dir entry's object mode.
    pub fn mode(&self) -> ObjectMode {
        self.mode
    }

    /// Return this dir entry's id.
    ///
    /// The same with [`Object::id()`]
    pub fn id(&self) -> String {
        format!("{}{}", self.acc.metadata().root(), self.path)
    }

    /// Return this dir entry's path.
    ///
    /// The same with [`Object::path()`]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Return this dir entry's name.
    ///
    /// The same with [`Object::name()`]
    pub fn name(&self) -> &str {
        get_basename(&self.path)
    }

    /// Fetch metadata about this dir entry.
    ///
    /// The same with [`Object::metadata()`]
    pub async fn metadata(&self) -> Result<ObjectMetadata> {
        let op = OpStat::new(self.path())?;

        self.acc.stat(&op).await
    }
}

/// DirEntry can convert into object without overhead.
impl From<DirEntry> for Object {
    fn from(d: DirEntry) -> Self {
        Object {
            acc: d.acc,
            path: d.path,
        }
    }
}
