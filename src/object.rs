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
use time::OffsetDateTime;

use crate::io::BytesRead;
use crate::io_util::seekable_read;
#[cfg(feature = "compress")]
use crate::io_util::CompressAlgorithm;
use crate::io_util::SeekableReader;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BytesWrite;

/// Handler for all object related operations.
#[derive(Clone, Debug)]
pub struct Object {
    acc: Arc<dyn Accessor>,
    meta: Metadata,
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
            meta: Metadata {
                path: Object::normalize_path(path),
                ..Default::default()
            },
        }
    }

    /// Make sure all operation are constructed by normalized path:
    ///
    /// - Path endswith `/` means it's a dir path.
    /// - Otherwise, it's a file path.
    ///
    /// # Normalize Rules
    ///
    /// - All whitespace will be trimmed: ` abc/def ` => `abc/def`
    /// - All leading / will be trimmed: `///abc` => `abc`
    /// - Internal // will be replaced by /: `abc///def` => `abc/def`
    /// - Empty path will be `/`: `` => `/`
    pub(crate) fn normalize_path(path: &str) -> String {
        // - all whitespace has been trimmed.
        // - all leading `/` has been trimmed.
        let path = path.trim().trim_start_matches('/');

        // Fast line for empty path.
        if path.is_empty() {
            return "/".to_string();
        }

        let has_trailing = path.ends_with('/');

        let mut p = path
            .split('/')
            .filter(|v| !v.is_empty())
            .collect::<Vec<&str>>()
            .join("/");

        // Append trailing back if input path is endswith `/`.
        if has_trailing {
            p.push('/');
        }

        p
    }

    pub(crate) fn accessor(&self) -> Arc<dyn Accessor> {
        self.acc.clone()
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
        if self.meta.path.ends_with('/') {
            let op = OpCreate::new(self.meta.path(), ObjectMode::DIR)?;
            self.acc.create(&op).await
        } else {
            let op = OpCreate::new(self.meta.path(), ObjectMode::FILE)?;
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
        let op = OpRead::new(self.meta.path(), range)?;
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
        let op = OpRead::new(self.meta.path(), range)?;
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
    /// If we can't find the correct algorithm, we will fallback to normal read instead.
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
    /// let bs = o.decompress_read().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "compress")]
    pub async fn decompress_read(&self) -> Result<Vec<u8>> {
        let algo = CompressAlgorithm::from_path(self.meta.path());

        match algo {
            None => self.read().await,
            Some(algo) => self.decompress_read_with(algo).await,
        }
    }

    /// Create a reader with auto detected compress algorithm.
    ///
    /// If we can't find the correct algorithm, we will fallback to normal read instead.
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
    pub async fn decompress_reader(&self) -> Result<impl BytesRead> {
        let algo = CompressAlgorithm::from_path(self.meta.path());

        let r = self.reader().await?;

        if let Some(algo) = algo {
            Ok(algo.into_reader(r))
        } else {
            Ok(Box::new(r))
        }
    }

    /// Read the whole object into a bytes with specific compress algorithm.
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

        Ok(algo.into_reader(r))
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
        let op = OpWrite::new(self.meta.path(), bs.as_ref().len() as u64)?;
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
        let op = OpWrite::new(self.meta.path(), size);
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
        let op = &OpDelete::new(self.meta.path())?;

        self.acc.delete(op).await
    }

    /// List current dir object.
    ///
    /// This function will create a new [`ObjectStreamer`][crate::ObjectStreamer] handle
    /// to list objects.
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
    /// # use futures::StreamExt;
    /// # #[tokio::main]
    /// # async fn main() -> Result<()> {
    /// # let op = Operator::new(memory::Backend::build().finish().await?);
    /// let o = op.object("path/to/dir/");
    /// let mut obs = o.list().await?;
    /// // ObjectStream implements `futures::Stream`
    /// while let Some(o) = obs.next().await {
    ///     let mut o = o?;
    ///     // It's highly possible that OpenDAL already did metadata during list.
    ///     // Use `Object::metadata_cached()` to get cached metadata at first.
    ///     let meta = o.metadata_cached().await?;
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
    pub async fn list(&self) -> Result<ObjectStreamer> {
        let op = &OpList::new(self.meta.path())?;

        self.acc.list(op).await
    }

    pub(crate) fn metadata_ref(&self) -> &Metadata {
        &self.meta
    }

    pub(crate) fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.meta
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
    pub async fn metadata(&self) -> Result<Metadata> {
        let op = &OpStat::new(self.meta.path())?;

        self.acc.stat(op).await
    }

    /// Use local cached metadata if possible.
    ///
    /// # Example
    ///
    /// ```
    /// use opendal::services::memory;
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    ///
    ///
    /// #[tokio::main]
    /// async fn main() -> Result<()> {
    ///     let op = Operator::new(memory::Backend::build().finish().await?);
    ///     let mut o = op.object("test");
    ///
    ///     o.metadata_cached().await;
    ///     // The second call to metadata_cached will have no cost.
    ///     o.metadata_cached().await;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub async fn metadata_cached(&mut self) -> Result<&Metadata> {
        if self.meta.complete() {
            return Ok(&self.meta);
        }

        let op = &OpStat::new(self.meta.path())?;
        self.meta = self.acc.stat(op).await?;

        Ok(&self.meta)
    }

    /// Check if this object exist or not.
    ///
    /// # Example
    ///
    /// ```
    /// use opendal::services::memory;
    /// use anyhow::Result;
    /// use futures::io;
    /// use opendal::Operator;
    ///
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
}

/// Metadata carries all object metadata.
#[derive(Debug, Clone, Default)]
pub struct Metadata {
    complete: bool,

    path: String,
    mode: Option<ObjectMode>,

    content_length: Option<u64>,
    content_md5: Option<String>,
    last_modified: Option<OffsetDateTime>,
}

impl Metadata {
    pub fn complete(&self) -> bool {
        self.complete
    }

    pub(crate) fn set_complete(&mut self) -> &mut Self {
        self.complete = true;
        self
    }

    /// Returns object path that relative to corresponding backend's root.
    pub fn path(&self) -> &str {
        &self.path
    }

    pub(crate) fn set_path(&mut self, path: &str) -> &mut Self {
        self.path = path.to_string();
        self
    }

    /// Object mode represent this object' mode.
    pub fn mode(&self) -> ObjectMode {
        debug_assert!(self.mode.is_some(), "mode must exist");

        self.mode.unwrap_or_default()
    }

    pub(crate) fn set_mode(&mut self, mode: ObjectMode) -> &mut Self {
        self.mode = Some(mode);
        self
    }

    /// Content length of this object
    pub fn content_length(&self) -> u64 {
        debug_assert!(self.content_length.is_some(), "content length must exist");

        self.content_length.unwrap_or_default()
    }

    pub(crate) fn set_content_length(&mut self, content_length: u64) -> &mut Self {
        self.content_length = Some(content_length);
        self
    }

    /// Content MD5 of this object.
    pub fn content_md5(&self) -> Option<String> {
        self.content_md5.clone()
    }

    pub(crate) fn set_content_md5(&mut self, content_md5: &str) -> &mut Self {
        self.content_md5 = Some(content_md5.to_string());
        self
    }

    /// Last modified of this object.
    pub fn last_modified(&self) -> Option<OffsetDateTime> {
        self.last_modified
    }

    pub(crate) fn set_last_modified(&mut self, last_modified: OffsetDateTime) -> &mut Self {
        self.last_modified = Some(last_modified);
        self
    }
}

/// ObjectMode represents the corresponding object's mode.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ObjectMode {
    /// FILE means the object has data to read.
    FILE,
    /// DIR means the object can be listed.
    DIR,
    /// Unknown means we don't know what we can do on thi object.
    Unknown,
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

/// ObjectStream represents a stream of object.
pub trait ObjectStream: futures::Stream<Item = Result<Object>> + Unpin + Send {}
impl<T> ObjectStream for T where T: futures::Stream<Item = Result<Object>> + Unpin + Send {}

/// ObjectStreamer is a boxed dyn [`ObjectStream`]
pub type ObjectStreamer = Box<dyn ObjectStream>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_dir_path() {
        let cases = vec![
            ("file path", "abc", "abc"),
            ("dir path", "abc/", "abc/"),
            ("empty path", "", "/"),
            ("root path", "/", "/"),
            ("root path with extra /", "///", "/"),
            ("abs file path", "/abc/def", "abc/def"),
            ("abs dir path", "/abc/def/", "abc/def/"),
            ("abs file path with extra /", "///abc/def", "abc/def"),
            ("abs dir path with extra /", "///abc/def/", "abc/def/"),
            ("file path contains ///", "abc///def", "abc/def"),
            ("dir path contains ///", "abc///def///", "abc/def/"),
            ("file with whitespace", "abc/def   ", "abc/def"),
        ];

        for (name, input, expect) in cases {
            assert_eq!(Object::normalize_path(input), expect, "{}", name)
        }
    }
}
