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

use std::io::Result;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use time::OffsetDateTime;

use crate::ops::OpStat;
use crate::path::get_basename;
use crate::Accessor;
use crate::Object;
use crate::ObjectMetadata;
use crate::ObjectMode;

/// DirStream represents a stream of Dir.
pub trait DirStream: futures::Stream<Item = Result<DirEntry>> + Unpin + Send {}
impl<T> DirStream for T where T: futures::Stream<Item = Result<DirEntry>> + Unpin + Send {}

/// DirStreamer is a boxed dyn [`DirStream`]
pub type DirStreamer = Box<dyn DirStream>;

/// DirIterate represents an iterator of Dir.
pub trait DirIterate: Iterator<Item = Result<DirEntry>> {}
impl<T> DirIterate for T where T: Iterator<Item = Result<DirEntry>> {}

/// DirIterator is a boxed dyn [`DirIterate`]
pub type DirIterator = Box<dyn DirIterate>;

/// DirEntry is returned by [`DirStream`] during object list.
///
/// DirEntry carries two information: path and mode. Users can check returning dir
/// entry's mode or convert into an object without overhead.
#[derive(Clone, Debug)]
pub struct DirEntry {
    acc: Arc<dyn Accessor>,

    mode: ObjectMode,
    path: String,

    // metadata fields
    etag: Option<String>,
    content_length: Option<u64>,
    content_md5: Option<String>,
    last_modified: Option<OffsetDateTime>,
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
            // all set to None
            etag: None,
            content_length: None,
            content_md5: None,
            last_modified: None,
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

    /// The ETag string of `DirEntry`'s corresponding object
    ///
    /// `etag` is a prefetched metadata field in `DirEntry`.
    ///
    /// It doesn't mean this metadata field of object doesn't exist if `etag` is `None`.
    /// Then you have to call `DirEntry::metadata()` to get the metadata you want.
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }

    /// The size of `DirEntry`'s corresponding object
    ///
    /// `content_length` is a prefetched metadata field in `DirEntry`.
    ///
    /// It doesn't mean this metadata field of object doesn't exist if `content_length` is `None`.
    /// Then you have to call `DirEntry::metadata()` to get the metadata you want.
    pub fn content_length(&self) -> Option<u64> {
        self.content_length
    }

    /// The MD5 message digest of `DirEntry`'s corresponding object
    ///
    /// `content_md5` is a prefetched metadata field in `DirEntry`
    ///
    /// It doesn't mean this metadata field of object doesn't exist if `content_md5` is `None`.
    /// Then you have to call `DirEntry::metadata()` to get the metadata you want.
    pub fn content_md5(&self) -> Option<&str> {
        self.content_md5.as_deref()
    }

    /// The last modified UTC datetime of `DirEntry`'s corresponding object
    ///
    /// `last_modified` is a prefetched metadata field in `DirEntry`
    ///
    /// It doesn't mean this metadata field of object doesn't exist if `last_modified` is `None`.
    /// Then you have to call `DirEntry::metadata()` to get the metadata you want.
    pub fn last_modified(&self) -> Option<OffsetDateTime> {
        self.last_modified
    }

    /// Fetch metadata about this dir entry.
    ///
    /// The same with [`Object::metadata()`]
    pub async fn metadata(&self) -> Result<ObjectMetadata> {
        self.acc.stat(self.path(), OpStat::new()).await
    }

    /// Fetch metadata about this dir entry.
    ///
    /// The same with [`Object::blocking_metadata()`]
    pub fn blocking_metadata(&self) -> Result<ObjectMetadata> {
        self.acc.blocking_stat(self.path(), OpStat::new())
    }
}

// implement setters for DirEntry's metadata fields
impl DirEntry {
    /// Set path for this entry.
    pub(crate) fn set_path(&mut self, path: &str) {
        debug_assert!(
            self.mode.is_dir() == path.ends_with('/'),
            "mode {:?} not match with path {}",
            self.mode,
            path
        );

        self.path = path.to_string();
    }
    /// record the ETag of `DirEntry`'s corresponding object
    pub(crate) fn set_etag(&mut self, etag: &str) {
        self.etag = Some(etag.to_string())
    }
    /// record the last modified time of `DirEntry`'s corresponding object
    pub(crate) fn set_last_modified(&mut self, last_modified: OffsetDateTime) {
        self.last_modified = Some(last_modified)
    }
    /// record the content length of `DirEntry`'s corresponding object
    pub(crate) fn set_content_length(&mut self, content_length: u64) {
        self.content_length = Some(content_length)
    }
    /// record the content's md5 of `DirEntry`'s corresponding object
    pub(crate) fn set_content_md5(&mut self, content_md5: &str) {
        self.content_md5 = Some(content_md5.to_string())
    }
}

/// DirEntry can convert into object without overhead.
impl From<DirEntry> for Object {
    fn from(d: DirEntry) -> Self {
        Object::new(d.acc, &d.path)
    }
}

/// EmptyDirStreamer that always return None.
pub(crate) struct EmptyDirStreamer;

impl futures::Stream for EmptyDirStreamer {
    type Item = Result<DirEntry>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

/// EmptyDirIterator that always return None.
pub(crate) struct EmptyDirIterator;

impl Iterator for EmptyDirIterator {
    type Item = Result<DirEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}
