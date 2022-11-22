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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;

use time::OffsetDateTime;

use crate::ops::OpStat;
use crate::path::get_basename;
use crate::Accessor;
use crate::Object;
use crate::ObjectMetadata;
use crate::ObjectMode;

/// ObjectEntry is returned by `ObjectStream` or `ObjectIterate` during object list.
///
/// Users can check returning object entry's mode or convert into an object without overhead.
#[derive(Debug)]
pub struct ObjectEntry {
    acc: Arc<dyn Accessor>,
    path: String,
    meta: Arc<Mutex<ObjectMetadata>>,

    /// Complete means this object entry already carries all metadata
    /// that it could have. Don't call metadata anymore.
    ///
    /// # Safety
    ///
    /// It's safe to clone this value. Because complete will only transform
    /// into `true` and never change back.
    ///
    /// For the worse case, we cloned a `false` to new ObjectEntry. The cost
    /// is an extra stat.
    complete: AtomicBool,
}

impl Clone for ObjectEntry {
    fn clone(&self) -> Self {
        Self {
            acc: self.acc.clone(),
            path: self.path.clone(),
            meta: self.meta.clone(),
            // Read comments on this field.
            complete: self.complete.load(Ordering::Relaxed).into(),
        }
    }
}

/// ObjectEntry can convert into object without overhead.
impl From<ObjectEntry> for Object {
    fn from(d: ObjectEntry) -> Self {
        Object::new(d.acc, &d.path)
    }
}

impl ObjectEntry {
    /// Create a new object entry by its corresponding underlying storage.
    pub fn new(acc: Arc<dyn Accessor>, path: &str, meta: ObjectMetadata) -> ObjectEntry {
        debug_assert!(
            meta.mode().is_dir() == path.ends_with('/'),
            "mode {:?} not match with path {}",
            meta.mode(),
            path
        );

        ObjectEntry {
            acc,
            path: path.to_string(),
            meta: Arc::new(Mutex::new(meta)),
            complete: AtomicBool::default(),
        }
    }

    /// Set accessor for this entry.
    pub fn set_accessor(&mut self, acc: Arc<dyn Accessor>) {
        self.acc = acc;
    }

    /// Complete means this object entry already carries all metadata
    /// that it could have. Don't call metadata anymore.
    pub fn set_complete(&mut self) -> &mut Self {
        self.complete.store(true, Ordering::Relaxed);
        self
    }

    /// Complete means this object entry already carries all metadata
    /// that it could have. Don't call metadata anymore.
    pub fn with_complete(mut self) -> Self {
        self.complete = AtomicBool::new(true);
        self
    }

    /// Convert [`ObjectEntry`] into [`Object`].
    ///
    /// This function is the same with already implemented `From` trait.
    /// This function will make our users happier to avoid writing
    /// generic type parameter
    pub fn into_object(self) -> Object {
        self.into()
    }

    /// Return this object entry's object mode.
    pub fn mode(&self) -> ObjectMode {
        self.meta.lock().expect("lock must succeed").mode()
    }

    /// Return this object entry's id.
    ///
    /// The same with [`Object::id()`]
    pub fn id(&self) -> String {
        format!("{}{}", self.acc.metadata().root(), self.path)
    }

    /// Return this object entry's path.
    ///
    /// The same with [`Object::path()`]
    pub fn path(&self) -> &str {
        &self.path
    }

    /// Set path for this entry.
    pub fn set_path(&mut self, path: &str) {
        #[cfg(debug_assertions)]
        {
            let mode = self.meta.lock().expect("lock must succeed").mode();
            assert_eq!(
                mode.is_dir(),
                path.ends_with('/'),
                "mode {mode:?} not match with path {path}",
            );
        }

        self.path = path.to_string();
    }

    /// Return this object entry's name.
    ///
    /// The same with [`Object::name()`]
    pub fn name(&self) -> &str {
        get_basename(&self.path)
    }

    /// Fetch metadata about this object entry.
    pub async fn metadata(&self) -> ObjectMetadata {
        if !self.complete.load(Ordering::Relaxed) {
            // We will ignore all errors happened during inner metadata.
            if let Ok(rp) = self.acc.stat(self.path(), OpStat::new()).await {
                self.set_metadata(rp.into_metadata());
                self.complete.store(true, Ordering::Relaxed);
            }
        }

        self.meta.lock().expect("lock must succeed").clone()
    }

    /// Fetch metadata about this object entry.
    ///
    /// The same with [`Object::blocking_metadata()`]
    pub fn blocking_metadata(&self) -> ObjectMetadata {
        if !self.complete.load(Ordering::Relaxed) {
            // We will ignore all errors happened during inner metadata.
            if let Ok(meta) = self.acc.blocking_stat(self.path(), OpStat::new()) {
                self.set_metadata(meta.into_metadata());
                self.complete.store(true, Ordering::Relaxed);
            }
        }

        self.meta.lock().expect("lock must succeed").clone()
    }

    /// Update ObjectEntry's metadata by setting new one.
    pub fn set_metadata(&self, meta: ObjectMetadata) -> &Self {
        let mut guard = self.meta.lock().expect("lock must succeed");
        *guard = meta;
        self
    }

    /// The size of `ObjectEntry`'s corresponding object
    ///
    /// `content_length` is a prefetched metadata field in `ObjectEntry`.
    pub async fn content_length(&self) -> u64 {
        if let Some(v) = self
            .meta
            .lock()
            .expect("lock must succeed")
            .content_length_raw()
        {
            return v;
        }

        self.metadata()
            .await
            .content_length_raw()
            .unwrap_or_default()
    }

    /// The MD5 message digest of `ObjectEntry`'s corresponding object
    ///
    /// `content_md5` is a prefetched metadata field in `ObjectEntry`
    ///
    /// It doesn't mean this metadata field of object doesn't exist if `content_md5` is `None`.
    /// Then you have to call `ObjectEntry::metadata()` to get the metadata you want.
    pub async fn content_md5(&self) -> Option<String> {
        if let Some(v) = self.meta.lock().expect("lock must succeed").content_md5() {
            return Some(v.to_string());
        }

        self.metadata().await.content_md5().map(|v| v.to_string())
    }

    /// The last modified UTC datetime of `ObjectEntry`'s corresponding object
    ///
    /// `last_modified` is a prefetched metadata field in `ObjectEntry`
    ///
    /// It doesn't mean this metadata field of object doesn't exist if `last_modified` is `None`.
    /// Then you have to call `ObjectEntry::metadata()` to get the metadata you want.
    pub async fn last_modified(&self) -> Option<OffsetDateTime> {
        if let Some(v) = self.meta.lock().expect("lock must succeed").last_modified() {
            return Some(v);
        }

        self.metadata().await.last_modified()
    }

    /// The ETag string of `ObjectEntry`'s corresponding object
    ///
    /// `etag` is a prefetched metadata field in `ObjectEntry`.
    ///
    /// It doesn't mean this metadata field of object doesn't exist if `etag` is `None`.
    /// Then you have to call `ObjectEntry::metadata()` to get the metadata you want.
    pub async fn etag(&self) -> Option<String> {
        if let Some(v) = self.meta.lock().expect("lock must succeed").etag() {
            return Some(v.to_string());
        }

        self.metadata().await.etag().map(|v| v.to_string())
    }
}
