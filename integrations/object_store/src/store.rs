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

use std::fmt::{self, Debug, Display, Formatter};
use std::future::IntoFuture;
use std::ops::Range;
use std::sync::Arc;

use crate::utils::*;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryStreamExt;
use object_store::path::Path;
use object_store::GetResult;
use object_store::GetResultPayload;
use object_store::ListResult;
use object_store::MultipartUpload;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::PutMultipartOpts;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::PutResult;
use object_store::{GetOptions, UploadPart};
use opendal::Writer;
use opendal::{Buffer, Metakey};
use opendal::{Operator, OperatorInfo};
use tokio::sync::{Mutex, Notify};

/// OpendalStore implements ObjectStore trait by using opendal.
///
/// This allows users to use opendal as an object store without extra cost.
///
/// Visit [`opendal::services`] for more information about supported services.
///
/// ```no_run
/// use std::sync::Arc;
///
/// use bytes::Bytes;
/// use object_store::path::Path;
/// use object_store::ObjectStore;
/// use object_store_opendal::OpendalStore;
/// use opendal::services::S3;
/// use opendal::{Builder, Operator};
///
/// #[tokio::main]
/// async fn main() {
///     let builder = S3::from_map(
///         vec![
///             ("access_key".to_string(), "my_access_key".to_string()),
///             ("secret_key".to_string(), "my_secret_key".to_string()),
///             ("endpoint".to_string(), "my_endpoint".to_string()),
///             ("region".to_string(), "my_region".to_string()),
///         ]
///         .into_iter()
///         .collect(),
///     ).unwrap();
///
///     // Create a new operator
///     let operator = Operator::new(builder).unwrap().finish();
///
///     // Create a new object store
///     let object_store = Arc::new(OpendalStore::new(operator));
///
///     let path = Path::from("data/nested/test.txt");
///     let bytes = Bytes::from_static(b"hello, world! I am nested.");
///
///     object_store.put(&path, bytes.clone().into()).await.unwrap();
///
///     let content = object_store
///         .get(&path)
///         .await
///         .unwrap()
///         .bytes()
///         .await
///         .unwrap();
///
///     assert_eq!(content, bytes);
/// }
/// ```
pub struct OpendalStore {
    info: Arc<OperatorInfo>,
    inner: Operator,
}

impl OpendalStore {
    /// Create OpendalStore by given Operator.
    pub fn new(op: Operator) -> Self {
        Self {
            info: op.info().into(),
            inner: op,
        }
    }

    /// The metakey that requested by object_store, should align with its meta.
    #[inline]
    fn metakey() -> flagset::FlagSet<Metakey> {
        Metakey::Mode
            | Metakey::LastModified
            | Metakey::ContentLength
            | Metakey::Etag
            | Metakey::Version
    }
}

impl Debug for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpendalStore")
            .field("scheme", &self.info.scheme())
            .field("name", &self.info.name())
            .field("root", &self.info.root())
            .field("capability", &self.info.full_capability())
            .finish()
    }
}

impl Display for OpendalStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let info = self.inner.info();
        write!(
            f,
            "Opendal({}, bucket={}, root={})",
            info.scheme(),
            info.name(),
            info.root()
        )
    }
}

impl From<Operator> for OpendalStore {
    fn from(value: Operator) -> Self {
        Self::new(value)
    }
}

#[async_trait]
impl ObjectStore for OpendalStore {
    async fn put(&self, location: &Path, bytes: PutPayload) -> object_store::Result<PutResult> {
        self.inner
            .write(location.as_ref(), Buffer::from_iter(bytes.into_iter()))
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn put_opts(
        &self,
        _location: &Path,
        _bytes: PutPayload,
        _opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "put_opts is not implemented so far",
            )),
        })
    }

    async fn put_multipart(
        &self,
        location: &Path,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let writer = self
            .inner
            .writer_with(location.as_ref())
            .concurrent(8)
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;
        let upload = OpendalMultipartUpload::new(writer, location.clone());

        Ok(Box::new(upload))
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOpts,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "put_multipart_opts is not implemented so far",
            )),
        })
    }

    async fn get(&self, location: &Path) -> object_store::Result<GetResult> {
        let meta = self
            .inner
            .stat(location.as_ref())
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: meta.last_modified().unwrap_or_default(),
            size: meta.content_length() as usize,
            e_tag: meta.etag().map(|x| x.to_string()),
            version: meta.version().map(|x| x.to_string()),
        };

        let r = self
            .inner
            .reader(location.as_ref())
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        let stream = r
            .into_bytes_stream(0..meta.size as u64)
            .await
            .map_err(|err| object_store::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            })?
            .into_send()
            .map_err(|err| object_store::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            });

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(stream)),
            range: 0..meta.size,
            meta,
            attributes: Default::default(),
        })
    }

    async fn get_opts(
        &self,
        _location: &Path,
        _options: GetOptions,
    ) -> object_store::Result<GetResult> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "get_opts is not implemented so far",
            )),
        })
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> object_store::Result<Bytes> {
        let bs = self
            .inner
            .read_with(location.as_ref())
            .range(range.start as u64..range.end as u64)
            .into_future()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(bs.to_bytes())
    }

    async fn head(&self, location: &Path) -> object_store::Result<ObjectMeta> {
        let meta = self
            .inner
            .stat(location.as_ref())
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: meta.last_modified().unwrap_or_default(),
            size: meta.content_length() as usize,
            e_tag: meta.etag().map(|x| x.to_string()),
            version: meta.version().map(|x| x.to_string()),
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        self.inner
            .delete(location.as_ref())
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(())
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        // object_store `Path` always removes trailing slash
        // need to add it back
        let path = prefix.map_or("".into(), |x| format!("{}/", x));

        let fut = async move {
            let stream = self
                .inner
                .lister_with(&path)
                .metakey(Self::metakey())
                .recursive(true)
                .await
                .map_err(|err| format_object_store_error(err, &path))?;

            let stream = stream.then(|res| async {
                let entry = res.map_err(|err| format_object_store_error(err, ""))?;
                let meta = entry.metadata();

                Ok(format_object_meta(entry.path(), meta))
            });
            Ok::<_, object_store::Error>(stream)
        };

        fut.into_stream().try_flatten().into_send().boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'_, object_store::Result<ObjectMeta>> {
        let path = prefix.map_or("".into(), |x| format!("{}/", x));
        let offset = offset.clone();

        let fut = async move {
            let fut = if self.inner.info().full_capability().list_with_start_after {
                self.inner
                    .lister_with(&path)
                    .start_after(offset.as_ref())
                    .metakey(Self::metakey())
                    .recursive(true)
                    .into_future()
                    .into_send()
                    .await
                    .map_err(|err| format_object_store_error(err, &path))?
                    .then(try_format_object_meta)
                    .into_send()
                    .boxed()
            } else {
                self.inner
                    .lister_with(&path)
                    .metakey(Self::metakey())
                    .recursive(true)
                    .into_future()
                    .into_send()
                    .await
                    .map_err(|err| format_object_store_error(err, &path))?
                    .try_filter(move |entry| futures::future::ready(entry.path() > offset.as_ref()))
                    .then(try_format_object_meta)
                    .into_send()
                    .boxed()
            };
            Ok::<_, object_store::Error>(fut)
        };

        fut.into_stream().into_send().try_flatten().boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let path = prefix.map_or("".into(), |x| format!("{}/", x));
        let mut stream = self
            .inner
            .lister_with(&path)
            .metakey(Self::metakey())
            .into_future()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, &path))?
            .into_send();

        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();

        while let Some(res) = stream.next().into_send().await {
            let entry = res.map_err(|err| format_object_store_error(err, ""))?;
            let meta = entry.metadata();

            if meta.is_dir() {
                common_prefixes.push(entry.path().into());
            } else {
                objects.push(format_object_meta(entry.path(), meta));
            }
        }

        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "copy is not implemented so far",
            )),
        })
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "rename is not implemented so far",
            )),
        })
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> object_store::Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "copy_if_not_exists is not implemented so far",
            )),
        })
    }
}

/// `MultipartUpload`'s impl based on `Writer` in opendal
///
/// # Notes
///
/// OpenDAL writer can handle concurrent internally we don't generate real `UploadPart` like existing
/// implementation do. Instead, we just write the part and notify the next task to be written.
///
/// The lock here doesn't really involve the write process, it's just for the notify mechanism.
struct OpendalMultipartUpload {
    writer: Arc<Mutex<Writer>>,
    location: Path,
    next_notify: Option<Arc<Notify>>,
}

impl OpendalMultipartUpload {
    fn new(writer: Writer, location: Path) -> Self {
        Self {
            writer: Arc::new(Mutex::new(writer)),
            location,
            next_notify: None,
        }
    }
}

#[async_trait]
impl MultipartUpload for OpendalMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = self.writer.clone();
        let location = self.location.clone();

        // Generate next notify which will be notified after the current part is written.
        let next_notify = Arc::new(Notify::new());
        // Fetch the notify for current part to wait for it to be written.
        let current_notify = self.next_notify.replace(next_notify.clone());

        async move {
            // current_notify == None means that it's the first part, we don't need to wait.
            if let Some(notify) = current_notify {
                // Wait for the previous part to be written
                notify.notified().await;
            }

            let mut writer = writer.lock().await;
            let result = writer
                .write(Buffer::from_iter(data.into_iter()))
                .await
                .map_err(|err| format_object_store_error(err, location.as_ref()));

            // Notify the next part to be written
            next_notify.notify_one();

            result
        }
        .into_send()
        .boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let mut writer = self.writer.lock().await;
        writer
            .close()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))?;

        Ok(PutResult {
            e_tag: None,
            version: None,
        })
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let mut writer = self.writer.lock().await;
        writer
            .abort()
            .into_send()
            .await
            .map_err(|err| format_object_store_error(err, self.location.as_ref()))
    }
}

impl Debug for OpendalMultipartUpload {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpendalMultipartUpload")
            .field("location", &self.location)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::path::Path;
    use object_store::{ObjectStore, WriteMultipart};
    use opendal::services;
    use rand::prelude::*;

    use super::*;

    async fn create_test_object_store() -> Arc<dyn ObjectStore> {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let object_store = Arc::new(OpendalStore::new(op));

        let path: Path = "data/test.txt".into();
        let bytes = Bytes::from_static(b"hello, world!");
        object_store.put(&path, bytes.into()).await.unwrap();

        let path: Path = "data/nested/test.txt".into();
        let bytes = Bytes::from_static(b"hello, world! I am nested.");
        object_store.put(&path, bytes.into()).await.unwrap();

        object_store
    }

    #[tokio::test]
    async fn test_basic() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let object_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(op));

        // Retrieve a specific file
        let path: Path = "data/test.txt".into();

        let bytes = Bytes::from_static(b"hello, world!");
        object_store.put(&path, bytes.clone().into()).await.unwrap();

        let meta = object_store.head(&path).await.unwrap();

        assert_eq!(meta.size, 13);

        assert_eq!(
            object_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            bytes
        );
    }

    #[tokio::test]
    async fn test_put_multipart() {
        let op = Operator::new(services::Memory::default()).unwrap().finish();
        let object_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(op));

        let mut rng = thread_rng();

        // Case complete
        let path: Path = "data/test_complete.txt".into();
        let upload = object_store.put_multipart(&path).await.unwrap();

        let mut write = WriteMultipart::new(upload);

        let mut all_bytes = vec![];
        let round = rng.gen_range(1..=1024);
        for _ in 0..round {
            let size = rng.gen_range(1..=1024);
            let mut bytes = vec![0; size];
            rng.fill_bytes(&mut bytes);

            all_bytes.extend_from_slice(&bytes);
            write.put(bytes.into());
        }

        let _ = write.finish().await.unwrap();

        let meta = object_store.head(&path).await.unwrap();

        assert_eq!(meta.size, all_bytes.len());

        assert_eq!(
            object_store
                .get(&path)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from(all_bytes)
        );

        // Case abort
        let path: Path = "data/test_abort.txt".into();
        let mut upload = object_store.put_multipart(&path).await.unwrap();
        upload.put_part(vec![1; 1024].into()).await.unwrap();
        upload.abort().await.unwrap();

        let res = object_store.head(&path).await;
        let err = res.unwrap_err();

        assert!(matches!(err, object_store::Error::NotFound { .. }))
    }

    #[tokio::test]
    async fn test_list() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let results = object_store.list(Some(&path)).collect::<Vec<_>>().await;
        assert_eq!(results.len(), 2);
        let mut locations = results
            .iter()
            .map(|x| x.as_ref().unwrap().location.as_ref())
            .collect::<Vec<_>>();

        let expected_files = vec![
            (
                "data/nested/test.txt",
                Bytes::from_static(b"hello, world! I am nested."),
            ),
            ("data/test.txt", Bytes::from_static(b"hello, world!")),
        ];

        let expected_locations = expected_files.iter().map(|x| x.0).collect::<Vec<&str>>();

        locations.sort();
        assert_eq!(locations, expected_locations);

        for (location, bytes) in expected_files {
            let path: Path = location.into();
            assert_eq!(
                object_store
                    .get(&path)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap(),
                bytes
            );
        }
    }

    #[tokio::test]
    async fn test_list_with_delimiter() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let result = object_store.list_with_delimiter(Some(&path)).await.unwrap();
        assert_eq!(result.objects.len(), 1);
        assert_eq!(result.common_prefixes.len(), 1);
        assert_eq!(result.objects[0].location.as_ref(), "data/test.txt");
        assert_eq!(result.common_prefixes[0].as_ref(), "data/nested");
    }

    #[tokio::test]
    async fn test_list_with_offset() {
        let object_store = create_test_object_store().await;
        let path: Path = "data/".into();
        let offset: Path = "data/nested/test.txt".into();
        let result = object_store
            .list_with_offset(Some(&path), &offset)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].as_ref().unwrap().location.as_ref(),
            "data/test.txt"
        );
    }
}
