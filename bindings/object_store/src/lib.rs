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

use std::ops::Range;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use chrono::NaiveDateTime;
use chrono::Utc;
use futures::stream::BoxStream;
use futures::Stream;
use object_store::path::Path;
use object_store::GetResult;
use object_store::ListResult;
use object_store::MultipartId;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use object_store::Result;
use opendal::ObjectReader;
use opendal::Operator;
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct OpendalStore {
    inner: Operator,
}

impl OpendalStore {
    /// Create OpendalStore by given Operator.
    pub fn new(op: Operator) -> Self {
        Self { inner: op }
    }
}

impl std::fmt::Display for OpendalStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "OpenDAL({:?})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for OpendalStore {
    async fn put(&self, location: &Path, bytes: Bytes) -> Result<()> {
        let o = self.inner.object(location.as_ref());
        Ok(o.write(bytes)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?)
    }

    async fn put_multipart(
        &self,
        _location: &Path,
    ) -> Result<(MultipartId, Box<dyn AsyncWrite + Unpin + Send>)> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "put_multipart is not implemented so far",
            )),
        })
    }

    async fn abort_multipart(&self, _location: &Path, _multipart_id: &MultipartId) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "abort_multipart is not implemented so far",
            )),
        })
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        let o = self.inner.object(location.as_ref());
        let r = o
            .reader()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(GetResult::Stream(Box::pin(OpendalReader { inner: r })))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let o = self.inner.object(location.as_ref());
        let bs = o
            .range_read(range.start as u64..range.end as u64)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(Bytes::from(bs))
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let o = self.inner.object(location.as_ref());
        let meta = o
            .stat()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        let (secs, nsecs) = meta
            .last_modified()
            .map(|v| (v.unix_timestamp(), v.nanosecond()))
            .unwrap_or((0, 0));

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: DateTime::from_utc(
                NaiveDateTime::from_timestamp_opt(secs, nsecs)
                    .expect("returning timestamp must be valid"),
                Utc,
            ),
            size: meta.content_length() as usize,
        })
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let o = self.inner.object(location.as_ref());
        o.delete()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(())
    }

    async fn list(&self, _prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        todo!()
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> Result<ListResult> {
        todo!()
    }

    async fn copy(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "copy is not implemented so far",
            )),
        })
    }

    async fn rename(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "rename is not implemented so far",
            )),
        })
    }

    async fn copy_if_not_exists(&self, _from: &Path, _to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "copy_if_not_exists is not implemented so far",
            )),
        })
    }
}

fn format_object_store_error(err: opendal::Error, path: &str) -> object_store::Error {
    use opendal::ErrorKind;
    match err.kind() {
        ErrorKind::ObjectNotFound => object_store::Error::NotFound {
            path: path.to_string(),
            source: Box::new(err),
        },
        ErrorKind::Unsupported => object_store::Error::NotSupported {
            source: Box::new(err),
        },
        ErrorKind::ObjectAlreadyExists => object_store::Error::AlreadyExists {
            path: path.to_string(),
            source: Box::new(err),
        },
        kind => object_store::Error::Generic {
            store: kind.into_static(),
            source: Box::new(err),
        },
    }
}

struct OpendalReader {
    inner: ObjectReader,
}

impl Stream for OpendalReader {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        use opendal::raw::output::Read;

        self.inner
            .poll_next(cx)
            .map_err(|err| object_store::Error::Generic {
                store: "IoError",
                source: Box::new(err),
            })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::path::Path;
    use object_store::ObjectStore;
    use opendal::services;

    use super::*;

    #[tokio::test]
    async fn test_basic() {
        let op = Operator::create(services::Memory::default())
            .unwrap()
            .finish();
        let object_store: Arc<dyn ObjectStore> = Arc::new(OpendalStore::new(op));

        // Retrieve a specific file
        let path: Path = "data/test.txt".try_into().unwrap();

        let bytes = Bytes::from_static(b"hello, world!");
        object_store.put(&path, bytes).await.unwrap();

        let meta = object_store.head(&path).await.unwrap();

        assert_eq!(meta.size, 13)
    }
}
