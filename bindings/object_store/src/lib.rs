// Copyright 2023 Datafuse Labs.
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

use async_trait::async_trait;
use bytes::Bytes;
use futures::{stream::BoxStream, StreamExt};
use object_store::path::Path;
use object_store::{GetResult, ListResult, MultipartId, ObjectMeta, ObjectStore, Result};
use opendal::Operator;
use std::ops::Range;
use tokio::io::AsyncWrite;

#[derive(Debug)]
pub struct OpendalStore {
    inner: Operator,
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

        Ok(GetResult::Stream(Box::pin(r)))
    }

    async fn get_range(&self, location: &Path, range: Range<usize>) -> Result<Bytes> {
        let o = self.inner.object(location.as_ref());
        let bs = o
            .range_read(range)
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(Bytes::from(bs))
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let o = self.inner.object(location.as_ref());
        let meta = o
            .metadata()
            .await
            .map_err(|err| format_object_store_error(err, location.as_ref()))?;

        Ok(ObjectMeta {
            location: location.clone(),
            last_modified: meta.last_modified().unwrap_or_default(),
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

    async fn list(&self, prefix: Option<&Path>) -> Result<BoxStream<'_, Result<ObjectMeta>>> {
        todo!()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        todo!()
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "copy is not implemented so far",
            )),
        })
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        Err(object_store::Error::NotSupported {
            source: Box::new(opendal::Error::new(
                opendal::ErrorKind::Unsupported,
                "rename is not implemented so far",
            )),
        })
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
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
        _ => object_store::Error::Generic {
            store: "BackendConfigInvalid",
            source: Box::new(err),
        },
    }
}
