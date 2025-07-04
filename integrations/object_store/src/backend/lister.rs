use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;

use futures::{stream::BoxStream, StreamExt};
use object_store::{ObjectMeta, ObjectStore};
use opendal::raw::*;
use opendal::*;

use super::error::parse_error;

pub struct ObjectStoreLister {
    stream: Arc<Mutex<BoxStream<'static, object_store::Result<ObjectMeta>>>>,
}

impl ObjectStoreLister {
    pub(crate) async fn new(
        store: Arc<dyn ObjectStore + 'static>,
        path: &str,
        args: OpList,
    ) -> Result<Self> {
        // If start_after is specified, use list_with_offset
        let mut stream = if let Some(start_after) = args.start_after() {
            store
                .list_with_offset(
                    Some(&object_store::path::Path::from(path)),
                    &object_store::path::Path::from(start_after),
                )
                .boxed()
        } else {
            store
                .list(Some(&object_store::path::Path::from(path)))
                .boxed()
        };

        // Process listing arguments
        if let Some(limit) = args.limit() {
            stream = stream.take(limit).boxed();
        }

        Ok(Self {
            stream: Arc::new(Mutex::new(stream)),
        })
    }
}

impl oio::List for ObjectStoreLister {
    fn next(&mut self) -> impl Future<Output = Result<Option<oio::Entry>>> + MaybeSend {
        let stream = self.stream.clone();
        async move {
            let next_item = {
                let mut stream = stream.lock().await;
                stream.next().await
            };
            match next_item {
                Some(Ok(meta)) => {
                    let mut metadata = Metadata::new(EntryMode::FILE);
                    let entry = oio::Entry::new(meta.location.as_ref(), metadata.clone());
                    metadata.set_content_length(meta.size);
                    metadata.set_last_modified(meta.last_modified);
                    if let Some(etag) = &meta.e_tag {
                        metadata.set_etag(etag.as_str());
                    }
                    if let Some(version) = meta.version {
                        metadata.set_version(version.as_str());
                    }
                    Ok(Some(entry))
                }
                Some(Err(e)) => Err(parse_error(e)),
                None => Ok(None),
            }
        }
    }
}
