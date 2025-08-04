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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{stream::BoxStream, StreamExt};
use object_store::{ObjectMeta, ObjectStore};
use opendal::raw::*;
use opendal::*;
use tokio::sync::Mutex;

use super::error::parse_error;

pub struct ObjectStoreLister {
    stream: Mutex<BoxStream<'static, object_store::Result<ObjectMeta>>>,
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
            stream: Mutex::new(stream),
        })
    }
}

impl oio::List for ObjectStoreLister {
    async fn next(&mut self) -> Result<Option<oio::Entry>> {
        let mut stream = self.stream.lock().await;
        match stream.next().await {
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
