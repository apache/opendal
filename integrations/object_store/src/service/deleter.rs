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

use std::collections::VecDeque;
use std::sync::Arc;

use futures::stream::{self, StreamExt};
use object_store::ObjectStore;
use opendal::raw::oio::BatchDeleteResult;
use opendal::raw::*;
use opendal::*;

use super::error::parse_error;

pub struct ObjectStoreDeleter {
    store: Arc<dyn ObjectStore + 'static>,
}

impl ObjectStoreDeleter {
    pub(crate) fn new(store: Arc<dyn ObjectStore + 'static>) -> Self {
        Self { store }
    }
}

impl oio::BatchDelete for ObjectStoreDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let object_path = object_store::path::Path::from(path);
        self.store.delete(&object_path).await.map_err(parse_error)
    }

    async fn delete_batch(&self, paths: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        // convert paths to stream, then use [`ObjectStore::delete_stream`] to delete them in batch
        let stream = stream::iter(paths.iter())
            .map(|(path, _)| {
                Ok::<_, object_store::Error>(object_store::path::Path::from(path.as_str()))
            })
            .boxed();
        let results = self.store.delete_stream(stream).collect::<Vec<_>>().await;

        // convert the results to [`BatchDeleteResult`]
        let mut result_batch = BatchDeleteResult::default();
        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(_) => result_batch
                    .succeeded
                    .push((paths[idx].0.clone(), paths[idx].1.clone())),
                Err(e) => result_batch.failed.push((
                    paths[idx].0.clone(),
                    paths[idx].1.clone(),
                    parse_error(e),
                )),
            }
        }

        Ok(result_batch)
    }
}
