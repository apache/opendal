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
use std::future::Future;
use std::sync::Arc;

use futures::FutureExt;
use object_store::ObjectStore;
use opendal::raw::*;
use opendal::*;

use super::error::parse_error;

pub struct ObjectStoreDeleter {
    store: Arc<dyn ObjectStore + 'static>,
    paths: VecDeque<object_store::path::Path>,
}

impl ObjectStoreDeleter {
    pub(crate) fn new(store: Arc<dyn ObjectStore + 'static>) -> Self {
        Self {
            store,
            paths: VecDeque::new(),
        }
    }
}

impl oio::Delete for ObjectStoreDeleter {
    fn delete(&mut self, path: &str, _: OpDelete) -> Result<()> {
        self.paths.push_back(object_store::path::Path::from(path));
        Ok(())
    }

    fn flush(&mut self) -> impl Future<Output = Result<usize>> + MaybeSend {
        async move {
            if self.paths.is_empty() {
                return Ok(0);
            }

            let mut count = 0;
            while let Some(path) = self.paths.front() {
                match self.store.delete(path).await {
                    Ok(_) => {
                        self.paths.pop_front();
                        count += 1;
                    }
                    Err(e) => {
                        return Err(parse_error(e));
                    }
                }
            }
            Ok(count)
        }
        .boxed()
    }
}
