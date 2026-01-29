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

use std::sync::Arc;

use opendal_core::Result;
use opendal_core::raw::Access;
use opendal_core::raw::OpDelete;
use opendal_core::raw::oio;

use crate::FoyerKey;
use crate::Inner;

pub struct Deleter<A: Access> {
    pub(crate) deleter: A::Deleter,
    pub(crate) keys: Vec<FoyerKey>,
    pub(crate) inner: Arc<Inner<A>>,
}

impl<A: Access> Deleter<A> {
    pub(crate) fn new(deleter: A::Deleter, inner: Arc<Inner<A>>) -> Self {
        Self {
            deleter,
            keys: vec![],
            inner,
        }
    }
}

impl<A: Access> oio::Delete for Deleter<A> {
    async fn delete(&mut self, path: &str, args: OpDelete) -> Result<()> {
        self.deleter.delete(path, args.clone()).await?;
        self.keys.push(FoyerKey::Full {
            path: path.to_string(),
            version: args.version().map(|v| v.to_string()),
        });
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        for key in &self.keys {
            self.inner.cache.remove(key);
        }
        self.deleter.close().await
    }
}
