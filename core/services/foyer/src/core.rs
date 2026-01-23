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

use std::fmt::Debug;

use foyer::HybridCache;

use opendal_core::Buffer;
use opendal_core::Result;

use super::FoyerKey;
use super::FoyerValue;

/// FoyerCore holds the foyer HybridCache instance.
#[derive(Clone)]
pub struct FoyerCore {
    pub(crate) cache: HybridCache<FoyerKey, FoyerValue>,
    pub(crate) name: Option<String>,
}

impl Debug for FoyerCore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerCore")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl FoyerCore {
    pub fn new(cache: HybridCache<FoyerKey, FoyerValue>, name: Option<String>) -> Self {
        Self { cache, name }
    }

    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub async fn get(&self, key: &str) -> Result<Option<Buffer>> {
        let foyer_key = FoyerKey {
            path: key.to_string(),
        };

        match self.cache.get(&foyer_key).await {
            Ok(Some(entry)) => Ok(Some(entry.value().0.clone())),
            Ok(None) => Ok(None),
            Err(_) => Ok(None),
        }
    }

    pub fn insert(&self, key: &str, value: Buffer) {
        let foyer_key = FoyerKey {
            path: key.to_string(),
        };
        self.cache.insert(foyer_key, FoyerValue(value));
    }

    pub fn remove(&self, key: &str) {
        let foyer_key = FoyerKey {
            path: key.to_string(),
        };
        self.cache.remove(&foyer_key);
    }
}
