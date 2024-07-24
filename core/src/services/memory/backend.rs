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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

use serde::Deserialize;
use serde::Serialize;

use crate::raw::adapters::typed_kv;
use crate::raw::Access;
use crate::*;

///Config for memory.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MemoryConfig {
    /// root of the backend.
    pub root: Option<String>,
}

impl Configurator for MemoryConfig {
    fn into_builder(self) -> impl Builder {
        MemoryBuilder { config: self }
    }
}

/// In memory service support. (BTreeMap Based)
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct MemoryBuilder {
    config: MemoryConfig,
}

impl MemoryBuilder {
    /// Set the root for BTreeMap.
    pub fn root(&mut self, path: &str) -> &mut Self {
        self.config.root = Some(path.into());
        self
    }
}

impl Builder for MemoryBuilder {
    const SCHEME: Scheme = Scheme::Memory;
    type Config = MemoryConfig;

    fn build(self) -> Result<impl Access> {
        let adapter = Adapter {
            inner: Arc::new(Mutex::new(BTreeMap::default())),
        };

        Ok(MemoryBackend::new(adapter).with_root(self.config.root.as_deref().unwrap_or_default()))
    }
}

/// Backend is used to serve `Accessor` support in memory.
pub type MemoryBackend = typed_kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    inner: Arc<Mutex<BTreeMap<String, typed_kv::Value>>>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryBackend").finish_non_exhaustive()
    }
}

impl typed_kv::Adapter for Adapter {
    fn info(&self) -> typed_kv::Info {
        typed_kv::Info::new(
            Scheme::Memory,
            &format!("{:?}", &self.inner as *const _),
            typed_kv::Capability {
                get: true,
                set: true,
                delete: true,
                scan: true,
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<typed_kv::Value>> {
        self.blocking_get(path)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<typed_kv::Value>> {
        match self.inner.lock().unwrap().get(path) {
            None => Ok(None),
            Some(bs) => Ok(Some(bs.to_owned())),
        }
    }

    async fn set(&self, path: &str, value: typed_kv::Value) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: typed_kv::Value) -> Result<()> {
        self.inner.lock().unwrap().insert(path.to_string(), value);

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        self.inner.lock().unwrap().remove(path);

        Ok(())
    }

    async fn scan(&self, path: &str) -> Result<Vec<String>> {
        self.blocking_scan(path)
    }

    fn blocking_scan(&self, path: &str) -> Result<Vec<String>> {
        let inner = self.inner.lock().unwrap();
        let keys: Vec<_> = if path.is_empty() {
            inner.keys().cloned().collect()
        } else {
            let right_range = if let Some(path) = path.strip_suffix('/') {
                format!("{}0", path)
            } else {
                format!("{}{}", path, std::char::MAX)
            };
            inner
                .range(path.to_string()..right_range)
                .filter(|(k, _)| k.as_str() != path)
                .map(|(k, _)| k.to_string())
                .collect()
        };

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accessor_metadata_name() {
        let b1 = MemoryBuilder::default().build().unwrap();
        assert_eq!(b1.info().name(), b1.info().name());

        let b2 = MemoryBuilder::default().build().unwrap();
        assert_ne!(b1.info().name(), b2.info().name())
    }
}
