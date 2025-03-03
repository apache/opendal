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

use crate::raw::adapters::typed_kv;
use crate::raw::Access;
use crate::services::MemoryConfig;
use crate::*;

impl Configurator for MemoryConfig {
    type Builder = MemoryBuilder;
    fn into_builder(self) -> Self::Builder {
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
    pub fn root(mut self, path: &str) -> Self {
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
            &format!("{:p}", Arc::as_ptr(&self.inner)),
            typed_kv::Capability {
                get: true,
                set: true,
                delete: true,
                scan: true,
                shared: false,
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

        if path.is_empty() {
            return Ok(inner.keys().cloned().collect());
        }

        let mut keys = Vec::new();
        for (key, _) in inner.range(path.to_string()..) {
            if !key.starts_with(path) {
                break;
            }
            keys.push(key.to_string());
        }
        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raw::adapters::typed_kv::{Adapter, Value};

    #[test]
    fn test_accessor_metadata_name() {
        let b1 = MemoryBuilder::default().build().unwrap();
        assert_eq!(b1.info().name(), b1.info().name());

        let b2 = MemoryBuilder::default().build().unwrap();
        assert_ne!(b1.info().name(), b2.info().name())
    }

    #[test]
    fn test_blocking_scan() {
        let adapter = super::Adapter {
            inner: Arc::new(Mutex::new(BTreeMap::default())),
        };

        adapter.blocking_set("aaa/bbb/", Value::new_dir()).unwrap();
        adapter.blocking_set("aab/bbb/", Value::new_dir()).unwrap();
        adapter.blocking_set("aab/ccc/", Value::new_dir()).unwrap();
        adapter
            .blocking_set(&format!("aab{}aaa/", std::char::MAX), Value::new_dir())
            .unwrap();
        adapter.blocking_set("aac/bbb/", Value::new_dir()).unwrap();

        let data = adapter.blocking_scan("aab").unwrap();
        assert_eq!(data.len(), 3);
        for path in data {
            assert!(path.starts_with("aab"));
        }
    }
}
