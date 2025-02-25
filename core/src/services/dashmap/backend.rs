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

use dashmap::DashMap;
use std::fmt::Debug;
use std::fmt::Formatter;

use crate::raw::adapters::typed_kv;
use crate::raw::Access;
use crate::services::DashmapConfig;
use crate::*;

impl Configurator for DashmapConfig {
    type Builder = DashmapBuilder;
    fn into_builder(self) -> Self::Builder {
        DashmapBuilder { config: self }
    }
}

/// [dashmap](https://github.com/xacrimon/dashmap) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct DashmapBuilder {
    config: DashmapConfig,
}

impl DashmapBuilder {
    /// Set the root for dashmap.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = if path.is_empty() {
            None
        } else {
            Some(path.to_string())
        };

        self
    }
}

impl Builder for DashmapBuilder {
    const SCHEME: Scheme = Scheme::Dashmap;
    type Config = DashmapConfig;

    fn build(self) -> Result<impl Access> {
        let mut backend = DashmapBackend::new(Adapter {
            inner: DashMap::default(),
        });
        if let Some(v) = self.config.root {
            backend = backend.with_root(&v);
        }

        Ok(backend)
    }
}

/// Backend is used to serve `Accessor` support in dashmap.
pub type DashmapBackend = typed_kv::Backend<Adapter>;

#[derive(Clone)]
pub struct Adapter {
    inner: DashMap<String, typed_kv::Value>,
}

impl Debug for Adapter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DashmapAdapter")
            .field("size", &self.inner.len())
            .finish_non_exhaustive()
    }
}

impl typed_kv::Adapter for Adapter {
    fn info(&self) -> typed_kv::Info {
        typed_kv::Info::new(
            Scheme::Dashmap,
            "dashmap",
            typed_kv::Capability {
                get: true,
                set: true,
                scan: true,
                delete: true,
                shared: false,
            },
        )
    }

    async fn get(&self, path: &str) -> Result<Option<typed_kv::Value>> {
        self.blocking_get(path)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<typed_kv::Value>> {
        match self.inner.get(path) {
            None => Ok(None),
            Some(bs) => Ok(Some(bs.value().to_owned())),
        }
    }

    async fn set(&self, path: &str, value: typed_kv::Value) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: typed_kv::Value) -> Result<()> {
        self.inner.insert(path.to_string(), value);

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        self.inner.remove(path);

        Ok(())
    }

    async fn scan(&self, path: &str) -> Result<Vec<String>> {
        self.blocking_scan(path)
    }

    fn blocking_scan(&self, path: &str) -> Result<Vec<String>> {
        let keys = self.inner.iter().map(|kv| kv.key().to_string());
        if path.is_empty() {
            Ok(keys.collect())
        } else {
            Ok(keys.filter(|k| k.starts_with(path)).collect())
        }
    }
}
