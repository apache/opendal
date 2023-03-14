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
use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

/// In memory service support. (HashMap Based)
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [ ] ~~list~~
/// - [x] scan
/// - [ ] ~~presign~~
/// - [x] blocking
#[derive(Default)]
pub struct MemoryBuilder {}

impl Builder for MemoryBuilder {
    const SCHEME: Scheme = Scheme::Memory;
    type Accessor = MemoryBackend;

    fn from_map(_: HashMap<String, String>) -> Self {
        Self::default()
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let adapter = Adapter {
            inner: Arc::new(Mutex::new(BTreeMap::default())),
        };

        Ok(MemoryBackend::new(adapter))
    }
}

/// Backend is used to serve `Accessor` support in memory.
pub type MemoryBackend = kv::Backend<Adapter>;

#[derive(Debug, Clone)]
pub struct Adapter {
    inner: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Memory,
            &format!("{:?}", &self.inner as *const _),
            AccessorCapability::Read | AccessorCapability::Write | AccessorCapability::Scan,
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.blocking_get(path)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        match self.inner.lock().get(path) {
            None => Ok(None),
            Some(bs) => Ok(Some(bs.to_vec())),
        }
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.inner.lock().insert(path.to_string(), value.to_vec());

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        self.inner.lock().remove(path);

        Ok(())
    }

    async fn scan(&self, path: &str) -> Result<Vec<String>> {
        self.blocking_scan(path)
    }

    fn blocking_scan(&self, path: &str) -> Result<Vec<String>> {
        let inner = self.inner.lock();
        let keys: Vec<_> = if path.is_empty() {
            inner.keys().cloned().collect()
        } else {
            let right_range = format!("{}0", &path[..path.len() - 1]);
            inner
                .range(path.to_string()..right_range)
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
