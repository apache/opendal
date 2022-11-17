// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::io::Result;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::adapters::kv;
use crate::Accessor;
use crate::AccessorCapability;
use crate::Scheme;

/// Builder for memory backend
#[derive(Default)]
pub struct Builder {}

impl Builder {
    /// Consume builder to build a memory backend.
    pub fn build(&mut self) -> Result<impl Accessor> {
        let adapter = Adapter {
            inner: Arc::new(Mutex::new(BTreeMap::default())),
        };

        Ok(Backend::new(adapter))
    }
}

/// Backend is used to serve `Accessor` support in memory.
pub type Backend = kv::Backend<Adapter>;

#[derive(Debug, Clone)]
pub struct Adapter {
    inner: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Memory,
            &format!("{:?}", &self.inner as *const _),
            AccessorCapability::Read | AccessorCapability::Write,
        )
    }

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.inner.lock().get(key) {
            None => Ok(None),
            Some(bs) => Ok(Some(bs.to_vec())),
        }
    }

    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.lock().insert(key.to_vec(), value.to_vec());

        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.lock().remove(key);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Accessor;

    #[test]
    fn test_accessor_metadata_name() {
        let b1 = Builder::default().build().unwrap();
        assert_eq!(b1.metadata().name(), b1.metadata().name());

        let b2 = Builder::default().build().unwrap();
        assert_ne!(b1.metadata().name(), b2.metadata().name())
    }
}
