// Copyright 2022 Datafuse Labs
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

use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use dashmap::DashMap;

use crate::raw::adapters::kv;
use crate::raw::*;
use crate::*;

/// [dashmap](https://github.com/xacrimon/dashmap) backend support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [ ] ~~list~~
/// - [ ] ~~scan~~
/// - [ ] ~~presign~~
/// - [ ] ~~multipart~~
/// - [x] blocking
#[derive(Default)]
pub struct DashmapBuilder {}

impl Builder for DashmapBuilder {
    const SCHEME: Scheme = Scheme::Dashmap;
    type Accessor = DashmapBackend;

    fn from_map(_: HashMap<String, String>) -> Self {
        Self::default()
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        Ok(DashmapBackend::new(Adapter {
            inner: DashMap::default(),
        }))
    }
}

/// Backend is used to serve `Accessor` support in dashmap.
pub type DashmapBackend = kv::Backend<Adapter>;

#[derive(Debug, Clone)]
pub struct Adapter {
    inner: DashMap<String, Vec<u8>>,
}

#[async_trait]
impl kv::Adapter for Adapter {
    fn metadata(&self) -> kv::Metadata {
        kv::Metadata::new(
            Scheme::Dashmap,
            &format!("{:?}", &self.inner as *const _),
            AccessorCapability::Read | AccessorCapability::Write,
        )
    }

    async fn get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        self.blocking_get(path)
    }

    fn blocking_get(&self, path: &str) -> Result<Option<Vec<u8>>> {
        match self.inner.get(path) {
            None => Ok(None),
            Some(bs) => Ok(Some(bs.to_vec())),
        }
    }

    async fn set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.blocking_set(path, value)
    }

    fn blocking_set(&self, path: &str, value: &[u8]) -> Result<()> {
        self.inner.insert(path.to_string(), value.to_vec());

        Ok(())
    }

    async fn delete(&self, path: &str) -> Result<()> {
        self.blocking_delete(path)
    }

    fn blocking_delete(&self, path: &str) -> Result<()> {
        self.inner.remove(path);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_accessor_metadata_name() {
        let b1 = DashmapBuilder::default().build().unwrap();
        assert_eq!(b1.metadata().name(), b1.metadata().name());

        let b2 = DashmapBuilder::default().build().unwrap();
        assert_ne!(b1.metadata().name(), b2.metadata().name())
    }
}
