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

use std::fmt::Debug;
use std::io::Result;

use async_trait::async_trait;
use flagset::FlagSet;

use crate::AccessorCapability;
use crate::AccessorMetadata;
use crate::Scheme;

/// KvAdapter is the adapter to underlying kv services.
///
/// By implement this trait, any kv service can work as an OpenDAL Service.
#[async_trait]
pub trait Adapter: Send + Sync + Debug + Clone + 'static {
    /// Return the medata of this key value accessor.
    fn metadata(&self) -> Metadata;

    /// Get a key from service.
    ///
    /// - return `Ok(None)` if this key is not exist.
    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Set a key into service.
    async fn set(&self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Delete a key from service.
    ///
    /// - return `Ok(())` even if this key is not exist.
    async fn delete(&self, key: &[u8]) -> Result<()>;
}

/// Metadata for this key value accessor.
pub struct Metadata {
    scheme: Scheme,
    name: String,
    capabilities: FlagSet<AccessorCapability>,
}

impl Metadata {
    /// Create a new KeyValueAccessorMetadata.
    pub fn new(
        scheme: Scheme,
        name: &str,
        capabilities: impl Into<FlagSet<AccessorCapability>>,
    ) -> Self {
        Self {
            scheme,
            name: name.to_string(),
            capabilities: capabilities.into(),
        }
    }

    /// Get the scheme.
    pub fn scheme(&self) -> Scheme {
        self.scheme
    }

    /// Get the name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the capabilities.
    pub fn capabilities(&self) -> FlagSet<AccessorCapability> {
        self.capabilities
    }
}

impl From<Metadata> for AccessorMetadata {
    fn from(m: Metadata) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_name(m.name());
        am.set_scheme(m.scheme());
        am.set_capabilities(m.capabilities());

        am
    }
}
