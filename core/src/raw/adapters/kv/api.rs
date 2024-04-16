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

use async_trait::async_trait;

use crate::raw::*;
use crate::Capability;
use crate::Scheme;
use crate::*;

/// KvAdapter is the adapter to underlying kv services.
///
/// By implement this trait, any kv service can work as an OpenDAL Service.
#[async_trait]
pub trait Adapter: Send + Sync + Debug + Unpin + 'static {
    /// Return the metadata of this key value accessor.
    fn metadata(&self) -> Metadata;

    /// Get a key from service.
    ///
    /// - return `Ok(None)` if this key is not exist.
    async fn get(&self, path: &str) -> Result<Option<Buffer>>;

    /// The blocking version of get.
    fn blocking_get(&self, path: &str) -> Result<Option<Buffer>> {
        let _ = path;

        Err(Error::new(
            ErrorKind::Unsupported,
            "kv adapter doesn't support this operation",
        )
        .with_operation("kv::Adapter::blocking_get"))
    }

    /// Set a key into service.
    async fn set(&self, path: &str, value: &[u8]) -> Result<()>;

    /// The blocking version of set.
    fn blocking_set(&self, path: &str, value: &[u8]) -> Result<()> {
        let _ = (path, value);

        Err(Error::new(
            ErrorKind::Unsupported,
            "kv adapter doesn't support this operation",
        )
        .with_operation("kv::Adapter::blocking_set"))
    }

    /// Delete a key from service.
    ///
    /// - return `Ok(())` even if this key is not exist.
    async fn delete(&self, path: &str) -> Result<()>;

    /// Delete a key from service in blocking way.
    ///
    /// - return `Ok(())` even if this key is not exist.
    fn blocking_delete(&self, path: &str) -> Result<()> {
        let _ = path;

        Err(Error::new(
            ErrorKind::Unsupported,
            "kv adapter doesn't support this operation",
        )
        .with_operation("kv::Adapter::blocking_delete"))
    }

    /// Scan a key prefix to get all keys that start with this key.
    async fn scan(&self, path: &str) -> Result<Vec<String>> {
        let _ = path;

        Err(Error::new(
            ErrorKind::Unsupported,
            "kv adapter doesn't support this operation",
        )
        .with_operation("kv::Adapter::scan"))
    }

    /// Scan a key prefix to get all keys that start with this key
    /// in blocking way.
    fn blocking_scan(&self, path: &str) -> Result<Vec<String>> {
        let _ = path;

        Err(Error::new(
            ErrorKind::Unsupported,
            "kv adapter doesn't support this operation",
        )
        .with_operation("kv::Adapter::blocking_scan"))
    }

    /// Append a key into service
    async fn append(&self, path: &str, value: &[u8]) -> Result<()> {
        let _ = path;
        let _ = value;

        Err(Error::new(
            ErrorKind::Unsupported,
            "kv adapter doesn't support this operation",
        )
        .with_operation("kv::Adapter::append"))
    }

    /// Append a key into service
    /// in blocking way.
    fn blocking_append(&self, path: &str, value: &[u8]) -> Result<()> {
        let _ = path;
        let _ = value;

        Err(Error::new(
            ErrorKind::Unsupported,
            "kv adapter doesn't support this operation",
        )
        .with_operation("kv::Adapter::blocking_append"))
    }
}

/// Metadata for this key value accessor.
pub struct Metadata {
    scheme: Scheme,
    name: String,
    capabilities: Capability,
}

impl Metadata {
    /// Create a new KeyValueAccessorInfo.
    pub fn new(scheme: Scheme, name: &str, capabilities: Capability) -> Self {
        Self {
            scheme,
            name: name.to_string(),
            capabilities,
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
    pub fn capabilities(&self) -> Capability {
        self.capabilities
    }
}

impl From<Metadata> for AccessorInfo {
    fn from(m: Metadata) -> AccessorInfo {
        let mut am = AccessorInfo::default();
        am.set_name(m.name());
        am.set_scheme(m.scheme());
        am.set_native_capability(m.capabilities());

        am
    }
}
