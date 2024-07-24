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

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::raw::*;
use crate::*;

/// Builder is used to set up underlying services.
///
/// This trait allows the developer to define a builder struct that can:
///
/// - build a service via builder style API.
/// - configure in-memory options like `http_client` or `customized_credential_load`.
///
/// Usually, users don't need to use or import this trait directly, they can use `Operator` API instead.
///
/// For example:
///
/// ```
/// # use anyhow::Result;
/// use opendal::services::Fs;
/// use opendal::Operator;
/// async fn test() -> Result<()> {
///     // Create fs backend builder.
///     let mut builder = Fs::default().root("/tmp");
///
///     // Build an `Operator` to start operating the storage.
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
pub trait Builder: Default + 'static {
    /// Associated scheme for this builder. It indicates what underlying service is.
    const SCHEME: Scheme;
    /// Associated configuration for this builder.
    type Config: Configurator;

    /// Consume the accessor builder to build a service.
    fn build(self) -> Result<impl Access>;
}

/// Dummy implementation of builder
impl Builder for () {
    const SCHEME: Scheme = Scheme::Custom("dummy");
    type Config = ();

    fn build(self) -> Result<impl Access> {
        Ok(())
    }
}

/// Configurator is used to configure the underlying service.
///
/// This trait allows the developer to define a configuration struct that can:
///
/// - deserialize from an iterator like hashmap or vector.
/// - convert into a service builder and finally build the underlying services.
///
/// Usually, users don't need to use or import this trait directly, they can use `Operator` API instead.
///
/// For example:
///
/// ```
/// # use anyhow::Result;
/// use std::collections::HashMap;
///
/// use opendal::services::MemoryConfig;
/// use opendal::Operator;
/// async fn test() -> Result<()> {
///     let mut cfg = MemoryConfig::default();
///     cfg.root = Some("/".to_string());
///
///     // Build an `Operator` to start operating the storage.
///     let op: Operator = Operator::from_config(cfg)?.finish();
///
///     Ok(())
/// }
/// ```
pub trait Configurator: Serialize + DeserializeOwned + 'static {
    /// Deserialize from an iterator.
    ///
    /// This API is provided by opendal, developer should not implement it.
    fn from_iter(iter: impl IntoIterator<Item = (String, String)>) -> Result<Self> {
        let cfg = ConfigDeserializer::new(iter.into_iter().collect());

        Self::deserialize(cfg).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "failed to deserialize config").set_source(err)
        })
    }

    /// Convert this configuration into a service builder.
    fn into_builder(self) -> impl Builder;
}

impl Configurator for () {
    fn into_builder(self) -> impl Builder {}
}
