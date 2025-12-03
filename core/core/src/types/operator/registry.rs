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

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};

use crate::types::builder::{Builder, Configurator};
use crate::types::{IntoOperatorUri, OperatorUri};
use crate::{Error, ErrorKind, Operator, Result};

/// Factory signature used to construct [`Operator`] from a URI and extra options.
pub type OperatorFactory = fn(&OperatorUri) -> Result<Operator>;

/// Default registry initialized with builtin services.
pub static DEFAULT_OPERATOR_REGISTRY: LazyLock<OperatorRegistry> = LazyLock::new(|| {
    let registry = OperatorRegistry::new();
    register_builtin_services(&registry);
    registry
});

/// Global registry that maps schemes to [`OperatorFactory`] functions.
#[derive(Debug, Default)]
pub struct OperatorRegistry {
    factories: Mutex<HashMap<String, OperatorFactory>>,
}

impl OperatorRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self {
            factories: Mutex::new(HashMap::new()),
        }
    }

    /// Register a builder for the given scheme.
    pub fn register<B: Builder>(&self, scheme: &str) {
        let key = scheme.to_ascii_lowercase();
        let mut guard = self
            .factories
            .lock()
            .expect("operator registry mutex poisoned");
        guard.insert(key, factory::<B::Config>);
    }

    /// Load an [`Operator`] via the factory registered for the URI's scheme.
    pub fn load(&self, uri: impl IntoOperatorUri) -> Result<Operator> {
        let parsed = uri.into_operator_uri()?;
        let scheme = parsed.scheme();

        let factory = self
            .factories
            .lock()
            .expect("operator registry mutex poisoned")
            .get(scheme)
            .copied()
            .ok_or_else(|| {
                Error::new(ErrorKind::Unsupported, "scheme is not registered")
                    .with_context("scheme", scheme.to_string())
            })?;

        factory(&parsed)
    }
}

fn register_builtin_services(registry: &OperatorRegistry) {
    let _ = registry;

    #[cfg(feature = "services-memory")]
    registry.register::<crate::services::Memory>(crate::services::MEMORY_SCHEME);
    #[cfg(feature = "services-fs")]
    registry.register::<crate::services::Fs>(crate::services::FS_SCHEME);
    #[cfg(feature = "services-s3")]
    registry.register::<crate::services::S3>(crate::services::S3_SCHEME);
    #[cfg(feature = "services-azblob")]
    registry.register::<crate::services::Azblob>(crate::services::AZBLOB_SCHEME);
    #[cfg(feature = "services-b2")]
    registry.register::<crate::services::B2>(crate::services::B2_SCHEME);
    #[cfg(feature = "services-cos")]
    registry.register::<crate::services::Cos>(crate::services::COS_SCHEME);
    #[cfg(feature = "services-gcs")]
    registry.register::<crate::services::Gcs>(crate::services::GCS_SCHEME);
    #[cfg(feature = "services-obs")]
    registry.register::<crate::services::Obs>(crate::services::OBS_SCHEME);
    #[cfg(feature = "services-oss")]
    registry.register::<crate::services::Oss>(crate::services::OSS_SCHEME);
    #[cfg(feature = "services-upyun")]
    registry.register::<crate::services::Upyun>(crate::services::UPYUN_SCHEME);
}

/// Factory adapter that builds an operator from a configurator type.
fn factory<C: Configurator>(uri: &OperatorUri) -> Result<Operator> {
    let cfg = C::from_uri(uri)?;
    let builder = Operator::from_config(cfg)?;
    Ok(builder.finish())
}
