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

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use crate::raw::{Layer, Servicer};
use crate::types::builder::{Builder, Configurator};
use crate::types::{IntoOperatorUri, OperatorUri};
use crate::{Error, ErrorKind, OperationContext, Operator, Result};

/// Factory signature used to construct [`Operator`] from a URI and extra options.
pub type OperatorFactory = fn(&OperatorUri) -> Result<Operator>;

type ServiceFactory = fn(&OperatorUri) -> Result<Servicer>;
type ContextFactory =
    dyn Fn(OperationContext, HashMap<String, String>) -> Result<OperationContext> + Send + Sync;
type LayerFactory = dyn Fn(HashMap<String, String>) -> Result<Arc<dyn Layer>> + Send + Sync;

/// Global registry for constructing operators from configuration.
pub struct OperatorRegistry {
    factories: Mutex<Factories>,
}

#[derive(Default)]
struct Factories {
    services: HashMap<String, ServiceFactory>,
    contexts: HashMap<String, Arc<ContextFactory>>,
    layers: HashMap<String, Arc<LayerFactory>>,
}

impl fmt::Debug for OperatorRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let factories = self.lock_factories();
        f.debug_struct("OperatorRegistry")
            .field("services", &factories.services.keys())
            .field("contexts", &factories.contexts.keys())
            .field("layers", &factories.layers.keys())
            .finish()
    }
}

impl OperatorRegistry {
    /// Get the global registry.
    pub fn get() -> &'static Self {
        /// The global registry used by [`Operator::from_uri`].
        ///
        /// OpenDAL always registers the `memory` service because OpenDAL uses it
        /// extensively in unit tests and it has no dependencies.
        ///
        /// The `opendal` facade crate registers other optional services. See
        /// `core/src/lib.rs` in the facade crate for details.
        static OPERATOR_REGISTRY: LazyLock<OperatorRegistry> = LazyLock::new(|| {
            let registry = OperatorRegistry {
                factories: Mutex::default(),
            };
            crate::services::register_memory_service(&registry);
            registry
        });

        &OPERATOR_REGISTRY
    }

    fn lock_factories(&self) -> MutexGuard<'_, Factories> {
        self.factories
            .lock()
            .expect("operator registry mutex poisoned")
    }

    /// Register a builder for the given scheme.
    pub fn register<B: Builder>(&self, scheme: &str) {
        self.lock_factories()
            .services
            .insert(scheme.to_ascii_lowercase(), service_factory::<B::Config>);
    }

    /// Register a context factory for `context.<name>.*` options.
    ///
    /// Names are matched case-insensitively in ASCII and use only letters,
    /// digits, underscores, and hyphens. Registering the same name replaces its
    /// factory for future loads. The factory receives the current base context
    /// and the options with `context.<name>.` removed from their keys. Return
    /// the input context unchanged when no options are recognized.
    pub fn register_context<F>(&self, name: &str, factory: F)
    where
        F: Fn(OperationContext, HashMap<String, String>) -> Result<OperationContext>
            + Send
            + Sync
            + 'static,
    {
        let previous = self
            .lock_factories()
            .contexts
            .insert(name.to_ascii_lowercase(), Arc::new(factory));
        // A replaced factory can own user state whose destructor accesses the registry.
        drop(previous);
    }

    /// Register a layer factory for a name in `layers`.
    ///
    /// Names are matched case-insensitively in ASCII and use only letters,
    /// digits, underscores, and hyphens. Registering the same name replaces its
    /// factory for future loads. The factory receives the options with
    /// `layer.<name>.` removed from their keys, or an empty map if none are set.
    pub fn register_layer<L, F>(&self, name: &str, factory: F)
    where
        L: Layer,
        F: Fn(HashMap<String, String>) -> Result<L> + Send + Sync + 'static,
    {
        let factory = move |options| Ok(Arc::new(factory(options)?) as Arc<dyn Layer>);
        let previous = self
            .lock_factories()
            .layers
            .insert(name.to_ascii_lowercase(), Arc::new(factory));
        drop(previous);
    }

    /// Return the set of schemes currently registered.
    ///
    /// For the global registry returned by [`OperatorRegistry::get`], this is
    /// exactly the set of schemes that [`Operator::from_uri`] can construct, as
    /// determined by the compiled-in `services-*` features.
    ///
    /// ```
    /// use opendal_core::OperatorRegistry;
    ///
    /// let schemes = OperatorRegistry::get().schemes();
    /// assert!(schemes.contains("memory"));
    /// ```
    pub fn schemes(&self) -> HashSet<String> {
        self.lock_factories().services.keys().cloned().collect()
    }

    /// Load an [`Operator`] from trusted URI and key-value configuration.
    ///
    /// The URI scheme selects the service. Options under `context.<name>.` build
    /// the base context in name order. The comma-separated `layers` option names
    /// layers from inner to outer. Options under `layer.<name>.` configure a layer
    /// only when that name appears in the list. Other options go to the service.
    ///
    /// Malformed scoped keys, invalid layer lists, and duplicate layer names
    /// return [`ErrorKind::ConfigInvalid`]. An unregistered scheme, context, or
    /// active layer returns [`ErrorKind::Unsupported`].
    ///
    /// Only pass URIs and options from trusted sources. URI query parameters can
    /// specify context and layers even when extra options are supplied.
    pub fn load(&self, uri: impl IntoOperatorUri) -> Result<Operator> {
        let mut uri = uri.into_operator_uri()?;
        let mut context_options = BTreeMap::<_, HashMap<_, _>>::new();
        let mut layer_options = HashMap::<_, HashMap<_, _>>::new();
        let mut layer_names = Vec::new();

        for (key, value) in uri.extract_options(|key| {
            key == "layers" || key.starts_with("context.") || key.starts_with("layer.")
        }) {
            if key == "layers" {
                layer_names = parse_layer_names(&value)?;
            } else if let Some(key) = key.strip_prefix("context.") {
                let (name, field) = scoped_option(key, "context")?;
                context_options
                    .entry(name.to_owned())
                    .or_default()
                    .insert(field.to_owned(), value);
            } else if let Some(key) = key.strip_prefix("layer.") {
                let (name, field) = scoped_option(key, "layer")?;
                layer_options
                    .entry(name.to_owned())
                    .or_default()
                    .insert(field.to_owned(), value);
            }
        }

        // Each lookup clones its factory while locked. The guard is gone before
        // invoking the factory, which may register or load other components.
        let factory = lookup_factory(&self.lock_factories().services, "scheme", uri.scheme())?;
        let service = factory(&uri)?;

        let mut context = OperationContext::default();
        for (name, options) in context_options {
            let factory = lookup_factory(&self.lock_factories().contexts, "context", &name)?;
            context = factory(context, options)?;
        }

        let mut layers = Vec::with_capacity(layer_names.len());
        for name in layer_names {
            let factory = lookup_factory(&self.lock_factories().layers, "layer", &name)?;
            let options = layer_options.remove(&name).unwrap_or_default();
            layers.push(factory(options)?);
        }

        Ok(Operator::assemble(service, context, layers))
    }
}

fn lookup_factory<F: Clone>(
    factories: &HashMap<String, F>,
    kind: &'static str,
    name: &str,
) -> Result<F> {
    if let Some(factory) = factories.get(name) {
        return Ok(factory.clone());
    }
    let mut available: Vec<_> = factories.keys().map(String::as_str).collect();
    available.sort_unstable();
    Err(
        Error::new(ErrorKind::Unsupported, format!("{kind} is not registered"))
            .with_context(kind, name.to_owned())
            .with_context("available", available.join(", ")),
    )
}

fn service_factory<C: Configurator>(uri: &OperatorUri) -> Result<Servicer> {
    let service = C::from_uri(uri)?.into_builder().build()?;
    Ok(Arc::new(service))
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'_' | b'-')
        })
}

fn scoped_option<'a>(key: &'a str, kind: &str) -> Result<(&'a str, &'a str)> {
    if let Some((name, field)) = key.split_once('.')
        && valid_name(name)
        && !field.is_empty()
    {
        return Ok((name, field));
    }
    Err(Error::new(
        ErrorKind::ConfigInvalid,
        format!("invalid {kind} option key"),
    ))
}

fn parse_layer_names(value: &str) -> Result<Vec<String>> {
    if value.trim_ascii().is_empty() {
        return Ok(Vec::new());
    }

    let names: Vec<_> = value
        .split(',')
        .map(|name| name.trim_ascii().to_ascii_lowercase())
        .collect();
    let mut seen = HashSet::new();
    for name in &names {
        if !valid_name(name) {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "invalid name in layers list",
            ));
        }
        if !seen.insert(name) {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "duplicate name in layers list",
            ));
        }
    }
    Ok(names)
}

#[cfg(test)]
mod tests;
