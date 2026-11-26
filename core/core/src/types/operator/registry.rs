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

/// Registry of service, context, and layer factories for string-based
/// operator configuration.
///
/// [`Operator::from_uri`] and [`Operator::via_iter`] use the global registry
/// returned by [`OperatorRegistry::get`]. Use [`OperatorRegistry::load`] to
/// construct a service with its configured base context and ordered layers.
/// Replacing a factory changes subsequent lookups. Operators already loaded
/// keep their service, base context, and layers.
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
    /// Get the process-wide registry used for string-based operator construction.
    ///
    /// The `memory` service is always registered. Other schemes, context
    /// providers, and layers must be registered before use. The `opendal`
    /// facade installs the factories it provides for enabled Cargo features
    /// when it initializes its defaults. With split crates, use the
    /// component's registration function or register a factory directly.
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

    /// Register a builder for the given service scheme.
    ///
    /// The scheme is matched case-insensitively in ASCII. Registering the same
    /// scheme replaces its builder for subsequent lookups.
    pub fn register<B: Builder>(&self, scheme: &str) {
        self.lock_factories()
            .services
            .insert(scheme.to_ascii_lowercase(), service_factory::<B::Config>);
    }

    /// Register a context factory for `context.<name>.*` options.
    ///
    /// Names are matched case-insensitively in ASCII and use only letters,
    /// digits, underscores, and hyphens. Registering the same name replaces its
    /// factory for subsequent lookups. The factory receives the current base
    /// context and the options with `context.<name>.` removed from their keys.
    /// Return the input context unchanged when no options are recognized.
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
    /// factory for subsequent lookups. The factory receives the options with
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
    /// This includes built-in and application-registered schemes. The set
    /// reflects the registrations at the time of the call.
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
    /// The registry partitions the normalized options, then builds the service
    /// selected by the URI scheme from the URI components and unscoped options.
    /// It starts with the default [`OperationContext`] and invokes context
    /// factories with `context.<name>.<field>` options in ascending order of
    /// their normalized names. Each factory receives the previous context and
    /// the normalized field keys without their scope prefix.
    ///
    /// The `layers` option lists layers from inner to outer, just like chained
    /// [`Operator::layer`] calls. For example, `layers=timeout,retry` places
    /// retry outside timeout and both outside the default layers that
    /// [`Operator::new`] applies. A missing, empty, or ASCII-whitespace-only
    /// list adds no configured layers. The registry trims ASCII whitespace
    /// around each comma-separated name and matches names case-insensitively in
    /// ASCII. It passes `layer.<name>.<field>` options to listed layers and
    /// ignores well-formed options for layers outside the list.
    ///
    /// The layers wrap the service and the configured base context. Use
    /// [`Operator::with_context`] to replace that base context and replay the
    /// same layers. Factories can access the registry while running. Each
    /// lookup uses the registration in effect at the time of that lookup.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorKind::ConfigInvalid`] for malformed scoped keys, even
    /// for inactive layers, and for invalid, empty, or duplicate names in a
    /// nonempty layer list. Scoped keys require a nonempty field and a name
    /// made of ASCII letters, digits, underscores, or hyphens. Returns
    /// [`ErrorKind::Unsupported`] for an unregistered scheme, configured context,
    /// or listed layer. URI conversion and factory errors propagate to the caller.
    ///
    /// Only pass URIs and options from trusted sources. URI query parameters can
    /// specify context and layers even when extra options are supplied. See the
    /// [operator configuration specification][crate::docs::specs::operator_configuration].
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
