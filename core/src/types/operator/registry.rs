use std::collections::HashMap;

use crate::services::Http;
use crate::*;

// In order to reduce boilerplate, we should return in this function a `Builder` instead of operator?.
pub type OperatorFactory = fn(&str, HashMap<String, String>) -> Result<Operator>;

// TODO: create an static registry? or a global() method of OperatorRegistry that lazily initializes the registry?
// Register only services in `Scheme::enabled()`

pub struct OperatorRegistry {
    // TODO: add Arc<Mutex<...>> to make it cheap to clone + thread safe? or is it not needed?
    registry: HashMap<String, OperatorFactory>,
}

impl OperatorRegistry {
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
        }
    }

    pub fn register(&mut self, scheme: &str, factory: OperatorFactory) {
        // TODO: should we receive a `&str` or a `String`? we are cloning it anyway
        self.registry.insert(scheme.to_string(), factory);
    }
    pub fn parse(
        &self,
        uri: &str,
        options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Operator> {
        let parsed_uri = http::Uri::try_from(uri).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "uri is invalid")
                .with_context("uri", uri)
                .set_source(err)
        })?;

        let scheme = parsed_uri.scheme().ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "uri is missing scheme").with_context("uri", uri)
        })?;

        let factory = self.registry.get(scheme.as_str()).ok_or_else(|| {
            Error::new(
                ErrorKind::ConfigInvalid,
                "could not find any operator factory for the given scheme",
            )
            .with_context("uri", uri)
            .with_context("scheme", scheme)
        })?;

        // TODO: `OperatorFactory` should receive `IntoIterator<Item = (String, String)>` instead of `HashMap<String, String>`?
        let options = options.into_iter().collect();

        // TODO: `OperatorFactory` should use `&str` instead of `String`? we are cloning it anyway
        factory(uri, options)
    }

    pub fn global() -> Self {
        let mut registry = Self::new();
        // TODO: have a `Builder::enabled()` method that returns the set of enabled services builders?
        // Similar to `Scheme::Enabled()`
        // or have an `Scheme::associated_builder` that given a scheme returns the associated builder?

        registry.register_builder::<Http>();
        registry
    }

    fn register_builder<B: Builder>(&mut self) {
        self.register(
            B::SCHEME.into_static(),
            operator_factory_from_configurator::<B::Config>(),
        );
    }
}

fn operator_factory_from_configurator<C: Configurator>() -> OperatorFactory {
    |uri, options| {
        let builder = C::from_uri(uri, options)?.into_builder();
        Operator::new(builder).map(OperatorBuilder::finish)
    }
}
