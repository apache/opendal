use std::collections::HashMap;

use crate::*;

pub type OperatorFactory = fn(String, HashMap<String, String>) -> Result<Operator>;

// TODO: create an static registry? or a global() method of OperatorRegistry that lazily initializes the registry?
// Register only services in `Scheme::enabled()`

pub struct OperatorRegistry {
    // TODO: add Arc<Mutex<...>> to make it cheap to clone + thread safe? or is it not needed?
    registry: HashMap<String, OperatorFactory>,
}

impl OperatorRegistry {
    pub fn register(&mut self, scheme: &str, factory: OperatorFactory) {
        // TODO: should we receive a `&str` or a `String`? we are cloning it anyway
        self.registry.insert(scheme.to_string(), factory);
    }
    pub fn parse(
        &self,
        uri: &str,
        options: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Operator> {
        let uri = http::Uri::try_from(uri).map_err(|err| {
            Error::new(ErrorKind::ConfigInvalid, "uri is invalid")
                .with_context("uri", uri)
                .set_source(err)
        })?;

        let scheme = uri.scheme().ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "uri is missing scheme").with_context("uri", uri)
        })?;

        let factory = self.registry.get(scheme).ok_or_else(|| {
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
        factory(uri.path().to_string(), options)
    }
}
