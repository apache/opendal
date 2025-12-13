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

use std::sync::Arc;

use crate::layers::*;
use crate::raw::*;
use crate::types::IntoOperatorUri;
use crate::*;

/// # Operator build API
///
/// Operator should be built via [`OperatorBuilder`]. We recommend to use [`Operator::new`] to get started:
///
/// ```
/// # use anyhow::Result;
/// use opendal_core::services::Memory;
/// use opendal_core::Operator;
/// async fn test() -> Result<()> {
///     // Create memory backend builder.
///     let builder = Memory::default();
///
///     // Build an `Operator` to start operating the storage.
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
impl Operator {
    /// Create a new operator with input builder.
    ///
    /// OpenDAL will call `builder.build()` internally, so we don't need
    /// to import `opendal::Builder` trait.
    ///
    /// # Examples
    ///
    /// Read more backend init examples in [examples](https://github.com/apache/opendal/tree/main/examples).
    ///
    /// ```
    /// # use anyhow::Result;
    /// use opendal_core::services::Memory;
    /// use opendal_core::Operator;
    /// async fn test() -> Result<()> {
    ///     // Create memory backend builder.
    ///     let builder = Memory::default();
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::new(builder)?.finish();
    ///
    ///     Ok(())
    /// }
    /// ```
    #[allow(clippy::new_ret_no_self)]
    pub fn new<B: Builder>(ab: B) -> Result<OperatorBuilder<impl Access>> {
        let acc = ab.build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Create a new operator from given config.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// use std::collections::HashMap;
    ///
    /// use opendal_core::services::MemoryConfig;
    /// use opendal_core::Operator;
    /// async fn test() -> Result<()> {
    ///     let cfg = MemoryConfig::default();
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::from_config(cfg)?.finish();
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn from_config<C: Configurator>(cfg: C) -> Result<OperatorBuilder<impl Access>> {
        let builder = cfg.into_builder();
        let acc = builder.build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Create a new operator from given iterator in static dispatch.
    ///
    /// # Notes
    ///
    /// `from_iter` generates a `OperatorBuilder` which allows adding layer in zero-cost way.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// use std::collections::HashMap;
    ///
    /// use opendal_core::services::Memory;
    /// use opendal_core::Operator;
    /// async fn test() -> Result<()> {
    ///     let map = HashMap::new();
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::from_iter::<Memory>(map)?.finish();
    ///
    ///     Ok(())
    /// }
    /// ```
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<B: Builder>(
        iter: impl IntoIterator<Item = (String, String)>,
    ) -> Result<OperatorBuilder<impl Access>> {
        let builder = B::Config::from_iter(iter)?.into_builder();
        let acc = builder.build()?;
        Ok(OperatorBuilder::new(acc))
    }

    /// Create a new operator by parsing configuration from a URI.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// use opendal_core::Operator;
    ///
    /// # fn example() -> Result<()> {
    /// let op = Operator::from_uri("memory://localhost/")?;
    /// # let _ = op;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_uri(uri: impl IntoOperatorUri) -> Result<Operator> {
        crate::DEFAULT_OPERATOR_REGISTRY.load(uri)
    }

    /// Create a new operator via given scheme and iterator of config value in dynamic dispatch.
    ///
    /// # Notes
    ///
    /// `via_iter` generates a `Operator` which allows building operator without generic type.
    ///
    /// # Examples
    ///
    /// ```
    /// # use anyhow::Result;
    /// use std::collections::HashMap;
    ///
    /// use opendal_core::Operator;
    /// use opendal_core::services;
    ///
    /// async fn test() -> Result<()> {
    ///     let map: Vec<(String, String)> = vec![];
    ///
    ///     // Build an `Operator` to start operating the storage.
    ///     let op: Operator = Operator::via_iter(services::MEMORY_SCHEME, map)?;
    ///
    ///     Ok(())
    /// }
    /// ```
    pub fn via_iter(
        scheme: impl AsRef<str>,
        iter: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Operator> {
        Operator::from_uri((scheme.as_ref(), iter))
    }

    /// Create a new layer with dynamic dispatch.
    ///
    /// Please note that `Layer` can modify internal contexts such as `HttpClient`
    /// and `Runtime` for the operator. Therefore, it is recommended to add layers
    /// before interacting with the storage. Adding or duplicating layers after
    /// accessing the storage may result in unexpected behavior.
    ///
    /// # Notes
    ///
    /// `OperatorBuilder::layer()` is using static dispatch which is zero
    /// cost. `Operator::layer()` is using dynamic dispatch which has a
    /// bit runtime overhead with an extra vtable lookup and unable to
    /// inline.
    ///
    /// It's always recommended to use `OperatorBuilder::layer()` instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// use opendal_core::layers::LoggingLayer;
    /// use opendal_core::services::Memory;
    /// use opendal_core::Operator;
    ///
    /// # async fn test() -> Result<()> {
    /// let op = Operator::new(Memory::default())?.finish();
    /// let op = op.layer(LoggingLayer::default());
    /// // All operations will go through the new_layer
    /// let _ = op.read("test_file").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn layer<L: Layer<Accessor>>(self, layer: L) -> Self {
        Self::from_inner(Arc::new(
            TypeEraseLayer.layer(layer.layer(self.into_inner())),
        ))
    }
}

/// OperatorBuilder is a typed builder to build an Operator.
///
/// # Notes
///
/// OpenDAL uses static dispatch internally and only performs dynamic
/// dispatch at the outmost type erase layer. OperatorBuilder is the only
/// public API provided by OpenDAL come with generic parameters.
///
/// It's required to call `finish` after the operator built.
///
/// # Examples
///
/// For users who want to support many services, we can build a helper function like the following:
///
/// ```
/// use std::collections::HashMap;
///
/// use opendal_core::layers::LoggingLayer;
/// use opendal_core::layers::RetryLayer;
/// use opendal_core::services;
/// use opendal_core::Builder;
/// use opendal_core::Operator;
/// use opendal_core::Result;
///
/// fn init_service<B: Builder>(cfg: HashMap<String, String>) -> Result<Operator> {
///     let op = Operator::from_iter::<B>(cfg)?
///         .layer(LoggingLayer::default())
///         .layer(RetryLayer::new())
///         .finish();
///
///     Ok(op)
/// }
///
/// async fn init(scheme: &str, cfg: HashMap<String, String>) -> Result<()> {
///     let _ = match scheme {
///         services::MEMORY_SCHEME => init_service::<services::Memory>(cfg)?,
///         _ => todo!(),
///     };
///
///     Ok(())
/// }
/// ```
pub struct OperatorBuilder<A: Access> {
    accessor: A,
}

impl<A: Access> OperatorBuilder<A> {
    /// Create a new operator builder.
    #[allow(clippy::new_ret_no_self)]
    pub fn new(accessor: A) -> OperatorBuilder<impl Access> {
        // Make sure error context layer has been attached.
        OperatorBuilder { accessor }
            .layer(ErrorContextLayer)
            .layer(SimulateLayer::default())
            .layer(CompleteLayer)
            .layer(CorrectnessCheckLayer)
    }

    /// Create a new layer with static dispatch.
    ///
    /// # Notes
    ///
    /// `OperatorBuilder::layer()` is using static dispatch which is zero
    /// cost. `Operator::layer()` is using dynamic dispatch which has a
    /// bit runtime overhead with an extra vtable lookup and unable to
    /// inline.
    ///
    /// It's always recommended to use `OperatorBuilder::layer()` instead.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use anyhow::Result;
    /// use opendal_core::layers::LoggingLayer;
    /// use opendal_core::services::Memory;
    /// use opendal_core::Operator;
    ///
    /// # async fn test() -> Result<()> {
    /// let op = Operator::new(Memory::default())?
    ///     .layer(LoggingLayer::default())
    ///     .finish();
    /// // All operations will go through the new_layer
    /// let _ = op.read("test_file").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn layer<L: Layer<A>>(self, layer: L) -> OperatorBuilder<L::LayeredAccess> {
        OperatorBuilder {
            accessor: layer.layer(self.accessor),
        }
    }

    /// Finish the building to construct an Operator.
    pub fn finish(self) -> Operator {
        let ob = self.layer(TypeEraseLayer);
        Operator::from_inner(Arc::new(ob.accessor) as Accessor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services;

    #[test]
    fn via_iter_delegates_to_registry() {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])
            .expect("memory scheme should be registered");
        let _ = op;
    }
}
