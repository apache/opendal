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

use crate::raw::*;
use crate::types::IntoOperatorUri;
use crate::*;

impl Operator {
    /// Create a new operator with input builder.
    ///
    /// OpenDAL calls [`Builder::build`] internally and returns a ready-to-use
    /// [`Operator`]. The default `auto-simulate` Cargo feature installs
    /// [`SimulateLayer`](crate::layers::SimulateLayer) automatically. With that
    /// feature disabled, use [`Operator::layer`] to apply it explicitly.
    ///
    /// # Examples
    ///
    /// ```
    /// use opendal_core::services::Memory;
    /// use opendal_core::{Operator, Result};
    ///
    /// # fn example() -> Result<()> {
    /// let op = Operator::new(Memory::default())?;
    /// # let _ = op;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new<B: Builder>(ab: B) -> Result<Operator> {
        let service = Arc::new(ab.build()?) as Servicer;
        Ok(Operator::assemble(
            service,
            OperationContext::default(),
            Vec::new(),
        ))
    }

    /// Create a new operator from given config.
    ///
    /// The config is converted into its corresponding builder and then follows
    /// the same construction path as [`Operator::new`].
    ///
    /// # Examples
    ///
    /// ```
    /// use opendal_core::services::MemoryConfig;
    /// use opendal_core::{Operator, Result};
    ///
    /// # fn example() -> Result<()> {
    /// let op = Operator::from_config(MemoryConfig::default())?;
    /// # let _ = op;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_config<C: Configurator>(cfg: C) -> Result<Operator> {
        let builder = cfg.into_builder();
        Operator::new(builder)
    }

    /// Create a new operator from given iterator.
    ///
    /// The service builder type is selected statically by `B`, while config
    /// values are parsed from the provided key-value iterator. To configure
    /// context and layers alongside service options, use [`Operator::via_iter`].
    ///
    /// # Examples
    ///
    /// ```
    /// use std::collections::HashMap;
    ///
    /// use opendal_core::services::Memory;
    /// use opendal_core::{Operator, Result};
    ///
    /// # fn example() -> Result<()> {
    /// let cfg: HashMap<String, String> = HashMap::new();
    /// let op = Operator::from_iter::<Memory>(cfg)?;
    /// # let _ = op;
    /// # Ok(())
    /// # }
    /// ```
    #[allow(clippy::should_implement_trait)]
    pub fn from_iter<B: Builder>(
        iter: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Operator> {
        let builder = B::Config::from_iter(iter)?.into_builder();
        Operator::new(builder)
    }

    /// Create a new operator by parsing configuration from a URI.
    ///
    /// The URI scheme is resolved through [`OperatorRegistry`], so this only
    /// works for services registered in the current build.
    ///
    /// # Examples
    ///
    /// ```
    /// use opendal_core::{Operator, Result};
    ///
    /// # fn example() -> Result<()> {
    /// let op = Operator::from_uri("memory://localhost/")?;
    /// # let _ = op;
    /// # Ok(())
    /// # }
    /// ```
    pub fn from_uri(uri: impl IntoOperatorUri) -> Result<Operator> {
        OperatorRegistry::get().load(uri)
    }

    /// Create a new operator via given scheme and iterator of config value.
    ///
    /// This is the scheme-driven variant of [`Operator::from_iter`]. It
    /// delegates to the registry, allowing callers to construct an operator
    /// without naming the builder type at compile time.
    ///
    /// # Examples
    ///
    /// ```
    /// use opendal_core::services;
    /// use opendal_core::{Operator, Result};
    ///
    /// # fn example() -> Result<()> {
    /// let cfg = Vec::<(String, String)>::new();
    /// let op = Operator::via_iter(services::MEMORY_SCHEME, cfg)?;
    /// # let _ = op;
    /// # Ok(())
    /// # }
    /// ```
    pub fn via_iter(
        scheme: impl AsRef<str>,
        iter: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Operator> {
        Operator::from_uri((scheme.as_ref(), iter))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(feature = "auto-simulate"))]
    use crate::layers::SimulateLayer;
    use crate::services;

    #[test]
    fn via_iter_delegates_to_registry() {
        let op = Operator::via_iter(services::MEMORY_SCHEME, [])
            .expect("memory scheme should be registered");
        let _ = op;
    }

    #[test]
    fn new_returns_finished_operator() {
        let op = Operator::new(services::Memory::default()).expect("memory builder should build");
        assert_eq!(op.info().scheme(), services::MEMORY_SCHEME);
    }

    #[tokio::test]
    async fn create_dir_follows_auto_simulate_feature() -> Result<()> {
        let op = Operator::new(DirectoryMarkerService)?;

        assert_eq!(
            op.info().capability().create_dir,
            cfg!(feature = "auto-simulate")
        );
        assert_eq!(
            op.info().capability().list_with_glob,
            cfg!(feature = "auto-simulate")
        );
        let result = op.create_dir("dir/").await;
        if cfg!(feature = "auto-simulate") {
            result?;
        } else {
            assert_eq!(result.unwrap_err().kind(), ErrorKind::Unsupported);
        }
        Ok(())
    }

    #[cfg(not(feature = "auto-simulate"))]
    #[tokio::test]
    async fn explicit_simulate_layer_enables_create_dir() -> Result<()> {
        let op = Operator::new(DirectoryMarkerService)?.layer(SimulateLayer::default());

        assert!(op.info().capability().create_dir);
        op.create_dir("dir/").await
    }

    #[derive(Debug, Default)]
    struct DirectoryMarkerService;

    impl Builder for DirectoryMarkerService {
        type Config = ();

        fn build(self) -> Result<impl Service> {
            Ok(self)
        }
    }

    impl Service for DirectoryMarkerService {
        type Reader = ();
        type Writer = oio::OneShotWriter<DirectoryMarkerWriter>;
        type Lister = ();
        type Deleter = ();
        type Copier = ();
        type Composer = ();

        fn info(&self) -> ServiceInfo {
            ServiceInfo::with_scheme("directory-marker")
        }

        fn capability(&self) -> Capability {
            Capability {
                list: true,
                write: true,
                write_can_empty: true,
                ..Capability::default()
            }
        }

        async fn create_dir(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpCreateDir,
        ) -> Result<RpCreateDir> {
            Service::create_dir(&(), ctx, path, args).await
        }

        async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
            Service::stat(&(), ctx, path, args).await
        }

        fn read(&self, ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
            Service::read(&(), ctx, path, args)
        }

        fn write(&self, _: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
            assert_eq!(path, "dir/");
            Ok(oio::OneShotWriter::new(DirectoryMarkerWriter))
        }

        fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
            Service::delete(&(), ctx)
        }

        fn list(&self, _: &OperationContext, _: &str, _: OpList) -> Result<Self::Lister> {
            Ok(())
        }

        fn copy(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpCopy,
        ) -> Result<Self::Copier> {
            Service::copy(&(), ctx, from, to, args)
        }

        async fn rename(
            &self,
            ctx: &OperationContext,
            from: &str,
            to: &str,
            args: OpRename,
        ) -> Result<RpRename> {
            Service::rename(&(), ctx, from, to, args).await
        }

        async fn presign(
            &self,
            ctx: &OperationContext,
            path: &str,
            args: OpPresign,
        ) -> Result<RpPresign> {
            Service::presign(&(), ctx, path, args).await
        }
    }

    struct DirectoryMarkerWriter;

    impl oio::OneShotWrite for DirectoryMarkerWriter {
        async fn write_once(&self, buffer: Buffer) -> Result<Metadata> {
            assert!(buffer.is_empty());
            Ok(MetadataBuilder::file(0).build())
        }
    }
}
