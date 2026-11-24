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
use std::sync::Arc;

use foyer::HybridCache;
use log::debug;

use super::FOYER_SCHEME;
use super::FoyerKey;
use super::FoyerValue;
use super::config::FoyerConfig;
use super::core::FoyerCore;
use super::deleter::FoyerDeleter;
use super::reader::*;
use super::writer::FoyerWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// [foyer](https://github.com/foyer-rs/foyer) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct FoyerBuilder {
    pub(super) config: FoyerConfig,
    pub(super) cache: Option<Arc<HybridCache<FoyerKey, FoyerValue>>>,
}

impl Debug for FoyerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FoyerBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl FoyerBuilder {
    /// Create a new [`FoyerBuilder`] with default settings.
    ///
    /// The cache will be lazily initialized when first accessed if not provided via [`Self::cache`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use opendal_service_foyer::Foyer;
    ///
    /// let builder = Foyer::new()
    ///     .memory(64 * 1024 * 1024)
    ///     .root("/cache");
    /// ```
    pub fn new() -> Self {
        Self {
            ..Default::default()
        }
    }

    /// Set the name of this cache instance.
    pub fn name(mut self, name: &str) -> Self {
        if !name.is_empty() {
            self.config.name = Some(name.to_owned());
        }
        self
    }

    /// Set a pre-built [`foyer::HybridCache`] instance.
    ///
    /// If provided, this cache will be used directly. Otherwise, a cache will be
    /// lazily initialized using the configured memory size.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use opendal_service_foyer::Foyer;
    /// use foyer::HybridCacheBuilder;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let cache = HybridCacheBuilder::new()
    ///     .memory(64 * 1024 * 1024)
    ///     .storage()
    ///     .build()
    ///     .await?;
    ///
    /// let builder = Foyer::new().cache(cache);
    /// # Ok(())
    /// # }
    /// ```
    pub fn cache(mut self, cache: HybridCache<FoyerKey, FoyerValue>) -> Self {
        self.cache = Some(Arc::new(cache));
        self
    }

    /// Set the root path of this backend.
    ///
    /// All operations will be relative to this root path.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = if path.is_empty() {
            None
        } else {
            Some(path.to_string())
        };
        self
    }

    /// Set the memory capacity in bytes for the cache.
    ///
    /// This is used when the cache is lazily initialized (i.e., when no pre-built cache
    /// is provided via [`Self::cache`]).
    ///
    /// Default is 1 GiB (1024 * 1024 * 1024 bytes).
    pub fn memory(mut self, size: usize) -> Self {
        self.config.memory = Some(size);
        self
    }

    /// Set the disk cache directory path.
    ///
    /// Enables hybrid cache with disk storage. When memory cache is full, data will
    /// be persisted to this directory.
    pub fn disk_path(mut self, path: &str) -> Self {
        self.config.disk_path = Some(path.to_string());
        self
    }

    /// Set the disk cache total capacity in bytes.
    ///
    /// Only used when `disk_path` is set.
    pub fn disk_capacity(mut self, size: usize) -> Self {
        self.config.disk_capacity = Some(size);
        self
    }

    /// Set the individual cache file size in bytes.
    ///
    /// Default is 1 MiB (1024 * 1024 bytes).
    /// Only used when `disk_path` is set.
    pub fn disk_file_size(mut self, size: usize) -> Self {
        self.config.disk_file_size = Some(size);
        self
    }

    /// Set the recovery mode when starting the cache.
    ///
    /// Valid values: "none" (default), "quiet", "strict".
    /// - "none": Don't recover from disk
    /// - "quiet": Recover and skip errors
    /// - "strict": Recover and panic on errors
    pub fn recover_mode(mut self, mode: &str) -> Self {
        if !mode.is_empty() {
            self.config.recover_mode = Some(mode.to_string());
        }
        self
    }

    /// Set the number of shards for concurrent access.
    ///
    /// Default is 1. Higher values improve concurrency but increase overhead.
    pub fn shards(mut self, count: usize) -> Self {
        self.config.shards = Some(count);
        self
    }
}

impl Builder for FoyerBuilder {
    type Config = FoyerConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let root = normalize_root(
            self.config
                .root
                .clone()
                .unwrap_or_else(|| "/".to_string())
                .as_str(),
        );

        let mut core = FoyerCore::try_new(self.config.clone())?;
        if let Some(cache) = self.cache {
            core = core.with_cache(cache);
        }

        debug!("backend build finished: {:?}", self.config);

        Ok(FoyerBackend::new(core).with_normalized_root(root))
    }
}

#[derive(Debug, Clone)]
pub struct FoyerBackend {
    pub(crate) core: Arc<FoyerCore>,
    pub(crate) root: String,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl FoyerBackend {
    fn new(core: FoyerCore) -> Self {
        let info = ServiceInfo::new(FOYER_SCHEME, "/", core.name().unwrap_or("foyer"));
        let capability = Capability {
            read: true,
            write: true,
            write_can_empty: true,
            delete: true,
            stat: true,
            shared: true,
            ..Default::default()
        };

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info,
            capability,
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info = self.info.with_root(&root);
        self.root = root;
        self
    }
}

impl Service for FoyerBackend {
    type Reader = oio::StreamReader<FoyerReader>;
    type Writer = FoyerWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<FoyerDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        self.info.clone()
    }

    fn capability(&self) -> Capability {
        self.capability
    }

    async fn create_dir(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, _ctx: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_absolute_path(&self.root, path);

        if p == build_absolute_path(&self.root, "") {
            Ok(RpStat::new(MetadataBuilder::dir().build()))
        } else {
            match self.core.get(&p).await? {
                Some(bs) => {
                    let metadata = if p.ends_with('/') {
                        MetadataBuilder::dir()
                    } else {
                        MetadataBuilder::file(bs.len() as u64)
                    };
                    Ok(RpStat::new(metadata.build()))
                }
                None => Err(Error::new(ErrorKind::NotFound, "key not found in foyer")),
            }
        }
    }
    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<FoyerReader> = {
            Ok(oio::StreamReader::new(FoyerReader::new(
                self.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        let output: FoyerWriter = {
            let p = build_absolute_path(&self.root, path);
            Ok(FoyerWriter::new(self.core.clone(), p))
        }?;

        Ok(output)
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<FoyerDeleter> = {
            Ok(oio::OneShotDeleter::new(FoyerDeleter::new(
                self.core.clone(),
                self.root.clone(),
            )))
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, _path: &str, _args: OpList) -> Result<Self::Lister> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    fn copy(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpCopy,
    ) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _ctx: &OperationContext,
        _from: &str,
        _to: &str,
        _args: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(
        &self,
        _ctx: &OperationContext,
        _path: &str,
        _args: OpPresign,
    ) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

#[cfg(test)]
mod tests {
    use opendal_core::raw::oio::Write as _;

    use super::*;

    fn assert_config_invalid(res: Result<impl Service>, context: &str) {
        let err = match res {
            Ok(_) => panic!("expected ConfigInvalid for {context}, got Ok"),
            Err(err) => err,
        };
        assert_eq!(
            err.kind(),
            ErrorKind::ConfigInvalid,
            "unexpected error for {context}: {err:?}"
        );
    }

    #[test]
    fn test_build_rejects_invalid_config() {
        // shards = 0
        assert_config_invalid(FoyerBuilder::new().shards(0).build(), "builder shards(0)");
        assert_config_invalid(
            FoyerConfig::from_iter([("shards".to_string(), "0".to_string())])
                .unwrap()
                .into_builder()
                .build(),
            "from_iter shards=0",
        );
        assert_config_invalid(
            FoyerConfig::from_uri(
                &OperatorUri::new("foyer:///?shards=0", Vec::<(String, String)>::new()).unwrap(),
            )
            .unwrap()
            .into_builder()
            .build(),
            "uri shards=0",
        );

        // disk_file_size = 0, <= page/blob-index size, or page-alignment overflow
        assert_config_invalid(
            FoyerBuilder::new().disk_file_size(0).build(),
            "builder disk_file_size(0)",
        );
        assert_config_invalid(
            FoyerBuilder::new()
                .disk_path("/tmp/foyer-unused")
                .disk_file_size(0)
                .build(),
            "builder disk_path + disk_file_size(0)",
        );
        assert_config_invalid(
            FoyerConfig::from_uri(
                &OperatorUri::new("foyer:///?disk_file_size=0", Vec::<(String, String)>::new())
                    .unwrap(),
            )
            .unwrap()
            .into_builder()
            .build(),
            "uri disk_file_size=0",
        );
        assert_config_invalid(
            FoyerBuilder::new().disk_file_size(4096).build(),
            "builder disk_file_size(4096)",
        );
        assert_config_invalid(
            FoyerBuilder::new().disk_file_size(usize::MAX).build(),
            "builder disk_file_size(usize::MAX)",
        );

        // disk_capacity = 0 or smaller than aligned disk_file_size
        assert_config_invalid(
            FoyerBuilder::new().disk_capacity(0).build(),
            "builder disk_capacity(0)",
        );
        assert_config_invalid(
            FoyerBuilder::new().disk_capacity(4096).build(),
            "builder disk_capacity(4096) < default disk_file_size",
        );
        assert_config_invalid(
            FoyerBuilder::new()
                .disk_capacity(16 * 1024)
                .disk_file_size(32 * 1024)
                .build(),
            "builder disk_capacity < disk_file_size",
        );

        // disk_path = "" rejected via Builder and direct FoyerConfig (e.g., JSON/TOML);
        // from_iter / URI empty values deserialize to None via ConfigDeserializer.
        assert_config_invalid(
            FoyerBuilder::new().disk_path("").build(),
            "builder disk_path(\"\")",
        );
        assert_config_invalid(
            FoyerConfig {
                disk_path: Some(String::new()),
                ..Default::default()
            }
            .into_builder()
            .build(),
            "config disk_path=Some(\"\")",
        );
        assert_eq!(
            FoyerConfig::from_iter([("disk_path".to_string(), "".to_string())])
                .unwrap()
                .disk_path,
            None
        );
        assert_eq!(
            FoyerConfig::from_uri(
                &OperatorUri::new("foyer:///?disk_path=", Vec::<(String, String)>::new()).unwrap(),
            )
            .unwrap()
            .disk_path,
            None
        );

        // invalid recover_mode rejected eagerly in build()
        assert_config_invalid(
            FoyerBuilder::new().recover_mode("invalid").build(),
            "builder recover_mode(invalid)",
        );
        assert_config_invalid(
            FoyerConfig::from_uri(
                &OperatorUri::new(
                    "foyer:///?recover_mode=invalid",
                    Vec::<(String, String)>::new(),
                )
                .unwrap(),
            )
            .unwrap()
            .into_builder()
            .build(),
            "uri recover_mode=invalid",
        );

        // Valid minimal disk sizes and empty recover_mode succeed
        assert!(
            FoyerBuilder::new()
                .disk_capacity(8192)
                .disk_file_size(8192)
                .build()
                .is_ok()
        );
        assert!(
            FoyerConfig::from_iter([("recover_mode".to_string(), "".to_string())])
                .unwrap()
                .into_builder()
                .build()
                .is_ok()
        );
    }

    #[tokio::test]
    async fn test_dir_and_root_stat_and_writer_metadata() -> Result<()> {
        let ctx = OperationContext::default();
        for root in ["/", "/sub/"] {
            let builder = if root == "/" {
                FoyerBuilder::new()
            } else {
                FoyerBuilder::new().root(root)
            };
            let service = builder.build()?;

            // Root stat returns DIR
            assert_eq!(
                service
                    .stat(&ctx, "/", OpStat::default())
                    .await?
                    .into_metadata()
                    .mode(),
                EntryMode::DIR,
                "root {root}"
            );

            // Writing root marker returns DIR metadata
            let mut root_writer = service.write(&ctx, "/", OpWrite::default())?;
            let root_meta = root_writer.close().await?;
            assert_eq!(root_meta.mode(), EntryMode::DIR, "root {root}");
            assert_eq!(
                service
                    .stat(&ctx, "/", OpStat::default())
                    .await?
                    .into_metadata()
                    .mode(),
                EntryMode::DIR,
                "root {root}"
            );

            // Writing and statting a directory marker returns DIR metadata
            let mut dir_writer = service.write(&ctx, "a/", OpWrite::default())?;
            let dir_meta = dir_writer.close().await?;
            assert_eq!(dir_meta.mode(), EntryMode::DIR, "root {root}");
            assert_eq!(
                service
                    .stat(&ctx, "a/", OpStat::default())
                    .await?
                    .into_metadata()
                    .mode(),
                EntryMode::DIR,
                "root {root}"
            );

            // Missing directory marker returns NotFound
            let missing_err = service
                .stat(&ctx, "missing/", OpStat::default())
                .await
                .unwrap_err();
            assert_eq!(missing_err.kind(), ErrorKind::NotFound, "root {root}");

            // Writing and statting a regular file returns FILE metadata with length
            let mut file_writer = service.write(&ctx, "a/file.txt", OpWrite::default())?;
            file_writer.write(Buffer::from("hello")).await?;
            let file_meta = file_writer.close().await?;
            assert_eq!(file_meta.mode(), EntryMode::FILE, "root {root}");
            assert_eq!(file_meta.content_length(), 5, "root {root}");

            let stat_meta = service
                .stat(&ctx, "a/file.txt", OpStat::default())
                .await?
                .into_metadata();
            assert_eq!(stat_meta.mode(), EntryMode::FILE, "root {root}");
            assert_eq!(stat_meta.content_length(), 5, "root {root}");

            // End-to-end check through Operator
            let op_builder = if root == "/" {
                FoyerBuilder::new()
            } else {
                FoyerBuilder::new().root(root)
            };
            let op = Operator::new(op_builder)?;
            assert_eq!(op.stat("/").await?.mode(), EntryMode::DIR, "root {root}");
            op.write("sim/file", "data").await?;
            let op_file_meta = op.stat("sim/file").await?;
            assert_eq!(op_file_meta.mode(), EntryMode::FILE, "root {root}");
            assert_eq!(op_file_meta.content_length(), 4, "root {root}");
        }
        Ok(())
    }
}
