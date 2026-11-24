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

use log::debug;
use opendal_core::raw::*;
use opendal_core::*;

use super::MINI_MOKA_SCHEME;
use super::config::MiniMokaConfig;
use super::core::*;
use super::deleter::MiniMokaDeleter;
use super::lister::MiniMokaLister;
use super::reader::*;
use super::writer::MiniMokaWriter;

/// Maximum TTL/TTI accepted by `mini_moka::sync::CacheBuilder::build` (1000 years).
const MAX_CACHE_DURATION: Duration = Duration::from_secs(1_000 * 365 * 24 * 3600);

fn entry_weight(key_len: usize, content_len: usize) -> u32 {
    u32::try_from(key_len.saturating_add(content_len)).unwrap_or(u32::MAX)
}

/// [mini-moka](https://github.com/moka-rs/mini-moka) backend support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct MiniMokaBuilder {
    pub(super) config: MiniMokaConfig,
    pub(super) time_to_live: Option<Duration>,
    pub(super) time_to_idle: Option<Duration>,
}

impl MiniMokaBuilder {
    /// Create a [`MiniMokaBuilder`] with default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the max capacity of the cache.
    ///
    /// Refer to [`mini-moka::sync::CacheBuilder::max_capacity`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.max_capacity)
    pub fn max_capacity(mut self, v: u64) -> Self {
        if v != 0 {
            self.config.max_capacity = Some(v);
        }
        self
    }

    /// Sets the time to live of the cache.
    ///
    /// Refer to [`mini-moka::sync::CacheBuilder::time_to_live`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_live)
    pub fn time_to_live(mut self, v: Duration) -> Self {
        if !v.is_zero() {
            self.time_to_live = Some(v);
        }
        self
    }

    /// Sets the time to idle of the cache.
    ///
    /// Refer to [`mini-moka::sync::CacheBuilder::time_to_idle`](https://docs.rs/mini-moka/latest/mini_moka/sync/struct.CacheBuilder.html#method.time_to_idle)
    pub fn time_to_idle(mut self, v: Duration) -> Self {
        if !v.is_zero() {
            self.time_to_idle = Some(v);
        }
        self
    }

    /// Set root path of this backend
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = if path.is_empty() {
            None
        } else {
            Some(path.to_string())
        };

        self
    }
}

impl Builder for MiniMokaBuilder {
    type Config = MiniMokaConfig;

    fn build(self) -> Result<impl Service> {
        debug!("backend build started: {:?}", self);

        let mut builder: mini_moka::sync::CacheBuilder<String, MiniMokaValue, _> =
            mini_moka::sync::Cache::builder();

        // Use entries' bytes as capacity weigher.
        builder = builder.weigher(|k, v| entry_weight(k.len(), v.content.len()));

        if let Some(v) = self.config.max_capacity.filter(|&v| v != 0) {
            builder = builder.max_capacity(v);
        }
        let time_to_live = match self.time_to_live {
            Some(value) => Some(value),
            None => self
                .config
                .time_to_live
                .map(signed_duration_to_duration)
                .transpose()?,
        }
        .filter(|duration| !duration.is_zero());
        if let Some(duration) = time_to_live {
            if duration > MAX_CACHE_DURATION {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "time_to_live must not exceed 1000 years",
                )
                .with_context("service", MINI_MOKA_SCHEME));
            }
            builder = builder.time_to_live(duration);
        }
        let time_to_idle = match self.time_to_idle {
            Some(value) => Some(value),
            None => self
                .config
                .time_to_idle
                .map(signed_duration_to_duration)
                .transpose()?,
        }
        .filter(|duration| !duration.is_zero());
        if let Some(duration) = time_to_idle {
            if duration > MAX_CACHE_DURATION {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "time_to_idle must not exceed 1000 years",
                )
                .with_context("service", MINI_MOKA_SCHEME));
            }
            builder = builder.time_to_idle(duration);
        }

        let cache = builder.build();

        let root = normalize_root(self.config.root.as_deref().unwrap_or("/"));

        let core = Arc::new(MiniMokaCore { cache });

        debug!("backend build finished: {root}");
        Ok(MiniMokaBackend::new(core, root))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::raw::oio::{List as _, Write as _};

    #[test]
    fn builder_preserves_subsecond_durations() {
        let time_to_live = Duration::new(1, 500_000_001);
        let time_to_idle = Duration::from_millis(750);
        let builder = MiniMokaBuilder::default()
            .time_to_live(time_to_live)
            .time_to_idle(time_to_idle);

        assert_eq!(builder.time_to_live, Some(time_to_live));
        assert_eq!(builder.time_to_idle, Some(time_to_idle));
    }

    /// Write a file through the raw service so tests exercise `Service::list`
    /// directly instead of `Operator` (which composes `SimulateLayer` under
    /// `--all-features` and would mask prefix filtering regressions).
    async fn write_file<S>(service: &S, ctx: &OperationContext, path: &str) -> Result<()>
    where
        S: Service,
        S::Writer: oio::Write,
    {
        let mut writer = service.write(ctx, path, OpWrite::default())?;
        writer.write(Buffer::from("x")).await?;
        writer.close().await?;
        Ok(())
    }

    async fn list_paths<S>(
        service: &S,
        ctx: &OperationContext,
        path: &str,
        recursive: bool,
    ) -> Result<Vec<String>>
    where
        S: Service,
        S::Lister: oio::List,
    {
        let args: OpList = options::ListOptions {
            recursive,
            ..Default::default()
        }
        .into();
        let mut lister = service.list(ctx, path, args)?;

        let mut paths = Vec::new();
        while let Some(entry) = lister.next().await? {
            paths.push(entry.path().to_string());
        }
        paths.sort_unstable();
        Ok(paths)
    }

    #[tokio::test]
    async fn recursive_list_only_returns_entries_under_prefix() -> Result<()> {
        let service = MiniMokaBuilder::default().build()?;
        let ctx = OperationContext::default();
        for path in ["a/1", "a/nested/2", "ab/1", "b/1"] {
            write_file(&service, &ctx, path).await?;
        }

        assert_eq!(
            list_paths(&service, &ctx, "a/", true).await?,
            ["a/1", "a/nested/2"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn recursive_list_only_returns_entries_under_prefix_with_root() -> Result<()> {
        let service = MiniMokaBuilder::default().root("/sub").build()?;
        let ctx = OperationContext::default();
        for path in ["a/1", "a/nested/2", "ab/1", "b/1"] {
            write_file(&service, &ctx, path).await?;
        }

        assert_eq!(
            list_paths(&service, &ctx, "a/", true).await?,
            ["a/1", "a/nested/2"]
        );
        assert_eq!(
            list_paths(&service, &ctx, "/", true).await?,
            ["a/1", "a/nested/2", "ab/1", "b/1"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn non_recursive_list_and_root_list_still_work() -> Result<()> {
        let core = MiniMokaCore {
            cache: mini_moka::sync::Cache::builder().build(),
        };
        let service = MiniMokaBackend::new(Arc::new(core), "/".to_string());
        let ctx = OperationContext::default();
        for path in ["a/1", "a/nested/2", "ab/1", "b/1"] {
            write_file(&service, &ctx, path).await?;
        }
        // A stored root marker key "" must be reported as the "/" entry.
        service.core.cache.insert(
            String::new(),
            MiniMokaValue {
                metadata: MetadataBuilder::dir().build(),
                content: Buffer::new(),
            },
        );

        assert_eq!(
            list_paths(&service, &ctx, "a/", false).await?,
            ["a/1", "a/nested/"]
        );
        assert_eq!(
            list_paths(&service, &ctx, "/", false).await?,
            ["/", "a/", "ab/", "b/"]
        );
        assert_eq!(
            list_paths(&service, &ctx, "/", true).await?,
            ["/", "a/1", "a/nested/2", "ab/1", "b/1"]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_create_dir_root_and_list() -> Result<()> {
        for root in ["/", "/sub/"] {
            let builder = if root == "/" {
                MiniMokaBuilder::default()
            } else {
                MiniMokaBuilder::default().root(root)
            };
            let op = Operator::new(builder)?.layer(layers::SimulateLayer::default());

            op.create_dir("/").await?;

            for entries in [
                op.list("/").await?,
                op.list_with("/").recursive(true).await?,
            ] {
                let actual: Vec<_> = entries
                    .iter()
                    .map(|e| (e.path(), e.metadata().mode()))
                    .collect();
                assert_eq!(actual, vec![("/", EntryMode::DIR)], "root {root}");
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_root_stat_and_writer_metadata() -> Result<()> {
        let ctx = OperationContext::default();
        for root in ["/", "/sub/"] {
            let builder = if root == "/" {
                MiniMokaBuilder::default()
            } else {
                MiniMokaBuilder::default().root(root)
            };
            let service = builder.build()?;

            assert_eq!(
                service
                    .stat(&ctx, "/", OpStat::default())
                    .await?
                    .into_metadata()
                    .mode(),
                EntryMode::DIR,
                "root {root}"
            );

            let mut writer = service.write(&ctx, "/", OpWrite::default())?;
            let meta = writer.close().await?;
            assert_eq!(meta.mode(), EntryMode::DIR, "root {root}");

            assert_eq!(
                service
                    .stat(&ctx, "/", OpStat::default())
                    .await?
                    .into_metadata()
                    .mode(),
                EntryMode::DIR,
                "root {root}"
            );
        }
        Ok(())
    }

    #[test]
    fn test_expiration_duration_upper_bound() -> Result<()> {
        // Exact 1000-year boundary succeeds without panicking.
        let _ = MiniMokaBuilder::default()
            .time_to_live(MAX_CACHE_DURATION)
            .time_to_idle(MAX_CACHE_DURATION)
            .build()?;

        let over_limit = MAX_CACHE_DURATION + Duration::from_nanos(1);
        for duration in [over_limit, Duration::MAX] {
            let err = MiniMokaBuilder::default()
                .time_to_live(duration)
                .build()
                .expect_err("time_to_live > 1000 years must fail");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

            let err = MiniMokaBuilder::default()
                .time_to_idle(duration)
                .build()
                .expect_err("time_to_idle > 1000 years must fail");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        let over_secs = format!("{}s", MAX_CACHE_DURATION.as_secs() + 1);
        for key in ["time_to_live", "time_to_idle"] {
            let err = MiniMokaConfig::from_iter([(key.to_string(), over_secs.clone())])?
                .into_builder()
                .build()
                .expect_err("config duration > 1000 years must fail");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        Ok(())
    }

    #[test]
    fn test_entry_weight_saturates_to_u32_max() {
        assert_eq!(entry_weight(3, 5), 8);
        assert_eq!(entry_weight(u32::MAX as usize, 0), u32::MAX);
        assert_eq!(entry_weight(u32::MAX as usize, 1), u32::MAX);
        assert_eq!(entry_weight(usize::MAX, usize::MAX), u32::MAX);
    }

    #[tokio::test]
    async fn test_config_zero_values_match_builder() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let service = MiniMokaConfig::from_iter([
                ("max_capacity".to_string(), "0".to_string()),
                ("time_to_live".to_string(), "0s".to_string()),
                ("time_to_idle".to_string(), "0s".to_string()),
            ])?
            .into_builder()
            .build()?;

            let ctx = OperationContext::default();
            write_file(&service, &ctx, "a/file.txt").await?;
            let meta = service
                .stat(&ctx, "a/file.txt", OpStat::default())
                .await?
                .into_metadata();
            assert_eq!(meta.mode(), EntryMode::FILE);
            assert_eq!(meta.content_length(), 1);
            Ok::<_, Error>(())
        })
        .await
        .expect("test timed out")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MiniMokaBackend {
    pub(crate) core: Arc<MiniMokaCore>,
    pub(crate) root: String,
    capability: Capability,
}

impl MiniMokaBackend {
    fn new(core: Arc<MiniMokaCore>, root: String) -> Self {
        let capability = Capability {
            stat: true,
            read: true,
            write: true,
            write_can_empty: true,
            delete: true,
            list: true,

            ..Default::default()
        };

        Self {
            core,
            root,
            capability,
        }
    }
}

impl Service for MiniMokaBackend {
    type Reader = oio::StreamReader<MiniMokaReader>;
    type Writer = MiniMokaWriter;
    type Lister = oio::HierarchyLister<MiniMokaLister>;
    type Deleter = oio::OneShotDeleter<MiniMokaDeleter>;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new(MINI_MOKA_SCHEME, &self.root, "")
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
            return Ok(RpStat::new(MetadataBuilder::dir().build()));
        }

        // Check if path exists directly in cache
        match self.core.get(&p) {
            Some(value) => {
                let mut metadata = value.metadata.clone().into_builder();
                if p.ends_with('/') {
                    metadata.set_dir();
                } else {
                    metadata.set_file(value.metadata.content_length());
                }
                Ok(RpStat::new(metadata.build()))
            }
            None => {
                if p.ends_with('/') {
                    let is_prefix = self
                        .core
                        .cache
                        .iter()
                        .any(|entry| entry.key().starts_with(&p) && entry.key() != &p);

                    if is_prefix {
                        let mut metadata = MetadataBuilder::unknown();
                        metadata.set_dir();
                        return Ok(RpStat::new(metadata.build()));
                    }
                }

                Err(Error::new(ErrorKind::NotFound, "path not found"))
            }
        }
    }
    fn read(&self, _ctx: &OperationContext, path: &str, op: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<MiniMokaReader> = {
            Ok(oio::StreamReader::new(MiniMokaReader::new(
                self.clone(),
                path,
                op,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, op: OpWrite) -> Result<Self::Writer> {
        let output: MiniMokaWriter = {
            let p = build_absolute_path(&self.root, path);
            let writer = MiniMokaWriter::new(self.core.clone(), p, op);
            Ok(writer)
        }?;

        Ok(output)
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<MiniMokaDeleter> = {
            let deleter = oio::OneShotDeleter::new(MiniMokaDeleter::new(
                self.core.clone(),
                self.root.clone(),
            ));
            Ok(deleter)
        }?;

        Ok(output)
    }

    fn list(&self, _ctx: &OperationContext, path: &str, op: OpList) -> Result<Self::Lister> {
        let output: oio::HierarchyLister<MiniMokaLister> = {
            let p = build_absolute_path(&self.root, path);

            let mini_moka_lister = MiniMokaLister::new(self.core.clone(), self.root.clone(), p);
            let lister = oio::HierarchyLister::new(mini_moka_lister, path, op.recursive());

            Ok(lister)
        }?;

        Ok(output)
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
