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

use super::CACACHE_SCHEME;
use super::config::CacacheConfig;
use super::core::CacacheCore;
use super::deleter::CacacheDeleter;
use super::reader::*;
use super::writer::CacacheWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// cacache service support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct CacacheBuilder {
    pub(super) config: CacacheConfig,
}

impl CacacheBuilder {
    /// Set the path to the cacache data directory. Will create if not exists.
    pub fn datadir(mut self, path: &str) -> Self {
        self.config.datadir = Some(path.into());
        self
    }
}

impl Builder for CacacheBuilder {
    type Config = CacacheConfig;

    fn build(self) -> Result<impl Service> {
        let datadir_path = self.config.datadir.ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "datadir is required but not set")
                .with_context("service", CACACHE_SCHEME)
        })?;

        let core = CacacheCore {
            path: datadir_path.clone(),
        };

        let info = ServiceInfo::new(CACACHE_SCHEME, "/", &datadir_path);
        let capability = Capability {
            read: true,
            write: true,
            delete: true,
            stat: true,
            rename: false,
            list: false,
            ..Default::default()
        };

        Ok(CacacheBackend {
            core: Arc::new(core),
            info,
            capability,
        })
    }
}

/// Backend for cacache services.
#[derive(Debug, Clone)]
pub struct CacacheBackend {
    pub(crate) core: Arc<CacacheCore>,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl Service for CacacheBackend {
    type Reader = oio::StreamReader<CacacheReader>;
    type Writer = CacacheWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<CacacheDeleter>;
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
        if path.is_empty() || path == "/" {
            return Ok(RpStat::new(MetadataBuilder::dir().build()));
        }

        let metadata = self.core.metadata(path).await?;

        match metadata {
            Some(meta) => {
                let mut md = if path.ends_with('/') {
                    MetadataBuilder::dir()
                } else {
                    MetadataBuilder::file(meta.size as u64)
                };
                // Convert u128 milliseconds to Timestamp
                if let Ok(dt) = Timestamp::from_millisecond(meta.time as i64) {
                    md.last_modified(dt);
                }
                Ok(RpStat::new(md.build()))
            }
            None => Err(Error::new(ErrorKind::NotFound, "entry not found")),
        }
    }
    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<CacacheReader> = {
            Ok(oio::StreamReader::new(CacacheReader::new(
                self.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        let output: CacacheWriter =
            { Ok(CacacheWriter::new(self.core.clone(), path.to_string())) }?;

        Ok(output)
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<CacacheDeleter> = {
            Ok(oio::OneShotDeleter::new(CacacheDeleter::new(
                self.core.clone(),
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
    use std::path::Path;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;

    use super::*;
    use opendal_core::raw::oio::Write as _;

    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);

            let path = std::env::temp_dir().join(format!(
                "opendal-cacache-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).expect("create temporary directory");

            Self { path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    #[tokio::test]
    async fn test_root_and_dir_marker_stat_and_writer_metadata() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let dir = TempDir::new();
            let service = CacacheBuilder::default()
                .datadir(dir.path().to_str().expect("valid utf-8 path"))
                .build()?;
            let ctx = OperationContext::default();

            assert_eq!(
                service
                    .stat(&ctx, "/", OpStat::default())
                    .await?
                    .into_metadata()
                    .mode(),
                EntryMode::DIR
            );
            assert_eq!(
                service
                    .stat(&ctx, "", OpStat::default())
                    .await?
                    .into_metadata()
                    .mode(),
                EntryMode::DIR
            );

            let mut dir_writer = service.write(&ctx, "a/", OpWrite::default())?;
            let dir_meta = dir_writer.close().await?;
            assert_eq!(dir_meta.mode(), EntryMode::DIR);
            assert_eq!(
                service
                    .stat(&ctx, "a/", OpStat::default())
                    .await?
                    .into_metadata()
                    .mode(),
                EntryMode::DIR
            );

            let mut file_writer = service.write(&ctx, "a/file.txt", OpWrite::default())?;
            file_writer.write(Buffer::from("hello")).await?;
            let file_meta = file_writer.close().await?;
            assert_eq!(file_meta.mode(), EntryMode::FILE);
            assert_eq!(file_meta.content_length(), 5);

            let stat_meta = service
                .stat(&ctx, "a/file.txt", OpStat::default())
                .await?
                .into_metadata();
            assert_eq!(stat_meta.mode(), EntryMode::FILE);
            assert_eq!(stat_meta.content_length(), 5);

            // Verify Operator with SimulateLayer::with_stat_dir(false) also observes DIR.
            let srv: Servicer = Arc::new(service);
            let op = Operator::from_parts(OperationContext::default(), srv)
                .layer(layers::SimulateLayer::default().with_stat_dir(false));
            assert_eq!(op.stat("/").await?.mode(), EntryMode::DIR);
            assert_eq!(op.stat("a/").await?.mode(), EntryMode::DIR);
            assert_eq!(op.stat("a/file.txt").await?.mode(), EntryMode::FILE);
            Ok::<_, Error>(())
        })
        .await
        .expect("test timed out")
    }
}
