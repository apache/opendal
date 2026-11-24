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
use std::sync::Mutex;

use super::PERSY_SCHEME;
use super::config::PersyConfig;
use super::core::*;
use super::deleter::PersyDeleter;
use super::reader::*;
use super::writer::PersyWriter;
use opendal_core::raw::*;
use opendal_core::*;

/// persy service support.
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct PersyBuilder {
    pub(super) config: PersyConfig,
}

impl PersyBuilder {
    /// Set the path to the persy data directory. Will create if not exists.
    pub fn datafile(mut self, path: &str) -> Self {
        self.config.datafile = if path.is_empty() {
            None
        } else {
            Some(path.into())
        };
        self
    }

    /// Set the name of the persy segment. Will create if not exists.
    pub fn segment(mut self, path: &str) -> Self {
        self.config.segment = if path.is_empty() {
            None
        } else {
            Some(path.into())
        };
        self
    }

    /// Set the name of the persy index. Will create if not exists.
    pub fn index(mut self, path: &str) -> Self {
        self.config.index = if path.is_empty() {
            None
        } else {
            Some(path.into())
        };
        self
    }
}

impl Builder for PersyBuilder {
    type Config = PersyConfig;

    fn build(self) -> Result<impl Service> {
        let datafile_path = self
            .config
            .datafile
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "datafile is required but not set")
                    .with_context("service", PERSY_SCHEME)
            })?;

        let segment = self
            .config
            .segment
            .filter(|v| !v.is_empty())
            .ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "segment is required but not set")
                    .with_context("service", PERSY_SCHEME)
            })?;

        let index = self.config.index.filter(|v| !v.is_empty()).ok_or_else(|| {
            Error::new(ErrorKind::ConfigInvalid, "index is required but not set")
                .with_context("service", PERSY_SCHEME)
        })?;

        let persy = persy::OpenOptions::new()
            .create(true)
            .open(&datafile_path)
            .map_err(|e| {
                Error::new(ErrorKind::ConfigInvalid, "open db")
                    .with_context("service", PERSY_SCHEME)
                    .with_context("datafile", datafile_path.clone())
                    .set_source(e)
            })?;

        let core = PersyCore {
            datafile: datafile_path.clone(),
            segment,
            index,
            persy,
            write_lock: Arc::new(Mutex::new(())),
        };

        // `persy` only runs the initialization of a datafile while creating it, so an
        // already existing datafile needs its segment and index created here.
        core.init().map_err(|err| {
            err.with_context("service", PERSY_SCHEME)
                .with_context("datafile", datafile_path)
        })?;

        Ok(PersyBackend::new(core))
    }
}

/// Backend for persy services.
#[derive(Clone, Debug)]
pub struct PersyBackend {
    pub(crate) core: Arc<PersyCore>,
    pub(crate) root: String,
    pub(crate) info: ServiceInfo,
    pub(crate) capability: Capability,
}

impl PersyBackend {
    pub fn new(core: PersyCore) -> Self {
        let info = ServiceInfo::new(PERSY_SCHEME, "/", &core.datafile);
        let capability = Capability {
            read: true,
            stat: true,
            write: true,
            write_can_empty: true,
            delete: true,
            ..Default::default()
        };

        Self {
            core: Arc::new(core),
            root: "/".to_string(),
            info,
            capability,
        }
    }
}

impl Service for PersyBackend {
    type Reader = oio::StreamReader<PersyReader>;
    type Writer = PersyWriter;
    type Lister = ();
    type Deleter = oio::OneShotDeleter<PersyDeleter>;
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
            let bs = self.core.get(&p)?;
            match bs {
                Some(bs) => {
                    let metadata = if p.ends_with('/') {
                        MetadataBuilder::dir()
                    } else {
                        MetadataBuilder::file(bs.len() as u64)
                    };
                    Ok(RpStat::new(metadata.build()))
                }
                None => Err(Error::new(ErrorKind::NotFound, "kv not found in persy")),
            }
        }
    }
    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        let output: oio::StreamReader<PersyReader> = {
            Ok(oio::StreamReader::new(PersyReader::new(
                self.clone(),
                path,
                args,
            )))
        }?;

        Ok(output)
    }

    fn write(&self, _ctx: &OperationContext, path: &str, _: OpWrite) -> Result<Self::Writer> {
        let output: PersyWriter = {
            let p = build_absolute_path(&self.root, path);
            Ok(PersyWriter::new(self.core.clone(), p))
        }?;

        Ok(output)
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        let output: oio::OneShotDeleter<PersyDeleter> = {
            Ok(oio::OneShotDeleter::new(PersyDeleter::new(
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
                "opendal-persy-unit-{}-{}",
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

    #[test]
    fn test_empty_config_fields_rejected() -> Result<()> {
        let dir = TempDir::new();
        let datafile = dir.path().join("db.persy");
        let datafile_str = datafile.to_str().expect("valid utf-8 path");

        // Builder setters reject empty values.
        let err = PersyBuilder::default()
            .datafile("")
            .segment("data")
            .index("index")
            .build()
            .expect_err("empty datafile must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let err = PersyBuilder::default()
            .datafile(datafile_str)
            .datafile("")
            .segment("data")
            .index("index")
            .build()
            .expect_err("clearing datafile to empty must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let err = PersyBuilder::default()
            .datafile(datafile_str)
            .segment("")
            .index("index")
            .build()
            .expect_err("empty segment must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        let err = PersyBuilder::default()
            .datafile(datafile_str)
            .segment("data")
            .index("")
            .build()
            .expect_err("empty index must fail");
        assert_eq!(err.kind(), ErrorKind::ConfigInvalid);

        // Config deserialization rejects empty values too.
        for (datafile, segment, index) in [
            ("", "data", "index"),
            (datafile_str, "", "index"),
            (datafile_str, "data", ""),
        ] {
            let err = PersyConfig::from_iter([
                ("datafile".to_string(), datafile.to_string()),
                ("segment".to_string(), segment.to_string()),
                ("index".to_string(), index.to_string()),
            ])?
            .into_builder()
            .build()
            .expect_err("empty config field must fail");
            assert_eq!(err.kind(), ErrorKind::ConfigInvalid);
        }

        assert!(
            !datafile.exists(),
            "rejected config must not create datafile"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_root_and_dir_marker_stat_and_writer_metadata() -> Result<()> {
        tokio::time::timeout(Duration::from_secs(10), async {
            let dir = TempDir::new();
            let datafile = dir.path().join("db.persy");
            let service = PersyBuilder::default()
                .datafile(datafile.to_str().expect("valid utf-8 path"))
                .segment("data")
                .index("index")
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

            let mut root_writer = service.write(&ctx, "/", OpWrite::default())?;
            let root_meta = root_writer.close().await?;
            assert_eq!(root_meta.mode(), EntryMode::DIR);
            assert_eq!(
                service
                    .stat(&ctx, "/", OpStat::default())
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
