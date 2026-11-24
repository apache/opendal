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

use super::MEMORY_SCHEME;
use super::config::MemoryConfig;
use super::core::*;
use super::deleter::MemoryDeleter;
use super::lister::MemoryLister;
use super::writer::MemoryWriter;
use crate::raw::oio;
use crate::raw::*;
use crate::*;

/// In memory service support. (BTreeMap Based)
#[doc = include_str!("docs.md")]
#[derive(Debug, Default)]
pub struct MemoryBuilder {
    pub(super) config: MemoryConfig,
}

impl MemoryBuilder {
    /// Set the root for BTreeMap.
    pub fn root(mut self, path: &str) -> Self {
        self.config.root = Some(path.into());
        self
    }
}

impl Builder for MemoryBuilder {
    type Config = MemoryConfig;

    fn build(self) -> Result<impl Service> {
        let root = normalize_root(self.config.root.as_deref().unwrap_or("/"));

        let core = MemoryCore::new();
        Ok(MemoryBackend::new(core).with_normalized_root(root))
    }
}

/// MemoryBackend implements [`Service`] for the in-memory map.
#[derive(Debug, Clone)]
pub struct MemoryBackend {
    core: Arc<MemoryCore>,
    root: String,
    info: ServiceInfo,
    capability: Capability,
}

impl MemoryBackend {
    fn new(core: MemoryCore) -> Self {
        let capability = Capability {
            read: true,
            read_with_suffix: true,
            write: true,
            write_can_empty: true,
            write_with_cache_control: true,
            write_with_content_type: true,
            write_with_content_disposition: true,
            write_with_content_encoding: true,
            write_with_content_language: true,
            write_with_if_not_exists: true,
            write_with_user_metadata: true,
            delete: true,
            delete_with_recursive: true,
            stat: true,
            list: true,
            list_with_recursive: true,
            list_with_recursive_flat: true,
            list_with_start_after: true,
            ..Default::default()
        };

        Self {
            info: ServiceInfo::new(MEMORY_SCHEME, "/", format!("{:p}", Arc::as_ptr(&core.data))),
            core: Arc::new(core),
            root: "/".to_string(),
            capability,
        }
    }

    fn with_normalized_root(mut self, root: String) -> Self {
        self.info = ServiceInfo::new(MEMORY_SCHEME, root.clone(), self.info.name());
        self.root = root;
        self
    }
}

impl Service for MemoryBackend {
    type Reader = oio::StreamReader<MemoryReader>;
    type Writer = MemoryWriter;
    type Lister = oio::HierarchyLister<MemoryLister>;
    type Deleter = oio::OneShotDeleter<MemoryDeleter>;
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
        _: &OperationContext,
        _: &str,
        _: OpCreateDir,
    ) -> Result<RpCreateDir> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn stat(&self, _: &OperationContext, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_absolute_path(&self.root, path);

        if p == build_absolute_path(&self.root, "") {
            Ok(RpStat::new(MetadataBuilder::dir().build()))
        } else {
            match self.core.get(&p)? {
                Some(value) => Ok(RpStat::new(value.metadata)),
                None => Err(Error::new(
                    ErrorKind::NotFound,
                    "memory doesn't have this path",
                )),
            }
        }
    }

    fn read(&self, _ctx: &OperationContext, path: &str, args: OpRead) -> Result<Self::Reader> {
        Ok(oio::StreamReader::new(MemoryReader::new(
            self.clone(),
            path,
            args,
        )))
    }

    fn write(&self, _ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        let p = build_absolute_path(&self.root, path);
        Ok(MemoryWriter::new(self.core.clone(), p, args))
    }

    fn delete(&self, _ctx: &OperationContext) -> Result<Self::Deleter> {
        Ok(oio::OneShotDeleter::new(MemoryDeleter::new(
            self.core.clone(),
            self.root.clone(),
        )))
    }

    fn list(&self, _ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        let p = build_absolute_path(&self.root, path);
        let start_after = args
            .start_after()
            .map(|v| build_absolute_path(&self.root, v));
        let mut keys = self.core.scan(&p, start_after.as_deref())?;

        // Non-recursive lists fold `d/x` and `d/y` into `d/`. If the marker is `d/` or `d/x`,
        // `d/y` would still yield `d/`, which sorts at or before the marker, so drop all of `d/`.
        if !args.recursive()
            && let Some(marker) = start_after.as_deref()
            && let Some(rest) = marker.strip_prefix(p.as_str())
            && let Some(idx) = rest.find('/')
        {
            let dir = &marker[..p.len() + idx + 1];
            keys.retain(|key| !key.starts_with(dir));
        }

        let lister = MemoryLister::new(&self.root, keys);
        let lister = oio::HierarchyLister::new(lister, path, args.recursive());

        Ok(lister)
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn rename(
        &self,
        _: &OperationContext,
        _: &str,
        _: &str,
        _: OpRename,
    ) -> Result<RpRename> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "operation is not supported",
        ))
    }
}

/// Reader returned by this backend.
pub struct MemoryReader {
    backend: MemoryBackend,
    path: String,
}

impl MemoryReader {
    fn new(backend: MemoryBackend, path: &str, _: OpRead) -> Self {
        Self {
            backend,
            path: path.to_string(),
        }
    }
}

impl oio::StreamRead for MemoryReader {
    async fn open(&self, range: BytesRange) -> Result<(RpRead, Box<dyn oio::ReadStreamDyn>)> {
        let backend = &self.backend;
        let path = self.path.as_str();
        let p = build_absolute_path(&backend.root, path);

        let value = match backend.core.get(&p)? {
            Some(value) => value,
            None => {
                return Err(Error::new(
                    ErrorKind::NotFound,
                    "memory doesn't have this path",
                ));
            }
        };

        let content = value
            .content
            .slice(range.to_content_range(value.content.len())?);
        Ok((
            RpRead::new(value.metadata),
            Box::new(content) as Box<dyn oio::ReadStreamDyn>,
        ))
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[tokio::test]
    async fn test_write_content_language_overwrite() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        let path = "test.txt";
        let written = op.write_with(path, "hello").content_language("en").await?;
        assert_eq!(written.content_language(), Some("en"));
        assert_eq!(op.stat(path).await?.content_language(), Some("en"));

        op.write(path, "bonjour").await?;
        assert_eq!(op.stat(path).await?.content_language(), None);
        Ok(())
    }

    #[tokio::test]
    async fn test_writer_close_and_overwrite_user_metadata() -> Result<()> {
        let op = Operator::new(services::Memory::default())?;
        let path = "test.txt";
        let mut writer = op
            .writer_with(path)
            .user_metadata([("owner".to_string(), "opendal".to_string())])
            .await?;
        writer.write("hello").await?;
        let written = writer.close().await?;
        assert_eq!(
            written
                .user_metadata()
                .and_then(|metadata| metadata.get("owner")),
            Some("opendal")
        );

        op.write_with(path, "bonjour").user_metadata([]).await?;
        assert!(op.stat(path).await?.user_metadata().unwrap().is_empty());

        op.write(path, "hello again").await?;
        assert!(op.stat(path).await?.user_metadata().is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_list_with_start_after() {
        // (name, path, recursive, start_after, expected)
        let cases: &[(&str, &str, bool, &str, &[&str])] = &[
            ("before prefix", "a/d/", true, "0", &["a/d/x", "a/d/y"]),
            ("at key", "a/", true, "a/2", &["a/d/x", "a/d/y", "a/z"]),
            ("between keys", "a/d/", true, "a/d/w", &["a/d/x", "a/d/y"]),
            ("key in folder", "a/", true, "a/d/x", &["a/d/y", "a/z"]),
            ("after prefix", "a/", true, "a0", &[]),
            ("folds folders", "a/", false, "a/1", &["a/2", "a/d/", "a/z"]),
            ("before folder", "a/", false, "a/c", &["a/d/", "a/z"]),
            ("at folder skips it", "a/", false, "a/d/", &["a/z"]),
            ("in folder skips it", "a/", false, "a/d/x", &["a/z"]),
            ("prefix without slash", "a/d", false, "a/d/x", &[]),
            ("list root", "/", false, "a/2", &["a0", "b/"]),
            ("list root recursively", "/", true, "a0", &["b/1"]),
        ];

        for root in ["/", "/root/"] {
            let op = Operator::new(services::Memory::default().root(root)).unwrap();
            for path in ["a/1", "a/2", "a/d/x", "a/d/y", "a/z", "a0", "b/1"] {
                op.write(path, "x").await.unwrap();
            }

            for &(name, path, recursive, start_after, expected) in cases {
                let actual: Vec<String> = op
                    .list_with(path)
                    .recursive(recursive)
                    .start_after(start_after)
                    .await
                    .unwrap()
                    .into_iter()
                    .map(|e| e.path().to_string())
                    .collect();
                assert_eq!(actual, expected, "{name} (root {root})");
            }
        }
    }

    #[tokio::test]
    async fn test_create_dir_root_and_list() -> Result<()> {
        for root in ["/", "/sub/"] {
            let builder = if root == "/" {
                services::Memory::default()
            } else {
                services::Memory::default().root(root)
            };
            let op = Operator::new(builder)?;
            #[cfg(not(feature = "auto-simulate"))]
            let op = op.layer(crate::layers::SimulateLayer::default());

            assert_eq!(op.stat("/").await?.mode(), EntryMode::DIR);
            assert!(op.list("/").await?.is_empty());

            op.create_dir("/").await?;
            assert_eq!(op.stat("/").await?.mode(), EntryMode::DIR);

            for entries in [
                op.list("/").await?,
                op.list("").await?,
                op.list_with("/").recursive(true).await?,
                op.list_with("").recursive(true).await?,
            ] {
                let actual: Vec<_> = entries
                    .iter()
                    .map(|e| (e.path(), e.metadata().mode()))
                    .collect();
                assert_eq!(actual, vec![("/", EntryMode::DIR)], "root {root}");
            }

            op.create_dir("a/").await?;
            op.write("a/b.txt", "hello").await?;

            let flat: Vec<_> = op
                .list("/")
                .await?
                .into_iter()
                .map(|e| (e.path().to_string(), e.metadata().mode()))
                .collect();
            assert_eq!(
                flat,
                vec![
                    ("/".to_string(), EntryMode::DIR),
                    ("a/".to_string(), EntryMode::DIR),
                ],
                "root {root}"
            );

            let recursive: Vec<_> = op
                .list_with("/")
                .recursive(true)
                .await?
                .into_iter()
                .map(|e| (e.path().to_string(), e.metadata().mode()))
                .collect();
            assert_eq!(
                recursive,
                vec![
                    ("/".to_string(), EntryMode::DIR),
                    ("a/".to_string(), EntryMode::DIR),
                    ("a/b.txt".to_string(), EntryMode::FILE),
                ],
                "root {root}"
            );

            let sub: Vec<_> = op
                .list("a/")
                .await?
                .into_iter()
                .map(|e| (e.path().to_string(), e.metadata().mode()))
                .collect();
            assert_eq!(
                sub,
                vec![
                    ("a/".to_string(), EntryMode::DIR),
                    ("a/b.txt".to_string(), EntryMode::FILE),
                ],
                "root {root}"
            );

            let after_root: Vec<_> = op
                .list_with("/")
                .start_after("/")
                .await?
                .into_iter()
                .map(|e| e.path().to_string())
                .collect();
            assert_eq!(after_root, vec!["a/".to_string()], "root {root}");

            op.delete_with("a/").recursive(true).await?;
            op.delete("/").await?;
            assert!(op.list("/").await?.is_empty(), "root {root}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_dir_marker_stored_as_dir_metadata() -> Result<()> {
        let srv: crate::raw::Servicer = std::sync::Arc::new(services::Memory::default().build()?);
        let op = Operator::from_parts(OperationContext::default(), srv)
            .layer(crate::layers::SimulateLayer::default().with_stat_dir(false));
        op.create_dir("a/").await?;
        assert_eq!(op.stat("a/").await?.mode(), EntryMode::DIR);
        Ok(())
    }
}
