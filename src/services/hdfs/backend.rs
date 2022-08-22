// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Debug;
use std::io::ErrorKind;
use std::io::Result;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use futures::AsyncReadExt;
use log::debug;
use log::info;
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use super::error::parse_io_error;
use crate::error::other;
use crate::error::BackendError;
use crate::error::ObjectError;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BytesReader;
use crate::BytesWriter;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Scheme;

/// Builder for hdfs services
#[derive(Debug, Default)]
pub struct Builder {
    root: Option<String>,
    name_node: Option<String>,
}

impl Builder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set name_node of this backend.
    ///
    /// Vaild format including:
    ///
    /// - `default`: using the default setting based on hadoop config.
    /// - `hdfs://127.0.0.1:9000`: connect to hdfs cluster.
    pub fn name_node(&mut self, name_node: &str) -> &mut Self {
        if !name_node.is_empty() {
            // Trim trailing `/` so that we can accept `http://127.0.0.1:9000/`
            self.name_node = Some(name_node.trim_end_matches('/').to_string())
        }

        self
    }

    /// Finish the building and create hdfs backend.
    pub fn build(&mut self) -> Result<Backend> {
        info!("backend build started: {:?}", &self);

        let name_node = match &self.name_node {
            None => {
                return Err(other(BackendError::new(
                    HashMap::new(),
                    anyhow!("endpoint must be specified"),
                )))
            }
            Some(v) => v,
        };

        // Make `/` as the default of root.
        let root = match &self.root {
            None => "/".to_string(),
            Some(v) => {
                debug_assert!(!v.is_empty());

                let mut v = v.clone();
                if !v.starts_with('/') {
                    return Err(other(BackendError::new(
                        HashMap::from([("root".to_string(), v.clone())]),
                        anyhow!("root must start with /"),
                    )));
                }
                if !v.ends_with('/') {
                    v.push('/');
                }

                v
            }
        };

        let client = hdrs::Client::connect(name_node).map_err(|e| {
            other(BackendError::new(
                HashMap::from([
                    ("root".to_string(), root.clone()),
                    ("endpoint".to_string(), name_node.clone()),
                ]),
                anyhow!("connect hdfs name node: {}", e),
            ))
        })?;

        // Create root dir if not exist.
        if let Err(e) = client.metadata(&root) {
            if e.kind() == ErrorKind::NotFound {
                debug!("root {} is not exist, creating now", root);

                client.create_dir(&root).map_err(|e| {
                    other(BackendError::new(
                        HashMap::from([
                            ("root".to_string(), root.clone()),
                            ("endpoint".to_string(), name_node.clone()),
                        ]),
                        anyhow!("create root dir: {}", e),
                    ))
                })?
            }
        }

        info!("backend build finished: {:?}", &self);
        Ok(Backend {
            root,
            client: Arc::new(client),
        })
    }

    /// Finish the building and create hdfs backend.
    #[deprecated = "Use Builder::build() instead"]
    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        Ok(Arc::new(self.build()?))
    }
}

/// Backend for hdfs services.
#[derive(Debug, Clone)]
pub struct Backend {
    root: String,
    client: Arc<hdrs::Client>,
}

/// hdrs::Client is thread-safe.
unsafe impl Send for Backend {}
unsafe impl Sync for Backend {}

impl Backend {
    /// Create a builder.
    #[deprecated = "Use Builder::default() instead"]
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Result<Self> {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "name_node" => builder.name_node(v),
                _ => continue,
            };
        }

        builder.build()
    }

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.clone();
        }

        PathBuf::from(&self.root)
            .join(path)
            .to_string_lossy()
            .to_string()
    }

    pub(crate) fn get_rel_path(&self, path: &str) -> String {
        match path.strip_prefix(&self.root) {
            Some(p) => p.to_string(),
            None => unreachable!(
                "invalid path {} that not start with backend root {}",
                &path, &self.root
            ),
        }
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Hdfs)
            .set_root(&self.root)
            .set_capabilities(None);

        am
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = self.get_abs_path(args.path());

        match args.mode() {
            ObjectMode::FILE => {
                let parent = PathBuf::from(&path)
                    .parent()
                    .ok_or_else(|| {
                        other(ObjectError::new(
                            "create",
                            &path,
                            anyhow!("malformed path: {:?}", &path),
                        ))
                    })?
                    .to_path_buf();

                self.client
                    .create_dir(&parent.to_string_lossy())
                    .map_err(|e| parse_io_error(e, "create", &parent.to_string_lossy()))?;

                self.client
                    .open_file()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .map_err(|e| parse_io_error(e, "create", &path))?;

                Ok(())
            }
            ObjectMode::DIR => {
                self.client
                    .create_dir(&path)
                    .map_err(|e| parse_io_error(e, "create", &path))?;

                Ok(())
            }
            ObjectMode::Unknown => unreachable!(),
        }
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = self.get_abs_path(args.path());

        let mut f = self.client.open_file().read(true).open(&path)?;

        if let Some(offset) = args.offset() {
            f.seek(SeekFrom::Start(offset))
                .map_err(|e| parse_io_error(e, "read", &path))?;
        };

        let f: BytesReader = match args.size() {
            None => Box::new(f),
            Some(size) => Box::new(f.take(size)),
        };

        Ok(f)
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = self.get_abs_path(args.path());
        debug!("object {} write start: size {}", &path, args.size());

        let parent = PathBuf::from(&path)
            .parent()
            .ok_or_else(|| {
                other(ObjectError::new(
                    "write",
                    &path,
                    anyhow!("malformed path: {:?}", &path),
                ))
            })?
            .to_path_buf();

        self.client
            .create_dir(&parent.to_string_lossy())
            .map_err(|e| parse_io_error(e, "write", &parent.to_string_lossy()))?;

        let mut f = self
            .client
            .open_file()
            .create(true)
            .write(true)
            .open(&path)?;

        let n = futures::io::copy(r, &mut f).await?;

        Ok(n)
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let path = self.get_abs_path(args.path());

        let meta = self
            .client
            .metadata(&path)
            .map_err(|e| parse_io_error(e, "stat", &path))?;

        let mut m = ObjectMetadata::default();
        if meta.is_dir() {
            m.set_mode(ObjectMode::DIR);
        } else if meta.is_file() {
            m.set_mode(ObjectMode::FILE);
        }
        m.set_content_length(meta.len());
        m.set_last_modified(OffsetDateTime::from(meta.modified()));

        Ok(m)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = self.get_abs_path(args.path());

        let meta = self.client.metadata(&path);

        if let Err(err) = meta {
            return if err.kind() == ErrorKind::NotFound {
                Ok(())
            } else {
                Err(parse_io_error(err, "delete", &path))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let result = if meta.is_dir() {
            self.client.remove_dir(&path)
        } else {
            self.client.remove_file(&path)
        };

        result.map_err(|e| parse_io_error(e, "delete", &path))?;

        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = self.get_abs_path(args.path());

        let f = self
            .client
            .read_dir(&path)
            .map_err(|e| parse_io_error(e, "list", &path))?;

        let rd = DirStream::new(Arc::new(self.clone()), args.path(), f);

        Ok(Box::new(rd))
    }
}
