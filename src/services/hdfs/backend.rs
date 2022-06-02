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
use log::error;
use log::info;
use log::warn;
use minitrace::trace;
use time::OffsetDateTime;

use super::error::parse_io_error;
use super::object_stream::Readdir;
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
use crate::Metadata;
use crate::ObjectMode;
use crate::ObjectStreamer;
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
    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
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
                if !v.starts_with('/') {
                    return Err(other(BackendError::new(
                        HashMap::from([("root".to_string(), v.clone())]),
                        anyhow!("root must start with /"),
                    )));
                }
                v.to_string()
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
        Ok(Arc::new(Backend {
            root,
            client: Arc::new(client),
        }))
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
    pub fn build() -> Builder {
        Builder::default()
    }

    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.clone();
        }

        format!("{}{}", &self.root, path.trim_start_matches('/'))
    }
}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Hdfs).set_root(&self.root);

        am
    }

    #[trace("create")]
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
                    .map_err(|e| {
                        let e = parse_io_error(e, "create", &parent.to_string_lossy());
                        error!("object {} mkdir for parent {:?}: {:?}", &path, &parent, e);
                        e
                    })?;

                self.client
                    .open_file()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&path)
                    .map_err(|e| {
                        let e = parse_io_error(e, "create", &path);
                        error!("object {} create: {:?}", &path, e);
                        e
                    })?;

                debug!("object {path} create file");
                Ok(())
            }
            ObjectMode::DIR => {
                self.client.create_dir(&path).map_err(|e| {
                    let e = parse_io_error(e, "create", &path);
                    error!("object {} create: {:?}", &path, e);
                    e
                })?;

                debug!("object {path} create dir");
                Ok(())
            }
            ObjectMode::Unknown => unreachable!(),
        }
    }

    #[trace("read")]
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = self.get_abs_path(args.path());

        debug!(
            "object {} read start: offset {:?}, size {:?}",
            &path,
            args.offset(),
            args.size()
        );

        let mut f = self.client.open_file().read(true).open(&path)?;

        if let Some(offset) = args.offset() {
            f.seek(SeekFrom::Start(offset)).map_err(|e| {
                let e = parse_io_error(e, "read", &path);
                error!("object {} seek: {:?}", &path, e);
                e
            })?;
        };

        let f: BytesReader = match args.size() {
            None => Box::new(f),
            Some(size) => Box::new(f.take(size)),
        };

        debug!(
            "object {} reader created: offset {:?}, size {:?}",
            &path,
            args.offset(),
            args.size()
        );
        Ok(f)
    }

    #[trace("write")]
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
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
            .map_err(|e| {
                let e = parse_io_error(e, "write", &parent.to_string_lossy());
                error!(
                    "object {} create_dir_all for parent {}: {:?}",
                    &path,
                    &parent.to_string_lossy(),
                    e
                );
                e
            })?;

        let f = self
            .client
            .open_file()
            .create(true)
            .write(true)
            .open(&path)?;

        debug!("object {} write finished: size {:?}", &path, args.size());
        Ok(Box::new(f))
    }

    #[trace("stat")]
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let path = self.get_abs_path(args.path());
        debug!("object {} stat start", &path);

        let meta = self.client.metadata(&path).map_err(|e| {
            let e = parse_io_error(e, "stat", &path);
            warn!("object {} stat: {:?}", &path, e);
            e
        })?;

        let mut m = Metadata::default();
        if meta.is_dir() {
            let mut p = args.path().to_string();
            if !p.ends_with('/') {
                p.push('/')
            }
            m.set_path(&p);
            m.set_mode(ObjectMode::DIR);
        } else if meta.is_file() {
            m.set_path(args.path());
            m.set_mode(ObjectMode::FILE);
        }
        m.set_content_length(meta.len());
        m.set_last_modified(OffsetDateTime::from(meta.modified()));
        m.set_complete();

        debug!("object {} stat finished: {:?}", &path, m);
        Ok(m)
    }

    #[trace("delete")]
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = self.get_abs_path(args.path());
        debug!("object {} delete start", &path);

        let meta = self.client.metadata(&path);

        if let Err(err) = meta {
            return if err.kind() == ErrorKind::NotFound {
                Ok(())
            } else {
                let e = parse_io_error(err, "delete", &path);
                error!("object {} delete: {:?}", &path, e);
                Err(e)
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

        debug!("object {} delete finished", &path);
        Ok(())
    }

    #[trace("list")]
    async fn list(&self, args: &OpList) -> Result<ObjectStreamer> {
        let path = self.get_abs_path(args.path());
        debug!("object {} list start", &path);

        let f = self.client.read_dir(&path).map_err(|e| {
            let e = parse_io_error(e, "list", &path);
            error!("object {} list: {:?}", &path, e);
            e
        })?;

        let rd = Readdir::new(Arc::new(self.clone()), &self.root, args.path(), f);

        Ok(Box::new(rd))
    }
}
