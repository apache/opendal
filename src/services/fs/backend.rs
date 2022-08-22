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
use std::io::ErrorKind;
use std::io::Result;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use async_compat::Compat;
use async_trait::async_trait;
use futures::AsyncReadExt;
use futures::AsyncSeekExt;
use log::info;
use time::OffsetDateTime;
use tokio::fs;

use super::dir_stream::DirStream;
use super::error::parse_io_error;
use crate::accessor::AccessorMetadata;
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
use crate::BytesReader;
use crate::DirStreamer;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::Scheme;

/// Builder for fs backend.
#[derive(Default, Debug)]
pub struct Builder {
    root: Option<String>,
}

impl Builder {
    /// Set root for backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Consume current builder to build a fs backend.
    pub fn build(&mut self) -> Result<Backend> {
        info!("backend build started: {:?}", &self);

        // Make `/` as the default of root.
        let root = match &self.root {
            None => "/".to_string(),
            Some(v) => {
                debug_assert!(!v.is_empty());

                let mut v = v.clone();

                if !v.starts_with('/') {
                    return Err(other(BackendError::new(
                        HashMap::from([("root".to_string(), v.clone())]),
                        anyhow!("Root must start with /"),
                    )));
                }
                if !v.ends_with('/') {
                    v.push('/');
                }

                v
            }
        };

        // If root dir is not exist, we must create it.
        if let Err(e) = std::fs::metadata(&root) {
            if e.kind() == ErrorKind::NotFound {
                std::fs::create_dir_all(&root).map_err(|e| parse_io_error(e, "build", &root))?;
            }
        }

        info!("backend build finished: {:?}", &self);
        Ok(Backend { root })
    }

    /// Consume current builder to build an fs backend.
    #[deprecated = "Use Builder::build() instead"]
    pub async fn finish(&mut self) -> Result<Arc<dyn Accessor>> {
        info!("backend build started: {:?}", &self);

        // Make `/` as the default of root.
        let root = match &self.root {
            None => "/".to_string(),
            Some(v) => {
                debug_assert!(!v.is_empty());

                let mut v = v.clone();

                if !v.starts_with('/') {
                    return Err(other(BackendError::new(
                        HashMap::from([("root".to_string(), v.clone())]),
                        anyhow!("Root must start with /"),
                    )));
                }
                if !v.ends_with('/') {
                    v.push('/');
                }

                v
            }
        };

        // If root dir is not exist, we must create it.
        if let Err(e) = fs::metadata(&root).await {
            if e.kind() == std::io::ErrorKind::NotFound {
                fs::create_dir_all(&root)
                    .await
                    .map_err(|e| parse_io_error(e, "build", &root))?;
            }
        }

        info!("backend build finished: {:?}", &self);
        Ok(Arc::new(Backend { root }))
    }
}

/// Backend is used to serve `Accessor` support for posix alike fs.
#[derive(Debug, Clone)]
pub struct Backend {
    root: String,
}

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
        am.set_scheme(Scheme::Fs)
            .set_root(&self.root)
            .set_capabilities(None);

        am
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        let path = self.get_abs_path(args.path());

        if args.mode() == ObjectMode::FILE {
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

            fs::create_dir_all(&parent)
                .await
                .map_err(|e| parse_io_error(e, "create", &parent.to_string_lossy()))?;

            fs::OpenOptions::new()
                .create(true)
                .write(true)
                .open(&path)
                .await
                .map_err(|e| parse_io_error(e, "create", &path))?;

            return Ok(());
        }

        if args.mode() == ObjectMode::DIR {
            fs::create_dir_all(&path)
                .await
                .map_err(|e| parse_io_error(e, "create", &path))?;

            return Ok(());
        }

        unreachable!()
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = self.get_abs_path(args.path());

        let f = fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .await
            .map_err(|e| parse_io_error(e, "read", &path))?;

        let mut f = Compat::new(f);

        if let Some(offset) = args.offset() {
            f.seek(SeekFrom::Start(offset))
                .await
                .map_err(|e| parse_io_error(e, "read", &path))?;
        };

        let r: BytesReader = match args.size() {
            Some(size) => Box::new(f.take(size)),
            None => Box::new(f),
        };

        Ok(Box::new(r))
    }

    async fn write(&self, args: &OpWrite, r: BytesReader) -> Result<u64> {
        let path = self.get_abs_path(args.path());

        // Create dir before write path.
        //
        // TODO(xuanwo): There are many works to do here:
        //   - Is it safe to create dir concurrently?
        //   - Do we need to extract this logic as new util functions?
        //   - Is it better to check the parent dir exists before call mkdir?
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

        fs::create_dir_all(&parent)
            .await
            .map_err(|e| parse_io_error(e, "write", &parent.to_string_lossy()))?;

        let f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| parse_io_error(e, "write", &path))?;

        let mut f = Compat::new(f);

        let size = futures::io::copy(r, &mut f).await?;

        Ok(size)
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        let path = self.get_abs_path(args.path());

        let meta = fs::metadata(&path)
            .await
            .map_err(|e| parse_io_error(e, "stat", &path))?;

        let mut m = ObjectMetadata::default();
        if meta.is_dir() {
            m.set_mode(ObjectMode::DIR);
        } else if meta.is_file() {
            m.set_mode(ObjectMode::FILE);
        } else {
            m.set_mode(ObjectMode::Unknown);
        }
        m.set_content_length(meta.len() as u64);
        m.set_last_modified(
            meta.modified()
                .map(OffsetDateTime::from)
                .map_err(|e| parse_io_error(e, "stat", &path))?,
        );

        Ok(m)
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        let path = self.get_abs_path(args.path());

        // PathBuf.is_dir() is not free, call metadata directly instead.
        let meta = fs::metadata(&path).await;

        if let Err(err) = meta {
            return if err.kind() == ErrorKind::NotFound {
                Ok(())
            } else {
                Err(parse_io_error(err, "delete", &path))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let f = if meta.is_dir() {
            fs::remove_dir(&path).await
        } else {
            fs::remove_file(&path).await
        };

        f.map_err(|e| parse_io_error(e, "delete", &path))?;

        Ok(())
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        let path = self.get_abs_path(args.path());

        let f = std::fs::read_dir(&path).map_err(|e| parse_io_error(e, "list", &path))?;

        let rd = DirStream::new(Arc::new(self.clone()), args.path(), f);

        Ok(Box::new(rd))
    }
}
