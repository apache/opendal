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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::io::ErrorKind;
use std::io::Result;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use log::debug;
use log::error;
use time::OffsetDateTime;
use tokio::sync::Mutex;

use super::error::parse_io_error;
use crate::error::other;
use crate::error::ObjectError;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpList;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::Accessor;
use crate::BytesReader;
use crate::BytesWriter;
use crate::Metadata;
use crate::ObjectMode;
use crate::ObjectStreamer;

pub struct Builder {}

#[derive(Debug, Clone)]
pub struct Backend {
    root: String,
    client: Arc<hdrs::Client>,
}

unsafe impl Send for Backend {}
unsafe impl Sync for Backend {}

impl Backend {
    pub(crate) fn get_abs_path(&self, path: &str) -> String {
        if path == "/" {
            return self.root.clone();
        }

        PathBuf::from(&self.root)
            .join(path)
            .to_string_lossy()
            .to_string()
    }
}

#[async_trait]
impl Accessor for Backend {
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
                    .open(args.path())
                    .map_err(|e| {
                        let e = parse_io_error(e, "create", &path);
                        error!("object {} create: {:?}", &path, e);
                        e
                    })?;

                Ok(())
            }
            ObjectMode::DIR => {
                self.client.create_dir(args.path()).map_err(|e| {
                    let e = parse_io_error(e, "create", &path);
                    error!("object {} create: {:?}", &path, e);
                    e
                })?;

                Ok(())
            }
            ObjectMode::Unknown => unreachable!(),
        }
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        let path = self.get_abs_path(args.path());
        debug!(
            "object {} read start: offset {:?}, size {:?}",
            &path,
            args.offset(),
            args.size()
        );

        let mut f = self.client.open_file().read(true).open(args.path())?;

        if let Some(offset) = args.offset() {
            f.seek(SeekFrom::Start(offset)).map_err(|e| {
                let e = parse_io_error(e, "read", &path);
                error!("object {} seek: {:?}", &path, e);
                e
            })?;
        };

        // TODO: implement take support

        debug!(
            "object {} reader created: offset {:?}, size {:?}",
            &path,
            args.offset(),
            args.size()
        );
        Ok(Box::new(f))
    }

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

        let mut f = self
            .client
            .open_file()
            .create(true)
            .write(true)
            .open(args.path())?;

        debug!("object {} write finished: size {:?}", &path, args.size());
        Ok(Box::new(f))
    }

    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        let path = self.get_abs_path(args.path());
        debug!("object {} stat start", &path);

        let meta = self.client.metadata(&path).map_err(|e| {
            let e = parse_io_error(e, "stat", &path);
            error!("object {} stat: {:?}", &path, e);
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

        debug!("object {} stat finished", &path);
        Ok(m)
    }

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

    async fn list(&self, args: &OpList) -> Result<ObjectStreamer> {
        todo!()
    }
}
