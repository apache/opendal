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

use std::cmp::min;
use std::fmt::Debug;
use std::io;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use log::debug;
use time::OffsetDateTime;

use super::dir_stream::DirStream;
use super::error::parse_io_error;
use crate::raw::*;
use crate::*;

/// Builder for hdfs services
#[derive(Debug, Default)]
pub struct Builder {
    root: Option<String>,
    name_node: Option<String>,
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();

        for (k, v) in it {
            let v = v.as_str();
            match k.as_ref() {
                "root" => builder.root(v),
                "name_node" => builder.name_node(v),
                _ => continue,
            };
        }

        builder
    }

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
    pub fn build(&mut self) -> Result<impl Accessor> {
        debug!("backend build started: {:?}", &self);

        let name_node = match &self.name_node {
            Some(v) => v,
            None => {
                return Err(
                    Error::new(ErrorKind::BackendConfigInvalid, "name node is empty")
                        .with_context("service", Scheme::Hdfs),
                )
            }
        };

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let client = hdrs::Client::connect(name_node).map_err(parse_io_error)?;

        // Create root dir if not exist.
        if let Err(e) = client.metadata(&root) {
            if e.kind() == io::ErrorKind::NotFound {
                debug!("root {} is not exist, creating now", root);

                client.create_dir(&root).map_err(parse_io_error)?
            }
        }

        debug!("backend build finished: {:?}", &self);
        Ok(apply_wrapper(Backend {
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

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        let mut am = AccessorMetadata::default();
        am.set_scheme(Scheme::Hdfs)
            .set_root(&self.root)
            .set_capabilities(
                AccessorCapability::Read
                    | AccessorCapability::Write
                    | AccessorCapability::List
                    | AccessorCapability::Blocking,
            )
            .set_hints(AccessorHint::ReadIsSeekable);

        am
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let p = build_rooted_abs_path(&self.root, path);

        match args.mode() {
            ObjectMode::FILE => {
                let parent = PathBuf::from(&p)
                    .parent()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "path shoud have parent but not, it must be malformed",
                        )
                        .with_context("input", &p)
                    })?
                    .to_path_buf();

                self.client
                    .create_dir(&parent.to_string_lossy())
                    .map_err(parse_io_error)?;

                self.client
                    .open_file()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&p)
                    .map_err(parse_io_error)?;

                Ok(RpCreate::default())
            }
            ObjectMode::DIR => {
                self.client.create_dir(&p).map_err(parse_io_error)?;

                Ok(RpCreate::default())
            }
            ObjectMode::Unknown => unreachable!(),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, output::Reader)> {
        use output::ReadExt;

        let p = build_rooted_abs_path(&self.root, path);

        // This will be addressed by https://github.com/datafuselabs/opendal/issues/506
        let meta = self.client.metadata(&p).map_err(parse_io_error)?;

        let f = self
            .client
            .open_file()
            .read(true)
            .open(&p)
            .map_err(parse_io_error)?;

        let br = args.range();
        let (start, end) = match (br.offset(), br.size()) {
            // Read a specific range.
            (Some(offset), Some(size)) => (offset, min(offset + size, meta.len())),
            // Read from offset.
            (Some(offset), None) => (offset, meta.len()),
            // Read the last size bytes.
            (None, Some(size)) => (meta.len() - size, meta.len()),
            // Read the whole file.
            (None, None) => (0, meta.len()),
        };

        let mut r = output::into_reader::from_fd(f, start, end);
        // Rewind to make sure we are on the correct offset.
        r.seek(SeekFrom::Start(0)).await.map_err(parse_io_error)?;

        Ok((RpRead::new(end - start), Box::new(r)))
    }

    async fn write(&self, path: &str, _: OpWrite, r: input::Reader) -> Result<RpWrite> {
        let p = build_rooted_abs_path(&self.root, path);

        let parent = PathBuf::from(&p)
            .parent()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "path shoud have parent but not, it must be malformed",
                )
                .with_context("input", &p)
            })?
            .to_path_buf();

        self.client
            .create_dir(&parent.to_string_lossy())
            .map_err(parse_io_error)?;

        let mut f = self
            .client
            .open_file()
            .create(true)
            .write(true)
            .open(&p)
            .map_err(parse_io_error)?;

        let n = futures::io::copy(r, &mut f).await.map_err(parse_io_error)?;

        Ok(RpWrite::new(n))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p).map_err(parse_io_error)?;

        let mode = if meta.is_dir() {
            ObjectMode::DIR
        } else if meta.is_file() {
            ObjectMode::FILE
        } else {
            ObjectMode::Unknown
        };
        let mut m = ObjectMetadata::new(mode);
        m.set_content_length(meta.len());
        m.set_last_modified(OffsetDateTime::from(meta.modified()));

        Ok(RpStat::new(m))
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p);

        if let Err(err) = meta {
            return if err.kind() == io::ErrorKind::NotFound {
                Ok(RpDelete::default())
            } else {
                Err(parse_io_error(err))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let result = if meta.is_dir() {
            self.client.remove_dir(&p)
        } else {
            self.client.remove_file(&p)
        };

        result.map_err(parse_io_error)?;

        Ok(RpDelete::default())
    }

    async fn list(&self, path: &str, _: OpList) -> Result<(RpList, ObjectPager)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match self.client.read_dir(&p) {
            Ok(f) => f,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((RpList::default(), Box::new(EmptyObjectPager)))
                } else {
                    Err(parse_io_error(e))
                }
            }
        };

        let rd = DirStream::new(&self.root, f);

        Ok((RpList::default(), Box::new(rd)))
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        let p = build_rooted_abs_path(&self.root, path);

        match args.mode() {
            ObjectMode::FILE => {
                let parent = PathBuf::from(&p)
                    .parent()
                    .ok_or_else(|| {
                        Error::new(
                            ErrorKind::Unexpected,
                            "path shoud have parent but not, it must be malformed",
                        )
                        .with_context("input", &p)
                    })?
                    .to_path_buf();

                self.client
                    .create_dir(&parent.to_string_lossy())
                    .map_err(parse_io_error)?;

                self.client
                    .open_file()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&p)
                    .map_err(parse_io_error)?;

                Ok(RpCreate::default())
            }
            ObjectMode::DIR => {
                self.client.create_dir(&p).map_err(parse_io_error)?;

                Ok(RpCreate::default())
            }
            ObjectMode::Unknown => unreachable!(),
        }
    }

    fn blocking_read(
        &self,
        path: &str,
        args: OpRead,
    ) -> Result<(RpRead, BlockingOutputBytesReader)> {
        use std::io::Read;

        let p = build_rooted_abs_path(&self.root, path);

        // This will be addressed by https://github.com/datafuselabs/opendal/issues/506
        let meta = self.client.metadata(&p).map_err(parse_io_error)?;

        let mut f = self
            .client
            .open_file()
            .read(true)
            .open(&p)
            .map_err(parse_io_error)?;

        let br = args.range();

        let (r, size): (BlockingOutputBytesReader, _) = match (br.offset(), br.size()) {
            (Some(offset), Some(size)) => {
                f.seek(SeekFrom::Start(offset)).map_err(parse_io_error)?;
                (Box::new(f.take(size)), min(size, meta.len() - offset))
            }
            (Some(offset), None) => {
                f.seek(SeekFrom::Start(offset)).map_err(parse_io_error)?;
                (Box::new(f), meta.len() - offset)
            }
            (None, Some(size)) => {
                // hdfs doesn't support seed from end.
                f.seek(SeekFrom::Start(meta.len() - size))
                    .map_err(parse_io_error)?;
                (Box::new(f), size)
            }
            (None, None) => (Box::new(f), meta.len()),
        };

        Ok((RpRead::new(size), r))
    }

    fn blocking_write(
        &self,
        path: &str,
        _: OpWrite,
        mut r: input::BlockingReader,
    ) -> Result<RpWrite> {
        let p = build_rooted_abs_path(&self.root, path);

        let parent = PathBuf::from(&p)
            .parent()
            .ok_or_else(|| {
                Error::new(
                    ErrorKind::Unexpected,
                    "path shoud have parent but not, it must be malformed",
                )
                .with_context("input", &p)
            })?
            .to_path_buf();

        self.client
            .create_dir(&parent.to_string_lossy())
            .map_err(parse_io_error)?;

        let mut f = self
            .client
            .open_file()
            .create(true)
            .write(true)
            .open(&p)
            .map_err(parse_io_error)?;

        let n = std::io::copy(&mut r, &mut f).map_err(parse_io_error)?;

        Ok(RpWrite::new(n))
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p).map_err(parse_io_error)?;

        let mode = if meta.is_dir() {
            ObjectMode::DIR
        } else if meta.is_file() {
            ObjectMode::FILE
        } else {
            ObjectMode::Unknown
        };
        let mut m = ObjectMetadata::new(mode);
        m.set_content_length(meta.len());
        m.set_last_modified(OffsetDateTime::from(meta.modified()));

        Ok(RpStat::new(m))
    }

    fn blocking_delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        let p = build_rooted_abs_path(&self.root, path);

        let meta = self.client.metadata(&p);

        if let Err(err) = meta {
            return if err.kind() == io::ErrorKind::NotFound {
                Ok(RpDelete::default())
            } else {
                Err(parse_io_error(err))
            };
        }

        // Safety: Err branch has been checked, it's OK to unwrap.
        let meta = meta.ok().unwrap();

        let result = if meta.is_dir() {
            self.client.remove_dir(&p)
        } else {
            self.client.remove_file(&p)
        };

        result.map_err(parse_io_error)?;

        Ok(RpDelete::default())
    }

    fn blocking_list(&self, path: &str, _: OpList) -> Result<(RpList, BlockingObjectPager)> {
        let p = build_rooted_abs_path(&self.root, path);

        let f = match self.client.read_dir(&p) {
            Ok(f) => f,
            Err(e) => {
                return if e.kind() == io::ErrorKind::NotFound {
                    Ok((RpList::default(), Box::new(EmptyBlockingObjectPager)))
                } else {
                    Err(parse_io_error(e))
                }
            }
        };

        let rd = DirStream::new(&self.root, f);

        Ok((RpList::default(), Box::new(rd)))
    }
}
