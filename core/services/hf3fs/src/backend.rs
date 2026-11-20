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

use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

use opendal_core::raw::*;
use opendal_core::*;

use crate::HF3FS_SCHEME;
use crate::config::Hf3fsConfig;
use crate::driver::IoDriver;
use crate::ffi::Api;
use crate::reader::Hf3fsReader;
use crate::writer::Hf3fsWriter;

/// Access 3FS through USRBIO for data and the mounted filesystem for metadata.
///
/// Requires Linux, a running 3FS FUSE client, and its compatible USRBIO shared
/// library. The root must already exist. Reads retain shared memory while space
/// permits; writes copy into shared memory. See the crate documentation for
/// memory limits and failure semantics.
#[derive(Debug, Default)]
pub struct Hf3fsBuilder {
    pub(crate) config: Hf3fsConfig,
}

impl Hf3fsBuilder {
    /// Set the existing absolute directory used as the storage root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = Some(root.into());
        self
    }

    /// Set the path to the compatible `libhf3fs_api_shared.so` library.
    /// Defaults to the system dynamic loader's search path.
    pub fn library_path(mut self, path: &str) -> Self {
        self.config.library_path = Some(path.into());
        self
    }

    /// Set the positive maximum number of in-flight requests per ring.
    /// Defaults to 32. Each operator owns one read ring and one write ring.
    pub fn queue_depth(mut self, depth: usize) -> Self {
        self.config.queue_depth = Some(depth);
        self
    }

    /// Set the positive maximum request size in bytes. Defaults to 1 MiB.
    /// Shared memory usage is approximately `3 * queue_depth * block_size`.
    pub fn block_size(mut self, size: usize) -> Self {
        self.config.block_size = Some(size);
        self
    }
}

impl Builder for Hf3fsBuilder {
    type Config = Hf3fsConfig;

    fn build(self) -> Result<impl Service> {
        let depth = self.config.queue_depth.unwrap_or(32);
        let block_size = self.config.block_size.unwrap_or(1024 * 1024);
        if depth == 0
            || depth >= i32::MAX as usize
            || block_size == 0
            || depth
                .checked_mul(3)
                .and_then(|n| n.checked_mul(block_size))
                .is_none_or(|n| n > isize::MAX as usize)
        {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "invalid USRBIO queue depth or block size",
            ));
        }
        let root = self
            .config
            .root
            .as_deref()
            .filter(|p| Path::new(p).is_absolute())
            .ok_or_else(|| {
                Error::new(ErrorKind::ConfigInvalid, "an absolute 3FS root is required")
            })?;
        let root = Path::new(root).canonicalize().map_err(new_std_io_error)?;
        if !root.is_dir() {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                "3FS root must be a directory",
            ));
        }
        let api = Api::load(Path::new(
            self.config
                .library_path
                .as_deref()
                .unwrap_or("libhf3fs_api_shared.so"),
        ))?;
        let c_root = CString::new(root.as_os_str().as_bytes())
            .map_err(|e| Error::new(ErrorKind::ConfigInvalid, "root contains NUL").set_source(e))?;
        let mount = api.mount_point(&c_root)?;
        let read = IoDriver::start(api.clone(), mount.clone(), depth, block_size, true)?;
        let write = IoDriver::start(api.clone(), mount, depth, block_size, false)?;
        let metadata = Arc::new(
            opendal_service_fs::Fs::default()
                .root(&root.to_string_lossy())
                .build()?,
        );
        Ok(Hf3fsBackend {
            core: Arc::new(Hf3fsCore {
                root,
                api,
                read,
                write,
            }),
            metadata,
        })
    }
}

pub(crate) struct Hf3fsCore {
    pub root: PathBuf,
    pub api: Arc<Api>,
    pub read: IoDriver,
    pub write: IoDriver,
}

impl Hf3fsCore {
    pub fn path(&self, path: &str) -> Result<PathBuf> {
        let path = Path::new(path.trim_end_matches('/'));
        if path.components().any(|c| {
            matches!(
                c,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        }) {
            return Err(Error::new(
                ErrorKind::NotFound,
                "path escapes the configured root",
            ));
        }
        Ok(self.root.join(path))
    }
}

struct Hf3fsBackend {
    core: Arc<Hf3fsCore>,
    metadata: Servicer,
}

impl std::fmt::Debug for Hf3fsBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hf3fsBackend")
            .field("root", &self.core.root)
            .finish_non_exhaustive()
    }
}

impl Service for Hf3fsBackend {
    type Reader = oio::PositionReader<Hf3fsReader>;
    type Writer = Hf3fsWriter;
    type Lister = oio::Lister;
    type Deleter = oio::Deleter;
    type Copier = ();
    type Composer = ();

    fn info(&self) -> ServiceInfo {
        ServiceInfo::new(HF3FS_SCHEME, self.core.root.to_string_lossy(), "")
    }

    fn capability(&self) -> Capability {
        Capability {
            stat: true,
            read: true,
            write: true,
            write_can_empty: true,
            write_can_multi: true,
            write_with_if_not_exists: true,
            create_dir: true,
            list: true,
            delete: true,
            delete_with_recursive: true,
            rename: true,
            shared: true,
            ..Default::default()
        }
    }

    fn read(&self, _: &OperationContext, path: &str, _: OpRead) -> Result<Self::Reader> {
        Ok(oio::PositionReader::new(Hf3fsReader {
            core: self.core.clone(),
            path: self.core.path(path)?,
        })
        .with_max_buf_size(self.core.read.block_size))
    }

    fn write(&self, ctx: &OperationContext, path: &str, args: OpWrite) -> Result<Self::Writer> {
        Ok(Hf3fsWriter::new(
            self.core.clone(),
            self.core.path(path)?,
            ctx.executor().clone(),
            args,
        ))
    }

    async fn stat(&self, ctx: &OperationContext, path: &str, args: OpStat) -> Result<RpStat> {
        self.metadata.stat(ctx, path, args).await
    }

    async fn create_dir(
        &self,
        ctx: &OperationContext,
        path: &str,
        args: OpCreateDir,
    ) -> Result<RpCreateDir> {
        self.metadata.create_dir(ctx, path, args).await
    }

    fn list(&self, ctx: &OperationContext, path: &str, args: OpList) -> Result<Self::Lister> {
        self.metadata.list(ctx, path, args)
    }

    fn delete(&self, ctx: &OperationContext) -> Result<Self::Deleter> {
        self.metadata.delete(ctx)
    }

    fn copy(&self, _: &OperationContext, _: &str, _: &str, _: OpCopy) -> Result<Self::Copier> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "3FS native copy is unsupported",
        ))
    }

    async fn presign(&self, _: &OperationContext, _: &str, _: OpPresign) -> Result<RpPresign> {
        Err(Error::new(
            ErrorKind::Unsupported,
            "3FS presign is unsupported",
        ))
    }

    async fn rename(
        &self,
        ctx: &OperationContext,
        from: &str,
        to: &str,
        args: OpRename,
    ) -> Result<RpRename> {
        self.metadata.rename(ctx, from, to, args).await
    }
}
