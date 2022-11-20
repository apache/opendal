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

use async_trait::async_trait;
use futures::io::Cursor;
use futures::AsyncReadExt;

use super::Adapter;
use crate::ops::BytesRange;
use crate::ops::OpCreate;
use crate::ops::OpDelete;
use crate::ops::OpRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::ops::Operation;
use crate::path::normalize_root;
use crate::Accessor;
use crate::AccessorMetadata;
use crate::BlockingBytesReader;
use crate::BytesReader;
use crate::Error;
use crate::ErrorKind;
use crate::ObjectMetadata;
use crate::ObjectMode;
use crate::ObjectReader;
use crate::Result;

/// Backend of kv service.
#[derive(Debug, Clone)]
pub struct Backend<S: Adapter> {
    kv: S,
    root: String,
}

impl<S> Backend<S>
where
    S: Adapter,
{
    /// Create a new kv backend.
    pub fn new(kv: S) -> Self {
        Self {
            kv,
            root: "/".to_string(),
        }
    }

    /// Configure root within this backend.
    pub fn with_root(mut self, root: &str) -> Self {
        self.root = normalize_root(root);
        self
    }
}

#[async_trait]
impl<S> Accessor for Backend<S>
where
    S: Adapter,
{
    fn metadata(&self) -> AccessorMetadata {
        let mut am: AccessorMetadata = self.kv.metadata().into();
        am.set_root(&self.root);

        am
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<()> {
        match args.mode() {
            ObjectMode::FILE => self.kv.set(path, &[]).await,
            _ => Ok(()),
        }
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<()> {
        match args.mode() {
            ObjectMode::FILE => self.kv.blocking_set(path, &[]),
            _ => Ok(()),
        }
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<ObjectReader> {
        let bs = match self.kv.get(path).await? {
            Some(bs) => bs,
            None => {
                return Err(
                    Error::new(ErrorKind::ObjectNotFound, "kv doesn't have this path")
                        .with_operation(Operation::Read.into_static())
                        .with_context("service", self.metadata().scheme().into_static())
                        .with_context("path", path),
                )
            }
        };

        let bs = self.apply_range(bs, args.range());

        let length = bs.len();
        Ok(ObjectReader::new(Box::new(Cursor::new(bs)))
            .with_meta(ObjectMetadata::new(ObjectMode::FILE).with_content_length(length as u64)))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<BlockingBytesReader> {
        let bs = match self.kv.blocking_get(path)? {
            Some(bs) => bs,
            None => {
                return Err(
                    Error::new(ErrorKind::ObjectNotFound, "kv doesn't have this path")
                        .with_operation(Operation::BlockingRead.into_static())
                        .with_context("service", self.metadata().scheme().into_static())
                        .with_context("path", path),
                )
            }
        };

        let bs = self.apply_range(bs, args.range());
        Ok(Box::new(std::io::Cursor::new(bs)))
    }

    async fn write(&self, path: &str, args: OpWrite, mut r: BytesReader) -> Result<u64> {
        let mut bs = Vec::with_capacity(args.size() as usize);
        r.read_to_end(&mut bs).await.map_err(|err| {
            Error::new(ErrorKind::Unexpected, "read from source")
                .with_operation(Operation::Write.into_static())
                .with_context("service", self.metadata().scheme().into_static())
                .with_context("path", path)
                .with_source(err)
        })?;

        self.kv.set(path, &bs).await?;

        Ok(args.size())
    }

    fn blocking_write(&self, path: &str, args: OpWrite, mut r: BlockingBytesReader) -> Result<u64> {
        let mut bs = Vec::with_capacity(args.size() as usize);
        r.read_to_end(&mut bs).map_err(|err| {
            Error::new(ErrorKind::Unexpected, "read from source")
                .with_operation(Operation::BlockingWrite.into_static())
                .with_context("service", self.metadata().scheme().into_static())
                .with_context("path", path)
                .with_source(err)
        })?;

        self.kv.blocking_set(path, &bs)?;

        Ok(args.size())
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        if path.ends_with('/') {
            Ok(ObjectMetadata::new(ObjectMode::DIR))
        } else {
            let bs = self.kv.get(path).await?;
            match bs {
                Some(bs) => {
                    Ok(ObjectMetadata::new(ObjectMode::FILE).with_content_length(bs.len() as u64))
                }
                None => Err(
                    Error::new(ErrorKind::ObjectNotFound, "kv doesn't have this path")
                        .with_operation(Operation::Stat.into_static())
                        .with_context("service", self.metadata().scheme().into_static())
                        .with_context("path", path),
                ),
            }
        }
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<ObjectMetadata> {
        if path.ends_with('/') {
            Ok(ObjectMetadata::new(ObjectMode::DIR))
        } else {
            let bs = self.kv.blocking_get(path)?;
            match bs {
                Some(bs) => {
                    Ok(ObjectMetadata::new(ObjectMode::FILE).with_content_length(bs.len() as u64))
                }
                None => Err(
                    Error::new(ErrorKind::ObjectNotFound, "kv doesn't have this path")
                        .with_operation(Operation::BlockingStat.into_static())
                        .with_context("service", self.metadata().scheme().into_static())
                        .with_context("path", path),
                ),
            }
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<()> {
        self.kv.delete(path).await?;
        Ok(())
    }

    fn blocking_delete(&self, path: &str, _: OpDelete) -> Result<()> {
        self.kv.blocking_delete(path)?;
        Ok(())
    }
}

impl<S> Backend<S>
where
    S: Adapter,
{
    fn apply_range(&self, mut bs: Vec<u8>, br: BytesRange) -> Vec<u8> {
        match (br.offset(), br.size()) {
            (Some(offset), Some(size)) => {
                let mut bs = bs.split_off(offset as usize);
                if (size as usize) < bs.len() {
                    let _ = bs.split_off(size as usize);
                }
                bs
            }
            (Some(offset), None) => bs.split_off(offset as usize),
            (None, Some(size)) => bs.split_off(bs.len() - size as usize),
            (None, None) => bs,
        }
    }
}
