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
use futures::AsyncReadExt;

use super::Adapter;
use crate::ops::*;
use crate::raw::*;
use crate::*;

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
impl<S: Adapter> Accessor for Backend<S> {
    type Reader = output::Cursor;
    type BlockingReader = output::Cursor;

    fn metadata(&self) -> AccessorMetadata {
        let mut am: AccessorMetadata = self.kv.metadata().into();
        am.set_root(&self.root)
            .set_hints(AccessorHint::ReadIsStreamable | AccessorHint::ReadIsSeekable);

        am
    }

    async fn create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        if args.mode() == ObjectMode::FILE {
            self.kv.set(path, &[]).await?;
        }

        Ok(RpCreate::default())
    }

    fn blocking_create(&self, path: &str, args: OpCreate) -> Result<RpCreate> {
        if args.mode() == ObjectMode::FILE {
            self.kv.blocking_set(path, &[])?;
        }

        Ok(RpCreate::default())
    }

    async fn read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::Reader)> {
        let path = path.to_string();

        let bs = match self.kv.get(&path).await? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(
                    ErrorKind::ObjectNotFound,
                    "kv doesn't have this path",
                ))
            }
        };

        let bs = self.apply_range(bs, args.range());

        let length = bs.len();
        Ok((RpRead::new(length as u64), output::Cursor::from(bs)))
    }

    fn blocking_read(&self, path: &str, args: OpRead) -> Result<(RpRead, Self::BlockingReader)> {
        let bs = match self.kv.blocking_get(path)? {
            Some(bs) => bs,
            None => {
                return Err(Error::new(
                    ErrorKind::ObjectNotFound,
                    "kv doesn't have this path",
                ))
            }
        };

        let bs = self.apply_range(bs, args.range());
        Ok((RpRead::new(bs.len() as u64), output::Cursor::from(bs)))
    }

    async fn write(&self, path: &str, args: OpWrite, mut r: input::Reader) -> Result<RpWrite> {
        let mut bs = Vec::with_capacity(args.size() as usize);
        r.read_to_end(&mut bs)
            .await
            .map_err(|err| Error::new(ErrorKind::Unexpected, "read from source").set_source(err))?;

        self.kv.set(path, &bs).await?;

        Ok(RpWrite::new(args.size()))
    }

    fn blocking_write(
        &self,
        path: &str,
        args: OpWrite,
        mut r: input::BlockingReader,
    ) -> Result<RpWrite> {
        let mut bs = Vec::with_capacity(args.size() as usize);
        r.read_to_end(&mut bs)
            .map_err(|err| Error::new(ErrorKind::Unexpected, "read from source").set_source(err))?;

        self.kv.blocking_set(path, &bs)?;

        Ok(RpWrite::new(args.size()))
    }

    async fn stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        if path.ends_with('/') {
            Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)))
        } else {
            let bs = self.kv.get(path).await?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    ObjectMetadata::new(ObjectMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(
                    ErrorKind::ObjectNotFound,
                    "kv doesn't have this path",
                )),
            }
        }
    }

    fn blocking_stat(&self, path: &str, _: OpStat) -> Result<RpStat> {
        if path.ends_with('/') {
            Ok(RpStat::new(ObjectMetadata::new(ObjectMode::DIR)))
        } else {
            let bs = self.kv.blocking_get(path)?;
            match bs {
                Some(bs) => Ok(RpStat::new(
                    ObjectMetadata::new(ObjectMode::FILE).with_content_length(bs.len() as u64),
                )),
                None => Err(Error::new(
                    ErrorKind::ObjectNotFound,
                    "kv doesn't have this path",
                )),
            }
        }
    }

    async fn delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        self.kv.delete(path).await?;
        Ok(RpDelete::default())
    }

    fn blocking_delete(&self, path: &str, _: OpDelete) -> Result<RpDelete> {
        self.kv.blocking_delete(path)?;
        Ok(RpDelete::default())
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
