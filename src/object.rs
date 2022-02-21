use std::fmt::{Debug, Display, Formatter};
// Copyright 2021 Datafuse Labs.
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
use std::sync::Arc;

use futures::io::Cursor;

use crate::error::Result;
use crate::ops::OpDelete;
use crate::ops::OpRandomRead;
use crate::ops::OpSequentialRead;
use crate::ops::OpStat;
use crate::ops::OpWrite;
use crate::readers::SeekableReader;
use crate::BoxedAsyncRead;
use crate::RandomReader;
use crate::Writer;
use crate::{Accessor, SequentialReader};

#[derive(Clone)]
pub struct Object {
    acc: Arc<dyn Accessor>,
    path: String,

    complete: bool,
    meta: Option<Metadata>,
}

impl Object {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: path.to_string(),
            complete: false,
            meta: None,
        }
    }
    pub async fn open(acc: Arc<dyn Accessor>, path: &str) -> Result<Self> {
        let meta = acc
            .stat(&OpStat {
                path: path.to_string(),
            })
            .await?;

        Ok(Object {
            acc,
            path: path.to_string(),
            complete: true,
            meta: Some(meta),
        })
    }
    pub fn create(acc: Arc<dyn Accessor>, path: &str, meta: Metadata) -> Self {
        Self {
            acc,
            path: path.to_string(),
            complete: false,
            meta: Some(meta),
        }
    }

    pub fn accessor(&self) -> Arc<dyn Accessor> {
        self.acc.clone()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn metadata(&mut self) -> Option<&Metadata> {
        self.meta.as_ref()
    }

    pub fn sequential_read(&self) -> SequentialReader {
        SequentialReader::new(self.acc.clone(), self.path.as_str())
    }

    pub fn random_read(&self) -> RandomReader {
        RandomReader::new(self)
    }

    pub async fn stat(&mut self) -> Result<&Metadata> {
        let op = &OpStat {
            path: self.path.to_string(),
        };

        let meta = self.acc.stat(op).await?;
        self.meta = Some(meta);

        Ok(self.meta.as_ref().expect("unreachable code"))
    }

    pub async fn delete(&mut self) -> Result<()> {
        let op = &OpDelete {
            path: self.path.clone(),
        };

        self.acc.delete(op).await
    }
}

#[derive(Debug, Clone, Default)]
pub struct Metadata {
    content_length: u64,
}

impl Metadata {
    pub fn content_length(&self) -> u64 {
        self.content_length
    }
    pub fn set_content_length(&mut self, content_length: u64) -> &mut Self {
        self.content_length = content_length;
        self
    }
}

// TODO: maybe we can implement `AsyncWrite` for it so that we can use `io::copy`?
pub struct ObjectBuilder {
    acc: Arc<dyn Accessor>,
    path: String,
}

impl ObjectBuilder {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,

            path: path.to_string(),
        }
    }

    pub async fn write(self, r: BoxedAsyncRead, size: u64) -> Result<usize> {
        let op = OpWrite {
            path: self.path.to_string(),
            size,
        };

        self.acc.write(r, &op).await
    }

    pub async fn write_bytes(self, bs: Vec<u8>) -> Result<usize> {
        let op = OpWrite {
            path: self.path.to_string(),
            size: bs.len() as u64,
        };
        let r = Box::new(futures::io::Cursor::new(bs));
        self.acc.write(r, &op).await
    }
}
