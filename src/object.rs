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

use crate::accessor::Features;
use crate::error::Result;
use crate::ops::{OpDelete, OpRead, OpStat, OpStatefulRead, OpWrite};
use crate::readers::SeekableReader;
use crate::Accessor;
use crate::{Reader, StatefulReader};

#[derive(Clone)]
pub struct Object {
    acc: Arc<dyn Accessor>,
    path: String,

    meta: Option<Metadata>,
}

impl Object {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: path.to_string(),
            meta: None,
        }
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub async fn metadata(&mut self) -> Result<&Metadata> {
        if self.meta.is_none() {
            let op = &OpStat {
                path: self.path.clone(),
            };

            let meta = self.acc.stat(op).await?;
            self.meta = Some(meta);
        }

        Ok(self.meta.as_ref().expect("unreachable code"))
    }

    pub async fn read(&self) -> Result<Reader> {
        let op = &OpRead {
            path: self.path.clone(),
            ..Default::default()
        };

        self.acc.read(op).await
    }

    pub async fn ranged_read(&self, offset: u64, size: u64) -> Result<Reader> {
        let op = &OpRead {
            path: self.path.clone(),
            offset: Some(offset),
            size: Some(size),
        };

        self.acc.read(op).await
    }

    pub async fn stateful_read(&self) -> Result<StatefulReader> {
        if self.acc.features().contains(Features::STATEFUL_READ) {
            let op = &OpStatefulRead {
                path: self.path.clone(),
            };

            self.acc.stateful_read(op).await
        } else {
            Ok(StatefulReader::new(Box::new(
                SeekableReader::try_new(self).await?,
            )))
        }
    }

    pub async fn write(&self, r: Reader, size: u64) -> Result<usize> {
        let op = &OpWrite {
            path: self.path.clone(),
            size,
        };

        self.acc.write(r, op).await
    }

    pub async fn write_bytes(&self, bs: Vec<u8>) -> Result<usize> {
        let op = &OpWrite {
            path: self.path.clone(),
            size: bs.len() as u64,
        };

        self.acc
            .write(Reader::new(Box::new(Cursor::new(bs))), op)
            .await
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
    pub(crate) fn set_content_length(&mut self, content_length: u64) -> &mut Self {
        self.content_length = content_length;
        self
    }
}
