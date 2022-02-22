use std::fmt::Debug;
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

use crate::error::Result;
use crate::ops::OpDelete;
use crate::ops::OpStat;
use crate::Accessor;
use crate::Reader;
use crate::Writer;

#[derive(Clone)]
pub struct Object {
    acc: Arc<dyn Accessor>,
    path: String,
}

impl Object {
    pub fn new(acc: Arc<dyn Accessor>, path: &str) -> Self {
        Self {
            acc,
            path: path.to_string(),
        }
    }

    pub fn accessor(&self) -> Arc<dyn Accessor> {
        self.acc.clone()
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    pub fn reader(&self) -> Reader {
        Reader::new(self.acc.clone(), self.path.as_str())
    }

    pub fn writer(&self) -> Writer {
        Writer::new(self.acc.clone(), self.path.as_str())
    }

    pub async fn metadata(&self) -> Result<Metadata> {
        let op = &OpStat::new(self.path());

        self.acc.stat(op).await
    }

    pub async fn delete(&self) -> Result<()> {
        let op = &OpDelete::new(&self.path);

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
