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

use std::sync::Arc;

use compio::{buf::buf_try, io::AsyncReadAt};

use super::core::CompfsCore;
use crate::raw::*;
use crate::*;

#[derive(Debug)]
pub struct CompfsReader {
    core: Arc<CompfsCore>,
    f: compio::fs::File,
}

impl CompfsReader {
    pub fn new(core: Arc<CompfsCore>, f: compio::fs::File) -> Self {
        Self { core, f }
    }
}

impl oio::Read for CompfsReader {
    async fn read_at(&self, offset: u64, limit: usize) -> Result<Buffer> {
        let mut bs = self.core.buf_pool.get();
        bs.reserve(limit);
        let f = self.f.clone();
        let mut bs = self
            .core
            .exec(move || async move {
                let (_, bs) = buf_try!(@try f.read_at(bs, offset).await);

                Ok(bs)
            })
            .await?;
        let frozen = bs.split().freeze();
        self.core.buf_pool.put(bs);
        Ok(Buffer::from(frozen))
    }
}
