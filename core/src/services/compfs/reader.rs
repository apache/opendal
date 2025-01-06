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

use compio::buf::buf_try;
use compio::io::AsyncReadAt;

use super::core::CompfsCore;
use crate::raw::*;
use crate::*;

#[derive(Debug)]
pub struct CompfsReader {
    core: Arc<CompfsCore>,
    file: compio::fs::File,
    offset: u64,
    end: Option<u64>,
}

impl CompfsReader {
    pub(super) fn new(core: Arc<CompfsCore>, file: compio::fs::File, range: BytesRange) -> Self {
        Self {
            core,
            file,
            offset: range.offset(),
            end: range.size().map(|v| v + range.offset()),
        }
    }
}

impl oio::Read for CompfsReader {
    async fn read(&mut self) -> Result<Buffer> {
        let pos = self.offset;
        if let Some(end) = self.end {
            if end >= pos {
                return Ok(Buffer::new());
            }
        }

        let mut bs = self.core.buf_pool.get();
        // reserve 64KB buffer by default, we should allow user to configure this or make it adaptive.
        bs.reserve(64 * 1024);
        let f = self.file.clone();
        let (n, mut bs) = self
            .core
            .exec(move || async move {
                let (n, bs) = buf_try!(@try f.read_at(bs, pos).await);
                Ok((n, bs))
            })
            .await?;
        let frozen = bs.split_to(n).freeze();
        self.offset += frozen.len() as u64;
        self.core.buf_pool.put(bs);
        Ok(Buffer::from(frozen))
    }
}
