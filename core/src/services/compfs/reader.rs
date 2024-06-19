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
    file: compio::fs::File,
    range: BytesRange,
}

impl CompfsReader {
    pub fn new(core: Arc<CompfsCore>, file: compio::fs::File, range: BytesRange) -> Self {
        Self { core, file, range }
    }
}

impl oio::Read for CompfsReader {
    async fn read(&mut self) -> Result<Buffer> {
        let mut bs = self.core.buf_pool.get();

        let pos = self.range.offset();
        let len = self.range.size().expect("range size is always Some");
        bs.reserve(len as _);
        let f = self.file.clone();
        let mut bs = self
            .core
            .exec(move || async move {
                let (_, bs) = buf_try!(@try f.read_at(bs, pos).await);
                Ok(bs)
            })
            .await?;
        let frozen = bs.split().freeze();
        self.core.buf_pool.put(bs);
        Ok(Buffer::from(frozen))
    }
}
