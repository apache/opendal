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

use bytes::BufMut;

use super::core::*;
use crate::raw::*;
use crate::*;

pub struct B2Reader {
    core: Arc<B2Core>,

    path: String,
    op: OpRead,
}

impl B2Reader {
    pub fn new(core: Arc<B2Core>, path: &str, op: OpRead) -> Self {
        B2Reader {
            core,
            path: path.to_string(),
            op,
        }
    }
}

impl oio::Read for B2Reader {
    async fn read_at(&self, buf: &mut oio::WritableBuf, offset: u64) -> Result<usize> {
        let range = BytesRange::new(offset, Some(buf.remaining_mut() as u64));

        self.core
            .download_file_by_name(&self.path, range, &self.op, buf)
            .await
    }
}
