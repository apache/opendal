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

use crate::raw::BytesRange;
use crate::services::opfs::core::OpfsCore;
use crate::{raw::oio, Buffer, Result};

pub struct OpfsReader {
    range: BytesRange,
    core: OpfsCore,
    file_name: String,
}

impl OpfsReader {
    pub(crate) fn new(range: BytesRange, file_name: String) -> Self {
        Self {
            range,
            core: OpfsCore::default(),
            file_name,
        }
    }
}

impl oio::Read for OpfsReader {
    async fn read(&mut self) -> Result<Buffer> {
        let core = self.core.clone();
        let buf = core.read_file_send(self.file_name.clone()).await?;

        let start = self.range.offset() as usize;
        let end = self.range.size().unwrap_or(0) as usize + start;

        Ok(Buffer::from(buf[start..end].to_vec()))
    }
}
