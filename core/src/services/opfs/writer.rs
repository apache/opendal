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

use crate::services::opfs::core::OpfsCore;
use crate::{raw::oio, Buffer, Metadata, Result};
use bytes::Buf;
use std::io::Read;

pub struct OpfsWriter {
    append: bool,
    file_name: String,
    core: OpfsCore,
}

impl OpfsWriter {
    pub fn new(file_name: String, append: bool) -> Self {
        Self {
            append,
            file_name,
            core: OpfsCore::default(),
        }
    }
}

impl oio::Write for OpfsWriter {
    async fn write(&mut self, bs: Buffer) -> Result<()> {
        let core = self.core.clone();
        let mut buffer = match self.append {
            true => {
                let core = self.core.clone();
                core.read_file_send(self.file_name.clone()).await?
            }
            false => Vec::new(),
        };

        bs.reader().read_to_end(&mut buffer).unwrap();

        core.store_file_send(self.file_name.clone(), buffer).await?;

        Ok(())
    }

    async fn close(&mut self) -> Result<Metadata> {
        Ok(Metadata::default())
    }

    async fn abort(&mut self) -> Result<()> {
        todo!()
    }
}
