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

use crate::ops::{OpCreate, OpDelete, OpList, OpRead, OpStat, OpWrite};
use crate::{Accessor, BytesReader, BytesWriter, Metadata, ObjectMode, ObjectStreamer};
use async_trait::async_trait;
use std::fmt::{Debug, Formatter};
use std::io::Result;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Builder {}

#[derive(Debug, Clone)]
pub struct Backend {
    client: Arc<hdrs::Client>,
}

unsafe impl Send for Backend {}
unsafe impl Sync for Backend {}

impl Backend {}

#[async_trait]
impl Accessor for Backend {
    async fn create(&self, args: &OpCreate) -> Result<()> {
        match args.mode() {
            ObjectMode::FILE => {
                let _ = self
                    .client
                    .open(args.path(), libc::O_CREAT | libc::O_WRONLY)?;
                Ok(())
            }
            ObjectMode::DIR => {
                let _ = self.client.mkdir(args.path())?;
                Ok(())
            }
            ObjectMode::Unknown => unreachable!(),
        }
    }
    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        todo!()
    }
    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        todo!()
    }
    async fn stat(&self, args: &OpStat) -> Result<Metadata> {
        todo!()
    }
    async fn delete(&self, args: &OpDelete) -> Result<()> {
        todo!()
    }
    async fn list(&self, args: &OpList) -> Result<ObjectStreamer> {
        todo!()
    }
}
