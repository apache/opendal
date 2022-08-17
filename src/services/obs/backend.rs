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

use std::io::Result;
use std::{fmt::Debug, sync::Arc};

use async_trait::async_trait;
use reqsign::services::huaweicloud::obs::Signer;

use crate::{
    http_util::HttpClient,
    ops::{OpCreate, OpDelete, OpList, OpRead, OpStat, OpWrite},
    Accessor, AccessorMetadata, BytesReader, BytesWriter, DirStreamer, ObjectMetadata,
};

/// Builder for Huaweicloud OBS services
#[derive(Default, Clone)]
pub struct Builder {
    root: Option<String>,
    container: String,
    endpoint: Option<String>,
    access_key: Option<String>,
    secret_key: Option<String>,
}

impl Debug for Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder")
            .field("root", &self.root)
            .field("container", &self.container)
            .field("endpoint", &self.endpoint)
            .field("access_key", &"<redacted>")
            .field("secret_key", &"<redacted>")
            .finish()
    }
}

impl Builder {}

// Backend for Huaweicloud OBS services.
#[derive(Debug, Clone)]
pub struct Backend {
    container: String,
    client: HttpClient,
    root: String,
    endpoint: String,
    signer: Arc<Signer>,
}

impl Backend {}

#[async_trait]
impl Accessor for Backend {
    fn metadata(&self) -> AccessorMetadata {
        todo!()
    }

    async fn create(&self, args: &OpCreate) -> Result<()> {
        todo!()
    }

    async fn read(&self, args: &OpRead) -> Result<BytesReader> {
        todo!()
    }

    async fn write(&self, args: &OpWrite) -> Result<BytesWriter> {
        todo!()
    }

    async fn stat(&self, args: &OpStat) -> Result<ObjectMetadata> {
        todo!()
    }

    async fn delete(&self, args: &OpDelete) -> Result<()> {
        todo!()
    }

    async fn list(&self, args: &OpList) -> Result<DirStreamer> {
        todo!()
    }
}
