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

use log::debug;

use super::backend::IpmfsBackend;
use crate::raw::*;
use crate::services::IpmfsConfig;
use crate::*;

impl Configurator for IpmfsConfig {
    type Builder = IpmfsBuilder;
    fn into_builder(self) -> Self::Builder {
        IpmfsBuilder {
            config: self,
            http_client: None,
        }
    }
}

/// IPFS file system support based on [IPFS MFS](https://docs.ipfs.tech/concepts/file-systems/) API.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] list
/// - [ ] presign
/// - [ ] blocking
///
/// # Configuration
///
/// - `root`: Set the work directory for backend
/// - `endpoint`: Customizable endpoint setting
///
/// You can refer to [`IpmfsBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```no_run
/// use anyhow::Result;
/// use opendal::services::Ipmfs;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Ipmfs::default()
///         // set the storage bucket for OpenDAL
///         .endpoint("http://127.0.0.1:5001");
///
///     let op: Operator = Operator::new(builder)?.finish();
///
///     Ok(())
/// }
/// ```
#[derive(Default, Debug)]
pub struct IpmfsBuilder {
    config: IpmfsConfig,
    http_client: Option<HttpClient>,
}

impl IpmfsBuilder {
    /// Set root for ipfs.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set endpoint for ipfs.
    ///
    /// Default: http://localhost:5001
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.config.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for IpmfsBuilder {
    const SCHEME: Scheme = Scheme::Ipmfs;
    type Config = IpmfsConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {}", root);

        let endpoint = self
            .config
            .endpoint
            .clone()
            .unwrap_or_else(|| "http://localhost:5001".to_string());

        let client = if let Some(client) = self.http_client {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Ipmfs)
            })?
        };

        Ok(IpmfsBackend::new(root, client, endpoint))
    }
}
