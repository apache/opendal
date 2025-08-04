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

use log::debug;

use super::backend::IpmfsBackend;
use super::core::IpmfsCore;
use super::DEFAULT_SCHEME;
use crate::raw::*;
use crate::services::IpmfsConfig;
use crate::*;
impl Configurator for IpmfsConfig {
    type Builder = IpmfsBuilder;

    #[allow(deprecated)]
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

    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
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
    #[deprecated(since = "0.53.0", note = "Use `Operator::update_http_client` instead")]
    #[allow(deprecated)]
    pub fn http_client(mut self, client: HttpClient) -> Self {
        self.http_client = Some(client);
        self
    }
}

impl Builder for IpmfsBuilder {
    type Config = IpmfsConfig;

    fn build(self) -> Result<impl Access> {
        let root = normalize_root(&self.config.root.unwrap_or_default());
        debug!("backend use root {root}");

        let endpoint = self
            .config
            .endpoint
            .clone()
            .unwrap_or_else(|| "http://localhost:5001".to_string());

        let info = AccessorInfo::default();
        info.set_scheme(DEFAULT_SCHEME)
            .set_root(&root)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,
                delete: true,

                list: true,

                shared: true,

                ..Default::default()
            });

        let accessor_info = Arc::new(info);
        let core = Arc::new(IpmfsCore {
            info: accessor_info,
            root: root.to_string(),
            endpoint: endpoint.to_string(),
        });

        Ok(IpmfsBackend { core })
    }
}
