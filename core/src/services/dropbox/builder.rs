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

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use super::backend::DropboxBackend;
use super::core::DropboxCore;
use crate::raw::*;
use crate::*;

/// [Dropbox](https://www.dropbox.com/) backend support.
///
/// # Capabilities
///
/// This service can be used to:
///
/// - [x] read
/// - [x] write
/// - [x] delete
/// - [ ] copy
/// - [ ] create
/// - [ ] list
/// - [ ] rename
///
/// # Notes
///
///
/// # Configuration
///
/// - `access_token`: set the access_token for google drive api
/// - `root`: Set the work directory for backend
///
/// You can refer to [`DropboxBuilder`]'s docs for more information
///
/// # Example
///
/// ## Via Builder
///
/// ```
/// use anyhow::Result;
/// use opendal::raw::OpWrite;
/// use opendal::services::Dropbox;
/// use opendal::Operator;
///
/// #[tokio::main]
/// async fn main() -> Result<()> {
///     // create backend builder
///     let mut builder = Dropbox::default();
///     builder.access_token("<token>");
///
///     let op: Operator = Operator::new(builder)?.finish();
///     Ok(())
/// }
/// ```

#[derive(Default)]
pub struct DropboxBuilder {
    access_token: Option<String>,
    root: Option<String>,
    http_client: Option<HttpClient>,
}

impl Debug for DropboxBuilder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Builder").finish()
    }
}

impl DropboxBuilder {
    /// default: no access token, which leads to failure
    pub fn access_token(&mut self, access_token: &str) -> &mut Self {
        self.access_token = Some(access_token.to_string());
        self
    }

    /// default: no root path, which leads to failure
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = Some(root.to_string());
        self
    }

    /// Specify the http client that used by this service.
    ///
    /// # Notes
    ///
    /// This API is part of OpenDAL's Raw API. `HttpClient` could be changed
    /// during minor updates.
    pub fn http_client(&mut self, http_client: HttpClient) -> &mut Self {
        self.http_client = Some(http_client);
        self
    }
}

impl Builder for DropboxBuilder {
    const SCHEME: Scheme = Scheme::Dropbox;
    type Accessor = DropboxBackend;

    fn from_map(map: HashMap<String, String>) -> Self {
        let mut builder = Self::default();
        map.get("access_token").map(|v| builder.access_token(v));
        builder
    }

    fn build(&mut self) -> Result<Self::Accessor> {
        let root = normalize_root(&self.root.take().unwrap_or_default());
        let client = if let Some(client) = self.http_client.take() {
            client
        } else {
            HttpClient::new().map_err(|err| {
                err.with_operation("Builder::build")
                    .with_context("service", Scheme::Dropbox)
            })?
        };
        let token = match self.access_token.clone() {
            Some(access_token) => access_token,
            None => {
                return Err(Error::new(
                    ErrorKind::ConfigInvalid,
                    "access_token is required",
                ))
            }
        };

        Ok(DropboxBackend {
            core: Arc::new(DropboxCore {
                root,
                token,
                client,
            }),
        })
    }
}
