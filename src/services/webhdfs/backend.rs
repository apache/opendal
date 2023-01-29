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
use core::fmt::Debug;

use log::debug;

use crate::{
    raw::{normalize_root, Accessor, HttpClient},
    Error, ErrorKind, Result, Scheme,
};

use super::signer::Signer;

/// Builder for WebHDFS service
/// # Note
/// Builder prefers delegation token to username authentication
#[derive(Default, Clone)]
pub struct Builder {
    root: Option<String>,
    endpoint: Option<String>,
    delegation: Option<String>,
    username: Option<String>,
    // proxy user
    doas: Option<String>,
}

impl Debug for Builder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut ds = f.debug_struct("Builder");
        ds.field("root", &self.root)
            .field("endpoint", &self.endpoint);
        if self.delegation.is_some() {
            ds.field("delegation", &"<redacted>");
        }
        if self.username.is_some() {
            ds.field("username", &"<redacted>");
        }
        if self.doas.is_some() {
            ds.field("do_as", &"<redacted>");
        }
        ds.finish()
    }
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Self::default();

        for (k, v) in it {
            let v = v.as_str();
            match &k {
                "root" => builder.root(v),
                "endpoint" => builder.endpoint(v),
                "delegation" => builder.delegation(v),
                "username" => builder.username(v),
                "do_as" => builder.do_as(v),
                _ => continue,
            }
        }

        builder
    }

    /// Set the working directory of this backend
    ///
    /// All operations will happen under this root
    pub fn root(mut self, root: &str) -> Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }

    /// Set the remote address of this backend
    ///
    /// Endpoints should be full uri, e.g.
    ///
    /// - `https://webhdfs.example.com:9870`
    /// - `http://192.168.66.88:9870`
    ///
    /// If user inputs endpoint without scheme, we will
    /// prepend `http://` to it.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };
        self
    }

    /// Set the delegation token of this backend,
    /// used for authentication
    /// # Note
    /// The builder prefers using delegation token over username.
    /// If both are set, delegation token will be used.
    pub fn delegation(mut self, delegation: &str) -> Self {
        self.delegation = if delegation.is_empty() {
            None
        } else {
            Some(delegation.to_string())
        };
        self
    }

    /// Set the username of this backend,
    /// used for authentication
    /// # Note
    /// The builder prefers using delegation token over username.
    /// If both are set, delegation token will be used. And username
    /// will be ignored.
    pub fn username(mut self, username: &str) -> Self {
        self.username = if username.is_empty() {
            None
        } else {
            Some(username.to_string())
        };
        self
    }

    /// Set the proxy user of this backend
    /// # Note
    /// The builder prefers using delegation token,
    /// If both are set, delegation token will be used. And do_as
    /// will be ignored
    pub fn doas(mut self, doas: &str) -> Self {
        self.doas = if doas.is_empty() {
            None
        } else {
            Some(doas.to_string())
        };
        self
    }

    /// build the backend
    pub fn build(&mut self) -> Result<impl Accessor> {
        debug!("building backend: {:?}", self);

        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let endpoint = match self.endpoint.take() {
            Some(e) => e,
            None => {
                return Err(
                    Error::new(ErrorKind::BackendConfigInvalid, "endpoint is empty")
                        .with_context("service", Scheme::WebHdfs),
                )
            }
        };
        let signer = {
            if let Some(token) = self.delegation.take() {
                Signer::new_delegation(&token)
            } else if let Some(username) = self.username.take() {
                let doas = if let Some(doas) = self.doas.take() {
                    &doas
                } else {
                    ""
                };
                Signer::new_user(&username, doas)
            } else {
                return Err(Error::new(
                    ErrorKind::BackendConfigInvalid,
                    "neither delegation token nor username is set",
                )
                .with_context("service", Scheme::WebHdfs));
            }
        };
        let client = HttpClient::new()?;
        Ok(Backend {
            root,
            endpoint,
            signer,
            client,
        })
    }
}

/// Backend for WebHDFS service
#[derive(Debug, Clone)]
pub struct Backend {
    root: String,
    endpoint: String,
    client: HttpClient,
    signer: Signer,
}
