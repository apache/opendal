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

//! This module contains some naive utilities
//! for WebHDFS authentication.

use http::Request;

use crate::{raw::AsyncBody, Error, ErrorKind, Result};

/// a naive WebHDFS signer implementation
/// # Note
/// WebHDFS supports using delegation token, or user and proxy user, for authentication.
/// So we use an enum to represent the signer.
#[derive(Clone, Debug)]
pub(super) enum Signer {
    /// Delegation token authentication
    Token(String),
    /// User name and proxy user authentication
    User(String, String),
}

impl Signer {
    /// create a new signer with delegation token
    pub fn new_delegation(delegation_token: &str) -> Self {
        Self::Token(delegation_token.to_string())
    }
    /// create a new signer with user name and proxy user
    ///
    /// # example
    ///
    /// Signer::new_user("user", "proxy_user") will make us login
    /// with identity of "user" but do as "proxy_user".
    ///
    /// Signer::new_user("user", "") will make us login with identity
    /// and on behave of nobody.
    pub fn new_user(username: &str, doas: &str) -> Self {
        Self::User(username.to_string(), doas.to_string())
    }

    /// sign a request
    pub fn sign(&self, req: &mut Request<AsyncBody>) -> Result<()> {
        match self {
            Self::Token(token) => {
                let mut hs = req.headers_mut();
                hs.insert(
                    "delegation",
                    token.parse().map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "signing request").set_source(err)
                    })?,
                );
            }
            Self::User(username, doas) => {
                let mut hs = req.headers_mut();
                hs.insert(
                    "user.name",
                    username.parse().map_err(|err| {
                        Error::new(ErrorKind::Unexpected, "signing request").set_source(err)
                    })?,
                );
                if !doas.is_empty() {
                    hs.insert(
                        "doas",
                        doas.parse().map_err(|err| {
                            Error::new(ErrorKind::Unexpected, "signing request").set_source(err)
                        })?,
                    );
                }
            }
        }
        Ok(())
    }
}
