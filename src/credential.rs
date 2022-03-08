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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;

#[derive(Clone)]
pub enum Credential {
    /// Plain refers to no credential has been provided, fallback to services'
    /// default logic.
    Plain,
    /// Basic refers to HTTP Basic Authentication.
    Basic { username: String, password: String },
    /// HMAC, also known as Access Key/Secret Key authentication.
    ///
    /// ## NOTE
    ///
    /// HMAC is just a common step of ak/sk authentication. And it's not the correct name for
    /// this type of authentication. But it's widely used and no ambiguities with other types.
    /// So we use it here to avoid using AkSk as a credential type.
    HMAC {
        access_key_id: String,
        secret_access_key: String,
    },
    /// Token refers to static API token.
    Token(String),
}

// Credential has sensitive data, we should not print it out in anyway.
impl Debug for Credential {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Credential::Plain => write!(f, "Credential::Plain"),
            Credential::Basic { .. } => write!(f, "Credential::Basic"),
            Credential::HMAC { .. } => write!(f, "Credential::HMAC"),
            Credential::Token(_) => write!(f, "Credential::Token"),
        }
    }
}

// Credential has sensitive data, we should not print it out in anyway.
impl Display for Credential {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Credential::Plain => write!(f, "plain"),
            Credential::Basic { .. } => write!(f, "basic"),
            Credential::HMAC { .. } => write!(f, "hmac"),
            Credential::Token(_) => write!(f, "token"),
        }
    }
}

impl Credential {
    pub fn basic(username: &str, password: &str) -> Credential {
        if username.is_empty() && password.is_empty() {
            return Credential::Plain;
        }

        Credential::Basic {
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    pub fn hmac(access_key_id: &str, secret_access_key: &str) -> Credential {
        if access_key_id.is_empty() && secret_access_key.is_empty() {
            return Credential::Plain;
        }

        Credential::HMAC {
            access_key_id: access_key_id.to_string(),
            secret_access_key: secret_access_key.to_string(),
        }
    }

    pub fn token(token: &str) -> Credential {
        if token.is_empty() {
            return Credential::Plain;
        }

        Credential::Token(token.to_string())
    }
}
