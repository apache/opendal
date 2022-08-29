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

use std::collections::HashMap;
use std::io::Result;

use anyhow::anyhow;
use log::info;

use crate::error::other;
use crate::error::BackendError;
use crate::http_util::HttpClient;
use crate::services::ipfs::Backend;

/// Builder for service ipfs.
#[derive(Default, Debug)]
pub struct Builder {
    root: Option<String>,
    endpoint: Option<String>,
}

impl Builder {
    /// Set root for ipfs.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    /// Set endpoint for ipfs.
    ///
    /// Default: http://localhost:5001
    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };
        self
    }

    /// Consume builder to build an ipfs::Backend.
    pub fn build(&mut self) -> Result<Backend> {
        let root = match &self.root {
            None => "/".to_string(),
            Some(v) => {
                debug_assert!(!v.is_empty());

                let mut v = v.clone();

                if !v.starts_with('/') {
                    return Err(other(BackendError::new(
                        HashMap::from([("root".to_string(), v.clone())]),
                        anyhow!("Root must start with /"),
                    )));
                }
                if !v.ends_with('/') {
                    v.push('/');
                }

                v
            }
        };

        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| "http://localhost:5001".to_string());

        let client = HttpClient::new();

        info!("backend build finished: {:?}", &self);
        Ok(Backend::new(root, client, endpoint))
    }
}
