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

use log::info;

use crate::error::other;
use crate::error::BackendError;
use crate::http_util::HttpClient;
use crate::services::ipfs::Backend;
use anyhow::anyhow;
use std::collections::HashMap;

#[derive(Default, Debug)]
pub struct Builder {
    root: Option<String>,
    endpoint: Option<String>,
}

impl Builder {
    /// Set root for backend.
    pub fn root(&mut self, root: &str) -> &mut Self {
        self.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };

        self
    }

    pub fn endpoint(&mut self, endpoint: &str) -> &mut Self {
        self.endpoint = if endpoint.is_empty() {
            None
        } else {
            Some(endpoint.to_string())
        };
        self
    }

    fn root_string(&self) -> Result<String> {
        match &self.root {
            None => Ok("/".to_string()),
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

                Ok(v)
            }
        }
    }

    fn endpoint_string(&self) -> String {
        self.endpoint
            .clone()
            .unwrap_or_else(|| "http://localhost:5001".to_string())
    }

    pub fn build(&mut self) -> Result<Backend> {
        let root = self.root_string()?;
        let client = HttpClient::new();
        let endpoint = self.endpoint_string();
        info!("backend build finished: {:?}", &self);
        Ok(Backend::new(root, client, endpoint))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_root_string() {
        let mut builder = Builder::default();
        builder.root("/foo/bar");
        assert_eq!(builder.root_string().unwrap(), "/foo/bar/".to_string());

        builder.root("foo");
        assert!(builder.root_string().is_err());
    }

    #[tokio::test]
    async fn test_finish() {
        let mut builder = Builder::default();
        builder.root("foo");

        assert!(builder.build().is_err());

        builder.root("/");
        assert!(builder.build().is_ok());
    }
}
