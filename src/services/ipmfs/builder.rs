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

use log::debug;

use super::backend::Backend;
use crate::raw::*;
use crate::Result;
use crate::Scheme;

/// Builder for service ipfs.
#[derive(Default, Debug)]
pub struct Builder {
    root: Option<String>,
    endpoint: Option<String>,
}

impl Builder {
    pub(crate) fn from_iter(it: impl Iterator<Item = (String, String)>) -> Self {
        let mut builder = Builder::default();

        for (key, val) in it {
            let val = val.as_str();
            match key.as_ref() {
                "root" => builder.root(val),
                "endpoint" => builder.endpoint(val),
                _ => continue,
            };
        }

        builder
    }

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
    pub fn build(&mut self) -> Result<impl Accessor> {
        let root = normalize_root(&self.root.take().unwrap_or_default());
        debug!("backend use root {}", root);

        let endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| "http://localhost:5001".to_string());

        let client = HttpClient::new().map_err(|err| {
            err.with_operation("Builder::build")
                .with_context("service", Scheme::Ipmfs)
        })?;

        debug!("backend build finished: {:?}", &self);
        Ok(apply_wrapper(Backend::new(root, client, endpoint)))
    }
}
