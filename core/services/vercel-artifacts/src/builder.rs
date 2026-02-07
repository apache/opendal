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

use std::fmt::Debug;
use std::sync::Arc;

use super::VERCEL_ARTIFACTS_SCHEME;
use super::backend::VercelArtifactsBackend;
use super::config::VercelArtifactsConfig;
use super::core::VercelArtifactsCore;
use opendal_core::raw::*;
use opendal_core::*;

/// [Vercel Cache](https://vercel.com/docs/concepts/monorepos/remote-caching) backend support.
#[doc = include_str!("docs.md")]
#[derive(Default)]
pub struct VercelArtifactsBuilder {
    pub(super) config: VercelArtifactsConfig,
}

impl Debug for VercelArtifactsBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VercelArtifactsBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl VercelArtifactsBuilder {
    /// set the bearer access token for Vercel
    ///
    /// default: no access token, which leads to failure
    pub fn access_token(mut self, access_token: &str) -> Self {
        self.config.access_token = Some(access_token.to_string());
        self
    }
}

impl Builder for VercelArtifactsBuilder {
    type Config = VercelArtifactsConfig;

    fn build(self) -> Result<impl Access> {
        let info = AccessorInfo::default();
        info.set_scheme(VERCEL_ARTIFACTS_SCHEME)
            .set_native_capability(Capability {
                stat: true,

                read: true,

                write: true,

                shared: true,

                ..Default::default()
            });

        match self.config.access_token.clone() {
            Some(access_token) => Ok(VercelArtifactsBackend {
                core: Arc::new(VercelArtifactsCore {
                    info: Arc::new(info),
                    access_token,
                }),
            }),
            None => Err(Error::new(ErrorKind::ConfigInvalid, "access_token not set")),
        }
    }
}
