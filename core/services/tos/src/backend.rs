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

use crate::TosConfig;
use crate::core::TosCore;
use opendal_core::raw::*;
use opendal_core::{Builder, Capability, Result};

const TOS_SCHEME: &str = "tos";

/// Builder for Volcengine TOS service.
#[derive(Default)]
pub struct TosBuilder {
    pub(super) config: TosConfig,
}

impl Debug for TosBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TosBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl TosBuilder {
    /// Set root of this backend.
    ///
    /// All operations will happen under this root.
    pub fn root(mut self, root: &str) -> Self {
        self.config.root = if root.is_empty() {
            None
        } else {
            Some(root.to_string())
        };
        self
    }

    /// Set bucket name of this backend.
    pub fn bucket(mut self, bucket: &str) -> Self {
        self.config.bucket = bucket.to_string();
        self
    }

    /// Set endpoint of this backend.
    ///
    /// Endpoint must be full uri, e.g.
    /// - TOS: `https://tos-cn-beijing.volces.com`
    /// - TOS with region: `https://tos-{region}.volces.com`
    ///
    /// If user inputs endpoint without scheme like "tos-cn-beijing.volces.com", we
    /// will prepend "https://" before it.
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        if !endpoint.is_empty() {
            self.config.endpoint = Some(endpoint.trim_end_matches('/').to_string());
        }
        self
    }

    /// Set region of this backend.
    ///
    /// Region represent the signing region of this endpoint.
    ///
    /// If region is not set, we will try to load it from environment.
    /// If still not set, default to `cn-beijing`.
    pub fn region(mut self, region: &str) -> Self {
        if !region.is_empty() {
            self.config.region = Some(region.to_string());
        }
        self
    }

    /// Set access_key_id of this backend.
    ///
    /// - If access_key_id is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn access_key_id(mut self, v: &str) -> Self {
        self.config.access_key_id = Some(v.to_string());
        self
    }

    /// Set secret_access_key of this backend.
    ///
    /// - If secret_access_key is set, we will take user's input first.
    /// - If not, we will try to load it from environment.
    pub fn secret_access_key(mut self, v: &str) -> Self {
        self.config.secret_access_key = Some(v.to_string());
        self
    }

    /// Set security_token of this backend.
    pub fn security_token(mut self, v: &str) -> Self {
        self.config.security_token = Some(v.to_string());
        self
    }

    /// Allow anonymous will allow opendal to send request without signing
    /// when credential is not loaded.
    pub fn allow_anonymous(mut self, allow: bool) -> Self {
        self.config.allow_anonymous = allow;
        self
    }

    /// Set bucket versioning status for this backend.
    ///
    /// If set to true, OpenDAL will support versioned operations like list with
    /// versions, read with version, etc.
    pub fn enable_versioning(mut self, enabled: bool) -> Self {
        self.config.enable_versioning = enabled;
        self
    }
}

impl Builder for TosBuilder {
    type Config = TosConfig;

    fn build(self) -> Result<impl Access> {
        let mut config = self.config;
        let region = config
            .region
            .clone()
            .unwrap_or_else(|| "cn-beijing".to_string());

        if config.endpoint.is_none() {
            config.endpoint = Some(format!("https://tos-{}.volces.com", region));
        }

        let endpoint = config.endpoint.clone().unwrap();
        let bucket = config.bucket.clone();
        let root = config.root.clone().unwrap_or_else(|| "/".to_string());

        let info = {
            let am = AccessorInfo::default();
            am.set_scheme(TOS_SCHEME)
                .set_root(&root)
                .set_name(&bucket)
                .set_native_capability(Capability {
                    read: false,

                    write: false,

                    delete: false,

                    list: false,

                    stat: false,

                    shared: false,

                    ..Default::default()
                });

            am.into()
        };

        let core = TosCore {
            info,
            bucket,
            endpoint: endpoint.clone(),
            root,
        };

        Ok(TosBackend {
            core: Arc::new(core),
        })
    }
}

#[derive(Debug)]
pub struct TosBackend {
    core: Arc<TosCore>,
}

impl Access for TosBackend {
    type Reader = ();
    type Writer = ();
    type Lister = ();
    type Deleter = ();

    fn info(&self) -> Arc<AccessorInfo> {
        self.core.info.clone()
    }
}
