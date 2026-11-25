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

use opendal_core::Result;
use serde::{Deserialize, Serialize};

use crate::preset::{self, Connection};

/// Configuration for Oracle Cloud Infrastructure Object Storage.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
#[non_exhaustive]
pub struct OciConfig {
    /// Root within the bucket. The default is `/`.
    ///
    /// <!-- @group General -->
    /// <!-- @default / -->
    pub root: Option<String>,
    /// Bucket name.
    ///
    /// <!-- @group General -->
    /// <!-- @example example-bucket -->
    pub bucket: String,
    /// Object Storage namespace. Required unless `endpoint` is set.
    ///
    /// <!-- @group General -->
    /// <!-- @example example-namespace -->
    /// <!-- @minimal true -->
    pub namespace: Option<String>,
    /// Bucket region, used as the signing region.
    ///
    /// <!-- @group General -->
    /// <!-- @example us-ashburn-1 -->
    pub region: String,
    /// Explicit HTTP(S) base endpoint. The default is
    /// `https://<namespace>.compat.objectstorage.<region>.oci.customer-oci.com`.
    ///
    /// <!-- @group General -->
    pub endpoint: Option<String>,
    /// Access key ID. Set this field together with `secret_access_key`.
    ///
    /// <!-- @group Credentials -->
    pub access_key_id: Option<String>,
    /// Secret access key. Set this field together with `access_key_id`.
    ///
    /// <!-- @group Credentials -->
    pub secret_access_key: Option<String>,
}

impl OciConfig {
    fn connection(&self) -> Result<Connection> {
        let endpoint = match &self.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => {
                let namespace = self.namespace.as_deref().ok_or_else(|| {
                    preset::config_error(OCI_SCHEME, "namespace is required without endpoint")
                })?;
                let namespace = preset::dns_label(namespace, OCI_SCHEME, "namespace")?;
                let region = preset::dns_label(&self.region, OCI_SCHEME, "region")?;
                format!("https://{namespace}.compat.objectstorage.{region}.oci.customer-oci.com")
            }
        };

        Ok(Connection {
            endpoint,
            region: self.region.clone(),
            virtual_host_style: false,
        })
    }
}

preset::provider_preset! {
    OciConfig, Oci, OCI_SCHEME = "oci", register_oci_service, "oci.md";
    required {
        bucket: "Set the bucket name.",
        region: "Set the bucket region.",
    }
    optional {
        root: "Set the root within the bucket.",
        namespace: "Set the Object Storage namespace.",
        endpoint: "Set an explicit base endpoint.",
        access_key_id: "Set the access key ID.",
        secret_access_key: "Set the secret access key.",
    }
    flags {}
}
