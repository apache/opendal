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

use opendal_core::{Configurator, OperatorUri, Result};
use serde::{Deserialize, Serialize};

use crate::Hf3fs;

/// Configuration for DeepSeek's 3FS (Fire-Flyer File System) USRBIO access on Linux.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct Hf3fsConfig {
    /// Existing absolute directory on a mounted DeepSeek 3FS (Fire-Flyer File System) instance. Required.
    pub root: Option<String>,
    /// USRBIO shared library path. Defaults to `libhf3fs_api_shared.so`.
    pub library_path: Option<String>,
    /// Maximum in-flight requests per read or write ring. Defaults to 32.
    pub queue_depth: Option<usize>,
    /// Maximum bytes per request. Defaults to 1 MiB.
    /// Shared memory usage is approximately `3 * queue_depth * block_size`.
    pub block_size: Option<usize>,
}

impl Configurator for Hf3fsConfig {
    type Builder = Hf3fs;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        let mut options = uri.options().clone();
        if let Some(root) = uri.root().filter(|v| !v.is_empty()) {
            options
                .entry("root".into())
                .or_insert_with(|| format!("/{root}"));
        }
        // A local path has no host. Ignoring one would silently move the
        // root: `hf3fs://tmp/data` would become `/data`. An explicit
        // `root` option takes precedence, as for fs.
        if !uri.options().contains_key("root")
            && let Some(host) = uri.name()
            && !host.eq_ignore_ascii_case("localhost")
        {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "hf3fs uri must not contain a host, use hf3fs:///path",
            )
            .with_context("host", host));
        }

        Self::from_iter(options)
    }

    fn into_builder(self) -> Self::Builder {
        Hf3fs { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;

    #[test]
    fn from_uri_rejects_host() {
        let no_opts = Vec::<(String, String)>::new;
        let uri = OperatorUri::new("hf3fs://tmp/data", no_opts()).unwrap();
        let err = Hf3fsConfig::from_uri(&uri).unwrap_err();
        assert_eq!(err.kind(), opendal_core::ErrorKind::ConfigInvalid);

        let uri = OperatorUri::new("hf3fs://localhost/data", no_opts()).unwrap();
        assert_eq!(
            Hf3fsConfig::from_uri(&uri).unwrap().root.as_deref(),
            Some("/data")
        );

        // An explicit `root` option takes precedence over the host.
        let uri = OperatorUri::new(
            "hf3fs://tmp/data",
            [("root".to_string(), "/explicit".to_string())],
        )
        .unwrap();
        assert_eq!(
            Hf3fsConfig::from_uri(&uri).unwrap().root.as_deref(),
            Some("/explicit")
        );
    }
}
