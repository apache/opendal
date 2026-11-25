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

use serde::Deserialize;
use serde::Serialize;

use super::backend::CompfsBuilder;

/// compio-based file system support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct CompfsConfig {
    /// root of this backend.
    ///
    /// All operations will happen under this root.
    pub root: Option<String>,
}

impl opendal_core::Configurator for CompfsConfig {
    type Builder = CompfsBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();

        let uri_path = uri.root().filter(|root| !root.is_empty());
        if let Some(root) = uri_path {
            map.insert("root".to_string(), uri_path_to_root(root));
        }

        // A local path has no host. Ignoring one would silently move the
        // root: `compfs://tmp/data` would become `/data`. The URI path wins
        // over an explicit `root` option, so a host is only ignored when the
        // URI has no path and the explicit `root` is used.
        if (uri_path.is_some() || !uri.options().contains_key("root"))
            && let Some(host) = uri.name()
            && !host.eq_ignore_ascii_case("localhost")
        {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "compfs uri must not contain a host, use compfs:///path",
            )
            .with_context("host", host));
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        CompfsBuilder { config: self }
    }
}

/// Turn the path of a `compfs:///` uri, without its leading `/`, into an
/// absolute root.
///
/// On Windows, `compfs:///C:/data` carries the drive in its path, and
/// `/C:/data` is not a valid path, so keep `C:/data` as is.
fn uri_path_to_root(path: &str) -> String {
    let bytes = path.as_bytes();
    let has_drive = bytes.len() >= 2
        && bytes[0].is_ascii_alphabetic()
        && bytes[1] == b':'
        && (bytes.len() == 2 || bytes[2] == b'/');
    if cfg!(windows) && has_drive {
        // A bare `C:` means the current directory of drive C, not its root.
        if bytes.len() == 2 {
            format!("{path}/")
        } else {
            path.to_string()
        }
    } else {
        format!("/{path}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;

    #[test]
    fn from_uri_sets_root() {
        let uri = OperatorUri::new("compfs:///workdir", Vec::<(String, String)>::new()).unwrap();

        let cfg = CompfsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.root.as_deref(), Some("/workdir"));
    }

    #[test]
    fn from_uri_with_windows_drive() {
        let uri = OperatorUri::new("compfs:///C:/data", Vec::<(String, String)>::new()).unwrap();

        let cfg = CompfsConfig::from_uri(&uri).unwrap();
        let expected = if cfg!(windows) { "C:/data" } else { "/C:/data" };
        assert_eq!(cfg.root.as_deref(), Some(expected));

        let uri = OperatorUri::new("compfs:///C:/", Vec::<(String, String)>::new()).unwrap();
        let cfg = CompfsConfig::from_uri(&uri).unwrap();
        let expected = if cfg!(windows) { "C:/" } else { "/C:" };
        assert_eq!(cfg.root.as_deref(), Some(expected));
    }

    #[test]
    fn from_uri_rejects_host() {
        let no_opts = Vec::<(String, String)>::new;
        let uri = OperatorUri::new("compfs://tmp/data", no_opts()).unwrap();
        let err = CompfsConfig::from_uri(&uri).unwrap_err();
        assert_eq!(err.kind(), opendal_core::ErrorKind::ConfigInvalid);

        let uri = OperatorUri::new("compfs://localhost/data", no_opts()).unwrap();
        assert_eq!(
            CompfsConfig::from_uri(&uri).unwrap().root.as_deref(),
            Some("/data")
        );

        // The URI path wins over an explicit `root`, so a host next to a path
        // is still dropped and rejected. It is only ignored when the URI has
        // no path and the explicit `root` is used.
        let explicit_root = || [("root".to_string(), "/explicit".to_string())];
        let uri = OperatorUri::new("compfs://tmp/data", explicit_root()).unwrap();
        assert!(CompfsConfig::from_uri(&uri).is_err());
        let uri = OperatorUri::new("compfs://tmp", explicit_root()).unwrap();
        assert_eq!(
            CompfsConfig::from_uri(&uri).unwrap().root.as_deref(),
            Some("/explicit")
        );

        // Without a host, a nonempty URI path still wins over a conflicting
        // explicit `root`, as verified in #9044.
        let uri = OperatorUri::new("compfs:///a?root=/b", no_opts()).unwrap();
        assert_eq!(
            CompfsConfig::from_uri(&uri).unwrap().root.as_deref(),
            Some("/a")
        );
    }
}
