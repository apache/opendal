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

use super::backend::FsBuilder;

/// config for file system
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FsConfig {
    /// root dir for backend
    pub root: Option<String>,

    /// tmp dir for atomic write
    pub atomic_write_dir: Option<String>,

    /// Bypass the page cache for file reads and writes on Linux. Defaults to `false`.
    pub direct_io: bool,
}

impl opendal_core::Configurator for FsConfig {
    type Builder = FsBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        // A local path has no host. Ignoring one would silently move the root:
        // `fs://tmp/data` would become `/data`, and `fs://C:/data` would become
        // `/data` on the current drive.
        if let Some(host) = uri.name()
            && !host.eq_ignore_ascii_case("localhost")
        {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "fs uri must not contain a host, use fs:///path or file:///path",
            )
            .with_context("host", host));
        }

        let mut map = uri.options().clone();

        if let Some(root) = uri.root().filter(|v| !v.is_empty()) {
            map.entry("root".to_string())
                .or_insert_with(|| uri_path_to_root(root));
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        FsBuilder { config: self }
    }
}

/// Turn the path of a `file:///` uri, without its leading `/`, into a root.
///
/// On Windows, `file:///C:/data` carries the drive in its path, and `/C:/data`
/// is not a valid path, so keep `C:/data` as is.
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
    fn from_uri_extracts_root() {
        let uri = OperatorUri::new("fs:///tmp/data", Vec::<(String, String)>::new()).unwrap();
        let cfg = FsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.root.as_deref(), Some("/tmp/data"));
    }

    #[test]
    fn from_uri_with_file_scheme() {
        // "file" is an alias for "fs" to support standard file:// URIs
        let uri = OperatorUri::new("file:///tmp/data", Vec::<(String, String)>::new()).unwrap();
        let cfg = FsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.root.as_deref(), Some("/tmp/data"));
    }

    #[test]
    fn from_uri_accepts_localhost() {
        let uri =
            OperatorUri::new("file://localhost/tmp/data", Vec::<(String, String)>::new()).unwrap();
        let cfg = FsConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.root.as_deref(), Some("/tmp/data"));
    }

    #[test]
    fn from_uri_rejects_host() {
        for base in ["fs://tmp/data", "file://server/share/data", "fs://C:/data"] {
            let uri = OperatorUri::new(base, Vec::<(String, String)>::new()).unwrap();
            let err = FsConfig::from_uri(&uri).unwrap_err();
            assert_eq!(err.kind(), opendal_core::ErrorKind::ConfigInvalid, "{base}");
        }
    }

    #[test]
    fn from_uri_with_windows_drive() {
        let uri = OperatorUri::new("file:///C:/data", Vec::<(String, String)>::new()).unwrap();
        let cfg = FsConfig::from_uri(&uri).unwrap();
        let expected = if cfg!(windows) { "C:/data" } else { "/C:/data" };
        assert_eq!(cfg.root.as_deref(), Some(expected));

        for base in ["file:///C:/", "file:///C:"] {
            let uri = OperatorUri::new(base, Vec::<(String, String)>::new()).unwrap();
            let cfg = FsConfig::from_uri(&uri).unwrap();
            let expected = if cfg!(windows) { "C:/" } else { "/C:" };
            assert_eq!(cfg.root.as_deref(), Some(expected), "{base}");
        }
    }
}
