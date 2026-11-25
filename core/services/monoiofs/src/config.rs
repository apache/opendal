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

use opendal_core::*;
use serde::Deserialize;
use serde::Serialize;

use super::backend::MonoiofsBuilder;

/// Config for monoiofs services support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct MonoiofsConfig {
    /// The Root of this backend.
    ///
    /// All operations will happen under this root.
    ///
    /// Builder::build will return error if not set.
    pub root: Option<String>,
}

impl Configurator for MonoiofsConfig {
    type Builder = MonoiofsBuilder;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        let mut map = uri.options().clone();

        let uri_path = uri.root().filter(|root| !root.is_empty());
        if let Some(root) = uri_path {
            map.insert("root".to_string(), format!("/{root}"));
        }

        // A local path has no host. Ignoring one would silently move the
        // root: `monoiofs://tmp/data` would become `/data`. The URI path wins
        // over an explicit `root` option, so a host is only ignored when the
        // URI has no path and the explicit `root` is used.
        if (uri_path.is_some() || !uri.options().contains_key("root"))
            && let Some(host) = uri.name()
            && !host.eq_ignore_ascii_case("localhost")
        {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "monoiofs uri must not contain a host, use monoiofs:///path",
            )
            .with_context("host", host));
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        MonoiofsBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_uri_sets_root() -> Result<()> {
        let uri = OperatorUri::new("monoiofs:///tmp", Vec::<(String, String)>::new())?;

        let cfg = MonoiofsConfig::from_uri(&uri)?;
        assert_eq!(cfg.root.as_deref(), Some("/tmp"));
        Ok(())
    }

    #[test]
    fn from_uri_rejects_host() {
        let no_opts = Vec::<(String, String)>::new;
        let uri = OperatorUri::new("monoiofs://tmp/data", no_opts()).unwrap();
        let err = MonoiofsConfig::from_uri(&uri).unwrap_err();
        assert_eq!(err.kind(), opendal_core::ErrorKind::ConfigInvalid);

        let uri = OperatorUri::new("monoiofs://localhost/data", no_opts()).unwrap();
        assert_eq!(
            MonoiofsConfig::from_uri(&uri).unwrap().root.as_deref(),
            Some("/data")
        );

        // The URI path wins over an explicit `root`, so a host next to a path
        // is still dropped and rejected. It is only ignored when the URI has
        // no path and the explicit `root` is used.
        let explicit_root = || [("root".to_string(), "/explicit".to_string())];
        let uri = OperatorUri::new("monoiofs://tmp/data", explicit_root()).unwrap();
        assert!(MonoiofsConfig::from_uri(&uri).is_err());
        let uri = OperatorUri::new("monoiofs://tmp", explicit_root()).unwrap();
        assert_eq!(
            MonoiofsConfig::from_uri(&uri).unwrap().root.as_deref(),
            Some("/explicit")
        );

        // Without a host, a nonempty URI path still wins over a conflicting
        // explicit `root`, as verified in #9044.
        let uri = OperatorUri::new("monoiofs:///a?root=/b", no_opts()).unwrap();
        assert_eq!(
            MonoiofsConfig::from_uri(&uri).unwrap().root.as_deref(),
            Some("/a")
        );
    }
}
