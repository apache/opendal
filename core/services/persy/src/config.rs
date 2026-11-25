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

use super::backend::PersyBuilder;

/// Config for persy service support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct PersyConfig {
    /// That path to the persy data file. The directory in the path must already exist.
    pub datafile: Option<String>,
    /// That name of the persy segment.
    pub segment: Option<String>,
    /// That name of the persy index.
    pub index: Option<String>,
}

impl opendal_core::Configurator for PersyConfig {
    type Builder = PersyBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(path) = uri.root()
            && !path.is_empty()
        {
            map.entry("datafile".to_string())
                .or_insert_with(|| format!("/{path}"));
        }

        // A local path has no host. Ignoring one would silently move the
        // datafile: `persy://tmp/data` would become `/data`. An explicit
        // `datafile` option takes precedence, as for fs.
        if !uri.options().contains_key("datafile")
            && let Some(host) = uri.name()
            && !host.eq_ignore_ascii_case("localhost")
        {
            return Err(opendal_core::Error::new(
                opendal_core::ErrorKind::ConfigInvalid,
                "persy uri must not contain a host, use persy:///path",
            )
            .with_context("host", host));
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        PersyBuilder { config: self }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;

    #[test]
    fn from_uri_sets_datafile_segment_and_index() {
        let uri = OperatorUri::new(
            "persy:///var/data/persy?segment=segment&index=index",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = PersyConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.datafile.as_deref(), Some("/var/data/persy"));
        assert_eq!(cfg.segment.as_deref(), Some("segment"));
        assert_eq!(cfg.index.as_deref(), Some("index"));
    }

    #[test]
    fn from_uri_rejects_host() {
        let no_opts = Vec::<(String, String)>::new;
        let uri = OperatorUri::new("persy://tmp/data", no_opts()).unwrap();
        let err = PersyConfig::from_uri(&uri).unwrap_err();
        assert_eq!(err.kind(), opendal_core::ErrorKind::ConfigInvalid);

        let uri = OperatorUri::new("persy://localhost/data", no_opts()).unwrap();
        assert_eq!(
            PersyConfig::from_uri(&uri).unwrap().datafile.as_deref(),
            Some("/data")
        );

        // An explicit `datafile` option takes precedence over the host.
        let uri = OperatorUri::new(
            "persy://tmp/data",
            [("datafile".to_string(), "/explicit".to_string())],
        )
        .unwrap();
        assert_eq!(
            PersyConfig::from_uri(&uri).unwrap().datafile.as_deref(),
            Some("/explicit")
        );
    }
}
