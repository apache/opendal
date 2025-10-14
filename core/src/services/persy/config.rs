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

use super::backend::PersyBuilder;
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use serde::Serialize;

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

impl crate::Configurator for PersyConfig {
    type Builder = PersyBuilder;
    fn from_uri(uri: &crate::types::OperatorUri) -> crate::Result<Self> {
        let mut map = uri.options().clone();

        if let Some(authority) = uri.authority() {
            let decoded = percent_decode_str(authority).decode_utf8_lossy();
            if !decoded.is_empty() {
                map.entry("datafile".to_string())
                    .or_insert_with(|| decoded.to_string());
            }
        }

        if let Some(path) = uri.root() {
            if !path.is_empty() {
                let (segment, rest) = match path.split_once('/') {
                    Some((segment, remainder)) => (segment, Some(remainder)),
                    None => (path, None),
                };

                if !segment.is_empty() {
                    map.entry("segment".to_string())
                        .or_insert_with(|| segment.to_string());
                }

                if let Some(index) = rest {
                    if !index.is_empty() {
                        map.entry("index".to_string())
                            .or_insert_with(|| index.to_string());
                    }
                }
            }
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
    use crate::Configurator;
    use crate::types::OperatorUri;

    #[test]
    fn from_uri_sets_datafile_segment_and_index() {
        let uri = OperatorUri::new(
            "persy://%2Fvar%2Fdata%2Fpersy/segment/index"
                .parse()
                .unwrap(),
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = PersyConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.datafile.as_deref(), Some("/var/data/persy"));
        assert_eq!(cfg.segment.as_deref(), Some("segment"));
        assert_eq!(cfg.index.as_deref(), Some("index"));
    }
}
