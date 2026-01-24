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

use super::backend::FoyerBuilder;
use opendal_core::{Configurator, Error, ErrorKind, OperatorUri, Result};

/// Config for Foyer services support.
#[derive(Default, Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct FoyerConfig {
    /// Name for this cache instance.
    pub name: Option<String>,
    /// Root path of this backend.
    pub root: Option<String>,
    /// Memory capacity in bytes for the cache.
    ///
    /// This is used when the cache is lazily initialized. Supports human-readable
    /// formats like "1MiB", "64MB", "1GiB", etc.
    pub memory: Option<usize>,
    /// Disk cache directory path.
    ///
    /// If set, enables hybrid cache with disk storage. Data will be persisted to
    /// this directory when memory cache is full.
    pub disk_path: Option<String>,
    /// Disk cache total capacity in bytes.
    ///
    /// Supports human-readable formats like "1GiB", "100MB", etc.
    /// Only used when `disk_path` is set.
    pub disk_capacity: Option<usize>,
    /// Individual cache file size in bytes.
    ///
    /// Supports human-readable formats. Default is 1MiB.
    /// Only used when `disk_path` is set.
    pub disk_file_size: Option<usize>,
    /// Recovery mode when starting the cache.
    ///
    /// Valid values: "none" (default), "quiet", "strict".
    /// - "none": Don't recover from disk
    /// - "quiet": Recover and skip errors
    /// - "strict": Recover and panic on errors
    pub recover_mode: Option<String>,
    /// Number of shards for concurrent access.
    ///
    /// Default is 1. Higher values improve concurrency but increase overhead.
    pub shards: Option<usize>,
}

impl Configurator for FoyerConfig {
    type Builder = FoyerBuilder;

    fn from_uri(uri: &OperatorUri) -> Result<Self> {
        let mut map = uri.options().clone();

        if let Some(name) = uri.option("name") {
            map.insert("name".to_string(), name.to_string());
        }

        if let Some(root) = uri.root() {
            if !root.is_empty() {
                map.insert("root".to_string(), root.to_string());
            }
        }

        // Parse capacity values with human-readable format support
        for (key, param_name) in [
            ("memory", "memory"),
            ("disk_capacity", "disk_capacity"),
            ("disk_file_size", "disk_file_size"),
        ] {
            if let Some(value_str) = uri.option(param_name) {
                match parse_capacity(value_str) {
                    Ok(size) => {
                        map.insert(key.to_string(), size.to_string());
                    }
                    Err(e) => {
                        return Err(Error::new(
                            ErrorKind::ConfigInvalid,
                            format!("invalid {}: {}", param_name, e),
                        )
                        .with_context("service", "foyer"));
                    }
                }
            }
        }

        Self::from_iter(map)
    }

    fn into_builder(self) -> Self::Builder {
        FoyerBuilder {
            config: self,
            ..Default::default()
        }
    }
}

fn parse_capacity(s: &str) -> Result<usize> {
    let s = s.trim();

    if let Ok(num) = s.parse::<usize>() {
        return Ok(num);
    }

    let (num_str, unit) = s.split_at(
        s.find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(s.len()),
    );

    let num: f64 = num_str
        .trim()
        .parse()
        .map_err(|_| Error::new(ErrorKind::ConfigInvalid, "invalid number format"))?;

    let multiplier = match unit.trim().to_lowercase().as_str() {
        "" => 1,
        "b" => 1,
        "k" | "kb" => 1000,
        "ki" | "kib" => 1024,
        "m" | "mb" => 1000 * 1000,
        "mi" | "mib" => 1024 * 1024,
        "g" | "gb" => 1000 * 1000 * 1000,
        "gi" | "gib" => 1024 * 1024 * 1024,
        "t" | "tb" => 1000_usize * 1000 * 1000 * 1000,
        "ti" | "tib" => 1024_usize * 1024 * 1024 * 1024,
        _ => {
            return Err(Error::new(
                ErrorKind::ConfigInvalid,
                format!("unknown unit: {}", unit.trim()),
            ));
        }
    };

    Ok((num * multiplier as f64) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;

    #[test]
    fn test_parse_capacity() {
        assert_eq!(parse_capacity("1024").unwrap(), 1024);
        assert_eq!(parse_capacity("1KB").unwrap(), 1000);
        assert_eq!(parse_capacity("1KiB").unwrap(), 1024);
        assert_eq!(parse_capacity("1MB").unwrap(), 1000 * 1000);
        assert_eq!(parse_capacity("1MiB").unwrap(), 1024 * 1024);
        assert_eq!(parse_capacity("1GB").unwrap(), 1000 * 1000 * 1000);
        assert_eq!(parse_capacity("1GiB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_capacity("64 MiB").unwrap(), 64 * 1024 * 1024);
        assert_eq!(
            parse_capacity("2.5GB").unwrap(),
            (2.5 * 1000.0 * 1000.0 * 1000.0) as usize
        );
    }

    #[test]
    fn test_from_uri_sets_memory() {
        let uri = OperatorUri::new(
            "foyer:///cache?name=test&memory=64MiB",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = FoyerConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.name.as_deref(), Some("test"));
        assert_eq!(cfg.root.as_deref(), Some("cache"));
        assert_eq!(cfg.memory, Some(64 * 1024 * 1024));
    }

    #[test]
    fn test_from_uri_sets_name_and_root() {
        let uri =
            OperatorUri::new("foyer:///data?name=session", Vec::<(String, String)>::new()).unwrap();

        let cfg = FoyerConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.name.as_deref(), Some("session"));
        assert_eq!(cfg.root.as_deref(), Some("data"));
    }

    #[test]
    fn test_from_uri_sets_disk_config() {
        let uri = OperatorUri::new(
            "foyer:///cache?memory=64MiB&disk_path=/tmp/foyer&disk_capacity=1GiB&disk_file_size=2MiB",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = FoyerConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.memory, Some(64 * 1024 * 1024));
        assert_eq!(cfg.disk_path.as_deref(), Some("/tmp/foyer"));
        assert_eq!(cfg.disk_capacity, Some(1024 * 1024 * 1024));
        assert_eq!(cfg.disk_file_size, Some(2 * 1024 * 1024));
    }

    #[test]
    fn test_from_uri_sets_recovery_and_shards() {
        let uri = OperatorUri::new(
            "foyer:///?recover_mode=quiet&shards=4",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = FoyerConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.recover_mode.as_deref(), Some("quiet"));
        assert_eq!(cfg.shards, Some(4));
    }
}
