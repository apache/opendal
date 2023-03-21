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

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use opendal::{services, Operator, Scheme};
use serde::Deserialize;
use toml;

macro_rules! update_options {
    ($m: expr) => {{
        let mut opts = $m.clone();
        opts.remove("type");
        opts
    }};
}

#[derive(Deserialize, Default)]
pub struct Config {
    profiles: HashMap<String, HashMap<String, String>>,
}

impl Config {
    /// Parse a local config file.
    ///
    /// - If the config file is not present, a default Config is returned.
    pub fn load_from_file<P: AsRef<Path>>(fp: P) -> Result<Config> {
        let config_path = fp.as_ref();
        if !config_path.exists() {
            return Ok(Config::default());
        }
        let data = fs::read_to_string(config_path)?;
        Config::load_from_str(&data)
    }

    pub(crate) fn load_from_str(s: &str) -> Result<Config> {
        let cfg: Config = toml::from_str(s)?;
        for (name, opts) in &cfg.profiles {
            if opts.get("type").is_none() {
                return Err(anyhow!("profile {}: missing 'type'", name));
            }
        }
        Ok(cfg)
    }

    /// Parse `<profile>://abc/def` into `op` and `location`.
    pub fn parse_location<'a>(&self, s: &'a str) -> Result<(Operator, &'a str)> {
        if !s.contains("://") {
            let mut fs = services::Fs::default();

            let filename = match s.rsplit_once(['/', '\\']) {
                Some((base, filename)) => {
                    fs.root(base);
                    filename
                }
                None => s,
            };

            return Ok((Operator::new(fs)?.finish(), filename));
        }

        let parts = s.splitn(2, "://").collect::<Vec<_>>();
        debug_assert!(parts.len() == 2);

        let profile_name = parts[0];
        let path = parts[1];

        let profile = self
            .profiles
            .get(profile_name)
            .ok_or_else(|| anyhow!("unknown profile: {}", profile_name))?;

        let svc = profile
            .get("type")
            .ok_or_else(|| anyhow!("missing 'type' in profile"))?;
        let scheme = Scheme::from_str(svc)?;
        match scheme {
            Scheme::S3 => {
                let opts = update_options!(profile);
                Ok((Operator::from_map::<services::S3>(opts)?.finish(), path))
            }
            Scheme::Oss => {
                let opts = update_options!(profile);
                Ok((Operator::from_map::<services::Oss>(opts)?.finish(), path))
            }
            _ => Err(anyhow!("invalid profile")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::Scheme;

    #[test]
    fn test_load_toml() {
        let cfg = Config::load_from_str(
            r#"
[profiles.mys3]
type = "s3"
region = "us-east-1"
access_key_id = "foo"
enable_virtual_host_style = "on"
"#,
        )
        .expect("load config");
        let profile = cfg.profiles["mys3"].clone();
        assert_eq!(profile["region"], "us-east-1");
        assert_eq!(profile["access_key_id"], "foo");
        assert_eq!(profile["enable_virtual_host_style"], "on");
    }

    #[test]
    fn test_parse_fs_location() {
        let cfg = Config::default();
        let (op, path) = cfg.parse_location("./foo/bar/1.txt").unwrap();
        assert_eq!("1.txt", path);
        let info = op.info();
        assert!(info.root().ends_with("foo/bar"));
        assert_eq!(Scheme::Fs, info.scheme());
    }

    #[test]
    fn test_parse_s3_location() {
        let cfg = Config {
            profiles: HashMap::from([(
                "mys3".into(),
                HashMap::from([
                    ("type".into(), "s3".into()),
                    ("bucket".into(), "mybucket".into()),
                    ("region".into(), "us-east-1".into()),
                ]),
            )]),
        };
        let (op, path) = cfg.parse_location("mys3://foo/1.txt").unwrap();
        assert_eq!("foo/1.txt", path);
        let info = op.info();
        assert_eq!(Scheme::S3, info.scheme());
        assert_eq!("mybucket", info.name());
    }
}
