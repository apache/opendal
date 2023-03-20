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

use anyhow::{anyhow, Result};
use opendal::{services, Operator};
use serde::Deserialize;
use toml;

type StringMap<T> = HashMap<String, T>;

macro_rules! set_bucket {
    ($m: expr, $bucket: ident) => {{
        let mut opts = $m.clone().unwrap();
        opts.insert("bucket".to_string(), $bucket.to_string());
        opts
    }};
}

#[derive(Deserialize, Default)]
pub struct Config {
    profiles: StringMap<Profile>,
}

#[derive(Deserialize, Default)]
pub struct Profile {
    // TODO: add more services
    s3: Option<StringMap<String>>,
    oss: Option<StringMap<String>>,
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
        Ok(toml::from_str(s)?)
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

        let bucket_and_path = parts[1].splitn(2, '/').collect::<Vec<_>>();
        debug_assert!(bucket_and_path.len() == 2);

        let bucket = bucket_and_path[0];
        let path = bucket_and_path[1];

        let profile = self
            .profiles
            .get(profile_name)
            .ok_or_else(|| anyhow!("unknown profile: {}", profile_name))?;

        if profile.s3.is_some() {
            let opts = set_bucket!(profile.s3, bucket);
            Ok((Operator::from_map::<services::S3>(opts)?.finish(), path))
        } else if profile.oss.is_some() {
            let opts = set_bucket!(profile.oss, bucket);
            Ok((Operator::from_map::<services::Oss>(opts)?.finish(), path))
        } else {
            Err(anyhow!("invalid profile"))
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
[profiles.mys3.s3]
region = "us-east-1"
access_key_id = "foo"
enable_virtual_host_style = "on"
"#,
        )
        .expect("load config");
        let profile = cfg.profiles["mys3"].s3.clone().unwrap();
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
            profiles: StringMap::from([(
                "mys3".into(),
                Profile {
                    s3: Some(StringMap::from([("region".into(), "us-east-1".into())])),
                    ..Default::default()
                },
            )]),
        };
        let (op, path) = cfg.parse_location("mys3://mybucket/foo/1.txt").unwrap();
        assert_eq!("foo/1.txt", path);
        let info = op.info();
        assert_eq!(Scheme::S3, info.scheme());
        assert_eq!("mybucket", info.name());
    }
}
