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
use std::env;
use std::fs;
use std::path::Path;
use std::str::FromStr;

use anyhow::{anyhow, Result};
use opendal::{services, Operator, Scheme};
use serde::Deserialize;
use toml;

#[derive(Deserialize, Default)]
pub struct Config {
    profiles: HashMap<String, HashMap<String, String>>,
}

impl Config {
    /// Load profiles from both environment variables and local config file,
    /// environment variables have higher precedence.
    pub fn load<P: AsRef<Path>>(fp: P) -> Result<Config> {
        let cfg = Config::load_from_file(fp)?;
        let profiles = Config::load_from_env().profiles.into_iter().fold(
            cfg.profiles,
            |mut acc, (name, opts)| {
                acc.entry(name).or_insert(HashMap::new()).extend(opts);
                acc
            },
        );
        Ok(Config { profiles })
    }
    /// Parse a local config file.
    ///
    /// - If the config file is not present, a default Config is returned.
    pub fn load_from_file<P: AsRef<Path>>(fp: P) -> Result<Config> {
        let config_path = fp.as_ref();
        if !config_path.exists() {
            return Ok(Config::default());
        }
        let data = fs::read_to_string(config_path)?;
        Ok(toml::from_str(&data)?)
    }

    /// Load config from environment variables.
    ///
    /// The format of each environment variable should be `OLI_PROFILE_{PROFILE NAME}_{OPTION}`,
    /// such as `OLI_PROFILE_PROFILE1_TYPE`, `OLI_PROFILE_MY-PROFILE_ACCESS_KEY_ID`.
    ///
    /// Please note that the profile name cannot contain underscores.
    pub(crate) fn load_from_env() -> Config {
        let prefix = "oli_profile_";
        let profiles = env::vars()
            .filter_map(|(k, v)| {
                k.to_lowercase().strip_prefix(prefix).and_then(
                    |k| -> Option<(String, String, String)> {
                        if let Some((profile_name, param)) = k.split_once('_') {
                            return Some((profile_name.to_string(), param.to_string(), v));
                        }
                        None
                    },
                )
            })
            .fold(HashMap::new(), |mut acc, (profile_name, key, val)| {
                acc.entry(profile_name)
                    .or_insert(HashMap::new())
                    .insert(key, val);
                acc
            });
        Config { profiles }
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
            Scheme::Azblob => Ok((
                Operator::from_map::<services::Azblob>(profile.clone())?.finish(),
                path,
            )),
            Scheme::Azdfs => Ok((
                Operator::from_map::<services::Azdfs>(profile.clone())?.finish(),
                path,
            )),
            #[cfg(feature = "services-dashmap")]
            Scheme::Dashmap => Ok((
                Operator::from_map::<services::Dashmap>(profile.clone())?.finish(),
                path,
            )),
            Scheme::Gcs => Ok((
                Operator::from_map::<services::Gcs>(profile.clone())?.finish(),
                path,
            )),
            Scheme::Ghac => Ok((
                Operator::from_map::<services::Ghac>(profile.clone())?.finish(),
                path,
            )),
            #[cfg(feature = "services-hdfs")]
            Scheme::Hdfs => Ok((
                Operator::from_map::<services::Hdfs>(profile.clone())?.finish(),
                path,
            )),
            Scheme::Http => Ok((
                Operator::from_map::<services::Http>(profile.clone())?.finish(),
                path,
            )),
            #[cfg(feature = "services-ftp")]
            Scheme::Ftp => Ok((
                Operator::from_map::<services::Ftp>(profile.clone())?.finish(),
                path,
            )),
            #[cfg(feature = "services-ipfs")]
            Scheme::Ipfs => Ok((
                Operator::from_map::<services::Ipfs>(profile.clone())?.finish(),
                path,
            )),
            Scheme::Ipmfs => Ok((
                Operator::from_map::<services::Ipmfs>(profile.clone())?.finish(),
                path,
            )),
            #[cfg(feature = "services-memcached")]
            Scheme::Memcached => Ok((
                Operator::from_map::<services::Memcached>(profile.clone())?.finish(),
                path,
            )),
            // ignore the memory backend
            #[cfg(feature = "services-moka")]
            Scheme::Moka => Ok((
                Operator::from_map::<services::Moka>(profile.clone())?.finish(),
                path,
            )),
            Scheme::Obs => Ok((
                Operator::from_map::<services::Obs>(profile.clone())?.finish(),
                path,
            )),
            Scheme::Oss => Ok((
                Operator::from_map::<services::Oss>(profile.clone())?.finish(),
                path,
            )),
            #[cfg(feature = "services-redis")]
            Scheme::Redis => Ok((
                Operator::from_map::<services::Redis>(profile.clone())?.finish(),
                path,
            )),
            #[cfg(feature = "services-rocksdb")]
            Scheme::Rocksdb => Ok((
                Operator::from_map::<services::Rocksdb>(profile.clone())?.finish(),
                path,
            )),
            Scheme::S3 => Ok((
                Operator::from_map::<services::S3>(profile.clone())?.finish(),
                path,
            )),
            #[cfg(feature = "services-sled")]
            Scheme::Sled => Ok((
                Operator::from_map::<services::Sled>(profile.clone())?.finish(),
                path,
            )),
            Scheme::Webdav => Ok((
                Operator::from_map::<services::Webdav>(profile.clone())?.finish(),
                path,
            )),
            Scheme::Webhdfs => Ok((
                Operator::from_map::<services::Webhdfs>(profile.clone())?.finish(),
                path,
            )),
            _ => Err(anyhow!("invalid profile")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal::Scheme;

    #[test]
    fn test_load_from_env() {
        let env_vars = vec![
            ("OLI_PROFILE_TEST1_TYPE", "s3"),
            ("OLI_PROFILE_TEST1_ACCESS_KEY_ID", "foo"),
            ("OLI_PROFILE_TEST2_TYPE", "oss"),
            ("OLI_PROFILE_TEST2_ACCESS_KEY_ID", "bar"),
        ];
        for (k, v) in &env_vars {
            env::set_var(k, v);
        }

        let profiles = Config::load_from_env().profiles;

        let profile1 = profiles["test1"].clone();
        assert_eq!(profile1["type"], "s3");
        assert_eq!(profile1["access_key_id"], "foo");

        let profile2 = profiles["test2"].clone();
        assert_eq!(profile2["type"], "oss");
        assert_eq!(profile2["access_key_id"], "bar");

        for (k, _) in &env_vars {
            env::remove_var(k);
        }
    }

    #[test]
    fn test_load_from_toml() -> Result<()> {
        let dir = env::temp_dir();
        let tmpfile = dir.join("oli1.toml");
        fs::write(
            &tmpfile,
            r#"
[profiles.mys3]
type = "s3"
region = "us-east-1"
access_key_id = "foo"
enable_virtual_host_style = "on"
"#,
        )?;
        let cfg = Config::load_from_file(tmpfile)?;
        let profile = cfg.profiles["mys3"].clone();
        assert_eq!(profile["region"], "us-east-1");
        assert_eq!(profile["access_key_id"], "foo");
        assert_eq!(profile["enable_virtual_host_style"], "on");
        Ok(())
    }

    #[test]
    fn test_load_config_from_file_and_env() -> Result<()> {
        let dir = env::temp_dir();
        let tmpfile = dir.join("oli2.toml");
        fs::write(
            &tmpfile,
            r#"
    [profiles.mys3]
    type = "s3"
    region = "us-east-1"
    access_key_id = "foo"
    "#,
        )?;
        let env_vars = vec![
            ("OLI_PROFILE_MYS3_REGION", "us-west-1"),
            ("OLI_PROFILE_MYS3_ENABLE_VIRTUAL_HOST_STYLE", "on"),
        ];
        for (k, v) in &env_vars {
            env::set_var(k, v);
        }
        let cfg = Config::load(tmpfile)?;
        let profile = cfg.profiles["mys3"].clone();
        assert_eq!(profile["region"], "us-west-1");
        assert_eq!(profile["access_key_id"], "foo");
        assert_eq!(profile["enable_virtual_host_style"], "on");

        for (k, _) in &env_vars {
            env::remove_var(k);
        }
        Ok(())
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
