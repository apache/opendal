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

use std::borrow::Cow;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use opendal::services;
use opendal::Operator;
use opendal::Scheme;
use serde::Deserialize;
use url::Url;

#[derive(Deserialize, Default)]
pub struct Config {
    profiles: HashMap<String, HashMap<String, String>>,
}

/// resolve_relative_path turns a relative path to an absolute path.
///
/// The reason why we don't use `fs::canonicalize` here is `fs::canonicalize`
/// will return an error if the path does not exist, which is unwanted.
pub fn resolve_relative_path(path: &Path) -> Cow<Path> {
    // NOTE: `path.is_absolute()` cannot handle cases like "/tmp/../a"
    if path
        .components()
        .all(|e| e != Component::ParentDir && e != Component::CurDir)
    {
        // it is an absolute path
        return path.into();
    }

    let root = Component::RootDir.as_os_str();
    let mut result = env::current_dir().unwrap_or_else(|_| PathBuf::from(root));
    for comp in path.components() {
        match comp {
            Component::ParentDir => {
                if result.parent().is_none() {
                    continue;
                }
                result.pop();
            }
            Component::CurDir => (),
            Component::RootDir | Component::Normal(_) | Component::Prefix(_) => {
                result.push(comp.as_os_str());
            }
        }
    }
    result.into()
}

impl Config {
    /// Load profiles from both environment variables and local config file,
    /// environment variables have higher precedence.
    pub fn load(fp: &Path) -> Result<Config> {
        let cfg = Config::load_from_file(fp)?;
        let profiles = Config::load_from_env().profiles.into_iter().fold(
            cfg.profiles,
            |mut acc, (name, opts)| {
                acc.entry(name).or_default().extend(opts);
                acc
            },
        );
        Ok(Config { profiles })
    }
    /// Parse a local config file.
    ///
    /// - If the config file is not present, a default Config is returned.
    pub fn load_from_file(config_path: &Path) -> Result<Config> {
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
            .fold(
                HashMap::new(),
                |mut acc: HashMap<String, HashMap<_, _>>, (profile_name, key, val)| {
                    acc.entry(profile_name).or_default().insert(key, val);
                    acc
                },
            );
        Config { profiles }
    }

    /// Parse `<profile>://abc/def` into `op` and `location`.
    pub fn parse_location(&self, s: &str) -> Result<(Operator, String)> {
        if !s.contains(":/") {
            let mut fs_builder = services::Fs::default();
            let fp = resolve_relative_path(Path::new(s));
            let fp_str = fp.as_os_str().to_string_lossy();

            let filename = match fp_str.split_once(['/', '\\']) {
                Some((base, filename)) => {
                    fs_builder = fs_builder.root(if base.is_empty() { "/" } else { base });
                    filename
                }
                _ => {
                    fs_builder = fs_builder.root(".");
                    s
                }
            };

            return Ok((Operator::new(fs_builder)?.finish(), filename.into()));
        }

        let location = Url::parse(s)?;
        if location.has_host() {
            Err(anyhow!("Host part in a location is not supported. Hint: are you typing `://` instead of `:/`?"))?;
        }

        let op = self.operator(location.scheme())?;
        let path = location.path().to_string();
        Ok((op, path))
    }

    pub fn operator(&self, profile_name: &str) -> Result<Operator> {
        let profile = self
            .profiles
            .get(profile_name)
            .ok_or_else(|| anyhow!("unknown profile: {}", profile_name))?;
        let svc = profile
            .get("type")
            .ok_or_else(|| anyhow!("missing 'type' in profile"))?;
        let scheme = Scheme::from_str(svc)?;
        Ok(Operator::via_iter(scheme, profile.clone())?)
    }
}

#[cfg(test)]
mod tests {
    use opendal::Scheme;

    use super::*;

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
        let dir = tempfile::tempdir()?;
        let tmpfile = dir.path().join("oli1.toml");
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
        let cfg = Config::load_from_file(&tmpfile)?;
        let profile = cfg.profiles["mys3"].clone();
        assert_eq!(profile["region"], "us-east-1");
        assert_eq!(profile["access_key_id"], "foo");
        assert_eq!(profile["enable_virtual_host_style"], "on");
        Ok(())
    }

    #[test]
    fn test_load_config_from_file_and_env() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let tmpfile = dir.path().join("oli2.toml");
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
        let cfg = Config::load(&tmpfile)?;
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
        struct TestCase {
            input: &'static str,
            suffix: &'static str,
        }
        let test_cases = vec![
            TestCase {
                input: "./foo/1.txt",
                suffix: "/foo/1.txt",
            },
            TestCase {
                input: "/tmp/../1.txt",
                suffix: "1.txt",
            },
            TestCase {
                input: "/tmp/../../1.txt",
                suffix: "1.txt",
            },
        ];
        let cfg = Config::default();
        for case in test_cases {
            let (op, path) = cfg.parse_location(case.input).unwrap();
            let info = op.info();
            assert_eq!(Scheme::Fs, info.scheme());
            assert_eq!("/", info.root());
            assert!(!path.starts_with('.'));
            assert!(path.ends_with(case.suffix));
        }
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
        let (op, path) = cfg.parse_location("mys3:///foo/1.txt").unwrap();
        assert_eq!("/foo/1.txt", path);
        let info = op.info();
        assert_eq!(Scheme::S3, info.scheme());
        assert_eq!("mybucket", info.name());
    }

    #[test]
    fn test_parse_s3_location2() {
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
        let (op, path) = cfg.parse_location("mys3:/foo/1.txt").unwrap();
        assert_eq!("/foo/1.txt", path);
        let info = op.info();
        assert_eq!(Scheme::S3, info.scheme());
        assert_eq!("mybucket", info.name());
    }

    #[test]
    fn test_parse_s3_location3() -> Result<()> {
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

        let uri = "mys3://foo/1.txt";
        let expected_msg = "Host part in a location is not supported.";
        match cfg.parse_location(uri) {
            Err(e) if e.to_string().contains(expected_msg) => Ok(()),
            _ => Err(anyhow!(
                "Getting an message \"{}\" is expected when parsing {}.",
                expected_msg,
                uri
            ))?,
        }
    }
}
