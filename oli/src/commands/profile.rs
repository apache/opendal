// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::env;
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;

use opendal::Operator;

const OLI_PROFILE_PREFIX: &str = "OLI_PROFILE_";

pub struct OliProfileGroup {
    pub name: String,
    pub typ: String,
    pub values: HashMap<String, String>,
}

impl OliProfileGroup {
    pub fn parse(name: &str, kvs: HashMap<String, String>) -> Result<Self> {
        if let Some(typ) = kvs.get("type") {
            Ok(OliProfileGroup {
                name: name.to_string(),
                typ: typ.to_string(),
                values: kvs,
            })
        } else {
            Err(anyhow!("oli profile must contains type"))
        }
    }
}

pub(crate) fn build_operators() -> Result<HashMap<String, Operator>> {
    let profiles = get_oli_profiles_from_env();
    let groups = parse_oli_profile_groups(profiles)?;

    let mut ret = HashMap::new();
    for group in groups {
        let operator = build_operator(&group)?;
        ret.insert(group.name, operator);
    }
    Ok(ret)
}

fn get_oli_profiles_from_env() -> Vec<(String, String)> {
    env::vars()
        .filter(|(k, _)| k.starts_with(OLI_PROFILE_PREFIX))
        .collect::<Vec<(String, String)>>()
}

fn parse_oli_profile_groups(profiles: Vec<(String, String)>) -> Result<Vec<OliProfileGroup>> {
    let mut groups: HashMap<String, HashMap<String, String>> = HashMap::new();
    for (key, value) in profiles {
        let (group, trimmed_key) = parse_oli_profile(key)?;
        if !groups.contains_key(&group) {
            groups.insert(group.clone(), HashMap::new());
        }
        groups
            .get_mut(&group)
            .unwrap()
            .insert(trimmed_key.to_lowercase(), value);
    }

    let mut rets = Vec::new();
    for (name, kvs) in groups {
        let group = OliProfileGroup::parse(&name, kvs)?;
        rets.push(group);
    }

    Ok(rets)
}

fn parse_oli_profile(key: String) -> Result<(String, String)> {
    let name_value = key
        .strip_prefix(OLI_PROFILE_PREFIX)
        .ok_or_else(|| anyhow!("invalid oli profile prefix"))?
        .split_once('_');

    match name_value {
        Some((name, value)) => Ok((name.to_string(), value.to_string())),
        None => Err(anyhow!("split name and value error")),
    }
}

fn build_operator(group: &OliProfileGroup) -> Result<Operator> {
    let scheme = opendal::Scheme::from_str(&group.typ)?;
    let iter = group.values.iter().map(|(k, v)| (k.clone(), v.clone()));
    Ok(Operator::from_iter(scheme, iter)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_operators() -> Result<()> {
        assert_eq!("azblob", opendal::Scheme::Azblob.to_string());
        env::set_var("OLI_PROFILE_SRC_TYPE", "azblob");
        env::set_var("OLI_PROFILE_SRC_ENDPOINT", "endpoint_value");
        env::set_var("OLI_PROFILE_SRC_CONTAINER", "container_value");
        env::set_var("OLI_PROFILE_SRC_ROOT", "/");
        env::set_var("OLI_PROFILE_DST_TYPE", "azblob");
        env::set_var("OLI_PROFILE_DST_ENDPOINT", "endpoint_value");
        env::set_var("OLI_PROFILE_DST_CONTAINER", "container_value");
        env::set_var("OLI_PROFILE_DST_ROOT", "/");
        let operators = build_operators()?;
        assert_eq!(2, operators.len());
        assert!(operators.contains_key("SRC"));
        assert!(operators.contains_key("DST"));
        Ok(())
    }

    #[test]
    fn test_build_operator() -> Result<()> {
        assert_eq!("azblob", opendal::Scheme::Azblob.to_string());
        env::set_var("OLI_PROFILE_SRC_TYPE", "azblob");
        env::set_var("OLI_PROFILE_SRC_ENDPOINT", "endpoint_value");
        env::set_var("OLI_PROFILE_SRC_CONTAINER", "container_value");
        env::set_var("OLI_PROFILE_SRC_ROOT", "/");
        let envs = get_oli_profiles_from_env();
        let groups = parse_oli_profile_groups(envs)?;
        let _operator = build_operator(groups.get(0).unwrap())?;
        Ok(())
    }

    #[test]
    fn test_parse_oli_profile_groups() -> Result<()> {
        env::set_var("OLI_PROFILE_SRC_TYPE", "S3");
        env::set_var("OLI_PROFILE_SRC_ENDPOINT", "http://127.0.0.1");
        let envs = get_oli_profiles_from_env();
        let groups = parse_oli_profile_groups(envs)?;
        assert_eq!(1, groups.len());
        Ok(())
    }
}
