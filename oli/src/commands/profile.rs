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

pub(crate) struct OliProfileGroup {
    pub _name: String,
    pub typ: String,
    pub values: HashMap<String, String>,
}

impl OliProfileGroup {
    pub(crate) fn parse_from_env(group: &str) -> Result<Self> {
        let prefix = group_prefix(group);
        let profiles = env::vars()
            .filter_map(|(k, v)| k.strip_prefix(&prefix).map(|k| (k.to_lowercase(), v)))
            .collect::<HashMap<String, String>>();

        match profiles.get("type") {
            Some(typ) => Ok(OliProfileGroup {
                _name: group.to_string(),
                typ: typ.to_string(),
                values: profiles,
            }),
            None => Err(anyhow!("oli profile must contains type")),
        }
    }

    pub(crate) fn build_operator(&self) -> Result<Operator> {
        let scheme = opendal::Scheme::from_str(&self.typ)?;
        let iter = self.values.iter().map(|(k, v)| (k.clone(), v.clone()));
        Ok(Operator::from_iter(scheme, iter)?)
    }
}

fn group_prefix(group: &str) -> String {
    format!("{}{}_", OLI_PROFILE_PREFIX, group.to_uppercase())
}

pub(crate) enum CopyPath {
    File(String),
    Profile((String, String)),
}

impl CopyPath {
    pub(crate) fn path(&self) -> String {
        match &self {
            CopyPath::File(path) => path.to_string(),
            CopyPath::Profile((_, path)) => path.to_string(),
        }
    }
}

impl FromStr for CopyPath {
    type Err = anyhow::Error;

    fn from_str(cp_path: &str) -> Result<Self, Self::Err> {
        match cp_path.split_once("://") {
            Some((name, path)) => Ok(Self::Profile((name.to_string(), path.to_string()))),
            _ => Ok(Self::File(cp_path.to_string())),
        }
    }
}

pub(crate) fn build_operator(path: &CopyPath) -> Result<Operator> {
    match path {
        CopyPath::File(..) => {
            let fs_backend = opendal::services::fs::Builder::default().build()?;
            Ok(opendal::Operator::new(fs_backend))
        }
        CopyPath::Profile((group, ..)) => OliProfileGroup::parse_from_env(group)?.build_operator(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_operator() -> Result<()> {
        assert_eq!("azblob", opendal::Scheme::Azblob.to_string());
        env::set_var("OLI_PROFILE_SRC_TYPE", "azblob");
        env::set_var("OLI_PROFILE_SRC_ENDPOINT", "endpoint_value");
        env::set_var("OLI_PROFILE_SRC_CONTAINER", "container_value");
        env::set_var("OLI_PROFILE_SRC_ROOT", "/");
        let group = OliProfileGroup::parse_from_env("src")?;
        assert_eq!("src", group._name);
        assert_eq!("azblob", group.typ);
        let _ = group.build_operator()?;
        Ok(())
    }
}
