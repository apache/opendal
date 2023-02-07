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

use anyhow::{anyhow, Result};
use opendal::services;
use opendal::{Operator, Scheme};
use std::collections::HashMap;
use std::env;
use std::str::FromStr;

/// Parse `s3://abc/def` into `op` and `location`.
pub fn parse_location(s: &str) -> Result<(Operator, &str)> {
    if !s.contains("://") {
        let mut fs = services::Fs::default();

        let filename = match s.rsplit_once(['/', '\\']) {
            Some((base, filename)) => {
                fs.root(base);
                filename
            }
            None => s,
        };

        return Ok((Operator::create(fs)?.finish(), filename));
    }

    let s = s.splitn(2, "://").collect::<Vec<_>>();
    debug_assert!(s.len() == 2);
    Ok((parse_profile(s[0])?, s[1]))
}

/// If name is a valid scheme, we will load from env directly.
/// Or, we will try to get env from `OLI_PROFILE_{NAME}_XXX`.
///
/// Especially, the type is specified by `OLI_PROFILE_{NAME}_TYPE`
pub fn parse_profile(name: &str) -> Result<Operator> {
    let prefix = format!("OLI_PROFILE_{name}_").to_lowercase();
    let cfg = env::vars()
        .filter_map(|(k, v)| {
            k.to_lowercase()
                .strip_prefix(&prefix)
                .map(|k| (k.to_string(), v))
        })
        .collect::<HashMap<String, String>>();

    let typ = cfg
        .get("type")
        .ok_or_else(|| anyhow!("type for profile {} is not specified", name))?;

    let scheme = Scheme::from_str(typ)?;

    let op = match scheme {
        Scheme::Fs => Operator::from_iter::<services::Fs>(cfg.into_iter())?.finish(),
        _ => unimplemented!(),
    };

    Ok(op)
}
