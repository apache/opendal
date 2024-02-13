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
use std::str::FromStr;

use anyhow::anyhow;
use anyhow::Result;
use frontend::Frontend;
use frontend::FrontendArgs;
use opendal::Operator;
use opendal::Scheme;

pub mod config;
pub use config::Config;

mod frontend;

pub async fn new_app(cfg: Config) -> Result<()> {
    if cfg.backend.has_host() {
        log::warn!("backend host will be ignored");
    }

    let scheme_str = cfg.backend.scheme();
    let op_args = cfg
        .backend
        .query_pairs()
        .into_owned()
        .collect::<HashMap<String, String>>();

    let scheme = match Scheme::from_str(scheme_str) {
        Ok(Scheme::Custom(_)) | Err(_) => Err(anyhow!("invalid scheme: {}", scheme_str)),
        Ok(s) => Ok(s),
    }?;
    let backend = Operator::via_map(scheme, op_args)?;

    let args = FrontendArgs {
        mount_path: cfg.mount_path,
        backend,
    };
    Frontend::execute(args).await
}
