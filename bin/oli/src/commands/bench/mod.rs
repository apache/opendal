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

use crate::config::Config;
use crate::params::config::ConfigParams;
use anyhow::Result;
use std::path::PathBuf;

mod report;
mod suite;

#[derive(Debug, clap::Parser)]
#[command(
    name = "bench",
    about = "Run benchmark against the storage backend",
    disable_version_flag = true
)]
pub struct BenchCmd {
    #[command(flatten)]
    pub config_params: ConfigParams,
    /// Name of the profile to use.
    #[arg()]
    pub profile: String,
    /// Path to the benchmark config.
    #[arg(
        value_parser = clap::value_parser!(PathBuf),
    )]
    pub bench: PathBuf,
}

impl BenchCmd {
    pub async fn run(self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;
        let op = cfg.operator(&self.profile)?;
        let suite = suite::BenchSuite::load(&self.bench)?;

        tokio::task::spawn_blocking(move || {
            suite.run(op).expect("failed to run bench suite");
        })
        .await?;
        Ok(())
    }
}
