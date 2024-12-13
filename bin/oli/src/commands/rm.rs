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

use anyhow::Result;

use crate::config::Config;
use crate::params::config::ConfigParams;

#[derive(Debug, clap::Parser)]
#[command(name = "rm", about = "Remove object", disable_version_flag = true)]
pub struct RmCmd {
    #[command(flatten)]
    pub config_params: ConfigParams,
    /// In the form of `<profile>:/<path>`.
    #[arg()]
    pub target: String,
    /// Remove objects recursively.
    #[arg(short, long)]
    pub recursive: bool,
}

impl RmCmd {
    pub async fn run(&self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;

        let (op, path) = cfg.parse_location(&self.target)?;

        if !self.recursive {
            println!("Delete: {path}");
            op.delete(&path).await?;
            return Ok(());
        }

        println!("Delete all: {path}");
        op.remove_all(&path).await?;
        Ok(())
    }
}
