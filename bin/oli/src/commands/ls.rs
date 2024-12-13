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
use futures::TryStreamExt;

use crate::config::Config;
use crate::params::config::ConfigParams;

#[derive(Debug, clap::Parser)]
#[command(name = "ls", about = "List object", disable_version_flag = true)]
pub struct LsCmd {
    #[command(flatten)]
    pub config_params: ConfigParams,
    /// In the form of `<profile>:/<path>`.
    #[arg()]
    pub target: String,
    /// List objects recursively.
    #[arg(short, long)]
    pub recursive: bool,
}

impl LsCmd {
    pub async fn run(&self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;

        let (op, path) = cfg.parse_location(&self.target)?;

        if !self.recursive {
            let mut ds = op.lister(&path).await?;
            while let Some(de) = ds.try_next().await? {
                println!("{}", de.name());
            }
            return Ok(());
        }

        let mut ds = op.lister_with(&path).recursive(true).await?;
        while let Some(de) = ds.try_next().await? {
            println!("{}", de.path());
        }
        Ok(())
    }
}
