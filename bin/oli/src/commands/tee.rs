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
use crate::make_tokio_runtime;
use crate::params::config::ConfigParams;
use anyhow::Result;
use futures::AsyncWriteExt;
use tokio::io::AsyncReadExt as TokioAsyncReadExt;
use tokio::io::AsyncWriteExt as TokioAsyncWriteExt;
#[derive(Debug, clap::Parser)]
#[command(
    name = "tee",
    about = "Read from standard input and write to destination and stdout",
    disable_version_flag = true
)]
pub struct TeeCmd {
    #[command(flatten)]
    pub config_params: ConfigParams,
    #[arg()]
    pub destination: String,
}

impl TeeCmd {
    pub fn run(self) -> Result<()> {
        make_tokio_runtime(1).block_on(self.do_run())
    }

    async fn do_run(self) -> Result<()> {
        let cfg = Config::load(&self.config_params.config)?;

        let (dst_op, dst_path) = cfg.parse_location(&self.destination)?;

        let mut writer = dst_op.writer(&dst_path).await?.into_futures_async_write();
        let mut stdout = tokio::io::stdout();

        let mut buf = vec![0; 8 * 1024 * 1024]; // 8MB buffer

        let mut stdin = tokio::io::stdin();
        loop {
            let n = stdin.read(&mut buf).await?;
            if n == 0 {
                break;
            }

            // Write to destination
            writer.write_all(&buf[..n]).await?;
            // Write to stdout
            stdout.write_all(&buf[..n]).await?;
        }

        writer.close().await?;
        stdout.flush().await?;

        Ok(())
    }
}
