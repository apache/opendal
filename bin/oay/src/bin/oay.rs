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

use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use oay::services::S3Service;
use oay::services::WebdavService;
use oay::Config;
use opendal::services::Fs;
use opendal::Operator;
use opendal::Scheme;

#[tokio::main]
async fn main() -> Result<()> {
    let _ = s3().await;
    webdav().await
}

async fn s3() -> Result<()> {
    logforth::stderr().apply();

    let cfg: Config =
        toml::from_str(&std::fs::read_to_string("oay.toml").context("failed to open oay.toml")?)?;
    let scheme = Scheme::from_str(&cfg.backend.typ).context("unsupported scheme")?;
    let op = Operator::via_iter(scheme, cfg.backend.map.clone())?;

    let s3 = S3Service::new(Arc::new(cfg), op);

    s3.serve().await?;

    Ok(())
}

async fn webdav() -> Result<()> {
    let cfg: Config = Config {
        backend: oay::BackendConfig {
            typ: "fs".to_string(),
            ..Default::default()
        },
        frontends: oay::FrontendsConfig {
            webdav: oay::WebdavConfig {
                enable: true,
                addr: "127.0.0.1:3000".to_string(),
            },
            ..Default::default()
        },
    };

    let builder = Fs::default().root("/tmp");

    let op = Operator::new(builder)?.finish();

    let webdav = WebdavService::new(Arc::new(cfg), op);

    webdav.serve().await?;

    Ok(())
}
