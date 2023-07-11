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
use oay::Config;
use opendal::Operator;
use opendal::Scheme;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer().pretty())
        .with(EnvFilter::from_default_env())
        .init();

    let cfg: Config =
        toml::from_str(&std::fs::read_to_string("oay.toml").context("failed to open oay.toml")?)?;
    let scheme = Scheme::from_str(&cfg.backend.typ).context("unsupported scheme")?;
    let op = Operator::via_map(scheme, cfg.backend.map.clone())?;

    let s3 = S3Service::new(Arc::new(cfg), op);

    s3.serve().await?;

    Ok(())
}
