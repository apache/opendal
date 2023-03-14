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

use log::debug;
use log::info;
use opendal::layers::LoggingLayer;
use opendal::services;
use opendal::Operator;
use opendal::Result;

/// Visit [`opendal::services`] for more service related config.
/// Visit [`opendal::Object`] for more object level APIs.
#[tokio::main]
async fn main() -> Result<()> {
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .try_init();

    // Pick a builder and configure it.
    let builder = services::Memory::default();

    // Init an operator
    let op = Operator::new(builder)?
        // Init with logging layer enabled.
        .layer(LoggingLayer::default())
        .finish();

    debug!("operator: {op:?}");

    // Write data into object test.
    op.write("test", "Hello, World!").await?;

    // Read data from object.
    let bs = op.read("test").await?;
    info!("content: {}", String::from_utf8_lossy(&bs));

    // Get object metadata.
    let meta = op.stat("test").await?;
    info!("meta: {:?}", meta);

    // Have fun!

    Ok(())
}
