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
use opendal::Result;
use opendal::{services, Operator};

use opendal::layers::*;

#[tokio::main]
async fn main() -> Result<()> {
    let builder = services::Memory::default();
    let registry = prometheus::default_registry();

    let op = Operator::new(builder)
        .expect("must init")
        .layer(PrometheusLayer::with_registry(registry.clone()).enable_path_label(usize::MAX))
        .layer(ConcurrentLimitLayer::new(1024))
        .layer(ImmutableIndexLayer::default())
        .layer(LoggingLayer::default())
        .layer(TimeoutLayer::default())
        .layer(MetricsLayer)
        .layer(RetryLayer::default())
        .layer(TracingLayer)
        .layer(MinitraceLayer)
        .layer(ThrottleLayer::new(1024, 1025))
        .finish();
    debug!("operator: {op:?}");

    // Write data into object test.
    op.write("test/abc/bcd", "Hello, World!").await?;
    // Read data from object.
    let bs = op.read("test/abc/bcd").await?;
    info!("content: {}", String::from_utf8_lossy(&bs));

    // Get object metadata.
    let meta = op.stat("test/abc/bcd").await?;
    info!("meta: {:?}", meta);

    Ok(())
}
