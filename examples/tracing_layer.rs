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

use std::error::Error;

use anyhow::Result;
use opendal::layers::TracingLayer;
use opendal::Operator;
use opendal::Scheme;
use opentelemetry::global;
use tracing::span;
use tracing_subscriber::prelude::*;
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_service_name("opendal_example")
        .install_simple()?;
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(opentelemetry)
        .try_init()?;

    let runtime = tokio::runtime::Runtime::new()?;

    runtime.block_on(async {
        let root = span!(tracing::Level::INFO, "app_start", work_units = 2);
        let _enter = root.enter();

        let _ = dotenvy::dotenv();
        let op = Operator::from_env(Scheme::S3)
            .expect("init operator must succeed")
            .layer(TracingLayer);

        op.object("test")
            .write("0".repeat(16 * 1024 * 1024).into_bytes())
            .await
            .expect("must succeed");
        op.object("test").metadata().await.expect("must succeed");
        op.object("test").read().await.expect("must succeed");
    });

    // Shut down the current tracer provider. This will invoke the shutdown
    // method on all span processors. span processors should export remaining
    // spans before return.
    global::shutdown_tracer_provider();
    Ok(())
}
