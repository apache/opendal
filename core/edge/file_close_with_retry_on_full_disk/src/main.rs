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

use std::env;

use opendal::layers::LoggingLayer;
use opendal::layers::RetryLayer;
use opendal::services::Fs;
use opendal::Operator;
use opendal::Result;
use rand::prelude::*;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let mut builder = Fs::default();
    builder.root(&env::var("OPENDAL_FS_ROOT").expect("root must be set for this test"));
    let op = Operator::new(builder)?
        .layer(LoggingLayer::default())
        .layer(RetryLayer::new().with_max_times(3))
        .finish();

    let size = thread_rng().gen_range(512 * 1024 + 1..4 * 1024 * 1024);
    let mut bs = vec![0; size];
    thread_rng().fill_bytes(&mut bs);

    let mut w = op.writer("/test").await?;
    w.write(bs).await?;
    let result = w.close().await;
    // Write file with size > 512KB should fail
    assert!(
        result.is_err(),
        "close file with retry on full disk should return error"
    );

    Ok(())
}
