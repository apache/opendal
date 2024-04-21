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

use aws_config::BehaviorVersion;
use aws_config::Region;
use aws_credential_types::Credentials;
use criterion::Criterion;
use opendal::raw::tests::TEST_RUNTIME;
use opendal::services;
use opendal::Operator;
use rand::prelude::*;
use tokio::io::AsyncReadExt;

fn main() {
    let _ = dotenvy::dotenv();
    let _ = tracing_subscriber::fmt()
        .pretty()
        .with_test_writer()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();

    let endpoint = env::var("OPENDAL_S3_ENDPOINT").unwrap();
    let access_key = env::var("OPENDAL_S3_ACCESS_KEY_ID").unwrap();
    let secret_key = env::var("OPENDAL_S3_SECRET_ACCESS_KEY").unwrap();
    let bucket = env::var("OPENDAL_S3_BUCKET").unwrap();
    let region = env::var("OPENDAL_S3_REGION").unwrap();

    // Init OpenDAL Operator.
    let mut cfg = services::S3::default();
    cfg.endpoint(&endpoint);
    cfg.access_key_id(&access_key);
    cfg.secret_access_key(&secret_key);
    cfg.bucket(&bucket);
    cfg.region(&region);
    let op = Operator::new(cfg).unwrap().finish();

    // Init AWS S3 SDK.
    let mut config_loader = aws_config::defaults(BehaviorVersion::latest());
    config_loader = config_loader.endpoint_url(&endpoint);
    config_loader = config_loader.region(Region::new(region.to_string()));
    config_loader =
        config_loader.credentials_provider(Credentials::from_keys(&access_key, &secret_key, None));
    let config = TEST_RUNTIME.block_on(config_loader.load());
    let s3_client = aws_sdk_s3::Client::new(&config);

    let mut c = Criterion::default().configure_from_args();
    bench_read(&mut c, op, s3_client, &bucket);

    c.final_summary();
}

fn bench_read(c: &mut Criterion, op: Operator, s3_client: aws_sdk_s3::Client, bucket: &str) {
    let mut group = c.benchmark_group("read");
    group.throughput(criterion::Throughput::Bytes(16 * 1024 * 1024));

    TEST_RUNTIME.block_on(prepare(op.clone()));

    group.bench_function("opendal_s3_reader", |b| {
        b.to_async(&*TEST_RUNTIME).iter(|| async {
            let r = op.reader("file").await.unwrap();
            let _ = r.read(..).await.unwrap();
        });
    });
    group.bench_function("aws_s3_sdk_into_async_read", |b| {
        b.to_async(&*TEST_RUNTIME).iter(|| async {
            let mut r = s3_client
                .get_object()
                .bucket(bucket)
                .key("file")
                .send()
                .await
                .unwrap()
                .body
                .into_async_read();
            let mut bs = Vec::new();
            let _ = r.read_to_end(&mut bs).await.unwrap();
        });
    });

    group.bench_function("opendal_s3_reader_with_capacity", |b| {
        b.to_async(&*TEST_RUNTIME).iter(|| async {
            let r = op.reader("file").await.unwrap();
            let _ = r.read(..16 * 1024 * 1024).await.unwrap();
        });
    });
    group.bench_function("aws_s3_sdk_into_async_read_with_capacity", |b| {
        b.to_async(&*TEST_RUNTIME).iter(|| async {
            let mut r = s3_client
                .get_object()
                .bucket(bucket)
                .key("file")
                .send()
                .await
                .unwrap()
                .body
                .into_async_read();
            let mut bs = Vec::with_capacity(16 * 1024 * 1024);
            let _ = r.read_to_end(&mut bs).await.unwrap();
        });
    });

    group.finish()
}

async fn prepare(op: Operator) {
    let mut rng = thread_rng();
    let mut content = vec![0; 16 * 1024 * 1024];
    rng.fill_bytes(&mut content);

    op.write("file", content.clone()).await.unwrap();
}
