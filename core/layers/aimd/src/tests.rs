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

use std::time::Duration;

use futures::{executor::block_on, poll, stream};
use http::{Request, Response};
use opendal_core::raw::oio::ReadStream;
use opendal_core::*;

use super::*;

#[test]
fn invalid_configuration_is_rejected() {
    let mut configs = vec![
        AimdConfig {
            initial_rate: 0.0,
            ..Default::default()
        },
        AimdConfig {
            min_rate: -1.0,
            ..Default::default()
        },
        AimdConfig {
            initial_rate: 6000.0,
            ..Default::default()
        },
        AimdConfig {
            min_rate: 3000.0,
            ..Default::default()
        },
        AimdConfig {
            decrease_factor: 1.0,
            ..Default::default()
        },
        AimdConfig {
            additive_increment: 0.0,
            ..Default::default()
        },
        AimdConfig {
            burst: 0,
            ..Default::default()
        },
        AimdConfig {
            window: Duration::ZERO,
            ..Default::default()
        },
        AimdConfig {
            window: Duration::MAX,
            ..Default::default()
        },
        AimdConfig {
            min_rate: f64::MIN_POSITIVE,
            ..Default::default()
        },
    ];
    for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        configs.extend([
            AimdConfig {
                initial_rate: value,
                ..Default::default()
            },
            AimdConfig {
                min_rate: value,
                ..Default::default()
            },
            AimdConfig {
                max_rate: value,
                ..Default::default()
            },
            AimdConfig {
                additive_increment: value,
                ..Default::default()
            },
            AimdConfig {
                decrease_factor: value,
                ..Default::default()
            },
        ]);
    }
    for config in configs {
        assert_eq!(
            AimdLayer::new(config).unwrap_err().kind(),
            ErrorKind::ConfigInvalid
        );
    }
}

fn layer() -> AimdLayer {
    AimdLayer::new(AimdConfig {
        initial_rate: 0.001,
        min_rate: 0.001,
        max_rate: 100.0,
        additive_increment: 10.0,
        burst: 1,
        window: Duration::from_secs(3600),
        ..Default::default()
    })
    .unwrap()
    .with_sleep(|_| std::future::pending::<()>())
}

#[derive(Clone, Default)]
struct CountingTransport(Arc<std::sync::atomic::AtomicUsize>);

impl HttpTransport for CountingTransport {
    async fn fetch(&self, _: Request<Buffer>) -> Result<Response<HttpBody>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Response::new(HttpBody::new(
            stream::iter([Ok(Buffer::from("a")), Ok(Buffer::from("b"))]),
            Some(2),
        )))
    }
}

#[test]
fn unmarked_and_presign_requests_bypass_admission() {
    block_on(async {
        let layer = layer();
        let counting = CountingTransport::default();
        let transport = AimdTransport {
            inner: HttpTransporter::new(counting.clone()),
            buckets: layer.buckets.clone(),
            sleep: layer.sleep.clone(),
        };
        layer.buckets.read.acquire(&*layer.sleep).await;
        layer.buckets.write.acquire(&*layer.sleep).await;
        layer.buckets.delete.acquire(&*layer.sleep).await;
        layer.buckets.list.acquire(&*layer.sleep).await;
        transport.fetch(Request::new(Buffer::new())).await.unwrap();
        transport
            .fetch(
                Request::builder()
                    .extension(Operation::Presign)
                    .body(Buffer::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(counting.0.load(std::sync::atomic::Ordering::Relaxed), 2);
    });
}

#[test]
fn every_operation_uses_its_expected_budget() {
    block_on(async {
        let layer = layer();
        let transport = AimdTransport {
            inner: HttpTransporter::new(CountingTransport::default()),
            buckets: layer.buckets.clone(),
            sleep: layer.sleep.clone(),
        };
        for (bucket, operations) in [
            (&layer.buckets.read, vec![Operation::Read, Operation::Stat]),
            (
                &layer.buckets.write,
                vec![
                    Operation::Write,
                    Operation::Copy,
                    Operation::Compose,
                    Operation::Rename,
                    Operation::Restore,
                    Operation::CreateDir,
                ],
            ),
            (&layer.buckets.delete, vec![Operation::Delete]),
            (&layer.buckets.list, vec![Operation::List]),
        ] {
            bucket.acquire(&*layer.sleep).await;
            for operation in operations {
                let req = Request::builder()
                    .extension(operation)
                    .body(Buffer::new())
                    .unwrap();
                let mut pending = Box::pin(transport.fetch(req));
                assert!(
                    poll!(&mut pending).is_pending(),
                    "{operation} must use this budget"
                );
            }
        }
    });
}

#[test]
fn memory_operations_are_not_paced() {
    block_on(async {
        let layer = layer();
        layer.buckets.read.acquire(&*layer.sleep).await;
        layer.buckets.write.acquire(&*layer.sleep).await;
        let op = Operator::new(opendal_core::services::Memory::default())
            .unwrap()
            .layer(layer);
        op.write("file", "value").await.unwrap();
        assert_eq!(op.read("file").await.unwrap().to_bytes().as_ref(), b"value");
    });
}

#[test]
fn cloned_layers_and_context_replay_share_request_admission() {
    block_on(async {
        let sleeps = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let calls = sleeps.clone();
        let layer = layer().with_sleep(move |duration| {
            assert!(duration > Duration::ZERO);
            calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            std::future::pending::<()>()
        });
        let counting = CountingTransport::default();
        let context =
            OperationContext::new().with_http_transport(HttpTransporter::new(counting.clone()));
        let first = Operator::new(opendal_core::services::Memory::default())
            .unwrap()
            .with_context(context.clone())
            .layer(layer.clone());
        let second = Operator::new(opendal_core::services::Memory::default())
            .unwrap()
            .layer(layer.clone())
            .with_context(context.clone());
        let mut response = first
            .context()
            .http_transport()
            .fetch(
                Request::builder()
                    .extension(Operation::Read)
                    .body(Buffer::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response
                .body_mut()
                .read_all()
                .await
                .unwrap()
                .to_bytes()
                .as_ref(),
            b"ab"
        );

        let mut blocked = Box::pin(
            second.context().http_transport().fetch(
                Request::builder()
                    .extension(Operation::Read)
                    .body(Buffer::new())
                    .unwrap(),
            ),
        );
        assert!(poll!(&mut blocked).is_pending());
        drop(blocked);

        let replayed = first.with_context(context);
        let mut blocked = Box::pin(
            replayed.context().http_transport().fetch(
                Request::builder()
                    .extension(Operation::Read)
                    .body(Buffer::new())
                    .unwrap(),
            ),
        );
        assert!(
            poll!(&mut blocked).is_pending(),
            "replay must not reset the bucket"
        );
        assert_eq!(counting.0.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert_eq!(sleeps.load(std::sync::atomic::Ordering::Relaxed), 2);
    });
}

#[tokio::test]
async fn default_timer_wakes_request_admission() {
    let start = std::time::Instant::now();
    let layer = AimdLayer::new(AimdConfig {
        initial_rate: 100.0,
        max_rate: 100.0,
        burst: 1,
        ..Default::default()
    })
    .unwrap();
    let counting = CountingTransport::default();
    let context =
        OperationContext::new().with_http_transport(HttpTransporter::new(counting.clone()));
    let op = Operator::new(opendal_core::services::Memory::default())
        .unwrap()
        .with_context(context)
        .layer(layer);
    for _ in 0..2 {
        op.context()
            .http_transport()
            .fetch(
                Request::builder()
                    .extension(Operation::Read)
                    .body(Buffer::new())
                    .unwrap(),
            )
            .await
            .unwrap();
    }
    assert!(start.elapsed() >= Duration::from_millis(10));
    assert_eq!(counting.0.load(std::sync::atomic::Ordering::Relaxed), 2);
}
