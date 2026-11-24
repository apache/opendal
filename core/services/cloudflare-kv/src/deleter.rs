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

use std::sync::Arc;

use bytes::Buf;
use http::StatusCode;
use opendal_core::raw::oio::BatchDeleteResult;
use opendal_core::raw::*;
use opendal_core::*;

use super::core::parse_error;
use super::core::*;
use super::model::CfKvDeleteResponse;

pub struct CloudflareKvDeleter {
    core: Arc<CloudflareKvCore>,
    ctx: OperationContext,
}

impl CloudflareKvDeleter {
    pub fn new(core: Arc<CloudflareKvCore>, ctx: OperationContext) -> Self {
        Self { core, ctx }
    }
}

impl oio::BatchDelete for CloudflareKvDeleter {
    async fn delete_once(&self, path: String, _: OpDelete) -> Result<()> {
        let path = build_absolute_path(&self.core.info.root(), &path);
        let key = path.trim_end_matches('/').to_string();
        let resp = self
            .core
            .delete(&self.ctx, std::slice::from_ref(&key))
            .await?;

        let status = resp.status();

        if status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("BulkDelete")),
                resp,
            ));
        }

        let bs = resp.clone().into_body();
        let res: CfKvDeleteResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        if !res.success {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("BulkDelete")),
                resp,
            ));
        }

        if let Some(result) = res.result
            && result.unsuccessful_keys.iter().any(|failed| failed == &key)
        {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "cloudflare_kv delete this key failed for reason we don't know",
            ));
        }

        Ok(())
    }

    async fn delete_batch(&self, batch: Vec<(String, OpDelete)>) -> Result<BatchDeleteResult> {
        let keys = batch
            .iter()
            .map(|path| {
                let path = build_absolute_path(&self.core.info.root(), &path.0);
                path.trim_end_matches('/').to_string()
            })
            .collect::<Vec<String>>();

        let resp = self.core.delete(&self.ctx, &keys).await?;

        let status = resp.status();

        if status != StatusCode::OK {
            return Err(parse_error(
                ErrorContext::new(ServiceOperation("BulkDelete")),
                resp,
            ));
        }

        let bs = resp.into_body();

        let res: CfKvDeleteResponse =
            serde_json::from_reader(bs.reader()).map_err(new_json_deserialize_error)?;

        let result = match (res.success, res.result) {
            (true, Some(result)) => result,
            _ => {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    "cloudflare_kv delete this key failed for reason we don't know",
                ));
            }
        };

        let mut batched_result = BatchDeleteResult {
            // `successful_key_count` is taken verbatim from the server response; cap it to
            // the number of keys we actually requested so a malicious/compromised endpoint
            // cannot drive `Vec::with_capacity` to an abort/OOM. (`unsuccessful_keys` is an
            // already-parsed Vec, so its length is inherently bounded.)
            succeeded: Vec::with_capacity(result.successful_key_count.min(batch.len())),
            failed: Vec::with_capacity(result.unsuccessful_keys.len().min(batch.len())),
        };

        for (item, key) in batch.into_iter().zip(keys.iter()) {
            if result.unsuccessful_keys.contains(key) {
                batched_result.failed.push((
                    item.0,
                    item.1,
                    Error::new(
                        ErrorKind::Unexpected,
                        "cloudflare_kv delete this key failed for reason we don't know",
                    ),
                ));
            } else {
                batched_result.succeeded.push(item);
            }
        }

        Ok(batched_result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::time::Duration;

    use futures::stream;
    use http::Response;
    use opendal_core::raw::oio::BatchDelete;

    use super::*;
    use crate::CLOUDFLARE_KV_SCHEME;

    struct DeleteTransport {
        sent_bodies: Arc<Mutex<Vec<Vec<String>>>>,
        response_body: String,
    }

    impl HttpTransport for DeleteTransport {
        async fn fetch(&self, req: http::Request<Buffer>) -> Result<Response<HttpBody>> {
            let sent: Vec<String> =
                serde_json::from_slice(&req.into_body().to_bytes()).expect("valid JSON key list");
            self.sent_bodies.lock().unwrap().push(sent);

            let body = Buffer::from(self.response_body.clone());
            let len = body.len() as u64;
            Response::builder()
                .status(StatusCode::OK)
                .body(HttpBody::new(stream::iter([Ok(body)]), Some(len)))
                .map_err(new_request_build_error)
        }
    }

    fn test_core(root: &str) -> Arc<CloudflareKvCore> {
        Arc::new(CloudflareKvCore {
            api_token: "Bearer test-token".to_string(),
            account_id: "acc123".to_string(),
            namespace_id: "ns456".to_string(),
            expiration_ttl: None,
            info: ServiceInfo::new(CLOUDFLARE_KV_SCHEME, root, ""),
            capability: Capability::default(),
        })
    }

    #[tokio::test]
    async fn delete_batch_matches_unsuccessful_keys_with_root_and_trailing_slash() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let sent_bodies = Arc::new(Mutex::new(Vec::new()));
            let transport = DeleteTransport {
                sent_bodies: sent_bodies.clone(),
                response_body: r#"{
                    "success": true,
                    "result": {
                        "successful_key_count": 1,
                        "unsuccessful_keys": ["prefix/a", "prefix/dir"]
                    }
                }"#
                .to_string(),
            };
            let ctx =
                OperationContext::default().with_http_transport(HttpTransporter::new(transport));
            let deleter = CloudflareKvDeleter::new(test_core("/prefix/"), ctx);

            let res = deleter
                .delete_batch(vec![
                    ("a".to_string(), OpDelete::default()),
                    ("dir/".to_string(), OpDelete::default()),
                    ("ok".to_string(), OpDelete::default()),
                ])
                .await
                .unwrap();

            assert_eq!(
                *sent_bodies.lock().unwrap(),
                vec![vec![
                    "prefix/a".to_string(),
                    "prefix/dir".to_string(),
                    "prefix/ok".to_string()
                ]]
            );

            let succeeded: Vec<&str> = res.succeeded.iter().map(|(p, _)| p.as_str()).collect();
            let failed: Vec<&str> = res.failed.iter().map(|(p, _, _)| p.as_str()).collect();
            assert_eq!(succeeded, vec!["ok"]);
            assert_eq!(failed, vec!["a", "dir/"]);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn delete_once_fails_when_key_is_in_unsuccessful_keys() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let sent_bodies = Arc::new(Mutex::new(Vec::new()));
            let transport = DeleteTransport {
                sent_bodies: sent_bodies.clone(),
                response_body: r#"{
                    "success": true,
                    "result": {
                        "successful_key_count": 0,
                        "unsuccessful_keys": ["prefix/dir"]
                    }
                }"#
                .to_string(),
            };
            let ctx =
                OperationContext::default().with_http_transport(HttpTransporter::new(transport));
            let deleter = CloudflareKvDeleter::new(test_core("/prefix/"), ctx);

            let err = deleter
                .delete_once("dir/".to_string(), OpDelete::default())
                .await
                .unwrap_err();
            assert_eq!(err.kind(), ErrorKind::Unexpected);
            assert_eq!(
                *sent_bodies.lock().unwrap(),
                vec![vec!["prefix/dir".to_string()]]
            );
        })
        .await
        .unwrap();
    }
}
