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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use aws_sdk_s3;
use aws_sdk_s3::output::ListObjectsV2Output;
use futures::future::BoxFuture;
use futures::ready;

use super::error::parse_unexpect_error;
use super::Backend;
use crate::error::Result;
use crate::Object;
use crate::ObjectMode;

pub struct S3ObjectStream {
    backend: Backend,
    bucket: String,
    path: String,

    token: String,
    done: bool,
    // s3 list_object futures.
    list_object_future: Option<BoxFuture<'static, Result<ListObjectsV2Output>>>,
    // s3 prefixes.
    common_prefixes: Vec<aws_sdk_s3::model::CommonPrefix>,
    common_prefixes_idx: usize,
    // s3 objects.
    objects: Vec<aws_sdk_s3::model::Object>,
    objects_idx: usize,
}

impl S3ObjectStream {
    pub fn new(backend: Backend, bucket: String, path: String) -> Self {
        Self {
            backend,
            bucket,
            path,

            token: "".to_string(),
            done: false,
            list_object_future: None,
            common_prefixes: vec![],
            common_prefixes_idx: 0,
            objects: vec![],
            objects_idx: 0,
        }
    }
}

impl futures::Stream for S3ObjectStream {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.common_prefixes_idx < self.common_prefixes.len() {
            self.common_prefixes_idx += 1;
            let prefix = &self.common_prefixes[self.common_prefixes_idx - 1].prefix();

            let mut o = Object::new(
                Arc::new(self.backend.clone()),
                &self
                    .backend
                    .get_rel_path(prefix.expect("prefix should not be None")),
            );
            let meta = o.metadata_mut();
            meta.set_mode(ObjectMode::DIR)
                .set_content_length(0)
                .set_complete();

            return Poll::Ready(Some(Ok(o)));
        }
        if self.objects_idx < self.objects.len() {
            self.objects_idx += 1;
            let object = &self.objects[self.objects_idx - 1];

            let mut o = Object::new(
                Arc::new(self.backend.clone()),
                &self
                    .backend
                    .get_rel_path(object.key().expect("key should not be None")),
            );
            let meta = o.metadata_mut();
            meta.set_mode(ObjectMode::FILE)
                .set_content_length(object.size as u64);

            return Poll::Ready(Some(Ok(o)));
        }

        if let Some(fut) = &mut self.list_object_future {
            let output = ready!(Pin::new(fut).poll(cx))?;
            // Set future to None once it's ready.
            self.list_object_future = None;
            // Set done to true once response returns is_truncated=false;
            self.done = !output.is_truncated;
            self.token = output.next_continuation_token.unwrap_or_default();
            self.common_prefixes_idx = 0;
            self.common_prefixes = output.common_prefixes.unwrap_or_default();
            self.objects_idx = 0;
            self.objects = output.contents.unwrap_or_default();

            // Make sure all common_prefixes and objects are consumed.
            if !self.common_prefixes.is_empty() || !self.objects.is_empty() {
                return self.poll_next(cx);
            }
        }

        if self.done {
            return Poll::Ready(None);
        }

        let client = self.backend.inner();
        let bucket = self.bucket.clone();
        let path = self.path.clone();
        let token = self.token.clone();
        let fut = async move {
            let mut req = client
                .list_objects_v2()
                .bucket(bucket)
                .prefix(&path)
                .delimiter("/");
            if !token.is_empty() {
                req = req.continuation_token(token);
            }
            req.send()
                .await
                .map_err(|e| parse_unexpect_error(e, "list", &path))
        };
        self.list_object_future = Some(Box::pin(fut));
        self.poll_next(cx)
    }
}
