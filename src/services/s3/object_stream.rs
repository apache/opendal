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
    state: State,
}

#[allow(clippy::large_enum_variant)]
enum State {
    Idle,
    Sending(BoxFuture<'static, Result<ListObjectsV2Output>>),
    /// # TODO
    ///
    /// It's better to move this large struct to heap as suggested by clippy.
    ///
    ///   --> src/services/s3/object_stream.rs:45:5
    ///    |
    /// 45 |     Listing((ListObjectsV2Output, usize, usize)),
    ///    |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ this variant is 256 bytes
    ///    |
    ///    = note: `-D clippy::large-enum-variant` implied by `-D warnings`
    /// note: and the second-largest variant is 16 bytes:
    ///   --> src/services/s3/object_stream.rs:44:5
    ///    |
    /// 44 |     Sending(BoxFuture<'static, Result<ListObjectsV2Output>>),
    ///    |     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
    ///    = help: for further information visit https://rust-lang.github.io/rust-clippy/master/index.html#large_enum_variant
    /// help: consider boxing the large fields to reduce the total size of the enum
    ///    |
    /// 45 |     Listing(Box<(ListObjectsV2Output, usize, usize)>),
    ///
    /// But stable rust doesn't support `State::Listing(box (output, common_prefixes_idx, objects_idx))` so far, let's wait a bit.
    Listing((ListObjectsV2Output, usize, usize)),
}

impl S3ObjectStream {
    pub fn new(backend: Backend, bucket: String, path: String) -> Self {
        Self {
            backend,
            bucket,
            path,

            token: "".to_string(),
            done: false,
            state: State::Idle,
        }
    }
}

impl futures::Stream for S3ObjectStream {
    type Item = Result<Object>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let backend = self.backend.clone();

        match &mut self.state {
            State::Idle => {
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
                self.state = State::Sending(Box::pin(fut));
                self.poll_next(cx)
            }
            State::Sending(fut) => {
                let output = ready!(Pin::new(fut).poll(cx))?;

                self.done = !output.is_truncated;
                self.token = output.continuation_token.clone().unwrap_or_default();
                self.state = State::Listing((output, 0, 0));
                self.poll_next(cx)
            }
            State::Listing((output, common_prefixes_idx, objects_idx)) => {
                if let Some(prefixes) = &output.common_prefixes {
                    if *common_prefixes_idx < prefixes.len() {
                        *common_prefixes_idx += 1;
                        let prefix = &prefixes[*common_prefixes_idx - 1].prefix();

                        let mut o = Object::new(
                            Arc::new(backend.clone()),
                            &backend.get_rel_path(prefix.expect("prefix should not be None")),
                        );
                        let meta = o.metadata_mut();
                        meta.set_mode(ObjectMode::DIR)
                            .set_content_length(0)
                            .set_complete();

                        return Poll::Ready(Some(Ok(o)));
                    }
                }
                if let Some(objects) = &output.contents {
                    if *objects_idx < objects.len() {
                        *objects_idx += 1;
                        let object = &objects[*objects_idx - 1];

                        let mut o = Object::new(
                            Arc::new(backend.clone()),
                            &backend.get_rel_path(object.key().expect("key should not be None")),
                        );
                        let meta = o.metadata_mut();
                        meta.set_mode(ObjectMode::FILE)
                            .set_content_length(object.size as u64);

                        return Poll::Ready(Some(Ok(o)));
                    }
                }

                if self.done {
                    return Poll::Ready(None);
                }

                self.state = State::Idle;
                self.poll_next(cx)
            }
        }
    }
}
