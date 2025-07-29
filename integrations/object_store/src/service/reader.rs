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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::StreamExt;
use object_store::GetRange;
use object_store::ObjectStore;

use futures::FutureExt;
use opendal::raw::*;
use opendal::Error;
use opendal::ErrorKind;
use opendal::*;

use super::error::parse_error;
use crate::utils::{IntoSendStream, SendWrapper};

/// ObjectStore reader
pub struct ObjectStoreReader {
    bytes_stream: Option<BoxStream<'static, object_store::Result<Bytes>>>,
    meta: object_store::ObjectMeta,
    args: OpRead,
}

impl ObjectStoreReader {
    pub(crate) async fn new(
        store: Arc<dyn ObjectStore + 'static>,
        path: &str,
        args: OpRead,
    ) -> Result<Self> {
        let path = object_store::path::Path::from(path);
        let opts = parse_read_args(&args)?;
        let result = store.get_opts(&path, opts).await.map_err(parse_error)?;
        let meta = result.meta.clone();
        let bytes_stream = Some(result.into_stream());
        Ok(Self {
            bytes_stream,
            meta,
            args,
        })
    }

    pub(crate) fn rp(&self) -> RpRead {
        let mut rp = RpRead::new().with_size(Some(self.meta.size));
        if !self.args.range().is_full() {
            let range = self.args.range();
            let size = match range.size() {
                Some(size) => size,
                None => self.meta.size,
            };
            rp = rp.with_range(Some(
                BytesContentRange::default().with_range(range.offset(), range.offset() + size - 1),
            ));
        }
        rp
    }
}

impl oio::Read for ObjectStoreReader {
    async fn read(&mut self) -> Result<Buffer> {
        let mut bytes_stream = match self.bytes_stream.take() {
            Some(bytes_stream) => bytes_stream,
            None => return Err(Error::new(ErrorKind::Unexpected, "no bytes to read")),
        };

        // convert bytes_stream to Buffer
        let mut all_bytes = Vec::new();
        while let Some(chunk_result) = bytes_stream.next().await {
            let chunk = chunk_result.map_err(parse_error)?;
            all_bytes.extend_from_slice(&chunk);
        }

        Ok(Buffer::from(all_bytes))
    }
}

fn parse_read_args(args: &OpRead) -> Result<object_store::GetOptions> {
    let mut options = object_store::GetOptions::default();

    if let Some(version) = args.version() {
        options.version = Some(version.to_string());
    }

    if let Some(if_match) = args.if_match() {
        options.if_match = Some(if_match.to_string());
    }

    if let Some(if_none_match) = args.if_none_match() {
        options.if_none_match = Some(if_none_match.to_string());
    }

    if let Some(if_modified_since) = args.if_modified_since() {
        options.if_modified_since = Some(if_modified_since);
    }

    if let Some(if_unmodified_since) = args.if_unmodified_since() {
        options.if_unmodified_since = Some(if_unmodified_since);
    }

    if !args.range().is_full() {
        let range = args.range();
        match range.size() {
            Some(size) => {
                options.range = Some(GetRange::Bounded(range.offset()..range.offset() + size));
            }
            None => {
                options.range = Some(GetRange::Offset(range.offset()));
            }
        }
    }

    Ok(options)
}
