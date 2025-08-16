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

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::TryStreamExt;
use object_store::path::Path as ObjectStorePath;
use object_store::ObjectStore;

use opendal::raw::*;
use opendal::*;
use tokio::sync::Mutex;

use super::core::parse_op_read;
use super::error::parse_error;

/// ObjectStore reader
pub struct ObjectStoreReader {
    bytes_stream: Mutex<BoxStream<'static, object_store::Result<Bytes>>>,
    meta: object_store::ObjectMeta,
    args: OpRead,
}

impl ObjectStoreReader {
    pub(crate) async fn new(
        store: Arc<dyn ObjectStore + 'static>,
        path: &str,
        args: OpRead,
    ) -> Result<Self> {
        let path = ObjectStorePath::from(path);
        let opts = parse_op_read(&args)?;
        let result = store.get_opts(&path, opts).await.map_err(parse_error)?;
        let meta = result.meta.clone();
        let bytes_stream = Mutex::new(result.into_stream());
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
        let mut bytes_stream = self.bytes_stream.lock().await;
        let bs = bytes_stream.try_next().await.map_err(parse_error)?;
        Ok(bs.map(Buffer::from).unwrap_or_default())
    }
}

