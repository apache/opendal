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

use std::borrow::Cow;
use std::future::Future;
use std::sync::Arc;

use futures::FutureExt;
use object_store::Attribute;
use object_store::AttributeValue;
use object_store::ObjectStore;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::PutResult;

use opendal::raw::*;
use opendal::*;

use super::error::parse_error;

pub struct ObjectStoreWriter {
    store: Arc<dyn ObjectStore + 'static>,
    path: object_store::path::Path,
    args: OpWrite,
    result: Option<PutResult>,
}

impl ObjectStoreWriter {
    pub fn new(store: Arc<dyn ObjectStore + 'static>, path: &str, args: OpWrite) -> Self {
        Self {
            store,
            path: object_store::path::Path::from(path),
            args,
            result: None,
        }
    }
}

impl oio::Write for ObjectStoreWriter {
    fn write(&mut self, bs: Buffer) -> impl Future<Output = Result<()>> + MaybeSend {
        async move {
            let bytes = bs.to_bytes();
            let payload = PutPayload::from(bytes);
            let opts = parse_write_args(&self.args)?;
            let result = self
                .store
                .put_opts(&self.path, payload, opts)
                .await
                .map_err(parse_error)?;
            self.result = Some(result);
            Ok(())
        }
        .boxed()
    }

    fn close(&mut self) -> impl Future<Output = Result<Metadata>> + MaybeSend {
        async {
            let result = match &self.result {
                Some(result) => result,
                None => return Err(Error::new(ErrorKind::Unexpected, "No result")),
            };

            let mut metadata = Metadata::new(EntryMode::FILE);
            if let Some(etag) = &result.e_tag {
                metadata.set_etag(etag);
            }
            if let Some(version) = &result.version {
                metadata.set_version(version);
            }
            Ok(metadata)
        }
        .boxed()
    }

    fn abort(&mut self) -> impl Future<Output = Result<()>> + MaybeSend {
        async { Ok(()) }.boxed()
    }
}

pub(crate) fn parse_write_args(args: &OpWrite) -> Result<PutOptions> {
    let mut opts = PutOptions::default();

    if let Some(content_type) = args.content_type() {
        opts.attributes.insert(
            Attribute::ContentType,
            AttributeValue::from(content_type.to_string()),
        );
    }

    if let Some(content_disposition) = args.content_disposition() {
        opts.attributes.insert(
            Attribute::ContentDisposition,
            AttributeValue::from(content_disposition.to_string()),
        );
    }

    if let Some(cache_control) = args.cache_control() {
        opts.attributes.insert(
            Attribute::CacheControl,
            AttributeValue::from(cache_control.to_string()),
        );
    }

    if let Some(user_metadata) = args.user_metadata() {
        for (key, value) in user_metadata {
            opts.attributes.insert(
                Attribute::Metadata(Cow::from(key.to_string())),
                AttributeValue::from(value.to_string()),
            );
        }
    }
    Ok(opts)
}
