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
use std::sync::Arc;

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
    async fn write(&mut self, bs: Buffer) -> Result<()> {
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

    async fn close(&mut self) -> Result<Metadata> {
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

    async fn abort(&mut self) -> Result<()> {
        Ok(())
    }
}

impl oio::MultipartWrite for ObjectStoreWriter {
    /// Write the entire object in one go.
    /// Used when the object is small enough to bypass multipart upload.
    async fn write_once(&self, _size: u64, body: Buffer) -> Result<Metadata> {
        let bytes = body.to_bytes();
        let payload = PutPayload::from(bytes);
        let opts = parse_write_args(&self.args)?;

        let result = self
            .store
            .put_opts(&self.path, payload, opts)
            .await
            .map_err(parse_error)?;

        // Build metadata from put result
        let mut metadata = Metadata::new(EntryMode::FILE);
        if let Some(etag) = &result.e_tag {
            metadata.set_etag(etag);
        }
        if let Some(version) = &result.version {
            metadata.set_version(version);
        }

        Ok(metadata)
    }

    // Generate a unique upload ID that we'll use to track this session
    async fn initiate_part(&self) -> Result<String> {
        // For ObjectStore, we need to use the underlying service's
        // multipart API. Different services use different approaches:                                                                                                                                                                None
        // - S3: InitiateMultipartUpload
        // - Azure: Block Blob operations
        // - GCS: Multipart upload

        let upload_id = format!("mp-{}", ulid::Ulid::new());
        Ok(upload_id)
    }

    async fn complete_part(
        &self,
        upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        todo!()
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        todo!()
    }

    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        _size: u64,
        _body: Buffer,
    ) -> Result<oio::MultipartPart> {
        todo!()
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
