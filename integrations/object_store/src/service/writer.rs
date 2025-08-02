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
use std::collections::HashMap;
use std::sync::Arc;

use object_store::Attribute;
use object_store::AttributeValue;
use object_store::MultipartUpload;
use object_store::ObjectStore;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::PutResult;

use opendal::raw::oio::MultipartPart;
use opendal::raw::*;
use opendal::*;

use super::error::parse_error;

pub struct ObjectStoreWriter {
    store: Arc<dyn ObjectStore + 'static>,
    path: object_store::path::Path,
    args: OpWrite,
    multipart_uploads: Arc<tokio::sync::Mutex<HashMap<String, Box<dyn MultipartUpload>>>>,
}

impl ObjectStoreWriter {
    pub fn new(store: Arc<dyn ObjectStore + 'static>, path: &str, args: OpWrite) -> Self {
        Self {
            store,
            path: object_store::path::Path::from(path),
            args,
            multipart_uploads: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    }
}

impl oio::MultipartWrite for ObjectStoreWriter {
    /// Write the entire object in one go.
    /// Used when the object is small enough to bypass multipart upload.
    async fn write_once(&self, size: u64, body: Buffer) -> Result<Metadata> {
        // Validate that actual body size matches expected size
        let actual_size = body.len() as u64;
        if actual_size != size {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Expected size {} but got {}", size, actual_size),
            ));
        }

        let bytes = body.to_bytes();
        let payload = PutPayload::from(bytes);
        let mut opts = convert_to_put_opts(&self.args)?;

        // Add size metadata for tracking
        opts.attributes.insert(
            Attribute::Metadata("content-size".into()),
            AttributeValue::from(size.to_string()),
        );

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
        // Create a unique upload ID using ULID
        let upload_id = ulid::Ulid::new().to_string();

        // Start a new multipart upload using object_store
        let opts = convert_to_put_opts(&self.args)?;
        let multipart_opts = convert_to_multipart_options(opts);
        let multipart_upload = self
            .store
            .put_multipart_opts(&self.path, multipart_opts)
            .await
            .map_err(parse_error)?;

        // Store the multipart upload for later use
        let mut uploads = self.multipart_uploads.lock().await;
        uploads.insert(upload_id.clone(), multipart_upload);

        Ok(upload_id)
    }

    /// Upload a single part of the multipart upload.
    /// Part numbers must be sequential starting from 1.
    /// Returns the ETag and part information for this uploaded part.
    async fn write_part(
        &self,
        upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<MultipartPart> {
        // Validate that actual body size matches expected size
        let actual_size = body.len() as u64;
        if actual_size != size {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Expected size {} but got {}", size, actual_size),
            ));
        }

        // Get the multipart upload for this upload_id
        let mut uploads = self.multipart_uploads.lock().await;
        let multipart_upload = uploads
            .get_mut(upload_id)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload ID not found"))?;

        // Convert Buffer to PutPayload
        let bytes = body.to_bytes();
        let payload = PutPayload::from(bytes);

        // Upload the part
        let upload_part = multipart_upload.put_part(payload);
        upload_part.await.map_err(parse_error)?;

        // Create MultipartPart with the part number
        // Note: We don't have the actual ETag from the part upload,
        // so we'll use a placeholder. In a real implementation,
        // you might need to track ETags differently.
        let multipart_part = MultipartPart {
            part_number,
            etag: format!("part-{}", part_number), // Placeholder ETag
            checksum: None,                        // No checksum for now
        };

        Ok(multipart_part)
    }

    async fn complete_part(
        &self,
        upload_id: &str,
        _parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        // Get the multipart upload for this upload_id
        let mut uploads = self.multipart_uploads.lock().await;
        let multipart_upload = uploads
            .get_mut(upload_id)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload ID not found"))?;

        // Complete the multipart upload
        let result = multipart_upload.complete().await.map_err(parse_error)?;

        // Remove the upload from tracking
        uploads.remove(upload_id);

        // Build metadata from the result
        let mut metadata = Metadata::new(EntryMode::FILE);
        if let Some(etag) = &result.e_tag {
            metadata.set_etag(etag);
        }
        if let Some(version) = &result.version {
            metadata.set_version(version);
        }

        Ok(metadata)
    }

    async fn abort_part(&self, upload_id: &str) -> Result<()> {
        // Get the multipart upload for this upload_id
        let mut uploads = self.multipart_uploads.lock().await;
        let multipart_upload = uploads
            .get_mut(upload_id)
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload ID not found"))?;

        // Abort the multipart upload
        multipart_upload.abort().await.map_err(parse_error)?;

        // Remove the upload from tracking
        uploads.remove(upload_id);

        Ok(())
    }
}

pub(crate) fn convert_to_put_opts(args: &OpWrite) -> Result<PutOptions> {
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

fn convert_to_multipart_options(opts: PutOptions) -> object_store::PutMultipartOptions {
    let mut multipart_opts = object_store::PutMultipartOptions::default();
    multipart_opts.attributes = opts.attributes;
    multipart_opts
}
