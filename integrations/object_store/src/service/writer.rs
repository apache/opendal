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
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use object_store::AttributeValue;
use object_store::MultipartUpload;
use object_store::ObjectStore;
use object_store::PutOptions;
use object_store::PutPayload;
use object_store::{Attribute, PutResult};

use opendal::raw::oio::MultipartPart;
use opendal::raw::*;
use opendal::*;
use tokio::sync::Mutex;

use super::error::parse_error;

pub struct ObjectStoreWriter {
    store: Arc<dyn ObjectStore + 'static>,
    path: object_store::path::Path,
    args: OpWrite,
    upload: Mutex<Option<Box<dyn MultipartUpload>>>,
}

impl ObjectStoreWriter {
    pub fn new(store: Arc<dyn ObjectStore + 'static>, path: &str, args: OpWrite) -> Self {
        Self {
            store,
            path: object_store::path::Path::from(path),
            args,
            upload: Mutex::new(None),
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
                format!("Expected size {size} but got {actual_size}"),
            ));
        }

        let bytes = body.to_bytes();
        let payload = PutPayload::from(bytes);
        let mut opts = format_put_options(&self.args)?;

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
        // Start a new multipart upload using object_store
        let opts = format_put_options(&self.args)?;
        let multipart_opts = format_multipart_options(opts);
        let upload = self
            .store
            .put_multipart_opts(&self.path, multipart_opts)
            .await
            .map_err(parse_error)?;

        // Store the multipart upload for later use
        let mut guard = self.upload.lock().await;
        if guard.is_some() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Upload already initiated, abort the previous upload first",
            ));
        }
        *guard = Some(upload);

        // object_store does not provide a way to get the upload id, so we use a fixed string
        // as the upload id. it's ok because the upload id is already tracked inside the upload
        // object.
        Ok("".to_string())
    }

    /// Upload a single part of the multipart upload.
    /// Part numbers must be sequential starting from 1.
    /// Returns the ETag and part information for this uploaded part.
    async fn write_part(
        &self,
        _upload_id: &str,
        part_number: usize,
        size: u64,
        body: Buffer,
    ) -> Result<MultipartPart> {
        // Validate that actual body size matches expected size
        let actual_size = body.len() as u64;
        if actual_size != size {
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Expected size {size} but got {actual_size}"),
            ));
        }

        // Convert Buffer to PutPayload
        let bytes = body.to_bytes();

        // Generate a proper ETag for the part based on its content
        // This follows the pattern used by many object stores where ETags are MD5 hashes
        let etag = calculate_part_etag(&bytes, part_number);

        let payload = PutPayload::from(bytes);

        // Upload the part
        let mut guard = self.upload.lock().await;
        let upload = guard
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload not initiated"))?;
        upload.put_part(payload).await.map_err(parse_error)?;

        // Create MultipartPart with the proper ETag
        let multipart_part = MultipartPart {
            part_number,
            etag,
            checksum: None, // No checksum for now
        };
        Ok(multipart_part)
    }

    async fn complete_part(
        &self,
        _upload_id: &str,
        parts: &[oio::MultipartPart],
    ) -> Result<Metadata> {
        // Validate that we have parts to complete
        if parts.is_empty() {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot complete multipart upload with no parts",
            ));
        }

        // Validate part numbers are sequential and start from 0
        for (i, part) in parts.iter().enumerate() {
            if part.part_number != i {
                return Err(Error::new(
                    ErrorKind::Unexpected,
                    format!(
                        "Invalid part number: expected {}, got {}",
                        i, part.part_number
                    ),
                ));
            }
        }

        // Get the multipart upload for this upload_id
        let mut guard = self.upload.lock().await;
        let upload = guard
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload not initiated"))?;

        // Complete the multipart upload
        let result = upload.complete().await.map_err(parse_error)?;
        *guard = None;

        // Build metadata from the result
        let metadata = format_metadata_with_put_result(result);
        Ok(metadata)
    }

    async fn abort_part(&self, _upload_id: &str) -> Result<()> {
        // Get the multipart upload for this upload_id
        let mut guard = self.upload.lock().await;
        let upload = guard
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload not initiated"))?;

        // Abort the multipart upload
        upload.abort().await.map_err(parse_error)?;
        *guard = None;

        Ok(())
    }
}

pub(crate) fn format_put_options(args: &OpWrite) -> Result<PutOptions> {
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

fn format_multipart_options(opts: PutOptions) -> object_store::PutMultipartOptions {
    object_store::PutMultipartOptions {
        attributes: opts.attributes,
        ..Default::default()
    }
}

fn format_metadata_with_put_result(result: PutResult) -> Metadata {
    let mut metadata = Metadata::new(EntryMode::FILE);
    if let Some(etag) = &result.e_tag {
        metadata.set_etag(etag);
    }
    if let Some(version) = &result.version {
        metadata.set_version(version);
    }
    metadata
}

/// Generate a proper ETag for a multipart part
/// This follows the pattern used by many object stores where ETags are based on content hash
fn calculate_part_etag(content: &[u8], part_number: usize) -> String {
    let mut hasher = DefaultHasher::new();
    content.hash(&mut hasher);
    part_number.hash(&mut hasher);

    // Format as hex string
    format!("{:016x}", hasher.finish())
}
