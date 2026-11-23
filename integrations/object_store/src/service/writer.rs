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

use std::collections::BTreeMap;
use std::sync::Arc;

use object_store::MultipartUpload;
use object_store::ObjectStore;
use object_store::PutPayload;
use object_store::path::Path as ObjectStorePath;
use object_store::{Attribute, AttributeValue};

use asyncband::mutex::Mutex;
use futures::future::try_join_all;
use opendal::raw::oio::MultipartPart;
use opendal::raw::*;
use opendal::*;

use super::core::{format_put_multipart_options, format_put_result, parse_op_write};
use super::error::parse_error;

struct MultipartUploadState {
    upload: Box<dyn MultipartUpload>,
    pending_parts: BTreeMap<usize, PutPayload>,
    next_part_number: usize,
    failed: bool,
}

pub struct ObjectStoreWriter {
    store: Arc<dyn ObjectStore + 'static>,
    path: ObjectStorePath,
    args: OpWrite,
    upload: Mutex<Option<MultipartUploadState>>,
}

impl ObjectStoreWriter {
    pub fn new(store: Arc<dyn ObjectStore + 'static>, path: &str, args: OpWrite) -> Self {
        Self {
            store,
            path: ObjectStorePath::from(path),
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
        let mut opts = parse_op_write(&self.args)?;

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
        let mut metadata = MetadataBuilder::unknown();
        if let Some(etag) = &result.e_tag {
            metadata.etag(etag);
        }
        if let Some(version) = &result.version {
            metadata.version(version);
        }

        Ok(metadata.build())
    }

    // Generate a unique upload ID that we'll use to track this session
    async fn initiate_part(&self) -> Result<String> {
        // Start a new multipart upload using object_store
        let opts = parse_op_write(&self.args)?;
        let multipart_opts = format_put_multipart_options(opts);
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
        *guard = Some(MultipartUploadState {
            upload,
            pending_parts: BTreeMap::new(),
            next_part_number: 0,
            failed: false,
        });

        // object_store does not provide a way to get the upload id, so we use a fixed string
        // as the upload id. it's ok because the upload id is already tracked inside the upload
        // object.
        Ok("".to_string())
    }

    /// Upload a single part of the multipart upload.
    /// Part numbers must be sequential starting from 0.
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
            let mut guard = self.upload.lock().await;
            if let Some(state) = guard.as_mut() {
                state.failed = true;
                state.pending_parts.clear();
            }
            return Err(Error::new(
                ErrorKind::Unexpected,
                format!("Expected size {size} but got {actual_size}"),
            ));
        }

        let payload = PutPayload::from(body.to_bytes());
        let mut guard = self.upload.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload not initiated"))?;
        if state.failed {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Multipart upload has failed",
            ));
        }
        if part_number < state.next_part_number || state.pending_parts.contains_key(&part_number) {
            state.failed = true;
            state.pending_parts.clear();
            return Err(
                Error::new(ErrorKind::Unexpected, "Duplicate multipart part number")
                    .with_context("part_number", part_number),
            );
        }
        state.pending_parts.insert(part_number, payload);

        // object_store assigns part IDs when put_part is called. Create its
        // futures in part-number order, then poll them concurrently without
        // holding the lock that orders the calls.
        let mut uploads = Vec::new();
        while let Some(payload) = state.pending_parts.remove(&state.next_part_number) {
            uploads.push(state.upload.put_part(payload));
            state.next_part_number += 1;
        }
        drop(guard);

        if let Err(err) = try_join_all(uploads).await {
            let mut guard = self.upload.lock().await;
            if let Some(state) = guard.as_mut() {
                state.failed = true;
                state.pending_parts.clear();
            }
            return Err(parse_error(err));
        }

        Ok(MultipartPart {
            part_number,
            // object_store does not expose per-part ETags.
            etag: String::new(),
            checksum: None,
            size: None,
        })
    }

    async fn complete_part(&self, _upload_id: &str, parts: &[MultipartPart]) -> Result<Metadata> {
        let mut guard = self.upload.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload not initiated"))?;
        if state.failed {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot complete a failed multipart upload",
            ));
        }

        let mut part_numbers: Vec<_> = parts.iter().map(|part| part.part_number).collect();
        part_numbers.sort_unstable();
        if parts.is_empty()
            || !state.pending_parts.is_empty()
            || !part_numbers.iter().copied().eq(0..state.next_part_number)
        {
            state.failed = true;
            state.pending_parts.clear();
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Multipart parts do not match the initiated upload",
            )
            .with_context("expected_parts", state.next_part_number)
            .with_context("actual_parts", parts.len()));
        }

        let result = match state.upload.complete().await {
            Ok(result) => result,
            Err(err) => {
                state.failed = true;
                return Err(parse_error(err));
            }
        };
        *guard = None;

        Ok(format_put_result(result))
    }

    async fn abort_part(&self, _upload_id: &str) -> Result<()> {
        let mut guard = self.upload.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload not initiated"))?;
        state.failed = true;
        state.pending_parts.clear();

        state.upload.abort().await.map_err(parse_error)?;
        *guard = None;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::join;
    use object_store::ObjectStoreExt;
    use object_store::memory::InMemory;
    use opendal::raw::oio::MultipartWrite;

    use super::*;

    #[tokio::test]
    async fn test_write_part_reversed_completion_preserves_order() {
        let store = Arc::new(InMemory::new());
        let path = ObjectStorePath::from("reversed.txt");
        let writer = ObjectStoreWriter::new(store.clone(), path.as_ref(), OpWrite::default());
        let upload_id = writer.initiate_part().await.unwrap();

        let part0 = writer.write_part(&upload_id, 0, 1, Buffer::from("A"));
        let part1 = writer.write_part(&upload_id, 1, 1, Buffer::from("B"));
        let (part1, part0) = join!(part1, part0);
        let parts = [part1.unwrap(), part0.unwrap()];
        writer.complete_part(&upload_id, &parts).await.unwrap();

        let content = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(content, Bytes::from_static(b"AB"));
    }

    #[tokio::test]
    async fn test_write_part_out_of_order_completion_preserves_order() {
        let store = Arc::new(InMemory::new());
        let path = ObjectStorePath::from("out-of-order.txt");
        let writer = ObjectStoreWriter::new(store.clone(), path.as_ref(), OpWrite::default());
        let upload_id = writer.initiate_part().await.unwrap();

        let part0 = writer.write_part(&upload_id, 0, 1, Buffer::from("A"));
        let part1 = writer.write_part(&upload_id, 1, 1, Buffer::from("B"));
        let part2 = writer.write_part(&upload_id, 2, 1, Buffer::from("C"));
        let part3 = writer.write_part(&upload_id, 3, 1, Buffer::from("D"));
        let (part2, part0, part3, part1) = join!(part2, part0, part3, part1);
        let parts = [
            part2.unwrap(),
            part0.unwrap(),
            part3.unwrap(),
            part1.unwrap(),
        ];
        writer.complete_part(&upload_id, &parts).await.unwrap();

        let content = store.get(&path).await.unwrap().bytes().await.unwrap();
        assert_eq!(content, Bytes::from_static(b"ABCD"));
    }

    #[tokio::test]
    async fn test_abort_part_discards_buffered_parts() {
        let store = Arc::new(InMemory::new());
        let path = ObjectStorePath::from("aborted.txt");
        let writer = ObjectStoreWriter::new(store.clone(), path.as_ref(), OpWrite::default());
        let upload_id = writer.initiate_part().await.unwrap();

        writer
            .write_part(&upload_id, 1, 1, Buffer::from("B"))
            .await
            .unwrap();
        writer.abort_part(&upload_id).await.unwrap();

        let result = store.head(&path).await;
        assert!(matches!(result, Err(object_store::Error::NotFound { .. })));
    }
}
