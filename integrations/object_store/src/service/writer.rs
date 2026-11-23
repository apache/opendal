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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use object_store::MultipartUpload;
use object_store::ObjectStore;
use object_store::PutPayload;
use object_store::path::Path as ObjectStorePath;

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
    failed: Arc<AtomicBool>,
    in_flight: Arc<AtomicUsize>,
}

struct InFlightUpload {
    failed: Arc<AtomicBool>,
    in_flight: Arc<AtomicUsize>,
    succeeded: bool,
}

impl InFlightUpload {
    fn new(state: &MultipartUploadState) -> Self {
        state.in_flight.fetch_add(1, Ordering::SeqCst);
        Self {
            failed: state.failed.clone(),
            in_flight: state.in_flight.clone(),
            succeeded: false,
        }
    }

    fn fail(&self) {
        self.failed.store(true, Ordering::SeqCst);
    }

    fn succeed(mut self) {
        self.succeeded = true;
    }
}

impl Drop for InFlightUpload {
    fn drop(&mut self) {
        if !self.succeeded {
            self.failed.store(true, Ordering::SeqCst);
        }
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
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
        let opts = parse_op_write(&self.args)?;

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
            failed: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(AtomicUsize::new(0)),
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
                state.failed.store(true, Ordering::SeqCst);
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
        if state.failed.load(Ordering::SeqCst) {
            state.pending_parts.clear();
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Multipart upload has failed",
            ));
        }
        if part_number < state.next_part_number || state.pending_parts.contains_key(&part_number) {
            state.failed.store(true, Ordering::SeqCst);
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
        let multipart_part = MultipartPart {
            part_number,
            // object_store does not expose per-part ETags.
            etag: String::new(),
            checksum: None,
            size: None,
        };
        if uploads.is_empty() {
            return Ok(multipart_part);
        }
        let in_flight = InFlightUpload::new(state);
        drop(guard);

        match try_join_all(uploads).await {
            Ok(_) => in_flight.succeed(),
            Err(err) => {
                in_flight.fail();
                let mut guard = self.upload.lock().await;
                if let Some(state) = guard.as_mut() {
                    state.pending_parts.clear();
                }
                return Err(parse_error(err));
            }
        }

        Ok(multipart_part)
    }

    async fn complete_part(&self, _upload_id: &str, parts: &[MultipartPart]) -> Result<Metadata> {
        let mut guard = self.upload.lock().await;
        let state = guard
            .as_mut()
            .ok_or_else(|| Error::new(ErrorKind::Unexpected, "Upload not initiated"))?;
        if state.in_flight.load(Ordering::SeqCst) != 0 {
            state.failed.store(true, Ordering::SeqCst);
            state.pending_parts.clear();
            return Err(Error::new(
                ErrorKind::Unexpected,
                "Cannot complete multipart upload with parts in flight",
            ));
        }
        // InFlightUpload marks failure before decrementing the counter. Reading
        // the counter first also observes a concurrent cancellation.
        if state.failed.load(Ordering::SeqCst) {
            state.pending_parts.clear();
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
            state.failed.store(true, Ordering::SeqCst);
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
                state.failed.store(true, Ordering::SeqCst);
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
        state.failed.store(true, Ordering::SeqCst);
        state.pending_parts.clear();

        state.upload.abort().await.map_err(parse_error)?;
        *guard = None;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::Mutex as StdMutex;
    use std::task::Poll;

    use crate::ObjectStoreBuilder;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::channel::oneshot;
    use futures::future::poll_fn;
    use futures::{FutureExt, join};
    use object_store::local::LocalFileSystem;
    use object_store::memory::InMemory;
    use object_store::{Attribute, AttributeValue, ObjectStoreExt, PutResult, UploadPart};
    use opendal::raw::oio::MultipartWrite;
    use tempfile::TempDir;

    use super::*;

    type PartControl = oneshot::Sender<object_store::Result<()>>;

    #[derive(Debug, Default)]
    struct ControlledState {
        parts: Vec<PutPayload>,
        controls: Vec<Option<PartControl>>,
        completed: bool,
        aborted: bool,
    }

    #[derive(Debug)]
    struct ControlledUpload {
        state: Arc<StdMutex<ControlledState>>,
    }

    #[async_trait]
    impl MultipartUpload for ControlledUpload {
        fn put_part(&mut self, data: PutPayload) -> UploadPart {
            let (tx, rx) = oneshot::channel();
            let mut state = self.state.lock().unwrap();
            state.parts.push(data);
            state.controls.push(Some(tx));
            drop(state);

            async move { rx.await.expect("part control dropped") }.boxed()
        }

        async fn complete(&mut self) -> object_store::Result<PutResult> {
            let mut state = self.state.lock().unwrap();
            assert!(state.controls.iter().all(|control| control.is_none()));
            state.completed = true;
            Ok(PutResult {
                e_tag: None,
                version: None,
                extensions: Default::default(),
            })
        }

        async fn abort(&mut self) -> object_store::Result<()> {
            let mut state = self.state.lock().unwrap();
            state.aborted = true;
            state.controls.clear();
            Ok(())
        }
    }

    async fn controlled_writer() -> (ObjectStoreWriter, String, Arc<StdMutex<ControlledState>>) {
        let writer = ObjectStoreWriter::new(
            Arc::new(InMemory::new()),
            "controlled.txt",
            OpWrite::default(),
        );
        let upload_id = writer.initiate_part().await.unwrap();
        let state = Arc::new(StdMutex::new(ControlledState::default()));
        writer.upload.lock().await.as_mut().unwrap().upload = Box::new(ControlledUpload {
            state: state.clone(),
        });
        (writer, upload_id, state)
    }

    async fn poll_once<F: Future + ?Sized>(mut future: Pin<&mut F>) -> Poll<F::Output> {
        poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await
    }

    fn finish_part(
        state: &Arc<StdMutex<ControlledState>>,
        index: usize,
        result: object_store::Result<()>,
    ) {
        let control = state.lock().unwrap().controls[index].take().unwrap();
        control.send(result).unwrap();
    }

    fn completed_content(state: &Arc<StdMutex<ControlledState>>) -> Bytes {
        let state = state.lock().unwrap();
        assert!(state.completed);
        let content = state
            .parts
            .iter()
            .flat_map(|part| part.iter())
            .flat_map(|bytes| bytes.to_vec())
            .collect::<Vec<_>>();
        Bytes::from(content)
    }

    fn multipart_part(part_number: usize) -> MultipartPart {
        MultipartPart {
            part_number,
            etag: String::new(),
            checksum: None,
            size: None,
        }
    }

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

    #[tokio::test]
    async fn test_async_part_completion_can_finish_out_of_order() {
        let (writer, upload_id, state) = controlled_writer().await;
        let mut part0 = Box::pin(writer.write_part(&upload_id, 0, 1, Buffer::from("A")));
        let mut part1 = Box::pin(writer.write_part(&upload_id, 1, 1, Buffer::from("B")));

        assert!(poll_once(part0.as_mut()).await.is_pending());
        assert!(poll_once(part1.as_mut()).await.is_pending());
        assert_eq!(state.lock().unwrap().controls.len(), 2);

        finish_part(&state, 1, Ok(()));
        let part1 = part1.await.unwrap();
        finish_part(&state, 0, Ok(()));
        let part0 = part0.await.unwrap();
        writer
            .complete_part(&upload_id, &[part1, part0])
            .await
            .unwrap();

        assert_eq!(completed_content(&state), Bytes::from_static(b"AB"));
    }

    #[tokio::test]
    async fn test_part_failure_poisons_upload_and_allows_abort() {
        let (writer, upload_id, state) = controlled_writer().await;
        let mut part0 = Box::pin(writer.write_part(&upload_id, 0, 1, Buffer::from("A")));
        assert!(poll_once(part0.as_mut()).await.is_pending());

        finish_part(
            &state,
            0,
            Err(object_store::Error::Generic {
                store: "test",
                source: std::io::Error::other("part upload failed").into(),
            }),
        );
        assert!(part0.await.is_err());
        assert!(
            writer
                .write_part(&upload_id, 1, 1, Buffer::from("B"))
                .await
                .is_err()
        );
        assert!(
            writer
                .complete_part(&upload_id, &[multipart_part(0)])
                .await
                .is_err()
        );

        writer.abort_part(&upload_id).await.unwrap();
        assert!(state.lock().unwrap().aborted);
    }

    #[tokio::test]
    async fn test_duplicate_write_part_is_rejected() {
        let store = Arc::new(InMemory::new());
        let writer = ObjectStoreWriter::new(store, "duplicate.txt", OpWrite::default());
        let upload_id = writer.initiate_part().await.unwrap();
        let part0 = writer
            .write_part(&upload_id, 0, 1, Buffer::from("A"))
            .await
            .unwrap();

        assert!(
            writer
                .write_part(&upload_id, 0, 1, Buffer::from("A"))
                .await
                .is_err()
        );
        assert!(writer.complete_part(&upload_id, &[part0]).await.is_err());
        writer.abort_part(&upload_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_complete_part_rejects_invalid_part_lists() {
        for (written, completed) in [(2, vec![0]), (2, vec![0, 0, 1]), (1, vec![0, 1])] {
            let store = Arc::new(InMemory::new());
            let writer = ObjectStoreWriter::new(store, "invalid.txt", OpWrite::default());
            let upload_id = writer.initiate_part().await.unwrap();
            for part_number in 0..written {
                writer
                    .write_part(&upload_id, part_number, 1, Buffer::from("A"))
                    .await
                    .unwrap();
            }

            let parts = completed
                .iter()
                .copied()
                .map(multipart_part)
                .collect::<Vec<_>>();
            assert!(
                writer.complete_part(&upload_id, &parts).await.is_err(),
                "written={written}, completed={completed:?}"
            );
            writer.abort_part(&upload_id).await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_dropping_write_part_poisons_upload() {
        let (writer, upload_id, state) = controlled_writer().await;
        let mut part0 = Box::pin(writer.write_part(&upload_id, 0, 1, Buffer::from("A")));
        assert!(poll_once(part0.as_mut()).await.is_pending());

        drop(part0);
        assert!(
            writer
                .write_part(&upload_id, 1, 1, Buffer::from("B"))
                .await
                .is_err()
        );
        assert!(
            writer
                .complete_part(&upload_id, &[multipart_part(0)])
                .await
                .is_err()
        );
        writer.abort_part(&upload_id).await.unwrap();
        assert!(state.lock().unwrap().aborted);
    }

    #[tokio::test]
    async fn test_complete_part_rejects_in_flight_upload() {
        let (writer, upload_id, state) = controlled_writer().await;
        let mut part0 = Box::pin(writer.write_part(&upload_id, 0, 1, Buffer::from("A")));
        assert!(poll_once(part0.as_mut()).await.is_pending());

        assert!(
            writer
                .complete_part(&upload_id, &[multipart_part(0)])
                .await
                .is_err()
        );
        assert!(!state.lock().unwrap().completed);

        drop(part0);
        writer.abort_part(&upload_id).await.unwrap();
    }

    #[tokio::test]
    async fn test_write_once_writes_to_local_file_system() {
        let temp_dir = TempDir::new().expect("create temp dir");
        let store = LocalFileSystem::new_with_prefix(temp_dir.path()).expect("create local store");
        let op = Operator::new(ObjectStoreBuilder::new(Arc::new(store)))
            .expect("create operator from local store");

        op.write("hello.txt", "world")
            .await
            .expect("write should succeed");
        let content = op.read("hello.txt").await.expect("read should succeed");

        assert_eq!(content.to_vec(), b"world");
    }

    #[tokio::test]
    async fn test_write_once_does_not_add_implicit_attributes() {
        let store = Arc::new(InMemory::new());
        let op = Operator::new(ObjectStoreBuilder::new(store.clone()))
            .expect("create operator from memory store");

        op.write("hello.txt", "world")
            .await
            .expect("write should succeed");

        let result = store
            .get_opts(
                &ObjectStorePath::from("hello.txt"),
                object_store::GetOptions::default(),
            )
            .await
            .expect("get should succeed");
        assert!(
            result.attributes.is_empty(),
            "unexpected attributes: {:?}",
            result.attributes
        );
    }

    #[tokio::test]
    async fn test_write_once_propagates_explicit_attributes() {
        let store = Arc::new(InMemory::new());
        let op = Operator::new(ObjectStoreBuilder::new(store.clone()))
            .expect("create operator from memory store");

        op.write_with("hello.txt", "world")
            .content_type("text/plain")
            .cache_control("no-cache")
            .content_disposition("attachment")
            .user_metadata([("owner".to_string(), "opendal".to_string())])
            .await
            .expect("write should succeed");

        let result = store
            .get_opts(
                &ObjectStorePath::from("hello.txt"),
                object_store::GetOptions::default(),
            )
            .await
            .expect("get should succeed");

        assert_eq!(
            result.attributes.get(&Attribute::ContentType),
            Some(&AttributeValue::from("text/plain"))
        );
        assert_eq!(
            result.attributes.get(&Attribute::CacheControl),
            Some(&AttributeValue::from("no-cache"))
        );
        assert_eq!(
            result.attributes.get(&Attribute::ContentDisposition),
            Some(&AttributeValue::from("attachment"))
        );
        assert_eq!(
            result.attributes.get(&Attribute::Metadata("owner".into())),
            Some(&AttributeValue::from("opendal"))
        );
        assert!(
            result
                .attributes
                .get(&Attribute::Metadata("content-size".into()))
                .is_none(),
            "write_once must not inject content-size"
        );
    }
}
