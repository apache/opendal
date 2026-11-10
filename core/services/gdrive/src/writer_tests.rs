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

use std::collections::VecDeque;
use std::sync::Mutex;

use futures::stream;
use http::Request;
use http::header::CONTENT_RANGE;

use super::*;
use crate::core::{GdrivePathQuery, GdriveRecentState, GdriveSigner};
use crate::path_index::GdrivePathIndex;
use opendal_core::raw::oio::Write;

#[derive(Clone)]
struct ScriptedTransport {
    responses: Arc<Mutex<VecDeque<Result<Response<Buffer>>>>>,
    requests: Arc<Mutex<Vec<Request<Buffer>>>>,
}

impl ScriptedTransport {
    fn new(responses: Vec<Result<Response<Buffer>>>) -> Self {
        Self {
            responses: Arc::new(Mutex::new(responses.into())),
            requests: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn ranges(&self) -> Vec<String> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter_map(|req| req.headers().get(CONTENT_RANGE))
            .map(|value| value.to_str().unwrap().to_string())
            .collect()
    }

    fn assert_finished(&self) {
        assert!(self.responses.lock().unwrap().is_empty());
    }
}

impl HttpTransport for ScriptedTransport {
    async fn fetch(&self, req: Request<Buffer>) -> Result<Response<HttpBody>> {
        self.requests.lock().unwrap().push(req);
        let response = self.responses.lock().unwrap().pop_front().unwrap()?;
        let (parts, body) = response.into_parts();
        let size = body.len() as u64;
        Ok(Response::from_parts(
            parts,
            HttpBody::new(stream::iter([Ok(body)]), Some(size)),
        ))
    }
}

fn initiated() -> Result<Response<Buffer>> {
    Ok(Response::builder()
        .header(LOCATION, "https://www.googleapis.com/upload/session")
        .body(Buffer::new())
        .unwrap())
}

fn incomplete(persisted: usize) -> Result<Response<Buffer>> {
    let mut response = Response::builder().status(308);
    if persisted > 0 {
        response = response.header(RANGE, format!("bytes=0-{}", persisted - 1));
    }
    Ok(response.body(Buffer::new()).unwrap())
}

fn completed() -> Result<Response<Buffer>> {
    Ok(Response::new(Buffer::from(
        r#"{"id":"file-id","name":"file","mimeType":"application/octet-stream"}"#,
    )))
}

fn lost_response() -> Result<Response<Buffer>> {
    Err(Error::new(ErrorKind::Unexpected, "lost upload response").set_temporary())
}

fn writer(transport: ScriptedTransport) -> GdriveWriter {
    let mut signer = GdriveSigner::new();
    signer.access_token = "test-token".into();
    signer.expires_in = Timestamp::MAX;
    let signer = Arc::new(asyncband::mutex::Mutex::new(signer));
    let core = Arc::new(GdriveCore {
        info: ServiceInfo::new("gdrive", "/", ""),
        capability: Capability::default(),
        root: "/".into(),
        drive_id: None,
        signer: signer.clone(),
        path_index: GdrivePathIndex::new(GdrivePathQuery::new(signer, None)),
        recent_entries: asyncband::mutex::Mutex::new(GdriveRecentState::default()),
    });
    GdriveWriter::new(
        core,
        OperationContext::new().with_http_transport(HttpTransporter::new(transport)),
        "/file".into(),
        Some("file-id".into()),
    )
}

#[tokio::test]
async fn small_and_empty_writes_use_one_upload() {
    for size in [0, 17, CHUNK_SIZE] {
        let transport = ScriptedTransport::new(vec![completed()]);
        let mut writer = writer(transport.clone());
        let content = vec![7; size];
        writer
            .write(content[..size / 2].to_vec().into())
            .await
            .unwrap();
        writer
            .write(content[size / 2..].to_vec().into())
            .await
            .unwrap();
        assert_eq!(writer.close().await.unwrap().content_length(), size as u64);
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method(), "PATCH");
        assert!(
            requests[0]
                .uri()
                .query()
                .unwrap()
                .contains("uploadType=media")
        );
        assert_eq!(requests[0].body().to_vec(), content);
        transport.assert_finished();
    }
}

#[tokio::test]
async fn interrupted_write_resumes_before_accepting_retried_input() {
    for persisted in [0, 256 * 1024, CHUNK_SIZE] {
        let mut responses = vec![initiated(), lost_response(), incomplete(persisted)];
        if persisted < CHUNK_SIZE {
            responses.push(incomplete(CHUNK_SIZE));
        }
        responses.push(completed());
        let transport = ScriptedTransport::new(responses);
        let mut writer = writer(transport.clone());
        writer.write(vec![7; CHUNK_SIZE + 3].into()).await.unwrap();
        assert!(
            writer
                .write(vec![9; 5].into())
                .await
                .unwrap_err()
                .is_temporary()
        );
        writer.write(vec![9; 5].into()).await.unwrap();
        assert_eq!(
            writer.close().await.unwrap().content_length(),
            (CHUNK_SIZE + 8) as u64
        );

        let mut expected = vec![format!("bytes 0-{}/*", CHUNK_SIZE - 1), "bytes */*".into()];
        if persisted < CHUNK_SIZE {
            expected.push(format!("bytes {persisted}-{}/*", CHUNK_SIZE - 1));
            let requests = transport.requests.lock().unwrap();
            assert_eq!(requests[3].body().to_vec(), vec![7; CHUNK_SIZE - persisted]);
        }
        expected.push(format!(
            "bytes {CHUNK_SIZE}-{}/{}",
            CHUNK_SIZE + 7,
            CHUNK_SIZE + 8
        ));
        assert_eq!(transport.ranges(), expected);
        let requests = transport.requests.lock().unwrap();
        assert_eq!(
            requests.last().unwrap().body().to_vec(),
            [vec![7; 3], vec![9; 5]].concat()
        );
        transport.assert_finished();
    }
}

#[tokio::test]
async fn partial_acknowledgement_resumes_from_server_offset() {
    let persisted = 256 * 1024;
    let transport = ScriptedTransport::new(vec![
        initiated(),
        incomplete(persisted),
        incomplete(persisted),
        incomplete(CHUNK_SIZE),
        completed(),
    ]);
    let mut writer = writer(transport.clone());
    writer.write(vec![7; CHUNK_SIZE + 1].into()).await.unwrap();
    assert!(writer.close().await.unwrap_err().is_temporary());
    writer.close().await.unwrap();
    assert_eq!(
        transport.ranges(),
        vec![
            format!("bytes 0-{}/*", CHUNK_SIZE - 1),
            "bytes */*".into(),
            format!("bytes {persisted}-{}/*", CHUNK_SIZE - 1),
            format!("bytes {CHUNK_SIZE}-{CHUNK_SIZE}/{}", CHUNK_SIZE + 1),
        ]
    );
    transport.assert_finished();
}

#[tokio::test]
async fn lost_final_response_queries_completion_or_resumes_remaining_bytes() {
    let tail = 512 * 1024;
    let total = CHUNK_SIZE + tail;
    for already_complete in [false, true] {
        let mut responses = vec![initiated(), incomplete(CHUNK_SIZE), lost_response()];
        if !already_complete {
            responses.push(incomplete(CHUNK_SIZE + tail / 2));
        }
        responses.push(completed());
        let transport = ScriptedTransport::new(responses);
        let mut writer = writer(transport.clone());
        writer.write(vec![7; total].into()).await.unwrap();
        assert!(writer.close().await.unwrap_err().is_temporary());
        assert_eq!(writer.close().await.unwrap().content_length(), total as u64);
        let mut expected = vec![
            format!("bytes 0-{}/*", CHUNK_SIZE - 1),
            format!("bytes {CHUNK_SIZE}-{}/{total}", total - 1),
            format!("bytes */{total}"),
        ];
        if !already_complete {
            expected.push(format!(
                "bytes {}-{}/{total}",
                CHUNK_SIZE + tail / 2,
                total - 1
            ));
        }
        assert_eq!(transport.ranges(), expected);
        transport.assert_finished();
    }
}

#[tokio::test]
async fn expired_session_does_not_start_another_upload() {
    let transport = ScriptedTransport::new(vec![
        initiated(),
        lost_response(),
        Ok(Response::builder().status(404).body(Buffer::new()).unwrap()),
    ]);
    let mut writer = writer(transport.clone());
    writer.write(vec![7; CHUNK_SIZE + 1].into()).await.unwrap();
    assert!(writer.close().await.unwrap_err().is_temporary());
    assert_eq!(
        writer.close().await.unwrap_err().kind(),
        ErrorKind::NotFound
    );
    assert_eq!(transport.requests.lock().unwrap().len(), 3);
    transport.assert_finished();
}

#[tokio::test]
async fn abort_cancels_an_active_session() {
    let transport = ScriptedTransport::new(vec![
        initiated(),
        incomplete(CHUNK_SIZE),
        Ok(Response::builder().status(499).body(Buffer::new()).unwrap()),
    ]);
    let mut writer = writer(transport.clone());
    writer.write(vec![7; CHUNK_SIZE + 1].into()).await.unwrap();
    writer.write(vec![9; 1].into()).await.unwrap();
    writer.abort().await.unwrap();
    assert_eq!(
        transport.requests.lock().unwrap().last().unwrap().method(),
        "DELETE"
    );
    assert!(writer.pending.is_empty());
    transport.assert_finished();
}
