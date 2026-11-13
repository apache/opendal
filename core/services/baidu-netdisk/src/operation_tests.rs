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

use crate::BaiduNetdisk;
use http::{Request, Response};
use opendal_core::*;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

type Files = Arc<Mutex<BTreeMap<String, bool>>>;

// Regression fixtures for directory replacement and deletion bugs reproduced
// against Baidu. Unexpected mutation requests fail rather than being accepted.
#[derive(Clone)]
struct FileServer(Files);

fn metadata(path: &str, is_dir: bool) -> Value {
    json!({"fs_id": 1, "path": path, "isdir": u8::from(is_dir), "size": 0})
}

impl HttpTransport for FileServer {
    async fn fetch(&self, request: Request<Buffer>) -> Result<Response<HttpBody>> {
        let query: HashMap<_, _> =
            form_urlencoded::parse(request.uri().query().unwrap_or_default().as_bytes())
                .into_owned()
                .collect();
        let mut files = self.0.lock().unwrap();
        let value = match query["method"].as_str() {
            "meta" => match files.get(&query["path"]) {
                Some(is_dir) => json!({"errno": 0, "list": [metadata(&query["path"], *is_dir)]}),
                None => json!({"errno": -9}),
            },
            "list" => {
                let prefix = format!("{}/", query["dir"]);
                let start = query["start"].parse::<usize>().unwrap();
                let limit = query["limit"].parse::<usize>().unwrap();
                let entries: Vec<_> = files
                    .iter()
                    .filter(|(path, _)| {
                        path.strip_prefix(&prefix)
                            .is_some_and(|tail| !tail.contains('/'))
                    })
                    .skip(start)
                    .take(limit)
                    .map(|(path, is_dir)| metadata(path, *is_dir))
                    .collect();
                json!({"errno": 0, "list": entries})
            }
            "filemanager" if query["opera"] == "delete" => {
                let body = request.into_body().to_bytes();
                let form: HashMap<_, _> = form_urlencoded::parse(&body).into_owned().collect();
                let paths: Vec<String> = serde_json::from_str(&form["filelist"]).unwrap();
                let mut info = Vec::new();
                for path in paths {
                    let prefix = format!("{path}/");
                    files.retain(|candidate, _| {
                        candidate != &path && !candidate.starts_with(&prefix)
                    });
                    info.push(json!({"errno": 0, "path": path}));
                }
                json!({"errno": 0, "info": info})
            }
            method => panic!("unexpected mutation request: {method}"),
        };
        let body = Buffer::from(value.to_string());
        let size = body.len() as u64;
        Ok(Response::new(HttpBody::new(
            futures::stream::iter(vec![Ok(body)]),
            Some(size),
        )))
    }
}

fn operator(entries: impl IntoIterator<Item = (String, bool)>) -> (Operator, Files) {
    let mut files = BTreeMap::from([("/apps/test".to_string(), true)]);
    files.extend(
        entries
            .into_iter()
            .map(|(path, is_dir)| (format!("/apps/test/{path}"), is_dir)),
    );
    let files = Arc::new(Mutex::new(files));
    let ctx = OperationContext::new()
        .with_http_transport(HttpTransporter::new(FileServer(files.clone())));
    let op = Operator::new(
        BaiduNetdisk::default()
            .root("/apps/test/")
            .access_token("test-token"),
    )
    .unwrap()
    .with_context(ctx);
    (op, files)
}

#[tokio::test]
async fn directory_destinations_are_rejected_before_mutation() {
    let (op, files) = operator([
        ("source".into(), false),
        ("target".into(), true),
        ("target/child".into(), false),
    ]);
    let before = files.lock().unwrap().clone();
    assert_eq!(
        op.copy("source", "target").await.unwrap_err().kind(),
        ErrorKind::IsADirectory
    );
    assert_eq!(
        op.rename("source", "target").await.unwrap_err().kind(),
        ErrorKind::IsADirectory
    );
    assert_eq!(
        op.write("target", "replacement").await.unwrap_err().kind(),
        ErrorKind::IsADirectory
    );
    assert_eq!(*files.lock().unwrap(), before);
}

#[tokio::test]
async fn recursive_directory_delete_preserves_same_named_file() {
    let (op, files) = operator([("file.txt".into(), false)]);
    op.delete_with("file.txt/").recursive(true).await.unwrap();
    assert!(files.lock().unwrap().contains_key("/apps/test/file.txt"));
}

#[tokio::test]
async fn recursive_prefix_delete_collects_all_pages_before_mutation() {
    let mut entries: Vec<_> = (0..1001)
        .map(|i| (format!("nested/match-{i:04}"), false))
        .collect();
    entries.extend([
        ("nested".into(), true),
        ("nested/match".into(), false),
        ("nested/match-tree".into(), true),
        ("nested/match-tree/child".into(), false),
        ("nested/keep".into(), false),
        ("match-outside".into(), false),
    ]);
    let (op, files) = operator(entries);
    op.delete_with("nested/match")
        .recursive(true)
        .await
        .unwrap();
    let remaining: Vec<_> = files.lock().unwrap().keys().cloned().collect();
    assert_eq!(
        remaining,
        [
            "/apps/test",
            "/apps/test/match-outside",
            "/apps/test/nested",
            "/apps/test/nested/keep"
        ]
    );
    op.delete_with("nested/match")
        .recursive(true)
        .await
        .unwrap();
}
