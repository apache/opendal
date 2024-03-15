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

use anyhow::Result;
use bytes::Bytes;
use dav_server::davpath::DavPath;
use dav_server::fs::DavFileSystem;
use dav_server_opendalfs::OpendalFs;
use opendal::services::Fs;
use opendal::Operator;

#[tokio::test]
async fn test_metadata() -> Result<()> {
    let mut builder = Fs::default();
    builder.root("/tmp");

    let op = Operator::new(builder)?.finish();

    let webdavfs = OpendalFs::new(op);

    let metadata = webdavfs
        .metadata(&DavPath::new("/").unwrap())
        .await
        .unwrap();
    assert_eq!(true, metadata.is_dir());

    Ok(())
}

#[tokio::test]
async fn test_write_and_read() -> Result<()> {
    let mut builder = Fs::default();
    builder.root("/tmp");

    let op = Operator::new(builder)?.finish();

    let webdavfs = OpendalFs::new(op);

    let path = &DavPath::new("/test_opendalfs_write_read.txt").expect("path must be valid");
    let content = "Hello dav-server-opendalfs.";

    let mut davfile = webdavfs
        .open(path, dav_server::fs::OpenOptions::default())
        .await
        .expect("open file must succeed");

    let num = 999;
    for i in 0..num {
        davfile
            .write_bytes(Bytes::from(format!("{}{}", content, i)))
            .await
            .expect("write file must succeed");
    }

    for i in 0..num {
        let read_content = davfile
            .read_bytes(content.len() + i.to_string().len())
            .await
            .expect("read file must succeed");
        assert_eq!(read_content, Bytes::from(format!("{}{}", content, i)));
    }

    webdavfs
        .remove_file(path)
        .await
        .expect("remove file must succeed");
    Ok(())
}
