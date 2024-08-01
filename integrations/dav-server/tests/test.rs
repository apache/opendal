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
use dav_server::davpath::DavPath;
use dav_server::fs::DavFileSystem;
use dav_server_opendalfs::OpendalFs;
use opendal::services::Fs;
use opendal::Operator;

#[tokio::test]
async fn test() -> Result<()> {
    let builder = Fs::default().root("/tmp");

    let op = Operator::new(builder)?.finish();

    let webdavfs = OpendalFs::new(op);

    let metadata = webdavfs
        .metadata(&DavPath::new("/").unwrap())
        .await
        .unwrap();
    println!("{}", metadata.is_dir());

    Ok(())
}
